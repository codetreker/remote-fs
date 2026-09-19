package smb

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"sync"
	"syscall"
	"time"
)

type fileHandle struct {
	authority                  *authoritySession
	reference                  storage.NodeReference
	file                       storage.File
	nodeID                     uint64
	grantedAccess, shareAccess uint32
	scope                      *storage.UseScope
	// OwnerReference is retired atomically by the retained reference lifecycle.
	rangeOwner       *storage.UseOwner
	mu               sync.Mutex
	closeMu          cleanupGate
	retiring, closed bool
	active           int
	wake             chan struct{}
}
type handleRegistry struct {
	tree         *tree
	limits       Limits
	mu           sync.Mutex
	handles      map[wire.FileID]*fileHandle
	reservations map[*openReservation]struct{}
	slots        int
	resultBytes  int64
	retired      bool
	generation   uint64
}
type openReservation struct {
	registry                       *handleRegistry
	generation                     uint64
	mu                             sync.Mutex
	reference                      storage.NodeReference
	file                           storage.File
	attr                           storage.Attr
	outcome                        storage.OpenOutcome
	installed, finished, finishing bool
	ready                          chan struct{}
	responseDone                   chan struct{}
	cleanup                        cleanupGate
	charge                         int64
}

func newHandleRegistry(t *tree, l Limits) *handleRegistry {
	return &handleRegistry{tree: t, limits: l, handles: make(map[wire.FileID]*fileHandle), reservations: make(map[*openReservation]struct{})}
}
func (r *handleRegistry) get(id wire.FileID) *fileHandle {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retired {
		return nil
	}
	return r.handles[id]
}
func (r *handleRegistry) reserve() (*openReservation, error) {
	return r.reserveResult(false)
}

func (r *handleRegistry) reserveResponse() (*openReservation, error) {
	return r.reserveResult(true)
}

func (r *handleRegistry) reserveResult(response bool) (*openReservation, error) {
	a := r.tree.authority
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	if a.stopping || a.closed || time.Until(a.deadline) <= 0 {
		return nil, syscall.EIO
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retired || r.slots >= r.limits.MaxOpens {
		return nil, syscall.ENOMEM
	}
	charge, err := storage.MetadataRetentionBytes(storage.MaxMetadataBytes)
	if err != nil {
		return nil, err
	}
	charge += 512
	if charge > r.limits.MaxDirectoryBytes-r.resultBytes {
		return nil, syscall.ENOMEM
	}
	s := r.tree.export.server
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.tree.export.stopping || r.tree.export.opens >= r.limits.MaxOpens {
		return nil, syscall.ENOMEM
	}
	p := &openReservation{registry: r, generation: r.generation, charge: charge, ready: make(chan struct{})}
	if response {
		p.responseDone = make(chan struct{})
	}
	r.slots++
	r.resultBytes += charge
	r.tree.export.opens++
	r.reservations[p] = struct{}{}
	return p, nil
}
func (p *openReservation) attachFile(result storage.OpenResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reference = result.File
	p.file = result.File
	p.attr = result.Attr
	p.outcome = result.Outcome
	close(p.ready)
}
func (p *openReservation) attachNode(result storage.NodeOpenResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reference = result.Reference
	p.attr = result.Attr
	p.outcome = result.Outcome
	close(p.ready)
}
func (p *openReservation) install(access, share uint32) (wire.FileID, error) {
	r := p.registry
	a := r.tree.authority
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished || p.finishing || p.installed || p.reference == nil || p.attr.ID == 0 {
		return wire.FileID{}, syscall.EINVAL
	}
	if a.stopping || a.closed || r.retired || p.generation != r.generation || time.Until(a.deadline) <= 0 {
		return wire.FileID{}, syscall.EIO
	}
	var id wire.FileID
	for {
		rand.Read(id[:])
		if _, exists := r.handles[id]; !exists {
			break
		}
	}
	r.handles[id] = &fileHandle{authority: a, reference: p.reference, file: p.file, nodeID: p.attr.ID, grantedAccess: access, shareAccess: share, wake: make(chan struct{})}
	p.reference = nil
	p.file = nil
	p.installed = true
	return id, nil
}

// The captured metadata remains charged while its caller constructs a response,
// even if tree retirement starts as soon as the native result is attached.
func (p *openReservation) releaseResponse() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.responseDone != nil {
		close(p.responseDone)
		p.responseDone = nil
	}
}

// finish releases only a known-clean failed open or the caller's completed
// response ownership. A returned cleanup-only reference remains charged on error.
func (p *openReservation) finish(ctx context.Context) error {
	return p.cleanup.run(ctx, func() error {
		p.mu.Lock()
		responseDone := p.responseDone
		p.mu.Unlock()
		if responseDone != nil {
			select {
			case <-responseDone:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case <-p.ready:
		case <-ctx.Done():
			return ctx.Err()
		}
		p.mu.Lock()
		if p.finished {
			p.mu.Unlock()
			return nil
		}
		if p.finishing {
			p.mu.Unlock()
			return ErrBusy
		}
		p.finishing = true
		ref := p.reference
		p.mu.Unlock()
		if ref != nil && !p.registry.tree.authority.isClosed() {
			if err := ref.Close(ctx); err != nil {
				p.mu.Lock()
				p.finishing = false
				p.mu.Unlock()
				return err
			}
		}
		r := p.registry
		r.mu.Lock()
		defer r.mu.Unlock()
		p.mu.Lock()
		defer p.mu.Unlock()
		p.reference = nil
		p.file = nil
		p.attr = storage.Attr{}
		p.finished = true
		delete(r.reservations, p)
		r.resultBytes -= p.charge
		p.charge = 0
		if !p.installed {
			r.slots--
			s := r.tree.export.server
			s.mu.Lock()
			r.tree.export.opens--
			s.mu.Unlock()
		}
		return nil
	})
}
func (h *fileHandle) borrow() (func(), error) {
	a := h.authority
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	if a.stopping || a.closed || time.Until(a.deadline) <= 0 || h.retiring || h.closed {
		return nil, syscall.EBADF
	}
	h.active++
	return func() { h.mu.Lock(); h.active--; close(h.wake); h.wake = make(chan struct{}); h.mu.Unlock() }, nil
}
func (h *fileHandle) close(ctx context.Context) error {
	return h.closeMu.run(ctx, func() error {
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return nil
		}
		h.retiring = true
		h.mu.Unlock()
		var errs []error
		if !h.authority.isClosed() {

			if err := h.reference.Close(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if err := errors.Join(errs...); err != nil {
			return err
		}
		for {
			h.mu.Lock()
			if h.active == 0 {
				h.closed = true
				h.reference = nil
				h.file = nil
				h.rangeOwner = nil
				h.scope = nil
				h.mu.Unlock()
				return nil
			}
			wake := h.wake
			h.mu.Unlock()
			select {
			case <-wake:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
}
func (r *handleRegistry) close(ctx context.Context) error {
	r.mu.Lock()
	r.retired = true
	r.generation++
	hs := make(map[wire.FileID]*fileHandle, len(r.handles))
	for id, h := range r.handles {
		hs[id] = h
	}
	ps := make([]*openReservation, 0, len(r.reservations))
	for p := range r.reservations {
		ps = append(ps, p)
	}
	r.mu.Unlock()
	var errs []error
	for _, p := range ps {
		if err := p.finish(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	for id, h := range hs {
		if err := r.closeID(ctx, id, h); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func (r *handleRegistry) closeID(ctx context.Context, id wire.FileID, h *fileHandle) error {
	if err := h.close(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	if r.handles[id] == h {
		delete(r.handles, id)
		r.slots--
		s := r.tree.export.server
		s.mu.Lock()
		r.tree.export.opens--
		s.mu.Unlock()
	}
	r.mu.Unlock()
	return nil
}
func (c *connection) closeHandle(ctx context.Context, t *tree, request wire.Request) ([]byte, uint32) {
	if len(request.Body) < 24 || binary.LittleEndian.Uint16(request.Body) != 24 || binary.LittleEndian.Uint16(request.Body[2:])&^uint16(1) != 0 {
		return nil, statusInvalid
	}
	id, err := request.FileID()
	if err != nil {
		return nil, statusInvalid
	}
	h := t.files.get(id)
	if h == nil {
		return nil, statusError(syscall.EBADF)
	}
	if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: storage.OpFileClose}); err != nil {
		return nil, statusError(err)
	}
	c.cancelOpen(t.sessionID, t.id, id, request.Header.MessageID)
	if err := t.files.closeID(ctx, id, h); err != nil {
		return nil, statusError(err)
	}
	// POSTQUERY is optional; no attributes are returned when unavailable.
	return wire.CloseResponseBody(0, wire.FileInformation{}), 0
}
func statusError(err error) uint32 {
	if err == nil {
		return 0
	}
	if errors.Is(err, authz.ErrDenied) {
		return statusDenied
	}
	switch storage.ErrnoOf(err) {
	case syscall.EINVAL:
		return statusInvalid
	case syscall.EACCES, syscall.EPERM:
		return statusDenied
	case syscall.ENOMEM, syscall.ENOSPC, syscall.EMFILE, syscall.EFBIG:
		return statusResources
	case syscall.EOPNOTSUPP, syscall.ENOSYS:
		return statusUnsupported
	case syscall.EBADF:
		return 0xc0000008
	case syscall.ENOENT:
		return 0xc0000034
	case syscall.ENOTDIR:
		return 0xc0000103
	case syscall.EINTR:
		return statusCancelled
	default:
		return statusIO
	}
}
