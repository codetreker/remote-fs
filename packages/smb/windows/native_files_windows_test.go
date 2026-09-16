package windows

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type nativeSession struct {
	authority   *nativeAuthority
	id          uint64
	options     storage.FileSessionOptions
	epoch       string
	expires     time.Time
	revision    uint64
	closed      bool
	files       map[storage.FileReferenceID]*nativeFile
	actions     map[storage.FileActionID]nativeReceipt
	owners      map[storage.RangeOwnerID]bool
	waits       map[storage.FileActionID]*fileaccess.Wait
	changed     chan struct{}
	rangeCounts map[nativeRangeKey]int
}
type nativeReceipt struct {
	fingerprint [32]byte
	result      storage.FileActionReceipt
	err         error
}
type nativeFile struct {
	session   *nativeSession
	node      *nativeNode
	reference storage.FileReferenceID
	claim     storage.AccessClaim
	closed    bool
	prepared  *storage.RemovalStatus
}

func nativeConflict(kind storage.FileConflictKind) error {
	return &storage.FileError{Code: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: kind}}
}
func nativeAccessError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, fileaccess.ErrConflict):
		return nativeConflict(storage.ConflictClaim)
	case errors.Is(err, fileaccess.ErrCapacity):
		return nativeConflict(storage.ConflictCapacity)
	case errors.Is(err, fileaccess.ErrRevision):
		return nativeConflict(storage.ConflictRevision)
	case errors.Is(err, fileaccess.ErrUnknownClaim), errors.Is(err, fileaccess.ErrAccess):
		return syscall.EBADF
	case errors.Is(err, fileaccess.ErrRetired):
		return syscall.ESTALE
	default:
		return err
	}
}
func (a *nativeAuthority) use(n *nativeNode, ref storage.FileReferenceID, uses storage.AccessUse) error {
	return nativeAccessError(a.access.CheckUse(n.attr.ID, uint64(ref), uint64(uses)))
}

var _ storage.FileSession = (*nativeSession)(nil)
var _ storage.File = (*nativeFile)(nil)

func (a *nativeAuthority) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	if err := o.Check(); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.sessions) >= 32 {
		return nil, storage.FileSessionStatus{}, syscall.EMFILE
	}
	a.nextSession++
	s := &nativeSession{authority: a, id: a.nextSession, options: o, epoch: fmt.Sprintf("native-%d", a.nextSession), expires: time.Now().Add(o.Lease), revision: 1, files: map[storage.FileReferenceID]*nativeFile{}, actions: map[storage.FileActionID]nativeReceipt{}, owners: map[storage.RangeOwnerID]bool{}, waits: map[storage.FileActionID]*fileaccess.Wait{}, changed: make(chan struct{}), rangeCounts: map[nativeRangeKey]int{}}
	a.sessions = append(a.sessions, s)
	return s, s.status(), nil
}
func (s *nativeSession) health(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.authority.expireSessions(); err != nil {
		return err
	}
	if s.closed {
		return syscall.ESTALE
	}
	if !time.Now().Before(s.expires) {
		if err := s.retire(); err != nil {
			return err
		}
		return syscall.ESTALE
	}
	return nil
}
func (s *nativeSession) status() storage.FileSessionStatus {
	remaining := max(0, time.Until(s.expires))
	if s.closed {
		remaining = 0
	}
	return storage.FileSessionStatus{Epoch: s.epoch, Remaining: remaining, Revision: s.revision, ActionEpoch: 1, HistoryRemaining: s.options.History, Retired: s.closed}
}
func (s *nativeSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.FileSessionStatus{}, err
	}
	if err := s.authority.expireSessions(); err != nil {
		return storage.FileSessionStatus{}, err
	}
	return s.status(), nil
}
func (s *nativeSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if err := s.health(ctx); err != nil {
		return storage.FileSessionStatus{}, err
	}
	s.expires = time.Now().Add(s.options.Lease)
	s.revision++
	close(s.changed)
	s.changed = make(chan struct{})
	return s.status(), nil
}
func nativeReceiptCopy(r storage.FileActionReceipt) storage.FileActionReceipt {
	r.Observation.Attr = nativeAttr(&nativeNode{attr: r.Observation.Attr})
	if r.Conflict != nil {
		conflict := *r.Conflict
		if conflict.Range != nil {
			held := *conflict.Range
			conflict.Range = &held
		}
		r.Conflict = &conflict
	}
	r.Observation.LinkTarget = bytes.Clone(r.Observation.LinkTarget)
	if r.Observation.Location != nil {
		location := *r.Observation.Location
		location.Ancestors = slices.Clone(location.Ancestors)
		for i := range location.Ancestors {
			location.Ancestors[i].Name = bytes.Clone(location.Ancestors[i].Name)
		}
		r.Observation.Location = &location
	}
	return r
}

func nativeNotAdmitted(err error) error {
	if err == nil {
		return nil
	}
	return &storage.FileError{NotAdmitted: true, Code: storage.ErrnoOf(err), Cause: err}
}
func (s *nativeSession) run(ctx context.Context, id storage.FileActionID, operation storage.Operation, input any, fn func() (storage.FileActionReceipt, error)) (storage.FileActionReceipt, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	return s.runLocked(ctx, id, operation, input, fn)
}
func (s *nativeSession) runLocked(ctx context.Context, id storage.FileActionID, operation storage.Operation, input any, fn func() (storage.FileActionReceipt, error)) (storage.FileActionReceipt, error) {
	encoded, err := json.Marshal(struct {
		Operation storage.Operation
		Input     any
	}{operation, input})
	if err != nil {
		return storage.FileActionReceipt{}, nativeNotAdmitted(err)
	}
	fingerprint := sha256.Sum256(encoded)
	if old, ok := s.actions[id]; ok {
		if old.fingerprint != fingerprint {
			return storage.FileActionReceipt{}, nativeNotAdmitted(syscall.EINVAL)
		}
		return nativeReceiptCopy(old.result), old.err
	}
	epoch, err := id.Epoch()
	if err != nil {
		return storage.FileActionReceipt{}, nativeNotAdmitted(err)
	}
	if epoch != 1 {
		return storage.FileActionReceipt{}, nativeNotAdmitted(syscall.ESTALE)
	}
	if operation != "file.session-close" {
		if err := s.health(ctx); err != nil {
			return storage.FileActionReceipt{}, nativeNotAdmitted(err)
		}
	} else if err := ctx.Err(); err != nil {
		return storage.FileActionReceipt{}, nativeNotAdmitted(err)
	}
	total := 0
	for _, session := range s.authority.sessions {
		total += len(session.actions)
	}
	if total >= 1024 || len(s.actions) >= s.options.MaxActions {
		return storage.FileActionReceipt{}, nativeNotAdmitted(syscall.ENOSPC)
	}
	result, err := fn()
	result.Action = id
	result.Operation = operation
	if result.State == 0 {
		result.State = storage.FileActionCompleted
	}
	result.HistoryRemaining = s.options.History
	if err != nil {
		if result.Effects == 0 {
			result.State = storage.FileActionNotApplied
		} else {
			result.State = storage.FileActionCompleted
		}
		result.Errno = storage.ErrnoOf(err)
		var fault *storage.FileError
		if errors.As(err, &fault) {
			result.Conflict = fault.Conflict
		}
	}
	s.actions[id] = nativeReceipt{fingerprint: fingerprint, result: nativeReceiptCopy(result), err: err}
	return nativeReceiptCopy(result), err
}
func (s *nativeSession) unknown(id storage.FileActionID) (storage.FileActionReceipt, error) {
	epoch, err := id.Epoch()
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if epoch != 1 || s.closed || !time.Now().Before(s.expires) {
		return storage.FileActionReceipt{}, syscall.ESTALE
	}
	return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown, Errno: syscall.EIO, HistoryRemaining: s.options.History}, syscall.EIO
}
func (s *nativeSession) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	r, ok := s.actions[id]
	if !ok {
		return s.unknown(id)
	}
	return nativeReceiptCopy(r.result), r.err
}
func (s *nativeSession) CancelAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	old, ok := s.actions[id]
	if !ok {
		return s.unknown(id)
	}
	if wait := s.waits[id]; wait != nil {
		wait.Cancel()
		delete(s.waits, id)
		old.result.State = storage.FileActionNotApplied
		old.result.Errno = syscall.EINTR
		old.err = context.Canceled
		s.actions[id] = old
	}
	return nativeReceiptCopy(old.result), old.err
}

func (s *nativeSession) Reference(ctx context.Context, id storage.FileReferenceID) (storage.File, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if err := s.health(ctx); err != nil {
		return nil, err
	}
	f := s.files[id]
	if f == nil || f.closed {
		return nil, syscall.EBADF
	}
	return f, nil
}
func (f *nativeFile) NodeID() uint64                     { return f.node.attr.ID }
func (f *nativeFile) Reference() storage.FileReferenceID { return f.reference }
func (f *nativeFile) health(ctx context.Context) error {
	if err := f.session.health(ctx); err != nil {
		return err
	}
	if f.closed {
		return syscall.EBADF
	}
	return nil
}
func (f *nativeFile) observation() storage.FileObservation {
	result := f.session.authority.observation(f.node)
	result.Removal = f.removal()
	return result
}
func (f *nativeFile) Stat(ctx context.Context, options storage.ObservationOptions) (storage.FileObservation, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := f.health(ctx); err != nil {
		return storage.FileObservation{}, err
	}
	result := a.observe(f.node, options)
	result.Removal = f.removal()
	return result, nil
}
func (s *nativeSession) StatNode(ctx context.Context, id uint64, options storage.ObservationOptions) (storage.FileObservation, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if err := s.health(ctx); err != nil {
		return storage.FileObservation{}, err
	}
	n := s.authority.nodes[id]
	if n == nil {
		return storage.FileObservation{}, syscall.ESTALE
	}
	return s.authority.observe(n, options), nil
}
func nativeSameLocation(a, b storage.EntryLocation) bool {
	if a.State != b.State || a.RootNodeID != b.RootNodeID || a.NodeID != b.NodeID || len(a.Ancestors) != len(b.Ancestors) {
		return false
	}
	for i, x := range a.Ancestors {
		y := b.Ancestors[i]
		if x.ParentID != y.ParentID || x.DirectoryRevision != y.DirectoryRevision || x.EntryID != y.EntryID || x.NodeID != y.NodeID || !bytes.Equal(x.Name, y.Name) {
			return false
		}
	}
	return true
}
func (s *nativeSession) checkWitness(w storage.EntryLocation) error {
	if err := w.Check(); err != nil {
		return err
	}
	n := s.authority.nodes[w.NodeID]
	if n == nil {
		return nativeConflict(storage.ConflictIdentity)
	}
	if !nativeSameLocation(s.authority.location(n), w) {
		return nativeConflict(storage.ConflictRevision)
	}
	return nil
}
func (s *nativeSession) target(t storage.EntryTarget) (*nativeNode, *nativeNode, error) {
	if err := t.Check(); err != nil {
		return nil, nil, err
	}
	parent := s.files[t.Parent]
	if parent == nil || parent.closed || parent.node.attr.ID != t.ParentID {
		return nil, nil, syscall.EBADF
	}
	p := parent.node
	if !p.attr.IsDir() {
		return nil, nil, syscall.ENOTDIR
	}
	if p.detached || p.state == storage.EntryDraining {
		return nil, nil, nativeConflict(storage.ConflictDraining)
	}
	if t.Witness != nil {
		if err := s.checkWitness(*t.Witness); err != nil {
			return nil, nil, err
		}
		if t.Witness.NodeID != p.attr.ID {
			return nil, nil, syscall.EINVAL
		}
	}
	if p.attr.DirectoryRevision != t.DirectoryRevision {
		return nil, nil, nativeConflict(storage.ConflictRevision)
	}
	n := s.authority.child(p.attr.ID, string(t.Name))
	if (t.ExpectedEntryID == 0) != (t.ExpectedNodeID == 0) {
		return nil, nil, syscall.EINVAL
	}
	if t.ExpectedNodeID == 0 {
		if n != nil {
			return nil, nil, nativeConflict(storage.ConflictIdentity)
		}
	} else if n == nil || n.attr.ID != t.ExpectedNodeID || n.entry != t.ExpectedEntryID {
		return nil, nil, nativeConflict(storage.ConflictIdentity)
	}
	if n != nil && t.ExpectedMetadataRevision != 0 && n.attr.MetadataRevision != t.ExpectedMetadataRevision {
		return nil, nil, nativeConflict(storage.ConflictRevision)
	}
	return p, n, nil
}
func (s *nativeSession) publicationGuard(ctx context.Context) fileaccess.Guard {
	return func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.closed || !time.Now().Before(s.expires) {
			return syscall.ESTALE
		}
		return nil
	}
}
func (f *nativeFile) publicationGuard(ctx context.Context) fileaccess.Guard {
	return func() error {
		if err := f.session.publicationGuard(ctx)(); err != nil {
			return err
		}
		if f.closed {
			return syscall.EBADF
		}
		return nil
	}
}
func (s *nativeSession) retain(ctx context.Context, n *nativeNode, claim storage.AccessClaim, prepared *storage.RemovalCondition) (storage.FileActionReceipt, error) {
	a := s.authority
	if n.state == storage.EntryDraining {
		return storage.FileActionReceipt{}, nativeConflict(storage.ConflictDraining)
	}
	if err := claim.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	active, own := 0, 0
	for _, session := range a.sessions {
		for _, file := range session.files {
			if !file.closed {
				active++
				if session == s {
					own++
				}
			}
		}
	}
	if active >= 256 || own >= s.options.MaxFiles {
		return storage.FileActionReceipt{}, syscall.EMFILE
	}
	if prepared != nil {
		if err := prepared.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if n.attr.ID == 1 || n.detached {
			return storage.FileActionReceipt{}, syscall.EBUSY
		}
		if n.attr.IsDir() != (*prepared == storage.RemovalIfEmpty) {
			return storage.FileActionReceipt{}, syscall.EINVAL
		}
		if claim.Uses&storage.RemoveEntry == 0 {
			return storage.FileActionReceipt{}, syscall.EBADF
		}
	}
	ref := storage.FileReferenceID(a.nextRef + 1)
	if err := nativeAccessError(a.access.RegisterClaim(uint64(ref), s.id, n.attr.ID, fileaccess.Claim{Uses: uint64(claim.Uses), Excludes: uint64(claim.Excludes)}, s.publicationGuard(ctx))); err != nil {
		return storage.FileActionReceipt{}, err
	}
	a.nextRef++
	f := &nativeFile{session: s, node: n, reference: ref, claim: claim}
	s.files[ref] = f
	if prepared != nil {
		if err := f.prepareCondition(*prepared); err != nil {
			delete(s.files, ref)
			if closeErr := a.access.CloseClaim(uint64(ref)); closeErr != nil {
				return storage.FileActionReceipt{}, errors.Join(err, closeErr)
			}
			return storage.FileActionReceipt{}, err
		}
	}
	result := storage.FileActionReceipt{Reference: ref, Observation: a.observation(n), Effects: storage.EffectRetained}
	if f.prepared != nil {
		result.Removal = f.removal()
		result.Observation.Removal = result.Removal
	}
	return result, nil
}
func (s *nativeSession) Retain(ctx context.Context, r storage.RetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.run(ctx, id, "file.retain", r, func() (storage.FileActionReceipt, error) {
		if err := r.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		n := s.authority.nodes[r.NodeID]
		if n == nil {
			return storage.FileActionReceipt{}, syscall.ESTALE
		}
		if r.ExpectedMetadataRevision != 0 && n.attr.MetadataRevision != r.ExpectedMetadataRevision {
			return storage.FileActionReceipt{}, nativeConflict(storage.ConflictRevision)
		}
		if r.Witness != nil {
			if err := s.checkWitness(*r.Witness); err != nil {
				return storage.FileActionReceipt{}, err
			}
			if r.Witness.NodeID != r.NodeID {
				return storage.FileActionReceipt{}, syscall.EINVAL
			}
		}
		return s.retain(ctx, n, r.Claim, r.Prepared)
	})
}
func (s *nativeSession) RetainAt(ctx context.Context, r storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.run(ctx, id, "file.retain-at", r, func() (storage.FileActionReceipt, error) {
		if err := r.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		_, n, err := s.target(r.Target)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		if n == nil {
			return storage.FileActionReceipt{}, syscall.ENOENT
		}
		return s.retain(ctx, n, r.Claim, r.Prepared)
	})
}

func (s *nativeSession) CreateAndRetainAt(ctx context.Context, r storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.run(ctx, id, storage.OpFileCreateAndRetainAt, r, func() (storage.FileActionReceipt, error) { return s.create(ctx, r, false) })
}
func (s *nativeSession) ReplaceAndRetainAt(ctx context.Context, r storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.run(ctx, id, storage.OpFileReplaceAndRetainAt, r, func() (storage.FileActionReceipt, error) { return s.create(ctx, r, true) })
}
func (s *nativeSession) create(ctx context.Context, r storage.CreateAndRetainRequest, replace bool) (storage.FileActionReceipt, error) {
	if replace {
		if err := r.CheckReplace(); err != nil {
			return storage.FileActionReceipt{}, err
		}
	} else if err := r.CheckCreate(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	a := s.authority
	p, old, err := s.target(r.Target)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if !replace && old != nil {
		return storage.FileActionReceipt{}, syscall.EEXIST
	}
	if replace && old == nil {
		return storage.FileActionReceipt{}, syscall.ENOENT
	}
	if err := r.Initial.Kind.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	if err := r.Initial.Metadata.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	if len(r.Initial.LinkTarget) > storage.MaxLinkTargetBytes {
		return storage.FileActionReceipt{}, syscall.EFBIG
	}
	if r.Initial.Kind != storage.NodeSymlink && len(r.Initial.LinkTarget) != 0 {
		return storage.FileActionReceipt{}, syscall.EINVAL
	}
	if len(a.nodes) >= 64 {
		return storage.FileActionReceipt{}, syscall.ENOSPC
	}
	if old != nil {
		if old.attr.IsDir() {
			return storage.FileActionReceipt{}, syscall.EISDIR
		}
		if err := a.use(old, 0, storage.RemoveEntry); err != nil {
			return storage.FileActionReceipt{}, err
		}
	}
	now := time.Now().UTC()
	attr := storage.Attr{ID: a.nextNode + 1, Kind: r.Initial.Kind, Metadata: r.Initial.Metadata.Clone(), MetadataRevision: 1, DirectoryRevision: 0, AccessTime: now, ModTime: now, CreationTime: &now, ChangeTime: &now}
	if attr.Kind == storage.NodeDirectory {
		attr.DirectoryRevision = 1
	}
	if r.Initial.AccessTime != nil {
		attr.AccessTime = *r.Initial.AccessTime
	}
	if r.Initial.ModTime != nil {
		attr.ModTime = *r.Initial.ModTime
	}
	if r.Initial.CreationTime != nil {
		v := *r.Initial.CreationTime
		attr.CreationTime = &v
	}
	if r.Initial.ChangeTime != nil {
		v := *r.Initial.ChangeTime
		attr.ChangeTime = &v
	}
	if attr.Kind == storage.NodeSymlink {
		attr.Size = int64(len(r.Initial.LinkTarget))
	}
	n := &nativeNode{parent: p.attr.ID, name: string(r.Target.Name), entry: storage.EntryID(a.nextEntry + 1), attr: attr, link: bytes.Clone(r.Initial.LinkTarget), state: storage.EntryActive}
	if err := a.location(n).Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	result, err := s.retain(ctx, n, r.Claim, r.Prepared)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if old != nil {
		before := a.image(old)
		a.record(old, metastore.Removed, metastore.ChangeName, before)
		old.detached = true
		old.state = storage.EntryDetached
	}
	a.nextNode++
	a.nextEntry++
	a.nodes[n.attr.ID] = n
	p.attr.DirectoryRevision++
	a.record(n, metastore.Created, metastore.ChangeName, nil)
	result.Observation = a.observation(n)
	result.Effects |= storage.EffectCreated
	return result, nil
}
func (s *nativeSession) ResetAndRetainAt(ctx context.Context, r storage.ResetAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.run(ctx, id, storage.OpFileResetAndRetainAt, r, func() (storage.FileActionReceipt, error) {
		if err := r.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		_, n, err := s.target(r.Target)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		if n == nil {
			return storage.FileActionReceipt{}, syscall.ENOENT
		}
		if n.attr.Kind != storage.NodeRegular {
			return storage.FileActionReceipt{}, syscall.EISDIR
		}
		if n.attr.MetadataRevision != r.ExpectedRevision {
			return storage.FileActionReceipt{}, nativeConflict(storage.ConflictRevision)
		}
		if err := r.Change.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if err := s.authority.use(n, 0, storage.WriteContent); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if err := s.authority.access.CheckIO(n.attr.ID, nil, fileaccess.Span{Start: 0, End: uint64(max(n.attr.Size, 1) - 1), Boundary: n.attr.Size == 0}, true); err != nil {
			return storage.FileActionReceipt{}, nativeRangeError(err)
		}
		result, err := s.retain(ctx, n, r.Claim, r.Prepared)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		before := s.authority.image(n)
		n.data = nil
		n.attr.Size = 0
		s.applyAttr(n, r.Change)
		s.touch(n)
		s.authority.record(n, metastore.Modified, metastore.ChangeContent|metastore.ChangeSize|metastore.ChangeAttributes|metastore.ChangeTime, before)
		result.Observation = s.authority.observation(n)
		result.Effects |= storage.EffectContentChanged
		return result, nil
	})
}
func (s *nativeSession) touch(n *nativeNode) {
	now := time.Now().UTC()
	n.attr.MetadataRevision++
	n.attr.ChangeTime = &now
}
func (s *nativeSession) applyAttr(n *nativeNode, c storage.AttrChange) {
	if c.Metadata != nil {
		n.attr.Metadata = (*c.Metadata).Clone()
	}
	if c.AccessTime != nil {
		n.attr.AccessTime = *c.AccessTime
	}
	if c.ModTime != nil {
		n.attr.ModTime = *c.ModTime
	}
	if c.CreationTime != nil {
		v := *c.CreationTime
		n.attr.CreationTime = &v
	}
	if c.ChangeTime != nil {
		v := *c.ChangeTime
		n.attr.ChangeTime = &v
	}
}
func (f *nativeFile) CheckObservation(ctx context.Context, c storage.ObservationCondition) (storage.FileObservation, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := f.health(ctx); err != nil {
		return storage.FileObservation{}, err
	}
	if f.node.attr.MetadataRevision != c.MetadataRevision || f.node.attr.DirectoryRevision != c.DirectoryRevision {
		return storage.FileObservation{}, nativeConflict(storage.ConflictRevision)
	}
	if err := f.session.checkWitness(c.Location); err != nil {
		return storage.FileObservation{}, err
	}
	if c.Location.NodeID != f.node.attr.ID {
		return storage.FileObservation{}, syscall.EINVAL
	}
	return a.observation(f.node), nil
}
func (f *nativeFile) LookupAt(ctx context.Context, name []byte) (storage.EntryLookup, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := f.health(ctx); err != nil {
		return storage.EntryLookup{}, err
	}
	if err := storage.CheckEntryName(name); err != nil {
		return storage.EntryLookup{}, err
	}
	if !f.node.attr.IsDir() {
		return storage.EntryLookup{}, syscall.ENOTDIR
	}
	result := storage.EntryLookup{ParentID: f.node.attr.ID, DirectoryRevision: f.node.attr.DirectoryRevision, Name: bytes.Clone(name)}
	if n := a.child(f.node.attr.ID, string(name)); n != nil {
		result.Found = true
		result.EntryID = n.entry
		result.Attr = nativeAttr(n)
	}
	return result, nil
}
func (f *nativeFile) ListAt(ctx context.Context, r storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := f.health(ctx); err != nil {
		return storage.DirectoryPage{}, err
	}
	if err := r.Check(); err != nil {
		return storage.DirectoryPage{}, err
	}
	n := f.node
	if !n.attr.IsDir() {
		return storage.DirectoryPage{}, syscall.ENOTDIR
	}
	if r.Revision != 0 && r.Revision != n.attr.DirectoryRevision || !r.Cursor.IsZero() && (r.Cursor.ParentID != n.attr.ID || r.Cursor.Revision != n.attr.DirectoryRevision) {
		return storage.DirectoryPage{}, nativeConflict(storage.ConflictRevision)
	}
	page := storage.DirectoryPage{ParentID: n.attr.ID, Revision: n.attr.DirectoryRevision, Done: true}
	remaining := r.MaxBytes - storage.DirectoryPageBaseBytes
	if remaining < 0 {
		return storage.DirectoryPage{}, syscall.EFBIG
	}
	children := []*nativeNode{}
	for _, child := range a.nodes {
		if !child.detached && child.parent == n.attr.ID && bytes.Compare([]byte(child.name), r.Cursor.After) > 0 {
			children = append(children, child)
		}
	}
	slices.SortFunc(children, func(a, b *nativeNode) int { return strings.Compare(a.name, b.name) })
	for _, child := range children {
		size, err := child.attr.Metadata.EncodedSize()
		if err != nil {
			return storage.DirectoryPage{}, err
		}
		charge, err := storage.DirectoryEntryBytes(len(child.name), size)
		if err != nil {
			return storage.DirectoryPage{}, err
		}
		if charge > remaining || len(page.Entries) == r.MaxEntries {
			if len(page.Entries) == 0 {
				return storage.DirectoryPage{}, syscall.EFBIG
			}
			page.Done = false
			last := page.Entries[len(page.Entries)-1]
			page.Next = storage.DirectoryCursor{ParentID: n.attr.ID, Revision: n.attr.DirectoryRevision, After: bytes.Clone(last.Name)}
			break
		}
		remaining -= charge
		page.Entries = append(page.Entries, storage.DirectoryEntry{EntryID: child.entry, Name: []byte(child.name), Attr: nativeAttr(child)})
	}
	return page, nil
}

func (f *nativeFile) io(ctx context.Context, owner *storage.RangeOwnerID, offset, length int64, write bool) error {
	if err := f.health(ctx); err != nil {
		return err
	}
	if offset < 0 || length < 0 || length > math.MaxInt64-offset {
		return syscall.EINVAL
	}
	if f.node.attr.Kind == storage.NodeSymlink {
		return syscall.ELOOP
	}
	if f.node.attr.Kind != storage.NodeRegular {
		return syscall.EISDIR
	}
	uses := storage.ReadContent
	if write {
		uses = storage.WriteContent
	}
	a := f.session.authority
	if err := a.use(f.node, f.reference, uses); err != nil {
		return err
	}
	var actor *fileaccess.Owner
	if owner != nil {
		actor = &fileaccess.Owner{Session: f.session.id, ID: uint64(*owner)}
	}
	err := a.access.CheckIO(f.node.attr.ID, actor, fileaccess.Span{Start: uint64(offset), End: uint64(offset + max(length, 1) - 1), Boundary: length == 0}, write)
	if errors.Is(err, fileaccess.ErrConflict) {
		return nativeConflict(storage.ConflictRange)
	}
	return nativeAccessError(err)
}
func (f *nativeFile) ReadAt(ctx context.Context, r storage.FileReadRequest) (storage.FileRead, error) {
	if err := r.Check(); err != nil {
		return storage.FileRead{}, err
	}
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := f.io(ctx, r.Owner, r.Offset, int64(r.Length), false); err != nil {
		return storage.FileRead{}, err
	}
	if int64(len(f.node.data)) > f.session.options.MaxFileSize {
		return storage.FileRead{}, syscall.EFBIG
	}
	start := min(r.Offset, int64(len(f.node.data)))
	end := min(start+int64(r.Length), int64(len(f.node.data)))
	return storage.FileRead{Attr: nativeAttr(f.node), Data: bytes.Clone(f.node.data[start:end])}, nil
}
func (f *nativeFile) mutate(ctx context.Context, id storage.FileActionID, operation storage.Operation, input any, fn func() (storage.FileActionReceipt, error)) (storage.FileActionReceipt, error) {
	return f.session.run(ctx, id, operation, struct {
		Reference storage.FileReferenceID
		Input     any
	}{f.reference, input}, func() (storage.FileActionReceipt, error) {
		if err := f.health(ctx); err != nil {
			return storage.FileActionReceipt{}, err
		}
		return fn()
	})
}
func (f *nativeFile) WriteAt(ctx context.Context, r storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileWrite, r, func() (storage.FileActionReceipt, error) {
		if err := r.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if err := f.io(ctx, r.Owner, r.Offset, int64(len(r.Data)), true); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if r.ExpectedSize != nil && *r.ExpectedSize != f.node.attr.Size {
			return storage.FileActionReceipt{}, nativeConflict(storage.ConflictRevision)
		}
		size := max(int64(len(f.node.data)), r.Offset+int64(len(r.Data)))
		if size > nativeMaxFileBytes || size > f.session.options.MaxFileSize {
			return storage.FileActionReceipt{}, syscall.EFBIG
		}
		before := f.session.authority.image(f.node)
		data := make([]byte, size)
		copy(data, f.node.data)
		copy(data[r.Offset:], r.Data)
		f.node.data = data
		f.node.attr.Size = size
		f.node.attr.ModTime = time.Now().UTC()
		f.session.touch(f.node)
		f.session.authority.record(f.node, metastore.Modified, metastore.ChangeContent|metastore.ChangeSize|metastore.ChangeModTime|metastore.ChangeTime, before)
		return storage.FileActionReceipt{Observation: f.observation(), Effects: storage.EffectContentChanged}, nil
	})
}
func (f *nativeFile) Truncate(ctx context.Context, r storage.FileTruncateRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileTruncate, r, func() (storage.FileActionReceipt, error) {
		if err := r.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if r.Size < 0 || r.Size > nativeMaxFileBytes || r.Size > f.session.options.MaxFileSize {
			return storage.FileActionReceipt{}, syscall.EFBIG
		}
		start := min(r.Size, f.node.attr.Size)
		length := max(r.Size, f.node.attr.Size) - start
		if err := f.io(ctx, r.Owner, start, length, true); err != nil {
			return storage.FileActionReceipt{}, err
		}
		before := f.session.authority.image(f.node)
		data := make([]byte, r.Size)
		copy(data, f.node.data)
		f.node.data = data
		f.node.attr.Size = r.Size
		f.node.attr.ModTime = time.Now().UTC()
		f.session.touch(f.node)
		f.session.authority.record(f.node, metastore.Modified, metastore.ChangeContent|metastore.ChangeSize|metastore.ChangeModTime|metastore.ChangeTime, before)
		return storage.FileActionReceipt{Observation: f.observation(), Effects: storage.EffectContentChanged}, nil
	})
}
func (s *nativeSession) setAttr(n *nativeNode, c storage.AttrChange) (storage.FileActionReceipt, error) {
	if err := c.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	if c.ExpectedRevision != 0 && c.ExpectedRevision != n.attr.MetadataRevision {
		return storage.FileActionReceipt{}, nativeConflict(storage.ConflictRevision)
	}
	before := s.authority.image(n)
	s.applyAttr(n, c)
	s.touch(n)
	mask := metastore.ChangeAttributes | metastore.ChangeAccessTime | metastore.ChangeModTime | metastore.ChangeCreationTime | metastore.ChangeTime
	s.authority.record(n, metastore.Modified, mask, before)
	return storage.FileActionReceipt{Observation: s.authority.observation(n), Effects: storage.EffectMetadataChanged}, nil
}
func (f *nativeFile) SetAttr(ctx context.Context, c storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileSetAttr, c, func() (storage.FileActionReceipt, error) { return f.session.setAttr(f.node, c) })
}
func (s *nativeSession) SetNodeAttr(ctx context.Context, node uint64, c storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.run(ctx, id, storage.OpFileSetNodeAttr, struct {
		ID     uint64
		Change storage.AttrChange
	}{node, c}, func() (storage.FileActionReceipt, error) {
		n := s.authority.nodes[node]
		if n == nil {
			return storage.FileActionReceipt{}, syscall.ESTALE
		}
		return s.setAttr(n, c)
	})
}
func (f *nativeFile) SetKind(ctx context.Context, r storage.SetKindRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileSetKind, r, func() (storage.FileActionReceipt, error) {
		if err := r.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if r.Witness != nil {
			if r.Witness.NodeID != f.node.attr.ID {
				return storage.FileActionReceipt{}, syscall.EINVAL
			}
			if err := f.session.checkWitness(*r.Witness); err != nil {
				return storage.FileActionReceipt{}, err
			}
		}
		if err := r.Kind.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if err := r.Metadata.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if r.ExpectedRevision != f.node.attr.MetadataRevision {
			return storage.FileActionReceipt{}, nativeConflict(storage.ConflictRevision)
		}
		previousRevision := max(uint64(f.node.attr.MetadataRevision), uint64(f.node.attr.DirectoryRevision))
		if previousRevision >= math.MaxInt64 {
			return storage.FileActionReceipt{}, syscall.EOVERFLOW
		}
		nextRevision := previousRevision + 1
		if len(r.LinkTarget) > storage.MaxLinkTargetBytes || r.Kind != storage.NodeSymlink && len(r.LinkTarget) != 0 {
			return storage.FileActionReceipt{}, syscall.EINVAL
		}
		count, err := f.session.authority.access.ClaimCount(f.node.attr.ID)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		if count != 1 || len(f.node.data) != 0 || !f.session.authority.empty(f.node) {
			return storage.FileActionReceipt{}, syscall.EBUSY
		}
		if err := f.session.authority.use(f.node, f.reference, storage.WriteContent); err != nil {
			return storage.FileActionReceipt{}, err
		}
		length := max(f.node.attr.Size, int64(len(r.LinkTarget)))
		var owner *fileaccess.Owner
		if r.Owner != nil {
			owner = &fileaccess.Owner{Session: f.session.id, ID: uint64(*r.Owner)}
		}
		if err := f.session.authority.access.CheckIO(f.node.attr.ID, owner, fileaccess.Span{Start: 0, End: uint64(max(length, 1) - 1), Boundary: length == 0}, true); err != nil {
			return storage.FileActionReceipt{}, nativeRangeError(err)
		}
		if err := f.publicationGuard(ctx)(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		before := f.session.authority.image(f.node)
		f.node.attr.Kind = r.Kind
		if r.Kind == storage.NodeDirectory {
			f.node.attr.DirectoryRevision = storage.DirectoryRevision(nextRevision)
		} else {
			f.node.attr.DirectoryRevision = 0
		}
		f.node.attr.Metadata = r.Metadata.Clone()
		f.node.link = bytes.Clone(r.LinkTarget)
		f.node.attr.Size = int64(len(r.LinkTarget))
		f.node.attr.MetadataRevision = storage.NodeMetadataRevision(nextRevision)
		now := time.Now().UTC()
		f.node.attr.ChangeTime = &now
		f.session.authority.record(f.node, metastore.Modified, metastore.ChangeAttributes|metastore.ChangeContent|metastore.ChangeSize|metastore.ChangeTime, before)
		return storage.FileActionReceipt{Observation: f.observation(), Effects: storage.EffectMetadataChanged}, nil
	})
}
func (f *nativeFile) Sync(ctx context.Context) error {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	return f.health(ctx)
}
func (f *nativeFile) Rename(ctx context.Context, r storage.RenameRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileRename, r, func() (storage.FileActionReceipt, error) {
		if err := r.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		a := f.session.authority
		sourceParent, n, err := f.session.target(r.Source)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		if n != f.node || n.attr.ID == 1 {
			return storage.FileActionReceipt{}, nativeConflict(storage.ConflictIdentity)
		}
		destinationParent, other, err := f.session.target(r.Destination)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		name := r.Destination.Name
		if r.NewName != nil {
			name = r.NewName
		}
		if !bytes.Equal(name, r.Destination.Name) {
			if occupant := a.child(destinationParent.attr.ID, string(name)); occupant != nil && occupant != n {
				return storage.FileActionReceipt{}, nativeConflict(storage.ConflictIdentity)
			}
		}
		if err := a.use(n, f.reference, storage.RemoveEntry); err != nil {
			return storage.FileActionReceipt{}, err
		}
		for p := destinationParent; p.attr.ID != 1; p = a.nodes[p.parent] {
			if p == n {
				return storage.FileActionReceipt{}, syscall.EINVAL
			}
		}
		prospective := *n
		prospective.parent = destinationParent.attr.ID
		prospective.name = string(name)
		if err := a.location(&prospective).Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if other != nil && other != n {
			if other.attr.IsDir() != n.attr.IsDir() {
				return storage.FileActionReceipt{}, syscall.EINVAL
			}
			if !a.empty(other) {
				return storage.FileActionReceipt{}, syscall.ENOTEMPTY
			}
			if err := a.use(other, 0, storage.RemoveEntry); err != nil {
				return storage.FileActionReceipt{}, err
			}
			a.record(other, metastore.Removed, metastore.ChangeName, a.image(other))
			other.detached = true
			other.state = storage.EntryDetached
		}

		before := a.image(n)
		n.parent = destinationParent.attr.ID
		n.name = string(name)
		sourceParent.attr.DirectoryRevision++
		if destinationParent != sourceParent {
			destinationParent.attr.DirectoryRevision++
		}
		f.session.touch(n)
		a.record(n, metastore.Renamed, metastore.ChangeName|metastore.ChangeTime, before)
		return storage.FileActionReceipt{Observation: a.observation(n), Effects: storage.EffectEntryMoved}, nil
	})
}
