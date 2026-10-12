package httprest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type transferRegistry struct {
	mu          sync.Mutex
	begins      *bodyAdmission
	backend     storage.FileTransfer
	options     TransferOptions
	gateWaiters int
	admission   *bodyAdmission
	history     *bodyAdmission
	entries     map[string]*servedTransfer
	closed      bool
	wake        chan struct{}
	done        chan struct{}
	err         error
}

type servedTransfer struct {
	mu              sync.Mutex
	token           string
	binding         storage.AccessBinding
	action          storage.ActionKey
	fingerprint     storage.ActionFingerprint
	digest          [32]byte
	declared        *int64
	importFile      storage.FileImport
	exportFile      storage.FileExport
	info            storage.ExportInfo
	ready           bool
	spool           *os.File
	spoolPath       string
	release         func()
	expires         time.Time
	closing         bool
	cleanupErr      error
	initializing    bool
	sealed          bool
	cancelRequested bool
	detachRequested bool
	settled         bool
	terminalClose   storage.ReferenceCloseResult
	terminalErr     error
	releaseHistory  func()
	consumed        bool
	actionGate      *bodyAdmission
}

func newTransferRegistry(s storage.Storage, options TransferOptions, gateWaiters int) (*transferRegistry, error) {
	if options.TerminalResults == 0 {
		options.TerminalResults = 8 * options.MaxConcurrent
	}
	if options.TerminalLifetime == 0 {
		options.TerminalLifetime = min(options.Lifetime, 2*time.Minute)
	}
	backend, ok := s.(storage.FileTransfer)
	if !ok {
		return nil, fmt.Errorf("httprest: transfer backend: %w", syscall.ENOSYS)
	}
	if err := backend.CheckFileTransfer(); err != nil {
		return nil, fmt.Errorf("httprest: transfer backing chain: %w", err)
	}
	info, err := os.Lstat(options.SpoolDir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("httprest: transfer spool must be a private directory: %w", syscall.EACCES)
	}
	r := &transferRegistry{backend: backend, options: options, gateWaiters: gateWaiters,
		admission: newBodyAdmission(options.MaxConcurrent, 0, 0),
		history:   newBodyAdmission(options.TerminalResults, 0, 0),
		begins:    newBodyAdmission(1, 0, gateWaiters),
		entries:   make(map[string]*servedTransfer), wake: make(chan struct{}, 1), done: make(chan struct{})}
	go r.run()
	return r, nil
}

func (r *transferRegistry) admit(ctx context.Context, size int64, action storage.ActionKey) (*servedTransfer, error) {
	if size < 0 || size > r.options.MaxBytes {
		return nil, syscall.EFBIG
	}
	var identity [32]byte
	rand.Read(identity[:])
	token := hex.EncodeToString(identity[:])
	releaseHistory, err := r.history.acquireOperation(ctx)
	if err != nil {
		return nil, err
	}
	releaseLocal, err := r.admission.acquireOperation(ctx)
	if err != nil {
		releaseHistory()
		return nil, err
	}
	binding, _ := storage.AccessBindingFrom(ctx)
	t := &servedTransfer{token: token, binding: binding, action: action, initializing: true, releaseHistory: releaseHistory, expires: time.Now().Add(r.options.Lifetime), release: releaseLocal}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		t.releaseHistory()
		t.release()
		return nil, syscall.EBUSY
	}
	t.actionGate = r.sharedActionGateLocked(action)
	r.entries[token] = t
	return t, nil
}

func (r *transferRegistry) lookup(ctx context.Context, token string) (*servedTransfer, error) {
	if !validFileCapability(token) {
		return nil, syscall.EINVAL
	}
	r.mu.Lock()
	t := r.entries[token]
	r.mu.Unlock()
	if t == nil {
		return nil, syscall.ESTALE
	}
	if err := storage.RequireAccessBinding(ctx, t.binding); err != nil {
		return nil, err
	}
	return t, nil
}

func (r *transferRegistry) sharedActionGate(action storage.ActionKey) *bodyAdmission {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sharedActionGateLocked(action)
}

func (r *transferRegistry) sharedActionGateLocked(action storage.ActionKey) *bodyAdmission {
	// Action identity and its gate are immutable before registry publication.
	// Control lookup cannot wait for a receive that owns the entry mutex.
	for _, entry := range r.entries {
		if entry.action == action {
			return entry.actionGate
		}
	}
	return newBodyAdmission(1, 0, r.gateWaiters)
}

func (r *transferRegistry) idle() bool {
	r.mu.Lock()
	entries := make([]*servedTransfer, 0, len(r.entries))
	for _, t := range r.entries {
		entries = append(entries, t)
	}
	r.mu.Unlock()
	for _, t := range entries {
		if !t.mu.TryLock() {
			return false
		}
		busy := !t.settled || t.action == (storage.ActionKey{}) && time.Now().Before(t.expires)
		t.mu.Unlock()
		if busy {
			return false
		}
	}
	return true
}

func (r *transferRegistry) forgetUnpublished(t *servedTransfer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.settled {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries[t.token] == t {
		delete(r.entries, t.token)
		t.releaseHistory()
	}
}

func (t *servedTransfer) cleanupContext(ctx context.Context) context.Context {
	if t.binding == (storage.AccessBinding{}) {
		return ctx
	}
	bound, err := storage.WithAccessBinding(ctx, t.binding)
	if err != nil {
		panic(err)
	}
	return bound
}

func (r *transferRegistry) settle(ctx context.Context, t *servedTransfer, requested ...string) (storage.ReferenceCloseResult, error) {
	if err := ctx.Err(); err != nil {
		return storage.ReferenceCloseResult{}, err
	}
	if !t.mu.TryLock() {
		return storage.ReferenceCloseResult{}, syscall.EBUSY
	}
	defer t.mu.Unlock()
	if t.initializing {
		return storage.ReferenceCloseResult{}, syscall.EBUSY
	}
	if t.settled {
		return t.terminalClose, t.terminalErr
	}
	if len(requested) != 0 && requested[0] == "cancel" {
		t.cancelRequested = true
	}
	if len(requested) != 0 && requested[0] == "detach" {
		t.detachRequested = true
	}
	t.closing = true
	result := storage.ReferenceCloseResult{Determined: true, Released: true}
	var semantic error
	if t.importFile != nil {
		var err error
		if !t.cancelRequested && (t.sealed || t.detachRequested) {
			result, err = t.importFile.Detach(ctx)
			if errors.Is(err, syscall.EBUSY) && !t.sealed && !result.Released {
				t.closing = false
				t.detachRequested = false
				return result, err
			}
		} else {
			result, err = t.importFile.Cancel(ctx)
		}
		if result.Check(err) != nil || !transferCloseSettled(result, err) {
			t.cleanupErr = errors.Join(err, syscall.EIO)
			return result, t.cleanupErr
		}
		semantic = err
		t.importFile = nil
	}
	if t.exportFile != nil {
		var err error
		result, err = t.exportFile.CloseWithResult(ctx)
		if result.Check(err) != nil || !transferCloseSettled(result, err) {
			t.cleanupErr = errors.Join(err, syscall.EIO)
			return result, t.cleanupErr
		}
		semantic = errors.Join(semantic, err)
		t.exportFile = nil
	}
	if t.spool != nil {
		err := t.spool.Close()
		t.spool = nil
		if err != nil {
			t.cleanupErr = err
			return storage.ReferenceCloseResult{}, err
		}
	}
	if t.spoolPath != "" {
		if err := os.Remove(t.spoolPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.cleanupErr = err
			return storage.ReferenceCloseResult{}, err
		}
		dir, err := os.Open(r.options.SpoolDir)
		if err == nil {
			err = errors.Join(dir.Sync(), dir.Close())
		}
		if err != nil {
			t.cleanupErr = err
			return storage.ReferenceCloseResult{}, err
		}
		t.spoolPath = ""
	}
	t.release()
	t.release = func() {}
	t.cleanupErr = nil
	t.settled = true
	t.terminalClose, t.terminalErr = result, semantic
	t.expires = time.Now().Add(r.options.TerminalLifetime)
	return result, semantic
}

func (r *transferRegistry) run() {
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	defer close(r.done)
	for {
		select {
		case <-timer.C:
		case <-r.wake:
		}
		r.mu.Lock()
		closed := r.closed
		entries := make([]*servedTransfer, 0, len(r.entries))
		for _, t := range r.entries {
			entries = append(entries, t)
		}
		r.mu.Unlock()
		var failures []error
		for _, t := range entries {
			if !t.mu.TryLock() {
				continue
			}
			if t.settled {
				remove := closed || !time.Now().Before(t.expires)
				t.mu.Unlock()
				if remove {
					r.mu.Lock()
					if r.entries[t.token] == t {
						delete(r.entries, t.token)
						t.releaseHistory()
					}
					r.mu.Unlock()
				}
				continue
			}
			cleanup := !t.initializing && (closed || t.closing || !time.Now().Before(t.expires))
			t.mu.Unlock()
			if cleanup {
				ctx, cancel := context.WithTimeout(t.cleanupContext(context.Background()), 5*time.Second)
				_, err := r.settle(ctx, t)
				cancel()
				if err != nil {
					failures = append(failures, err)
				}
			}
		}
		r.mu.Lock()
		r.err = errors.Join(failures...)
		done := r.closed && len(r.entries) == 0
		r.mu.Unlock()
		if done {
			return
		}
	}
}

func (r *transferRegistry) close(ctx context.Context) error {
	r.stop()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		r.mu.Lock()
		err := r.err
		r.mu.Unlock()
		return errors.Join(ctx.Err(), err)
	}
}

func (r *transferRegistry) stop() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
