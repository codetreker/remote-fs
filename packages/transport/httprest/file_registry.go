package httprest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

const fileCleanupTimeout = 30 * time.Second

type fileRegistry struct {
	mu              sync.Mutex
	limits          FileLimits
	backend         storage.FileStorage
	sessions        map[string]*servedFileSession
	enrolling       int
	active          sync.WaitGroup
	closed, running bool
	wake            chan struct{}
	done            chan struct{}
	err             error
}

type servedFileSession struct {
	mu                       sync.Mutex
	cleanupMu                sync.Mutex
	active                   sync.WaitGroup
	native                   storage.FileSession
	options                  storage.FileSessionOptions
	authority                string
	revision, actionEpoch    uint64
	expires, forgetAfter     time.Time
	retired, cleanupComplete bool
	closeAction              storage.FileActionID
	lifetime                 context.Context
	cancel                   context.CancelCauseFunc
	cleanup                  context.Context
}

func newFileRegistry(s storage.Storage, limits FileLimits) *fileRegistry {
	backend, _ := s.(storage.FileStorage)
	return &fileRegistry{backend: backend, limits: limits, sessions: make(map[string]*servedFileSession), wake: make(chan struct{}, 1)}
}
func fileCapability() string {
	var value [32]byte
	rand.Read(value[:])
	return hex.EncodeToString(value[:])
}
func maxDuration(d time.Duration) time.Duration { return max(d, 0) }

func (r *fileRegistry) begin() (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, syscall.EIO
	}
	r.active.Add(1)
	return r.active.Done, nil
}
func (r *fileRegistry) startLocked() {
	if !r.running {
		r.running = true
		r.done = make(chan struct{})
		go r.run(r.done)
	}
}
func (r *fileRegistry) stop() {
	r.mu.Lock()
	r.closed = true
	for _, s := range r.sessions {
		s.retire(errHandlerStopped)
	}
	r.startLocked()
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *fileRegistry) wait(ctx context.Context) error {
	r.mu.Lock()
	done := r.done
	r.mu.Unlock()
	select {
	case <-done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close retains failed cleanup ownership. A later call retries the same native
// close action; the backend remains owned by the caller.
func (h *Handler) Close(ctx context.Context) error {
	h.Stop()
	h.files.stop()
	return h.files.wait(ctx)
}

func (r *fileRegistry) run(done chan struct{}) {
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
		case <-r.wake:
		}
		r.mu.Lock()
		closing := r.closed
		r.mu.Unlock()
		if closing {
			r.active.Wait()
		}
		r.mu.Lock()
		sessions := make(map[string]*servedFileSession, len(r.sessions))
		for id, s := range r.sessions {
			sessions[id] = s
		}
		r.mu.Unlock()
		var cleanupErr error
		now := time.Now()
		for id, s := range sessions {
			s.mu.Lock()
			retire := closing || s.retired || !now.Before(s.expires)
			s.mu.Unlock()
			if !retire {
				continue
			}
			s.retire(syscall.ESTALE)
			s.active.Wait()
			ctx, cancel := context.WithTimeout(s.cleanup, fileCleanupTimeout)
			err := s.closeOwned(ctx)
			cancel()
			s.mu.Lock()
			forget := closing || !time.Now().Before(s.forgetAfter)
			s.mu.Unlock()
			r.mu.Lock()
			if err == nil && forget {
				delete(r.sessions, id)
			} else if closing {
				cleanupErr = errors.Join(cleanupErr, err)
			}
			r.mu.Unlock()
		}
		if closing {
			r.mu.Lock()
			r.err = cleanupErr
			r.running = false
			close(done)
			r.mu.Unlock()
			return
		}
	}
}

func (s *servedFileSession) retire(cause error) {
	s.mu.Lock()
	if !s.retired {
		s.retired = true
		s.forgetAfter = time.Now().Add(s.options.History)
		s.cancel(cause)
	}
	s.mu.Unlock()
}

func (s *servedFileSession) begin(ctx context.Context, reconcile bool) (context.Context, func(), error) {
	s.mu.Lock()
	if !reconcile && (s.retired || !time.Now().Before(s.expires)) {
		s.mu.Unlock()
		return nil, nil, syscall.ESTALE
	}
	if reconcile {
		s.mu.Unlock()
		return ctx, func() {}, nil
	}
	s.active.Add(1)
	s.mu.Unlock()
	child, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.lifetime, func() { cancel(context.Cause(s.lifetime)) })
	return child, func() { stop(); cancel(nil); s.active.Done() }, nil
}

func (s *servedFileSession) closeOwned(ctx context.Context) error {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	s.mu.Lock()
	if s.cleanupComplete {
		s.mu.Unlock()
		return nil
	}
	if s.closeAction == "" {
		action, err := storage.NewFileActionID(s.actionEpoch)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		s.closeAction = action
	}
	action := s.closeAction
	s.mu.Unlock()
	result, err := s.native.Close(ctx, action)
	if err != nil {
		return err
	}
	receipt, validation := fileReceiptOf(result)
	if validation == nil {
		validation = validateFileReceipt(fileRequest{Op: storage.OpFileSessionClose, Action: action}, *receipt)
	}
	if validation != nil {
		return errors.Join(validation, syscall.EIO)
	}
	if result.Errno != 0 || result.State != storage.FileActionCompleted && result.State != storage.FileActionRetired {
		return syscall.EIO
	}
	s.mu.Lock()
	s.cleanupComplete = true
	s.forgetAfter = laterFileTime(s.forgetAfter, time.Now().Add(result.HistoryRemaining))
	s.mu.Unlock()
	return nil
}

func (r *fileRegistry) enroll(ctx context.Context, options storage.FileSessionOptions) (fileResponse, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return fileResponse{}, syscall.EIO
	}
	if len(r.sessions)+r.enrolling >= r.limits.MaxSessions {
		r.mu.Unlock()
		return fileResponse{}, syscall.EAGAIN
	}
	r.enrolling++
	r.startLocked()
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.enrolling--; r.mu.Unlock() }()
	started := time.Now()
	native, status, err := r.backend.NewFileSession(ctx, options)
	if native == nil {
		if err == nil {
			err = syscall.EIO
		}
		return fileResponse{}, err
	}
	cleanup := context.WithoutCancel(ctx)
	life, cancel := context.WithCancelCause(cleanup)
	s := &servedFileSession{native: native, options: options, cleanup: cleanup, lifetime: life, cancel: cancel, authority: status.Epoch, actionEpoch: status.ActionEpoch, revision: status.Revision, expires: started.Add(status.Remaining)}
	invalid := validateFileStatus(status, options)
	id := fileCapability()
	r.mu.Lock()
	r.sessions[id] = s
	closed := r.closed
	r.mu.Unlock()
	if err != nil || invalid != nil || status.Retired || status.Fenced || closed || ctx.Err() != nil || !time.Now().Before(s.expires) {
		s.retire(syscall.ESTALE)
		return fileResponse{}, errors.Join(err, invalid, ctx.Err(), syscall.EIO)
	}
	status.Remaining = maxDuration(time.Until(s.expires))
	status.HistoryRemaining = maxDuration(status.HistoryRemaining - time.Since(started))
	return fileResponse{Session: id, Status: &status}, nil
}

func (s *servedFileSession) observe(status storage.FileSessionStatus, started time.Time) (storage.FileSessionStatus, error) {
	if err := validateFileStatus(status, s.options); err != nil {
		return storage.FileSessionStatus{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if status.Epoch != s.authority {
		return storage.FileSessionStatus{}, syscall.EIO
	}
	if status.Revision > s.revision {
		s.revision = status.Revision
		s.expires = started.Add(status.Remaining)
	}
	if status.ActionEpoch > s.actionEpoch {
		s.actionEpoch = status.ActionEpoch
	}
	status.Remaining = maxDuration(min(time.Until(s.expires), status.Remaining-time.Since(started)))
	status.HistoryRemaining = maxDuration(status.HistoryRemaining - time.Since(started))
	return status, nil
}

func laterFileTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
