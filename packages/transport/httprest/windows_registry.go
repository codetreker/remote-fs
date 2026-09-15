package httprest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/storage"
)

type windowsRegistry struct {
	mu        sync.Mutex
	backend   storage.WindowsStorage
	limits    FileLimits
	sessions  map[string]*servedWindowsSession
	enrolling int
	active    sync.WaitGroup
	closed    bool
	running   bool
	wake      chan struct{}
	done      chan struct{}
	err       error
}

type servedWindowsSession struct {
	mu                          sync.Mutex
	native                      storage.WindowsSession
	options                     storage.FileSessionOptions
	authority                   string
	revision                    uint64
	expires                     time.Time
	retired                     bool
	lifetime                    context.Context
	cancel                      context.CancelCauseFunc
	cleanup                     context.Context
	files                       map[string]*servedWindowsFile
	references                  map[string]string
	actions                     map[storage.WindowsActionID]*servedWindowsAction
	dataActions, cleanupActions int
}

type servedWindowsFile struct {
	native      storage.WindowsFile
	reference   string
	node        uint64
	closed      bool
	forgetAfter time.Time
}

type servedWindowsAction struct {
	digest  [32]byte
	op      storage.Operation
	file    string
	cleanup bool
	running bool
	known   bool
	done    chan struct{}
	expires time.Time
}

func newWindowsRegistry(s storage.Storage, limits FileLimits) *windowsRegistry {
	backend, _ := s.(storage.WindowsStorage)
	return &windowsRegistry{backend: backend, limits: limits, sessions: make(map[string]*servedWindowsSession), wake: make(chan struct{}, 1), done: make(chan struct{})}
}

func (r *windowsRegistry) startLocked() {
	if !r.running {
		r.running = true
		go r.run()
	}
}

func (r *windowsRegistry) begin() (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, syscall.EIO
	}
	r.active.Add(1)
	return r.active.Done, nil
}

func (r *windowsRegistry) stop() {
	r.mu.Lock()
	r.closed = true
	for _, session := range r.sessions {
		session.retire(errHandlerStopped)
	}
	r.startLocked()
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *windowsRegistry) wait(ctx context.Context) error {
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *windowsRegistry) run() {
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	defer close(r.done)
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
		sessions := make(map[string]*servedWindowsSession, len(r.sessions))
		for id, session := range r.sessions {
			sessions[id] = session
		}
		r.mu.Unlock()
		now := time.Now()
		for id, session := range sessions {
			session.mu.Lock()
			retire := closing || session.retired || !now.Before(session.expires)
			session.pruneLocked(now)
			session.mu.Unlock()
			if !retire {
				continue
			}
			session.retire(syscall.ESTALE)
			ctx, cancel := context.WithTimeout(session.cleanup, session.options.Lease)
			err := session.native.Close(ctx)
			cancel()
			r.mu.Lock()
			if err == nil {
				delete(r.sessions, id)
			} else if closing {
				r.err = errors.Join(r.err, err)
			}
			r.mu.Unlock()
		}
		if closing {
			return
		}
	}
}

func (s *servedWindowsSession) retire(cause error) {
	s.mu.Lock()
	s.retired = true
	s.cancel(cause)
	s.mu.Unlock()
}

func (s *servedWindowsSession) pruneLocked(now time.Time) {
	for id, action := range s.actions {
		if !action.running && !action.expires.IsZero() && !now.Before(action.expires) {
			delete(s.actions, id)
			if action.cleanup {
				s.cleanupActions--
			} else {
				s.dataActions--
			}
		}
	}
	for capability, file := range s.files {
		if file.closed && !now.Before(file.forgetAfter) {
			delete(s.references, file.reference)
			delete(s.files, capability)
		}
	}
}

func (r *windowsRegistry) enroll(ctx context.Context, options storage.FileSessionOptions) (windowsResponse, error) {
	response := emptyWindowsResponse()
	r.mu.Lock()
	if len(r.sessions)+r.enrolling >= r.limits.MaxSessions {
		r.mu.Unlock()
		return response, syscall.EAGAIN
	}
	r.enrolling++
	r.startLocked()
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.enrolling--; r.mu.Unlock() }()
	started := time.Now()
	native, err := r.backend.NewWindowsSession(ctx, options)
	if native == nil {
		if err == nil {
			err = syscall.EIO
		}
		return response, err
	}
	status, statusErr := native.Status(ctx)
	err = errors.Join(err, statusErr)
	if statusErr == nil && (status.Epoch == "" || len(status.Epoch) > MaxLockCapabilityBytes || !utf8.ValidString(status.Epoch) || status.ActionEpoch == 0 || status.Revision == 0 || status.Remaining <= 0 || status.Remaining > options.Lease || status.HistoryRemaining < 0 || status.HistoryRemaining > options.History || status.Retired) {
		err = errors.Join(err, syscall.EIO)
	}
	cleanup := context.WithoutCancel(ctx)
	lifetime, cancel := context.WithCancelCause(cleanup)
	session := &servedWindowsSession{native: native, options: options, authority: status.Epoch, revision: status.Revision, expires: started.Add(status.Remaining), lifetime: lifetime, cancel: cancel, cleanup: cleanup, files: make(map[string]*servedWindowsFile), references: make(map[string]string), actions: make(map[storage.WindowsActionID]*servedWindowsAction)}
	id := fileCapability()
	r.mu.Lock()
	closed := r.closed
	r.sessions[id] = session
	r.mu.Unlock()
	if err != nil || closed || !time.Now().Before(session.expires) {
		session.retire(syscall.ESTALE)
		return response, errors.Join(err, syscall.EIO)
	}
	status.Remaining = maxDuration(time.Until(session.expires))
	status.HistoryRemaining = maxDuration(status.HistoryRemaining - time.Since(started))
	response.Session, response.Status = id, &status
	return response, nil
}

func (s *servedWindowsSession) beginAction(ctx context.Context, req windowsRequest, digest [32]byte, limits FileLimits) (*servedWindowsAction, error) {
	checkedEpoch := false
	for {
		s.mu.Lock()
		now := time.Now()
		if s.retired || !now.Before(s.expires) {
			s.mu.Unlock()
			return nil, syscall.ESTALE
		}
		s.pruneLocked(now)
		if previous := s.actions[req.Action]; previous != nil {
			if previous.digest != digest {
				s.mu.Unlock()
				return nil, syscall.EINVAL
			}
			if previous.running {
				done := previous.done
				s.mu.Unlock()
				select {
				case <-done:
					continue
				case <-ctx.Done():
					return nil, operationFailure(Request{Op: OpWindows}, ctx.Err(), false)
				}
			}
			previous.running, previous.done = true, make(chan struct{})
			s.mu.Unlock()
			return previous, nil
		}
		if !checkedEpoch {
			s.mu.Unlock()
			status, err := s.native.Status(ctx)
			if err != nil {
				return nil, err
			}
			if status.Retired || status.Epoch != s.authority {
				s.retire(syscall.ESTALE)
				return nil, syscall.ESTALE
			}
			epoch, err := req.Action.Epoch()
			if err != nil {
				return nil, err
			}
			if epoch != status.ActionEpoch {
				return nil, syscall.ESTALE
			}
			checkedEpoch = true
			continue
		}
		cleanup := req.Op == storage.OpWindowsClose
		if cleanup && s.cleanupActions >= limits.MaxCleanupActions {
			s.retired = true
			s.cancel(syscall.EIO)
			s.mu.Unlock()
			return nil, syscall.EIO
		}
		if !cleanup && s.dataActions >= limits.MaxActions {
			s.mu.Unlock()
			return nil, syscall.EAGAIN
		}
		action := &servedWindowsAction{digest: digest, op: req.Op, file: req.File, cleanup: cleanup, running: true, done: make(chan struct{})}
		if req.Op == storage.OpWindowsOpen {
			active := 0
			for _, file := range s.files {
				if !file.closed {
					active++
				}
			}
			retainedBeyondActive := len(s.files) > s.options.MaxFiles && len(s.files)-s.options.MaxFiles >= limits.MaxActions+limits.MaxCleanupActions
			if active >= s.options.MaxFiles || retainedBeyondActive {
				s.mu.Unlock()
				return nil, syscall.EMFILE
			}
			action.file = fileCapability()
			s.files[action.file] = &servedWindowsFile{}
		}
		if cleanup {
			s.cleanupActions++
		} else {
			s.dataActions++
		}
		s.actions[req.Action] = action
		s.mu.Unlock()
		return action, nil
	}
}

func (s *servedWindowsSession) finishAction(action *servedWindowsAction) {
	s.mu.Lock()
	action.running = false
	close(action.done)
	s.mu.Unlock()
}

func (s *servedWindowsSession) bind(file storage.WindowsFile, attr storage.WindowsAttr, action *servedWindowsAction) (string, error) {
	if file == nil {
		s.retire(syscall.EIO)
		return "", syscall.EIO
	}
	reference := file.Reference()
	if reference == "" || len(reference) > storage.WindowsMaxReferenceBytes || strings.IndexByte(reference, 0) >= 0 || attr.ID == 0 || attr.NameInfo.Check() != nil {
		s.retire(syscall.EIO)
		return "", syscall.EIO
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if capability := s.references[reference]; capability != "" {
		entry := s.files[capability]
		if entry == nil || entry.node != attr.ID || action != nil && action.op == storage.OpWindowsOpen && action.file != capability {
			s.retired = true
			s.cancel(syscall.EIO)
			return "", syscall.EIO
		}
		return capability, nil
	}
	if action == nil || action.op != storage.OpWindowsOpen || action.file == "" || s.files[action.file] == nil {
		s.retired = true
		s.cancel(syscall.EIO)
		return "", syscall.EIO
	}
	entry := s.files[action.file]
	if entry.native != nil {
		s.retired = true
		s.cancel(syscall.EIO)
		return "", syscall.EIO
	}
	entry.native, entry.reference, entry.node = file, reference, attr.ID
	s.references[reference] = action.file
	return action.file, nil
}

func (s *servedWindowsSession) file(capability string) (storage.WindowsFile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.files[capability]
	if entry == nil || entry.native == nil {
		return nil, syscall.ESTALE
	}
	return entry.native, nil
}

func (s *servedWindowsSession) lookup(lookup storage.WindowsLookup) (storage.WindowsLookup, error) {
	if lookup.ParentReference == "" {
		return lookup, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.files[lookup.ParentReference]
	if parent == nil || parent.native == nil || parent.closed || parent.node != lookup.ParentID {
		return storage.WindowsLookup{}, syscall.ESTALE
	}
	lookup.ParentReference = parent.reference
	return lookup, nil
}

func (s *servedWindowsSession) actionResponse(result storage.WindowsActionResult) (*windowsAction, error) {
	if result.HistoryRemaining < 0 || result.HistoryRemaining > s.options.History {
		s.retire(syscall.EIO)
		return nil, syscall.EIO
	}
	s.mu.Lock()
	action := s.actions[result.Action]
	s.mu.Unlock()
	var capability string
	var err error
	if result.File != nil {
		capability, err = s.bind(result.File, result.Attr, action)
		if err != nil {
			return nil, err
		}
	}
	wire, err := windowsActionOf(result, capability)
	if err != nil {
		s.retire(syscall.EIO)
		return nil, errors.Join(syscall.EIO, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if action != nil {
		action.known = true
	}
	if action != nil && result.State != storage.WindowsActionPending {
		// Retain slightly beyond native receipt expiry; early eviction could give a
		// replayed open another capability for the same still-retained native result.
		action.expires = time.Now().Add(result.HistoryRemaining)
		if action.op == storage.OpWindowsOpen && result.File == nil {
			if file := s.files[action.file]; file != nil && file.native == nil {
				delete(s.files, action.file)
			}
		}
		if action.op == storage.OpWindowsClose && result.State == storage.WindowsActionCompleted && result.Errno == 0 {
			if file := s.files[action.file]; file != nil {
				file.closed = true
				file.forgetAfter = time.Now().Add(2 * s.options.History)
			}
		}
	}
	return wire, nil
}
