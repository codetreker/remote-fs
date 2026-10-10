package objectstore

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// The session mutex protects these fields and the session's close receipt map.
// Each retained reference owns two receipt positions throughout its lifetime.
type referenceCloseState struct {
	receipts    map[uint64]*referenceCloseReceipt
	next        uint64
	released    bool
	implicit    *storage.CloseAttempt
	finalResult storage.ReferenceCloseResult
	finalErr    error
}

type referenceCloseReceipt struct {
	attempt storage.CloseAttempt
	owner   *referenceCloseState
	run     *referenceCloseRun
	result  storage.ReferenceCloseResult
	err     error
	expires time.Time
	outcome storage.FileActionOutcome
}

type referenceCloseRun struct {
	done   chan struct{}
	result storage.ReferenceCloseResult
	err    error
}

func (s *fileSession) CheckRecoverableReferenceClose() error {
	return s.native.CheckFileStore()
}

var (
	_ storage.RecoverableReferenceClose = (*fileSession)(nil)
	_ storage.ReferenceCloseActions     = (*openFile)(nil)
	_ storage.ReferenceCloseActions     = (*nodeReference)(nil)
)

func (s *fileSession) pruneCloseHistoryLocked(now time.Time) {
	for id, receipt := range s.closeActions {
		if receipt.outcome != storage.FileActionPending && receipt.outcome != storage.FileActionUnknown && !now.Before(receipt.expires) {
			delete(s.closeActions, id)
			delete(receipt.owner.receipts, receipt.attempt.Generation)
		}
	}
	for reference := range s.closeRefs {
		if reference.released && len(reference.receipts) == 0 {
			delete(s.closeRefs, reference)
		}
	}
}

func (r *referenceCloseState) query(ctx context.Context, s *fileSession, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	if err := attempt.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	var result storage.FileActionReceipt
	err := s.locks.WithCloseAdmission(ctx, func(epoch uint64) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if r.next == 0 {
			r.next = 1
		}
		s.pruneCloseHistoryLocked(time.Now())
		receipt := r.receipts[attempt.Generation]
		if receipt != nil && receipt.attempt.Action == attempt.Action {
			result = storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: receipt.outcome}
			return nil
		}
		requested, _ := attempt.Action.Epoch()
		if requested > epoch {
			return syscall.EINVAL
		}
		if requested < epoch {
			result = storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionRetired}
			return nil
		}
		if receipt != nil {
			result = storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}
			return nil
		}
		if attempt.Generation > r.next {
			return syscall.EINVAL
		}
		outcome := storage.FileActionNotExecuted
		if attempt.Generation < r.next || r.released {
			outcome = storage.FileActionRetired
		}
		result = storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: outcome}
		return nil
	})
	return result, err
}

func (r *referenceCloseState) status(ctx context.Context, s *fileSession) (storage.CloseOwnerStatus, error) {
	var result storage.CloseOwnerStatus
	err := s.locks.WithCloseAdmission(ctx, func(epoch uint64) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.pruneCloseHistoryLocked(time.Now())
		if r.next == 0 {
			r.next = 1
		}
		result = storage.CloseOwnerStatus{Released: r.released, NextGeneration: r.next, CurrentEpoch: epoch}
		if current := r.receipts[r.next]; current != nil &&
			(current.outcome == storage.FileActionPending || current.outcome == storage.FileActionUnknown) {
			attempt := current.attempt
			result.Current = &attempt
			result.CurrentOutcome = current.outcome
		}
		result.Ready = !result.Released && result.Current == nil && len(r.receipts) < 2
		return nil
	})
	if err == nil {
		err = result.Check()
	}
	return result, err
}

func (r *referenceCloseState) run(ctx context.Context, s *fileSession, attempt storage.CloseAttempt, closeNative func() (storage.ReferenceCloseResult, error)) (storage.ReferenceCloseResult, error) {
	return r.runAttempt(ctx, s, attempt, closeNative)
}

func (r *referenceCloseState) existingLocked(s *fileSession, attempt storage.CloseAttempt, closeNative func() (storage.ReferenceCloseResult, error)) (*referenceCloseRun, storage.ReferenceCloseResult, error, bool) {
	receipt := r.receipts[attempt.Generation]
	if receipt == nil {
		return nil, storage.ReferenceCloseResult{}, nil, false
	}
	if receipt.attempt.Action != attempt.Action {
		return nil, storage.ReferenceCloseResult{}, syscall.EINVAL, true
	}
	if receipt.outcome == storage.FileActionUnknown {
		receipt.run = &referenceCloseRun{done: make(chan struct{})}
		receipt.outcome = storage.FileActionPending
		r.launchClose(s, receipt, receipt.run, closeNative)
	}
	if receipt.outcome == storage.FileActionPending {
		return receipt.run, storage.ReferenceCloseResult{}, nil, true
	}
	return nil, receipt.result, receipt.err, true
}

func (r *referenceCloseState) admitLocked(s *fileSession, attempt storage.CloseAttempt, closeNative func() (storage.ReferenceCloseResult, error)) (*referenceCloseRun, error) {
	if attempt.Generation != r.next || r.released {
		return nil, syscall.ESTALE
	}
	if len(r.receipts) >= 2 {
		return nil, syscall.EAGAIN
	}
	if s.closeActions == nil {
		s.closeActions = make(map[storage.FileActionID]*referenceCloseReceipt)
	}
	if s.closeActions[attempt.Action] != nil || s.actions[attempt.Action] != nil {
		return nil, syscall.EINVAL
	}
	if r.receipts == nil {
		r.receipts = make(map[uint64]*referenceCloseReceipt)
	}
	receipt := &referenceCloseReceipt{attempt: attempt, owner: r, run: &referenceCloseRun{done: make(chan struct{})}, outcome: storage.FileActionPending}
	r.receipts[attempt.Generation] = receipt
	s.closeActions[attempt.Action] = receipt
	r.launchClose(s, receipt, receipt.run, closeNative)
	return receipt.run, nil
}

func finishReferenceCloseWait(ctx context.Context, run *referenceCloseRun, result storage.ReferenceCloseResult, resultErr error, err error) (storage.ReferenceCloseResult, error) {
	if err != nil {
		return storage.ReferenceCloseResult{}, err
	}
	if run != nil {
		return awaitReferenceClose(ctx, run)
	}
	return result, resultErr
}

func (r *referenceCloseState) runAttempt(ctx context.Context, s *fileSession, attempt storage.CloseAttempt, closeNative func() (storage.ReferenceCloseResult, error)) (storage.ReferenceCloseResult, error) {
	if err := attempt.Check(); err != nil {
		return storage.ReferenceCloseResult{}, err
	}
	s.mu.Lock()
	s.pruneCloseHistoryLocked(time.Now())
	if r.next == 0 {
		r.next = 1
	}
	if run, result, resultErr, handled := r.existingLocked(s, attempt, closeNative); handled {
		s.mu.Unlock()
		return finishReferenceCloseWait(ctx, run, result, resultErr, nil)
	}
	s.mu.Unlock()
	var run *referenceCloseRun
	var result storage.ReferenceCloseResult
	var resultErr error
	var err error
	admit := func(epoch uint64) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.pruneCloseHistoryLocked(time.Now())
		if existingRun, existingResult, existingErr, handled := r.existingLocked(s, attempt, closeNative); handled {
			run, result, resultErr = existingRun, existingResult, existingErr
			return nil
		}
		if attempt.Generation != r.next || r.released {
			return syscall.ESTALE
		}
		requested, _ := attempt.Action.Epoch()
		if requested > epoch {
			return syscall.EINVAL
		}
		if requested < epoch {
			return &storage.CloseActionNotExecutedError{CurrentEpoch: epoch}
		}
		run, err = r.admitLocked(s, attempt, closeNative)
		return err
	}
	err = s.locks.WithCloseAdmission(ctx, admit)
	if err != nil {
		s.mu.Lock()
		if existingRun, existingResult, existingErr, handled := r.existingLocked(s, attempt, closeNative); handled {
			s.mu.Unlock()
			return finishReferenceCloseWait(ctx, existingRun, existingResult, existingErr, nil)
		}
		s.mu.Unlock()
	}
	return finishReferenceCloseWait(ctx, run, result, resultErr, err)
}

func (r *referenceCloseState) launchClose(s *fileSession, receipt *referenceCloseReceipt, run *referenceCloseRun, closeNative func() (storage.ReferenceCloseResult, error)) {
	go func() {
		result, err := closeNative()
		if invalid := result.Check(err); invalid != nil {
			result = storage.ReferenceCloseResult{}
			err = errors.Join(err, invalid)
		}
		s.mu.Lock()
		run.result, run.err = result, err
		receipt.result, receipt.err = result, err
		receipt.expires = time.Now().Add(s.options.History)
		switch {
		case result.Released:
			r.released = true
			r.finalResult, r.finalErr = result, err
			receipt.outcome = storage.FileActionCompleted
		case result.Determined:
			r.next++
			receipt.outcome = storage.FileActionCompleted
		default:
			receipt.outcome = storage.FileActionUnknown
		}
		close(run.done)
		s.mu.Unlock()
	}()
}

func awaitReferenceClose(ctx context.Context, run *referenceCloseRun) (storage.ReferenceCloseResult, error) {
	select {
	case <-run.done:
		return run.result, run.err
	case <-ctx.Done():
		return storage.ReferenceCloseResult{}, ctx.Err()
	}
}

func (r *referenceCloseState) runImplicit(ctx context.Context, s *fileSession, closeNative func() (storage.ReferenceCloseResult, error)) (storage.ReferenceCloseResult, error) {
	for attempts := 0; attempts < 4; attempts++ {
		s.mu.Lock()
		if r.released {
			result, err := r.finalResult, r.finalErr
			s.mu.Unlock()
			return result, err
		}
		if r.next == 0 {
			r.next = 1
		}
		var attempt storage.CloseAttempt
		if current := r.receipts[r.next]; current != nil {
			attempt = current.attempt
		}
		generation := r.next
		s.mu.Unlock()
		if attempt.Action == "" {
			epoch, err := s.locks.CloseEpoch(ctx)
			if err != nil {
				return storage.ReferenceCloseResult{}, err
			}
			id, err := storage.NewFileActionID(epoch)
			if err != nil {
				return storage.ReferenceCloseResult{}, err
			}
			attempt = storage.CloseAttempt{Action: id, Generation: generation}
		}
		result, err := r.runAttempt(ctx, s, attempt, closeNative)
		s.mu.Lock()
		admitted := false
		if current := r.receipts[attempt.Generation]; current != nil && current.attempt.Action == attempt.Action {
			admitted = true
			remembered := attempt
			r.implicit = &remembered
		}
		s.mu.Unlock()
		var missed *storage.CloseActionNotExecutedError
		if !admitted && (errors.As(err, &missed) || errors.Is(err, syscall.EINVAL)) {
			continue
		}
		return result, err
	}
	return storage.ReferenceCloseResult{}, syscall.EAGAIN
}

func (r *referenceCloseState) retryable(s *fileSession) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !r.released && len(r.receipts) != 0
}
