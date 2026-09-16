package smb

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type mutationAttemptKey struct{}
type mutationAttempt struct{ submitted bool }

func (f *clientFile) conditional(ctx context.Context, original windowsActionID, attempt func(context.Context, windowsActionID) (windowsActionResult, error)) (windowsActionResult, error) {
	actual := original
	for tries := 0; tries < 8; tries++ {
		if err := f.session.rememberOpen(original, actual, clientAction{}); err != nil {
			return windowsActionResult{}, err
		}
		marker := &mutationAttempt{}
		result, err := attempt(context.WithValue(ctx, mutationAttemptKey{}, marker), actual)
		if !marker.submitted {
			f.session.forgetAction(original, actual)
			if !errors.Is(err, syscall.EIO) && storage.ErrnoOf(err) == syscall.EAGAIN && ctx.Err() == nil && tries < 7 {
				continue
			}
			result.Action = original
			return result, err
		}
		receipt := result.Receipt
		if receipt.Action != actual && receipt.State != storage.FileActionUnknown {
			err = errors.Join(syscall.EIO, err)
		}
		f.session.recordOpen(original, receipt)
		result.Action = original
		state := f.session.action(original)
		if state == nil || !state.retryable || errors.Is(err, syscall.EIO) || tries == 7 || ctx.Err() != nil {
			return result, err
		}
		next, nextErr := f.session.next(ctx)
		if nextErr != nil {
			return result, nextErr
		}
		actual = next
	}
	panic("bounded conditional mutation loop")
}
func (f *clientFile) SetAttr(ctx context.Context, request windowsAttrChange, id windowsActionID) (windowsActionResult, error) {
	return f.conditional(ctx, id, func(ctx context.Context, id windowsActionID) (windowsActionResult, error) {
		return f.setAttrAttempt(ctx, request, id)
	})
}
func (f *clientFile) Rename(ctx context.Context, request windowsRenameRequest, id windowsActionID) (windowsActionResult, error) {
	return f.conditional(ctx, id, func(ctx context.Context, id windowsActionID) (windowsActionResult, error) {
		return f.renameAttempt(ctx, request, id)
	})
}
func (f *clientFile) SetDeletePending(ctx context.Context, value bool, id windowsActionID) (windowsActionResult, error) {
	return f.conditional(ctx, id, func(ctx context.Context, id windowsActionID) (windowsActionResult, error) {
		return f.setDeletePendingAttempt(ctx, value, id)
	})
}
func (f *clientFile) SetLink(ctx context.Context, target string, id windowsActionID) (windowsActionResult, error) {
	return f.conditional(ctx, id, func(ctx context.Context, id windowsActionID) (windowsActionResult, error) {
		return f.setLinkAttempt(ctx, target, id)
	})
}

func (s *clientSession) forgetAction(original, actual storage.FileActionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.actions[original]; a != nil && a.actual == actual {
		delete(s.actions, original)
	}
}
func (s *clientSession) forgetRangeAction(id storage.FileActionID, rangeAction *clientRangeAction) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.actions[id]; a != nil && a.rangeAction == rangeAction {
		delete(s.actions, id)
	}
}

type clientCleanupError struct{ err error }

func (e *clientCleanupError) Error() string { return e.err.Error() }
func (e *clientCleanupError) Unwrap() error { return e.err }
func cleanupFailed(err error) error         { return &clientCleanupError{err: errors.Join(syscall.EIO, err)} }

func rejectedInvocation(id windowsActionID, err error) (windowsActionResult, error) {
	return windowsActionResult{Action: id, State: windowsActionRejected, Errno: storage.ErrnoOf(err), notAdmitted: true}, err
}
