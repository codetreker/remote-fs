package smb

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (c *connection) lock(ctx context.Context, t *tree, r wire.Request, key *signing.Session) ([]byte, uint32) {
	l, err := r.Lock()
	if err != nil {
		return nil, statusInvalid
	}
	f, ok := t.files.file(l.FileID)
	if !ok {
		return nil, 0xc0000128
	}
	ranges := make([]windowsLockRange, len(l.Elements))
	for i, e := range l.Elements {
		kind := lockInvalid
		switch e.Flags &^ wire.LockFailImmediately {
		case wire.LockShared:
			kind = lockShared
		case wire.LockExclusive:
			kind = lockExclusive
		case wire.LockUnlock:
			kind = lockUnlock
		}
		ranges[i] = windowsLockRange{Offset: e.Offset, Length: e.Length, Type: kind, FailImmediately: e.Flags&wire.LockFailImmediately != 0}
	}
	action, err := t.files.actionID(ctx)
	if err != nil {
		return nil, statusError(err)
	}
	if pending, ok := ctx.Value(pendingKey{}).(func(*signing.Session) error); ok {
		ctx = context.WithValue(ctx, rangePendingKey{}, func() error { return pending(key) })
	}
	result, err := f.LockBatch(ctx, windowsLockBatch{Ranges: ranges}, action)
	if result.notAdmitted && storage.IsFileCallNotAdmitted(err) {
		return nil, statusError(err)
	}
	if err != nil {
		if errno := storage.ErrnoOf(err); errno != syscall.EIO && errno != syscall.EINTR {
			return nil, statusError(err)
		}
		check, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.server.config.Limits.CleanupTimeout)
		if ctx.Err() != nil {
			result, err = t.session.CancelAction(check, action)
			if !knownAction(result, err, action) {
				result, err = t.session.QueryAction(check, action)
			}
		} else {
			result, err = t.session.QueryAction(check, action)
		}
		cancel()
		if !knownAction(result, err, action) {
			t.files.fence()
			return nil, statusIO
		}
	}
	if !knownAction(result, err, action) {
		t.files.fence()
		return nil, statusIO
	}

	if result.State == windowsActionCancelled {
		return nil, statusCancelled
	}
	status := statusAction(result, err)
	if status != 0 {
		return nil, status
	}
	return wire.EmptyResponseBody(), 0
}
