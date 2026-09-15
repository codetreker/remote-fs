package smb

import (
	"context"
	"syscall"
	"time"

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
	ranges := make([]storage.WindowsLockRange, len(l.Elements))
	for i, e := range l.Elements {
		kind := storage.Shared
		if e.Flags&wire.LockExclusive != 0 {
			kind = storage.Exclusive
		}
		if e.Flags&wire.LockUnlock != 0 {
			kind = storage.Unlock
		}
		ranges[i] = storage.WindowsLockRange{Offset: e.Offset, Length: e.Length, Type: kind, FailImmediately: e.Flags&wire.LockFailImmediately != 0}
	}
	action, err := t.files.actionID()
	if err != nil {
		return nil, statusError(err)
	}
	result, err := f.LockBatch(ctx, storage.WindowsLockBatch{Ranges: ranges}, action)
	if err != nil {
		if errno := storage.ErrnoOf(err); errno != syscall.EIO && errno != syscall.EINTR {
			return nil, statusError(err)
		}
		check, cancel := context.WithTimeout(context.Background(), c.server.config.Limits.CleanupTimeout)
		result, err = t.session.QueryAction(check, action)
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
	if result.State == storage.WindowsActionPending {
		if pending, ok := ctx.Value(pendingKey{}).(func(*signing.Session) error); ok {
			if pending(key) != nil {
				t.files.fence()
				return nil, statusIO
			}
		}
		// The authority owns cancellation-versus-grant ordering. Local context
		// cancellation cannot establish that a remotely admitted lock disappeared.
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for result.State == storage.WindowsActionPending {
			select {
			case <-ctx.Done():
				check, cancel := context.WithTimeout(context.Background(), c.server.config.Limits.CleanupTimeout)
				result, err = t.session.CancelAction(check, action)
				if !knownAction(result, err, action) {
					result, err = t.session.QueryAction(check, action)
				}
				cancel()
				if !knownAction(result, err, action) || result.State == storage.WindowsActionPending {
					t.files.fence()
					return nil, statusIO
				}
			case <-ticker.C:
				next, queryErr := t.session.QueryAction(ctx, action)
				if !knownAction(next, queryErr, action) {
					if ctx.Err() != nil {
						continue
					}
					t.files.fence()
					return nil, statusIO
				}
				result = next
				err = queryErr
			}
		}
	}
	if result.State == storage.WindowsActionCancelled {
		return nil, statusCancelled
	}
	status := statusAction(result, err)
	if status != 0 {
		return nil, status
	}
	return wire.EmptyResponseBody(), 0
}
