package fuse

import (
	"context"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type fileActionCall func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)

func (v *volume) fileAction(ctx context.Context, op storage.Operation, call fileActionCall) (storage.FileActionReceipt, error) {
	return v.fileActionWithin(ctx, ctx, op, call)
}

func (v *volume) fileActionWithin(ctx, request context.Context, op storage.Operation, call fileActionCall) (storage.FileActionReceipt, error) {
	epoch, err := v.actionEpoch(request)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	id, err := storage.NewFileActionID(epoch)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	return v.issueAction(ctx, id, op, func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		return call(request, id)
	})
}

func (v *volume) issueAction(ctx context.Context, id storage.FileActionID, op storage.Operation, call fileActionCall) (storage.FileActionReceipt, error) {
	receipt, err := call(ctx, id)
	return v.reconcileAction(ctx, id, op, receipt, err)
}

func (v *volume) reconcileAction(ctx context.Context, id storage.FileActionID, op storage.Operation, receipt storage.FileActionReceipt, err error) (storage.FileActionReceipt, error) {
	if terminal, result := actionResult(receipt, id, op, err); terminal {
		return receipt, result
	}
	if receipt.Action == "" && storage.IsFileCallNotAdmitted(err) {
		return receipt, err
	}
	cleanup, cancel := v.cleanupContext(ctx)
	defer cancel()
	observed, queryErr := v.files.QueryAction(cleanup, id)
	if terminal, result := actionResult(observed, id, op, queryErr); terminal {
		return observed, result
	}
	if errnoOf(queryErr) == syscall.ESTALE && errnoOf(v.check()) == syscall.ESTALE {
		return observed, syscall.ESTALE
	}
	observed, queryErr = v.files.CancelAction(cleanup, id)
	if terminal, result := actionResult(observed, id, op, queryErr); terminal {
		return observed, result
	}
	if errnoOf(queryErr) == syscall.ESTALE && errnoOf(v.check()) == syscall.ESTALE {
		return observed, syscall.ESTALE
	}
	cause := errors.Join(err, queryErr, fmt.Errorf("file action outcome is unknown: %w", syscall.EIO))
	v.fence(cause)
	return observed, cause
}

func actionResult(receipt storage.FileActionReceipt, id storage.FileActionID, op storage.Operation, cause error) (bool, error) {
	if receipt.Action != id || receipt.Operation != op || receipt.State != storage.FileActionCompleted && receipt.State != storage.FileActionNotApplied {
		return false, nil
	}
	if receipt.State == storage.FileActionNotApplied && receipt.Effects != 0 {
		return false, nil
	}
	if receipt.Errno != 0 {
		if cause != nil && errnoOf(cause) == receipt.Errno {
			return true, cause
		}
		return true, receipt.Errno
	}
	if receipt.State == storage.FileActionNotApplied {
		return true, syscall.EINTR
	}
	return true, nil
}

func (v *volume) closeReference(ctx context.Context, file storage.File) error {
	v.mu.Lock()
	epoch := v.status.ActionEpoch
	v.mu.Unlock()
	id, err := storage.NewFileActionID(epoch)
	if err != nil {
		return err
	}
	receipt, err := file.Close(ctx, id)
	if receipt.State == storage.FileActionRetired {
		if terminal, result := cleanupResult(receipt, storage.OpFileClose, file.Reference(), err); terminal {
			return result
		}
	}
	_, err = v.reconcileAction(ctx, id, storage.OpFileClose, receipt, err)
	return err
}

func (v *volume) closeSession(ctx context.Context) error {
	v.mu.Lock()
	epoch := v.status.ActionEpoch
	v.mu.Unlock()
	id, err := storage.NewFileActionID(epoch)
	if err != nil {
		return err
	}
	receipt, err := v.files.Close(ctx, id)
	if terminal, result := cleanupResult(receipt, storage.OpFileSessionClose, 0, err); terminal {
		return result
	}
	_, err = v.reconcileAction(ctx, id, storage.OpFileSessionClose, receipt, err)
	return err
}

func cleanupResult(receipt storage.FileActionReceipt, operation storage.Operation, reference storage.FileReferenceID, cause error) (bool, error) {
	if receipt.State != storage.FileActionRetired || receipt.Action != "" || receipt.Effects != 0 || receipt.HistoryRemaining != 0 || receipt.Operation != operation || receipt.Reference != reference {
		return false, nil
	}
	if receipt.Errno != 0 {
		if cause != nil && errnoOf(cause) == receipt.Errno {
			return true, cause
		}
		return true, receipt.Errno
	}
	return true, nil
}
