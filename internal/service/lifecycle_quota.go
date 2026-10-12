package service

import (
	"context"
	"errors"
	"strconv"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (l *Lifecycle) quota(ctx context.Context, op management.Operation, volume management.Volume) (result management.Operation, resultErr error) {
	result = op
	mutation, err := lifecycleMutation(op)
	if err != nil {
		return op, err
	}
	if op.Phase == management.PhaseAccepted {
		op, err = l.transition(ctx, op, management.PhaseApplying, management.OperationTransition{})
		if err != nil {
			return op, err
		}
	}
	if op.Phase != management.PhaseApplying {
		return op, management.ErrCorrupt
	}
	lease, err := l.config.Hooks.Acquire(ctx, volume)
	if err != nil {
		return op, err
	}
	if lease.Store == nil || lease.Release == nil {
		return op, syscall.EIO
	}
	released := false
	defer func() {
		if !released {
			resultErr = errors.Join(resultErr, lease.Release())
		}
	}()
	command := storage.QuotaCommand{Action: storage.ActionKey{SubjectID: string(op.ActorUserID), Namespace: uint64(op.Action.Namespace), Nonce: op.Action.Nonce}, ExpectedRevision: uint64(op.ExpectedRevision), QuotaBytes: mutation.QuotaBytes}
	command.Fingerprint = command.EffectFingerprint()
	namespace, err := l.config.Management.GetNamespace(ctx, op.ActorUserID, op.Action.Namespace)
	if err != nil {
		return op, err
	}
	if err := lease.Store.CheckQuotaAcceptance(); err != nil {
		return op, err
	}
	receipt, err := lease.Store.AcceptQuotaAction(ctx, storage.AcceptedQuotaAction{
		Command: command, Namespace: storage.ActionNamespace{SubjectID: string(op.ActorUserID), Sequence: uint64(namespace.Sequence), AcceptUntil: namespace.AcceptUntil}, AcceptedAt: op.CreatedAt,
	})
	if err := checkLifecycleQuotaResult(receipt, command, err); err != nil {
		return op, err
	}
	if receipt.Outcome == storage.DurableActionPending {
		receipt, err = lease.Store.SetQuota(ctx, command)
		if err := checkLifecycleQuotaResult(receipt, command, err); err != nil {
			return op, err
		}
	}
	if receipt.Outcome == storage.DurableActionCompleted || receipt.Outcome == storage.DurableActionNotExecuted {
		released = true
		if err := lease.Release(); err != nil {
			return op, err
		}
	}
	switch receipt.Outcome {
	case storage.DurableActionCompleted:
		return l.complete(ctx, op, struct {
			QuotaBytes    string `json:"quotaBytes"`
			QuotaRevision string `json:"quotaRevision"`
		}{strconv.FormatInt(receipt.QuotaBytes, 10), strconv.FormatUint(receipt.QuotaRevision, 10)})
	case storage.DurableActionNotExecuted:
		return l.failDefinitive(ctx, op, "native-quota-"+strconv.FormatUint(uint64(receipt.Failure), 10))
	default:
		return op, syscall.EIO
	}
}

func checkLifecycleQuotaReceipt(receipt storage.QuotaReceipt, command storage.QuotaCommand) error {
	if err := receipt.Check(); err != nil {
		return err
	}
	if receipt.Action != command.Action || receipt.Fingerprint != command.Fingerprint {
		return syscall.EIO
	}
	if receipt.Outcome == storage.DurableActionCompleted && (receipt.QuotaBytes != command.QuotaBytes || receipt.QuotaRevision != command.ExpectedRevision+1) {
		return syscall.EIO
	}
	return nil
}

func checkLifecycleQuotaResult(receipt storage.QuotaReceipt, command storage.QuotaCommand, nativeErr error) error {
	if nativeErr != nil {
		if receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure == 0 || !errors.Is(nativeErr, receipt.Failure) {
			return nativeErr
		}
		if err := checkLifecycleQuotaReceipt(receipt, command); err != nil {
			return errors.Join(nativeErr, err)
		}
		return nil
	}
	return checkLifecycleQuotaReceipt(receipt, command)
}
