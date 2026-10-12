package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ metastore.NativeFileTransfer = (*Store)(nil)

func (s *Store) CheckNativeFileTransfer() error { return s.CheckFileStore() }

func (s *Store) loadImport(ctx context.Context, tx *sql.Tx, key storage.ActionKey) (metastore.ImportReservation, error) {
	result := metastore.ImportReservation{Receipt: storage.ImportReceipt{Action: key, Outcome: storage.DurableActionUnknown}}
	record, found, err := s.readAction(ctx, tx, key, importAction)
	if err != nil {
		return result, err
	}
	if !found {
		return result, s.unknownAction(ctx, tx, key, &result.Receipt.Outcome)
	}
	if err := json.Unmarshal(record.result, &result.Receipt); err != nil {
		return result, syscall.EIO
	}
	if result.Receipt.Outcome != storage.DurableActionPending {
		return result, nil
	}
	var encoded, binding []byte
	var commandBytes, bindingBytes int64
	err = tx.QueryRowContext(ctx, `SELECT object_key,
	 CASE WHEN typeof(command)='blob' AND length(command)<=262144 THEN command END,sealed,
	 CASE WHEN typeof(access_binding)='blob' AND length(access_binding)<=16384 THEN access_binding END,
	 CASE WHEN typeof(command)='blob' THEN length(command) ELSE -1 END,
	 CASE WHEN typeof(access_binding)='blob' THEN length(access_binding) ELSE -1 END
	 FROM imports WHERE volume=? AND subject=? AND namespace=? AND nonce=?`, s.volume, key.SubjectID, key.Namespace, key.Nonce).Scan(&result.Key, &encoded, &result.Sealed, &binding, &commandBytes, &bindingBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return result, syscall.EIO
	}
	if err != nil {
		return result, err
	}
	if commandBytes < 0 || commandBytes > 262144 || int64(len(encoded)) != commandBytes || bindingBytes < 0 || bindingBytes > 16384 || int64(len(binding)) != bindingBytes {
		return result, syscall.EIO
	}
	if err := json.Unmarshal(binding, &result.Binding); err != nil {
		return result, syscall.EIO
	}
	if result.Binding != (storage.AccessBinding{}) && result.Binding.Check() != nil {
		return result, syscall.EIO
	}
	if err := json.Unmarshal(encoded, &result.Command); err != nil {
		return result, syscall.EIO
	}
	if result.Command.Check() != nil || result.Command.Action != key || result.Command.Fingerprint != record.fingerprint {
		return result, syscall.EIO
	}
	return result, nil
}

func (s *Store) ResumeImportReservation(ctx context.Context, key storage.ActionKey) (metastore.ImportReservation, error) {
	if err := checkActionSubject(ctx, key); err != nil {
		return metastore.ImportReservation{}, err
	}
	if err := key.Check(); err != nil {
		return metastore.ImportReservation{}, err
	}
	var result metastore.ImportReservation
	err := s.inspect(ctx, func(tx *sql.Tx) error { var err error; result, err = s.loadImport(ctx, tx, key); return err })
	return result, err
}
func (s *Store) PrepareImport(ctx context.Context, c storage.ImportCommand) (metastore.ImportReservation, error) {
	if err := checkActionSubject(ctx, c.Action); err != nil {
		return metastore.ImportReservation{}, err
	}
	if err := c.Check(); err != nil {
		return metastore.ImportReservation{}, err
	}
	if err := s.CheckNativeFileTransfer(); err != nil {
		return metastore.ImportReservation{}, err
	}
	key, err := sqlvalue.NewKey()
	if err != nil {
		return metastore.ImportReservation{}, err
	}
	var result metastore.ImportReservation
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return result, err
	}
	defer s.coordinator.commit.release()
	known := false
	err = s.inspect(ctx, func(tx *sql.Tx) error {
		record, found, err := s.readAction(ctx, tx, c.Action, importAction)
		if err != nil {
			return err
		}
		known = found
		if !found {
			return nil
		}
		if record.fingerprint != c.Fingerprint {
			return syscall.EINVAL
		}
		result, err = s.loadImport(ctx, tx, c.Action)
		return err
	})
	if err != nil {
		return result, err
	}
	if known {
		return result, nil
	}
	err = s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error {
		record, found, err := s.readAction(ctx, tx, c.Action, importAction)
		if err != nil {
			return err
		}
		if found {
			if record.fingerprint != c.Fingerprint {
				return syscall.EINVAL
			}
			result, err = s.loadImport(ctx, tx, c.Action)
			return err
		}
		if err := s.checkActionAdmission(ctx, tx, c.Action); err != nil {
			return err
		}
		at := time.Now()
		sec, nsec := sqlvalue.StoredTime(at)
		if err := s.reserveObject(ctx, tx, key, c.DeclaredBytes, sec, nsec); err != nil {
			return err
		}
		binding, _ := storage.AccessBindingFrom(ctx)
		result = metastore.ImportReservation{Binding: binding, Key: key, Command: c, Receipt: storage.ImportReceipt{Action: c.Action, Fingerprint: c.Fingerprint, Outcome: storage.DurableActionPending}}
		if err := s.saveAction(ctx, tx, c.Action, importAction, c.Fingerprint, result.Receipt.Outcome, 0, result.Receipt); err != nil {
			return err
		}
		encoded, err := json.Marshal(c)
		if err != nil {
			return err
		}
		if len(encoded) > 262144 {
			return syscall.EFBIG
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO imports(volume,subject,namespace,nonce,object_key,command,access_binding) VALUES(?,?,?,?,?,?,?)`, s.volume, c.Action.SubjectID, c.Action.Namespace, c.Action.Nonce, string(key), encoded, mustJSON(binding))
		return err
	})
	if err != nil {
		return metastore.ImportReservation{}, err
	}
	return result, nil
}
func (s *Store) SealImport(ctx context.Context, key storage.ActionKey, object metastore.Object) error {
	if err := key.Check(); err != nil {
		return err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	var existing metastore.ImportReservation
	if err := s.inspect(ctx, func(tx *sql.Tx) error { var err error; existing, err = s.loadImport(ctx, tx, key); return err }); err != nil {
		return err
	}
	if err := checkActionSubject(ctx, key); err != nil {
		return err
	}
	if existing.Receipt.Outcome.Terminal() {
		return syscall.EALREADY
	}
	if existing.Sealed {
		if object.Key != existing.Key || object.Size != existing.Command.DeclaredBytes || !bytes.Equal(object.Digest, existing.Command.Digest[:]) {
			return syscall.EINVAL
		}
		return nil
	}
	return s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error {
		reservation, err := s.loadImport(ctx, tx, key)
		if err != nil {
			return err
		}
		if err := checkActionSubject(ctx, key); err != nil {
			return err
		}
		if !reservation.Sealed {
			if err := storage.RequireAccessBinding(ctx, reservation.Binding); err != nil {
				return err
			}
		}
		if reservation.Receipt.Outcome != storage.DurableActionPending {
			return syscall.EALREADY
		}
		if object.Key != reservation.Key || object.Size != reservation.Command.DeclaredBytes || !bytes.Equal(object.Digest, reservation.Command.Digest[:]) {
			return syscall.EINVAL
		}
		if reservation.Sealed {
			return nil
		}
		result, err := tx.ExecContext(ctx, `UPDATE objects SET digest=? WHERE volume=? AND key=? AND state=? AND size=?`, object.Digest, s.volume, string(object.Key), stateReserved, object.Size)
		if err != nil {
			return err
		}
		if err := sqlvalue.ExactlyOne(result, "sealing unpublished import"); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE imports SET sealed=1 WHERE volume=? AND subject=? AND namespace=? AND nonce=?`, s.volume, key.SubjectID, key.Namespace, key.Nonce)
		return err
	})
}
func (s *Store) QueryImportAction(ctx context.Context, key storage.ActionKey) (storage.ImportReceipt, error) {
	reservation, err := s.ResumeImportReservation(ctx, key)
	if err != nil {
		return reservation.Receipt, err
	}
	if reservation.Receipt.Failure != 0 {
		return reservation.Receipt, reservation.Receipt.Failure
	}
	return reservation.Receipt, nil
}

func (s *Store) importTarget(ctx context.Context, tx *sql.Tx, c storage.ImportCommand) (metastore.Node, bool, error) {
	parent := c.Target.Name.Parent
	if _, _, err := s.directoryTarget(ctx, tx, parent, 0); err != nil {
		return metastore.Node{}, false, err
	}
	if err := s.checkNamespaceGuards(ctx, tx, c.Target.Guards); err != nil {
		return metastore.Node{}, false, err
	}
	if err := s.checkNameAddition(ctx, tx, int64(parent.NodeID)); err != nil {
		return metastore.Node{}, false, err
	}
	old, found, err := s.lookup(ctx, tx, int64(parent.NodeID), c.Target.Name.RawLeaf)
	if err != nil {
		return old, found, err
	}
	if err := checkChildCondition(c.Expected, old, found); err != nil {
		return old, found, err
	}
	if found {
		if old.Kind != storage.NodeRegular && old.Kind != storage.NodeSymlink {
			return old, found, nodeKindMismatch(old.Kind, storage.NodeRegular)
		}
		if pending, err := s.nodePendingUnlink(ctx, tx, old.ID); err != nil {
			return old, found, err
		} else if pending {
			return old, found, storage.ErrPendingDelete
		}
		if err := s.fileDomain.coordinator.CheckUse(ctx, uint64(old.ID), storage.UseScope{}, storage.DeleteName); err != nil {
			return old, found, err
		}
	}
	allocation, err := allocatedSize(storage.NodeRegular, c.DeclaredBytes)
	if err != nil {
		return old, found, err
	}
	delta := allocation
	if found && s.coordinator.pins[retainedNode{s.volume, old.ID}] == 0 {
		delta -= old.AllocationSize
	}
	return old, found, s.roomFor(ctx, tx, delta)
}

func (s *Store) PublishImport(ctx context.Context, key storage.ActionKey) (storage.ImportReceipt, error) {
	if err := checkActionSubject(ctx, key); err != nil {
		return storage.ImportReceipt{}, err
	}
	if err := key.Check(); err != nil {
		return storage.ImportReceipt{}, err
	}
	result := storage.ImportReceipt{Action: key, Outcome: storage.DurableActionUnknown}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return result, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return result, err
	}
	if err := metastore.CheckFilePublication(ctx); err != nil {
		return result, err
	}
	var reservation metastore.ImportReservation
	if err := s.inspect(ctx, func(tx *sql.Tx) error { var err error; reservation, err = s.loadImport(ctx, tx, key); return err }); err != nil {
		return result, err
	}
	if reservation.Receipt.Outcome.Terminal() {
		if reservation.Receipt.Failure != 0 {
			return reservation.Receipt, reservation.Receipt.Failure
		}
		return reservation.Receipt, nil
	}
	var old metastore.Node
	var found bool
	var refused error
	plan := func(tx *sql.Tx) (*volumeIntent, bool, error) {
		var err error
		reservation, err = s.loadImport(ctx, tx, key)
		if err != nil {
			return nil, false, err
		}
		result = reservation.Receipt
		if result.Outcome != storage.DurableActionPending {
			return nil, false, nil
		}
		if !reservation.Sealed {
			return nil, false, syscall.EBUSY
		}
		old, found, err = s.importTarget(ctx, tx, reservation.Command)
		if err != nil {
			if actionErrno(err) == 0 {
				return nil, false, err
			}
			refused = err
			return nil, true, nil
		}
		intent := &volumeIntent{kind: locking.CreateMutation, nodes: []int64{}, addedBytes: reservation.Command.DeclaredBytes}
		if found {
			intent.kind = locking.RemoveMutation
			intent.nodes = []int64{old.ID}
		}
		return intent, true, nil
	}
	apply := func(tx *sql.Tx) error {
		if result.Outcome != storage.DurableActionPending {
			return nil
		}
		if refused != nil {
			return s.refuseImport(ctx, tx, reservation, actionErrno(refused), &result)
		}
		c := reservation.Command
		at := time.Now()
		if found {
			if err := s.unlink(ctx, tx, int64(c.Target.Name.Parent.NodeID), c.Target.Name.RawLeaf); err != nil {
				return err
			}
			if err := s.discard(ctx, tx, old, at); err != nil {
				return err
			}
			if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: int64(c.Target.Name.Parent.NodeID), Name: c.Target.Name.RawLeaf}); err != nil {
				return err
			}
		}
		if err := s.accountFileChange(ctx, tx, 0, c.DeclaredBytes); err != nil {
			return err
		}
		allocation, err := allocatedSize(storage.NodeRegular, c.DeclaredBytes)
		if err != nil {
			return err
		}
		node, err := s.insertNode(ctx, tx, storage.NodeRegular, storage.InitialFields{Attr: storage.AttrChange{ModTime: &at}}, at)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE nodes SET size=?,allocation_size=?,content=? WHERE volume=? AND id=?`, c.DeclaredBytes, allocation, string(reservation.Key), s.volume, node.ID); err != nil {
			return err
		}
		if err := s.link(ctx, tx, int64(c.Target.Name.Parent.NodeID), c.Target.Name.RawLeaf, node.ID); err != nil {
			return err
		}
		if err := s.recordCreated(ctx, tx, metastore.Location{Parent: int64(c.Target.Name.Parent.NodeID), Name: c.Target.Name.RawLeaf}, node.ID); err != nil {
			return err
		}
		if err := s.touch(ctx, tx, int64(c.Target.Name.Parent.NodeID), at); err != nil {
			return err
		}
		updated, err := tx.ExecContext(ctx, `UPDATE objects SET state=? WHERE volume=? AND key=? AND state=? AND size=? AND digest=?`, stateReferenced, s.volume, string(reservation.Key), stateReserved, c.DeclaredBytes, c.Digest[:])
		if err != nil {
			return err
		}
		if err := sqlvalue.ExactlyOne(updated, "publishing sealed import"); err != nil {
			return err
		}
		node, err = s.nodeByID(ctx, tx, node.ID)
		if err != nil {
			return err
		}
		result = storage.ImportReceipt{Action: key, Fingerprint: c.Fingerprint, Outcome: storage.DurableActionCompleted, Attr: node.Attr()}
		if err := s.saveAction(ctx, tx, key, importAction, c.Fingerprint, result.Outcome, 0, result); err != nil {
			return err
		}
		return s.deleteImport(ctx, tx, key)
	}
	err := s.mutatePlannedTransactionLocked(ctx, ctx, plan, apply)
	if err != nil {
		// A healthy coordinator proves rollback completed. Only a determined refusal
		// gets a second receipt transaction; uncertain finalization retains pending.
		if reservation.Key != "" && s.coordinator.healthy() == nil && actionErrno(err) != 0 {
			refusalErr := s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error { return s.refuseImport(ctx, tx, reservation, actionErrno(err), &result) })
			if refusalErr == nil {
				return result, result.Failure
			}
			err = errors.Join(err, refusalErr)
		}
		return storage.ImportReceipt{Action: key, Fingerprint: result.Fingerprint, Outcome: storage.DurableActionUnknown}, sqlerr.Failure(err)
	}
	if result.Failure != 0 {
		return result, result.Failure
	}
	return result, nil
}

func actionErrno(err error) syscall.Errno {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	var lock *locking.Error
	if errors.As(err, &lock) {
		return locking.Errno(lock.Code)
	}
	if errors.Is(err, storage.ErrConditionConflict) {
		return syscall.ESTALE
	}
	if errors.Is(err, storage.ErrPendingDelete) {
		return syscall.EBUSY
	}
	return 0
}
func (s *Store) deleteImport(ctx context.Context, tx *sql.Tx, key storage.ActionKey) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM imports WHERE volume=? AND subject=? AND namespace=? AND nonce=?`, s.volume, key.SubjectID, key.Namespace, key.Nonce)
	return err
}
func (s *Store) refuseImport(ctx context.Context, tx *sql.Tx, r metastore.ImportReservation, failure syscall.Errno, result *storage.ImportReceipt) error {
	*result = storage.ImportReceipt{Action: r.Command.Action, Fingerprint: r.Command.Fingerprint, Outcome: storage.DurableActionNotExecuted, Failure: failure}
	state := stateUnresolved
	if r.Sealed {
		state = stateGarbage
	}
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET state=? WHERE volume=? AND key=? AND state=?`, state, s.volume, string(r.Key), stateReserved); err != nil {
		return err
	}
	if err := s.saveAction(ctx, tx, r.Command.Action, importAction, r.Command.Fingerprint, result.Outcome, failure, *result); err != nil {
		return err
	}
	return s.deleteImport(ctx, tx, r.Command.Action)
}
func (s *Store) AbandonImport(ctx context.Context, key storage.ActionKey) error {
	if err := checkActionSubject(ctx, key); err != nil {
		return err
	}
	if err := key.Check(); err != nil {
		return err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	var existing metastore.ImportReservation
	if err := s.inspect(ctx, func(tx *sql.Tx) error { var err error; existing, err = s.loadImport(ctx, tx, key); return err }); err != nil {
		return err
	}
	if existing.Receipt.Outcome.Terminal() {
		return nil
	}
	return s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error {
		reservation, err := s.loadImport(ctx, tx, key)
		if err != nil {
			return err
		}
		if reservation.Receipt.Outcome != storage.DurableActionPending {
			return nil
		}
		var receipt storage.ImportReceipt
		return s.refuseImport(ctx, tx, reservation, syscall.ECANCELED, &receipt)
	})
}

func checkActionSubject(ctx context.Context, key storage.ActionKey) error {
	if binding, present := storage.AccessBindingFrom(ctx); present && binding.SubjectID != key.SubjectID {
		return syscall.EACCES
	}
	return nil
}
func mustJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
