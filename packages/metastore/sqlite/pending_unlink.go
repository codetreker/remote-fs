package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ metastore.ReferenceStateAccess = (*retainedFile)(nil)
var _ metastore.DeleteIntent = (*retainedFile)(nil)

func (f *retainedFile) CheckReferenceState() error { return f.store.CheckFileStore() }
func (f *retainedFile) CheckDeleteIntent() error   { return f.store.CheckFileStore() }

func (s *Store) referenceState(ctx context.Context, tx *sql.Tx, id int64) (metastore.ReferenceState, error) {
	state, err := s.fileState(ctx, tx, id)
	if err != nil {
		return metastore.ReferenceState{}, err
	}
	var pending bool
	var generation int64
	if err := tx.QueryRowContext(ctx, `SELECT pending_unlink,pending_generation FROM nodes WHERE volume=? AND id=?`, s.volume, id).Scan(&pending, &generation); err != nil {
		return metastore.ReferenceState{}, err
	}
	if generation < 0 || pending && generation == 0 {
		return metastore.ReferenceState{}, fmt.Errorf("node %d has an invalid pending-unlink generation: %w", id, syscall.EIO)
	}
	result := metastore.ReferenceState{State: state, PendingUnlink: pending}
	if pending {
		result.PendingGeneration = binary.BigEndian.AppendUint64(nil, uint64(generation))
	}
	return result, nil
}

func (f *retainedFile) State(ctx context.Context) (metastore.ReferenceState, error) {
	if f.metadata&storage.ReadMetadata == 0 {
		return metastore.ReferenceState{}, syscall.EBADF
	}
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return metastore.ReferenceState{}, err
	}
	defer f.store.coordinator.commit.release()
	if err := f.checkPendingUnlinkAccess(ctx); err != nil {
		return metastore.ReferenceState{}, err
	}
	var state metastore.ReferenceState
	err := f.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = f.store.returnedReferenceState(ctx, tx, f.id)
		return err
	})
	return state, sqlerr.Failure(err)
}

func (f *retainedFile) checkPendingUnlinkAccess(ctx context.Context) error {
	if err := f.check(); err != nil {
		return err
	}
	if err := f.store.checkFileOwnership(); err != nil {
		return err
	}
	return metastore.CheckFilePublication(ctx)
}

// An old armed reference has lost its process owner, but its accepted deletion
// remains authoritative until recovery consumes or activates that exact intent.
func (s *Store) nodePendingUnlink(ctx context.Context, tx *sql.Tx, id int64) (bool, error) {
	var blocked bool
	err := tx.QueryRowContext(ctx, `SELECT pending_unlink OR EXISTS (
		SELECT 1 FROM close_intents WHERE volume=? AND node=? AND incarnation<>?
	) FROM nodes WHERE volume=? AND id=?`, s.volume, id, s.fileDomain.incarnation[:], s.volume, id).Scan(&blocked)
	if errors.Is(err, sql.ErrNoRows) {
		return false, syscall.ESTALE
	}
	return blocked, err
}

func (s *Store) recoveredUnlinkContribution(ctx context.Context, tx *sql.Tx, id int64) (int64, error) {
	var intents int64
	var pending bool
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM close_intents WHERE volume=? AND node=? AND incarnation<>?),
		EXISTS(SELECT 1 FROM nodes WHERE volume=? AND id=? AND pending_unlink=1)`,
		s.volume, id, s.fileDomain.incarnation[:], s.volume, id).Scan(&intents, &pending)
	if err != nil {
		return 0, err
	}
	if intents != 0 {
		return intents, nil
	}
	if pending {
		return 1, nil
	}
	return 0, nil
}

func (s *Store) checkUnlinkCondition(ctx context.Context, tx *sql.Tx, state metastore.FileState, condition storage.UnlinkCondition, requireEmpty bool) (bool, error) {
	if state.ID == s.root {
		return false, syscall.EBUSY
	}
	switch condition {
	case storage.UnlinkFile:
		if state.IsDir() {
			return false, syscall.EISDIR
		}
		return true, nil
	case storage.UnlinkIfEmpty:
		if !state.IsDir() {
			return false, syscall.ENOTDIR
		}
		if !requireEmpty {
			return true, nil
		}
		return s.isEmpty(ctx, tx, state.ID)
	default:
		return false, syscall.EINVAL
	}
}

func (f *retainedFile) checkDeleteUse(ctx context.Context, uses []storage.TargetUse) error {
	if f.use.Uses&storage.DeleteName == 0 {
		return syscall.EACCES
	}
	for _, target := range uses {
		if target.NodeID != uint64(f.id) {
			return syscall.EINVAL
		}
		if _, err := f.store.resolveUseScope(ctx, target.Scope, target.NodeID, storage.DeleteName); err != nil {
			return err
		}
	}
	return f.store.fileDomain.coordinator.CheckUse(ctx, uint64(f.id), f.scope, storage.DeleteName)
}

// The atomic open validates intent.Guards before changing names or content;
// the new reference and its admitted use claim already belong to this transaction.
func (s *Store) armCloseIntent(ctx context.Context, tx *sql.Tx, f *retainedFile, intent *storage.CloseIntent) error {
	if intent == nil {
		return nil
	}
	if err := intent.Check(); err != nil {
		return err
	}
	if err := f.checkDeleteUse(ctx, intent.Uses); err != nil {
		return err
	}
	state, err := s.fileState(ctx, tx, f.id)
	if err != nil {
		return err
	}
	if _, err := s.checkUnlinkCondition(ctx, tx, state, intent.Condition, false); err != nil {
		return err
	}
	reference, err := hex.DecodeString(f.scope.Token)
	if err != nil || len(reference) != 16 {
		return storage.ErrInvalidScope
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO close_intents(volume,node,incarnation,reference,if_empty) VALUES(?,?,?,?,?)`,
		s.volume, f.id, s.fileDomain.incarnation[:], reference, intent.Condition == storage.UnlinkIfEmpty)
	return err
}

func (s *Store) activatePendingUnlink(ctx context.Context, tx *sql.Tx, id int64) error {
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET pending_unlink=1,pending_generation=pending_generation+1
		WHERE volume=? AND id=? AND pending_generation<?`, s.volume, id, int64(math.MaxInt64))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("node %d exhausted its pending-unlink generation: %w", id, syscall.EOVERFLOW)
	}
	if err := s.setNodeChangeTime(ctx, tx, id, time.Now()); err != nil {
		return err
	}
	return s.recordNamedChanged(ctx, tx, id)
}

func (f *retainedFile) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (metastore.ReferenceState, error) {
	if err := command.Check(); err != nil {
		return metastore.ReferenceState{}, err
	}
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return metastore.ReferenceState{}, err
	}
	defer f.store.coordinator.commit.release()
	if err := f.checkPendingUnlinkAccess(ctx); err != nil {
		return metastore.ReferenceState{}, err
	}
	var state metastore.ReferenceState
	s := f.store
	err := s.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.SetAttrMutation, node: f.id}, func(tx *sql.Tx) error {
		if err := s.checkNamespaceGuards(ctx, tx, command.Guards); err != nil {
			return err
		}
		if err := f.checkDeleteUse(ctx, command.Uses); err != nil {
			return err
		}
		before, err := s.fileState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		empty, err := s.checkUnlinkCondition(ctx, tx, before, command.Condition, true)
		if err != nil {
			return err
		}
		if !empty {
			return syscall.ENOTEMPTY
		}
		if err := s.activatePendingUnlink(ctx, tx, f.id); err != nil {
			return err
		}
		state, err = s.returnedReferenceState(ctx, tx, f.id)
		return err
	})
	return state, sqlerr.Failure(err)
}

func (f *retainedFile) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (metastore.ReferenceState, error) {
	if err := command.Check(); err != nil {
		return metastore.ReferenceState{}, err
	}
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return metastore.ReferenceState{}, err
	}
	defer f.store.coordinator.commit.release()
	if err := f.checkPendingUnlinkAccess(ctx); err != nil {
		return metastore.ReferenceState{}, err
	}
	var state metastore.ReferenceState
	s := f.store
	err := s.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.SetAttrMutation, node: f.id}, func(tx *sql.Tx) error {
		if err := s.checkNamespaceGuards(ctx, tx, command.Guards); err != nil {
			return err
		}
		if err := f.checkDeleteUse(ctx, command.Uses); err != nil {
			return err
		}
		before, err := s.referenceState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if !before.PendingUnlink || !bytes.Equal(command.Generation, before.PendingGeneration) {
			return storage.ErrConditionConflict
		}
		if _, err := tx.ExecContext(ctx, `UPDATE nodes SET pending_unlink=0 WHERE volume=? AND id=?`, s.volume, f.id); err != nil {
			return err
		}
		if err := s.setNodeChangeTime(ctx, tx, f.id, time.Now()); err != nil {
			return err
		}
		if err := s.recordNamedChanged(ctx, tx, f.id); err != nil {
			return err
		}
		state, err = s.returnedReferenceState(ctx, tx, f.id)
		return err
	})
	return state, sqlerr.Failure(err)
}

// Fixed cleanup carries cancellation and accounting, but never inherits expired
// publication guards, business authorization or a former holder's Strong proofs.
type unlinkCleanupContext struct{ context.Context }

func (unlinkCleanupContext) Value(any) any { return nil }

func pendingUnlinkCleanupContext(ctx context.Context, accounting storage.PublicationAccountingChain) context.Context {
	ctx = storage.WithPublicationAccountingChain(unlinkCleanupContext{ctx}, accounting)
	return locking.WithScope(ctx, locking.MutationScope{})
}

func (s *Store) consumeCloseIntent(ctx context.Context, tx *sql.Tx, id int64, reference []byte, ifEmpty bool) error {
	state, err := s.fileState(ctx, tx, id)
	if err != nil {
		return err
	}
	condition := storage.UnlinkFile
	if ifEmpty {
		condition = storage.UnlinkIfEmpty
	}
	activate, err := s.checkUnlinkCondition(ctx, tx, state, condition, true)
	if err != nil {
		return err
	}
	if activate && !state.Detached {
		if err := s.activatePendingUnlink(ctx, tx, id); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM close_intents WHERE volume=? AND node=? AND reference=?`, s.volume, id, reference)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("node %d lost its retained close intent: %w", id, syscall.EIO)
	}
	return nil
}

// Retirement keeps the physical pin and access claim until this transition is
// known durable. Repeating a failed close uses the same retained intent row.
func (f *retainedFile) consumeCloseIntentLocked(ctx context.Context) error {
	if !f.closeIntent {
		return nil
	}
	reference, err := hex.DecodeString(f.scope.Token)
	if err != nil || len(reference) != 16 {
		return storage.ErrInvalidScope
	}
	s := f.store
	ctx = pendingUnlinkCleanupContext(ctx, storage.PublicationAccountingFrom(ctx))
	err = s.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.SetAttrMutation, node: f.id}, func(tx *sql.Tx) error {
		var ifEmpty bool
		err := tx.QueryRowContext(ctx, `SELECT if_empty FROM close_intents WHERE volume=? AND node=? AND incarnation=? AND reference=?`,
			s.volume, f.id, s.fileDomain.incarnation[:], reference).Scan(&ifEmpty)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("node %d lost its retained close intent: %w", f.id, syscall.EIO)
		}
		if err != nil {
			return err
		}
		return s.consumeCloseIntent(ctx, tx, f.id, reference, ifEmpty)
	})
	if err == nil {
		f.closeIntent = false
	}
	return sqlerr.Failure(err)
}

// The caller owns the last live pin, or has established that every prior
// incarnation is retired. Named deletion always enters the normal Strong gate.
func (s *Store) finalizePendingUnlinkLocked(ctx context.Context, id int64) error {
	pins := s.coordinator.pins[retainedNode{s.volume, id}]
	if pins > 1 {
		return syscall.EBUSY
	}
	var state metastore.ReferenceState
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = s.referenceState(ctx, tx, id)
		return err
	}); err != nil {
		return sqlerr.Failure(err)
	}
	if !state.PendingUnlink {
		return nil
	}
	ctx = pendingUnlinkCleanupContext(ctx, storage.PublicationAccountingFrom(ctx))
	intent := &volumeIntent{kind: locking.RemoveMutation, node: id, cleanup: state.State.Detached}
	err := s.mutateTransactionLocked(ctx, ctx, intent, func(tx *sql.Tx) error {
		if !state.State.Detached {
			if state.State.ID == s.root {
				return syscall.EBUSY
			}
			if state.State.IsDir() {
				empty, err := s.isEmpty(ctx, tx, id)
				if err != nil {
					return err
				}
				if !empty {
					return syscall.ENOTEMPTY
				}
			}
			var parent int64
			var leaf []byte
			if err := tx.QueryRowContext(ctx, `SELECT parent,name FROM entries WHERE volume=? AND node=?`, s.volume, id).Scan(&parent, &leaf); err != nil {
				return err
			}
			if err := s.unlink(ctx, tx, parent, leaf); err != nil {
				return err
			}
			if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: parent, Name: leaf}); err != nil {
				return err
			}
			if err := s.touch(ctx, tx, parent, time.Now()); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM close_intents WHERE volume=? AND node=?`, s.volume, id); err != nil {
			return err
		}
		return s.discardNode(ctx, tx, state.State.Node)
	})
	return sqlerr.Failure(err)
}

// RetryPendingUnlinks performs one bounded maintenance pass. Named state changes
// use normal publication admission, including durable Strong recovery checks.
// Live references retain their own cleanup and accounting; recovered references
// use the currently bound volume accounting chain.
// MaxRetainedFiles caps the batch and FileOperationTimeout bounds the pass.
// An error can follow successful cleanup of other nodes; fencing ends the pass.
func (s *Store) RetryPendingUnlinks(ctx context.Context, limit int) error {
	if limit < 1 {
		return syscall.EINVAL
	}
	ctx, cancel := context.WithTimeout(ctx, s.fileOperationTimeout)
	defer cancel()
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return err
	}
	if s.fileDomain == nil || s.fileDomain.stores <= 0 {
		return syscall.ESTALE
	}
	if s.fileDomain.recoveredReferences == 0 {
		return nil
	}
	limit = min(limit, s.fileDomain.maxFiles)
	var nodes []int64
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT id FROM (
			SELECT id FROM (SELECT id FROM nodes WHERE volume=? AND pending_unlink=1 AND id>? ORDER BY id LIMIT ?)
			UNION
			SELECT node AS id FROM (SELECT DISTINCT node FROM close_intents WHERE volume=? AND node>? ORDER BY node LIMIT ?)
		) ORDER BY id LIMIT ?`, s.volume, s.fileDomain.pendingUnlinkCursor, limit, s.volume, s.fileDomain.pendingUnlinkCursor, limit, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return err
			}
			nodes = append(nodes, id)
		}
		return rows.Err()
	})
	if err != nil {
		return sqlerr.Failure(err)
	}
	ctx = pendingUnlinkCleanupContext(ctx, s.fileDomain.maintenanceAccounting)
	var failures []error
	for _, id := range nodes {
		s.fileDomain.pendingUnlinkCursor = id
		if s.coordinator.pins[retainedNode{s.volume, id}] != 0 {
			continue
		}
		if err := s.retryRecoveredUnlinkLocked(ctx, id); err != nil {
			failures = append(failures, err)
			if s.coordinator.healthy() != nil || ctx.Err() != nil {
				return errors.Join(failures...)
			}
		}
	}
	if len(nodes) < limit {
		s.fileDomain.pendingUnlinkCursor = 0
	}
	return errors.Join(failures...)
}

func (s *Store) retryRecoveredUnlinkLocked(ctx context.Context, id int64) error {
	var reference []byte
	var ifEmpty bool
	var current bool
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM close_intents WHERE volume=? AND node=? AND incarnation=?)`,
			s.volume, id, s.fileDomain.incarnation[:]).Scan(&current); err != nil {
			return err
		}
		if current {
			return nil
		}
		return tx.QueryRowContext(ctx, `SELECT reference,if_empty FROM close_intents
			WHERE volume=? AND node=? AND incarnation<>? ORDER BY reference LIMIT 1`,
			s.volume, id, s.fileDomain.incarnation[:]).Scan(&reference, &ifEmpty)
	})
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return sqlerr.Failure(err)
	}
	if current {
		return nil
	}
	if err == nil {
		if err := s.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.SetAttrMutation, node: id}, func(tx *sql.Tx) error {
			return s.consumeCloseIntent(ctx, tx, id, reference, ifEmpty)
		}); err != nil {
			return sqlerr.Failure(err)
		}
	}
	return s.finalizePendingUnlinkLocked(ctx, id)
}
