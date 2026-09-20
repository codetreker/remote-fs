package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
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
	var explicit bool
	var generation int64
	var accepted bool
	if err := tx.QueryRowContext(ctx, `SELECT pending_unlink,pending_generation,EXISTS(
		SELECT 1 FROM delete_intents WHERE volume=? AND node=? AND outcome IN (?,?))
		FROM nodes WHERE volume=? AND id=?`, s.volume, id, storage.DeleteIntentPending,
		storage.DeleteIntentCleanupFailed, s.volume, id).Scan(&explicit, &generation, &accepted); err != nil {
		return metastore.ReferenceState{}, err
	}
	pending := explicit || accepted
	if generation < 0 || pending && generation == 0 {
		return metastore.ReferenceState{}, fmt.Errorf("node %d has an invalid pending-unlink generation: %w", id, syscall.EIO)
	}
	result := metastore.ReferenceState{State: state, LinkTarget: bytes.Clone(state.LinkTarget), PendingUnlink: pending}
	if pending {
		result.PendingGeneration = binary.BigEndian.AppendUint64(nil, uint64(generation))
	}
	return result, nil
}

func (s *Store) returnedReferenceState(ctx context.Context, tx *sql.Tx, id int64) (metastore.ReferenceState, error) {
	if err := s.checkNodeResultBudget(ctx, tx, id); err != nil {
		return metastore.ReferenceState{}, err
	}
	state, err := s.referenceState(ctx, tx, id)
	if err != nil {
		return metastore.ReferenceState{}, err
	}
	if int64(len(state.LinkTarget)) > storage.MaxLinkTargetBytes {
		return metastore.ReferenceState{}, syscall.EFBIG
	}
	return state, nil
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

func (s *Store) nodePendingUnlink(ctx context.Context, tx *sql.Tx, id int64) (bool, error) {
	var blocked bool
	err := tx.QueryRowContext(ctx, `SELECT pending_unlink OR EXISTS(
		SELECT 1 FROM delete_intents WHERE volume=? AND node=? AND outcome IN (?,?))
		FROM nodes WHERE volume=? AND id=?`, s.volume, id, storage.DeleteIntentPending,
		storage.DeleteIntentCleanupFailed, s.volume, id).Scan(&blocked)
	if errors.Is(err, sql.ErrNoRows) {
		return false, syscall.ESTALE
	}
	return blocked, err
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

func deleteIntentHash(node, parent int64, name []byte, intent storage.CloseIntent) ([32]byte, error) {
	metadata := make(map[string][]byte, len(intent.ExpectedMetadata))
	for namespace, version := range intent.ExpectedMetadata {
		metadata[namespace] = append([]byte{}, version...)
	}
	uses := append([]storage.TargetUse(nil), intent.Uses...)
	sort.Slice(uses, func(i, j int) bool {
		if uses[i].NodeID != uses[j].NodeID {
			return uses[i].NodeID < uses[j].NodeID
		}
		return uses[i].Scope.Token < uses[j].Scope.Token
	})
	payload := struct {
		Node, Parent int64
		Name         []byte
		Condition    storage.UnlinkCondition
		Metadata     map[string][]byte
		Uses         []storage.TargetUse
	}{node, parent, name, intent.Condition, metadata, uses}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func (s *Store) advancePendingGeneration(ctx context.Context, tx *sql.Tx, id int64) error {
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET pending_generation=pending_generation+1
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
	return nil
}

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
	if state.Detached {
		return syscall.ESTALE
	}
	if err := checkExpectedMetadata(state.Metadata, intent.ExpectedMetadata); err != nil {
		return err
	}
	if _, err := s.checkUnlinkCondition(ctx, tx, state, intent.Condition, false); err != nil {
		return err
	}
	var parent int64
	var name []byte
	if err := tx.QueryRowContext(ctx, `SELECT parent,name FROM entries WHERE volume=? AND node=?`, s.volume, f.id).Scan(&parent, &name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return syscall.ESTALE
		}
		return err
	}
	reference, err := hex.DecodeString(f.scope.Token)
	if err != nil || len(reference) != 16 {
		return storage.ErrInvalidScope
	}
	hash, err := deleteIntentHash(f.id, parent, name, *intent)
	if err != nil {
		return err
	}
	var existing []byte
	err = tx.QueryRowContext(ctx, `SELECT request_hash FROM delete_intents WHERE intent=?`, string(intent.ID)).Scan(&existing)
	if err == nil {
		if !bytes.Equal(existing, hash[:]) {
			return syscall.EINVAL
		}
		return syscall.EEXIST
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM delete_intents WHERE volume=?`, s.volume).Scan(&count); err != nil {
		return err
	}
	if count >= int64(s.maxDeleteIntents) {
		return syscall.EAGAIN
	}
	now := time.Now()
	_, err = tx.ExecContext(ctx, `INSERT INTO delete_intents
		(intent,volume,node,parent,name,reference,request_hash,if_empty,outcome,failure,updated_sec,updated_nsec)
		VALUES(?,?,?,?,?,?,?,?,?,NULL,?,?)`, string(intent.ID), s.volume, f.id, parent, name, reference,
		hash[:], intent.Condition == storage.UnlinkIfEmpty, storage.DeleteIntentArmed, now.Unix(), now.Nanosecond())
	if err == nil {
		f.closeIntent = intent.ID
	}
	return err
}

func (s *Store) activateExplicitPendingUnlink(ctx context.Context, tx *sql.Tx, id int64) error {
	blocked, err := s.nodePendingUnlink(ctx, tx, id)
	if err != nil {
		return err
	}
	if !blocked {
		if err := s.advancePendingGeneration(ctx, tx, id); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET pending_unlink=1 WHERE volume=? AND id=?`, s.volume, id)
	if err != nil {
		return err
	}
	if err := sqlvalue.ExactlyOne(result, "setting pending unlink"); err != nil {
		return err
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
		if err := f.checkDeleteUse(ctx, command.Uses); err != nil {
			return err
		}
		before, err := s.fileState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if err := checkExpectedMetadata(before.Metadata, command.ExpectedMetadata); err != nil {
			return err
		}
		if before.Detached {
			return syscall.ESTALE
		}
		var names int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM entries WHERE volume=? AND node=?`, s.volume, f.id).Scan(&names); err != nil {
			return err
		}
		if names != 1 {
			return syscall.ESTALE
		}
		empty, err := s.checkUnlinkCondition(ctx, tx, before, command.Condition, true)
		if err != nil {
			return err
		}
		if !empty {
			return syscall.ENOTEMPTY
		}
		if err := s.activateExplicitPendingUnlink(ctx, tx, f.id); err != nil {
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
		if err := f.checkDeleteUse(ctx, command.Uses); err != nil {
			return err
		}
		before, err := s.referenceState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		var explicit bool
		if err := tx.QueryRowContext(ctx, `SELECT pending_unlink FROM nodes WHERE volume=? AND id=?`, s.volume, f.id).Scan(&explicit); err != nil {
			return err
		}
		if !explicit || !bytes.Equal(command.Generation, before.PendingGeneration) {
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

func (s *Store) QueryDeleteIntent(ctx context.Context, id storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	if err := id.Check(); err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	status := storage.DeleteIntentStatus{ID: id}
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		var node int64
		if err := tx.QueryRowContext(ctx, `SELECT node,outcome FROM delete_intents WHERE volume=? AND intent=?`, s.volume, string(id)).Scan(&node, &status.Outcome); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				status.Outcome = storage.DeleteIntentUnknown
				return nil
			}
			return err
		}
		if node < 1 {
			return syscall.EIO
		}
		status.NodeID = uint64(node)
		if status.Outcome == storage.DeleteIntentCleanupFailed {
			var failure sql.NullInt64
			if err := tx.QueryRowContext(ctx, `SELECT failure FROM delete_intents WHERE volume=? AND intent=?`, s.volume, string(id)).Scan(&failure); err != nil {
				return err
			}
			if !failure.Valid || failure.Int64 <= 0 {
				return syscall.EIO
			}
			status.Failure = syscall.Errno(failure.Int64)
		}
		return nil
	})
	if err == nil {
		err = status.Check()
	}
	return status, sqlerr.Failure(err)
}

func (s *Store) AcknowledgeDeleteIntent(ctx context.Context, command storage.AcknowledgeDeleteIntentCommand) error {
	if err := command.Check(); err != nil {
		return err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return err
	}
	err := s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error {
		var outcome storage.DeleteIntentOutcome
		err := tx.QueryRowContext(ctx, `SELECT outcome FROM delete_intents WHERE volume=? AND intent=?`, s.volume, string(command.Intent)).Scan(&outcome)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if outcome != storage.DeleteIntentCompleted && outcome != storage.DeleteIntentNotExecuted {
			return syscall.EBUSY
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM delete_intents WHERE volume=? AND intent=?`, s.volume, string(command.Intent))
		if err != nil {
			return err
		}
		return sqlvalue.ExactlyOne(result, "acknowledging deletion intent")
	})
	return sqlerr.Failure(err)
}

func (s *Store) setDeleteIntentOutcome(ctx context.Context, tx *sql.Tx, id storage.DeleteIntentID, outcome storage.DeleteIntentOutcome, failure *syscall.Errno) error {
	now := time.Now()
	var stored any
	if failure != nil {
		stored = int64(*failure)
	}
	result, err := tx.ExecContext(ctx, `UPDATE delete_intents SET outcome=?,failure=?,updated_sec=?,updated_nsec=?
		WHERE volume=? AND intent=?`, outcome, stored, now.Unix(), now.Nanosecond(), s.volume, string(id))
	if err != nil {
		return err
	}
	return sqlvalue.ExactlyOne(result, "updating deletion intent status")
}

type unlinkCleanupContext struct{ context.Context }

func (unlinkCleanupContext) Value(any) any { return nil }

func pendingUnlinkCleanupContext(ctx context.Context, accounting storage.PublicationAccountingChain) context.Context {
	return locking.WithScope(storage.WithPublicationAccountingChain(unlinkCleanupContext{ctx}, accounting), locking.MutationScope{})
}

// Retirement turns the durable armed obligation into pending before its
// DeleteName claim is released. A known failed condition becomes NotExecuted.
func (f *retainedFile) consumeCloseIntentLocked(ctx context.Context) error {
	if f.closeIntent == "" {
		return nil
	}
	s := f.store
	intentID := f.closeIntent
	ctx = pendingUnlinkCleanupContext(ctx, storage.PublicationAccountingFrom(ctx))
	var resultErr error
	err := s.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.SetAttrMutation, node: f.id}, func(tx *sql.Tx) error {
		var node int64
		var ifEmpty bool
		var outcome storage.DeleteIntentOutcome
		if err := tx.QueryRowContext(ctx, `SELECT node,if_empty,outcome FROM delete_intents WHERE volume=? AND intent=?`,
			s.volume, string(intentID)).Scan(&node, &ifEmpty, &outcome); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("node %d lost deletion intent %q: %w", f.id, intentID, syscall.EIO)
			}
			return err
		}
		if node != f.id {
			return syscall.EIO
		}
		if outcome != storage.DeleteIntentArmed {
			if outcome == storage.DeleteIntentNotExecuted {
				resultErr = syscall.ENOTEMPTY
				return nil
			}
			if outcome == storage.DeleteIntentCleanupFailed {
				return syscall.EIO
			}
			return nil
		}
		state, err := s.fileState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		condition := storage.UnlinkFile
		if ifEmpty {
			condition = storage.UnlinkIfEmpty
		}
		if state.Detached {
			if err := s.setDeleteIntentOutcome(ctx, tx, intentID, storage.DeleteIntentNotExecuted, nil); err != nil {
				return err
			}
			return nil
		}
		executable, err := s.checkUnlinkCondition(ctx, tx, state, condition, true)
		if err != nil {
			return err
		}
		if !executable {
			if err := s.setDeleteIntentOutcome(ctx, tx, intentID, storage.DeleteIntentNotExecuted, nil); err != nil {
				return err
			}
			resultErr = syscall.ENOTEMPTY
			return nil
		}
		pending, err := s.nodePendingUnlink(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if !pending {
			if err := s.advancePendingGeneration(ctx, tx, f.id); err != nil {
				return err
			}
		}
		return s.setDeleteIntentOutcome(ctx, tx, intentID, storage.DeleteIntentPending, nil)
	})
	if err == nil {
		f.closeIntent = ""
	}
	return errors.Join(sqlerr.Failure(err), resultErr)
}

func errnoPointer(err error) *syscall.Errno {
	errno := storage.ErrnoOf(err)
	return &errno
}

func (s *Store) markNodeDeleteIntents(ctx context.Context, tx *sql.Tx, node int64, outcome storage.DeleteIntentOutcome, failure *syscall.Errno) error {
	now := time.Now()
	var stored any
	if failure != nil {
		stored = int64(*failure)
	}
	_, err := tx.ExecContext(ctx, `UPDATE delete_intents SET outcome=?,failure=?,updated_sec=?,updated_nsec=?
		WHERE volume=? AND node=? AND outcome IN (?,?)`, outcome, stored, now.Unix(), now.Nanosecond(),
		s.volume, node, storage.DeleteIntentPending, storage.DeleteIntentCleanupFailed)
	return err
}

func (s *Store) finalizePendingUnlinkLocked(ctx context.Context, id int64) error {
	if s.coordinator.pins[retainedNode{s.volume, id}] > 1 {
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
		if err := s.markNodeDeleteIntents(ctx, tx, id, storage.DeleteIntentCompleted, nil); err != nil {
			return err
		}
		return s.discardNode(ctx, tx, state.State.Node)
	})
	if err == nil {
		return nil
	}
	classified := sqlerr.Failure(err)
	markErr := s.mutateTransactionLocked(context.WithoutCancel(ctx), context.WithoutCancel(ctx), nil, func(tx *sql.Tx) error {
		return s.markNodeDeleteIntents(context.WithoutCancel(ctx), tx, id, storage.DeleteIntentCleanupFailed, errnoPointer(classified))
	})
	return errors.Join(classified, sqlerr.Failure(markErr))
}

// RetryPendingUnlinks performs one bounded recovery pass. A previous process's
// armed close intent is interpreted as its reference having closed.
func (s *Store) RetryPendingUnlinks(ctx context.Context, limit int) error {
	if limit < 1 {
		return syscall.EINVAL
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return err
	}
	ctx = pendingUnlinkCleanupContext(ctx, s.fileDomain.maintenanceAccounting)
	limit = min(limit, s.maxDeleteIntents)
	var nodes []int64
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT id FROM (
			SELECT id FROM nodes WHERE volume=? AND pending_unlink=1 AND id>?
			UNION SELECT DISTINCT node AS id FROM delete_intents
			WHERE volume=? AND outcome IN (?,?,?) AND node>?
		) ORDER BY id LIMIT ?`, s.volume, s.fileDomain.pendingUnlinkCursor, s.volume,
			storage.DeleteIntentArmed, storage.DeleteIntentPending, storage.DeleteIntentCleanupFailed,
			s.fileDomain.pendingUnlinkCursor, limit)
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
	var failures []error
	for _, id := range nodes {
		s.fileDomain.pendingUnlinkCursor = id
		if s.coordinator.pins[retainedNode{s.volume, id}] != 0 {
			continue
		}
		if err := s.activateRecoveredIntentsLocked(ctx, id); err != nil {
			failures = append(failures, err)
			continue
		}
		if err := s.finalizePendingUnlinkLocked(ctx, id); err != nil {
			failures = append(failures, err)
		}
	}
	if len(nodes) < limit {
		s.fileDomain.pendingUnlinkCursor = 0
	}
	return errors.Join(failures...)
}

func (s *Store) activateRecoveredIntentsLocked(ctx context.Context, id int64) error {
	return s.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.SetAttrMutation, node: id}, func(tx *sql.Tx) error {
		state, err := s.fileState(ctx, tx, id)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT intent,if_empty,outcome FROM delete_intents
			WHERE volume=? AND node=? AND outcome IN (?,?,?) ORDER BY intent`, s.volume, id,
			storage.DeleteIntentArmed, storage.DeleteIntentPending, storage.DeleteIntentCleanupFailed)
		if err != nil {
			return err
		}
		defer rows.Close()
		type pending struct {
			id      storage.DeleteIntentID
			ifEmpty bool
			outcome storage.DeleteIntentOutcome
		}
		var intents []pending
		for rows.Next() {
			var item pending
			if err := rows.Scan(&item.id, &item.ifEmpty, &item.outcome); err != nil {
				return err
			}
			intents = append(intents, item)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, item := range intents {
			if item.outcome == storage.DeleteIntentPending || item.outcome == storage.DeleteIntentCleanupFailed {
				continue
			}
			if state.Detached {
				if err := s.setDeleteIntentOutcome(ctx, tx, item.id, storage.DeleteIntentNotExecuted, nil); err != nil {
					return err
				}
				continue
			}
			condition := storage.UnlinkFile
			if item.ifEmpty {
				condition = storage.UnlinkIfEmpty
			}
			executable, err := s.checkUnlinkCondition(ctx, tx, state, condition, true)
			if err != nil {
				return err
			}
			outcome := storage.DeleteIntentPending
			if !executable {
				outcome = storage.DeleteIntentNotExecuted
			} else if pending, err := s.nodePendingUnlink(ctx, tx, id); err != nil {
				return err
			} else if !pending {
				if err := s.advancePendingGeneration(ctx, tx, id); err != nil {
					return err
				}
			}
			if err := s.setDeleteIntentOutcome(ctx, tx, item.id, outcome, nil); err != nil {
				return err
			}
		}
		return nil
	})
}
