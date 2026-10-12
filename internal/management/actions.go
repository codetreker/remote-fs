package management

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

type ActionKey struct {
	Namespace int64
	Nonce     string
}

type ActionNamespace struct {
	Actor       ID
	Sequence    int64
	AcceptUntil time.Time
}

func validateActionKey(key ActionKey) error {
	if key.Namespace < 1 || len(key.Nonce) < 1 || len(key.Nonce) > 128 || !utf8.ValidString(key.Nonce) {
		return fmt.Errorf("invalid action key: %w", syscall.EINVAL)
	}
	return nil
}

func (s *Store) logicalNowTx(ctx context.Context, tx *sql.Tx, actor ID) (time.Time, error) {
	if _, err := requireUserTx(ctx, tx, actor); err != nil {
		return time.Time{}, err
	}
	var high int64
	if err := tx.QueryRowContext(ctx, "SELECT logical_time_high_water FROM users WHERE id=?", actor).Scan(&high); err != nil {
		return time.Time{}, err
	}
	now := s.now().UTC().UnixMilli()
	if now < high {
		now = high
	}
	if now < 0 {
		return time.Time{}, fmt.Errorf("negative logical time: %w", ErrCorrupt)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE users SET logical_time_high_water=? WHERE id=?", now, actor); err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(now).UTC(), nil
}

// IssueNamespace durably allocates the next sequence for this actor. Issuance
// and its immutable admission deadline use the actor's monotonic logical clock.
func (s *Store) IssueNamespace(ctx context.Context, actor ID) (ActionNamespace, error) {
	var issued ActionNamespace
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		now, err := s.logicalNowTx(ctx, tx, actor)
		if err != nil {
			return err
		}
		var sequence, floor int64
		if err := tx.QueryRowContext(ctx, "SELECT next_action_namespace,retired_before_namespace FROM users WHERE id=?", actor).Scan(&sequence, &floor); err != nil {
			return err
		}
		if sequence < 1 || sequence <= floor {
			return ErrCorrupt
		}
		if sequence == math.MaxInt64 {
			return ErrCapacity
		}
		var all, owned int
		if err := tx.QueryRowContext(ctx, "SELECT count(*),count(CASE WHEN actor_user_id=? THEN 1 END) FROM action_namespaces", actor).Scan(&all, &owned); err != nil {
			return err
		}
		if all >= s.options.MaxNamespaces || owned >= s.options.MaxNamespacesPerUser {
			return ErrCapacity
		}
		until := time.UnixMilli(now.Add(s.options.NamespaceLifetime).UnixMilli()).UTC()
		if until.UnixMilli() <= now.UnixMilli() {
			return ErrCapacity
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO action_namespaces(actor_user_id,sequence,accept_until) VALUES(?,?,?)", actor, sequence, until.UnixMilli()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE users SET next_action_namespace=? WHERE id=?", sequence+1, actor); err != nil {
			return err
		}
		issued = ActionNamespace{Actor: actor, Sequence: sequence, AcceptUntil: until}
		return nil
	})
	return issued, err
}

// GetNamespace returns the original admission window even after it expires.
// Registering that same window at a volume authority cannot renew acceptance.
func (s *Store) GetNamespace(ctx context.Context, actor ID, sequence int64) (ActionNamespace, error) {
	if sequence < 1 {
		return ActionNamespace{}, fmt.Errorf("invalid action namespace sequence: %w", syscall.EINVAL)
	}
	var floor int64
	var until sql.NullInt64
	var retiredAt sql.NullInt64
	err := s.read.QueryRowContext(ctx, `SELECT u.retired_before_namespace,n.accept_until,n.retired_at
		FROM users u LEFT JOIN action_namespaces n ON n.actor_user_id=u.id AND n.sequence=? WHERE u.id=?`, sequence, actor).Scan(&floor, &until, &retiredAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ActionNamespace{}, ErrNotFound
	}
	if err != nil {
		return ActionNamespace{}, err
	}
	if sequence <= floor || retiredAt.Valid {
		return ActionNamespace{}, ErrHistoryExpired
	}
	if !until.Valid {
		return ActionNamespace{}, ErrNotFound
	}
	if until.Int64 < 0 || !validID(actor) {
		return ActionNamespace{}, ErrCorrupt
	}
	return ActionNamespace{Actor: actor, Sequence: sequence, AcceptUntil: time.UnixMilli(until.Int64).UTC()}, nil
}

// ActorRetirement supplies the durable native floor for a newly created volume.
// Management's inclusive floor becomes the native authority's exclusive floor+1.
func (s *Store) ActorRetirement(ctx context.Context, actor ID) (retiredBefore, nativeRevision int64, proof []byte, err error) {
	err = s.read.QueryRowContext(ctx, "SELECT retired_before_namespace,retired_native_revision,retired_native_proof FROM users WHERE id=?", actor).Scan(&retiredBefore, &nativeRevision, &proof)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil, ErrNotFound
	}
	if err != nil {
		return 0, 0, nil, err
	}
	if !validID(actor) || retiredBefore < 0 || nativeRevision < 0 || (retiredBefore > 0 && (nativeRevision == 0 || len(proof) <= 32)) || (retiredBefore == 0 && (nativeRevision != 0 || len(proof) != 0)) {
		return 0, 0, nil, ErrCorrupt
	}
	return retiredBefore, nativeRevision, proof, nil
}

// ListNamespaceActors gives the lifecycle controller a bounded sweep of actors
// whose namespaces may need native retirement. It is not a public user directory.
func (s *Store) ListNamespaceActors(ctx context.Context, after ID, limit int) ([]ID, error) {
	if limit < 1 || limit > s.options.MaxPageSize {
		return nil, fmt.Errorf("invalid namespace actor page size: %w", syscall.EINVAL)
	}
	rows, err := s.read.QueryContext(ctx, "SELECT DISTINCT actor_user_id FROM action_namespaces WHERE actor_user_id>? ORDER BY actor_user_id LIMIT ?", after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	actors := make([]ID, 0, limit)
	for rows.Next() {
		var actor ID
		if err := rows.Scan(&actor); err != nil {
			return nil, err
		}
		if !validID(actor) {
			return nil, ErrCorrupt
		}
		actors = append(actors, actor)
	}
	return actors, rows.Err()
}

// Known action keys are looked up before this check so a retained receipt remains
// queryable after the namespace stops accepting new actions.
func (s *Store) checkNamespaceTx(ctx context.Context, tx *sql.Tx, actor ID, sequence int64) (time.Time, error) {
	now, err := s.logicalNowTx(ctx, tx, actor)
	if err != nil {
		return time.Time{}, err
	}
	var retired int64
	if err := tx.QueryRowContext(ctx, "SELECT retired_before_namespace FROM users WHERE id=?", actor).Scan(&retired); err != nil {
		return time.Time{}, err
	}
	if sequence <= retired {
		return time.Time{}, ErrHistoryExpired
	}
	var until int64
	var retiredAt sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT accept_until,retired_at FROM action_namespaces WHERE actor_user_id=? AND sequence=?", actor, sequence).Scan(&until, &retiredAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, ErrNotFound
		}
		return time.Time{}, err
	}
	if retiredAt.Valid {
		return time.Time{}, ErrHistoryExpired
	}
	if now.UnixMilli() >= until {
		return time.Time{}, ErrExpired
	}
	return now, nil
}

func (s *Store) knownNamespaceParticipation(ctx context.Context, actor ID, sequence int64, volumeID ID) (known bool, returned error) {
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			returned = errors.Join(returned, err)
		}
	}()
	if _, err := requireOwnedVolumeTx(ctx, tx, actor, volumeID); err != nil {
		return false, err
	}
	var floor int64
	if err := tx.QueryRowContext(ctx, "SELECT retired_before_namespace FROM users WHERE id=?", actor).Scan(&floor); err != nil {
		return false, err
	}
	if sequence <= floor {
		return false, ErrHistoryExpired
	}
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM namespace_volume_participants WHERE actor_user_id=? AND sequence=? AND volume_id=?)", actor, sequence, volumeID).Scan(&known); err != nil {
		return false, err
	}
	if !known {
		return false, nil
	}
	var until int64
	var retiredAt sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT accept_until,retired_at FROM action_namespaces WHERE actor_user_id=? AND sequence=?", actor, sequence).Scan(&until, &retiredAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrCorrupt
		}
		return false, err
	}
	if until < 0 {
		return false, ErrCorrupt
	}
	if retiredAt.Valid {
		return false, ErrHistoryExpired
	}
	return true, nil
}

// RegisterNamespaceParticipation durably records the authority domain before
// native registration. A proven recorded pair replays without writing or renewing
// its admission window. The native retired floor fences concurrent retirement.
func (s *Store) RegisterNamespaceParticipation(ctx context.Context, actor ID, sequence int64, volumeID ID) error {
	if sequence < 1 || !validID(volumeID) {
		return fmt.Errorf("invalid namespace participation: %w", syscall.EINVAL)
	}
	known, err := s.knownNamespaceParticipation(ctx, actor, sequence, volumeID)
	if err != nil || known {
		return err
	}
	var refused error
	err = s.writeTx(ctx, func(tx *sql.Tx) error {
		if _, err := requireOwnedVolumeTx(ctx, tx, actor, volumeID); err != nil {
			return err
		}
		var floor int64
		if err := tx.QueryRowContext(ctx, "SELECT retired_before_namespace FROM users WHERE id=?", actor).Scan(&floor); err != nil {
			return err
		}
		if sequence <= floor {
			return ErrHistoryExpired
		}
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM namespace_volume_participants WHERE actor_user_id=? AND sequence=? AND volume_id=?)", actor, sequence, volumeID).Scan(&known); err != nil {
			return err
		}
		if known {
			var present bool
			if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM action_namespaces WHERE actor_user_id=? AND sequence=? AND retired_at IS NULL)", actor, sequence).Scan(&present); err != nil {
				return err
			}
			if !present {
				return ErrCorrupt
			}
			return nil
		}
		now, err := s.checkNamespaceTx(ctx, tx, actor, sequence)
		if err != nil {
			if errors.Is(err, ErrExpired) || errors.Is(err, ErrHistoryExpired) || errors.Is(err, ErrNotFound) {
				refused = err
				return nil
			}
			return err
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM namespace_volume_participants").Scan(&count); err != nil {
			return err
		}
		if count >= s.options.MaxNamespaceParticipants {
			return ErrCapacity
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO namespace_volume_participants(actor_user_id,sequence,volume_id,created_at) VALUES(?,?,?,?)", actor, sequence, volumeID, now.UnixMilli())
		return err
	})
	if err != nil {
		return err
	}
	return refused
}

// CheckNamespaceAdmission gates native actions proven unknown by their authority.
// Known action queries and settlement replay retain their original identities.
func (s *Store) CheckNamespaceAdmission(ctx context.Context, actor ID, sequence int64) error {
	if sequence < 1 || sequence == math.MaxInt64 {
		return fmt.Errorf("invalid namespace sequence: %w", syscall.EINVAL)
	}
	var refused error
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		_, err := s.checkNamespaceTx(ctx, tx, actor, sequence)
		if errors.Is(err, ErrExpired) || errors.Is(err, ErrHistoryExpired) || errors.Is(err, ErrNotFound) {
			refused = err
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	return refused
}

func terminalOperation(state OperationState) bool {
	return state == OperationSucceeded || state == OperationFailed
}

func operationPhaseAllowed(kind OperationKind, phase OperationPhase) bool {
	switch kind {
	case OperationCreate:
		return phase == PhaseAccepted || phase == PhaseInitializing || phase == PhaseBound || phase == PhaseCompleted
	case OperationRename:
		return phase == PhaseCompleted
	case OperationQuota:
		return phase == PhaseAccepted || phase == PhaseApplying || phase == PhaseCompleted
	case OperationDelete:
		return phase == PhaseAccepted || phase == PhaseFrozen || phase == PhaseVerifiedClose || phase == PhaseQuarantined || phase == PhaseReclaiming || phase == PhaseCompleted
	}
	return false
}

func nextOperationPhase(kind OperationKind, from, to OperationPhase) bool {
	if from == to {
		return true
	}
	var phases []OperationPhase
	switch kind {
	case OperationCreate:
		phases = []OperationPhase{PhaseAccepted, PhaseInitializing, PhaseBound, PhaseCompleted}
	case OperationQuota:
		phases = []OperationPhase{PhaseAccepted, PhaseApplying, PhaseCompleted}
	case OperationDelete:
		phases = []OperationPhase{PhaseAccepted, PhaseFrozen, PhaseVerifiedClose, PhaseQuarantined, PhaseReclaiming, PhaseCompleted}
	}
	for i := 0; i+1 < len(phases); i++ {
		if phases[i] == from && phases[i+1] == to {
			return true
		}
	}
	return false
}

func validOperationError(code string) bool {
	return code != "" && len(code) <= 128 && !strings.ContainsAny(code, "\x00\r\n")
}

const operationColumns = "id,actor_user_id,volume_id,kind,action_namespace,action_nonce,fingerprint,expected_revision,payload,state,phase,revision,verified_root_proof,planned_reclaim_proof,quarantine_leaf,reclaiming_proof,result,error_code,settled,settlement_proof,created_at,updated_at"

func equalFingerprint(a, b []byte) bool {
	return len(a) == 32 && len(b) == 32 && subtle.ConstantTimeCompare(a, b) == 1
}

func scanOperation(row rowScanner) (Operation, error) {
	var operation Operation
	var created, updated int64
	var leaf, errorCode sql.NullString
	if err := row.Scan(&operation.ID, &operation.ActorUserID, &operation.VolumeID, &operation.Kind, &operation.Action.Namespace, &operation.Action.Nonce, &operation.Fingerprint, &operation.ExpectedRevision, &operation.Payload, &operation.State, &operation.Phase, &operation.Revision, &operation.VerifiedRootProof, &operation.PlannedReclaimProof, &leaf, &operation.ReclaimingProof, &operation.Result, &errorCode, &operation.Settled, &operation.SettlementProof, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Operation{}, ErrNotFound
		}
		return Operation{}, err
	}
	if !validID(operation.ID) || !validID(operation.ActorUserID) || !validID(operation.VolumeID) || !operationPhaseAllowed(operation.Kind, operation.Phase) || operation.Revision < 1 || len(operation.Fingerprint) != 32 || validateActionKey(operation.Action) != nil || !(operation.State == OperationPending || operation.State == OperationRunning || terminalOperation(operation.State)) {
		return Operation{}, ErrCorrupt
	}
	operation.QuarantineLeaf, operation.ErrorCode = leaf.String, errorCode.String
	if operation.State == OperationSucceeded && (operation.Phase != PhaseCompleted || !operation.Settled || len(operation.Result) == 0) ||
		operation.State == OperationFailed && !validOperationError(operation.ErrorCode) ||
		operation.State != OperationFailed && operation.ErrorCode != "" ||
		operation.State == OperationPending && operation.Phase != PhaseAccepted ||
		operation.Phase == PhaseCompleted && operation.State != OperationSucceeded ||
		operation.Settled && !terminalOperation(operation.State) {
		return Operation{}, ErrCorrupt
	}
	if operation.Kind == OperationCreate && (operation.Phase == PhaseBound || operation.Phase == PhaseCompleted) && len(operation.VerifiedRootProof) == 0 {
		return Operation{}, ErrCorrupt
	}
	if operation.Kind == OperationDelete {
		if operation.Phase == PhaseVerifiedClose || operation.Phase == PhaseQuarantined || operation.Phase == PhaseReclaiming || operation.Phase == PhaseCompleted {
			if len(operation.VerifiedRootProof) == 0 || len(operation.PlannedReclaimProof) == 0 || operation.QuarantineLeaf != string(operation.VolumeID)+"-"+string(operation.ID) {
				return Operation{}, ErrCorrupt
			}
		}
		if (operation.Phase == PhaseReclaiming || operation.Phase == PhaseCompleted) && len(operation.ReclaimingProof) == 0 {
			return Operation{}, ErrCorrupt
		}
	}
	operation.CreatedAt, operation.UpdatedAt = time.UnixMilli(created).UTC(), time.UnixMilli(updated).UTC()
	return operation, nil
}

func operationByKeyTx(ctx context.Context, tx *sql.Tx, actor ID, key ActionKey) (Operation, error) {
	return scanOperation(tx.QueryRowContext(ctx, "SELECT "+operationColumns+" FROM operations WHERE actor_user_id=? AND action_namespace=? AND action_nonce=?", actor, key.Namespace, key.Nonce))
}

func insertOperationTx(ctx context.Context, tx *sql.Tx, operation Operation) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO operations(`+operationColumns+`) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, operation.ID, operation.ActorUserID, operation.VolumeID, operation.Kind, operation.Action.Namespace, operation.Action.Nonce, operation.Fingerprint, operation.ExpectedRevision, operation.Payload, operation.State, operation.Phase, operation.Revision, operation.VerifiedRootProof, operation.PlannedReclaimProof, operation.QuarantineLeaf, operation.ReclaimingProof, operation.Result, operation.ErrorCode, operation.Settled, operation.SettlementProof, operation.CreatedAt.UnixMilli(), operation.UpdatedAt.UnixMilli())
	return err
}

func (s *Store) operationCapacityTx(ctx context.Context, tx *sql.Tx, terminal bool) error {
	var pending int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM operations WHERE state IN ('pending','running') OR settled=0").Scan(&pending); err != nil {
		return err
	}
	if !terminal && pending >= s.options.MaxOperations {
		return ErrCapacity
	}
	return s.receiptCapacityTx(ctx, tx)
}

func (s *Store) Operation(ctx context.Context, actor, id ID) (Operation, error) {
	operation, err := scanOperation(s.read.QueryRowContext(ctx, "SELECT "+operationColumns+" FROM operations WHERE id=?", id))
	if err != nil {
		return Operation{}, err
	}
	if operation.ActorUserID != actor {
		return Operation{}, ErrDenied
	}
	return operation, nil
}

func (s *Store) FindOperation(ctx context.Context, actor ID, key ActionKey) (Operation, error) {
	if err := validateActionKey(key); err != nil {
		return Operation{}, err
	}
	var operation Operation
	var absent error
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		if _, err := requireUserTx(ctx, tx, actor); err != nil {
			return err
		}
		var err error
		operation, err = operationByKeyTx(ctx, tx, actor, key)
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		_, absent = s.checkNamespaceTx(ctx, tx, actor, key.Namespace)
		if absent == nil {
			absent = ErrNotFound
		}
		return nil
	})
	if err == nil {
		err = absent
	}
	return operation, err
}

// PendingOperations is for the service controller; public queries use Operation
// and FindOperation to enforce the original actor identity.
func (s *Store) PendingOperations(ctx context.Context, after ID, limit int) ([]Operation, error) {
	if limit < 1 || limit > s.options.MaxPageSize {
		return nil, fmt.Errorf("invalid operation page size: %w", syscall.EINVAL)
	}
	rows, err := s.read.QueryContext(ctx, "SELECT "+operationColumns+" FROM operations WHERE (state IN ('pending','running') OR settled=0) AND id>? ORDER BY id LIMIT ?", after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	operations := make([]Operation, 0, limit)
	for rows.Next() {
		operation, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		operations = append(operations, operation)
	}
	return operations, rows.Err()
}

func immutableProof(old, requested []byte) ([]byte, error) {
	if len(requested) == 0 {
		return old, nil
	}
	if len(old) > 0 && subtle.ConstantTimeCompare(old, requested) != 1 {
		return nil, ErrConflict
	}
	return requested, nil
}

func (s *Store) TransitionOperation(ctx context.Context, id ID, revision int64, change OperationTransition) (Operation, error) {
	var result Operation
	if revision < 1 || revision == math.MaxInt64 {
		return result, fmt.Errorf("invalid operation revision: %w", syscall.EINVAL)
	}
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		operation, err := scanOperation(tx.QueryRowContext(ctx, "SELECT "+operationColumns+" FROM operations WHERE id=?", id))
		if err != nil {
			return err
		}
		if operation.Revision != revision || terminalOperation(operation.State) {
			return ErrConflict
		}
		if change.State != OperationRunning && change.State != OperationSucceeded && change.State != OperationFailed {
			return fmt.Errorf("invalid operation state transition: %w", syscall.EINVAL)
		}
		if !operationPhaseAllowed(operation.Kind, change.Phase) || !nextOperationPhase(operation.Kind, operation.Phase, change.Phase) {
			return ErrConflict
		}
		if change.State == OperationFailed && !validOperationError(change.ErrorCode) {
			return fmt.Errorf("failed operation needs an error code: %w", syscall.EINVAL)
		}
		if change.State != OperationFailed && change.ErrorCode != "" {
			return fmt.Errorf("error code on a successful or running operation: %w", syscall.EINVAL)
		}
		if change.State == OperationSucceeded && (change.Phase != PhaseCompleted || !change.Settled || len(change.Result) == 0) {
			return fmt.Errorf("success requires a settled completion receipt: %w", syscall.EINVAL)
		}
		if change.Phase == PhaseCompleted && change.State != OperationSucceeded {
			return ErrConflict
		}
		if change.Settled && !terminalOperation(change.State) {
			return ErrConflict
		}
		verified, err := immutableProof(operation.VerifiedRootProof, change.VerifiedRootProof)
		if err != nil {
			return err
		}
		planned, err := immutableProof(operation.PlannedReclaimProof, change.PlannedReclaimProof)
		if err != nil {
			return err
		}
		reclaiming, err := immutableProof(operation.ReclaimingProof, change.ReclaimingProof)
		if err != nil {
			return err
		}
		leaf := operation.QuarantineLeaf
		if change.QuarantineLeaf != "" {
			if change.QuarantineLeaf != string(operation.VolumeID)+"-"+string(operation.ID) || (leaf != "" && leaf != change.QuarantineLeaf) {
				return ErrConflict
			}
			leaf = change.QuarantineLeaf
		}
		if len(verified)+len(planned)+len(reclaiming) > s.options.MaxProofBytes || len(change.Result) > s.options.MaxPayloadBytes {
			return ErrCapacity
		}
		if operation.Kind == OperationCreate && (change.Phase == PhaseBound || change.Phase == PhaseCompleted) && len(verified) == 0 {
			return ErrConflict
		}
		if operation.Kind == OperationDelete {
			if change.Phase == PhaseVerifiedClose || change.Phase == PhaseQuarantined || change.Phase == PhaseReclaiming || change.Phase == PhaseCompleted {
				if len(verified) == 0 || len(planned) == 0 || leaf == "" {
					return ErrConflict
				}
			}
			if (change.Phase == PhaseReclaiming || change.Phase == PhaseCompleted) && len(reclaiming) == 0 {
				return ErrConflict
			}
		}
		now, err := s.logicalNowTx(ctx, tx, operation.ActorUserID)
		if err != nil {
			return err
		}
		volume, err := requireOwnedVolumeTx(ctx, tx, operation.ActorUserID, operation.VolumeID)
		if err != nil {
			return err
		}
		if volume.LastOperationID != operation.ID {
			return ErrConflict
		}
		if operation.Kind == OperationDelete && operation.Phase == PhaseAccepted && change.Phase == PhaseFrozen {
			if volume.State != VolumeActive || volume.ManagementRevision != operation.ExpectedRevision || volume.ManagementRevision == math.MaxInt64 {
				return ErrConflict
			}
			volume.State, volume.ManagementRevision = VolumeDeleting, volume.ManagementRevision+1
		}
		if change.State == OperationSucceeded {
			switch operation.Kind {
			case OperationCreate:
				if volume.State != VolumeCreating {
					return ErrConflict
				}
				volume.State = VolumeActive
			case OperationDelete:
				if volume.State != VolumeDeleting {
					return ErrConflict
				}
				volume.State = VolumeDeleted
			}
		}
		if change.State == OperationFailed && operation.Kind == OperationCreate {
			volume.State = VolumeFailed
		}
		if _, err := tx.ExecContext(ctx, "UPDATE volumes SET state=?,management_revision=?,updated_at=? WHERE id=?", volume.State, volume.ManagementRevision, now.UnixMilli(), volume.ID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET state=?,phase=?,revision=revision+1,verified_root_proof=?,planned_reclaim_proof=?,quarantine_leaf=?,reclaiming_proof=?,result=?,error_code=?,settled=?,updated_at=? WHERE id=? AND revision=?`, change.State, change.Phase, verified, planned, leaf, reclaiming, change.Result, change.ErrorCode, change.Settled, now.UnixMilli(), operation.ID, revision); err != nil {
			return err
		}
		result = operation
		result.State, result.Phase, result.Revision = change.State, change.Phase, revision+1
		result.VerifiedRootProof, result.PlannedReclaimProof, result.QuarantineLeaf, result.ReclaimingProof = verified, planned, leaf, reclaiming
		result.Result, result.ErrorCode, result.Settled, result.UpdatedAt = change.Result, change.ErrorCode, change.Settled, now
		return nil
	})
	return result, err
}

func (s *Store) SettleOperation(ctx context.Context, id ID, revision int64, proof []byte) (Operation, error) {
	var result Operation
	if len(proof) == 0 || len(proof) > s.options.MaxProofBytes || revision < 1 || revision == math.MaxInt64 {
		return result, fmt.Errorf("invalid operation settlement proof: %w", syscall.EINVAL)
	}
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		operation, err := scanOperation(tx.QueryRowContext(ctx, "SELECT "+operationColumns+" FROM operations WHERE id=?", id))
		if err != nil {
			return err
		}
		if operation.Revision != revision || !terminalOperation(operation.State) {
			return ErrConflict
		}
		if operation.Settled {
			if subtle.ConstantTimeCompare(operation.SettlementProof, proof) == 1 {
				result = operation
				return nil
			}
			return ErrConflict
		}
		now, err := s.logicalNowTx(ctx, tx, operation.ActorUserID)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE operations SET settled=1,settlement_proof=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?", proof, now.UnixMilli(), id, revision); err != nil {
			return err
		}
		operation.Settled, operation.SettlementProof, operation.Revision, operation.UpdatedAt = true, proof, revision+1, now
		result = operation
		return nil
	})
	return result, err
}
