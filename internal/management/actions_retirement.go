package management

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"math"
	"slices"
	"time"
)

type NamespaceRetirement struct {
	ActorUserID           ID
	ExpectedRetiredBefore int64
	Floor                 int64
	NativeRevision        int64
	Cutoff                time.Time
	ObservedAt            time.Time
	VolumeIDs             []ID
	Participants          []NamespaceParticipation
	Digest                [sha256.Size]byte
}

type NamespaceParticipation struct {
	Sequence int64
	VolumeID ID
}

func (s *Store) eligibleRetirementTx(ctx context.Context, tx *sql.Tx, actor ID, observedAt, cutoff time.Time) (int64, error) {
	var floor int64
	if err := tx.QueryRowContext(ctx, "SELECT retired_before_namespace FROM users WHERE id=?", actor).Scan(&floor); err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT sequence,accept_until FROM action_namespaces WHERE actor_user_id=? AND sequence>? ORDER BY sequence LIMIT ?", actor, floor, s.options.MaxNamespacesPerUser)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var sequence, acceptUntil int64
		if err := rows.Scan(&sequence, &acceptUntil); err != nil {
			return 0, err
		}
		if acceptUntil > observedAt.UnixMilli() {
			break
		}
		var blocked int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM operations WHERE actor_user_id=? AND action_namespace=? AND (state IN ('pending','running') OR settled=0 OR updated_at>?)`, actor, sequence, cutoff.UnixMilli()).Scan(&blocked); err != nil {
			return 0, err
		}
		if blocked > 0 {
			break
		}
		floor = sequence
	}
	return floor, rows.Err()
}

func retirementDigest(candidate NamespaceRetirement) ([sha256.Size]byte, error) {
	candidate.Digest = [sha256.Size]byte{}
	payload, err := json.Marshal(candidate)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(payload), nil
}

func (s *Store) retirementVolumesTx(ctx context.Context, tx *sql.Tx, actor ID, previous, floor int64) ([]ID, error) {
	rows, err := tx.QueryContext(ctx, `SELECT volume_id FROM operations WHERE actor_user_id=? AND action_namespace>? AND action_namespace<=?
		UNION SELECT volume_id FROM namespace_volume_participants WHERE actor_user_id=? AND sequence>? AND sequence<=? ORDER BY volume_id`, actor, previous, floor, actor, previous, floor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]ID, 0)
	for rows.Next() {
		var id ID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if len(ids) == s.options.MaxVolumes {
			return nil, ErrCorrupt
		}
		if !validID(id) {
			return nil, ErrCorrupt
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) retirementParticipantsTx(ctx context.Context, tx *sql.Tx, actor ID, previous, floor int64) ([]NamespaceParticipation, error) {
	rows, err := tx.QueryContext(ctx, "SELECT sequence,volume_id FROM namespace_volume_participants WHERE actor_user_id=? AND sequence>? AND sequence<=? ORDER BY sequence,volume_id", actor, previous, floor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	participants := make([]NamespaceParticipation, 0)
	for rows.Next() {
		var participant NamespaceParticipation
		if err := rows.Scan(&participant.Sequence, &participant.VolumeID); err != nil {
			return nil, err
		}
		if len(participants) == s.options.MaxNamespaceParticipants || !validID(participant.VolumeID) {
			return nil, ErrCorrupt
		}
		participants = append(participants, participant)
	}
	return participants, rows.Err()
}

// RetirementCandidate does not delete anything. The controller first confirms
// native retirement through Floor+1 on each returned volume, then records that
// evidence with ConfirmRetirement. Unknown native results retain all receipts.
func (s *Store) RetirementCandidate(ctx context.Context, actor ID) (NamespaceRetirement, error) {
	var candidate NamespaceRetirement
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		now, err := s.logicalNowTx(ctx, tx, actor)
		if err != nil {
			return err
		}
		candidate.ActorUserID, candidate.ObservedAt, candidate.Cutoff = actor, now, now.Add(-s.options.ReceiptRetention)
		if err := tx.QueryRowContext(ctx, "SELECT retired_before_namespace,retired_native_revision FROM users WHERE id=?", actor).Scan(&candidate.ExpectedRetiredBefore, &candidate.NativeRevision); err != nil {
			return err
		}
		if candidate.NativeRevision == math.MaxInt64 {
			return ErrCapacity
		}
		candidate.NativeRevision++
		candidate.Floor, err = s.eligibleRetirementTx(ctx, tx, actor, candidate.ObservedAt, candidate.Cutoff)
		if err != nil {
			return err
		}
		candidate.VolumeIDs, err = s.retirementVolumesTx(ctx, tx, actor, candidate.ExpectedRetiredBefore, candidate.Floor)
		if err != nil {
			return err
		}
		candidate.Participants, err = s.retirementParticipantsTx(ctx, tx, actor, candidate.ExpectedRetiredBefore, candidate.Floor)
		if err != nil {
			return err
		}
		candidate.Digest, err = retirementDigest(candidate)
		return err
	})
	return candidate, err
}

func (s *Store) ConfirmRetirement(ctx context.Context, actor ID, candidate NamespaceRetirement, nativeRevision int64, nativeProof []byte) (int64, error) {
	digest, err := retirementDigest(candidate)
	if err != nil {
		return 0, err
	}
	if actor != candidate.ActorUserID || candidate.Digest != digest || candidate.Floor <= candidate.ExpectedRetiredBefore || candidate.Floor == math.MaxInt64 || nativeRevision != candidate.NativeRevision || nativeRevision < 1 || len(nativeProof) == 0 {
		return 0, ErrConflict
	}
	if len(nativeProof) > s.options.MaxProofBytes-sha256.Size {
		return 0, ErrCapacity
	}
	if !candidate.Cutoff.Equal(candidate.ObservedAt.Add(-s.options.ReceiptRetention)) || candidate.ObservedAt.UnixMilli() < 0 {
		return 0, ErrConflict
	}
	var floor int64
	err = s.writeTx(ctx, func(tx *sql.Tx) error {
		now, err := s.logicalNowTx(ctx, tx, actor)
		if err != nil {
			return err
		}
		if now.Before(candidate.ObservedAt) {
			return ErrConflict
		}
		var priorRevision int64
		if err := tx.QueryRowContext(ctx, "SELECT retired_before_namespace,retired_native_revision FROM users WHERE id=?", actor).Scan(&floor, &priorRevision); err != nil {
			return err
		}
		if floor != candidate.ExpectedRetiredBefore || priorRevision == math.MaxInt64 || priorRevision+1 != nativeRevision {
			return ErrConflict
		}
		eligible, err := s.eligibleRetirementTx(ctx, tx, actor, candidate.ObservedAt, candidate.Cutoff)
		if err != nil {
			return err
		}
		if eligible != candidate.Floor {
			return ErrConflict
		}
		volumes, err := s.retirementVolumesTx(ctx, tx, actor, floor, eligible)
		if err != nil {
			return err
		}
		if len(volumes) != len(candidate.VolumeIDs) {
			return ErrConflict
		}
		for i := range volumes {
			if volumes[i] != candidate.VolumeIDs[i] {
				return ErrConflict
			}
		}
		participants, err := s.retirementParticipantsTx(ctx, tx, actor, floor, eligible)
		if err != nil {
			return err
		}
		if !slices.Equal(participants, candidate.Participants) {
			return ErrConflict
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM operations WHERE actor_user_id=? AND action_namespace>? AND action_namespace<=?", actor, floor, eligible); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM namespace_volume_participants WHERE actor_user_id=? AND sequence>? AND sequence<=?", actor, floor, eligible); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM action_namespaces WHERE actor_user_id=? AND sequence>? AND sequence<=?", actor, floor, eligible); err != nil {
			return err
		}
		boundProof := make([]byte, sha256.Size+len(nativeProof))
		copy(boundProof, candidate.Digest[:])
		copy(boundProof[sha256.Size:], nativeProof)
		if _, err := tx.ExecContext(ctx, "UPDATE users SET retired_before_namespace=?,retired_native_revision=?,retired_native_proof=? WHERE id=?", eligible, nativeRevision, boundProof, actor); err != nil {
			return err
		}
		floor = eligible
		return nil
	})
	return floor, err
}
