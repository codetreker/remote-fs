package management

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

const volumeColumns = "id,owner_user_id,display_name,storage_name,state,management_revision,last_operation_id,created_at,updated_at"

func scanVolume(row rowScanner) (Volume, error) {
	var volume Volume
	var created, updated int64
	if err := row.Scan(&volume.ID, &volume.OwnerUserID, &volume.DisplayName, &volume.StorageName, &volume.State, &volume.ManagementRevision, &volume.LastOperationID, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Volume{}, ErrNotFound
		}
		return Volume{}, err
	}
	if !validID(volume.ID) || !validID(volume.OwnerUserID) || !validID(volume.LastOperationID) || volume.StorageName != string(volume.ID) || volume.ManagementRevision < 1 || validateVolumeName(volume.DisplayName) != nil || !validVolumeState(volume.State) {
		return Volume{}, ErrCorrupt
	}
	volume.CreatedAt, volume.UpdatedAt = time.UnixMilli(created).UTC(), time.UnixMilli(updated).UTC()
	return volume, nil
}

func validVolumeState(state VolumeState) bool {
	return state == VolumeCreating || state == VolumeActive || state == VolumeDeleting || state == VolumeDeleted || state == VolumeFailed
}

func validateVolumeName(name string) error {
	if !utf8.ValidString(name) || utf8.RuneCountInString(name) < 1 || utf8.RuneCountInString(name) > 128 {
		return fmt.Errorf("volume name must contain 1–128 Unicode scalars: %w", syscall.EINVAL)
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return fmt.Errorf("volume name contains a control character: %w", syscall.EINVAL)
		}
	}
	return nil
}

func requireOwnedVolumeTx(ctx context.Context, tx *sql.Tx, actor, id ID) (Volume, error) {
	if _, err := requireUserTx(ctx, tx, actor); err != nil {
		return Volume{}, err
	}
	volume, err := scanVolume(tx.QueryRowContext(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id=?", id))
	if err != nil {
		return Volume{}, err
	}
	if volume.OwnerUserID != actor {
		return Volume{}, ErrDenied
	}
	return volume, nil
}

func requireActiveVolumeTx(ctx context.Context, tx *sql.Tx, actor, id ID) (Volume, error) {
	volume, err := requireOwnedVolumeTx(ctx, tx, actor, id)
	if err != nil {
		return Volume{}, err
	}
	if volume.State != VolumeActive {
		return Volume{}, ErrConflict
	}
	return volume, nil
}

func (s *Store) GetVolume(ctx context.Context, actor, id ID) (Volume, error) {
	volume, err := scanVolume(s.read.QueryRowContext(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id=?", id))
	if err != nil {
		return Volume{}, err
	}
	if volume.OwnerUserID != actor {
		return Volume{}, ErrDenied
	}
	return volume, nil
}

func (s *Store) ListVolumes(ctx context.Context, actor, after ID, limit int) ([]Volume, error) {
	if limit < 1 || limit > s.options.MaxPageSize {
		return nil, fmt.Errorf("invalid volume page size: %w", syscall.EINVAL)
	}
	rows, err := s.read.QueryContext(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE owner_user_id=? AND id>? ORDER BY id LIMIT ?", actor, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	volumes := make([]Volume, 0, limit)
	for rows.Next() {
		volume, err := scanVolume(rows)
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, volume)
	}
	return volumes, rows.Err()
}

// ListCatalogVolumes is for offline maintenance while the caller holds exclusive
// service-root ownership. It is not a public owner or administrator access API.
func (s *Store) ListCatalogVolumes(ctx context.Context, after ID, limit int) ([]Volume, error) {
	if limit < 1 || limit > s.options.MaxPageSize {
		return nil, fmt.Errorf("invalid catalog volume page size: %w", syscall.EINVAL)
	}
	rows, err := s.read.QueryContext(ctx, "SELECT "+volumeColumns+" FROM volumes WHERE id>? ORDER BY id LIMIT ?", after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	volumes := make([]Volume, 0, limit)
	for rows.Next() {
		volume, err := scanVolume(rows)
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, volume)
	}
	return volumes, rows.Err()
}

func (s *Store) CreateVolume(ctx context.Context, actor ID, key ActionKey, name string, quotaBytes int64) (Operation, error) {
	return s.mutateVolume(ctx, actor, OperationCreate, VolumeMutation{Action: key, Name: name, QuotaBytes: quotaBytes})
}

func (s *Store) RenameVolume(ctx context.Context, actor ID, key ActionKey, volume ID, revision int64, name string) (Operation, error) {
	return s.mutateVolume(ctx, actor, OperationRename, VolumeMutation{Action: key, VolumeID: volume, ExpectedRevision: revision, Name: name})
}

func (s *Store) ChangeQuota(ctx context.Context, actor ID, key ActionKey, volume ID, nativeRevision, quotaBytes int64) (Operation, error) {
	return s.mutateVolume(ctx, actor, OperationQuota, VolumeMutation{Action: key, VolumeID: volume, ExpectedRevision: nativeRevision, QuotaBytes: quotaBytes})
}

func (s *Store) DeleteVolume(ctx context.Context, actor ID, key ActionKey, volume ID, revision int64) (Operation, error) {
	return s.mutateVolume(ctx, actor, OperationDelete, VolumeMutation{Action: key, VolumeID: volume, ExpectedRevision: revision})
}

func validateMutation(kind OperationKind, request VolumeMutation) error {
	if err := validateActionKey(request.Action); err != nil {
		return err
	}
	if kind == OperationCreate || kind == OperationRename {
		if err := validateVolumeName(request.Name); err != nil {
			return err
		}
	}
	if kind == OperationCreate || kind == OperationQuota {
		if request.QuotaBytes < 0 {
			return fmt.Errorf("quota must be nonnegative: %w", syscall.EINVAL)
		}
	}
	if kind != OperationCreate && (request.VolumeID == "" || request.ExpectedRevision < 1 || request.ExpectedRevision == math.MaxInt64) {
		return fmt.Errorf("invalid volume or revision: %w", syscall.EINVAL)
	}
	return nil
}

func mutationPayload(actor ID, kind OperationKind, request VolumeMutation) ([]byte, []byte, error) {
	payload, err := json.Marshal(struct {
		Actor   ID
		Kind    OperationKind
		Request VolumeMutation
	}{actor, kind, request})
	if err != nil {
		return nil, nil, err
	}
	fingerprint := sha256.Sum256(payload)
	return payload, fingerprint[:], nil
}

func (s *Store) mutateVolume(ctx context.Context, actor ID, kind OperationKind, request VolumeMutation) (Operation, error) {
	if err := validateMutation(kind, request); err != nil {
		return Operation{}, err
	}
	payload, fingerprint, err := mutationPayload(actor, kind, request)
	if err != nil {
		return Operation{}, err
	}
	if len(payload) > s.options.MaxPayloadBytes {
		return Operation{}, ErrCapacity
	}
	var operation Operation
	var refused error
	err = s.writeTx(ctx, func(tx *sql.Tx) error {
		if _, err := requireUserTx(ctx, tx, actor); err != nil {
			return err
		}
		known, err := operationByKeyTx(ctx, tx, actor, request.Action)
		if err == nil {
			if !equalFingerprint(known.Fingerprint, fingerprint) {
				return ErrConflict
			}
			operation = known
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		now, err := s.checkNamespaceTx(ctx, tx, actor, request.Action.Namespace)
		if err != nil {
			if errors.Is(err, ErrExpired) || errors.Is(err, ErrHistoryExpired) || errors.Is(err, ErrNotFound) {
				refused = err
				return nil
			}
			return err
		}
		if err := s.operationCapacityTx(ctx, tx, kind == OperationRename); err != nil {
			return err
		}
		var volume Volume
		if kind == OperationCreate {
			var count int
			if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM volumes").Scan(&count); err != nil {
				return err
			}
			if count >= s.options.MaxVolumes {
				return ErrCapacity
			}
			id, err := NewID()
			if err != nil {
				return err
			}
			volume = Volume{ID: id, OwnerUserID: actor, StorageName: string(id), DisplayName: request.Name, State: VolumeCreating, ManagementRevision: 1, CreatedAt: now, UpdatedAt: now}
		} else {
			volume, err = requireActiveVolumeTx(ctx, tx, actor, request.VolumeID)
			if err != nil {
				return err
			}
			if kind != OperationQuota && volume.ManagementRevision != request.ExpectedRevision {
				return ErrConflict
			}
			var pending int
			if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM operations WHERE volume_id=? AND state IN ('pending','running')", volume.ID).Scan(&pending); err != nil {
				return err
			}
			if pending > 0 {
				return ErrConflict
			}
		}
		if kind == OperationCreate || kind == OperationRename {
			var duplicate int
			if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM volumes WHERE owner_user_id=? AND display_name=? AND state<>'deleted' AND id<>?", actor, request.Name, volume.ID).Scan(&duplicate); err != nil {
				return err
			}
			if duplicate > 0 {
				return ErrConflict
			}
		}
		operationID, err := NewID()
		if err != nil {
			return err
		}
		operation = Operation{ID: operationID, ActorUserID: actor, VolumeID: volume.ID, Kind: kind, Action: request.Action, Fingerprint: fingerprint, ExpectedRevision: request.ExpectedRevision, Payload: payload, State: OperationPending, Phase: PhaseAccepted, Revision: 1, CreatedAt: now, UpdatedAt: now}
		if kind == OperationRename {
			volume.DisplayName = request.Name
			volume.ManagementRevision++
			operation.State, operation.Phase, operation.Settled = OperationSucceeded, PhaseCompleted, true
			operation.Result, err = json.Marshal(struct{ ManagementRevision int64 }{volume.ManagementRevision})
			if err != nil {
				return err
			}
		}
		volume.LastOperationID, volume.UpdatedAt = operation.ID, now
		if kind == OperationCreate {
			_, err = tx.ExecContext(ctx, `INSERT INTO volumes(`+volumeColumns+`) VALUES(?,?,?,?,?,?,?,?,?)`, volume.ID, actor, volume.DisplayName, volume.StorageName, volume.State, volume.ManagementRevision, volume.LastOperationID, now.UnixMilli(), now.UnixMilli())
		} else {
			_, err = tx.ExecContext(ctx, "UPDATE volumes SET display_name=?,management_revision=?,last_operation_id=?,updated_at=? WHERE id=?", volume.DisplayName, volume.ManagementRevision, volume.LastOperationID, now.UnixMilli(), volume.ID)
		}
		if err != nil {
			return err
		}
		return insertOperationTx(ctx, tx, operation)
	})
	if err != nil {
		return Operation{}, err
	}
	if refused != nil {
		return Operation{}, refused
	}
	return operation, nil
}
