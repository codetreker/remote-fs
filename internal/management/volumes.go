package management

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

const volumeColumns = "id,owner_user_id,display_name,storage_name,initial_quota_bytes,state,management_revision,error_code,created_at,updated_at"

func scanVolume(row rowScanner) (Volume, error) {
	var volume Volume
	var created, updated int64
	if err := row.Scan(&volume.ID, &volume.OwnerUserID, &volume.DisplayName, &volume.StorageName, &volume.InitialQuotaBytes, &volume.State, &volume.ManagementRevision, &volume.ErrorCode, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Volume{}, ErrNotFound
		}
		return Volume{}, err
	}
	if !validID(volume.ID) || !validID(volume.OwnerUserID) || volume.StorageName != string(volume.ID) || volume.InitialQuotaBytes < 4096 || volume.ManagementRevision < 1 || validateVolumeName(volume.DisplayName) != nil || !validVolumeState(volume.State) {
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

func (s *Store) CreateVolume(ctx context.Context, actor ID, name string, quotaBytes int64) (Volume, error) {
	if err := validateVolumeName(name); err != nil {
		return Volume{}, err
	}
	if quotaBytes < 4096 {
		return Volume{}, fmt.Errorf("initial quota must be at least 4096 bytes: %w", syscall.EINVAL)
	}
	id, err := NewID()
	if err != nil {
		return Volume{}, err
	}
	var volume Volume
	err = s.writeTx(ctx, func(tx *sql.Tx) error {
		if _, err := requireUserTx(ctx, tx, actor); err != nil {
			return err
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM volumes").Scan(&count); err != nil {
			return err
		}
		if count >= s.options.MaxVolumes {
			return ErrCapacity
		}
		if err := requireAvailableVolumeName(ctx, tx, actor, id, name); err != nil {
			return err
		}
		now := s.now().UTC()
		volume = Volume{ID: id, OwnerUserID: actor, DisplayName: name, StorageName: string(id), InitialQuotaBytes: quotaBytes, State: VolumeCreating, ManagementRevision: 1, CreatedAt: now, UpdatedAt: now}
		_, err := tx.ExecContext(ctx, "INSERT INTO volumes("+volumeColumns+") VALUES(?,?,?,?,?,?,?,?,?,?)", volume.ID, volume.OwnerUserID, volume.DisplayName, volume.StorageName, volume.InitialQuotaBytes, volume.State, volume.ManagementRevision, volume.ErrorCode, unixMillis(now), unixMillis(now))
		return err
	})
	return volume, err
}

func requireAvailableVolumeName(ctx context.Context, tx *sql.Tx, actor, id ID, name string) error {
	var duplicate bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM volumes WHERE owner_user_id=? AND display_name=? AND state<>'deleted' AND id<>?)", actor, name, id).Scan(&duplicate); err != nil {
		return err
	}
	if duplicate {
		return ErrConflict
	}
	return nil
}

func (s *Store) RenameVolume(ctx context.Context, actor, id ID, revision int64, name string) (Volume, error) {
	if err := validateVolumeName(name); err != nil {
		return Volume{}, err
	}
	return s.updateVolume(ctx, actor, id, revision, func(tx *sql.Tx, volume *Volume) error {
		if volume.State != VolumeActive {
			return ErrConflict
		}
		if err := requireAvailableVolumeName(ctx, tx, actor, id, name); err != nil {
			return err
		}
		volume.DisplayName = name
		return nil
	})
}

func (s *Store) DeleteVolume(ctx context.Context, actor, id ID, revision int64) (Volume, error) {
	return s.updateVolume(ctx, actor, id, revision, func(_ *sql.Tx, volume *Volume) error {
		if volume.State != VolumeActive && volume.State != VolumeFailed {
			return ErrConflict
		}
		volume.State, volume.ErrorCode = VolumeDeleting, ""
		return nil
	})
}

func (s *Store) SetVolumeState(ctx context.Context, actor, id ID, revision int64, state VolumeState, errorCode string) (Volume, error) {
	if len(errorCode) > 128 || (state != VolumeFailed && errorCode != "") || (state == VolumeFailed && errorCode == "") {
		return Volume{}, fmt.Errorf("invalid volume outcome: %w", syscall.EINVAL)
	}
	return s.updateVolume(ctx, actor, id, revision, func(_ *sql.Tx, volume *Volume) error {
		allowed := volume.State == VolumeCreating && (state == VolumeActive || state == VolumeFailed) || volume.State == VolumeDeleting && (state == VolumeDeleted || state == VolumeFailed)
		if !allowed {
			return ErrConflict
		}
		volume.State, volume.ErrorCode = state, errorCode
		return nil
	})
}

func (s *Store) updateVolume(ctx context.Context, actor, id ID, revision int64, change func(*sql.Tx, *Volume) error) (Volume, error) {
	if !validID(id) || revision < 1 || revision == math.MaxInt64 {
		return Volume{}, fmt.Errorf("invalid volume identity or revision: %w", syscall.EINVAL)
	}
	var volume Volume
	err := s.writeTx(ctx, func(tx *sql.Tx) error {
		var err error
		volume, err = requireOwnedVolumeTx(ctx, tx, actor, id)
		if err != nil {
			return err
		}
		if volume.ManagementRevision != revision {
			return ErrConflict
		}
		if err := change(tx, &volume); err != nil {
			return err
		}
		volume.ManagementRevision++
		volume.UpdatedAt = s.now().UTC()
		_, err = tx.ExecContext(ctx, "UPDATE volumes SET display_name=?,state=?,management_revision=?,error_code=?,updated_at=? WHERE id=?", volume.DisplayName, volume.State, volume.ManagementRevision, volume.ErrorCode, unixMillis(volume.UpdatedAt), volume.ID)
		return err
	})
	if err != nil {
		return Volume{}, err
	}
	return volume, nil
}
