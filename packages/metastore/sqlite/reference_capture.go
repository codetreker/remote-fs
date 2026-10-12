package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Store) captureNamespace(ctx context.Context, tx *sql.Tx, id int64, namespace string) (storage.NodeKind, storage.MetadataObservation, error) {
	var kind, metadataBytes int64
	var encoded []byte
	err := tx.QueryRowContext(ctx, `SELECT
		CASE WHEN typeof(kind)='integer' THEN kind ELSE 0 END,
		CASE WHEN typeof(metadata)='blob' THEN length(metadata) ELSE -1 END,
		CASE WHEN typeof(metadata)='blob' AND length(metadata)<=? THEN metadata END
		FROM nodes WHERE volume=? AND id=?`, storage.MaxMetadataBytes, s.volume, id).Scan(&kind, &metadataBytes, &encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, storage.MetadataObservation{}, syscall.ESTALE
	}
	if err != nil {
		return 0, storage.MetadataObservation{}, err
	}
	if kind < int64(storage.NodeRegular) || kind > int64(storage.NodeSymlink) || metadataBytes < 0 ||
		metadataBytes > storage.MaxMetadataBytes || int64(len(encoded)) != metadataBytes {
		return 0, storage.MetadataObservation{}, syscall.EIO
	}
	metadata, err := storage.DecodeMetadata(encoded)
	if err != nil {
		return 0, storage.MetadataObservation{}, err
	}
	observed := storage.MetadataObservation{NodeID: uint64(id)}
	if value, ok := metadata[namespace]; ok {
		observed.Value = &value
	}
	if err := checkContentMetadataValue(observed.Value); err != nil {
		return 0, storage.MetadataObservation{}, err
	}
	return storage.NodeKind(kind), observed, nil
}

type pendingDeletion struct {
	pending    bool
	generation int64
}

func (s *Store) capturePendingDeletion(ctx context.Context, tx *sql.Tx, id int64) (pendingDeletion, error) {
	var explicit, generation int64
	var accepted, invalidIntent bool
	err := tx.QueryRowContext(ctx, `SELECT
		CASE WHEN typeof(n.pending_unlink)='integer' THEN n.pending_unlink ELSE -1 END,
		CASE WHEN typeof(n.pending_generation)='integer' THEN n.pending_generation ELSE -1 END,
		EXISTS(SELECT 1 FROM delete_intents i WHERE i.volume=n.volume AND i.node=n.id AND i.outcome IN (?,?)),
		EXISTS(SELECT 1 FROM delete_intents i WHERE i.volume=n.volume AND i.node=n.id
			AND (typeof(i.outcome)!='integer' OR i.outcome NOT BETWEEN 1 AND 5))
		FROM nodes n WHERE n.volume=? AND n.id=?`, storage.DeleteIntentPending, storage.DeleteIntentCleanupFailed,
		s.volume, id).Scan(&explicit, &generation, &accepted, &invalidIntent)
	if errors.Is(err, sql.ErrNoRows) {
		return pendingDeletion{}, syscall.ESTALE
	}
	if err != nil {
		return pendingDeletion{}, err
	}
	pending := explicit == 1 || accepted
	if explicit < 0 || explicit > 1 || generation < 0 || pending && generation == 0 || invalidIntent {
		return pendingDeletion{}, syscall.EIO
	}
	return pendingDeletion{pending: pending, generation: generation}, nil
}
