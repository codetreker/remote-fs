package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

func initialMetadata(values map[string][]byte) (map[string]storage.OpaquePayload, error) {
	if err := storage.CheckInitialMetadata(values); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	result := make(map[string]storage.OpaquePayload, len(values))
	for namespace, data := range values {
		result[namespace] = storage.OpaquePayload{
			Version: binary.BigEndian.AppendUint64(nil, 1),
			Data:    bytes.Clone(data),
		}
	}
	return result, nil
}

func (s *Store) insertNode(
	ctx context.Context,
	tx *sql.Tx,
	kind storage.NodeKind,
	initial storage.InitialFields,
	at time.Time,
) (metastore.Node, error) {
	if err := kind.Check(); err != nil {
		return metastore.Node{}, err
	}
	if err := initial.Check(); err != nil {
		return metastore.Node{}, err
	}
	if kind != storage.NodeSymlink && len(initial.LinkTarget) != 0 || kind == storage.NodeSymlink && len(initial.LinkTarget) == 0 {
		return metastore.Node{}, syscall.EINVAL
	}
	id, err := dbstate.AllocateNodeID(ctx, tx)
	if err != nil {
		return metastore.Node{}, err
	}
	access, modified, birth, changed := at, at, at, at
	if initial.Attr.AccessTime != nil {
		access = *initial.Attr.AccessTime
	}
	if initial.Attr.ModTime != nil {
		modified = *initial.Attr.ModTime
	}
	if initial.Attr.BirthTime != nil {
		birth = *initial.Attr.BirthTime
	}
	metadata, err := initialMetadata(initial.Metadata)
	if err != nil {
		return metastore.Node{}, err
	}
	encoded, err := storage.EncodeMetadata(metadata)
	if err != nil {
		return metastore.Node{}, err
	}
	if err := storage.CheckAttrResultBudget(ctx, storage.Attr{
		ID: uint64(id), Kind: kind, Size: int64(len(initial.LinkTarget)), AccessTime: access, ModTime: modified, BirthTime: &birth, ChangeTime: &changed,
	}, int64(len(encoded))); err != nil {
		return metastore.Node{}, err
	}
	accessSec, accessNsec := sqlvalue.StoredTime(access)
	modifiedSec, modifiedNsec := sqlvalue.StoredTime(modified)
	birthSec, birthNsec := sqlvalue.StoredTime(birth)
	changeSec, changeNsec := sqlvalue.StoredTime(changed)
	target := append([]byte{}, initial.LinkTarget...)
	if _, err := tx.ExecContext(ctx, `INSERT INTO nodes
		(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content,
		 birth_sec,birth_nsec,change_sec,change_nsec,metadata,link_target)
		VALUES(?,?,?,?,?,?,?,?,NULL,?,?,?,?,?,?)`,
		id, s.volume, int64(kind), len(initial.LinkTarget), accessSec, accessNsec, modifiedSec, modifiedNsec,
		birthSec, birthNsec, changeSec, changeNsec, encoded, target); err != nil {
		return metastore.Node{}, err
	}
	return metastore.Node{ID: id, Kind: kind, Size: int64(len(initial.LinkTarget)), AccessTime: access, ModTime: modified,
		BirthTime: &birth, ChangeTime: &changed, Metadata: metadata, LinkTarget: bytes.Clone(initial.LinkTarget)}, nil
}

func (s *Store) setNodeChangeTime(ctx context.Context, tx *sql.Tx, id int64, at time.Time) error {
	sec, nsec := sqlvalue.StoredTime(at)
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET change_sec=?,change_nsec=? WHERE volume=? AND id=?`, sec, nsec, s.volume, id)
	if err != nil {
		return err
	}
	return sqlvalue.ExactlyOne(result, "updating node change time")
}

func (s *Store) setNodeMetadata(ctx context.Context, tx *sql.Tx, id int64, namespace string, expected, data []byte, at time.Time) (storage.OpaquePayload, error) {
	if err := storage.CheckMetadataUpdate(namespace, expected, data); err != nil {
		return storage.OpaquePayload{}, err
	}
	node, err := s.nodeByID(ctx, tx, id)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	current, present := node.Metadata[namespace]
	if present != (len(expected) != 0) || present && !bytes.Equal(current.Version, expected) {
		return storage.OpaquePayload{}, storage.ErrConditionConflict
	}
	version := uint64(1)
	if present {
		if len(current.Version) != 8 || binary.BigEndian.Uint64(current.Version) == 0 {
			return storage.OpaquePayload{}, syscall.EIO
		}
		version = binary.BigEndian.Uint64(current.Version)
		if version == math.MaxUint64 {
			return storage.OpaquePayload{}, syscall.EOVERFLOW
		}
		version++
	}
	updated := storage.OpaquePayload{Version: binary.BigEndian.AppendUint64(nil, version), Data: bytes.Clone(data)}
	metadata := storage.CloneMetadata(node.Metadata)
	if metadata == nil {
		metadata = make(map[string]storage.OpaquePayload)
	}
	metadata[namespace] = updated
	encoded, err := storage.EncodeMetadata(metadata)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	sec, nsec := sqlvalue.StoredTime(at)
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET metadata=?,change_sec=?,change_nsec=? WHERE volume=? AND id=?`, encoded, sec, nsec, s.volume, id)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	if err := sqlvalue.ExactlyOne(result, "updating a node metadata namespace"); err != nil {
		return storage.OpaquePayload{}, err
	}
	return updated, nil
}

func (s *Store) metadataUsage(ctx context.Context, tx *sql.Tx) (int64, error) {
	var raw any
	var storageClass string
	if err := tx.QueryRowContext(ctx, `SELECT CASE WHEN typeof(metadata_used)='integer' THEN metadata_used END,
		typeof(metadata_used) FROM volumes WHERE id=?`, s.volume).Scan(&raw, &storageClass); err != nil {
		return 0, err
	}
	used, ok := sqlvalue.StoredInteger(raw, storageClass)
	if !ok || used < 0 {
		return 0, syscall.EIO
	}
	return used, nil
}

func (s *Store) checkMetadataLimit(ctx context.Context, tx *sql.Tx) error {
	used, err := s.metadataUsage(ctx, tx)
	if err != nil {
		return err
	}
	if used > s.maxMetadataBytes {
		return syscall.EFBIG
	}
	return nil
}
