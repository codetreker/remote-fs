package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

func initialDirectoryRevision() []byte {
	return binary.BigEndian.AppendUint64(nil, 1)
}

func validDirectoryRevision(token []byte) bool {
	if len(token) != 8 {
		return false
	}
	revision := binary.BigEndian.Uint64(token)
	return revision > 0 && revision <= math.MaxInt64
}

func (s *Store) directoryRevision(ctx context.Context, tx *sql.Tx, id int64) (uint64, error) {
	var token []byte
	err := tx.QueryRowContext(ctx,
		`SELECT directory_revision FROM nodes WHERE volume=? AND id=?`, s.volume, id).Scan(&token)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, syscall.ESTALE
	}
	if err != nil {
		return 0, err
	}
	if len(token) == 0 {
		return 0, syscall.ENOTDIR
	}
	if !validDirectoryRevision(token) {
		return 0, fmt.Errorf("directory %d has an invalid name-set revision: %w", id, syscall.EIO)
	}
	revision := binary.BigEndian.Uint64(token)
	return revision, nil
}

func (s *Store) advanceDirectoryRevision(ctx context.Context, tx *sql.Tx, id int64) error {
	revision, err := s.directoryRevision(ctx, tx, id)
	if err != nil {
		return err
	}
	if revision == math.MaxInt64 {
		return fmt.Errorf("directory %d exhausted its name-set revision: %w", id, syscall.EOVERFLOW)
	}
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET directory_revision=? WHERE volume=? AND id=?`,
		binary.BigEndian.AppendUint64(nil, revision+1), s.volume, id)
	if err != nil {
		return err
	}
	return sqlvalue.ExactlyOne(result, "advancing a directory revision")
}

// A v4 replication stream does not carry directory revisions. Replica roots
// synthesized from that wire format use the native numeric token so namespace
// changes can still invalidate local snapshots. Opaque or exhausted tokens are
// replaced with a bounded digest of the prior token and applied change identity;
// a following revision-bearing source event restores its exact token.
func (s *Store) advanceReplicaDirectoryRevision(ctx context.Context, tx *sql.Tx, id, position int64) error {
	var token []byte
	if err := tx.QueryRowContext(ctx, `SELECT directory_revision FROM nodes WHERE volume=? AND id=?`, s.volume, id).Scan(&token); err != nil {
		return err
	}
	var next []byte
	if validDirectoryRevision(token) && binary.BigEndian.Uint64(token) < math.MaxInt64 {
		next = binary.BigEndian.AppendUint64(nil, binary.BigEndian.Uint64(token)+1)
	} else {
		material := append([]byte("remote-fs replica directory revision\x00"), token...)
		material = binary.BigEndian.AppendUint64(material, uint64(id))
		material = binary.BigEndian.AppendUint64(material, uint64(position))
		digest := sha256.Sum256(material)
		next = digest[:]
	}
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET directory_revision=? WHERE volume=? AND id=?`,
		next, s.volume, id)
	if err != nil {
		return err
	}
	return sqlvalue.ExactlyOne(result, "advancing a replica directory revision")
}
