package sqlite

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"syscall"
)

func (s *Store) directoryRevision(ctx context.Context, tx *sql.Tx, id int64) (int64, error) {
	var token []byte
	err := tx.QueryRowContext(ctx, `SELECT directory_revision FROM nodes WHERE volume=? AND id=?`, s.volume, id).Scan(&token)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, syscall.ESTALE
	}
	if err != nil {
		return 0, err
	}
	if len(token) == 0 {
		return 0, syscall.ENOTDIR
	}
	if len(token) != 8 || binary.BigEndian.Uint64(token) == 0 || binary.BigEndian.Uint64(token) > math.MaxInt64 {
		return 0, fmt.Errorf("directory %d has an invalid name-set revision: %w", id, syscall.EIO)
	}
	return int64(binary.BigEndian.Uint64(token)), nil
}

func (s *Store) advanceDirectoryRevision(ctx context.Context, tx *sql.Tx, id int64) error {
	revision, err := s.directoryRevision(ctx, tx, id)
	if err != nil {
		return err
	}
	if revision == math.MaxInt64 {
		return fmt.Errorf("directory %d exhausted its name-set revision: %w", id, syscall.EOVERFLOW)
	}
	token := binary.BigEndian.AppendUint64(nil, uint64(revision+1))
	_, err = tx.ExecContext(ctx, `UPDATE nodes SET directory_revision=? WHERE volume=? AND id=?`, token, s.volume, id)
	return err
}
