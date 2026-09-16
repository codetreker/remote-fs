package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type childReservationCheck func(index int, nameBytes, metadataBytes int64, attr storage.Attr) error

func (s *Store) listChildrenBounded(ctx context.Context, tx *sql.Tx, parent int64, result *storage.ListResult) error {
	return s.listChildrenBoundedChecked(ctx, tx, parent, result, nil)
}

type reservedChild struct {
	node          int64
	nameBytes     int64
	metadataBytes int64
	reservation   *storage.ListReservation
}

func (s *Store) listChildrenBoundedChecked(ctx context.Context, tx *sql.Tx, parent int64, result *storage.ListResult, check childReservationCheck) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT length(CAST(e.name AS BLOB)), `+nodeAttrColumns+` FROM entries e JOIN nodes n ON n.id = e.node
		 WHERE e.volume = ? AND e.parent = ? ORDER BY e.name`,
		s.volume, parent)
	if err != nil {
		return err
	}
	reserved := []reservedChild{}
	for rows.Next() {
		var (
			nameBytes int64
			node      nodeAttrScan
		)
		if err := rows.Scan(append([]any{&nameBytes}, node.fields()...)...); err != nil {
			rows.Close()
			return err
		}
		attr, err := node.attr()
		if err != nil {
			rows.Close()
			return err
		}
		if check != nil {
			if err := check(len(reserved), nameBytes, node.metadataBytes, attr); err != nil {
				rows.Close()
				return err
			}
		}
		reservation, err := result.Reserve(nameBytes, node.metadataBytes, attr)
		if err != nil {
			rows.Close()
			return err
		}
		reserved = append(reserved, reservedChild{node: node.id, nameBytes: nameBytes, metadataBytes: node.metadataBytes, reservation: reservation})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, child := range reserved {
		name, metadata, err := s.reservedChildPayload(ctx, tx, parent, child)
		if err != nil {
			return err
		}
		if err := child.reservation.Commit(name, metadata); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) reservedChildPayload(ctx context.Context, tx *sql.Tx, parent int64, child reservedChild) (string, map[string]storage.OpaquePayload, error) {
	var total, matching int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*), coalesce(sum(
			parent = ? AND length(CAST(name AS BLOB)) = ? AND typeof(name) = 'blob' AND
			length(name) > 0 AND name NOT IN (X'2e', X'2e2e') AND
			instr(name, X'2f') = 0 AND instr(name, X'00') = 0
		), 0)
		FROM entries WHERE volume = ? AND node = ?`,
		parent, child.nameBytes, s.volume, child.node).Scan(&total, &matching); err != nil {
		return "", nil, err
	}
	if total != 1 || matching != 1 {
		return "", nil, fmt.Errorf(
			"node %d has %d entries, of which %d match its reserved parent and name: %w",
			child.node, total, matching, syscall.EIO,
		)
	}
	var name, metadata []byte
	if err := tx.QueryRowContext(ctx,
		`SELECT e.name,n.metadata FROM entries e JOIN nodes n ON n.id=e.node
		 WHERE e.volume=? AND e.node=? AND typeof(n.metadata)='blob' AND length(n.metadata)=?`,
		s.volume, child.node, child.metadataBytes).Scan(&name, &metadata); err != nil {
		return "", nil, err
	}
	decoded, err := storage.DecodeMetadata(metadata)
	return string(name), decoded, err
}
