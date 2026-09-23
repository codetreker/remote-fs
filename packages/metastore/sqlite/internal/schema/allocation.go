package schema

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

const allocationUnit int64 = 4096

func expectedAllocation(kind, size int64) (int64, error) {
	if size < 0 || kind < 1 || kind > 3 {
		return 0, syscall.EIO
	}
	if kind != 1 || size == 0 {
		return 0, nil
	}
	if size > math.MaxInt64-(allocationUnit-1) {
		return 0, syscall.EOVERFLOW
	}
	return ((size + allocationUnit - 1) / allocationUnit) * allocationUnit, nil
}

func backfillAuthorityAllocation(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT volume,kind,size FROM nodes ORDER BY volume,id`)
	if err != nil {
		return err
	}
	var current, total int64
	for rows.Next() {
		var volume, kind, size int64
		if err := rows.Scan(&volume, &kind, &size); err != nil {
			rows.Close()
			return err
		}
		if volume != current {
			current, total = volume, 0
		}
		allocation, err := expectedAllocation(kind, size)
		if err != nil || allocation > math.MaxInt64-total {
			rows.Close()
			return fmt.Errorf("volume %d cannot represent its virtual allocation: %w", volume, syscall.EOVERFLOW)
		}
		total += allocation
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = tx.QueryContext(ctx, `SELECT node_kind,size FROM changes WHERE node IS NOT NULL`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var kind, size int64
		if err := rows.Scan(&kind, &size); err != nil {
			rows.Close()
			return err
		}
		if _, err := expectedAllocation(kind, size); err != nil {
			rows.Close()
			return fmt.Errorf("retained change cannot represent its virtual allocation: %w", syscall.EOVERFLOW)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, statement := range []string{
		`UPDATE nodes SET allocation_size=CASE WHEN kind=1 AND size>0 THEN ((size+4095)/4096)*4096 ELSE 0 END`,
		`UPDATE changes SET allocation_size=CASE WHEN node IS NULL THEN NULL WHEN node_kind=1 AND size>0 THEN ((size+4095)/4096)*4096 ELSE 0 END`,
		`UPDATE volumes SET allocated_used=coalesce((SELECT sum(allocation_size) FROM nodes WHERE nodes.volume=volumes.id),0)`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func validateAllocation(ctx context.Context, db sqlvalue.Queryer, volume *int64, replica bool) error {
	where := ""
	changeWhere := ""
	var args []any
	if volume != nil {
		where = "WHERE v.id=?"
		changeWhere = "WHERE volume=?"
		args = []any{*volume}
	}
	rows, err := db.QueryContext(ctx, `SELECT v.id,v.allocated_used,n.id,n.kind,n.size,n.allocation_size
		FROM volumes v LEFT JOIN nodes n ON n.volume=v.id `+where+` ORDER BY v.id,n.id`, args...)
	if err != nil {
		return err
	}
	var current, recorded, calculated int64
	haveVolume := false
	finish := func() error {
		if haveVolume && calculated != recorded {
			return fmt.Errorf("volume %d allocation counter records %d bytes, nodes hold %d: %w", current, recorded, calculated, syscall.EIO)
		}
		return nil
	}
	for rows.Next() {
		var volumeID, counter int64
		var nodeID, kind, size, allocation sql.NullInt64
		if err := rows.Scan(&volumeID, &counter, &nodeID, &kind, &size, &allocation); err != nil {
			rows.Close()
			return fmt.Errorf("reading allocation ledger: %w", err)
		}
		if !haveVolume || volumeID != current {
			if err := finish(); err != nil {
				rows.Close()
				return err
			}
			current, recorded, calculated, haveVolume = volumeID, counter, 0, true
			if counter < 0 {
				rows.Close()
				return fmt.Errorf("volume %d has a negative allocation counter: %w", current, syscall.EIO)
			}
		}
		if !nodeID.Valid {
			continue
		}
		if !kind.Valid || !size.Valid || !replica && !allocation.Valid {
			rows.Close()
			return fmt.Errorf("node %d has incomplete allocation facts: %w", nodeID.Int64, syscall.EIO)
		}
		if replica {
			if allocation.Valid && allocation.Int64 < 0 {
				rows.Close()
				return fmt.Errorf("node %d has negative allocation: %w", nodeID.Int64, syscall.EIO)
			}
		} else {
			want, err := expectedAllocation(kind.Int64, size.Int64)
			if err != nil || allocation.Int64 != want {
				rows.Close()
				return fmt.Errorf("node %d has invalid allocation: %w", nodeID.Int64, syscall.EIO)
			}
		}
		if allocation.Int64 > math.MaxInt64-calculated {
			rows.Close()
			return fmt.Errorf("node %d has invalid or overflowing allocation: %w", nodeID.Int64, syscall.EIO)
		}
		calculated += allocation.Int64
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := finish(); err != nil {
		return err
	}
	rows, err = db.QueryContext(ctx, `SELECT position,node,node_kind,size,allocation_size FROM changes `+changeWhere, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var position int64
		var node, kind, size, allocation sql.NullInt64
		if err := rows.Scan(&position, &node, &kind, &size, &allocation); err != nil {
			return err
		}
		if !node.Valid {
			if allocation.Valid {
				return fmt.Errorf("removed change %d carries allocation: %w", position, syscall.EIO)
			}
			continue
		}
		if !kind.Valid || !size.Valid || !allocation.Valid {
			return fmt.Errorf("change %d has incomplete allocation facts: %w", position, syscall.EIO)
		}
		want, err := expectedAllocation(kind.Int64, size.Int64)
		if err != nil || allocation.Int64 != want {
			return fmt.Errorf("change %d has invalid allocation: %w", position, syscall.EIO)
		}
	}
	return rows.Err()
}
