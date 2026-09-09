package changes

import (
	"context"
	"database/sql"
	"fmt"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

// The numbers the log stores for metastore.ChangeKind.
//
// They are written down here rather than taken from the constants' own values, because those
// come from an iota in another package: reordering it is an edit nobody would think to check
// this file for, and the damage would be a stored log whose Created rows read back as Removed
// against replicas that had already applied them.
const (
	KindCreated  = 0
	KindRemoved  = 1
	KindModified = 2
	KindRenamed  = 3
)

func storedKind(kind metastore.ChangeKind) (int64, error) {
	switch kind {
	case metastore.Created:
		return KindCreated, nil
	case metastore.Removed:
		return KindRemoved, nil
	case metastore.Modified:
		return KindModified, nil
	case metastore.Renamed:
		return KindRenamed, nil
	}
	return 0, fmt.Errorf("%w: %d is not a kind of change", syscall.EIO, kind)
}

func loadedKind(stored int64) (metastore.ChangeKind, error) {
	switch stored {
	case KindCreated:
		return metastore.Created, nil
	case KindRemoved:
		return metastore.Removed, nil
	case KindModified:
		return metastore.Modified, nil
	case KindRenamed:
		return metastore.Renamed, nil
	}
	return 0, fmt.Errorf("%w: the log holds a change of kind %d, which this build has no meaning for",
		syscall.EIO, stored)
}

// Record appends a change and its committed position inside the caller's transaction.
// Sharing that transaction prevents a committed tree mutation from losing its event
// while replicas continue under the same incarnation.
func Record(ctx context.Context, tx *sql.Tx, namespace int64, change metastore.Change) error {
	kind, err := storedKind(change.Kind)
	if err != nil {
		return err
	}

	var fromParent, fromName any
	if change.From != nil {
		fromParent, fromName = change.From.Parent, change.From.Name
	}
	var node, mode, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec, content any
	if change.Node != nil {
		accessSec, accessNsec := sqlvalue.StoredTime(change.Node.AccessTime)
		changeSec, changeNsec := sqlvalue.StoredTime(change.Node.ModTime)
		node, mode, size = change.Node.ID, int64(change.Node.Mode), change.Node.Size
		atimeSec, atimeNsec = accessSec, accessNsec
		mtimeSec, mtimeNsec = changeSec, changeNsec
		content = sqlvalue.StoredKey(change.Node.Content)
	}

	sec, nsec := sqlvalue.StoredTime(time.Now())
	var previousRaw any
	var previousType string
	if err := tx.QueryRowContext(ctx,
		`SELECT CASE WHEN typeof(committed_position) = 'integer' THEN committed_position END,
		        typeof(committed_position)
		 FROM logs WHERE namespace = ?`, namespace,
	).Scan(&previousRaw, &previousType); err != nil {
		return err
	}
	previous, ok := sqlvalue.StoredInteger(previousRaw, previousType)
	if !ok || previous < 0 {
		return fmt.Errorf("the namespace log stores an invalid committed position: %w", syscall.EIO)
	}
	position, err := dbstate.AllocateChangePosition(ctx, tx)
	if err != nil {
		return err
	}
	if previous >= position {
		return fmt.Errorf("the namespace log tail %d does not precede allocated position %d: %w",
			previous, position, syscall.EIO)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO changes (position, previous_position, namespace, kind, parent, name, from_parent, from_name,
		                     node, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content,
		                     recorded_sec, recorded_nsec)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		position, previous, namespace, kind, change.Parent, change.Name, fromParent, fromName,
		node, mode, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec, content,
		sec, nsec)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE logs SET committed_position = ? WHERE namespace = ?`,
		position, namespace)
	return err
}

// newIncarnation mints a name for a run of history.
//
// Random rather than a counter. A counter would go backwards when this database is restored
// from a backup, and two logs that have nothing to do with each other both sitting at 1 is
// entirely possible; either way a replica would be told it may resume against history it has
// never seen. A random value leaves one branch — it matches, or everything is rebuilt.
func newIncarnation() (metastore.Incarnation, error) {
	value, err := sqlvalue.RandomHex(16)
	if err != nil {
		return "", fmt.Errorf("%w: minting an incarnation: %w", syscall.EIO, err)
	}
	return metastore.Incarnation(value), nil
}

// CreateLog gives a namespace an empty log: no entries, nothing committed, and an
// incarnation nothing has ever resumed against.
func CreateLog(ctx context.Context, tx *sql.Tx, namespace int64) error {
	incarnation, err := newIncarnation()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO logs (namespace, incarnation, committed_position, trimmed_through, trimmed_by_age)
		VALUES (?, ?, 0, 0, 0)`, namespace, string(incarnation))
	return err
}
