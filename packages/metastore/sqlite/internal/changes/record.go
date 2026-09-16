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
	"github.com/codetreker/remote-fs/packages/storage"
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
func Record(ctx context.Context, tx *sql.Tx, volume int64, change metastore.Change) error {
	kind, err := storedKind(change.Kind)
	if err != nil {
		return err
	}

	notification, err := metastore.EncodeNotification(change)
	if err != nil {
		return err
	}

	lengths := metastore.ChangePayloadLengths{Name: int64(len(change.Name)), Notification: int64(len(notification))}
	if change.From != nil {
		lengths.FromName = int64(len(change.From.Name))
	}
	if change.Node != nil {
		lengths.Content = int64(len(change.Node.Content))
		lengths.Target = int64(len(change.Node.LinkTarget))
		metadata, err := storage.EncodeMetadata(change.Node.Metadata)
		if err != nil {
			return err
		}
		lengths.Metadata = int64(len(metadata))
	}
	if err := lengths.Check(); err != nil {
		return err
	}

	identityHighWater, err := metastore.NotificationIdentityHighWater(change.Notification)
	if err != nil {
		return err
	}
	var fromParent, fromName any
	if change.From != nil {
		fromParent, fromName = change.From.Parent, change.From.Name
	}
	var node, nodeKind, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec, content any
	var creationSec, creationNsec, changeSec, changeNsec, metadataRevision, directoryRevision, metadata, target any
	if change.Node != nil {
		n := change.Node
		accessSec, accessNsec := sqlvalue.StoredTime(n.AccessTime)
		modifiedSec, modifiedNsec := sqlvalue.StoredTime(n.ModTime)
		node, nodeKind, size = n.ID, int64(n.Kind), n.Size
		atimeSec, atimeNsec = accessSec, accessNsec
		mtimeSec, mtimeNsec = modifiedSec, modifiedNsec
		creationSec, creationNsec = storedOptionalTime(n.CreationTime)
		changeSec, changeNsec = storedOptionalTime(n.ChangeTime)
		metadataRevision, directoryRevision = int64(n.MetadataRevision), int64(n.DirectoryRevision)
		encoded, err := storage.EncodeMetadata(n.Metadata)
		if err != nil {
			return err
		}
		metadata = encoded
		target = append([]byte{}, n.LinkTarget...)
		content = sqlvalue.StoredKey(n.Content)
	}

	sec, nsec := sqlvalue.StoredTime(time.Now())
	var previousRaw any
	var previousType string
	if err := tx.QueryRowContext(ctx,
		`SELECT CASE WHEN typeof(committed_position) = 'integer' THEN committed_position END,
		        typeof(committed_position)
		 FROM logs WHERE volume = ?`, volume,
	).Scan(&previousRaw, &previousType); err != nil {
		return err
	}
	previous, ok := sqlvalue.StoredInteger(previousRaw, previousType)
	if !ok || previous < 0 {
		return fmt.Errorf("the volume log stores an invalid committed position: %w", syscall.EIO)
	}
	position, err := dbstate.AllocateChangePosition(ctx, tx)
	if err != nil {
		return err
	}
	if previous >= position {
		return fmt.Errorf("the volume log tail %d does not precede allocated position %d: %w",
			previous, position, syscall.EIO)
	}
	_, err = tx.ExecContext(ctx, `
  INSERT INTO changes(position,previous_position,identity_high_water,volume,kind,parent,name,from_parent,from_name,
   node,node_kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content,
   creation_sec,creation_nsec,change_sec,change_nsec,metadata_revision,directory_revision,metadata,link_target,
   recorded_sec,recorded_nsec,notification)
  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		position, previous, identityHighWater, volume, kind, change.Parent, change.Name, fromParent, fromName,
		node, nodeKind, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec, content,
		creationSec, creationNsec, changeSec, changeNsec, metadataRevision, directoryRevision, metadata, target,
		sec, nsec, notification)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE logs SET committed_position = ? WHERE volume = ?`,
		position, volume)
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

// CreateLog gives a volume an empty log: no entries, nothing committed, and an
// incarnation nothing has ever resumed against.
func CreateLog(ctx context.Context, tx *sql.Tx, volume int64) error {
	incarnation, err := newIncarnation()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO logs (volume, incarnation, committed_position, trimmed_through, trimmed_by_age)
		VALUES (?, ?, 0, 0, 0)`, volume, string(incarnation))
	return err
}

func storedOptionalTime(value *time.Time) (any, any) {
	if value == nil {
		return nil, nil
	}
	sec, nsec := sqlvalue.StoredTime(*value)
	return sec, nsec
}
