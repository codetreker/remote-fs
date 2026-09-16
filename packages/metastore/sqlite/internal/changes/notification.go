package changes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

// ValidateNotifications validates immutable history from one database snapshot.
// The caller owns snapshot consistency. Record and total payload-byte budgets
// are checked before loading BLOBs; each scalar query closes before its payload
// query, so a one-connection database is also a valid non-concurrent caller.
func ValidateNotifications(ctx context.Context, db sqlvalue.Queryer, volume *int64, maxRecords, maxBytes int64) error {
	if maxRecords < 0 || maxBytes < 0 {
		return fmt.Errorf("negative notification integrity budget: %w", syscall.EINVAL)
	}
	var cursor metastore.Position
	var seen int64
	remaining := maxBytes
	for {
		where := ""
		var args []any
		var expectedVolume int64
		if volume != nil {
			where = " WHERE volume=?"
			args = append(args, *volume)
			expectedVolume = *volume
		}
		if seen != 0 {
			if where == "" {
				where = " WHERE "
			} else {
				where += " AND "
			}
			where += "position > ?"
			args = append(args, int64(cursor))
		}
		rows, err := db.QueryContext(ctx, `SELECT `+changeMetadataColumns+` FROM changes`+where+` ORDER BY position LIMIT 1`, args...)
		if err != nil {
			return err
		}
		if !rows.Next() {
			return errors.Join(rows.Err(), rows.Close())
		}
		c, lengths, _, identityHighWater, scanErr := scanChangeMetadata(rows, expectedVolume)
		if err := errors.Join(scanErr, rows.Err(), rows.Close()); err != nil {
			return err
		}
		if seen >= maxRecords {
			return fmt.Errorf("notification history exceeds the %d-record integrity bound: %w", maxRecords, syscall.EFBIG)
		}
		payloadBytes := lengths.Name + lengths.FromName + lengths.Content + lengths.Metadata + lengths.Target + lengths.Notification
		if payloadBytes > remaining {
			return fmt.Errorf("notification history exceeds the %d-byte integrity bound: %w", maxBytes, syscall.EFBIG)
		}
		remaining -= payloadBytes
		seen++
		var name, fromName, metadata, target, encoded []byte
		var content sql.NullString
		var root int64
		if err := db.QueryRowContext(ctx,
			`SELECT c.name,c.from_name,c.content,c.metadata,c.link_target,c.notification,
				CASE WHEN typeof(v.root) = 'integer' THEN v.root END
			 FROM changes c JOIN volumes v ON v.id = c.volume WHERE c.position=?
				AND coalesce(length(c.name),0)=? AND coalesce(length(c.from_name),0)=?
				AND coalesce(length(CAST(c.content AS BLOB)),0)=? AND coalesce(length(c.metadata),0)=?
				AND coalesce(length(c.link_target),0)=? AND length(c.notification)=?`,
			int64(c.Position), lengths.Name, lengths.FromName, lengths.Content, lengths.Metadata, lengths.Target, lengths.Notification,
		).Scan(&name, &fromName, &content, &metadata, &target, &encoded, &root); err != nil {
			return err
		}
		if err := validateChangePayload(c, name, fromName, content); err != nil {
			return err
		}
		c.Name = name
		if c.From != nil {
			c.From.Name = fromName
		}
		if c.Node != nil {
			c.Node.Content = metastore.Key(content.String)
			c.Node.LinkTarget = target
			decoded, err := storage.DecodeMetadata(metadata)
			if err != nil {
				return err
			}
			c.Node.Metadata = decoded
		}
		decoded, err := metastore.DecodeNotification(c, encoded)
		if err != nil {
			return err
		}
		if err := validateNotificationRoot(decoded, root); err != nil {
			return err
		}
		actual, err := metastore.NotificationIdentityHighWater(decoded)
		if err != nil || actual != identityHighWater {
			return fmt.Errorf("change identity high-water disagrees with images: %w", errors.Join(syscall.EIO, err))
		}
		cursor = c.Position
	}
}

func validateNotificationRoot(notification *metastore.Notification, root int64) error {
	if notification == nil || root <= 0 {
		return fmt.Errorf("notification has no valid volume root: %w", syscall.EIO)
	}
	for _, image := range []*metastore.EventImage{notification.Before, notification.After} {
		if image != nil && image.Location.RootNodeID != uint64(root) {
			return fmt.Errorf("notification image belongs to root %d, expected volume root %d: %w",
				image.Location.RootNodeID, root, syscall.EIO)
		}
	}
	return nil
}
