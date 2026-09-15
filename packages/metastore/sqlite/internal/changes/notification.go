package changes

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

// ValidateNotifications follows the schema's scalar and total byte admission.
// It checks immutable facts against their row without consulting the current tree.
func ValidateNotifications(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	var args []any
	if volume != nil {
		where = " WHERE volume=?"
		args = []any{*volume}
	}
	rows, err := db.QueryContext(ctx, `SELECT kind,parent,name,from_parent,from_name,node,mode,notification FROM changes`+where, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var c metastore.Change
		var kind int64
		var parent, node, mode sql.NullInt64
		var from, encoded []byte
		if err := rows.Scan(&kind, &c.Parent, &c.Name, &parent, &from, &node, &mode, &encoded); err != nil {
			rows.Close()
			return err
		}
		c.Kind, err = loadedKind(kind)
		if err != nil {
			rows.Close()
			return err
		}
		if parent.Valid {
			c.From = &metastore.Location{Parent: parent.Int64, Name: from}
		}
		if node.Valid {
			c.Node = &metastore.Node{ID: node.Int64, Mode: fs.FileMode(mode.Int64)}
		}
		if _, err := metastore.DecodeNotification(c, encoded); err != nil {
			rows.Close()
			return err
		}
	}
	return errors.Join(rows.Err(), rows.Close())
}
