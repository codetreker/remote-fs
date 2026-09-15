package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"math"
	"slices"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

// locationFacts reads only ancestors: moving a directory never expands its subtree.
// Each name length is checked before SQLite copies the payload into this process.
func (s *Store) locationFacts(ctx context.Context, tx *sql.Tx, at metastore.Location) (*metastore.LocationFacts, error) {
	if at.Parent == 0 {
		return nil, nil
	}
	if len(at.Name) > metastore.MaxNotificationNameBytes {
		return nil, fmt.Errorf("notification leaf exceeds byte bound: %w", syscall.EFBIG)
	}
	facts := &metastore.LocationFacts{LeafName: slices.Clone(at.Name)}
	bytesHeld := len(at.Name)
	seen := make(map[int64]bool)
	id := at.Parent
	for {
		if len(facts.Ancestors) >= metastore.MaxNotificationAncestors {
			return nil, fmt.Errorf("notification ancestry exceeds depth bound: %w", syscall.EFBIG)
		}
		if id <= 0 || seen[id] {
			return nil, fmt.Errorf("notification ancestry is cyclic or invalid: %w", syscall.EIO)
		}
		seen[id] = true
		if id == s.root {
			facts.Ancestors = append(facts.Ancestors, metastore.DirectoryAncestor{DirectoryID: id})
			break
		}
		var parent, length int64
		var storageClass string
		if err := tx.QueryRowContext(ctx, `SELECT parent,length(name),typeof(name) FROM entries WHERE volume=? AND node=?`, s.volume, id).Scan(&parent, &length, &storageClass); err != nil {
			return nil, err
		}
		if storageClass != "blob" || length <= 0 {
			return nil, fmt.Errorf("notification ancestor has invalid name: %w", syscall.EIO)
		}
		if length > int64(metastore.MaxNotificationNameBytes-bytesHeld) {
			return nil, fmt.Errorf("notification ancestors exceed byte bound: %w", syscall.EFBIG)
		}
		var name []byte
		if err := tx.QueryRowContext(ctx, `SELECT name FROM entries WHERE volume=? AND node=?`, s.volume, id).Scan(&name); err != nil {
			return nil, err
		}
		if int64(len(name)) != length {
			return nil, fmt.Errorf("notification ancestor length changed: %w", syscall.EIO)
		}
		bytesHeld += len(name)
		facts.Ancestors = append(facts.Ancestors, metastore.DirectoryAncestor{DirectoryID: id, Name: name})
		id = parent
	}
	slices.Reverse(facts.Ancestors)
	return facts, nil
}

// notificationDirectory captures the Windows link kind while the source node
// still exists; a symlink's POSIX mode does not distinguish its target kind.
func (s *Store) notificationDirectory(ctx context.Context, tx *sql.Tx, node metastore.Node) (bool, error) {
	if node.Mode.Type() != fs.ModeSymlink {
		return node.IsDir(), nil
	}
	var raw any
	var storageClass string
	if err := tx.QueryRowContext(ctx, `SELECT windows_attributes,typeof(windows_attributes) FROM nodes WHERE volume=? AND id=?`, s.volume, node.ID).Scan(&raw, &storageClass); err != nil {
		return false, err
	}
	attributes, ok := sqlvalue.StoredInteger(raw, storageClass)
	if !ok || attributes < 0 || attributes > math.MaxUint32 || uint32(attributes)&^(storage.WindowsSettableDOSAttributes|storage.WindowsDOSDirectory) != 0 {
		return false, fmt.Errorf("invalid notification symlink directory hint: %w", syscall.EIO)
	}
	return uint32(attributes)&storage.WindowsDOSDirectory != 0, nil
}
