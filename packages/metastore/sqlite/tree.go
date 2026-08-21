package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// nodeColumns is every column of a node, in the order scanNode reads them. Queries that
// join entries to nodes alias the node table `n`.
const nodeColumns = `n.id, n.mode, n.size, n.atime_sec, n.atime_nsec, n.mtime_sec, n.mtime_nsec, n.content`

// scanner is what a *sql.Row and a *sql.Rows have in common, so that one node reader serves
// both the single lookups and the listing.
type scanner interface{ Scan(dest ...any) error }

// scanNode reads one node's columns.
//
// content is NULL for a directory and for a file that has never been written, and both of
// those are the empty Key: the contract has one absence, not two.
func scanNode(row scanner) (metastore.Node, error) {
	var (
		node               metastore.Node
		mode               int64
		atimeSec, mtimeSec int64
		atimeNsec          int32
		mtimeNsec          int32
		content            sql.NullString
	)
	if err := row.Scan(&node.ID, &mode, &node.Size, &atimeSec, &atimeNsec, &mtimeSec, &mtimeNsec, &content); err != nil {
		return metastore.Node{}, err
	}
	node.Mode = fs.FileMode(mode)
	node.AccessTime = loadedTime(atimeSec, atimeNsec)
	node.ModTime = loadedTime(mtimeSec, mtimeNsec)
	node.Content = metastore.Key(content.String)
	return node, nil
}

// rootNode reads the directory the namespace starts from. It is a node nobody made, and
// nothing removes or replaces it.
func (s *Store) rootNode(ctx context.Context, tx *sql.Tx) (metastore.Node, error) {
	return scanNode(tx.QueryRowContext(ctx,
		`SELECT `+nodeColumns+` FROM nodes n JOIN namespaces ns ON ns.root = n.id WHERE ns.id = ?`, s.namespace))
}

// lookup finds the child of parent called name, byte for byte.
func lookup(ctx context.Context, tx *sql.Tx, parent int64, name []byte) (metastore.Node, bool, error) {
	node, err := scanNode(tx.QueryRowContext(ctx,
		`SELECT `+nodeColumns+` FROM entries e JOIN nodes n ON n.id = e.node WHERE e.parent = ? AND e.name = ?`,
		parent, name))
	if errors.Is(err, sql.ErrNoRows) {
		return metastore.Node{}, false, nil
	}
	if err != nil {
		return metastore.Node{}, false, err
	}
	return node, true, nil
}

// resolve walks a cleaned path from the root, one component at a time.
//
// The two refusals it makes are the ones a kernel makes for free on a local directory and
// that nothing here can defer to anyone for. A component that is not a directory is
// ENOTDIR, and a component that is not there is ENOENT; they are decided separately because
// they are different facts, and collapsing them tells a caller a file is missing when what
// is missing is the directory it would have been in.
//
// The last component is not required to be a directory. Whether it has to be is the
// business of the operation that asked.
func (s *Store) resolve(ctx context.Context, tx *sql.Tx, cleaned string) (metastore.Node, error) {
	node, err := s.rootNode(ctx, tx)
	if err != nil {
		return metastore.Node{}, err
	}
	if cleaned == "" {
		return node, nil
	}
	for _, name := range strings.Split(cleaned, "/") {
		if !node.IsDir() {
			return metastore.Node{}, syscall.ENOTDIR
		}
		child, found, err := lookup(ctx, tx, node.ID, []byte(name))
		if err != nil {
			return metastore.Node{}, err
		}
		if !found {
			return metastore.Node{}, syscall.ENOENT
		}
		node = child
	}
	return node, nil
}

// resolveParent returns the directory that holds the last component of cleaned, along with
// that component's name.
//
// The directory has to be there already. Nothing in this contract creates an intermediate
// directory on the way to a name, and a store that did would turn a write to a path whose
// parent is gone into a silent success — go-fuse commits an unlinked file to a placeholder
// path under a directory that does not exist, which fails loudly on a real filesystem and
// must fail here for the same reason.
func (s *Store) resolveParent(ctx context.Context, tx *sql.Tx, cleaned string) (metastore.Node, []byte, error) {
	dir, name := splitPath(cleaned)
	parent, err := s.resolve(ctx, tx, dir)
	if err != nil {
		return metastore.Node{}, nil, err
	}
	if !parent.IsDir() {
		return metastore.Node{}, nil, syscall.ENOTDIR
	}
	return parent, []byte(name), nil
}

func (s *Store) Stat(ctx context.Context, path string) (metastore.Node, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return metastore.Node{}, pathError("stat", path, err)
	}
	var node metastore.Node
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		found, err := s.resolve(ctx, tx, cleaned)
		node = found
		return err
	}); err != nil {
		return metastore.Node{}, pathError("stat", path, failure(err))
	}
	return node, nil
}

// List returns a directory's children in byte order.
//
// The ordering is the storage engine's, not a sort applied afterwards: name is a BLOB, and
// SQLite compares a BLOB by its bytes under the default BINARY collation. Measured against
// modernc.org/sqlite v1.57.0, `ORDER BY name` over the names "A", "README", "Z", "a",
// "readme", "z", "\x80" and "\xff\xfe" returns them in exactly that order, so `readme`
// sorts after `README` rather than beside it and a name that is not valid UTF-8 sorts by
// the bytes it is made of.
func (s *Store) List(ctx context.Context, path string) ([]metastore.Child, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return nil, pathError("list", path, err)
	}
	var children []metastore.Child
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		dir, err := s.resolve(ctx, tx, cleaned)
		if err != nil {
			return err
		}
		if !dir.IsDir() {
			return syscall.ENOTDIR
		}
		children, err = listChildren(ctx, tx, dir.ID)
		return err
	}); err != nil {
		return nil, pathError("list", path, failure(err))
	}
	return children, nil
}

func listChildren(ctx context.Context, tx *sql.Tx, parent int64) ([]metastore.Child, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT e.name, `+nodeColumns+` FROM entries e JOIN nodes n ON n.id = e.node WHERE e.parent = ? ORDER BY e.name`,
		parent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	children := []metastore.Child{}
	for rows.Next() {
		var (
			name               []byte
			node               metastore.Node
			mode               int64
			atimeSec, mtimeSec int64
			atimeNsec          int32
			mtimeNsec          int32
			content            sql.NullString
		)
		if err := rows.Scan(&name, &node.ID, &mode, &node.Size,
			&atimeSec, &atimeNsec, &mtimeSec, &mtimeNsec, &content); err != nil {
			return nil, err
		}
		node.Mode = fs.FileMode(mode)
		node.AccessTime = loadedTime(atimeSec, atimeNsec)
		node.ModTime = loadedTime(mtimeSec, mtimeNsec)
		node.Content = metastore.Key(content.String)
		children = append(children, metastore.Child{Name: name, Node: node})
	}
	return children, rows.Err()
}

// SetAttr applies the attributes a change names and leaves the rest alone.
//
// Only the named columns are written, which is what keeps a mode change from disturbing the
// times and a time change from disturbing the mode. A kernel sends the two as separate
// requests, and neither may clear what the other set.
func (s *Store) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	if err := change.Check(); err != nil {
		return pathError("setattr", path, err)
	}
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError("setattr", path, err)
	}
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		// A change that names nothing still answers for the node it names, and resolving the
		// path is what answers for it.
		node, err := s.resolve(ctx, tx, cleaned)
		if err != nil {
			return err
		}
		if change.Empty() {
			return nil
		}
		return applyChange(ctx, tx, node, change)
	}); err != nil {
		return pathError("setattr", path, failure(err))
	}
	return nil
}

func applyChange(ctx context.Context, tx *sql.Tx, node metastore.Node, change storage.AttrChange) error {
	var (
		columns []string
		args    []any
	)
	if change.Mode != nil {
		// The type bits are kept and the settable ones replaced: a directory does not become
		// a file by being chmod-ed, and Check has already refused a change naming a kind.
		// The three special bits travel in storage.SettableMode with the permission bits, so
		// setuid, setgid and sticky are set and cleared here like any other bit.
		mode := node.Mode&^storage.SettableMode | *change.Mode&storage.SettableMode
		columns = append(columns, "mode = ?")
		args = append(args, int64(mode))
	}
	if change.AccessTime != nil {
		sec, nsec := storedTime(*change.AccessTime)
		columns = append(columns, "atime_sec = ?", "atime_nsec = ?")
		args = append(args, sec, nsec)
	}
	if change.ModTime != nil {
		sec, nsec := storedTime(*change.ModTime)
		columns = append(columns, "mtime_sec = ?", "mtime_nsec = ?")
		args = append(args, sec, nsec)
	}
	args = append(args, node.ID)
	_, err := tx.ExecContext(ctx, `UPDATE nodes SET `+strings.Join(columns, ", ")+` WHERE id = ?`, args...)
	return err
}

// Create records an empty file. It references no object, because zero bytes are worth no
// round trip to an object store.
func (s *Store) Create(ctx context.Context, path string) error {
	return s.makeNode(ctx, "create", path, fileMode)
}

func (s *Store) Mkdir(ctx context.Context, path string) error {
	return s.makeNode(ctx, "mkdir", path, fs.ModeDir|dirMode)
}

// makeNode records a new node of the given mode, failing with EEXIST if anything is already
// at the name.
func (s *Store) makeNode(ctx context.Context, op, path string, mode fs.FileMode) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError(op, path, err)
	}
	// The root is already there, and neither a file nor a directory may take its place.
	if cleaned == "" {
		return pathError(op, path, syscall.EEXIST)
	}
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		parent, name, err := s.resolveParent(ctx, tx, cleaned)
		if err != nil {
			return err
		}
		now := time.Now()
		sec, nsec := storedTime(now)
		result, err := tx.ExecContext(ctx, `
			INSERT INTO nodes (namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
			VALUES (?, ?, 0, ?, ?, ?, ?, NULL)`,
			s.namespace, int64(mode), sec, nsec, sec, nsec)
		if err != nil {
			return err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return err
		}
		if err := link(ctx, tx, parent.ID, name, id); err != nil {
			return err
		}
		return touch(ctx, tx, parent.ID, now)
	}); err != nil {
		return pathError(op, path, failure(err))
	}
	return nil
}

// link puts a name in a directory. A name already there is EEXIST, which the primary key
// over (parent, name) is what decides — so two writers racing for one name cannot both be
// told they made it.
func link(ctx context.Context, tx *sql.Tx, parent int64, name []byte, node int64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO entries (parent, name, node) VALUES (?, ?, ?)`,
		parent, name, node); err != nil {
		if isUniqueViolation(err) {
			return syscall.EEXIST
		}
		return err
	}
	return nil
}

func unlink(ctx context.Context, tx *sql.Tx, parent int64, name []byte) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM entries WHERE parent = ? AND name = ?`, parent, name)
	return err
}

// touch records that a directory's contents changed. A directory's modification time is the
// moment a name was last added to it or taken out of it, which is what a local filesystem
// reports and what anything comparing a directory against what it holds reads.
func touch(ctx context.Context, tx *sql.Tx, id int64, at time.Time) error {
	sec, nsec := storedTime(at)
	_, err := tx.ExecContext(ctx, `UPDATE nodes SET mtime_sec = ?, mtime_nsec = ? WHERE id = ?`, sec, nsec, id)
	return err
}

// isEmpty reports whether a directory holds no names.
func isEmpty(ctx context.Context, tx *sql.Tx, parent int64) (bool, error) {
	var one int
	switch err := tx.QueryRowContext(ctx, `SELECT 1 FROM entries WHERE parent = ? LIMIT 1`, parent).Scan(&one); {
	case errors.Is(err, sql.ErrNoRows):
		return true, nil
	case err != nil:
		return false, err
	}
	return false, nil
}

// Remove takes a file out of its directory. The object it referenced becomes garbage rather
// than being deleted, because nothing here reaches the object store.
func (s *Store) Remove(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError("unlink", path, err)
	}
	// The root is a directory, and removing a directory through here is EISDIR.
	if cleaned == "" {
		return pathError("unlink", path, syscall.EISDIR)
	}
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		parent, name, err := s.resolveParent(ctx, tx, cleaned)
		if err != nil {
			return err
		}
		node, found, err := lookup(ctx, tx, parent.ID, name)
		if err != nil {
			return err
		}
		if !found {
			return syscall.ENOENT
		}
		if node.IsDir() {
			return syscall.EISDIR
		}
		if err := unlink(ctx, tx, parent.ID, name); err != nil {
			return err
		}
		if err := s.discard(ctx, tx, node); err != nil {
			return err
		}
		return touch(ctx, tx, parent.ID, time.Now())
	}); err != nil {
		return pathError("unlink", path, failure(err))
	}
	return nil
}

func (s *Store) RemoveDir(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError("rmdir", path, err)
	}
	// The root is not a node any caller made. Removing it would empty the namespace out of
	// existence and leave everything afterwards answering ENOENT — a missing file where the
	// truth is a missing namespace.
	if cleaned == "" {
		return pathError("rmdir", path, syscall.EBUSY)
	}
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		parent, name, err := s.resolveParent(ctx, tx, cleaned)
		if err != nil {
			return err
		}
		node, found, err := lookup(ctx, tx, parent.ID, name)
		if err != nil {
			return err
		}
		if !found {
			return syscall.ENOENT
		}
		if !node.IsDir() {
			return syscall.ENOTDIR
		}
		empty, err := isEmpty(ctx, tx, node.ID)
		if err != nil {
			return err
		}
		if !empty {
			return syscall.ENOTEMPTY
		}
		if err := unlink(ctx, tx, parent.ID, name); err != nil {
			return err
		}
		if err := s.discard(ctx, tx, node); err != nil {
			return err
		}
		return touch(ctx, tx, parent.ID, time.Now())
	}); err != nil {
		return pathError("rmdir", path, failure(err))
	}
	return nil
}

// discard drops a node that has just lost its name, retires the object it referenced, and
// credits its bytes back to the namespace.
//
// A directory reaches here too: it references no object and holds no bytes, so both of
// those are nothing to do rather than cases to keep apart.
func (s *Store) discard(ctx context.Context, tx *sql.Tx, node metastore.Node) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM nodes WHERE id = ?`, node.ID); err != nil {
		return err
	}
	if node.Content != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET state = ? WHERE key = ?`,
			stateGarbage, string(node.Content)); err != nil {
			return err
		}
	}
	return s.charge(ctx, tx, -node.Size)
}

// Rename moves a node by rewriting the one entry that names it, which is what moves a whole
// subtree at the cost of moving the directory at its head: the nodes beneath keep the parent
// they always had, and none of their rows is touched.
func (s *Store) Rename(ctx context.Context, from, to string) error {
	cleanFrom, err := storage.CleanPath(from)
	if err != nil {
		return linkError(from, to, err)
	}
	cleanTo, err := storage.CleanPath(to)
	if err != nil {
		return linkError(from, to, err)
	}
	// Naming the root either way is EBUSY, as it is for rename(2) with "/".
	if cleanFrom == "" || cleanTo == "" {
		return linkError(from, to, syscall.EBUSY)
	}
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		return s.rename(ctx, tx, cleanFrom, cleanTo)
	}); err != nil {
		return linkError(from, to, failure(err))
	}
	return nil
}

func (s *Store) rename(ctx context.Context, tx *sql.Tx, cleanFrom, cleanTo string) error {
	fromParent, fromName, err := s.resolveParent(ctx, tx, cleanFrom)
	if err != nil {
		return err
	}
	moving, found, err := lookup(ctx, tx, fromParent.ID, fromName)
	if err != nil {
		return err
	}
	if !found {
		return syscall.ENOENT
	}

	toParent, toName, err := s.resolveParent(ctx, tx, cleanTo)
	if err != nil {
		return err
	}
	displaced, occupied, err := lookup(ctx, tx, toParent.ID, toName)
	if err != nil {
		return err
	}

	// Both names resolve to one entry: POSIX has rename(2) "return successfully and perform
	// no other action". The node has to be looked up to know it, because the same two strings
	// name a node that is not there just as readily, and that is ENOENT.
	if occupied && displaced.ID == moving.ID {
		return nil
	}

	// A directory cannot be moved inside itself; the subtree would hang off a node no root
	// reaches. A path prefix answers it without walking ancestors, because a node here has
	// exactly one path: an entry is keyed by (parent, name) and nothing links a node twice.
	// EINVAL is what rename(2) reports for it.
	if strings.HasPrefix(cleanTo, cleanFrom+"/") {
		return syscall.EINVAL
	}

	if occupied {
		switch {
		case displaced.IsDir() && !moving.IsDir():
			return syscall.EISDIR
		case !displaced.IsDir() && moving.IsDir():
			return syscall.ENOTDIR
		case displaced.IsDir():
			empty, err := isEmpty(ctx, tx, displaced.ID)
			if err != nil {
				return err
			}
			if !empty {
				return syscall.ENOTEMPTY
			}
		}
		if err := unlink(ctx, tx, toParent.ID, toName); err != nil {
			return err
		}
		if err := s.discard(ctx, tx, displaced); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `UPDATE entries SET parent = ?, name = ? WHERE parent = ? AND name = ?`,
		toParent.ID, toName, fromParent.ID, fromName); err != nil {
		return err
	}

	now := time.Now()
	if err := touch(ctx, tx, fromParent.ID, now); err != nil {
		return err
	}
	if toParent.ID == fromParent.ID {
		return nil
	}
	return touch(ctx, tx, toParent.ID, now)
}
