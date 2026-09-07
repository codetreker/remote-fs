package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// nodeColumns is every column of a node, in the order nodeScan reads them. Queries that
// join entries to nodes alias the node table `n`.
const nodeColumns = `n.id, n.mode, n.size, n.atime_sec, n.atime_nsec, n.mtime_sec, n.mtime_nsec, n.content`

// nodeAttrColumns omits content for bounded directory enumeration. A listing exposes Attr,
// and loading a content key before the caller reserves an entry would defeat its byte bound.
const nodeAttrColumns = `n.id, n.mode, n.size, n.atime_sec, n.atime_nsec, n.mtime_sec, n.mtime_nsec`

// scanner is what a *sql.Row and a *sql.Rows have in common, so that one node reader serves
// both the single lookups and the listing.
type scanner interface{ Scan(dest ...any) error }

// nodeScan holds a node's columns as the database spells them.
//
// It exists because Scan takes a whole row at once: a query that puts the name or the parent
// in front of a node's columns cannot delegate to a reader that only knows about the node,
// and three copies of the same eight-column scan are three places for the column order to
// drift away from nodeColumns.
type nodeScan struct {
	id                 int64
	mode               int64
	size               int64
	atimeSec, mtimeSec int64
	atimeNsec          int32
	mtimeNsec          int32
	content            sql.NullString
}

type nodeAttrScan struct {
	id                 int64
	mode               int64
	size               int64
	atimeSec, mtimeSec int64
	atimeNsec          int32
	mtimeNsec          int32
}

func (s *nodeAttrScan) fields() []any {
	return []any{&s.id, &s.mode, &s.size, &s.atimeSec, &s.atimeNsec, &s.mtimeSec, &s.mtimeNsec}
}

func (s *nodeAttrScan) attr() storage.Attr {
	return storage.Attr{
		ID:         uint64(s.id),
		Mode:       fs.FileMode(s.mode),
		Size:       s.size,
		AccessTime: loadedTime(s.atimeSec, s.atimeNsec),
		ModTime:    loadedTime(s.mtimeSec, s.mtimeNsec),
	}
}

// fields are the destinations for nodeColumns, in that order.
func (s *nodeScan) fields() []any {
	return []any{&s.id, &s.mode, &s.size, &s.atimeSec, &s.atimeNsec, &s.mtimeSec, &s.mtimeNsec, &s.content}
}

// node renders what was scanned. content is NULL for a directory and for a file that has
// never been written, and both of those are the empty Key: the contract has one absence, not
// two.
func (s *nodeScan) node() metastore.Node {
	return metastore.Node{
		ID:         s.id,
		Mode:       fs.FileMode(s.mode),
		Size:       s.size,
		AccessTime: loadedTime(s.atimeSec, s.atimeNsec),
		ModTime:    loadedTime(s.mtimeSec, s.mtimeNsec),
		Content:    metastore.Key(s.content.String),
	}
}

// scanNode reads one node's columns.
func scanNode(row scanner) (metastore.Node, error) {
	var node nodeScan
	if err := row.Scan(node.fields()...); err != nil {
		return metastore.Node{}, err
	}
	return node.node(), nil
}

// rootNode reads the directory the namespace starts from. It is a node nobody made, and
// nothing removes or replaces it.
func (s *Store) rootNode(ctx context.Context, tx *sql.Tx) (metastore.Node, error) {
	return scanNode(tx.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes n WHERE n.id = ?`, s.root))
}

// lookup finds the child of parent called name, byte for byte.
func (s *Store) lookup(ctx context.Context, tx *sql.Tx, parent int64, name []byte) (metastore.Node, bool, error) {
	node, err := scanNode(tx.QueryRowContext(ctx,
		`SELECT `+nodeColumns+` FROM entries e JOIN nodes n ON n.id = e.node
		 WHERE e.namespace = ? AND e.parent = ? AND e.name = ?`,
		s.namespace, parent, name))
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
		child, found, err := s.lookup(ctx, tx, node.ID, []byte(name))
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
		children, err = s.listChildren(ctx, tx, dir.ID)
		return err
	}); err != nil {
		return nil, pathError("list", path, failure(err))
	}
	return children, nil
}

// ListBounded first scans only each name's length and fixed-size attributes. The caller
// reserves that entry before SQLite is allowed to copy the name BLOB into Go memory, then
// the name is loaded and committed under the same read transaction.
func (s *Store) ListBounded(ctx context.Context, path string, result *storage.ListResult) (returned error) {
	if result == nil {
		return pathError("list", path, syscall.EINVAL)
	}
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError("list", path, err)
	}
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		dir, err := s.resolve(ctx, tx, cleaned)
		if err != nil {
			return err
		}
		if !dir.IsDir() {
			return syscall.ENOTDIR
		}
		return s.listChildrenBounded(ctx, tx, dir.ID, result)
	}); err != nil {
		return pathError("list", path, failure(err))
	}
	return nil
}

type reservedChild struct {
	node        int64
	nameBytes   int64
	reservation *storage.ListReservation
}

func (s *Store) listChildrenBounded(ctx context.Context, tx *sql.Tx, parent int64, result *storage.ListResult) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT length(CAST(e.name AS BLOB)), `+nodeAttrColumns+` FROM entries e JOIN nodes n ON n.id = e.node
		 WHERE e.namespace = ? AND e.parent = ? ORDER BY e.name`,
		s.namespace, parent)
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
		reservation, err := result.Reserve(nameBytes, node.attr())
		if err != nil {
			rows.Close()
			return err
		}
		reserved = append(reserved, reservedChild{node: node.id, nameBytes: nameBytes, reservation: reservation})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, child := range reserved {
		name, err := s.reservedName(ctx, tx, parent, child)
		if err != nil {
			return err
		}
		if err := child.reservation.Commit(name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) reservedName(ctx context.Context, tx *sql.Tx, parent int64, child reservedChild) (string, error) {
	var total, matching int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*), coalesce(sum(
			parent = ? AND length(CAST(name AS BLOB)) = ? AND typeof(name) = 'blob' AND
			length(name) > 0 AND name NOT IN (X'2e', X'2e2e') AND
			instr(name, X'2f') = 0 AND instr(name, X'00') = 0
		), 0)
		FROM entries WHERE namespace = ? AND node = ?`,
		parent, child.nameBytes, s.namespace, child.node).Scan(&total, &matching); err != nil {
		return "", err
	}
	if total != 1 || matching != 1 {
		return "", fmt.Errorf(
			"node %d has %d entries, of which %d match its reserved parent and name: %w",
			child.node, total, matching, syscall.EIO,
		)
	}
	var name []byte
	if err := tx.QueryRowContext(ctx,
		`SELECT name FROM entries WHERE namespace = ? AND node = ?`,
		s.namespace, child.node).Scan(&name); err != nil {
		return "", err
	}
	return string(name), nil
}

func (s *Store) listChildren(ctx context.Context, tx *sql.Tx, parent int64) ([]metastore.Child, error) {
	children := []metastore.Child{}
	if err := s.visitChildren(ctx, tx, parent, func(child metastore.Child) error {
		children = append(children, child)
		return nil
	}); err != nil {
		return nil, err
	}
	return children, nil
}

func (s *Store) visitChildren(ctx context.Context, tx *sql.Tx, parent int64, add func(metastore.Child) error) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT e.name, `+nodeColumns+` FROM entries e JOIN nodes n ON n.id = e.node
		 WHERE e.namespace = ? AND e.parent = ? ORDER BY e.name`,
		s.namespace, parent)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name []byte
			node nodeScan
		)
		if err := rows.Scan(append([]any{&name}, node.fields()...)...); err != nil {
			return err
		}
		if err := add(metastore.Child{Name: name, Node: node.node()}); err != nil {
			return err
		}
	}
	return rows.Err()
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
		// Nothing was written, so nothing is recorded. An event carrying a node identical to
		// the one every replica already holds would still cost a position and a slot in the
		// retention window, and a caller sending empty changes would push real events out of it.
		if change.Empty() {
			return nil
		}
		if err := applyChange(ctx, tx, node, change); err != nil {
			return err
		}
		return s.recordChanged(ctx, tx, node.ID)
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
		id, err := allocateNodeID(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO nodes (id, namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
			VALUES (?, ?, ?, 0, ?, ?, ?, ?, NULL)`,
			id, s.namespace, int64(mode), sec, nsec, sec, nsec); err != nil {
			return err
		}
		if err := s.link(ctx, tx, parent.ID, name, id); err != nil {
			return err
		}
		if err := s.recordCreated(ctx, tx, metastore.Location{Parent: parent.ID, Name: name}, id); err != nil {
			return err
		}
		return s.touch(ctx, tx, parent.ID, now)
	}); err != nil {
		return pathError(op, path, failure(err))
	}
	return nil
}

// link puts a name in a directory. A name already there is EEXIST, which the primary key is
// what decides — so two writers racing for one name cannot both be told they made it.
func (s *Store) link(ctx context.Context, tx *sql.Tx, parent int64, name []byte, node int64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO entries (namespace, parent, name, node) VALUES (?, ?, ?, ?)`,
		s.namespace, parent, name, node); err != nil {
		if isUniqueViolation(err) {
			return syscall.EEXIST
		}
		return err
	}
	return nil
}

func (s *Store) unlink(ctx context.Context, tx *sql.Tx, parent int64, name []byte) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM entries WHERE namespace = ? AND parent = ? AND name = ?`,
		s.namespace, parent, name)
	return err
}

// touch records that a directory's contents changed. A directory's modification time is the
// moment a name was last added to it or taken out of it, which is what a local filesystem
// reports and what anything comparing a directory against what it holds reads.
//
// The log entry is part of touching rather than a step beside it, so that no future caller can
// move a directory's time without saying so. A replica is a snapshot plus the changes after
// it: a modification time that moved without an event stays frozen at whatever the snapshot
// caught, and a build tool comparing a directory against its contents would read a time that
// stopped being true, with nothing behind it to correct the answer.
func (s *Store) touch(ctx context.Context, tx *sql.Tx, id int64, at time.Time) error {
	sec, nsec := storedTime(at)
	if _, err := tx.ExecContext(ctx, `UPDATE nodes SET mtime_sec = ?, mtime_nsec = ? WHERE id = ?`,
		sec, nsec, id); err != nil {
		return err
	}
	return s.recordChanged(ctx, tx, id)
}

// isEmpty reports whether a directory holds no names.
func (s *Store) isEmpty(ctx context.Context, tx *sql.Tx, parent int64) (bool, error) {
	var one int
	switch err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM entries WHERE namespace = ? AND parent = ? LIMIT 1`, s.namespace, parent).Scan(&one); {
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
		node, found, err := s.lookup(ctx, tx, parent.ID, name)
		if err != nil {
			return err
		}
		if !found {
			return syscall.ENOENT
		}
		if node.IsDir() {
			return syscall.EISDIR
		}
		if err := s.unlink(ctx, tx, parent.ID, name); err != nil {
			return err
		}
		if err := s.discard(ctx, tx, node); err != nil {
			return err
		}
		if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: parent.ID, Name: name}); err != nil {
			return err
		}
		return s.touch(ctx, tx, parent.ID, time.Now())
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
		node, found, err := s.lookup(ctx, tx, parent.ID, name)
		if err != nil {
			return err
		}
		if !found {
			return syscall.ENOENT
		}
		if !node.IsDir() {
			return syscall.ENOTDIR
		}
		empty, err := s.isEmpty(ctx, tx, node.ID)
		if err != nil {
			return err
		}
		if !empty {
			return syscall.ENOTEMPTY
		}
		if err := s.unlink(ctx, tx, parent.ID, name); err != nil {
			return err
		}
		if err := s.discard(ctx, tx, node); err != nil {
			return err
		}
		if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: parent.ID, Name: name}); err != nil {
			return err
		}
		return s.touch(ctx, tx, parent.ID, time.Now())
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
	moving, found, err := s.lookup(ctx, tx, fromParent.ID, fromName)
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
	displaced, occupied, err := s.lookup(ctx, tx, toParent.ID, toName)
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
			empty, err := s.isEmpty(ctx, tx, displaced.ID)
			if err != nil {
				return err
			}
			if !empty {
				return syscall.ENOTEMPTY
			}
		}
		if err := s.unlink(ctx, tx, toParent.ID, toName); err != nil {
			return err
		}
		if err := s.discard(ctx, tx, displaced); err != nil {
			return err
		}
		if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: toParent.ID, Name: toName}); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE entries SET parent = ?, name = ? WHERE namespace = ? AND parent = ? AND name = ?`,
		toParent.ID, toName, s.namespace, fromParent.ID, fromName); err != nil {
		return err
	}
	// The node itself is untouched by the move — only the entry naming it was rewritten — so
	// the one read before the move is what the destination holds now.
	if err := s.recordRenamed(ctx, tx,
		metastore.Location{Parent: toParent.ID, Name: toName},
		metastore.Location{Parent: fromParent.ID, Name: fromName}, moving); err != nil {
		return err
	}

	now := time.Now()
	if err := s.touch(ctx, tx, fromParent.ID, now); err != nil {
		return err
	}
	if toParent.ID == fromParent.ID {
		return nil
	}
	return s.touch(ctx, tx, toParent.ID, now)
}
