package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

// rootNode reads the directory the volume starts from. It is a node nobody made, and
// nothing removes or replaces it.
func (s *Store) rootNode(ctx context.Context, tx *sql.Tx) (metastore.Node, error) {
	return scanNode(tx.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes n WHERE n.id = ?`, s.root))
}

// lookup finds the child of parent called name, byte for byte.
func (s *Store) lookup(ctx context.Context, tx *sql.Tx, parent int64, name []byte) (metastore.Node, bool, error) {
	node, err := scanNode(tx.QueryRowContext(ctx,
		`SELECT `+nodeColumns+` FROM entries e JOIN nodes n ON n.id = e.node
		 WHERE e.volume = ? AND e.parent = ? AND e.name = ?`,
		s.volume, parent, name))
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
	operation, checkingIO := metastore.FileIOFromContext(ctx)
	if checkingIO {
		if err := s.coordinator.commit.acquire(ctx); err != nil {
			return metastore.Node{}, err
		}
		defer s.coordinator.commit.release()
	}
	var node metastore.Node
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		found, err := s.resolve(ctx, tx, cleaned)
		node = found
		if err == nil && checkingIO {
			return s.checkFileIOLocked(ctx, tx, node.ID, node.Size, operation)
		}
		return err
	}); err != nil {
		return metastore.Node{}, pathError("stat", path, sqlerr.Failure(err))
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
		return nil, pathError("list", path, sqlerr.Failure(err))
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
		return pathError("list", path, sqlerr.Failure(err))
	}
	return nil
}

type reservedChild struct {
	node          int64
	nameBytes     int64
	metadataBytes int64
	reservation   *storage.ListReservation
}

func (s *Store) listChildrenBounded(ctx context.Context, tx *sql.Tx, parent int64, result *storage.ListResult) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT length(CAST(e.name AS BLOB)), `+nodeMetadataColumns+`,`+nodePayloadLengths+` FROM entries e JOIN nodes n ON n.id = e.node
		 WHERE e.volume = ? AND e.parent = ? ORDER BY e.name`,
		s.volume, parent)
	if err != nil {
		return err
	}
	reserved := []reservedChild{}
	for rows.Next() {
		var (
			nameBytes int64
			node      nodeMetadataScan
		)
		if err := rows.Scan(append([]any{&nameBytes}, node.fields()...)...); err != nil {
			rows.Close()
			return err
		}
		value, err := node.node()
		attr := value.Attr()
		if err != nil {
			rows.Close()
			return err
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
		name, err := s.reservedName(ctx, tx, parent, child)
		if err != nil {
			return err
		}
		var encoded []byte
		if err := tx.QueryRowContext(ctx, `SELECT metadata FROM nodes WHERE volume=? AND id=? AND length(metadata)=?`, s.volume, child.node, child.metadataBytes).Scan(&encoded); err != nil {
			return err
		}
		metadata, err := storage.DecodeMetadata(encoded)
		if err != nil {
			return err
		}
		if err := child.reservation.Commit(name, metadata); err != nil {
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
		FROM entries WHERE volume = ? AND node = ?`,
		parent, child.nameBytes, s.volume, child.node).Scan(&total, &matching); err != nil {
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
		`SELECT name FROM entries WHERE volume = ? AND node = ?`,
		s.volume, child.node).Scan(&name); err != nil {
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
		 WHERE e.volume = ? AND e.parent = ? ORDER BY e.name`,
		s.volume, parent)
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
		value, err := node.node()
		if err != nil {
			return err
		}
		if err := add(metastore.Child{Name: name, Node: value}); err != nil {
			return err
		}
	}
	return rows.Err()
}

// SetAttr applies the attributes a change names and leaves the rest alone.
//
// Metadata replacement checks its observed revision. Time-only changes preserve
// every opaque value and advance the same metadata revision.
func (s *Store) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	if err := change.Check(); err != nil {
		return pathError("setattr", path, err)
	}
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError("setattr", path, err)
	}
	if err := s.mutateVolume(ctx, locking.SetAttrMutation, []string{cleaned}, func(tx *sql.Tx) error {
		// A change that names nothing still answers for the node it names, and resolving the
		// path is what answers for it.
		node, err := s.resolve(ctx, tx, cleaned)
		if err != nil {
			return err
		}
		// Nothing was written, so nothing is recorded. An event carrying a node identical to
		// the one every replica already holds would still cost a position and a slot in the
		// retention window, and a caller sending empty changes would push real events out of it.
		if change.ExpectedRevision != 0 && change.ExpectedRevision != node.MetadataRevision {
			return syscall.EAGAIN
		}
		if change.Empty() {
			return nil
		}
		if err := applyChange(ctx, tx, node, change); err != nil {
			return err
		}
		return s.recordChanged(ctx, tx, node)
	}); err != nil {
		return pathError("setattr", path, sqlerr.Failure(err))
	}
	return nil
}

func applyChange(ctx context.Context, tx *sql.Tx, node metastore.Node, change storage.AttrChange) error {
	if err := change.Check(); err != nil {
		return err
	}
	if change.ExpectedRevision != 0 && change.ExpectedRevision != node.MetadataRevision {
		return syscall.EAGAIN
	}
	if change.Empty() {
		return nil
	}
	if node.MetadataRevision >= math.MaxInt64 {
		return syscall.EOVERFLOW
	}
	columns := []string{"metadata_revision=metadata_revision+1"}
	args := []any{}
	if change.Metadata != nil {
		metadata, err := storage.EncodeMetadata(*change.Metadata)
		if err != nil {
			return err
		}
		columns = append(columns, "metadata=?")
		args = append(args, metadata)
	}
	changed := time.Now()
	if change.ChangeTime != nil {
		changed = *change.ChangeTime
	}
	times := []struct {
		prefix string
		value  *time.Time
	}{{"atime", change.AccessTime}, {"mtime", change.ModTime}, {"creation", change.CreationTime}, {"change", &changed}}
	for _, field := range times {
		if field.value != nil {
			sec, nsec := sqlvalue.StoredTime(*field.value)
			columns = append(columns, field.prefix+"_sec=?", field.prefix+"_nsec=?")
			args = append(args, sec, nsec)
		}
	}
	args = append(args, node.ID, int64(node.MetadataRevision))
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET `+strings.Join(columns, ",")+` WHERE id=? AND metadata_revision=?`, args...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return syscall.ESTALE
	}
	return nil
}

// Create records an empty file. It references no object, because zero bytes are worth no
// round trip to an object store.
func (s *Store) Create(ctx context.Context, path string) error {
	return s.makeNode(ctx, "create", path, storage.NodeRegular)
}

func (s *Store) Mkdir(ctx context.Context, path string) error {
	return s.makeNode(ctx, "mkdir", path, storage.NodeDirectory)
}

// makeNode records a new node of the given kind, failing with EEXIST if anything is already
// at the name.
func (s *Store) makeNode(ctx context.Context, op, path string, kind storage.NodeKind) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError(op, path, err)
	}
	// The root is already there, and neither a file nor a directory may take its place.
	if cleaned == "" {
		return pathError(op, path, syscall.EEXIST)
	}
	if err := s.mutateVolume(ctx, locking.CreateMutation, []string{cleaned}, func(tx *sql.Tx) error {
		parent, name, err := s.resolveParent(ctx, tx, cleaned)
		if err != nil {
			return err
		}
		now := time.Now()
		sec, nsec := sqlvalue.StoredTime(now)
		id, err := dbstate.AllocateNodeID(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO nodes (id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,creation_sec,creation_nsec,change_sec,change_nsec,metadata_revision,directory_revision,metadata,link_target,content)
 VALUES (?,?,?,0,?,?,?,?,?,?,?,?,1,?,X'52464d010000',X'',NULL)`,
			id, s.volume, int64(kind), sec, nsec, sec, nsec, sec, nsec, sec, nsec, directoryInitialRevision(kind)); err != nil {
			return err
		}
		if err := s.link(ctx, tx, parent.ID, name, id); err != nil {
			return err
		}
		if err := s.recordCreated(ctx, tx, metastore.Location{Parent: parent.ID, Name: name}, id); err != nil {
			return err
		}
		return s.touch(ctx, tx, parent.ID, now, parent)
	}); err != nil {
		return pathError(op, path, sqlerr.Failure(err))
	}
	return nil
}

// link puts a name in a directory. A name already there is EEXIST, which the primary key is
// what decides — so two writers racing for one name cannot both be told they made it.
func directoryInitialRevision(kind storage.NodeKind) int64 {
	if kind == storage.NodeDirectory {
		return 1
	}
	return 0
}

func (s *Store) bumpDirectoryRevision(ctx context.Context, tx *sql.Tx, id int64) error {
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET directory_revision=directory_revision+1 WHERE volume=? AND id=? AND kind=? AND directory_revision<?`, s.volume, id, int64(storage.NodeDirectory), int64(math.MaxInt64))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return syscall.EOVERFLOW
	}
	return nil
}

func (s *Store) link(ctx context.Context, tx *sql.Tx, parent int64, name []byte, node int64) error {
	if err := s.checkParentEntriesActive(ctx, tx, parent); err != nil {
		return err
	}
	entry, err := dbstate.AllocateEntryID(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO entries(volume,parent,name,node,id) VALUES(?,?,?,?,?)`, s.volume, parent, name, node, entry); err != nil {
		if sqlerr.IsUniqueViolation(err) {
			return syscall.EEXIST
		}
		return err
	}
	return s.bumpDirectoryRevision(ctx, tx, parent)
}
func (s *Store) unlink(ctx context.Context, tx *sql.Tx, parent int64, name []byte) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM entries WHERE volume=? AND parent=? AND name=?`, s.volume, parent, name)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return syscall.ENOENT
	}
	return s.bumpDirectoryRevision(ctx, tx, parent)
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
func (s *Store) touch(ctx context.Context, tx *sql.Tx, id int64, at time.Time, observed ...metastore.Node) error {
	before, err := nodeByID(ctx, tx, id)
	if err != nil {
		return err
	}
	if len(observed) > 0 {
		before = observed[0]
	}
	sec, nsec := sqlvalue.StoredTime(at)
	if _, err := tx.ExecContext(ctx, `UPDATE nodes SET mtime_sec = ?, mtime_nsec = ? WHERE id = ?`,
		sec, nsec, id); err != nil {
		return err
	}
	return s.recordChanged(ctx, tx, before)
}

// isEmpty reports whether a directory holds no names.
func (s *Store) isEmpty(ctx context.Context, tx *sql.Tx, parent int64) (bool, error) {
	var one int
	switch err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM entries WHERE volume = ? AND parent = ? LIMIT 1`, s.volume, parent).Scan(&one); {
	case errors.Is(err, sql.ErrNoRows):
		return true, nil
	case err != nil:
		return false, err
	}
	return false, nil
}

// Remove detaches a retained file or retires an unretained file and its object.
func (s *Store) Remove(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError("unlink", path, err)
	}
	// The root is a directory, and removing a directory through here is EISDIR.
	if cleaned == "" {
		return pathError("unlink", path, syscall.EISDIR)
	}
	if err := s.mutateVolume(ctx, locking.RemoveMutation, []string{cleaned}, func(tx *sql.Tx) error {
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
		if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: parent.ID, Name: name}, node); err != nil {
			return err
		}
		if err := s.unlink(ctx, tx, parent.ID, name); err != nil {
			return err
		}
		if err := s.discard(ctx, tx, node); err != nil {
			return err
		}
		return s.touch(ctx, tx, parent.ID, time.Now(), parent)
	}); err != nil {
		return pathError("unlink", path, sqlerr.Failure(err))
	}
	return nil
}

func (s *Store) RemoveDir(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError("rmdir", path, err)
	}
	// The root is not a node any caller made. Removing it would empty the volume out of
	// existence and leave everything afterwards answering ENOENT — a missing file where the
	// truth is a missing volume.
	if cleaned == "" {
		return pathError("rmdir", path, syscall.EBUSY)
	}
	if err := s.mutateVolume(ctx, locking.RemoveMutation, []string{cleaned}, func(tx *sql.Tx) error {
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
		if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: parent.ID, Name: name}, node); err != nil {
			return err
		}
		if err := s.unlink(ctx, tx, parent.ID, name); err != nil {
			return err
		}
		if err := s.discard(ctx, tx, node); err != nil {
			return err
		}
		return s.touch(ctx, tx, parent.ID, time.Now(), parent)
	}); err != nil {
		return pathError("rmdir", path, sqlerr.Failure(err))
	}
	return nil
}

// Physical pins keep the current object and its charge after volume removal.
// The caller holds the same gate as file open and final physical release.
func (s *Store) discard(ctx context.Context, tx *sql.Tx, node metastore.Node) error {
	if s.coordinator.pins[retainedNode{s.volume, node.ID}] > 0 {
		_, err := tx.ExecContext(ctx, `UPDATE nodes SET detached=1 WHERE volume=? AND id=?`, s.volume, node.ID)
		return err
	}
	return s.discardNode(ctx, tx, node)
}

func (s *Store) discardNode(ctx context.Context, tx *sql.Tx, node metastore.Node) error {
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
	if err := s.mutateVolume(ctx, locking.RenameMutation, []string{cleanFrom, cleanTo}, func(tx *sql.Tx) error {
		return s.rename(ctx, tx, cleanFrom, cleanTo)
	}); err != nil {
		return linkError(from, to, sqlerr.Failure(err))
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
	if err := s.checkParentEntriesActive(ctx, tx, toParent.ID); err != nil {
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
		if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: toParent.ID, Name: toName}, displaced); err != nil {
			return err
		}
		if err := s.unlink(ctx, tx, toParent.ID, toName); err != nil {
			return err
		}
		if err := s.discard(ctx, tx, displaced); err != nil {
			return err
		}
	}

	before, err := s.captureEventImage(ctx, tx, moving)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE entries SET parent = ?, name = ? WHERE volume = ? AND parent = ? AND name = ?`,
		toParent.ID, toName, s.volume, fromParent.ID, fromName); err != nil {
		return err
	}

	if err := s.bumpDirectoryRevision(ctx, tx, fromParent.ID); err != nil {
		return err
	}
	if toParent.ID != fromParent.ID {
		if err := s.bumpDirectoryRevision(ctx, tx, toParent.ID); err != nil {
			return err
		}
	}
	if err := s.recordRenamed(ctx, tx, before, moving.ID); err != nil {
		return err
	}
	now := time.Now()
	if err := s.touch(ctx, tx, fromParent.ID, now, fromParent); err != nil {
		return err
	}
	if toParent.ID == fromParent.ID {
		return nil
	}
	return s.touch(ctx, tx, toParent.ID, now, toParent)
}
