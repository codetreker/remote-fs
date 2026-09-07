package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Reserve records the intent to write path's new contents and returns the key to write them
// under.
//
// The row is committed before the key is handed back, and that is the whole point of the
// call. An object written under a key no committed record mentions cannot be told apart
// from one a writer is about to commit. Recording the key first keeps that uncertainty
// durable and bounded; a sweep never infers ownership from its age.
//
// The refusals below are the commit's, made early so that a write which cannot land does
// not pay to upload its bytes first. They are advisory: the namespace may change between
// the reservation and the commit, so commit asks all of them again and is the one whose
// answer decides. A refusal here leaves nothing behind — the reservation is inserted in the
// same transaction that checks, so a refused reservation is not a key for a sweeper to find.
func (s *Store) Reserve(ctx context.Context, path string, size int64) (metastore.Key, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return "", pathError("reserve", path, err)
	}
	if size < 0 {
		return "", pathError("reserve", path, fmt.Errorf(
			"an object of %d bytes is not a length: %w", size, syscall.EINVAL))
	}
	// The root is a directory, and a directory holds no contents to write.
	if cleaned == "" {
		return "", pathError("reserve", path, syscall.EISDIR)
	}
	if size > s.objectLimits.MaxPendingBytes {
		return "", pathError("reserve", path, fmt.Errorf(
			"an object of %d bytes exceeds the pending-object byte limit of %d: %w",
			size, s.objectLimits.MaxPendingBytes, syscall.EFBIG))
	}

	key, err := newKey()
	if err != nil {
		return "", err
	}
	sec, nsec := storedTime(time.Now())
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		// The same questions the commit will ask, asked before the bytes are paid for. The
		// answers are not binding — the namespace may change between the two calls, which is
		// why commit asks them again and is the one that decides — so nothing here is
		// recorded except the reservation itself.
		parent, name, err := s.resolveParent(ctx, tx, cleaned)
		if err != nil {
			return err
		}
		node, found, err := s.lookup(ctx, tx, parent.ID, name)
		if err != nil {
			return err
		}
		if found && node.IsDir() {
			return syscall.EISDIR
		}
		// What the commit would charge: the difference against whatever the name holds now,
		// not the whole object, so overwriting a large file with a slightly larger one is not
		// refused by a namespace that has room for the difference.
		var held int64
		if found {
			held = node.Size
		}
		if err := s.roomFor(ctx, tx, size-held); err != nil {
			return err
		}
		// Admission reads only the indexed reserved, unresolved, and garbage ranges. Full
		// validation is performed when the namespace opens and when ObjectStatus is requested;
		// putting that scan here would make every write grow with the live namespace.
		status, err := readPendingObjectStatus(ctx, tx, s.namespace, s.objectLimits)
		if err != nil {
			return err
		}
		if wouldExceed(s.objectLimits.MaxPendingObjects,
			status.ReservedCount, status.UnresolvedCount, status.GarbageCount, 1) {
			return fmt.Errorf(
				"the pending object backlog holds %d reserved, %d unresolved, and %d garbage objects under its limit of %d: %w",
				status.ReservedCount, status.UnresolvedCount, status.GarbageCount,
				s.objectLimits.MaxPendingObjects, syscall.EAGAIN)
		}
		if wouldExceed(s.objectLimits.MaxPendingBytes,
			status.ReservedBytes, status.UnresolvedBytes, status.GarbageBytes, size) {
			return fmt.Errorf(
				"the pending object backlog holds %d reserved, %d unresolved, and %d garbage bytes and cannot accept %d more under its limit of %d: %w",
				status.ReservedBytes, status.UnresolvedBytes, status.GarbageBytes, size,
				s.objectLimits.MaxPendingBytes, syscall.EAGAIN)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO objects (key, namespace, state, size, digest, created_sec, created_nsec)
			VALUES (?, ?, ?, ?, NULL, ?, ?)`,
			string(key), s.namespace, stateReserved, size, sec, nsec)
		return err
	}); err != nil {
		return "", pathError("reserve", path, failure(err))
	}
	return key, nil
}

// Quarantine retires a reservation whose Put did not establish ownership of the key.
func (s *Store) Quarantine(ctx context.Context, key metastore.Key) error {
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		var state int
		switch err := tx.QueryRowContext(ctx,
			`SELECT state FROM objects WHERE key = ? AND namespace = ?`,
			string(key), s.namespace).Scan(&state); {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return err
		}

		switch state {
		case stateReserved:
			_, err := tx.ExecContext(ctx,
				`UPDATE objects SET state = ? WHERE key = ? AND namespace = ?`,
				stateUnresolved, string(key), s.namespace)
			return err
		case stateUnresolved:
			return nil
		case stateReferenced, stateGarbage:
			return fmt.Errorf("object %q has already reached state %d: %w", key, state, syscall.EINVAL)
		default:
			return fmt.Errorf("object %q has unknown state %d: %w", key, state, syscall.EIO)
		}
	}); err != nil {
		return fmt.Errorf("quarantining object %q: %w", key, failure(err))
	}
	return nil
}

// Abandon makes a reservation immediately collectable after its caller has positive proof
// that the object store created the object under its key.
func (s *Store) Abandon(ctx context.Context, key metastore.Key) error {
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		var state int
		switch err := tx.QueryRowContext(ctx,
			`SELECT state FROM objects WHERE key = ? AND namespace = ?`,
			string(key), s.namespace).Scan(&state); {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return err
		}

		switch state {
		case stateReserved:
			_, err := tx.ExecContext(ctx,
				`UPDATE objects SET state = ? WHERE key = ? AND namespace = ?`,
				stateGarbage, string(key), s.namespace)
			return err
		case stateGarbage:
			return nil
		case stateReferenced:
			return fmt.Errorf("object %q is referenced by a name: %w", key, syscall.EINVAL)
		case stateUnresolved:
			return fmt.Errorf("ownership of object %q is unresolved: %w", key, syscall.EINVAL)
		default:
			return fmt.Errorf("object %q has unknown state %d: %w", key, state, syscall.EIO)
		}
	}); err != nil {
		return fmt.Errorf("abandoning object %q: %w", key, failure(err))
	}
	return nil
}

// Commit points a path at an object that has been written and accounts for its bytes.
//
// The whole of it is one transaction: the file appears, the counter moves, the object
// becomes referenced and the one it displaced becomes garbage together or not at all. The
// allowance is checked in that same transaction, so no window exists between deciding there
// is room and taking it.
func (s *Store) Commit(ctx context.Context, path string, object metastore.Object) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError("commit", path, err)
	}
	// The root is a directory, and a directory holds no contents to point anywhere.
	if cleaned == "" {
		return pathError("commit", path, syscall.EISDIR)
	}
	if object.Size < 0 {
		return pathError("commit", path, fmt.Errorf(
			"an object of %d bytes is not a length: %w", object.Size, syscall.EINVAL))
	}
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		return s.commit(ctx, tx, cleaned, object)
	}); err != nil {
		return pathError("commit", path, failure(err))
	}
	return nil
}

func (s *Store) commit(ctx context.Context, tx *sql.Tx, cleaned string, object metastore.Object) error {
	// An empty key commits a file with no contents. Zero bytes are not worth an object, so
	// nothing was reserved for them and there is nothing to look up.
	if object.Key != "" {
		// The key must be one we reserved and have not already pointed a name at. A key in any
		// other state — never reserved, already referenced, or swept — is a caller committing
		// something this namespace has no record of writing.
		var state int
		switch err := tx.QueryRowContext(ctx, `SELECT state FROM objects WHERE key = ? AND namespace = ?`,
			string(object.Key), s.namespace).Scan(&state); {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("object %q was never reserved in this namespace: %w", object.Key, syscall.EINVAL)
		case err != nil:
			return err
		case state != stateReserved:
			return fmt.Errorf("object %q is not awaiting a commit: %w", object.Key, syscall.EINVAL)
		}
	} else if object.Size != 0 {
		return fmt.Errorf("a file of %d bytes was committed with no object to hold them: %w",
			object.Size, syscall.EINVAL)
	}

	// The directory holding the file has to exist already, and nothing here creates it. A
	// store that treated the path as a flat key would take this write silently: after an
	// unlink, go-fuse commits to a placeholder path under a directory that was never made,
	// which fails loudly on a real filesystem and has to fail here too. Landing the bytes
	// instead would put them on a key nobody reads and nobody cleans up.
	parent, name, err := s.resolveParent(ctx, tx, cleaned)
	if err != nil {
		return err
	}
	node, found, err := s.lookup(ctx, tx, parent.ID, name)
	if err != nil {
		return err
	}
	if found && node.IsDir() {
		return syscall.EISDIR
	}

	var (
		held      int64
		displaced metastore.Key
	)
	if found {
		held, displaced = node.Size, node.Content
	}
	if err := s.account(ctx, tx, object.Size-held); err != nil {
		return err
	}

	sec, nsec := storedTime(object.ModTime)
	if found {
		// The mode the file already had stands: replacing the contents is not a request to
		// change it, and the access time belongs to whoever last read the file.
		if _, err := tx.ExecContext(ctx,
			`UPDATE nodes SET size = ?, mtime_sec = ?, mtime_nsec = ?, content = ? WHERE id = ?`,
			object.Size, sec, nsec, storedKey(object.Key), node.ID); err != nil {
			return err
		}
		// Modified rather than Created, because the name held this node before the commit. The
		// commit that makes the file records Created instead: a replica told that a name it has
		// never held was modified would have to invent the entry the event describes.
		if err := s.recordChanged(ctx, tx, node.ID); err != nil {
			return err
		}
	} else if err := s.createCommitted(ctx, tx, parent, name, object); err != nil {
		return err
	}

	// The committed size replaces the requested size recorded by the reservation, and the
	// digest is recorded now because it is not known until the bytes have been written. A file
	// with no contents reserved nothing, so there is no row to move.
	if object.Key != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET state = ?, size = ?, digest = ? WHERE key = ?`,
			stateReferenced, object.Size, object.Digest, string(object.Key)); err != nil {
			return err
		}
	}
	if displaced == "" {
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE objects SET state = ? WHERE key = ?`, stateGarbage, string(displaced))
	return err
}

// storedKey renders a content key for the column that holds it. A file with no contents
// stores NULL rather than an empty string, because the column references the object table
// and no object is named by the empty key.
func storedKey(key metastore.Key) any {
	if key == "" {
		return nil
	}
	return string(key)
}

// createCommitted makes the file a commit is pointing at when nothing is at the name yet.
// It gets the mode a new file is made with, and the directory holding it records that its
// contents changed.
func (s *Store) createCommitted(ctx context.Context, tx *sql.Tx, parent metastore.Node, name []byte, object metastore.Object) error {
	now := time.Now()
	accessSec, accessNsec := storedTime(now)
	sec, nsec := storedTime(object.ModTime)
	id, err := allocateNodeID(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO nodes (id, namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, s.namespace, int64(fileMode), object.Size, accessSec, accessNsec, sec, nsec, storedKey(object.Key)); err != nil {
		return err
	}
	if err := s.link(ctx, tx, parent.ID, name, id); err != nil {
		return err
	}
	if err := s.recordCreated(ctx, tx, metastore.Location{Parent: parent.ID, Name: name}, id); err != nil {
		return err
	}
	return s.touch(ctx, tx, parent.ID, now)
}

// account moves the namespace's byte counter by delta, refusing what the allowance cannot
// pay for.
//
// The refusal and the charge are one step inside the caller's transaction, which is what
// leaves no window between deciding there is room and taking it.
func (s *Store) account(ctx context.Context, tx *sql.Tx, delta int64) error {
	if err := s.roomFor(ctx, tx, delta); err != nil {
		return err
	}
	return s.charge(ctx, tx, delta)
}

// roomFor refuses a change of delta bytes the allowance cannot pay for, without moving the
// counter.
//
// It is separate from the charge because a reservation asks the question without taking the
// room: the bytes are not the namespace's until they are committed, and a reservation that
// charged would have to be refunded by something — nothing refunds a reservation that is
// never committed, so the counter would drift up by every abandoned write.
//
// A namespace with no allowance is never refused, and neither is a change that shrinks one:
// a workspace already over its limit would otherwise have no way back under it.
func (s *Store) roomFor(ctx context.Context, tx *sql.Tx, delta int64) error {
	if s.allowance == 0 || delta <= 0 {
		return nil
	}
	used, err := s.used(ctx, tx)
	if err != nil {
		return err
	}
	// Written as a subtraction from the allowance rather than an addition to the count, so
	// that a namespace holding close to what a byte count holds cannot wrap the sum into a
	// figure that passes.
	if delta > s.allowance-used {
		return fmt.Errorf("%d more bytes would carry the namespace past its allowance of %d bytes, of which %d are taken: %w",
			delta, s.allowance, used, syscall.EDQUOT)
	}
	return nil
}

// charge moves the counter without asking the allowance anything.
func (s *Store) charge(ctx context.Context, tx *sql.Tx, delta int64) error {
	if delta == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE namespaces SET used = used + ? WHERE id = ?`, delta, s.namespace)
	return err
}

func (s *Store) used(ctx context.Context, tx *sql.Tx) (int64, error) {
	var used int64
	err := tx.QueryRowContext(ctx, `SELECT used FROM namespaces WHERE id = ?`, s.namespace).Scan(&used)
	return used, err
}

// ObjectStatus reports the object records object-store maintenance owns for this namespace.
// Referenced objects are live content and are intentionally absent; Space accounts for them.
type ObjectStatus struct {
	// ReservedCount is the number of uploads that may still commit.
	ReservedCount int64
	// ReservedBytes is the sum of the sizes requested when those uploads were reserved.
	// objectstore.Storage reserves the exact payload length. A direct caller that commits a
	// different size sees the requested size here until that commit succeeds.
	ReservedBytes int64
	// UnresolvedCount is the number of failed Puts whose ownership outcome cannot authorize
	// either commit or deletion.
	UnresolvedCount int64
	// UnresolvedBytes is the sum of their requested payload sizes.
	UnresolvedBytes int64
	// GarbageCount is the number of objects the sweeper has yet to delete and forget.
	GarbageCount int64
	// GarbageBytes is the sum of their recorded sizes. A displaced committed object records
	// its committed size; an abandoned reservation records its requested size.
	GarbageBytes int64
	// OverLimit reports that the existing combined reserved, unresolved, and garbage backlog
	// is above this Store's configured object or byte limit. Cleanup remains available while
	// it is true.
	OverLimit bool
}

// ObjectStatus returns the current object maintenance status for this Store's namespace. It
// validates the namespace's object relationships, rooted node tree, and used-byte accounting
// before reporting a successful snapshot; Reserve performs only the indexed pending-record
// validation required on its write path.
func (s *Store) ObjectStatus(ctx context.Context) (ObjectStatus, error) {
	tx, err := s.beginReadSnapshot(ctx, s.read)
	if err != nil {
		return ObjectStatus{}, fmt.Errorf("opening an object status snapshot: %w", err)
	}
	if err := validateNamespaceIntegrity(
		ctx, tx, s.namespace, s.maxIntegrityRecords, s.maxIntegrityBytes,
	); err != nil {
		primary := fmt.Errorf("validating namespace integrity: %w", failure(err))
		return ObjectStatus{}, finishReadTransaction("object status transaction", tx, primary)
	}
	status, err := readPendingObjectStatus(ctx, tx, s.namespace, s.objectLimits)
	if err != nil {
		return ObjectStatus{}, finishReadTransaction("object status transaction", tx, err)
	}
	if err := tx.Commit(); err != nil {
		return ObjectStatus{}, fmt.Errorf("closing the object status snapshot: %w", failure(err))
	}
	return status, nil
}

type objectStatusQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type integrityQueryer interface {
	objectStatusQueryer
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// validateIntegrityWork counts the retained graph with scalar aggregates before any recursive
// traversal. wouldExceed performs the addition without overflow. A scoped count includes every
// entry whose label, parent, or child touches the namespace, so a corrupt label cannot hide
// work from the configured bound.
func validateIntegrityWork(
	ctx context.Context,
	db integrityQueryer,
	namespace *int64,
	maxIntegrityRecords int64,
) error {
	var logTables int64
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'table' AND name IN ('logs', 'changes')`).Scan(&logTables); err != nil {
		return fmt.Errorf("reading the integrity-work schema: %w", failure(err))
	}
	if logTables != 0 && logTables != 2 {
		return fmt.Errorf("the database has %d of the 2 required log tables: %w", logTables, syscall.EIO)
	}

	var namespaces, nodes, objects, entries, logs, changes int64
	var err error
	if namespace == nil {
		if logTables == 0 {
			err = db.QueryRowContext(ctx, `
				SELECT
					(SELECT count(*) FROM namespaces),
					(SELECT count(*) FROM nodes),
					(SELECT count(*) FROM objects),
					(SELECT count(*) FROM entries)`).Scan(&namespaces, &nodes, &objects, &entries)
		} else {
			err = db.QueryRowContext(ctx, `
				SELECT
					(SELECT count(*) FROM namespaces),
					(SELECT count(*) FROM nodes),
					(SELECT count(*) FROM objects),
					(SELECT count(*) FROM entries),
					(SELECT count(*) FROM logs),
					(SELECT count(*) FROM changes)`).Scan(
				&namespaces, &nodes, &objects, &entries, &logs, &changes,
			)
		}
	} else {
		err = db.QueryRowContext(ctx, `
			SELECT
				(SELECT count(*) FROM namespaces WHERE id = ?),
				(SELECT count(*) FROM nodes WHERE namespace = ?),
				(SELECT count(*) FROM objects o
				 WHERE o.namespace = ? OR EXISTS (
					 SELECT 1 FROM nodes n WHERE n.namespace = ? AND n.content = o.key
				 )),
				(SELECT count(*)
				 FROM entries e
				 LEFT JOIN nodes parent ON parent.id = e.parent
				 LEFT JOIN nodes child ON child.id = e.node
				 WHERE e.namespace = ? OR parent.namespace = ? OR child.namespace = ?),
				(SELECT count(*) FROM logs WHERE namespace = ?),
				(SELECT count(*) FROM changes WHERE namespace = ?)`,
			*namespace, *namespace, *namespace, *namespace,
			*namespace, *namespace, *namespace, *namespace, *namespace).Scan(
			&namespaces, &nodes, &objects, &entries, &logs, &changes,
		)
	}
	if err != nil {
		return fmt.Errorf("counting namespace integrity work: %w", failure(err))
	}
	if namespaces < 0 || nodes < 0 || objects < 0 || entries < 0 || logs < 0 || changes < 0 {
		return fmt.Errorf("the database returned a negative namespace integrity count: %w", syscall.EIO)
	}
	if wouldExceed(maxIntegrityRecords, namespaces, nodes, objects, entries, logs, changes) {
		return fmt.Errorf(
			"namespace integrity requires %d namespaces, %d nodes, %d objects, %d entries, %d logs, and %d changes, above the configured work limit of %d; raise MaxIntegrityRecords to open it: %w",
			namespaces, nodes, objects, entries, logs, changes, maxIntegrityRecords, syscall.EFBIG)
	}
	return nil
}

// validateIntegrityBytes admits variable-length names using SQLite's O(1) BLOB length before
// any content-sensitive predicate such as instr examines them. The row count has already been
// admitted, so streaming these fixed-width lengths is bounded in both records and bytes.
func validateIntegrityBytes(
	ctx context.Context,
	db integrityQueryer,
	namespace *int64,
	maxIntegrityBytes int64,
	version int,
) error {
	remaining := maxIntegrityBytes
	entryWhere := ""
	changeWhere := ""
	entryArgs := []any{}
	changeArgs := []any{}
	if namespace != nil {
		entryWhere = `
			LEFT JOIN nodes parent ON parent.id = e.parent
			LEFT JOIN nodes child ON child.id = e.node
			WHERE e.namespace = ? OR parent.namespace = ? OR child.namespace = ?`
		changeWhere = "WHERE namespace = ?"
		entryArgs = []any{*namespace, *namespace, *namespace}
		changeArgs = []any{*namespace}
	}
	rows, err := db.QueryContext(ctx, `
		SELECT typeof(e.name), CASE WHEN typeof(e.name) = 'blob' THEN length(e.name) END
		FROM entries e `+entryWhere, entryArgs...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var storageClass string
		var length sql.NullInt64
		if err := rows.Scan(&storageClass, &length); err != nil {
			rows.Close()
			return err
		}
		if storageClass != "blob" || !length.Valid || length.Int64 < 0 {
			rows.Close()
			return fmt.Errorf("an entry name is stored as %s rather than a BLOB: %w", storageClass, syscall.EIO)
		}
		if length.Int64 > remaining {
			rows.Close()
			return fmt.Errorf("entry and change names exceed the %d-byte integrity limit: %w",
				maxIntegrityBytes, syscall.EFBIG)
		}
		remaining -= length.Int64
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if version < 2 {
		return nil
	}

	rows, err = db.QueryContext(ctx, `
		SELECT
			typeof(name), CASE WHEN typeof(name) = 'blob' THEN length(name) END,
			typeof(from_name), CASE WHEN typeof(from_name) = 'blob' THEN length(from_name) END
		FROM changes `+changeWhere, changeArgs...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var nameType, fromNameType string
		var nameLength, fromNameLength sql.NullInt64
		if err := rows.Scan(&nameType, &nameLength, &fromNameType, &fromNameLength); err != nil {
			rows.Close()
			return err
		}
		for _, field := range []struct {
			name         string
			storageClass string
			length       sql.NullInt64
		}{
			{"name", nameType, nameLength},
			{"from_name", fromNameType, fromNameLength},
		} {
			if field.storageClass == "null" {
				continue
			}
			if field.storageClass != "blob" || !field.length.Valid || field.length.Int64 < 0 {
				rows.Close()
				return fmt.Errorf("a retained change %s is stored as %s rather than a BLOB or NULL: %w",
					field.name, field.storageClass, syscall.EIO)
			}
			if field.length.Int64 > remaining {
				rows.Close()
				return fmt.Errorf("entry and change names exceed the %d-byte integrity limit: %w",
					maxIntegrityBytes, syscall.EFBIG)
			}
			remaining -= field.length.Int64
		}
	}
	return errors.Join(rows.Err(), rows.Close())
}

// validateStorageClasses rejects SQLite's dynamically typed values before any cursor or
// payload reader can coerce them into a plausible row or order them in another storage class.
func validateStorageClasses(ctx context.Context, db integrityQueryer, namespace *int64) error {
	namespaceWhere := ""
	nodeWhere := ""
	objectWhere := ""
	entryWhere := ""
	logWhere := ""
	changeWhere := ""
	var scopeArgs []any
	if namespace != nil {
		namespaceWhere = "WHERE id = ?"
		nodeWhere = "WHERE namespace = ?"
		objectWhere = `WHERE (o.namespace = ? OR EXISTS (
			SELECT 1 FROM nodes n WHERE n.namespace = ? AND n.content = o.key))`
		entryWhere = `WHERE (e.namespace = ? OR parent.namespace = ? OR child.namespace = ?)`
		logWhere = "WHERE namespace = ?"
		changeWhere = "WHERE namespace = ?"
		scopeArgs = []any{*namespace}
	}

	var invalidNamespaces int64
	query := `SELECT count(*) FROM namespaces ` + namespaceWhere
	if namespaceWhere == "" {
		query += " WHERE "
	} else {
		query += " AND "
	}
	query += `(
		typeof(id) != 'integer' OR id <= 0 OR typeof(name) != 'text' OR name = '' OR
		typeof(root) != 'integer' OR root <= 0 OR typeof(used) != 'integer')`
	if err := db.QueryRowContext(ctx, query, scopeArgs...).Scan(&invalidNamespaces); err != nil {
		return err
	}

	objectArgs := []any{}
	entryArgs := []any{}
	if namespace != nil {
		objectArgs = []any{*namespace, *namespace}
		entryArgs = []any{*namespace, *namespace, *namespace}
	}
	queries := []struct {
		name  string
		query string
		args  []any
	}{
		{"nodes", `SELECT count(*) FROM nodes ` + nodeWhere + predicateJoin(nodeWhere) + `(
			typeof(id) != 'integer' OR id <= 0 OR
			typeof(namespace) != 'integer' OR namespace <= 0 OR
			typeof(mode) != 'integer' OR typeof(size) != 'integer' OR
			typeof(atime_sec) != 'integer' OR typeof(atime_nsec) != 'integer' OR
			typeof(mtime_sec) != 'integer' OR typeof(mtime_nsec) != 'integer' OR
			typeof(content) NOT IN ('text', 'null'))`, scopeArgs},
		{"objects", `SELECT count(*) FROM objects o ` + objectWhere + predicateJoin(objectWhere) + `(
			typeof(o.key) != 'text' OR o.key = '' OR
			typeof(o.namespace) != 'integer' OR o.namespace <= 0 OR
			typeof(o.state) != 'integer' OR typeof(o.size) != 'integer' OR
			typeof(o.digest) NOT IN ('blob', 'null') OR
			typeof(o.created_sec) != 'integer' OR typeof(o.created_nsec) != 'integer')`,
			objectArgs},
		{"entries", `SELECT count(*) FROM entries e
		 LEFT JOIN nodes parent ON parent.id = e.parent
		 LEFT JOIN nodes child ON child.id = e.node ` + entryWhere + predicateJoin(entryWhere) + `(
			typeof(e.namespace) != 'integer' OR e.namespace <= 0 OR
			typeof(e.parent) != 'integer' OR e.parent <= 0 OR
			typeof(e.name) != 'blob' OR length(e.name) = 0 OR
			e.name IN (X'2e', X'2e2e') OR instr(e.name, X'2f') != 0 OR instr(e.name, X'00') != 0 OR
			typeof(e.node) != 'integer' OR e.node <= 0)`,
			entryArgs},
		{"logs", `SELECT count(*) FROM logs ` + logWhere + predicateJoin(logWhere) + `(
			typeof(namespace) != 'integer' OR namespace <= 0 OR
			typeof(incarnation) != 'text' OR incarnation = '' OR
			typeof(committed_position) != 'integer' OR typeof(trimmed_through) != 'integer' OR
			typeof(trimmed_by_age) != 'integer')`, scopeArgs},
		{"changes", `SELECT count(*) FROM changes ` + changeWhere + predicateJoin(changeWhere) + `(
			typeof(position) != 'integer' OR position <= 0 OR
			typeof(previous_position) != 'integer' OR previous_position < 0 OR
			typeof(namespace) != 'integer' OR namespace <= 0 OR
			typeof(kind) != 'integer' OR typeof(parent) != 'integer' OR
			typeof(name) NOT IN ('blob', 'null') OR
			typeof(from_parent) NOT IN ('integer', 'null') OR
			typeof(from_name) NOT IN ('blob', 'null') OR
			typeof(node) NOT IN ('integer', 'null') OR typeof(mode) NOT IN ('integer', 'null') OR
			typeof(size) NOT IN ('integer', 'null') OR typeof(atime_sec) NOT IN ('integer', 'null') OR
			typeof(atime_nsec) NOT IN ('integer', 'null') OR typeof(mtime_sec) NOT IN ('integer', 'null') OR
			typeof(mtime_nsec) NOT IN ('integer', 'null') OR typeof(content) NOT IN ('text', 'null') OR
			typeof(recorded_sec) != 'integer' OR typeof(recorded_nsec) != 'integer')`, scopeArgs},
		{"backing store", `SELECT count(*) FROM backing_store WHERE
			typeof(singleton) != 'integer' OR singleton != 1 OR
			typeof(store_id) != 'text' OR store_id = ''`, nil},
		{"durable state", `SELECT count(*) FROM database_state WHERE
			typeof(singleton) != 'integer' OR singleton != 1 OR
			typeof(database_id) != 'text' OR length(database_id) != 32 OR
			typeof(generation) != 'integer' OR generation < 0 OR
			typeof(node_high_water) != 'integer' OR node_high_water < 0 OR
			typeof(change_high_water) != 'integer' OR change_high_water < 0`, nil},
	}
	if invalidNamespaces != 0 {
		return fmt.Errorf("the database holds %d namespace rows in an invalid SQLite storage class: %w",
			invalidNamespaces, syscall.EIO)
	}
	for _, check := range queries {
		var count int64
		if err := db.QueryRowContext(ctx, check.query, check.args...).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("the database holds %d %s rows in an invalid SQLite storage class: %w",
				count, check.name, syscall.EIO)
		}
	}
	return nil
}

func predicateJoin(where string) string {
	if where == "" {
		return " WHERE "
	}
	return " AND "
}

func validateNodeValues(ctx context.Context, db integrityQueryer, namespace *int64) error {
	where := ""
	var args []any
	if namespace != nil {
		where = "WHERE namespace = ? AND "
		args = []any{*namespace}
	} else {
		where = "WHERE "
	}
	args = append(args,
		int64(math.MaxUint32), int64(fs.ModeType), int64(fs.ModeDir),
		int64(fs.ModeType), int64(fs.ModeDir), int64(fs.ModeType),
	)
	var invalid int64
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM nodes `+where+`(
			mode < 0 OR mode > ? OR (mode & ?) NOT IN (0, ?) OR size < 0 OR
			atime_nsec < 0 OR atime_nsec >= 1000000000 OR
			mtime_nsec < 0 OR mtime_nsec >= 1000000000 OR
			((mode & ?) = ? AND (size != 0 OR content IS NOT NULL)) OR
			(content IS NOT NULL AND content = '')
		)`, args...).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("the database holds %d nodes with invalid metadata values: %w", invalid, syscall.EIO)
	}
	return nil
}

// validateVersionTwoLogIntegrity checks the historical log format before migration resets
// its incarnation. Version 2 has no predecessor chain, so this proves row shape and tail only.
func validateVersionTwoLogIntegrity(ctx context.Context, db integrityQueryer, namespace *int64) error {
	if err := validateVersionTwoLogStorageClasses(ctx, db, namespace); err != nil {
		return err
	}
	return validateLogIntegrityVersion(ctx, db, namespace, false)
}

func validateVersionTwoLogStorageClasses(ctx context.Context, db integrityQueryer, namespace *int64) error {
	where := ""
	var args []any
	if namespace != nil {
		where = "WHERE namespace = ? AND "
		args = []any{*namespace}
	} else {
		where = "WHERE "
	}
	var invalidLogs, invalidChanges int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM logs `+where+`(
		typeof(namespace) != 'integer' OR typeof(incarnation) != 'text' OR
		typeof(committed_position) != 'integer' OR typeof(trimmed_through) != 'integer' OR
		typeof(trimmed_by_age) != 'integer')`, args...).Scan(&invalidLogs); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM changes `+where+`(
		typeof(position) != 'integer' OR typeof(namespace) != 'integer' OR
		typeof(kind) != 'integer' OR typeof(parent) != 'integer' OR
		typeof(name) NOT IN ('blob', 'null') OR
		typeof(from_parent) NOT IN ('integer', 'null') OR
		typeof(from_name) NOT IN ('blob', 'null') OR
		typeof(node) NOT IN ('integer', 'null') OR typeof(mode) NOT IN ('integer', 'null') OR
		typeof(size) NOT IN ('integer', 'null') OR typeof(atime_sec) NOT IN ('integer', 'null') OR
		typeof(atime_nsec) NOT IN ('integer', 'null') OR typeof(mtime_sec) NOT IN ('integer', 'null') OR
		typeof(mtime_nsec) NOT IN ('integer', 'null') OR typeof(content) NOT IN ('text', 'null') OR
		typeof(recorded_sec) != 'integer' OR typeof(recorded_nsec) != 'integer')`, args...).Scan(&invalidChanges); err != nil {
		return err
	}
	if invalidLogs != 0 || invalidChanges != 0 {
		return fmt.Errorf("schema version 2 holds %d log rows and %d change rows in invalid SQLite storage classes: %w",
			invalidLogs, invalidChanges, syscall.EIO)
	}
	return nil
}

// validateLogIntegrity checks the durable tail, predecessor chain, and operation-dependent
// shape of every retained change before Snapshot or Since may expose it as history.
func validateLogIntegrity(ctx context.Context, db integrityQueryer, namespace *int64) error {
	return validateLogIntegrityVersion(ctx, db, namespace, true)
}

func validateLogIntegrityVersion(ctx context.Context, db integrityQueryer, namespace *int64, predecessors bool) error {
	namespaceWhere := ""
	changeWhere := ""
	var args []any
	if namespace != nil {
		namespaceWhere = "WHERE ns.id = ? AND "
		changeWhere = "WHERE c.namespace = ? AND "
		args = []any{*namespace}
	} else {
		namespaceWhere = "WHERE "
		changeWhere = "WHERE "
	}

	tailPredicate := `l.committed_position != coalesce((
				SELECT max(c.position) FROM changes c WHERE c.namespace = ns.id
			), l.trimmed_through)`
	if predecessors {
		tailPredicate = `l.committed_position != coalesce((
				SELECT max(c.position) FROM changes c WHERE c.namespace = ns.id
			), l.trimmed_through) OR
			EXISTS (
				SELECT 1 FROM (
					SELECT position, previous_position,
						row_number() OVER (ORDER BY position) AS ordinal,
						lag(position) OVER (ORDER BY position) AS preceding
					FROM changes WHERE namespace = ns.id
				) chain
				WHERE chain.previous_position != CASE
					WHEN chain.ordinal = 1 THEN l.trimmed_through
					ELSE chain.preceding
				END
			)`
	}
	var invalidLogs int64
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM namespaces ns
		LEFT JOIN logs l ON l.namespace = ns.id
		`+namespaceWhere+`(
			l.namespace IS NULL OR l.incarnation = '' OR
			l.committed_position < 0 OR l.trimmed_through < 0 OR
			l.trimmed_by_age NOT IN (0, 1) OR l.trimmed_through > l.committed_position OR
			`+tailPredicate+` OR
			EXISTS (
				SELECT 1 FROM changes c
				WHERE c.namespace = ns.id AND c.position <= l.trimmed_through
			)
		)`, args...).Scan(&invalidLogs); err != nil {
		return err
	}

	predecessorPredicate := ""
	if predecessors {
		predecessorPredicate = `
			OR typeof(c.previous_position) != 'integer'
			OR c.previous_position < 0 OR c.previous_position >= c.position`
	}
	var invalidChanges int64
	changeArgs := append([]any{}, args...)
	changeArgs = append(changeArgs,
		kindCreated, kindRemoved, kindModified, kindRenamed,
		kindCreated, kindRemoved, kindRenamed, kindModified,
		kindRenamed, kindRenamed,
		kindRemoved, kindRemoved,
		int64(math.MaxUint32), int64(fs.ModeType), int64(fs.ModeDir),
		int64(fs.ModeType), int64(fs.ModeDir), int64(fs.ModeType),
	)
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM changes c
		LEFT JOIN namespaces ns ON ns.id = c.namespace
		LEFT JOIN nodes current_node ON current_node.id = c.node
		LEFT JOIN nodes current_parent ON current_parent.id = c.parent
		LEFT JOIN nodes current_from_parent ON current_from_parent.id = c.from_parent
		`+changeWhere+`(
			ns.id IS NULL OR c.position <= 0 OR c.kind NOT IN (?, ?, ?, ?) OR c.parent < 0 OR
			(c.kind IN (?, ?, ?) AND (c.parent <= 0 OR c.name IS NULL OR length(c.name) = 0)) OR
			(c.kind = ? AND NOT (
				(c.parent = 0 AND c.name IS NULL) OR
				(c.parent > 0 AND c.name IS NOT NULL AND length(c.name) > 0)
			)) OR
			(c.kind = ? AND (c.from_parent IS NULL OR c.from_parent <= 0 OR
				c.from_name IS NULL OR length(c.from_name) = 0)) OR
			(c.kind != ? AND (c.from_parent IS NOT NULL OR c.from_name IS NOT NULL)) OR
			(c.name IS NOT NULL AND (
				c.name IN (X'2e', X'2e2e') OR instr(c.name, X'2f') != 0 OR instr(c.name, X'00') != 0
			)) OR
			(c.from_name IS NOT NULL AND (
				c.from_name IN (X'2e', X'2e2e') OR
				instr(c.from_name, X'2f') != 0 OR instr(c.from_name, X'00') != 0
			)) OR
			(c.kind = ? AND (c.node IS NOT NULL OR c.mode IS NOT NULL OR c.size IS NOT NULL OR
				c.atime_sec IS NOT NULL OR c.atime_nsec IS NOT NULL OR c.mtime_sec IS NOT NULL OR
				c.mtime_nsec IS NOT NULL OR c.content IS NOT NULL)) OR
			(c.kind != ? AND (c.node IS NULL OR c.mode IS NULL OR c.size IS NULL OR
				c.atime_sec IS NULL OR c.atime_nsec IS NULL OR c.mtime_sec IS NULL OR
				c.mtime_nsec IS NULL)) OR
			(c.node IS NOT NULL AND (
				c.node <= 0 OR c.mode < 0 OR c.mode > ? OR c.size < 0 OR
				c.atime_nsec < 0 OR c.atime_nsec >= 1000000000 OR
				c.mtime_nsec < 0 OR c.mtime_nsec >= 1000000000 OR
				(c.mode & ?) NOT IN (0, ?) OR
				((c.mode & ?) = ? AND (c.size != 0 OR c.content IS NOT NULL)) OR
				((c.mode & ?) = 0 AND c.content IS NULL AND c.size != 0) OR
				(c.content IS NOT NULL AND c.content = '')
			)) OR
			(current_node.id IS NOT NULL AND current_node.namespace != c.namespace) OR
			(current_parent.id IS NOT NULL AND c.parent != 0 AND current_parent.namespace != c.namespace) OR
			(current_from_parent.id IS NOT NULL AND current_from_parent.namespace != c.namespace) OR
			c.recorded_nsec < 0 OR c.recorded_nsec >= 1000000000
			`+predecessorPredicate+`
		)`, changeArgs...).Scan(&invalidChanges); err != nil {
		return err
	}
	if invalidLogs != 0 || invalidChanges != 0 {
		return fmt.Errorf("the database holds %d invalid namespace logs and %d invalid change rows: %w",
			invalidLogs, invalidChanges, syscall.EIO)
	}
	return nil
}

const pendingObjectStatusQuery = `
	SELECT
		count(CASE WHEN state = ? THEN 1 END),
		coalesce(sum(CASE WHEN state = ? THEN size ELSE 0 END), 0),
		count(CASE WHEN state = ? THEN 1 END),
		coalesce(sum(CASE WHEN state = ? THEN size ELSE 0 END), 0),
		count(CASE WHEN state = ? THEN 1 END),
		coalesce(sum(CASE WHEN state = ? THEN size ELSE 0 END), 0),
		coalesce(sum(CASE WHEN typeof(size) != 'integer' OR size < 0 THEN 1 ELSE 0 END), 0)
	FROM objects INDEXED BY objects_by_state
	WHERE namespace = ? AND state IN (?, ?, ?)`

func readPendingObjectStatus(
	ctx context.Context,
	db objectStatusQueryer,
	namespace int64,
	limits ObjectLimits,
) (ObjectStatus, error) {
	var (
		status      ObjectStatus
		invalidSize int64
	)
	err := db.QueryRowContext(ctx, pendingObjectStatusQuery,
		stateReserved, stateReserved, stateUnresolved, stateUnresolved,
		stateGarbage, stateGarbage,
		namespace, stateReserved, stateUnresolved, stateGarbage).Scan(
		&status.ReservedCount,
		&status.ReservedBytes,
		&status.UnresolvedCount,
		&status.UnresolvedBytes,
		&status.GarbageCount,
		&status.GarbageBytes,
		&invalidSize,
	)
	if err != nil {
		return ObjectStatus{}, fmt.Errorf("reading object maintenance status: %w", failure(err))
	}
	if invalidSize != 0 {
		return ObjectStatus{}, fmt.Errorf("the database holds %d pending objects with an invalid size: %w",
			invalidSize, syscall.EIO)
	}
	status.OverLimit = wouldExceed(limits.MaxPendingObjects,
		status.ReservedCount, status.UnresolvedCount, status.GarbageCount) ||
		wouldExceed(limits.MaxPendingBytes,
			status.ReservedBytes, status.UnresolvedBytes, status.GarbageBytes)
	return status, nil
}

// validateLegacyObjectIntegrity refuses object records written before reservation size and Put
// ownership were recorded exactly. A non-referenced legacy row cannot prove that an object was
// created by this reservation, so carrying it forward into a deletion-authorized state would
// risk deleting unrelated bytes under the same key. Version 1 entry endpoints are checked in
// their native layout, and the normalized relationships are checked again before the migration
// transaction may commit.
func validateLegacyObjectIntegrity(
	ctx context.Context,
	db integrityQueryer,
	version int,
) error {
	var nonReferenced, invalidSizes int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			coalesce(sum(CASE WHEN typeof(state) != 'integer' OR state != ? THEN 1 ELSE 0 END), 0),
			coalesce(sum(CASE WHEN typeof(size) != 'integer' OR size < 0 THEN 1 ELSE 0 END), 0)
		FROM objects`, stateReferenced).Scan(&nonReferenced, &invalidSizes); err != nil {
		return err
	}
	if nonReferenced != 0 {
		return fmt.Errorf(
			"schema version %d holds %d non-referenced object records whose ownership and payload size cannot be proven: %w",
			version, nonReferenced, syscall.EIO)
	}
	if invalidSizes != 0 {
		return fmt.Errorf("schema version %d holds %d referenced objects with an invalid size: %w",
			version, invalidSizes, syscall.EIO)
	}
	if err := validateObjectRelationships(ctx, db, nil); err != nil {
		return err
	}
	return nil
}

// validateNamespaceIntegrity is the full namespace pass run at open and by ObjectStatus.
// The state field is a promise to the sweeper: only an object referenced by exactly one file
// in its own namespace may be protected from deletion, and every other state must be
// unreferenced. Both directions are checked because either half can be damaged while the
// foreign-key constraints are disabled by an external writer.
func validateNamespaceIntegrity(
	ctx context.Context,
	db integrityQueryer,
	namespace int64,
	maxIntegrityRecords, maxIntegrityBytes int64,
) error {
	if err := validateIntegrityWork(ctx, db, &namespace, maxIntegrityRecords); err != nil {
		return err
	}
	if err := validateIntegrityBytes(ctx, db, &namespace, maxIntegrityBytes, schema.Version()); err != nil {
		return err
	}
	if err := validateStorageClasses(ctx, db, &namespace); err != nil {
		return err
	}
	if err := validateIdentityBounds(ctx, db, namespace); err != nil {
		return err
	}
	if err := validateNodeValues(ctx, db, &namespace); err != nil {
		return err
	}
	var invalidStates, invalidSizes int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			coalesce(sum(CASE WHEN typeof(state) != 'integer' OR state NOT IN (?, ?, ?, ?) THEN 1 ELSE 0 END), 0),
			coalesce(sum(CASE WHEN typeof(size) != 'integer' OR size < 0 THEN 1 ELSE 0 END), 0)
		FROM objects
		WHERE namespace = ?`,
		stateReserved, stateReferenced, stateGarbage, stateUnresolved, namespace).Scan(
		&invalidStates, &invalidSizes,
	); err != nil {
		return err
	}
	if invalidStates != 0 || invalidSizes != 0 {
		return fmt.Errorf("the database holds %d objects in an unknown state and %d objects with an invalid size: %w",
			invalidStates, invalidSizes, syscall.EIO)
	}
	if err := validateObjectRelationships(ctx, db, &namespace); err != nil {
		return err
	}
	if err := validateNodeRelationships(ctx, db, &namespace); err != nil {
		return err
	}
	if err := validateUsedAccounting(ctx, db, &namespace); err != nil {
		return err
	}
	return validateLogIntegrity(ctx, db, &namespace)
}

// validateObjectRelationships checks both directions of the node/object relation. A nil
// namespace validates the whole database during a legacy migration; a non-nil namespace keeps
// ordinary reopen and status checks scoped to the Store being served.
func validateObjectRelationships(
	ctx context.Context,
	db integrityQueryer,
	namespace *int64,
) error {
	nodeWhere := ""
	objectWhere := ""
	var scopeArgs []any
	if namespace != nil {
		nodeWhere = "WHERE n.namespace = ?"
		objectWhere = "WHERE o.namespace = ?"
		scopeArgs = []any{*namespace}
	}
	var invalidNodes int64
	nodeArgs := append([]any{int64(fs.ModeType), stateReferenced}, scopeArgs...)
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(CASE
			WHEN n.content IS NULL THEN
				CASE WHEN typeof(n.size) != 'integer' OR n.size != 0 THEN 1 ELSE 0 END
			WHEN typeof(n.content) != 'text'
				OR n.content = ''
				OR typeof(n.mode) != 'integer'
				OR typeof(n.size) != 'integer'
				OR n.size < 0
				OR (n.mode & ?) != 0
				OR o.key IS NULL
				OR o.namespace != n.namespace
				OR o.state != ?
				OR o.size != n.size
			THEN 1
			ELSE 0
		END), 0)
		FROM nodes n
		LEFT JOIN objects o ON o.key = n.content
		`+nodeWhere, nodeArgs...).Scan(&invalidNodes); err != nil {
		return err
	}

	var invalidReferenced, referencedPending int64
	objectArgs := append([]any{
		stateReferenced, stateReserved, stateGarbage, stateUnresolved,
	}, scopeArgs...)
	if err := db.QueryRowContext(ctx, `
		SELECT
			coalesce(sum(CASE
				WHEN state = ? AND (reference_count != 1 OR same_namespace_count != 1)
				THEN 1 ELSE 0 END), 0),
			coalesce(sum(CASE
				WHEN state IN (?, ?, ?) AND reference_count != 0
				THEN 1 ELSE 0 END), 0)
		FROM (
			SELECT
				o.state,
				count(n.id) AS reference_count,
				count(CASE WHEN n.namespace = o.namespace THEN 1 END) AS same_namespace_count
			FROM objects o
			LEFT JOIN nodes n ON n.content = o.key
			`+objectWhere+`
			GROUP BY o.key, o.namespace, o.state
		)`, objectArgs...).Scan(
		&invalidReferenced, &referencedPending,
	); err != nil {
		return err
	}
	if invalidNodes != 0 || invalidReferenced != 0 || referencedPending != 0 {
		return fmt.Errorf(
			"the database holds %d nodes with invalid object relationships, %d referenced objects without exactly one same-namespace file, and %d pending objects still referenced: %w",
			invalidNodes, invalidReferenced, referencedPending, syscall.EIO)
	}
	return nil
}

// validateVersionOneNodeRelationships checks the entry table before migration 0002 replaces
// it. Version 1 has no entry namespace column, so the parent and child nodes are the only proof
// that an entry stays within one namespace. Running this before the rebuild prevents its inner
// join from silently discarding an entry whose endpoint is missing.
func validateVersionOneNodeRelationships(ctx context.Context, db integrityQueryer) error {
	var invalidRoots int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(CASE
			WHEN typeof(ns.root) != 'integer'
				OR root.id IS NULL
				OR root.namespace != ns.id
				OR typeof(root.mode) != 'integer'
				OR (root.mode & ?) = 0
			THEN 1 ELSE 0
		END), 0)
		FROM namespaces ns
		LEFT JOIN nodes root ON root.id = ns.root`, int64(fs.ModeDir)).Scan(&invalidRoots); err != nil {
		return err
	}

	var invalidNodes int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(invalid), 0)
		FROM (
			SELECT CASE
				WHEN ns.id IS NULL THEN 1
				WHEN n.id = ns.root AND count(e.node) != 0 THEN 1
				WHEN n.id != ns.root AND count(e.node) != 1 THEN 1
				ELSE 0
			END AS invalid
			FROM nodes n
			LEFT JOIN namespaces ns ON ns.id = n.namespace
			LEFT JOIN entries e ON e.node = n.id
			GROUP BY n.id, n.namespace, ns.id, ns.root
		)`).Scan(&invalidNodes); err != nil {
		return err
	}

	var invalidEntries int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(CASE
			WHEN parent.id IS NULL
				OR child.id IS NULL
				OR parent.namespace != child.namespace
				OR typeof(parent.mode) != 'integer'
				OR (parent.mode & ?) = 0
			THEN 1 ELSE 0
		END), 0)
		FROM entries e
		LEFT JOIN nodes parent ON parent.id = e.parent
		LEFT JOIN nodes child ON child.id = e.node`, int64(fs.ModeDir)).Scan(&invalidEntries); err != nil {
		return err
	}
	if invalidRoots != 0 || invalidNodes != 0 || invalidEntries != 0 {
		return fmt.Errorf(
			"schema version 1 holds %d invalid namespace roots, %d nodes with invalid entry cardinality, and %d entries with invalid endpoints: %w",
			invalidRoots, invalidNodes, invalidEntries, syscall.EIO)
	}

	var unreachableNodes int64
	if err := db.QueryRowContext(ctx, `
		WITH RECURSIVE reachable(namespace, node) AS (
			SELECT id, root FROM namespaces
			UNION
			SELECT reachable.namespace, e.node
			FROM reachable
			JOIN entries e ON e.parent = reachable.node
			JOIN nodes child
				ON child.id = e.node
				AND child.namespace = reachable.namespace
		)
		SELECT count(*)
		FROM nodes n
		LEFT JOIN reachable
			ON reachable.namespace = n.namespace
			AND reachable.node = n.id
		WHERE reachable.node IS NULL`).Scan(&unreachableNodes); err != nil {
		return err
	}
	if unreachableNodes != 0 {
		return fmt.Errorf("schema version 1 holds %d nodes outside their namespace root's tree: %w",
			unreachableNodes, syscall.EIO)
	}
	return nil
}

// validateNodeRelationships checks the entry cardinality that makes nodes a tree. The root has
// no name, every other node has exactly one, and an entry belongs to the same namespace as both
// nodes it connects. A nil namespace validates every namespace before a legacy migration.
func validateNodeRelationships(
	ctx context.Context,
	db integrityQueryer,
	namespace *int64,
) error {
	namespaceWhere := ""
	nodeWhere := ""
	entryWhere := ""
	var scopeArgs []any
	if namespace != nil {
		namespaceWhere = "WHERE ns.id = ?"
		nodeWhere = "WHERE n.namespace = ?"
		entryWhere = "WHERE e.namespace = ? OR parent.namespace = ? OR child.namespace = ?"
		scopeArgs = []any{*namespace}
	}

	rootArgs := append([]any{int64(fs.ModeDir)}, scopeArgs...)
	var invalidRoots int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(CASE
			WHEN typeof(ns.root) != 'integer'
				OR root.id IS NULL
				OR root.namespace != ns.id
				OR typeof(root.mode) != 'integer'
				OR (root.mode & ?) = 0
			THEN 1 ELSE 0
		END), 0)
		FROM namespaces ns
		LEFT JOIN nodes root ON root.id = ns.root
		`+namespaceWhere, rootArgs...).Scan(&invalidRoots); err != nil {
		return err
	}

	var invalidNodes int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(invalid), 0)
		FROM (
			SELECT CASE
				WHEN ns.id IS NULL THEN 1
				WHEN n.id = ns.root AND count(e.node) != 0 THEN 1
				WHEN n.id != ns.root AND (
					count(e.node) != 1
					OR count(CASE WHEN e.namespace = n.namespace THEN 1 END) != 1
				) THEN 1
				ELSE 0
			END AS invalid
			FROM nodes n
			LEFT JOIN namespaces ns ON ns.id = n.namespace
			LEFT JOIN entries e ON e.node = n.id
			`+nodeWhere+`
			GROUP BY n.id, n.namespace, ns.id, ns.root
		)`, scopeArgs...).Scan(&invalidNodes); err != nil {
		return err
	}

	entryArgs := append([]any{int64(fs.ModeDir)}, scopeArgs...)
	if namespace != nil {
		entryArgs = append(entryArgs, *namespace, *namespace)
	}
	var invalidEntries int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(CASE
			WHEN typeof(e.namespace) != 'integer'
				OR parent.id IS NULL
				OR child.id IS NULL
				OR e.namespace != parent.namespace
				OR e.namespace != child.namespace
				OR typeof(parent.mode) != 'integer'
				OR (parent.mode & ?) = 0
			THEN 1 ELSE 0
		END), 0)
		FROM entries e
		LEFT JOIN nodes parent ON parent.id = e.parent
		LEFT JOIN nodes child ON child.id = e.node
		`+entryWhere, entryArgs...).Scan(&invalidEntries); err != nil {
		return err
	}

	if invalidRoots != 0 || invalidNodes != 0 || invalidEntries != 0 {
		return fmt.Errorf(
			"the database holds %d invalid namespace roots, %d nodes with invalid entry cardinality, and %d entries crossing an invalid relationship: %w",
			invalidRoots, invalidNodes, invalidEntries, syscall.EIO)
	}
	return validateNodeReachability(ctx, db, namespace)
}

// validateNodeReachability proves that the cardinality-checked entry graph is one tree rooted
// at each namespace root. UNION, rather than UNION ALL, admits each stored namespace/node pair
// once, so a disconnected cycle terminates and the query's work is bounded by stored state.
func validateNodeReachability(
	ctx context.Context,
	db integrityQueryer,
	namespace *int64,
) error {
	seedWhere := ""
	nodeWhere := ""
	var args []any
	if namespace != nil {
		seedWhere = "WHERE id = ?"
		nodeWhere = " AND n.namespace = ?"
		args = []any{*namespace, *namespace}
	}

	var unreachableNodes int64
	if err := db.QueryRowContext(ctx, `
		WITH RECURSIVE reachable(namespace, node) AS (
			SELECT id, root FROM namespaces `+seedWhere+`
			UNION
			SELECT reachable.namespace, e.node
			FROM reachable
			JOIN entries e
				ON e.namespace = reachable.namespace
				AND e.parent = reachable.node
		)
		SELECT count(*)
		FROM nodes n
		LEFT JOIN reachable
			ON reachable.namespace = n.namespace
			AND reachable.node = n.id
		WHERE reachable.node IS NULL`+nodeWhere, args...).Scan(&unreachableNodes); err != nil {
		return err
	}
	if unreachableNodes != 0 {
		return fmt.Errorf("the database holds %d nodes outside their namespace root's tree: %w",
			unreachableNodes, syscall.EIO)
	}
	return nil
}

// validateUsedAccounting streams each namespace and its nodes in key order. File sizes are
// added only after checking that the next addition fits in int64, so a corrupt database cannot
// wrap an aggregate into a plausible counter.
func validateUsedAccounting(
	ctx context.Context,
	db integrityQueryer,
	namespace *int64,
) error {
	where := ""
	var args []any
	if namespace != nil {
		where = "WHERE ns.id = ?"
		args = []any{*namespace}
	}
	rows, err := db.QueryContext(ctx, `
		SELECT
			ns.id, ns.used, typeof(ns.used),
			n.id, n.mode, typeof(n.mode), n.size, typeof(n.size)
		FROM namespaces ns
		LEFT JOIN nodes n ON n.namespace = ns.id
		`+where+`
		ORDER BY ns.id, n.id`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		haveNamespace      bool
		currentNamespace   int64
		recordedUsed       int64
		recordedUsedValid  bool
		calculatedUsed     int64
		calculatedOverflow bool
		invalidUsed        int64
		invalidNodeValues  int64
		overflowed         int64
		mismatched         int64
	)
	finishNamespace := func() {
		if !haveNamespace {
			return
		}
		if calculatedOverflow {
			overflowed++
			return
		}
		if recordedUsedValid && recordedUsed != calculatedUsed {
			mismatched++
		}
	}

	for rows.Next() {
		var (
			namespaceID int64
			usedRaw     any
			usedType    string
			nodeID      sql.NullInt64
			modeRaw     any
			modeType    string
			sizeRaw     any
			sizeType    string
		)
		if err := rows.Scan(
			&namespaceID, &usedRaw, &usedType,
			&nodeID, &modeRaw, &modeType, &sizeRaw, &sizeType,
		); err != nil {
			return err
		}
		if !haveNamespace || namespaceID != currentNamespace {
			finishNamespace()
			haveNamespace = true
			currentNamespace = namespaceID
			calculatedUsed = 0
			calculatedOverflow = false
			recordedUsed, recordedUsedValid = storedInteger(usedRaw, usedType)
			if !recordedUsedValid || recordedUsed < 0 {
				invalidUsed++
				recordedUsedValid = false
			}
		}
		if !nodeID.Valid {
			continue
		}
		mode, modeValid := storedInteger(modeRaw, modeType)
		size, sizeValid := storedInteger(sizeRaw, sizeType)
		if !modeValid || mode < 0 || mode > math.MaxUint32 || !sizeValid || size < 0 {
			invalidNodeValues++
			continue
		}
		if fs.FileMode(mode).Type() != 0 || calculatedOverflow {
			continue
		}
		if size > math.MaxInt64-calculatedUsed {
			calculatedOverflow = true
			continue
		}
		calculatedUsed += size
	}
	if err := rows.Err(); err != nil {
		return err
	}
	finishNamespace()
	if invalidUsed != 0 || invalidNodeValues != 0 || overflowed != 0 || mismatched != 0 {
		return fmt.Errorf(
			"the database holds %d namespaces with an invalid used counter, %d nodes with invalid accounting values, %d namespaces whose file sizes overflow, and %d namespaces whose used counter disagrees with their files: %w",
			invalidUsed, invalidNodeValues, overflowed, mismatched, syscall.EIO)
	}
	return nil
}

func storedInteger(value any, storageClass string) (int64, bool) {
	if storageClass != "integer" {
		return 0, false
	}
	integer, ok := value.(int64)
	return integer, ok
}

func wouldExceed(limit int64, values ...int64) bool {
	remaining := limit
	for _, value := range values {
		if value > remaining {
			return true
		}
		remaining -= value
	}
	return false
}

// Garbage returns objects nothing references, oldest recorded second first, so that a caller
// may delete them from the object store. Order within one second is unspecified; keeping the
// indexed prefix order avoids sorting the whole garbage backlog for one bounded batch.
//
// An object a name stopped pointing at and a reservation explicitly abandoned by a caller
// with positive ownership proof are garbage. Age is never ownership proof: a reservation
// may belong to a slow writer in another process, so reserved and unresolved rows are not
// returned.
const garbageQuery = `
	SELECT key FROM objects INDEXED BY objects_by_state
	WHERE namespace = ? AND state = ?
	ORDER BY created_sec
	LIMIT ?`

func (s *Store) Garbage(ctx context.Context, limit int) ([]metastore.Key, error) {
	if limit < 0 {
		return nil, fmt.Errorf("a limit of %d objects is not a count: %w", limit, syscall.EINVAL)
	}
	if err := s.coordinator.beginHealthyRead(); err != nil {
		return nil, err
	}
	defer s.coordinator.endHealthyRead()
	rows, err := s.read.QueryContext(ctx, garbageQuery, s.namespace, stateGarbage, limit)
	if err != nil {
		return nil, fmt.Errorf("collecting objects nothing references: %w", failure(err))
	}
	defer rows.Close()

	keys := []metastore.Key{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("reading an object eligible for collection: %w", failure(err))
		}
		keys = append(keys, metastore.Key(key))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading objects eligible for collection: %w", failure(err))
	}
	return keys, nil
}

// Forget drops the records of objects whose bytes are gone.
//
// Only garbage may be forgotten. Dropping a reserved, unresolved, or referenced record can
// leave a live or unknown object untracked, and the whole call fails before deleting any
// record when a batch contains one.
//
// A key with no record at all is not an error. Forgetting is driven by a sweeper that may
// have been interrupted between deleting the object and recording that it did, so running
// it again has to converge rather than fail.
func (s *Store) Forget(ctx context.Context, keys []metastore.Key) error {
	if len(keys) == 0 {
		return nil
	}
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		for _, key := range keys {
			var state int
			switch err := tx.QueryRowContext(ctx, `SELECT state FROM objects WHERE key = ? AND namespace = ?`,
				string(key), s.namespace).Scan(&state); {
			case errors.Is(err, sql.ErrNoRows):
				continue
			case err != nil:
				return err
			case state == stateReserved || state == stateUnresolved || state == stateReferenced:
				return fmt.Errorf("object %q is in state %d rather than garbage: %w", key, state, syscall.EINVAL)
			case state != stateGarbage:
				return fmt.Errorf("object %q has unknown state %d: %w", key, state, syscall.EIO)
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE key = ? AND namespace = ?`,
				string(key), s.namespace); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("forgetting objects: %w", failure(err))
	}
	return nil
}
