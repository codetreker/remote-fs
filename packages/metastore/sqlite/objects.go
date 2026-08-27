package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
// from one a writer is about to commit, and a sweeper that cannot tell those apart either
// deletes live data or waits out a grace period long enough to make its own correctness a
// guess about how slow a write can be.
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
		node, found, err := lookup(ctx, tx, parent.ID, name)
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

		_, err = tx.ExecContext(ctx, `
			INSERT INTO objects (key, namespace, state, size, digest, created_sec, created_nsec)
			VALUES (?, ?, ?, 0, NULL, ?, ?)`,
			string(key), s.namespace, stateReserved, sec, nsec)
		return err
	}); err != nil {
		return "", pathError("reserve", path, failure(err))
	}
	return key, nil
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
	node, found, err := lookup(ctx, tx, parent.ID, name)
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

	// The size and the digest are recorded now rather than at the reservation, because
	// neither is known until the bytes have been written. A file with no contents reserved
	// nothing, so there is no row to move.
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
	result, err := tx.ExecContext(ctx, `
		INSERT INTO nodes (namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		s.namespace, int64(fileMode), object.Size, accessSec, accessNsec, sec, nsec, storedKey(object.Key))
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

// Garbage returns objects nothing references, oldest first, so that a caller may delete them
// from the object store.
//
// Two kinds come back. An object a name stopped pointing at is garbage from the moment it
// was displaced. A reservation nobody committed is garbage once it is older than grace,
// which is the one place a duration bounds anything here: it says how long a write may take
// between reserving a key and committing it.
//
// Handing a key out and retiring the reservation that held it happen in one change, and
// that is what makes the sweep safe rather than merely usual. A reservation the caller is
// about to delete the bytes of must stop being committable at the moment it is offered: if
// it did not, a write slow enough to be swept could still commit afterwards, and the name
// it committed would point at bytes the sweeper had already deleted. Nothing would report a
// failure — the write would return success and the file would be unreadable from then on.
func (s *Store) Garbage(ctx context.Context, limit int, grace time.Duration) ([]metastore.Key, error) {
	if limit < 0 {
		return nil, fmt.Errorf("a limit of %d objects is not a count: %w", limit, syscall.EINVAL)
	}
	// A negative grace puts the cutoff in the future, which would offer up reservations made
	// moments ago — keys whose bytes a writer is still uploading. Deleting those is the
	// data loss the reservation exists to prevent.
	if grace < 0 {
		return nil, fmt.Errorf("a grace period of %v is not a duration a write may take: %w", grace, syscall.EINVAL)
	}
	cutoffSec, cutoffNsec := storedTime(time.Now().Add(-grace))

	var keys []metastore.Key
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT key FROM objects
			WHERE namespace = ?
			  AND (state = ?
			       OR (state = ? AND (created_sec < ? OR (created_sec = ? AND created_nsec < ?))))
			ORDER BY created_sec, created_nsec
			LIMIT ?`,
			s.namespace, stateGarbage, stateReserved, cutoffSec, cutoffSec, cutoffNsec, limit)
		if err != nil {
			return err
		}
		defer rows.Close()

		keys = []metastore.Key{}
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				return err
			}
			keys = append(keys, metastore.Key(key))
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()

		for _, key := range keys {
			if _, err := tx.ExecContext(ctx, `UPDATE objects SET state = ? WHERE key = ? AND namespace = ?`,
				stateGarbage, string(key), s.namespace); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("collecting objects nothing references: %w", failure(err))
	}
	return keys, nil
}

// Forget drops the records of objects whose bytes are gone.
//
// A key that is still referenced is EINVAL, and the whole call fails with it: dropping the
// record would leave a name pointing at nothing, and doing it for some of the keys before
// discovering the offending one would leave the caller unable to say what happened.
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
			case state == stateReferenced:
				return fmt.Errorf("object %q is still referenced by a name: %w", key, syscall.EINVAL)
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
