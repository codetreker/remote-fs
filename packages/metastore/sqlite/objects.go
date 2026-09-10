package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/schema"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
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
// not pay to upload its bytes first. They are advisory: the volume may change between
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

	key, err := sqlvalue.NewKey()
	if err != nil {
		return "", err
	}
	sec, nsec := sqlvalue.StoredTime(time.Now())
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		// The same questions the commit will ask, asked before the bytes are paid for. The
		// answers are not binding — the volume may change between the two calls, which is
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
		// refused by a volume that has room for the difference.
		var held int64
		if found {
			held = node.Size
		}
		if err := s.roomFor(ctx, tx, size-held); err != nil {
			return err
		}
		return s.reserveObject(ctx, tx, key, size, sec, nsec)
	}); err != nil {
		return "", pathError("reserve", path, sqlerr.Failure(err))
	}
	return key, nil
}

func (s *Store) reserveObject(ctx context.Context, tx *sql.Tx, key metastore.Key, size, sec int64, nsec int32) error {
	// Admission reads only the indexed reserved, unresolved, and garbage ranges. Full
	// validation is performed when the volume opens and when ObjectStatus is requested;
	// putting that scan here would make every write grow with the live volume.
	status, err := readPendingObjectStatus(ctx, tx, s.volume, s.objectLimits)
	if err != nil {
		return err
	}
	if sqlvalue.WouldExceed(s.objectLimits.MaxPendingObjects,
		status.ReservedCount, status.UnresolvedCount, status.GarbageCount, 1) {
		return fmt.Errorf(
			"the pending object backlog holds %d reserved, %d unresolved, and %d garbage objects under its limit of %d: %w",
			status.ReservedCount, status.UnresolvedCount, status.GarbageCount,
			s.objectLimits.MaxPendingObjects, syscall.EAGAIN)
	}
	if sqlvalue.WouldExceed(s.objectLimits.MaxPendingBytes,
		status.ReservedBytes, status.UnresolvedBytes, status.GarbageBytes, size) {
		return fmt.Errorf(
			"the pending object backlog holds %d reserved, %d unresolved, and %d garbage bytes and cannot accept %d more under its limit of %d: %w",
			status.ReservedBytes, status.UnresolvedBytes, status.GarbageBytes, size,
			s.objectLimits.MaxPendingBytes, syscall.EAGAIN)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO objects (key, volume, state, size, digest, created_sec, created_nsec)
		VALUES (?, ?, ?, ?, NULL, ?, ?)`,
		string(key), s.volume, stateReserved, size, sec, nsec)
	return err
}

// Quarantine retires a reservation whose Put did not establish ownership of the key.
func (s *Store) Quarantine(ctx context.Context, key metastore.Key) error {
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		var state int
		switch err := tx.QueryRowContext(ctx,
			`SELECT state FROM objects WHERE key = ? AND volume = ?`,
			string(key), s.volume).Scan(&state); {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return err
		}

		switch state {
		case stateReserved:
			_, err := tx.ExecContext(ctx,
				`UPDATE objects SET state = ? WHERE key = ? AND volume = ?`,
				stateUnresolved, string(key), s.volume)
			return err
		case stateUnresolved:
			return nil
		case stateReferenced, stateGarbage:
			return fmt.Errorf("object %q has already reached state %d: %w", key, state, syscall.EINVAL)
		default:
			return fmt.Errorf("object %q has unknown state %d: %w", key, state, syscall.EIO)
		}
	}); err != nil {
		return fmt.Errorf("quarantining object %q: %w", key, sqlerr.Failure(err))
	}
	return nil
}

// Abandon makes a reservation immediately collectable after its caller has positive proof
// that the object store created the object under its key.
func (s *Store) Abandon(ctx context.Context, key metastore.Key) error {
	if err := s.mutate(ctx, func(tx *sql.Tx) error {
		var state int
		switch err := tx.QueryRowContext(ctx,
			`SELECT state FROM objects WHERE key = ? AND volume = ?`,
			string(key), s.volume).Scan(&state); {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return err
		}

		switch state {
		case stateReserved:
			_, err := tx.ExecContext(ctx,
				`UPDATE objects SET state = ? WHERE key = ? AND volume = ?`,
				stateGarbage, string(key), s.volume)
			return err
		case stateGarbage:
			return nil
		case stateReferenced:
			return fmt.Errorf("object %q is referenced by a file: %w", key, syscall.EINVAL)
		case stateUnresolved:
			return fmt.Errorf("ownership of object %q is unresolved: %w", key, syscall.EINVAL)
		default:
			return fmt.Errorf("object %q has unknown state %d: %w", key, state, syscall.EIO)
		}
	}); err != nil {
		return fmt.Errorf("abandoning object %q: %w", key, sqlerr.Failure(err))
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
	if err := s.mutateVolume(ctx, locking.WriteMutation, []string{cleaned}, func(tx *sql.Tx) error {
		return s.commit(ctx, tx, cleaned, object)
	}); err != nil {
		return pathError("commit", path, sqlerr.Failure(err))
	}
	return nil
}

func (s *Store) commit(ctx context.Context, tx *sql.Tx, cleaned string, object metastore.Object) error {
	// An empty key commits a file with no contents. Zero bytes are not worth an object, so
	// nothing was reserved for them and there is nothing to look up.
	if object.Key != "" {
		// The key must be one we reserved and have not already pointed a name at. A key in any
		// other state — never reserved, already referenced, or swept — is a caller committing
		// something this volume has no record of writing.
		var state int
		switch err := tx.QueryRowContext(ctx, `SELECT state FROM objects WHERE key = ? AND volume = ?`,
			string(object.Key), s.volume).Scan(&state); {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("object %q was never reserved in this volume: %w", object.Key, syscall.EINVAL)
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

	sec, nsec := sqlvalue.StoredTime(object.ModTime)
	if found {
		if err := s.advanceContentRevision(ctx, tx, node.ID); err != nil {
			return err
		}
		// The mode the file already had stands: replacing the contents is not a request to
		// change it, and the access time belongs to whoever last read the file.
		if _, err := tx.ExecContext(ctx,
			`UPDATE nodes SET size = ?, mtime_sec = ?, mtime_nsec = ?, content = ? WHERE id = ?`,
			object.Size, sec, nsec, sqlvalue.StoredKey(object.Key), node.ID); err != nil {
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

// createCommitted makes the file a commit is pointing at when nothing is at the name yet.
// It gets the mode a new file is made with, and the directory holding it records that its
// contents changed.
func (s *Store) createCommitted(ctx context.Context, tx *sql.Tx, parent metastore.Node, name []byte, object metastore.Object) error {
	now := time.Now()
	accessSec, accessNsec := sqlvalue.StoredTime(now)
	sec, nsec := sqlvalue.StoredTime(object.ModTime)
	id, err := dbstate.AllocateNodeID(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO nodes (id, volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, s.volume, int64(fileMode), object.Size, accessSec, accessNsec, sec, nsec, sqlvalue.StoredKey(object.Key)); err != nil {
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

// account moves the volume's byte counter by delta, refusing what the allowance cannot
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
// room: the bytes are not the volume's until they are committed, and a reservation that
// charged would have to be refunded by something — nothing refunds a reservation that is
// never committed, so the counter would drift up by every abandoned write.
//
// A volume with no allowance is never refused, and neither is a change that shrinks one:
// a volume already over its limit would otherwise have no way back under it.
func (s *Store) roomFor(ctx context.Context, tx *sql.Tx, delta int64) error {
	if s.allowance == 0 || delta <= 0 {
		return nil
	}
	used, err := s.used(ctx, tx)
	if err != nil {
		return err
	}
	// Written as a subtraction from the allowance rather than an addition to the count, so
	// that a volume holding close to what a byte count holds cannot wrap the sum into a
	// figure that passes.
	if delta > s.allowance-used {
		return fmt.Errorf("%d more bytes would carry the volume past its allowance of %d bytes, of which %d are taken: %w",
			delta, s.allowance, used, syscall.EDQUOT)
	}
	return nil
}

// charge moves the counter without asking the allowance anything.
func (s *Store) charge(ctx context.Context, tx *sql.Tx, delta int64) error {
	if delta == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE volumes SET used = used + ? WHERE id = ?`, delta, s.volume)
	return err
}

func (s *Store) used(ctx context.Context, tx *sql.Tx) (int64, error) {
	var used int64
	err := tx.QueryRowContext(ctx, `SELECT used FROM volumes WHERE id = ?`, s.volume).Scan(&used)
	return used, err
}

// ObjectStatus reports the object records object-store maintenance owns for this volume.
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

// ObjectStatus returns the current object maintenance status for this Store's volume. It
// validates the volume's object relationships, rooted node tree, and used-byte accounting
// before reporting a successful snapshot; Reserve performs only the indexed pending-record
// validation required on its write path.
func (s *Store) ObjectStatus(ctx context.Context) (ObjectStatus, error) {
	tx, err := s.beginReadSnapshot(ctx, s.read)
	if err != nil {
		return ObjectStatus{}, fmt.Errorf("opening an object status snapshot: %w", err)
	}
	if err := schema.ValidateVolumeIntegrity(
		ctx, tx, s.volume, s.maxIntegrityRecords, s.maxIntegrityBytes,
	); err != nil {
		primary := fmt.Errorf("validating volume integrity: %w", sqlerr.ReadFailure(ctx, err))
		return ObjectStatus{}, finishReadTransaction(ctx, "object status transaction", tx, primary)
	}
	status, err := readPendingObjectStatus(ctx, tx, s.volume, s.objectLimits)
	if err != nil {
		return ObjectStatus{}, finishReadTransaction(ctx, "object status transaction", tx, err)
	}
	if err := finishReadTransaction(ctx, "object status transaction", tx, nil); err != nil {
		return ObjectStatus{}, err
	}
	return status, nil
}

type objectStatusQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
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
	WHERE volume = ? AND state IN (?, ?, ?)`

func readPendingObjectStatus(
	ctx context.Context,
	db objectStatusQueryer,
	volume int64,
	limits ObjectLimits,
) (ObjectStatus, error) {
	var (
		status      ObjectStatus
		invalidSize int64
	)
	err := db.QueryRowContext(ctx, pendingObjectStatusQuery,
		stateReserved, stateReserved, stateUnresolved, stateUnresolved,
		stateGarbage, stateGarbage,
		volume, stateReserved, stateUnresolved, stateGarbage).Scan(
		&status.ReservedCount,
		&status.ReservedBytes,
		&status.UnresolvedCount,
		&status.UnresolvedBytes,
		&status.GarbageCount,
		&status.GarbageBytes,
		&invalidSize,
	)
	if err != nil {
		return ObjectStatus{}, fmt.Errorf("reading object maintenance status: %w", sqlerr.ReadFailure(ctx, err))
	}
	if invalidSize != 0 {
		return ObjectStatus{}, fmt.Errorf("the database holds %d pending objects with an invalid size: %w",
			invalidSize, syscall.EIO)
	}
	status.OverLimit = sqlvalue.WouldExceed(limits.MaxPendingObjects,
		status.ReservedCount, status.UnresolvedCount, status.GarbageCount) ||
		sqlvalue.WouldExceed(limits.MaxPendingBytes,
			status.ReservedBytes, status.UnresolvedBytes, status.GarbageBytes)
	return status, nil
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
	WHERE volume = ? AND state = ?
	ORDER BY created_sec
	LIMIT ?`

func (s *Store) Garbage(ctx context.Context, limit int) ([]metastore.Key, error) {
	if limit < 0 {
		return nil, fmt.Errorf("a limit of %d objects is not a count: %w", limit, syscall.EINVAL)
	}
	if err := s.beginHealthyRead(ctx); err != nil {
		return nil, err
	}
	defer s.coordinator.endHealthyRead()
	rows, err := s.read.QueryContext(ctx, garbageQuery, s.volume, stateGarbage, limit)
	if err != nil {
		return nil, fmt.Errorf("collecting objects nothing references: %w", sqlerr.ReadFailure(ctx, err))
	}
	defer rows.Close()

	keys := []metastore.Key{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("reading an object eligible for collection: %w", sqlerr.ReadFailure(ctx, err))
		}
		keys = append(keys, metastore.Key(key))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading objects eligible for collection: %w", sqlerr.ReadFailure(ctx, err))
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
//
// Admission honors cancellation. An admitted batch owns its transaction through
// finalization so storage shutdown can drain its garbage cleanup.
func (s *Store) Forget(ctx context.Context, keys []metastore.Key) error {
	if len(keys) == 0 {
		return nil
	}
	// Interrupting a write statement can make SQLite roll back the entire transaction.
	// Owning both statements and finalization preserves explicit cleanup observation when
	// storage shutdown cancels maintenance in the middle of an admitted batch.
	// https://www.sqlite.org/c3ref/interrupt.html
	admission := ctx
	ctx = context.WithoutCancel(ctx)
	if err := s.mutateTransaction(admission, ctx, nil, func(tx *sql.Tx) error {
		for _, key := range keys {
			var state int
			switch err := tx.QueryRowContext(ctx, `SELECT state FROM objects WHERE key = ? AND volume = ?`,
				string(key), s.volume).Scan(&state); {
			case errors.Is(err, sql.ErrNoRows):
				continue
			case err != nil:
				return err
			case state == stateReserved || state == stateUnresolved || state == stateReferenced:
				return fmt.Errorf("object %q is in state %d rather than garbage: %w", key, state, syscall.EINVAL)
			case state != stateGarbage:
				return fmt.Errorf("object %q has unknown state %d: %w", key, state, syscall.EIO)
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE key = ? AND volume = ?`,
				string(key), s.volume); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("forgetting objects: %w", sqlerr.Failure(err))
	}
	return nil
}

// The states an object passes through. Reserved is the state a key is in between the
// reservation and the commit. Unresolved retires a reservation whose Put did not prove
// ownership of whatever may be under the key. Garbage is deletion-authorized because a
// name stopped referencing the object or a caller with positive ownership proof abandoned
// it.
//
// The numbers are stored, so they are part of the schema.
const (
	stateReserved   = 0
	stateReferenced = 1
	stateGarbage    = 2
	stateUnresolved = 3
)
