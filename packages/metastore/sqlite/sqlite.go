// Package sqlite implements metastore.Store in a SQLite database.
//
// It is the metastore the object-store namespace is built on: the bytes of every file live
// under an opaque key in a container of blobs, and everything that makes those bytes a
// filesystem — the tree, the modes, the times, the sizes, and which key holds which file's
// contents — lives here. The division and what it buys are described in
// packages/storage/objectstore.
//
// SQLite rather than a server database because a namespace's metadata is small, is read and
// written by one process, and needs transactions more than it needs a network. The driver
// is modernc.org/sqlite, which is a translation of SQLite into Go rather than a binding, so
// this package builds with cgo off and cross-compiles like any other Go code.
//
// # Two pools
//
// SQLite in WAL mode admits one writer and any number of concurrent readers. The two pools
// here are that rule expressed in Go.
//
// The writer pool is held to a single connection and opens its transactions with BEGIN
// IMMEDIATE. Every mutation in this package reads before it writes — the quota check reads
// the counter it is about to move, a rename reads the node it is about to displace — and
// under a deferred BEGIN two such transactions each take a read lock and then find they
// cannot upgrade, which SQLite reports to one of them as a failure it must retry. Taking
// the write lock at the start makes them queue instead.
//
// The reader pool takes no write lock, so a listing never waits behind a write and never
// makes one wait. It matters for Space in particular: a mount answers Statfs under a
// two-second deadline, because df touches every mountpoint on the machine.
package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	"modernc.org/sqlite"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// The modes a node is made with, matching what localdir's Create and Mkdir give a new file
// and a new directory. A namespace held in a database and one held in a directory should
// not disagree about what `touch` and `mkdir` produce.
const (
	fileMode fs.FileMode = 0o644
	dirMode  fs.FileMode = 0o755
)

// busyTimeout is how long a statement waits for a lock another connection holds before
// giving up. Within one process the single writer connection makes contention impossible;
// this covers a second process on the same database, where the wait is bounded by how long
// one transaction here takes, and every one of them is a handful of indexed statements.
const busyTimeout = 5 * time.Second

// Store is one namespace held in a SQLite database.
type Store struct {
	write *sql.DB
	read  *sql.DB

	namespace int64

	// allowance is the namespace's ceiling in bytes, or zero for a namespace that has none.
	// It belongs to the Store rather than to the database because it is a property of how
	// the namespace is being served, and because the contract makes it unchanging for the
	// life of the value: one that answers Space answers always, and one that refuses never
	// starts.
	allowance int64
}

var _ metastore.Store = (*Store)(nil)

// Open holds the namespace called namespace in the SQLite database at database, under an
// allowance of allowance bytes. An allowance of zero is a namespace with none, whose Space
// reports syscall.ENOSYS for as long as the Store exists.
//
// The database is created if it is not there, as is the namespace: a namespace with no
// tree yet is one holding an empty root directory, not an error. Several namespaces may
// share one database, and one Store is bound to exactly one of them.
func Open(ctx context.Context, database, namespace string, allowance int64) (*Store, error) {
	if namespace == "" {
		return nil, fmt.Errorf("a namespace needs a name: %w", syscall.EINVAL)
	}
	if allowance < 0 {
		return nil, fmt.Errorf("an allowance of %d bytes is not a quantity of bytes: %w", allowance, syscall.EINVAL)
	}

	write, err := openPool(database, true)
	if err != nil {
		return nil, err
	}
	// One writer. SQLite allows no more, and saying so here means our own transactions
	// queue on a mutex rather than discovering the lock is taken and unwinding.
	write.SetMaxOpenConns(1)

	read, err := openPool(database, false)
	if err != nil {
		write.Close()
		return nil, err
	}

	id, err := prepare(ctx, write, namespace)
	if err != nil {
		write.Close()
		read.Close()
		return nil, fmt.Errorf("opening namespace %q in %s: %w", namespace, database, err)
	}
	return &Store{write: write, read: read, namespace: id, allowance: allowance}, nil
}

// openPool opens one connection pool over the database file.
//
// The pragmas travel in the DSN rather than being issued once, because a pool opens
// connections as it needs them and a pragma is a property of a connection: one issued after
// Open would hold for whichever connection happened to run it.
//
// WAL is what lets the reader pool read while the writer writes. foreign_keys is off by
// default in SQLite, and the references in the schema are checks that would otherwise be
// decoration.
func openPool(database string, writer bool) (*sql.DB, error) {
	pragmas := url.Values{}
	pragmas.Add("_pragma", "journal_mode(WAL)")
	pragmas.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeout.Milliseconds()))
	pragmas.Add("_pragma", "foreign_keys(1)")
	if writer {
		pragmas.Set("_txlock", "immediate")
	}
	db, err := sql.Open("sqlite", "file:"+database+"?"+pragmas.Encode())
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Close releases both pools. A failure to close either is reported, since an unflushed WAL
// is not something to discover later.
func (s *Store) Close() error {
	return errors.Join(s.write.Close(), s.read.Close())
}

// Space reports the allowance and what is left of it.
//
// Used is read rather than computed: it is one column of one row, kept exact by the
// transactions that move bytes. Avail is what the allowance leaves and never less than
// zero — an allowance lowered underneath content already written leaves Used above Total,
// and a negative Avail arrives in a kernel reply's unsigned field as room no disk holds.
func (s *Store) Space(ctx context.Context) (storage.Space, error) {
	if s.allowance == 0 {
		return storage.Space{}, fmt.Errorf("this namespace is held under no allowance: %w", syscall.ENOSYS)
	}
	var used int64
	if err := s.read.QueryRowContext(ctx, `SELECT used FROM namespaces WHERE id = ?`, s.namespace).Scan(&used); err != nil {
		return storage.Space{}, fmt.Errorf("reading what the namespace holds: %w", failure(err))
	}
	space := storage.Space{Total: s.allowance, Used: used, Avail: max(s.allowance-used, 0)}
	// The counter is exact, so a figure that could not be true of anything is this package
	// having lost count rather than a number to repair. Reporting it anyway would put a
	// fabricated quantity in front of a caller as measured fact (R-ERR-2).
	if !space.Coherent() {
		return storage.Space{}, fmt.Errorf("the namespace is recorded as holding %d bytes of its %d byte allowance, which cannot be true: %w",
			used, s.allowance, syscall.EIO)
	}
	return space, nil
}

// mutate runs f inside a write transaction, committing it if f succeeds and rolling it back
// if it does not.
//
// Every operation that changes anything goes through here, which is what makes each of them
// all-or-nothing. It is also where the tree's invariants are kept: a commit that creates a
// file, charges its bytes and retires the object it displaced either does all three or none
// of them, and no window exists in which a caller could observe the counter disagreeing with
// the tree.
func (s *Store) mutate(ctx context.Context, f func(tx *sql.Tx) error) error {
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return failure(err)
	}
	defer tx.Rollback()

	if err := f(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return failure(err)
	}
	return nil
}

// inspect runs f against the reader pool inside a transaction.
//
// A read transaction rather than bare statements, because resolving a path is one query per
// component: without one, a rename landing between two of them would let a walk descend
// into a tree that never existed in that shape.
func (s *Store) inspect(ctx context.Context, f func(tx *sql.Tx) error) error {
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return failure(err)
	}
	defer tx.Rollback()
	return f(tx)
}

// failure names what a driver-level error means to a caller of this contract.
//
// Anything that is already an errno keeps it — those are ours, decided by the operations in
// this package. Everything else is the database failing to answer, which is EIO: it is not
// a missing file, and reporting it as one would put "that file is not there" in front of a
// caller when the truth is that we could not find out. That is the answer R-ERR-2 forbids
// above every other.
func failure(err error) error {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return err
	}
	return fmt.Errorf("%w: %w", syscall.EIO, err)
}

// isUniqueViolation reports whether err is the database refusing a duplicate name.
//
// Two codes, because the schema raises it through a primary key while a uniqueness
// constraint declared any other way raises the other. Measured against modernc.org/sqlite
// v1.57.0: a duplicate (parent, name) surfaces as *sqlite.Error with code 1555,
// SQLITE_CONSTRAINT_PRIMARYKEY.
func isUniqueViolation(err error) bool {
	var e *sqlite.Error
	if !errors.As(err, &e) {
		return false
	}
	const (
		constraintPrimaryKey = 1555
		constraintUnique     = 2067
	)
	return e.Code() == constraintPrimaryKey || e.Code() == constraintUnique
}

// storedTime splits an instant into the two columns it occupies. Nanosecond is always
// within one second, so the pair is unambiguous and spans every time.Time there is.
func storedTime(t time.Time) (sec int64, nsec int32) {
	return t.Unix(), int32(t.Nanosecond())
}

// loadedTime rebuilds an instant from its two columns.
func loadedTime(sec int64, nsec int32) time.Time {
	return time.Unix(sec, int64(nsec))
}

// newKey mints a key for an object.
//
// Sixteen random bytes rather than a counter or anything derived from the path. The
// contract requires a key nothing can derive from the path and nothing may reuse once the
// object it named is gone; a counter satisfies neither once a database is restored from a
// backup, and a path-derived key would have two writers to one path choose the same key.
func newKey() (metastore.Key, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("%w: minting an object key: %w", syscall.EIO, err)
	}
	return metastore.Key(hex.EncodeToString(raw[:])), nil
}

// pathError wraps err as a failure at a namespace path.
//
// The path is the one the caller named, never a host path and never anything about the
// database file. A caller of this contract knows only namespace paths, and an error naming
// something else describes a filesystem it cannot see.
func pathError(op, path string, err error) error {
	return &os.PathError{Op: op, Path: path, Err: err}
}

// linkError wraps err as a failure of a rename, which has two paths to name.
func linkError(from, to string, err error) error {
	return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
}

// splitPath separates a cleaned path into the directory holding the node and the node's own
// name. The root has no name and never reaches here.
func splitPath(cleaned string) (dir, name string) {
	if i := strings.LastIndexByte(cleaned, '/'); i >= 0 {
		return cleaned[:i], cleaned[i+1:]
	}
	return "", cleaned
}
