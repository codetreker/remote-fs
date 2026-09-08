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
// this package builds with cgo off. Native lease ownership and recovery use Linux
// filesystem facilities, including xattrs and flock.
//
// # Three pools
//
// SQLite in WAL mode admits one writer and any number of concurrent readers. The three pools
// here separate those roles and keep long-lived snapshots from consuming ordinary read
// capacity.
//
// The writer pool is held to a single connection and opens its transactions with BEGIN
// IMMEDIATE. Every mutation in this package reads before it writes — the quota check reads
// the counter it is about to move, a rename reads the node it is about to displace — and
// under a deferred BEGIN two such transactions each take a read lock and then find they
// cannot upgrade, which SQLite reports to one of them as a failure it must retry. Taking
// the write lock at the start makes them queue instead.
//
// The ordinary reader pool takes no write lock, so a listing never waits behind a write and
// never makes one wait. It matters for Space in particular: a mount answers Statfs under a
// two-second deadline, because df touches every mountpoint on the machine. Snapshots use a
// separate bounded pool because their transactions live for the duration of a remote stream.
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
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"modernc.org/sqlite"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Namespace creation modes are independent of the host process's umask.
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
	write        *sql.DB
	read         *sql.DB
	snapshotRead *sql.DB

	namespace    int64
	databasePath string

	// root is the id of the directory the namespace starts from. It is fixed for the life of
	// the namespace — nothing removes or replaces the root — so it is read once rather than
	// joined for on every path resolution, and it is what tells the one node with no name from
	// a node that has lost the one it had.
	root int64

	// allowance is the namespace's ceiling in bytes, or zero for a namespace that has none.
	// It belongs to the Store rather than to the database because it is a property of how
	// the namespace is being served, and because the contract makes it unchanging for the
	// life of the value: one that answers Space answers always, and one that refuses never
	// starts.
	allowance int64

	// window is how much of the change log this Store keeps. It belongs here for the same
	// reason the allowance does: it says how the namespace is being served rather than what it
	// holds, and two processes serving one database may reasonably differ about it.
	window Window

	// objectLimits bound maintenance state accumulated for this namespace. They belong to the
	// serving Store for the same reason as allowance and window: reopening may choose a tighter
	// bound, observe the existing backlog as over-limit, and recover by sweeping it.
	objectLimits ObjectLimits

	// maxIntegrityRecords bounds retained graph and history rows examined before this Store
	// accepts the namespace or reports a successful integrity-checked result.
	maxIntegrityRecords int64
	maxIntegrityBytes   int64

	coordinator          *databaseCoordinator
	locks                *locking.Authority
	leaseRecovery        *LeaseRecovery
	leaseOwner           *leaseDatabaseFile
	witness              CommitWitness
	closeMu              sync.Mutex
	closed               bool
	closeErr             error
	readClosed           bool
	closePool            func(*sql.DB) error
	releasePersistentWAL func(context.Context, *sql.DB) error
}

var _ metastore.Store = (*Store)(nil)

// Open holds the namespace called namespace in the SQLite database at database, under an
// allowance of allowance bytes and a change log held to window.
//
// database is a native filesystem path; URI parameters and percent escapes are rejected.
//
// An allowance of zero is a namespace with none, whose Space reports syscall.ENOSYS for as
// long as the Store exists. A window is required rather than defaulted, because every number
// in it is a value a log could plausibly be held to and none of them has a zero that means
// "unset"; DefaultWindow is the answer for a caller with no reason of its own.
//
// The database is created if it is not there, as is the namespace: a namespace with no
// tree yet is one holding an empty root directory, not an error. Several namespaces may
// share one database, and one Store is bound to exactly one of them. A database written
// against an older schema is carried forward here; one written against a newer schema is
// refused, because nothing in this build can know what a later version did to the columns it
// addresses. Open serves only an unbound database; a database whose metadata is bound to a
// backing store must be opened through OpenBound with that store's identity. Schema versions 1
// and 2 are carried forward only when every object is referenced by exactly one same-namespace
// file with the same size, every non-root node is reachable from its root through exactly one
// entry while the root has none, and the recorded used-byte count is the overflow-safe sum of
// regular-file sizes. A non-referenced legacy object is refused with syscall.EIO because those
// schemas do not prove which bytes the reservation owns or whether its size is exact.
func Open(ctx context.Context, database, namespace string, allowance int64, window Window) (*Store, error) {
	return OpenWithOptions(ctx, database, namespace, allowance, Options{Window: window})
}

// OpenWithObjectLimits is Open with explicit bounds on reserved, unresolved, and garbage objects. Zero
// fields in limits select the defaults returned by DefaultObjectLimits.
func OpenWithObjectLimits(
	ctx context.Context,
	database, namespace string,
	allowance int64,
	window Window,
	limits ObjectLimits,
) (*Store, error) {
	return OpenWithOptions(ctx, database, namespace, allowance, Options{
		Window:       window,
		ObjectLimits: limits,
	})
}

// OpenWithOptions is Open with explicit serving-resource bounds. Zero object-limit fields,
// reader limits, and integrity-work limit select the defaults returned by DefaultOptions.
func OpenWithOptions(
	ctx context.Context,
	database, namespace string,
	allowance int64,
	options Options,
) (*Store, error) {
	return open(ctx, database, namespace, "", allowance, options)
}

// OpenBound is Open for a database whose metadata names objects in the backing store
// identified by storeID.
//
// The binding belongs to the database, not to one namespace: every namespace in a database
// names objects in the same store. A new or empty database is bound before its first namespace
// is created. Once bound, it can only be reopened with the same storeID; Open cannot bypass
// the binding. A database that already holds an unbound namespace is refused, because there is
// no evidence that its existing object keys belong to the proposed store.
func OpenBound(
	ctx context.Context,
	database, namespace, storeID string,
	allowance int64,
	window Window,
) (*Store, error) {
	return OpenBoundWithOptions(ctx, database, namespace, storeID, allowance, Options{Window: window})
}

// OpenBoundWithObjectLimits is OpenBound with explicit bounds on reserved, unresolved, and
// garbage objects. Zero fields in limits select the defaults returned by DefaultObjectLimits.
func OpenBoundWithObjectLimits(
	ctx context.Context,
	database, namespace, storeID string,
	allowance int64,
	window Window,
	limits ObjectLimits,
) (*Store, error) {
	return OpenBoundWithOptions(ctx, database, namespace, storeID, allowance, Options{
		Window:       window,
		ObjectLimits: limits,
	})
}

// OpenBoundWithOptions is OpenBound with explicit serving-resource bounds. Zero object-limit
// fields, reader limits, and integrity-work limit select the defaults returned by DefaultOptions.
func OpenBoundWithOptions(
	ctx context.Context,
	database, namespace, storeID string,
	allowance int64,
	options Options,
) (*Store, error) {
	if storeID == "" {
		return nil, fmt.Errorf("a backing store needs an identity: %w", syscall.EINVAL)
	}
	return open(ctx, database, namespace, storeID, allowance, options)
}

// OpenBoundDurableWithOptions opens a backing-store-bound namespace with an external commit
// witness. The namespace creation policy is enforced inside the same transaction that binds,
// validates, and prepares the database.
func OpenBoundDurableWithOptions(
	ctx context.Context,
	database, namespace, storeID string,
	allowance int64,
	options Options,
	mode NamespaceOpenMode,
	startup DurableStartup,
	witness CommitWitness,
) (*Store, error) {
	if storeID == "" {
		return nil, fmt.Errorf("a backing store needs an identity: %w", syscall.EINVAL)
	}
	durable := &durableOpen{mode: mode, startup: startup, witness: witness}
	if err := durable.check(); err != nil {
		return nil, err
	}
	return openConfiguredWithHooks(ctx, database, namespace, storeID, allowance, options, durable, storeOpenHooks{
		openPool:          openPool,
		openDurableWriter: openPersistentWriterPool,
		acquireLeaseOwner: acquireLeaseDatabase,
		prepare:           prepare,
		closePool: func(db *sql.DB) error {
			return db.Close()
		},
	})
}

func open(
	ctx context.Context,
	database, namespace, storeID string,
	allowance int64,
	options Options,
) (*Store, error) {
	return openWithHooks(ctx, database, namespace, storeID, allowance, options, storeOpenHooks{
		openPool:          openPool,
		prepare:           prepare,
		acquireLeaseOwner: acquireLeaseDatabase,
		closePool: func(db *sql.DB) error {
			return db.Close()
		},
	})
}

type storeOpenHooks struct {
	acquireLeaseOwner func(string, bool, bool) (*leaseDatabaseFile, error)
	openPool          func(context.Context, string, bool, int) (*sql.DB, error)
	openDurableWriter func(context.Context, string, int) (*sql.DB, error)
	prepare           func(context.Context, *sql.DB, string, string, Window, int64, int64) (int64, int64, error)
	closePool         func(*sql.DB) error
}

func openWithHooks(
	ctx context.Context,
	database, namespace, storeID string,
	allowance int64,
	options Options,
	hooks storeOpenHooks,
) (*Store, error) {
	return openConfiguredWithHooks(ctx, database, namespace, storeID, allowance, options, nil, hooks)
}

func openConfiguredWithHooks(
	ctx context.Context,
	database, namespace, storeID string,
	allowance int64,
	options Options,
	durable *durableOpen,
	hooks storeOpenHooks,
) (opened *Store, returnErr error) {
	if namespace == "" {
		return nil, fmt.Errorf("a namespace needs a name: %w", syscall.EINVAL)
	}
	if allowance < 0 {
		return nil, fmt.Errorf("an allowance of %d bytes is not a quantity of bytes: %w", allowance, syscall.EINVAL)
	}
	options, err := options.Effective()
	if err != nil {
		return nil, err
	}
	database, err = filepath.Abs(database)
	if err != nil {
		return nil, err
	}
	if err := validateNativeLeaseOpening(database, options.leaseRecoveryOwner); err != nil {
		return nil, err
	}
	coordinator, err := acquireCoordinator(database, durable != nil || options.leaseRecoveryOwner)
	if err != nil {
		return nil, err
	}
	releaseOnFailure := true
	owner := options.leaseOwner
	acquiredHere := owner == nil
	defer func() {
		if releaseOnFailure {
			if acquiredHere && owner != nil {
				if err := owner.Close(); err != nil {
					returnErr = errors.Join(returnErr, fmt.Errorf("closing native metadata ownership after open failed: %w", &durabilityFailure{err: err}))
				}
			}
			releaseCoordinator(coordinator)
		}
	}()
	if owner != nil {
		if !options.leaseRecoveryOwner || !owner.exclusive || owner.path != database {
			return nil, fmt.Errorf("borrowed native metadata ownership does not match this open: %w", syscall.EINVAL)
		}
		if err := verifyLeaseDatabase(owner); err != nil {
			return nil, err
		}
	} else if hooks.acquireLeaseOwner != nil {
		create := durable == nil || durable.mode != RequireExistingNamespace
		owner, err = hooks.acquireLeaseOwner(database, options.leaseRecoveryOwner, create)
		if err != nil {
			if OpenFailureRetainsOwnership(err) {
				releaseOnFailure = false
			}
			return nil, err
		}
	}
	if err := validateNativeLeaseOpening(database, options.leaseRecoveryOwner); err != nil {
		return nil, err
	}
	cleanup := func(primary error, pools ...openPoolHandle) error {
		cleanupErr := closeOpenPools(hooks.closePool, pools...)
		if cleanupErr != nil {
			releaseOnFailure = false
		}
		return errors.Join(primary, cleanupErr)
	}

	var write *sql.DB
	if durable != nil && hooks.openDurableWriter != nil {
		write, err = hooks.openDurableWriter(ctx, database, 1)
	} else {
		write, err = hooks.openPool(ctx, database, true, 1)
	}
	if err != nil {
		if OpenFailureRetainsOwnership(err) {
			releaseOnFailure = false
		}
		return nil, err
	}

	read, err := hooks.openPool(ctx, database, false, options.MaxReaderConnections)
	if err != nil {
		return nil, cleanup(err, openPoolHandle{"writer pool", write})
	}
	snapshotRead, err := hooks.openPool(ctx, database, false, options.MaxSnapshotReaderConnections)
	if err != nil {
		return nil, cleanup(err,
			openPoolHandle{"reader pool", read},
			openPoolHandle{"writer pool", write},
		)
	}

	if err := coordinator.commit.acquire(ctx); err != nil {
		return nil, cleanup(err,
			openPoolHandle{"snapshot reader pool", snapshotRead},
			openPoolHandle{"reader pool", read},
			openPoolHandle{"writer pool", write},
		)
	}
	if err := coordinator.healthy(); err != nil {
		coordinator.commit.release()
		return nil, cleanup(err,
			openPoolHandle{"snapshot reader pool", snapshotRead},
			openPoolHandle{"reader pool", read},
			openPoolHandle{"writer pool", write},
		)
	}
	var id, root int64
	var state DurableState
	if durable == nil {
		prepareNamespace := hooks.prepare
		if options.requireExistingNamespace {
			prepareNamespace = prepareExistingLeaseNamespace
		}
		id, root, err = prepareNamespace(
			ctx, write, namespace, storeID, options.Window,
			options.MaxIntegrityRecords, options.MaxIntegrityBytes,
		)
		if isUncertainCommit(err) {
			coordinator.poisonWith(err)
		}
	} else {
		id, root, state, err = prepareConfigured(
			ctx, write, namespace, storeID, options.Window,
			options.MaxIntegrityRecords, options.MaxIntegrityBytes, durable,
		)
		if isUncertainCommit(err) {
			coordinator.poisonWith(err)
		}
		if err == nil {
			if witnessErr := durable.witness.Accept(state); witnessErr != nil {
				coordinator.poisonWith(fmt.Errorf("publishing accepted SQLite state at generation %d: %w",
					state.Generation, witnessErr))
				err = coordinator.healthy()
			}
		}
	}
	if err == nil {
		err = validateLeaseOpening(ctx, write, options.leaseRecoveryOwner)
	}
	coordinator.commit.release()
	if err != nil {
		primary := fmt.Errorf("opening namespace %q in %s: %w", namespace, database, failure(err))
		return nil, cleanup(primary,
			openPoolHandle{"snapshot reader pool", snapshotRead},
			openPoolHandle{"reader pool", read},
			openPoolHandle{"writer pool", write},
		)
	}
	store := &Store{
		write: write, read: read, snapshotRead: snapshotRead,
		namespace: id, root: root,
		databasePath: database,
		leaseOwner:   owner,
		allowance:    allowance, window: options.Window, objectLimits: options.ObjectLimits,
		maxIntegrityRecords: options.MaxIntegrityRecords,
		maxIntegrityBytes:   options.MaxIntegrityBytes,
		coordinator:         coordinator,
		closePool:           hooks.closePool,
	}
	if durable != nil {
		store.witness = durable.witness
		store.releasePersistentWAL = disablePersistentWAL
	}
	releaseOnFailure = false
	return store, nil
}

type openPoolHandle struct {
	name string
	db   *sql.DB
}

type openOwnershipRetentionError struct{ err error }

func (e *openOwnershipRetentionError) Error() string { return e.err.Error() }
func (e *openOwnershipRetentionError) Unwrap() error { return e.err }

// OpenFailureRetainsOwnership reports that SQLite could not prove every native handle was
// closed while abandoning an Open operation. The database coordinator remains reserved, so
// callers which own the database path must retain that ownership for the process lifetime.
func OpenFailureRetainsOwnership(err error) bool {
	var retained *openOwnershipRetentionError
	return errors.As(err, &retained)
}

func closeOpenPools(closePool func(*sql.DB) error, pools ...openPoolHandle) error {
	var failures []error
	for _, pool := range pools {
		if err := closePool(pool.db); err != nil {
			failures = append(failures, poolCloseFailure(pool.name, err))
		}
	}
	return errors.Join(failures...)
}

func poolCloseFailure(pool string, err error) error {
	return openCloseFailure("SQLite "+pool, err)
}

func openCloseFailure(what string, err error) error {
	if err == nil {
		return nil
	}
	return &openOwnershipRetentionError{err: fmt.Errorf(
		"closing the %s after open failed; database ownership must be retained: %w", what, err)}
}

func poolCloseError(pool string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("closing the SQLite %s: %w", pool, err)
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
func openPool(ctx context.Context, database string, writer bool, maxConnections int) (*sql.DB, error) {
	return openPoolWith(ctx, database, writer, maxConnections, requireFullSynchronous, func(db *sql.DB) error {
		return db.Close()
	})
}

func openPoolWith(
	ctx context.Context,
	database string,
	writer bool,
	maxConnections int,
	verifySynchronous func(context.Context, *sql.DB) error,
	closePool func(*sql.DB) error,
) (*sql.DB, error) {
	db, err := sql.Open("sqlite", poolDataSource(database, writer))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConnections)
	if writer {
		if err := verifySynchronous(ctx, db); err != nil {
			return nil, errors.Join(err,
				poolCloseFailure("writer pool", closePool(db)))
		}
	}
	return db, nil
}

func poolDataSource(database string, writer bool) string {
	pragmas := url.Values{}
	pragmas.Add("_pragma", "journal_mode(WAL)")
	pragmas.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeout.Milliseconds()))
	pragmas.Add("_pragma", "foreign_keys(1)")
	if writer {
		pragmas.Add("_pragma", "synchronous(FULL)")
		pragmas.Add("_pragma", "wal_autocheckpoint(0)")
		pragmas.Set("_txlock", "immediate")
	}
	return "file:" + database + "?" + pragmas.Encode()
}

// requireFullSynchronous confirms that the connection setting the writer DSN requests is the
// setting SQLite actually applied. FULL is numeric level 2 in SQLite's PRAGMA result.
func requireFullSynchronous(ctx context.Context, db *sql.DB) error {
	const full = 2
	var effective int
	if err := db.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&effective); err != nil {
		return fmt.Errorf("reading the SQLite writer's synchronous setting: %w", failure(err))
	}
	if effective != full {
		return fmt.Errorf("the SQLite writer uses synchronous level %d, want FULL (%d): %w",
			effective, full, syscall.EIO)
	}
	return nil
}

// Close retires the lock authority and releases all pools. A witnessed Store stops new
// operations, refuses while a snapshot reader remains, checkpoints every WAL frame, and
// publishes that checkpoint. EBUSY or a checkpoint-witness failure leaves the writer open
// so Close can be retried without losing the WAL evidence the external witness requires.
// A preserved authority fence remains an error even when SQL pools close successfully;
// external lifetime ownership can be released only after a nil result.
func (s *Store) Close() error {
	return s.CloseContext(context.Background())
}

// Terminal reports that database/sql cannot retry pool closure. It does not prove that a
// driver handle was released after a close error and never authorizes releasing external
// lifetime ownership; only a nil CloseContext result does that.
func (s *Store) Terminal() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	return s.closed
}

// CloseContext is Close with a deadline for waiting on the commit gate and completing the
// witnessed checkpoint. Lock authority retirement and admitted operation drain precede
// that wait. A canceled attempt leaves the writer and persistent WAL open so a later call
// can retry cleanup; it does not reactivate a retired authority.
func (s *Store) CloseContext(ctx context.Context) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return s.closeErr
	}
	var lockErr error
	if s.locks != nil {
		lockErr = s.locks.Close()
		if lockErr != nil && s.witness != nil {
			return lockErr
		}
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return errors.Join(lockErr, err)
	}
	defer s.coordinator.commit.release()
	if s.witness != nil {
		s.coordinator.health.Lock()
		if s.coordinator.poison != nil {
			err := s.coordinator.healthErrorLocked()
			s.coordinator.health.Unlock()
			return fmt.Errorf("closing a poisoned witnessed SQLite database: %w", err)
		}
		s.coordinator.closing = true
		s.coordinator.health.Unlock()
		if inUse := s.read.Stats().InUse + s.snapshotRead.Stats().InUse; inUse != 0 {
			return fmt.Errorf("the SQLite database still has %d active readers at close: %w", inUse, syscall.EBUSY)
		}
		if !s.readClosed {
			if err := errors.Join(
				poolCloseError("reader pool", s.closePool(s.read)),
				poolCloseError("snapshot reader pool", s.closePool(s.snapshotRead)),
			); err != nil {
				s.closeErr = err
				s.closed = true
				return s.closeErr
			}
			s.readClosed = true
		}
		result, err := s.checkpointLocked(ctx, FullCheckpoint)
		if err != nil {
			return fmt.Errorf("checkpointing the witnessed SQLite database before close: %w", err)
		}
		if !result.Complete {
			return fmt.Errorf("SQLite copied %d of %d WAL frames before close: %w",
				result.CheckpointedFrames, result.LogFrames, syscall.EBUSY)
		}
		if err := s.releasePersistentWAL(ctx, s.write); err != nil {
			return fmt.Errorf("releasing persistent SQLite WAL after witnessed checkpoint: %w", err)
		}
	} else {
		// Metadata cleanup can fail after authority retirement while Close waits for the gate.
		// Recheck after that drain before a successful pool close releases native ownership.
		s.coordinator.health.RLock()
		if s.coordinator.poison != nil {
			lockErr = errors.Join(lockErr, s.coordinator.healthErrorLocked())
		}
		s.coordinator.health.RUnlock()
	}
	if s.witness != nil {
		s.closeErr = poolCloseError("writer pool", s.closePool(s.write))
	} else {
		s.closeErr = errors.Join(
			poolCloseError("writer pool", s.closePool(s.write)),
			poolCloseError("reader pool", s.closePool(s.read)),
			poolCloseError("snapshot reader pool", s.closePool(s.snapshotRead)),
		)
	}
	poolErr := s.closeErr
	s.closeErr = s.finishPoolClosure(poolErr, lockErr)
	s.closed = true
	return s.closeErr
}

// Abort terminally closes a witnessed Store which will not be exposed to a caller. It omits
// checkpoint publication and relies on the persistent WAL configured by durable open, so the
// next open either reconciles that evidence or fails closed. Callers must first stop every
// operation and retain external ownership until Abort returns.
func (s *Store) Abort() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return s.closeErr
	}
	var lockErr error
	if s.locks != nil {
		lockErr = s.locks.Close()
	}
	if err := s.coordinator.commit.acquire(context.Background()); err != nil {
		return errors.Join(lockErr, err)
	}
	defer s.coordinator.commit.release()
	s.coordinator.health.Lock()
	s.coordinator.closing = true
	s.coordinator.health.Unlock()
	poolErr := errors.Join(
		poolCloseError("reader pool", s.closePool(s.read)),
		poolCloseError("snapshot reader pool", s.closePool(s.snapshotRead)),
		poolCloseError("writer pool", s.closePool(s.write)),
	)
	s.closeErr = s.finishPoolClosure(poolErr, lockErr)
	s.readClosed = true
	s.closed = true
	return s.closeErr
}

func (s *Store) finishPoolClosure(poolErr, authorityErr error) error {
	var ownerErr error
	if poolErr == nil && s.leaseOwner != nil && (!s.leaseOwner.exclusive || authorityErr == nil) {
		if err := s.leaseOwner.Close(); err != nil {
			ownerErr = fmt.Errorf("closing native metadata ownership: %w", &durabilityFailure{err: err})
		}
	}
	if poolErr == nil {
		releaseCoordinator(s.coordinator)
	}
	return errors.Join(authorityErr, poolErr, ownerErr)
}

// Space reports the allowance and what is left of it.
//
// Used is read rather than computed: it is one column of one row, kept exact by the
// transactions that move bytes. Avail is what the allowance leaves and never less than
// zero — an allowance lowered underneath content already written leaves Used above Total,
// and a negative Avail arrives in a kernel reply's unsigned field as room no disk holds.
func (s *Store) Space(ctx context.Context) (storage.Space, error) {
	if err := s.beginHealthyRead(ctx); err != nil {
		return storage.Space{}, err
	}
	defer s.coordinator.endHealthyRead()
	if s.allowance == 0 {
		return storage.Space{}, fmt.Errorf("this namespace is held under no allowance: %w", syscall.ENOSYS)
	}
	var used int64
	if err := s.read.QueryRowContext(ctx, `SELECT used FROM namespaces WHERE id = ?`, s.namespace).Scan(&used); err != nil {
		return storage.Space{}, fmt.Errorf("reading what the namespace holds: %w", readFailure(ctx, err))
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
//
// The trim rides here rather than beside each append. A write transaction is the only moment
// this package is guaranteed to have one to ride on, and running it on every one of them
// rather than only on the ones that recorded something is what lets an object sweep keep an
// otherwise quiet namespace's log inside its age bound. A transaction with nothing to discard
// pays two indexed lookups for the answer.
func (s *Store) mutate(ctx context.Context, f func(tx *sql.Tx) error) error {
	return s.mutatePublication(ctx, nil, f)
}

func (s *Store) mutatePublication(ctx context.Context, intent *namespaceIntent, f func(tx *sql.Tx) error) error {
	return s.mutateTransaction(ctx, ctx, intent, f)
}

func (s *Store) mutateTransaction(ctx, transactionContext context.Context, intent *namespaceIntent, f func(tx *sql.Tx) error) (returnErr error) {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	if err := s.coordinator.healthy(); err != nil {
		return err
	}
	tx, err := s.write.BeginTx(transactionContext, nil)
	if err != nil {
		return failure(err)
	}
	observationHeld := false
	defer func() {
		returnErr = s.finishMutationTransaction(tx, returnErr, observationHeld)
	}()

	var publication *namespacePublication
	if intent != nil {
		publication, err = s.prepareNamespacePublication(ctx, tx, *intent)
		if err != nil {
			return err
		}
	}
	if err := f(tx); err != nil {
		return err
	}
	if publication != nil {
		if err := s.finishNamespacePublication(ctx, tx, publication); err != nil {
			return err
		}
	}
	if err := trim(ctx, tx, s.namespace, s.window); err != nil {
		return failure(err)
	}
	state, err := advanceGeneration(ctx, tx)
	if err != nil {
		return failure(err)
	}
	s.coordinator.health.Lock()
	observationHeld = true
	if err := s.coordinator.healthErrorLocked(); err != nil {
		return err
	}
	if publication != nil {
		return s.publishNamespace(ctx, tx, state, publication)
	}
	return s.commitPrepared(tx, state)
}

// The commit gate remains held until cleanup finishes. Fresh views wait for rollback
// success or fencing, including when staging failed before publication admission.
func (s *Store) finishMutationTransaction(tx rollbacker, primary error, observationHeld bool) error {
	if !observationHeld {
		s.coordinator.health.Lock()
	}
	defer s.coordinator.health.Unlock()
	rollbackErr := tx.Rollback()
	if rollbackErr == nil || rollbackErr == sql.ErrTxDone {
		return primary
	}
	err := errors.Join(primary, fmt.Errorf("rolling back the SQLite mutation: %w", &durabilityFailure{err: rollbackErr}))
	s.coordinator.poisonLocked(err)
	if s.locks != nil {
		s.locks.Fence(err)
	}
	return err
}

func (s *Store) commitPrepared(tx *sql.Tx, state DurableState) error {
	if err := tx.Commit(); err != nil {
		uncertain := &uncertainCommitError{err: failure(err)}
		s.coordinator.poisonLocked(uncertain)
		if s.locks != nil {
			s.locks.Fence(s.coordinator.healthErrorLocked())
		}
		return s.coordinator.healthErrorLocked()
	}
	err := s.acceptLocked(state)
	if err != nil && s.locks != nil {
		s.locks.Fence(err)
	}
	return err
}

// inspect runs f against the reader pool inside a transaction.
//
// A read transaction rather than bare statements, because resolving a path is one query per
// component: without one, a rename landing between two of them would let a walk descend
// into a tree that never existed in that shape.
func (s *Store) inspect(ctx context.Context, f func(tx *sql.Tx) error) error {
	tx, err := s.beginReadSnapshot(ctx, s.read)
	if err != nil {
		return err
	}
	return finishReadTransaction(ctx, "reader transaction", tx, f(tx))
}

// beginReadSnapshot orders a read transaction before an unresolved commit or after its
// witness publication. The first query pins SQLite's snapshot while the health gate is held;
// the potentially long scan and caller-owned result accounting then proceed without delaying
// a writer's commit boundary.
func (s *Store) beginReadSnapshot(ctx context.Context, pool *sql.DB) (*sql.Tx, error) {
	if err := s.beginHealthyRead(ctx); err != nil {
		return nil, err
	}
	tx, err := pool.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		s.coordinator.endHealthyRead()
		return nil, readFailure(ctx, err)
	}
	var singleton int
	pinErr := tx.QueryRowContext(ctx,
		`SELECT 1 FROM database_state WHERE singleton = 1`,
	).Scan(&singleton)
	s.coordinator.endHealthyRead()
	if pinErr != nil {
		return nil, finishReadTransaction(ctx, "snapshot pin transaction", tx, pinErr)
	}
	return tx, nil
}

type rollbacker interface {
	Rollback() error
}

func finishReadTransaction(ctx context.Context, subject string, tx rollbacker, primary error) error {
	primary = readFailure(ctx, primary)
	rollbackErr := tx.Rollback()
	// database/sql rolls back when the transaction's owning context ends. The direct
	// ErrTxDone then reports completed cleanup, not another database failure.
	// https://github.com/golang/go/blob/e3336a22ad3f0a90bd252c95d8b5544e02674205/src/database/sql/sql.go#L2207-L2230
	if rollbackErr == sql.ErrTxDone && ctx.Err() != nil {
		if primary == nil {
			return failure(ctx.Err())
		}
		return primary
	}
	if rollbackErr != nil {
		rollbackErr = fmt.Errorf("releasing the SQLite %s: %w", subject, &readCleanupFailure{cause: rollbackErr})
	}
	return errors.Join(primary, rollbackErr)
}

type readCleanupFailure struct{ cause error }

func (e *readCleanupFailure) Error() string         { return e.cause.Error() }
func (e *readCleanupFailure) Unwrap() error         { return e.cause }
func (e *readCleanupFailure) Is(target error) bool  { return target == syscall.EIO }
func (e *readCleanupFailure) Classification() error { return syscall.EIO }

// failure preserves cancellation and known operation errors. An unclassified database
// failure or an expired deadline cannot establish a filesystem result and is EIO.
func failure(err error) error {
	if err == nil || storage.ErrnoOf(err) != syscall.EIO || errors.Is(err, syscall.EIO) {
		return err
	}
	return fmt.Errorf("%w: %w", syscall.EIO, err)
}

// SQLite can return SQLITE_INTERRUPT without the driver's ctx.Err substitution. A read
// transaction can also return ErrTxDone when automatic rollback wins after its context
// check. ctx must own the transaction when classifying ErrTxDone. Independent joined
// failures and writes with an uncertain outcome retain their fault classification.
// https://gitlab.com/cznic/sqlite/-/blob/6e86ac4a89e3f36359d1947e36355c469b18430c/rows.go#L107-120
// https://github.com/golang/go/blob/e3336a22ad3f0a90bd252c95d8b5544e02674205/src/database/sql/sql.go#L2245-L2258
func readFailure(ctx context.Context, err error) error {
	if ctx.Err() != nil && (err == sql.ErrTxDone || interruptedRead(err)) {
		return failure(&readCancellationError{cause: err, canceled: ctx.Err()})
	}
	return failure(err)
}

func interruptedRead(err error) bool {
	if err == nil {
		return false
	}
	if _, classified := err.(interface{ Classification() error }); classified {
		return false
	}
	if coded, ok := err.(interface{ Code() int }); ok {
		const sqliteInterrupt = 9
		return coded.Code() == sqliteInterrupt
	}
	if err == context.Canceled || err == syscall.EINTR {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !interruptedRead(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return interruptedRead(wrapped.Unwrap())
	}
	return false
}

type readCancellationError struct {
	cause    error
	canceled error
}

func (e *readCancellationError) Error() string         { return e.cause.Error() }
func (e *readCancellationError) Unwrap() []error       { return []error{e.cause, e.canceled} }
func (e *readCancellationError) Classification() error { return e.canceled }

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
	value, err := randomHex(16)
	if err != nil {
		return "", fmt.Errorf("%w: minting an object key: %w", syscall.EIO, err)
	}
	return metastore.Key(value), nil
}

// randomHex returns n random bytes rendered as hex. The values built on it — an object key
// and a log's incarnation — have the same requirement and it is the only one they have: to
// differ from every other value ever minted, by this process or any other.
func randomHex(n int) (string, error) {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
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
