// Package localstore holds a durable object-store volume entirely under one local
// directory. File contents are immutable local-disk objects and filesystem metadata is a
// SQLite database bound to that object store's durable identity.
package localstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

const (
	metastoreFilename = "metastore.sqlite"

	// MaxVolumeBytes is the largest volume name the durable root binding can hold.
	MaxVolumeBytes = 1024
)

var metastoreAuxiliaryFilenames = [...]string{
	metastoreFilename + "-journal",
	metastoreFilename + "-shm",
	metastoreFilename + "-wal",
}

// Config describes one volume held beneath Root.
//
// Root must already exist as a private directory. Every ancestor of its pathname remains
// protected from replacement for the Store's lifetime; administrative processes and other
// processes running as the serving user are part of the deployment trust boundary. The
// object store owns its format and lifetime lock, while this package fixes the metadata
// database at metastore.sqlite inside it. Completion binds Root to Volume permanently;
// a different volume name is a different store root, not another volume in this one.
//
// ObjectLimits bounds reserved, unresolved, and garbage object records before new uploads are
// admitted.
// MaxReaderConnections bounds SQLite's ordinary volume and log reads; zero uses
// sqlite.DefaultMaxReaderConnections.
// MaxSnapshotReaderConnections bounds SQLite connections held by long-lived snapshots; zero
// uses sqlite.DefaultMaxSnapshotReaderConnections.
// MaxIntegrityRecords bounds the retained records examined by SQLite integrity checks; zero
// uses sqlite.DefaultMaxIntegrityRecords.
// MaxIntegrityBytes bounds the variable-length names examined by SQLite integrity checks;
// zero uses sqlite.DefaultMaxIntegrityBytes.
// The backing filesystem remains the hard physical ceiling for all bytes.
// LocalDisk.MaintenanceReserveBytes keeps deletion and SQLite maintenance possible when that
// ceiling is reached; removal remains available while new object publication is refused.
// Locks enables the paired file-lease authority. Once initialized, reopening requires Locks;
// its omission cannot disable existing protection. InitializeLocks permits the first durable
// lease binding or completion of its matching intent; missing active evidence fails closed.
type Config struct {
	Root                         string
	Volume                       string
	Quota                        int64
	Window                       sqlite.Window
	ObjectLimits                 sqlite.ObjectLimits
	MaxReaderConnections         int
	MaxSnapshotReaderConnections int
	MaxIntegrityRecords          int64
	MaxIntegrityBytes            int64
	// Retained-file and advisory limits use SQLite defaults when omitted.
	MaxRetainedFiles int
	Advisory         advisory.Config
	LocalDisk        localdisk.Options
	Maintenance      objectstore.Options
	Locks            *locking.Options
	InitializeLocks  bool
}

// Status is an operational view collected from every durable part of a Store.
type Status struct {
	Volume                       string
	Space                        storage.Space
	Objects                      sqlite.ObjectStatus
	ObjectLimits                 sqlite.ObjectLimits
	MaxReaderConnections         int
	MaxSnapshotReaderConnections int
	MaxIntegrityRecords          int64
	MaxIntegrityBytes            int64
	LocalDisk                    localdisk.Status
	Maintenance                  objectstore.MaintenanceStatus
	Checkpoint                   CheckpointStatus
}

// CheckpointStatus reports whether accepted SQLite state still depends on WAL frames and
// whether the bounded checkpoint worker has encountered a failure it has not recovered from.
type CheckpointStatus struct {
	AcceptedGeneration     int64
	CheckpointedGeneration int64
	Pending                bool
	LastError              error
}

// Store is one volume backed by a local object store and its bound SQLite metadata.
type Store struct {
	volume                       *objectstore.Storage
	meta                         *sqlite.Store
	durable                      *durableMetastore
	objects                      *localdisk.Objects
	anchor                       *rootAnchor
	objectLimits                 sqlite.ObjectLimits
	maxReaderConnections         int
	maxSnapshotReaderConnections int
	maxIntegrityRecords          int64
	maxIntegrityBytes            int64
	volumeName                   string
	closeMu                      sync.Mutex
	closeRunning                 *closeAttempt
	volumeCloseAttempted         bool
	closed                       bool
	lastCloseErr                 error
}

var _ storage.Storage = (*Store)(nil)
var _ storage.BoundedStorage = (*Store)(nil)

// Open acquires Root, opens the database bound to that root's durable object-store ID, and
// starts storage maintenance. Partial failures abort unexposed SQLite handles before
// releasing root ownership; a terminal driver-close error retains ownership fail-closed.
func Open(ctx context.Context, config Config) (*Store, error) {
	return open(ctx, config, openHooks{})
}

type openHooks struct {
	afterDurableMetastore func(*durableMetastore) error
	afterVolume           func(*objectstore.Storage, *durableMetastore) error
	openDurable           func() (*sqlite.Store, error)
	retainsOwnership      func(error) bool
}

func open(ctx context.Context, config Config, hooks openHooks) (*Store, error) {
	root, err := validate(config)
	if err != nil {
		return nil, err
	}
	sqliteOptions, err := config.sqliteOptions()
	if err != nil {
		return nil, err
	}
	anchor, err := openRootAnchor(root)
	if err != nil {
		return nil, err
	}
	if config.Locks == nil {
		_, err := unix.Fgetxattr(anchor.fd, "user.remote-fs.lease-state", nil)
		if err == nil {
			return nil, errors.Join(fmt.Errorf("the local store requires its configured lease authority: %w", syscall.EIO), anchor.Close())
		}
		if !errors.Is(err, syscall.ENODATA) && !errors.Is(err, syscall.ENOTSUP) {
			return nil, errors.Join(fmt.Errorf("checking local store lease binding: %w", err), anchor.Close())
		}
	}

	localDiskOptions := config.LocalDisk
	localDiskOptions.CompositeInitialization = true
	objects, err := localdisk.Open(ctx, root, localDiskOptions)
	if err != nil {
		return nil, errors.Join(err, anchor.Close())
	}
	recoveryStart := time.Now()
	if err := verifyAnchoredRoot(objects, anchor); err != nil {
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	storeID := objects.ID()
	initialization := objects.CompositeInitializationState()
	complete, err := anchor.Completed(storeID, config.Volume)
	if err != nil {
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	if objects.NewlyInitialized() && complete {
		return nil, errors.Join(
			fmt.Errorf("a completion marker appeared in a newly initialized object store: %w", syscall.EIO),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	if !complete && initialization == localdisk.NoCompositeInitialization {
		return nil, errors.Join(
			fmt.Errorf("the object store has neither completion nor initialization intent: %w", syscall.EIO),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	intent, err := anchor.InspectInitializationIntent(storeID, config.Volume, complete)
	if err != nil {
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	if !complete && intent == initializationIntentMissing {
		return nil, errors.Join(
			fmt.Errorf("the object store initialization intent disappeared: %w", syscall.EIO),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	if err := anchor.MakeMetastoreFilesPrivate(); err != nil {
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	metastoreRoot, err := anchor.InspectMetastore()
	if err != nil {
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	witness, witnessExists, witnessStageExists, err := anchor.InspectMetastoreWitness(storeID, config.Volume)
	if err != nil {
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	if complete && !witnessExists {
		return nil, errors.Join(
			fmt.Errorf("the completed local store has no metastore witness: %w", syscall.EIO),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	if !complete && intent == initializationIntentBound &&
		metastoreRoot.State == metastoreInitialized && !witnessExists && !witnessStageExists {
		bootstrap, err := schemaLessMetastore(ctx, filepath.Join(root, metastoreFilename))
		if err != nil {
			return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
		}
		if bootstrap {
			metastoreRoot.State = metastoreBootstrap
		}
	}
	if metastoreRoot.State != metastoreInitialized && (witnessExists || witnessStageExists) {
		return nil, errors.Join(
			fmt.Errorf("metastore witness state exists beside an uninitialized metadata database: %w", syscall.EIO),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	if witnessStageExists {
		// The final witness is the acknowledgement boundary. A stage proves that a
		// SQLite commit happened, so it must be considered before an absent database
		// can be classified as bootstrap state, but it is not promoted as authority.
		if err := witness.RemoveInterruptedStage(); err != nil {
			return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
		}
	}
	if !complete && intent == initializationIntentPristine {
		if err := anchor.BindInitialization(
			storeID,
			config.Volume,
			metastoreRoot.State != metastoreMissing,
		); err != nil {
			return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
		}
	}
	if complete && metastoreRoot.State != metastoreInitialized {
		return nil, errors.Join(
			fmt.Errorf("the completed local store does not have an initialized %s and cannot prove its volume metadata: %w",
				metastoreFilename, syscall.EIO),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	databasePath := filepath.Join(root, metastoreFilename)
	if metastoreRoot.State == metastoreInitialized {
		if err := requireBoundDatabase(ctx, databasePath, storeID.String(), config.Volume); err != nil {
			return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
		}
	} else if metastoreRoot.State == metastoreMissing {
		if err := anchor.CreatePrivateMetastore(); err != nil {
			return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
		}
	}
	openMode := sqlite.CreateVolumeIfMissing
	if metastoreRoot.State == metastoreInitialized {
		openMode = sqlite.RequireExistingVolume
	}

	openDurable := hooks.openDurable
	if openDurable == nil {
		openDurable = func() (*sqlite.Store, error) {
			opener := sqlite.OpenBoundDurableWithOptions
			if config.Locks != nil {
				opener = sqlite.OpenBoundDurableLeaseWithOptions
			}
			return opener(
				ctx,
				databasePath,
				config.Volume,
				storeID.String(),
				config.Quota,
				sqliteOptions,
				openMode,
				witness.Startup(metastoreRoot.WALPresent, metastoreRoot.WALNonEmpty),
				witness,
			)
		}
	}
	meta, err := openDurable()
	if err != nil {
		retainsOwnership := hooks.retainsOwnership
		if retainsOwnership == nil {
			retainsOwnership = sqlite.OpenFailureRetainsOwnership
		}
		if retainsOwnership(err) {
			return nil, err
		}
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	durableMeta := newDurableMetastore(meta, witness)
	if config.Locks != nil {
		leaseAnchor, err := sqlite.OpenLeaseAnchor(sqlite.LeaseAnchorConfig{
			Directory: root, Name: ".leases", Identity: storeID.String() + ":" + config.Volume,
			BindingFD: anchor.fd, RecoveryStart: recoveryStart, Initialize: config.InitializeLocks,
		})
		if err != nil {
			return nil, errors.Join(err, cleanupDurableOpen(durableMeta, objects, anchor))
		}
		durableMeta.leaseAnchor = leaseAnchor
		if err := meta.ConfigureLeaseRecovery(ctx, sqlite.LeaseRecoveryConfig{
			Witness: leaseAnchor, RecoveryStart: recoveryStart, StateID: leaseAnchor.StateID(),
			Initialize: leaseAnchor.Initializing(),
		}); err != nil {
			return nil, errors.Join(err, cleanupDurableOpen(durableMeta, objects, anchor))
		}
		if err := leaseAnchor.Complete(); err != nil {
			return nil, errors.Join(err, cleanupDurableOpen(durableMeta, objects, anchor))
		}
		if err := meta.EnableLocks(ctx, *config.Locks); err != nil {
			return nil, errors.Join(err, cleanupDurableOpen(durableMeta, objects, anchor))
		}
	}
	if hooks.afterDurableMetastore != nil {
		if err := hooks.afterDurableMetastore(durableMeta); err != nil {
			return nil, errors.Join(err, cleanupDurableOpen(durableMeta, objects, anchor))
		}
	}
	if err := verifyAnchoredRoot(objects, anchor); err != nil {
		return nil, errors.Join(err, cleanupDurableOpen(durableMeta, objects, anchor))
	}
	if err := anchor.MakeMetastoreFilesPrivate(); err != nil {
		return nil, errors.Join(err, cleanupDurableOpen(durableMeta, objects, anchor))
	}
	if inspected, err := anchor.InspectMetastore(); err != nil || inspected.State != metastoreInitialized {
		if err == nil {
			err = fmt.Errorf("SQLite opened without initializing %s beneath the locked root: %w",
				metastoreFilename, syscall.EIO)
		}
		return nil, errors.Join(err, cleanupDurableOpen(durableMeta, objects, anchor))
	}
	if !witness.Exists() {
		return nil, errors.Join(
			fmt.Errorf("SQLite opened without publishing a metastore witness: %w", syscall.EIO),
			cleanupDurableOpen(durableMeta, objects, anchor),
		)
	}
	if err := requireBoundDatabase(ctx, databasePath, storeID.String(), config.Volume); err != nil {
		return nil, errors.Join(err, cleanupDurableOpen(durableMeta, objects, anchor))
	}

	heldObjects := &durabilityHeldObjects{Objects: objects, durable: durableMeta}
	volume, err := objectstore.NewWithOptions(heldObjects, durableMeta, config.Maintenance)
	if err != nil {
		return nil, errors.Join(err, cleanupDurableOpen(durableMeta, objects, anchor))
	}
	if hooks.afterVolume != nil {
		if err := hooks.afterVolume(volume, durableMeta); err != nil {
			return nil, errors.Join(err, cleanupVolumeOpen(volume, durableMeta, objects, anchor))
		}
	}
	if !complete {
		if err := anchor.PublishCompletion(storeID, config.Volume); err != nil {
			return nil, errors.Join(err, cleanupVolumeOpen(volume, durableMeta, objects, anchor))
		}
	}
	if err := anchor.RemoveInitializationIntent(); err != nil {
		return nil, errors.Join(err, cleanupVolumeOpen(volume, durableMeta, objects, anchor))
	}
	if err := verifyAnchoredRoot(objects, anchor); err != nil {
		return nil, errors.Join(err, cleanupVolumeOpen(volume, durableMeta, objects, anchor))
	}
	return &Store{
		volume: volume, meta: meta, durable: durableMeta, objects: objects, anchor: anchor,
		volumeName: config.Volume, objectLimits: sqliteOptions.ObjectLimits,
		maxReaderConnections:         sqliteOptions.MaxReaderConnections,
		maxSnapshotReaderConnections: sqliteOptions.MaxSnapshotReaderConnections,
		maxIntegrityRecords:          sqliteOptions.MaxIntegrityRecords,
		maxIntegrityBytes:            sqliteOptions.MaxIntegrityBytes,
	}, nil
}

func (config Config) sqliteOptions() (sqlite.Options, error) {
	return (sqlite.Options{
		MaxRetainedFiles:             config.MaxRetainedFiles,
		Advisory:                     config.Advisory,
		Window:                       config.Window,
		ObjectLimits:                 config.ObjectLimits,
		MaxReaderConnections:         config.MaxReaderConnections,
		MaxSnapshotReaderConnections: config.MaxSnapshotReaderConnections,
		MaxIntegrityRecords:          config.MaxIntegrityRecords,
		MaxIntegrityBytes:            config.MaxIntegrityBytes,
	}).Effective()
}

func validate(config Config) (string, error) {
	if config.InitializeLocks && config.Locks == nil {
		return "", fmt.Errorf("lease initialization requires lock options: %w", syscall.EINVAL)
	}
	if config.Locks != nil {
		if err := config.Locks.Validate(); err != nil {
			return "", err
		}
	}
	if config.Root == "" {
		return "", fmt.Errorf("a local store needs a root directory: %w", syscall.EINVAL)
	}
	if config.Volume == "" {
		return "", fmt.Errorf("a local store needs a volume name: %w", syscall.EINVAL)
	}
	if len(config.Volume) > MaxVolumeBytes {
		return "", fmt.Errorf("the local store volume name is %d bytes; the largest is %d: %w",
			len(config.Volume), MaxVolumeBytes, syscall.ENAMETOOLONG)
	}
	if config.Quota < limited.MinLimit {
		return "", fmt.Errorf("a local store quota of %d bytes is below the minimum of %d: %w",
			config.Quota, limited.MinLimit, syscall.EINVAL)
	}
	if config.Window.Floor < 1 {
		return "", fmt.Errorf("a local store log floor of %d entries is not positive: %w",
			config.Window.Floor, syscall.EINVAL)
	}
	if config.Window.Cap < config.Window.Floor {
		return "", fmt.Errorf("a local store log cap of %d entries is below its floor of %d: %w",
			config.Window.Cap, config.Window.Floor, syscall.EINVAL)
	}
	if config.Window.Age <= 0 {
		return "", fmt.Errorf("a local store log age of %v is not positive: %w",
			config.Window.Age, syscall.EINVAL)
	}
	if config.Maintenance.SweepInterval <= 0 {
		return "", fmt.Errorf("the local store sweep interval %v is not positive: %w",
			config.Maintenance.SweepInterval, syscall.EINVAL)
	}
	if config.Maintenance.SweepBatch <= 0 {
		return "", fmt.Errorf("the local store sweep batch %d is not positive: %w",
			config.Maintenance.SweepBatch, syscall.EINVAL)
	}
	if err := config.LocalDisk.Validate(); err != nil {
		return "", err
	}
	sqliteOptions, err := config.sqliteOptions()
	if err != nil {
		return "", err
	}
	maxObjectBytes := config.LocalDisk.MaxObjectBytes
	if maxObjectBytes == 0 {
		maxObjectBytes = localdisk.DefaultMaxObjectBytes
	}
	if sqliteOptions.ObjectLimits.MaxPendingBytes < maxObjectBytes {
		return "", fmt.Errorf("the pending-object byte limit %d is below the local object limit %d: %w",
			sqliteOptions.ObjectLimits.MaxPendingBytes, maxObjectBytes, syscall.EINVAL)
	}
	root, err := filepath.Abs(config.Root)
	if err != nil {
		return "", fmt.Errorf("resolve local store root %q: %w", config.Root, err)
	}
	if strings.ContainsAny(root, "%?#") {
		return "", fmt.Errorf("local store root %q contains a character SQLite interprets in file URIs: %w",
			config.Root, syscall.EINVAL)
	}
	return root, nil
}

func requireBoundDatabase(ctx context.Context, path, storeID, volumeName string) (returned error) {
	database, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open the existing local store binding: %v: %w", err, syscall.EIO)
	}
	defer func() {
		if err := database.Close(); err != nil {
			returned = errors.Join(returned,
				fmt.Errorf("close the existing local store binding: %v: %w", err, syscall.EIO))
		}
	}()
	tx, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin the existing local store binding probe: %v: %w", err, syscall.EIO)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT typeof(singleton),
		       CASE WHEN typeof(singleton) = 'integer' THEN singleton ELSE 0 END,
		       typeof(store_id), length(CAST(store_id AS BLOB))
		FROM backing_store
		LIMIT 2`)
	if err != nil {
		return fmt.Errorf("read the existing local store binding metadata: %v: %w", err, syscall.EIO)
	}
	type bindingMetadata struct {
		singletonClass string
		singleton      int64
		storeClass     string
		storeBytes     int64
	}
	var bindings []bindingMetadata
	for rows.Next() {
		var binding bindingMetadata
		if err := rows.Scan(&binding.singletonClass, &binding.singleton, &binding.storeClass, &binding.storeBytes); err != nil {
			rows.Close()
			return fmt.Errorf("decode the existing local store binding metadata: %v: %w", err, syscall.EIO)
		}
		bindings = append(bindings, binding)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("finish the existing local store binding metadata: %v: %w", err, syscall.EIO)
	}
	if len(bindings) != 1 || bindings[0].singletonClass != "integer" || bindings[0].singleton != 1 ||
		bindings[0].storeClass != "text" || bindings[0].storeBytes != int64(len(storeID)) {
		return fmt.Errorf("the local store binding metadata is not the expected bounded singleton: %w", syscall.EIO)
	}
	var bound string
	if err := tx.QueryRowContext(ctx, `SELECT store_id FROM backing_store WHERE singleton = 1`).Scan(&bound); err != nil {
		return fmt.Errorf("read the existing local store binding: %v: %w", err, syscall.EIO)
	}
	if bound != storeID {
		return fmt.Errorf("the metadata is bound to object store %q, not %q: %w",
			bound, storeID, syscall.EINVAL)
	}

	rows, err = tx.QueryContext(ctx, `
		SELECT typeof(id),
		       CASE WHEN typeof(id) = 'integer' THEN id ELSE 0 END,
		       typeof(name), length(CAST(name AS BLOB))
		FROM volumes
		LIMIT 2`)
	if err != nil {
		return fmt.Errorf("read the existing local store volume metadata: %v: %w", err, syscall.EIO)
	}
	type volumeMetadata struct {
		idClass   string
		id        int64
		nameClass string
		nameBytes int64
	}
	var volumes []volumeMetadata
	for rows.Next() {
		var candidate volumeMetadata
		if err := rows.Scan(&candidate.idClass, &candidate.id, &candidate.nameClass, &candidate.nameBytes); err != nil {
			rows.Close()
			return fmt.Errorf("decode the existing local store volume metadata: %v: %w", err, syscall.EIO)
		}
		volumes = append(volumes, candidate)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("finish the existing local store volume metadata: %v: %w", err, syscall.EIO)
	}
	if len(volumes) != 1 || volumes[0].idClass != "integer" || volumes[0].id < 1 ||
		volumes[0].nameClass != "text" || volumes[0].nameBytes < 1 ||
		volumes[0].nameBytes > MaxVolumeBytes || volumes[0].nameBytes != int64(len(volumeName)) {
		return fmt.Errorf("the local store database does not hold exactly volume %q: %w",
			volumeName, syscall.EIO)
	}
	var name string
	if err := tx.QueryRowContext(ctx, `SELECT name FROM volumes WHERE id = ?`, volumes[0].id).Scan(&name); err != nil {
		return fmt.Errorf("read the existing local store volume: %v: %w", err, syscall.EIO)
	}
	if name != volumeName {
		return fmt.Errorf("the local store database binds volume %q, not %q: %w",
			name, volumeName, syscall.EIO)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finish the existing local store binding probe: %v: %w", err, syscall.EIO)
	}
	return nil
}

func schemaLessMetastore(ctx context.Context, path string) (bool, error) {
	database, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return false, fmt.Errorf("open the interrupted SQLite bootstrap: %v: %w", err, syscall.EIO)
	}
	var present int
	queryErr := database.QueryRowContext(ctx, `SELECT 1 FROM sqlite_schema LIMIT 1`).Scan(&present)
	closeErr := database.Close()
	if errors.Is(queryErr, sql.ErrNoRows) && closeErr == nil {
		return true, nil
	}
	if queryErr != nil || closeErr != nil {
		return false, fmt.Errorf("inspect the interrupted SQLite bootstrap: %v: %w",
			errors.Join(queryErr, closeErr), syscall.EIO)
	}
	return false, nil
}

func closeFailure(what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("closing the %s after open failed: %w", what, err)
}

func cleanupDurableOpen(
	durable *durableMetastore,
	objects *localdisk.Objects,
	anchor *rootAnchor,
) error {
	metaErr := durable.Close()
	if !durable.terminallyClosed() {
		// No Store value will escape an Open failure, so a retryable graceful close
		// has no future caller. Abort closes the unexposed SQLite handles while root
		// ownership is still held; recovery then judges the retained disk evidence.
		metaErr = errors.Join(metaErr, durable.abortUnexposed())
	}
	if !durable.closedSuccessfully() {
		return closeFailure("metastore", metaErr)
	}
	return errors.Join(
		closeFailure("metastore", metaErr),
		closeFailure("local object store", objects.Close()),
		anchor.Close(),
	)
}

func cleanupVolumeOpen(
	volume *objectstore.Storage,
	durable *durableMetastore,
	objects *localdisk.Objects,
	anchor *rootAnchor,
) error {
	volumeErr := volume.Close()
	if !durable.terminallyClosed() {
		volumeErr = errors.Join(volumeErr, durable.abortUnexposed())
	}
	if !durable.closedSuccessfully() {
		return closeFailure("volume", volumeErr)
	}
	return errors.Join(
		closeFailure("volume", volumeErr),
		closeFailure("local object store", objects.Close()),
		anchor.Close(),
	)
}

func (s *Store) Stat(ctx context.Context, path string) (storage.Attr, error) {
	return s.volume.Stat(ctx, path)
}

func (s *Store) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	return s.volume.SetAttr(ctx, path, change)
}

func (s *Store) CheckBounded() error { return s.volume.CheckBounded() }

// CheckPublicationAccounting reports native quota settlement at the publication boundary.
func (s *Store) CheckPublicationAccounting() error { return s.volume.CheckPublicationAccounting() }

func (s *Store) List(ctx context.Context, path string) ([]storage.Entry, error) {
	return s.volume.List(ctx, path)
}

func (s *Store) ListBounded(ctx context.Context, path string, result *storage.ListResult) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	return s.volume.ListBounded(ctx, path, result)
}

func (s *Store) Read(ctx context.Context, path string) ([]byte, error) {
	return s.volume.Read(ctx, path)
}

func (s *Store) ReadBounded(ctx context.Context, path string, maxBytes int64) ([]byte, error) {
	return s.volume.ReadBounded(ctx, path, maxBytes)
}

func (s *Store) Write(ctx context.Context, path string, content []byte) error {
	return s.volume.Write(ctx, path, content)
}

func (s *Store) Create(ctx context.Context, path string) error {
	return s.volume.Create(ctx, path)
}

func (s *Store) Mkdir(ctx context.Context, path string) error {
	return s.volume.Mkdir(ctx, path)
}

func (s *Store) Remove(ctx context.Context, path string) error {
	return s.volume.Remove(ctx, path)
}

func (s *Store) RemoveDir(ctx context.Context, path string) error {
	return s.volume.RemoveDir(ctx, path)
}

func (s *Store) Rename(ctx context.Context, from, to string) error {
	return s.volume.Rename(ctx, from, to)
}

func (s *Store) Space(ctx context.Context) (storage.Space, error) {
	return s.volume.Space(ctx)
}

// Sweep removes at most limit unreferenced objects.
func (s *Store) Sweep(ctx context.Context, limit int) (int, error) {
	return s.volume.Sweep(ctx, limit)
}

// MaintenanceStatus returns the most recent sweep outcome.
func (s *Store) MaintenanceStatus() objectstore.MaintenanceStatus {
	return s.volume.MaintenanceStatus()
}

// Close stops maintenance, closes SQLite before releasing the local-disk lifetime lock,
// and gives concurrent callers the same result.
func (s *Store) Close() error {
	s.closeMu.Lock()
	if s.closed {
		err := s.lastCloseErr
		s.closeMu.Unlock()
		return err
	}
	if s.closeRunning != nil {
		attempt := s.closeRunning
		s.closeMu.Unlock()
		<-attempt.done
		return attempt.err
	}
	attempt := &closeAttempt{done: make(chan struct{})}
	s.closeRunning = attempt
	s.closeMu.Unlock()

	if err := s.volume.CloseFileSessions(); err != nil {
		s.closeMu.Lock()
		s.lastCloseErr = err
		attempt.err = err
		s.closeRunning = nil
		close(attempt.done)
		s.closeMu.Unlock()
		return err
	}
	s.closeMu.Lock()
	firstAttempt := !s.volumeCloseAttempted
	s.volumeCloseAttempted = true
	s.closeMu.Unlock()

	err := error(nil)
	if firstAttempt {
		err = s.volume.Close()
	} else {
		err = s.durable.Close()
	}
	if !s.durable.closedSuccessfully() {
		s.closeMu.Lock()
		s.lastCloseErr = err
		attempt.err = err
		s.closeRunning = nil
		close(attempt.done)
		s.closeMu.Unlock()
		return err
	}
	err = errors.Join(err, s.objects.Close(), s.anchor.Close())
	s.closeMu.Lock()
	s.closed = true
	s.lastCloseErr = err
	attempt.err = err
	s.closeRunning = nil
	close(attempt.done)
	s.closeMu.Unlock()
	return err
}

// Log returns the durable change log written in the same transaction as volume edits.
func (s *Store) Log() metastore.Log { return s.meta }

// LockService returns the authority paired with this volume when Locks was configured.
func (s *Store) LockService() locking.Service { return s.meta.LockService() }

// Status queries every component even when one fails, then returns all failures together.
// The returned fields therefore remain useful for diagnosis without presenting a partial
// snapshot as a successful answer.
func (s *Store) Status(ctx context.Context) (Status, error) {
	space, spaceErr := s.meta.Space(ctx)
	localDisk, localDiskErr := s.objects.Status(ctx)
	spaceStatusErr := error(nil)
	if spaceErr == nil {
		space, spaceStatusErr = clampStatusSpace(space, localDisk, localDiskErr)
	}
	objects, objectsErr := s.meta.ObjectStatus(ctx)
	checkpoint := s.durable.status()
	return Status{
			Volume:                       s.volumeName,
			Space:                        space,
			Objects:                      objects,
			ObjectLimits:                 s.objectLimits,
			MaxReaderConnections:         s.maxReaderConnections,
			MaxSnapshotReaderConnections: s.maxSnapshotReaderConnections,
			MaxIntegrityRecords:          s.maxIntegrityRecords,
			MaxIntegrityBytes:            s.maxIntegrityBytes,
			LocalDisk:                    localDisk,
			Maintenance:                  s.MaintenanceStatus(),
			Checkpoint:                   checkpoint,
		}, errors.Join(
			statusFailure("logical space", spaceErr),
			statusFailure("combined space", spaceStatusErr),
			statusFailure("object records", objectsErr),
			statusFailure("local object store", localDiskErr),
			statusFailure("SQLite checkpoint", checkpoint.LastError),
		)
}

func clampStatusSpace(
	space storage.Space,
	localDisk localdisk.Status,
	localDiskErr error,
) (storage.Space, error) {
	if !space.Coherent() {
		return storage.Space{}, fmt.Errorf(
			"the metastore reports a total of %d bytes with %d used and %d available, which cannot be true of anything: %w",
			space.Total, space.Used, space.Avail, syscall.EIO,
		)
	}
	if localDiskErr != nil {
		return space, nil
	}
	if localDisk.PhysicalAvailable < 0 {
		return storage.Space{}, fmt.Errorf("the local object store reports %d available bytes: %w",
			localDisk.PhysicalAvailable, syscall.EIO)
	}
	space.Avail = min(space.Avail, localDisk.PhysicalAvailable)
	return space, nil
}

func statusFailure(what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("reading local store %s: %w", what, err)
}
