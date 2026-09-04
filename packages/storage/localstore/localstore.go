// Package localstore holds a durable object-store namespace entirely under one local
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
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

const (
	metastoreFilename = "metastore.sqlite"

	// MaxWorkspaceBytes is the largest workspace name the durable root binding can hold.
	MaxWorkspaceBytes = 1024
)

var metastoreAuxiliaryFilenames = [...]string{
	metastoreFilename + "-journal",
	metastoreFilename + "-shm",
	metastoreFilename + "-wal",
}

// Config describes one workspace held beneath Root.
//
// Root must already exist as a private directory. Every ancestor of its pathname remains
// protected from replacement for the Store's lifetime; administrative processes and other
// processes running as the serving user are part of the deployment trust boundary. The
// object store owns its format and lifetime lock, while this package fixes the metadata
// database at metastore.sqlite inside it. Completion binds Root to Workspace permanently;
// a different workspace name is a different store root, not another namespace in this one.
//
// ObjectLimits bounds reserved, unresolved, and garbage object records before new uploads are
// admitted.
// MaxReaderConnections bounds SQLite's ordinary namespace and log reads; zero uses
// sqlite.DefaultMaxReaderConnections.
// MaxSnapshotReaderConnections bounds SQLite connections held by long-lived snapshots; zero
// uses sqlite.DefaultMaxSnapshotReaderConnections.
// MaxIntegrityRecords bounds the retained records examined by SQLite integrity checks; zero
// uses sqlite.DefaultMaxIntegrityRecords.
// The backing filesystem remains the hard physical ceiling for all bytes.
// LocalDisk.MaintenanceReserveBytes keeps deletion and SQLite maintenance possible when that
// ceiling is reached; removal remains available while new object publication is refused.
type Config struct {
	Root                         string
	Workspace                    string
	Quota                        int64
	Window                       sqlite.Window
	ObjectLimits                 sqlite.ObjectLimits
	MaxReaderConnections         int
	MaxSnapshotReaderConnections int
	MaxIntegrityRecords          int64
	LocalDisk                    localdisk.Options
	Maintenance                  objectstore.Options
}

// Status is an operational view collected from every durable part of a Store.
type Status struct {
	Workspace                    string
	Space                        storage.Space
	Objects                      sqlite.ObjectStatus
	ObjectLimits                 sqlite.ObjectLimits
	MaxReaderConnections         int
	MaxSnapshotReaderConnections int
	MaxIntegrityRecords          int64
	LocalDisk                    localdisk.Status
	Maintenance                  objectstore.MaintenanceStatus
}

// Store is one workspace backed by a local object store and its bound SQLite metadata.
type Store struct {
	namespace                    *objectstore.Storage
	meta                         *sqlite.Store
	objects                      *localdisk.Objects
	objectLimits                 sqlite.ObjectLimits
	maxReaderConnections         int
	maxSnapshotReaderConnections int
	maxIntegrityRecords          int64
	workspace                    string
}

var _ storage.Storage = (*Store)(nil)
var _ storage.BoundedStorage = (*Store)(nil)

// Open acquires Root, opens the database bound to that root's durable object-store ID, and
// starts storage maintenance. Every partial failure releases the resources already opened.
func Open(ctx context.Context, config Config) (*Store, error) {
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

	localDiskOptions := config.LocalDisk
	localDiskOptions.CompositeInitialization = true
	objects, err := localdisk.Open(ctx, root, localDiskOptions)
	if err != nil {
		return nil, errors.Join(err, anchor.Close())
	}
	if err := verifyAnchoredRoot(objects, anchor); err != nil {
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	storeID := objects.ID()
	initialization := objects.CompositeInitializationState()
	complete, err := anchor.Completed(storeID, config.Workspace)
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
	if err := anchor.MakeMetastoreFilesPrivate(); err != nil {
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	databaseExists, err := anchor.RequireMetastoreFiles()
	if err != nil {
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	if !complete {
		if err := anchor.BindInitialization(storeID, config.Workspace, databaseExists); err != nil {
			return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
		}
	}
	if complete && !databaseExists {
		return nil, errors.Join(
			fmt.Errorf("the completed local store has no %s and cannot prove its namespace metadata: %w",
				metastoreFilename, syscall.EIO),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	databasePath := filepath.Join(root, metastoreFilename)
	if complete {
		if err := requireBoundDatabase(ctx, databasePath, storeID.String(), config.Workspace); err != nil {
			return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
		}
	} else if !databaseExists {
		if err := anchor.CreatePrivateMetastore(); err != nil {
			return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
		}
	}

	meta, err := sqlite.OpenBoundWithOptions(
		ctx,
		databasePath,
		config.Workspace,
		storeID.String(),
		config.Quota,
		sqliteOptions,
	)
	if err != nil {
		return nil, errors.Join(err, closeFailure("local object store", objects.Close()), anchor.Close())
	}
	if err := verifyAnchoredRoot(objects, anchor); err != nil {
		return nil, errors.Join(
			err,
			closeFailure("metastore", meta.Close()),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	if err := anchor.MakeMetastoreFilesPrivate(); err != nil {
		return nil, errors.Join(
			err,
			closeFailure("metastore", meta.Close()),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	if exists, err := anchor.RequireMetastoreFiles(); err != nil || !exists {
		if err == nil {
			err = fmt.Errorf("SQLite opened without creating %s beneath the locked root: %w",
				metastoreFilename, syscall.EIO)
		}
		return nil, errors.Join(
			err,
			closeFailure("metastore", meta.Close()),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	if err := requireBoundDatabase(ctx, databasePath, storeID.String(), config.Workspace); err != nil {
		return nil, errors.Join(
			err,
			closeFailure("metastore", meta.Close()),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}

	namespace, err := objectstore.NewWithOptions(objects, meta, config.Maintenance)
	if err != nil {
		return nil, errors.Join(
			err,
			closeFailure("metastore", meta.Close()),
			closeFailure("local object store", objects.Close()),
			anchor.Close(),
		)
	}
	if !complete {
		if err := anchor.PublishCompletion(storeID, config.Workspace); err != nil {
			return nil, errors.Join(err, closeFailure("namespace", namespace.Close()), anchor.Close())
		}
	}
	if err := anchor.RemoveInitializationIntent(); err != nil {
		return nil, errors.Join(err, closeFailure("namespace", namespace.Close()), anchor.Close())
	}
	if err := verifyAnchoredRoot(objects, anchor); err != nil {
		return nil, errors.Join(err, closeFailure("namespace", namespace.Close()), anchor.Close())
	}
	if err := anchor.Close(); err != nil {
		return nil, errors.Join(err, closeFailure("namespace", namespace.Close()))
	}
	return &Store{
		namespace: namespace, meta: meta, objects: objects,
		workspace: config.Workspace, objectLimits: sqliteOptions.ObjectLimits,
		maxReaderConnections:         sqliteOptions.MaxReaderConnections,
		maxSnapshotReaderConnections: sqliteOptions.MaxSnapshotReaderConnections,
		maxIntegrityRecords:          sqliteOptions.MaxIntegrityRecords,
	}, nil
}

func (config Config) sqliteOptions() (sqlite.Options, error) {
	return (sqlite.Options{
		Window:                       config.Window,
		ObjectLimits:                 config.ObjectLimits,
		MaxReaderConnections:         config.MaxReaderConnections,
		MaxSnapshotReaderConnections: config.MaxSnapshotReaderConnections,
		MaxIntegrityRecords:          config.MaxIntegrityRecords,
	}).Effective()
}

func validate(config Config) (string, error) {
	if config.Root == "" {
		return "", fmt.Errorf("a local store needs a root directory: %w", syscall.EINVAL)
	}
	if config.Workspace == "" {
		return "", fmt.Errorf("a local store needs a workspace name: %w", syscall.EINVAL)
	}
	if len(config.Workspace) > MaxWorkspaceBytes {
		return "", fmt.Errorf("the local store workspace name is %d bytes; the largest is %d: %w",
			len(config.Workspace), MaxWorkspaceBytes, syscall.ENAMETOOLONG)
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

func requireBoundDatabase(ctx context.Context, path, storeID, workspace string) error {
	database, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open the existing local store binding: %v: %w", err, syscall.EIO)
	}
	var bound string
	queryErr := database.QueryRowContext(ctx,
		`SELECT store_id FROM backing_store WHERE singleton = 1`).Scan(&bound)
	if queryErr != nil {
		closeErr := database.Close()
		return fmt.Errorf("read the existing local store binding: %v: %w",
			errors.Join(queryErr, closeErr), syscall.EIO)
	}
	if bound != storeID {
		return errors.Join(
			fmt.Errorf("the metadata is bound to object store %q, not %q: %w",
				bound, storeID, syscall.EINVAL),
			closeFailure("binding probe", database.Close()),
		)
	}
	var namespaces, matching int
	queryErr = database.QueryRowContext(ctx, `
		SELECT count(*), count(CASE WHEN name = ? THEN 1 END)
		FROM namespaces`, workspace).Scan(&namespaces, &matching)
	closeErr := database.Close()
	if queryErr != nil || closeErr != nil {
		return fmt.Errorf("read the existing local store workspace: %v: %w",
			errors.Join(queryErr, closeErr), syscall.EIO)
	}
	if namespaces != 1 || matching != 1 {
		return fmt.Errorf("the local store database holds %d workspaces, of which %d match %q: %w",
			namespaces, matching, workspace, syscall.EIO)
	}
	return nil
}

func closeFailure(what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("closing the %s after open failed: %w", what, err)
}

func (s *Store) Stat(ctx context.Context, path string) (storage.Attr, error) {
	return s.namespace.Stat(ctx, path)
}

func (s *Store) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	return s.namespace.SetAttr(ctx, path, change)
}

func (s *Store) CheckBounded() error { return s.namespace.CheckBounded() }

func (s *Store) List(ctx context.Context, path string) ([]storage.Entry, error) {
	return s.namespace.List(ctx, path)
}

func (s *Store) ListBounded(ctx context.Context, path string, result *storage.ListResult) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	return s.namespace.ListBounded(ctx, path, result)
}

func (s *Store) Read(ctx context.Context, path string) ([]byte, error) {
	return s.namespace.Read(ctx, path)
}

func (s *Store) ReadBounded(ctx context.Context, path string, maxBytes int64) ([]byte, error) {
	return s.namespace.ReadBounded(ctx, path, maxBytes)
}

func (s *Store) Write(ctx context.Context, path string, content []byte) error {
	return s.namespace.Write(ctx, path, content)
}

func (s *Store) Create(ctx context.Context, path string) error {
	return s.namespace.Create(ctx, path)
}

func (s *Store) Mkdir(ctx context.Context, path string) error {
	return s.namespace.Mkdir(ctx, path)
}

func (s *Store) Remove(ctx context.Context, path string) error {
	return s.namespace.Remove(ctx, path)
}

func (s *Store) RemoveDir(ctx context.Context, path string) error {
	return s.namespace.RemoveDir(ctx, path)
}

func (s *Store) Rename(ctx context.Context, from, to string) error {
	return s.namespace.Rename(ctx, from, to)
}

func (s *Store) Space(ctx context.Context) (storage.Space, error) {
	return s.namespace.Space(ctx)
}

// Sweep removes at most limit unreferenced objects.
func (s *Store) Sweep(ctx context.Context, limit int) (int, error) {
	return s.namespace.Sweep(ctx, limit)
}

// MaintenanceStatus returns the most recent sweep outcome.
func (s *Store) MaintenanceStatus() objectstore.MaintenanceStatus {
	return s.namespace.MaintenanceStatus()
}

// Close stops maintenance, closes SQLite before releasing the local-disk lifetime lock,
// and gives concurrent callers the same result.
func (s *Store) Close() error { return s.namespace.Close() }

// Log returns the durable change log written in the same transaction as namespace edits.
func (s *Store) Log() metastore.Log { return s.meta }

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
	return Status{
			Workspace:                    s.workspace,
			Space:                        space,
			Objects:                      objects,
			ObjectLimits:                 s.objectLimits,
			MaxReaderConnections:         s.maxReaderConnections,
			MaxSnapshotReaderConnections: s.maxSnapshotReaderConnections,
			MaxIntegrityRecords:          s.maxIntegrityRecords,
			LocalDisk:                    localDisk,
			Maintenance:                  s.MaintenanceStatus(),
		}, errors.Join(
			statusFailure("logical space", spaceErr),
			statusFailure("combined space", spaceStatusErr),
			statusFailure("object records", objectsErr),
			statusFailure("local object store", localDiskErr),
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
