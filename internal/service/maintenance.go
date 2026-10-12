package service

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
)

// Maintenance retains the service's stable exclusive anchor across offline
// database operations.
type Maintenance struct {
	root   *stateRoot
	store  *management.Store
	native *localstore.Store
	config Config
	mu     sync.Mutex
	closed bool
}

// MaintenanceFailure retains an offline owner's handles when cleanup cannot
// confirm settlement. Close retries that same owner without reacquiring a root.
type MaintenanceFailure struct {
	Maintenance *Maintenance
	Cause       error
}

func (e *MaintenanceFailure) Error() string {
	return "maintenance retains state-root ownership: " + e.Cause.Error()
}
func (e *MaintenanceFailure) Unwrap() error                   { return e.Cause }
func (e *MaintenanceFailure) Close(ctx context.Context) error { return e.Maintenance.Close(ctx) }

func finishMaintenance(ctx context.Context, m *Maintenance, cause error) error {
	if err := m.Close(ctx); err != nil {
		return &MaintenanceFailure{Maintenance: m, Cause: errors.Join(cause, err)}
	}
	return cause
}

func OpenMaintenance(ctx context.Context, config Config) (*Maintenance, error) {
	r, err := openRoot(ctx, config, false)
	if err != nil {
		return nil, err
	}
	m := &Maintenance{root: r, config: config}
	if err := m.openStore(ctx, false); err != nil {
		return nil, finishMaintenance(context.WithoutCancel(ctx), m, err)
	}
	return m, nil
}

func InitState(ctx context.Context, config Config) (returned error) {
	r, err := openRoot(ctx, config, true)
	if err != nil {
		return err
	}
	m := &Maintenance{root: r, config: config}
	defer func() { returned = finishMaintenance(context.WithoutCancel(ctx), m, returned) }()
	if err := m.openStore(ctx, true); err != nil {
		return err
	}
	return r.BindDatabase(m.store.DatabaseID())
}

func (m *Maintenance) openStore(ctx context.Context, create bool) error {
	options := m.config.Management
	options.Create = create
	options.ExpectedDatabaseID = m.root.DatabaseID
	var err error
	m.store, err = management.Open(ctx, m.root.ManagementPath, options)
	if err != nil {
		return err
	}
	return nil
}

func (m *Maintenance) StateRoot() string { return m.root.Path }
func (m *Maintenance) DataRoot() string  { return m.root.DataPath }

func (m *Maintenance) IssueNamespace(ctx context.Context, actor management.ID) (management.ActionNamespace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.store == nil {
		return management.ActionNamespace{}, management.ErrClosed
	}
	return m.store.IssueNamespace(ctx, actor)
}

func InitializeAdmin(ctx context.Context, config Config, login, displayName, password string) (_ management.User, returned error) {
	m, err := OpenMaintenance(ctx, config)
	if err != nil {
		return management.User{}, err
	}
	defer func() { returned = finishMaintenance(context.WithoutCancel(ctx), m, returned) }()
	return m.AddAdmin(ctx, login, displayName, password)
}

func (m *Maintenance) AddAdmin(ctx context.Context, login, displayName, password string) (management.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.store == nil {
		return management.User{}, errors.New("administrator maintenance requires an open catalog")
	}
	hash, err := HashAdminPassword(password)
	if err != nil {
		return management.User{}, err
	}
	return m.store.AddAdmin(ctx, login, displayName, hash)
}

func (m *Maintenance) ResetAdminPassword(ctx context.Context, login, password string) (management.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.store == nil {
		return management.User{}, errors.New("administrator maintenance requires an open catalog")
	}
	credential, err := m.store.AdminCredential(ctx, login)
	if err != nil {
		return management.User{}, err
	}
	hash, err := HashAdminPassword(password)
	if err != nil {
		return management.User{}, err
	}
	return m.store.ResetAdminPassword(ctx, credential.User.ID, hash)
}

func (m *Maintenance) Quiesce(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("maintenance is closed")
	}
	return m.quiesce(ctx)
}

func (m *Maintenance) quiesce(_ context.Context) error {
	if err := m.closeNative(); err != nil {
		return err
	}
	if m.store != nil {
		if err := m.store.Close(); err != nil {
			return fmt.Errorf("close maintenance catalog: %w", err)
		}
		m.store = nil
	}
	return nil
}

func (m *Maintenance) closeNative() error {
	if m.native == nil {
		return nil
	}
	if err := m.native.Close(); err != nil {
		return fmt.Errorf("close maintenance volume: %w", err)
	}
	m.native = nil
	return nil
}

func (m *Maintenance) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	if err := m.quiesce(ctx); err != nil {
		return err
	}
	if err := m.root.Close(); err != nil {
		return err
	}
	m.closed = true
	return nil
}

func (m *Maintenance) Validate(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("maintenance is closed")
	}
	return m.validate(ctx)
}

func (m *Maintenance) validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.closeNative(); err != nil {
		return err
	}
	if err := m.root.validateAnchor(); err != nil {
		return err
	}
	view, err := m.root.dataView()
	if err != nil {
		return err
	}
	same := true
	if m.root.data != nil {
		same, err = sameOpenedFile(m.root.data, view.data)
	}
	err = errors.Join(err, view.Close())
	if err != nil {
		return err
	}
	if !same {
		return fmt.Errorf("data subtree was replaced outside maintenance: %w", management.ErrCorrupt)
	}
	if m.store == nil {
		if err := m.openStore(ctx, false); err != nil {
			return err
		}
	}
	if m.store.DatabaseID() != m.root.DatabaseID {
		return fmt.Errorf("catalog does not match stable root identity: %w", management.ErrCorrupt)
	}
	var after management.ID
	var catalog []management.Volume
	for {
		volumes, err := m.store.ListCatalogVolumes(ctx, after, m.config.Management.MaxPageSize)
		if err != nil {
			return err
		}
		for _, volume := range volumes {
			catalog = append(catalog, volume)
			native, err := m.nativeValidationRequired(ctx, volume)
			if err != nil {
				return err
			}
			if native {
				if err := m.validateNativeVolume(ctx, volume); err != nil {
					return fmt.Errorf("validate volume %s: %w", volume.ID, err)
				}
			}
			after = volume.ID
		}
		if len(volumes) < m.config.Management.MaxPageSize {
			return validateMaintenanceLifecycle(ctx, m, catalog)
		}
	}
}

func (m *Maintenance) nativeValidationRequired(ctx context.Context, volume management.Volume) (bool, error) {
	if volume.State == management.VolumeActive {
		return true, nil
	}
	if volume.State == management.VolumeDeleted {
		return false, nil
	}
	op, err := m.store.Operation(ctx, volume.OwnerUserID, volume.LastOperationID)
	if err != nil {
		return false, err
	}
	return (op.Kind == management.OperationCreate && (op.Phase == management.PhaseBound || op.Phase == management.PhaseCompleted)) || (op.Kind == management.OperationDelete && op.Phase == management.PhaseFrozen), nil
}

func (m *Maintenance) validateNativeVolume(ctx context.Context, volume management.Volume) (returned error) {
	root := filepath.Join(m.root.VolumesPath, volume.StorageName)
	config, err := volumeConfig(m.config, root, volume, false, 0)
	if err != nil {
		return err
	}
	store, err := localstore.Open(ctx, config)
	if store != nil {
		m.native = store
		defer func() { returned = errors.Join(returned, m.closeNative()) }()
	}
	if err != nil {
		return err
	}
	_, err = store.Summary(ctx)
	return err
}
