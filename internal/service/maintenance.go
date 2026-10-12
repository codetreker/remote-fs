package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/codetreker/remote-fs/internal/management"
)

// Maintenance retains the service's stable exclusive anchor across offline
// database operations.
type Maintenance struct {
	root   *stateRoot
	store  *management.Store
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

func (m *Maintenance) Close(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	if m.store != nil {
		if err := m.store.Close(); err != nil {
			return fmt.Errorf("close maintenance catalog: %w", err)
		}
		m.store = nil
	}
	if err := m.root.Close(); err != nil {
		return err
	}
	m.closed = true
	return nil
}
