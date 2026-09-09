package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
)

// LockingConfig opens one exclusively owned metadata database with lease enforcement.
// Its parent directory must be deployment-owned and protected from other writers. Lease
// evidence stays beside Database and is bound to its native inode. The recovery duration
// covers every namespace in the database. Initialize
// permits an explicit first binding or completion of its recorded initialization intent.
type LockingConfig struct {
	Database   string
	Namespace  string
	Allowance  int64
	SQLite     Options
	Locks      locking.Options
	Initialize bool
}

// LockingStore retains native database ownership through successful closure of every pool.
// The embedded Store supplies the namespace, publication hooks, and paired lock authority.
type LockingStore struct {
	*Store
	ownerMu sync.Mutex
	file    *nativelease.Database
	anchor  *LeaseAnchor
}

// OpenLocking validates native lifetime ownership and both lease evidence components before
// exposing the namespace. An ordinary reopen never creates a missing database or lease state.
func OpenLocking(ctx context.Context, config LockingConfig) (*LockingStore, error) {
	if config.Database == "" || config.Namespace == "" || strings.ContainsAny(config.Database, "%?#\x00") {
		return nil, fmt.Errorf("locking SQLite needs a native database path and namespace: %w", syscall.EINVAL)
	}
	if config.Allowance < 0 {
		return nil, fmt.Errorf("a metadata allowance must not be negative: %w", syscall.EINVAL)
	}
	options, err := config.SQLite.Effective()
	if err != nil {
		return nil, err
	}
	if err := config.Locks.Validate(); err != nil {
		return nil, err
	}
	database, err := filepath.Abs(config.Database)
	if err != nil {
		return nil, err
	}
	file, err := nativelease.AcquireDatabase(database, true, config.Initialize)
	if err != nil {
		return nil, err
	}
	start := file.Acquired()
	anchor, err := OpenLeaseAnchor(LeaseAnchorConfig{
		Directory: filepath.Dir(database), Name: "." + filepath.Base(database) + ".leases",
		Identity: "sqlite-database-lease-recovery", BindingFD: file.FD(), RecoveryStart: start, Initialize: config.Initialize,
	})
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	options.leaseRecoveryOwner = true
	options.leaseOwner = file
	options.requireExistingNamespace = !anchor.Initializing()
	store, err := OpenWithOptions(ctx, database, config.Namespace, config.Allowance, options)
	if err != nil {
		if OpenFailureRetainsOwnership(err) {
			return nil, err
		}
		return nil, errors.Join(err, anchor.Close(), file.Close())
	}
	opened := &LockingStore{Store: store, file: file, anchor: anchor}
	cleanup := func(primary error) (*LockingStore, error) {
		if err := store.Abort(); err != nil {
			return nil, errors.Join(primary, err)
		}
		return nil, errors.Join(primary, anchor.Close(), file.Close())
	}
	if err := nativelease.VerifyDatabase(file); err != nil {
		return cleanup(err)
	}
	if err := store.ConfigureLeaseRecovery(ctx, LeaseRecoveryConfig{
		Witness: anchor, RecoveryStart: start, StateID: anchor.StateID(), Initialize: anchor.Initializing(),
	}); err != nil {
		return cleanup(err)
	}
	if err := anchor.Complete(); err != nil {
		return cleanup(err)
	}
	if err := store.EnableLocks(ctx, config.Locks); err != nil {
		return cleanup(err)
	}
	return opened, nil
}

func prepareOwnedLeaseNamespace(ctx context.Context, db *sql.DB, namespace, storeID string, window Window, maxRecords, maxBytes int64) (int64, int64, error) {
	id, root, _, err := prepareConfigured(ctx, db, namespace, storeID, window, maxRecords, maxBytes, &durableOpen{mode: CreateNamespaceIfMissing, reapDetached: true})
	return id, root, err
}

func prepareExistingOwnedLeaseNamespace(ctx context.Context, db *sql.DB, namespace, storeID string, window Window, maxRecords, maxBytes int64) (int64, int64, error) {
	id, root, _, err := prepareConfigured(ctx, db, namespace, storeID, window, maxRecords, maxBytes, &durableOpen{mode: RequireExistingNamespace, reapDetached: true})
	return id, root, err
}

// Close preserves native ownership on any database-close failure, including uncertain
// driver closure. A later Close can retry nonterminal database-close failures.
func (s *LockingStore) Close() error {
	return s.CloseContext(context.Background())
}

func (s *LockingStore) CloseContext(ctx context.Context) error {
	s.ownerMu.Lock()
	defer s.ownerMu.Unlock()
	if err := s.Store.CloseContext(ctx); err != nil {
		return err
	}
	return s.closeOwnership()
}

// Abort closes an unexposed namespace and releases ownership only when every database pool
// has closed successfully. Failed native database cleanup retains the lifetime lock.
func (s *LockingStore) Abort() error {
	s.ownerMu.Lock()
	defer s.ownerMu.Unlock()
	if err := s.Store.Abort(); err != nil {
		return err
	}
	return s.closeOwnership()
}

func (s *LockingStore) closeOwnership() error {
	if s.file == nil {
		return nil
	}
	if err := s.anchor.Close(); err != nil {
		return err
	}
	err := s.file.Close()
	if err == nil {
		s.file = nil
	}
	return err
}
