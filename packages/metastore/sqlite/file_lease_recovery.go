package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
)

func (r *LeaseRecovery) table() string {
	if r.file {
		return "file_lease_recovery"
	}
	return "lease_recovery"
}

func (s *Store) FileLeaseInitializationPending(ctx context.Context) (bool, error) {
	var state int
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT state FROM file_lease_initialization WHERE singleton=1`).Scan(&state)
	})
	if err != nil {
		return false, err
	}
	if state != 0 && state != 1 {
		return false, syscall.EIO
	}
	return state == 0, nil
}

// File recovery has its own evidence domain. Completing bootstrap cannot lower
// either the file watermark or an independently configured Strong watermark.
func (s *Store) ConfigureFileLeaseRecovery(ctx context.Context, config LeaseRecoveryConfig) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed || s.fileLeaseRecovery != nil {
		return syscall.EINVAL
	}
	if err := nativelease.VerifyExclusiveOwnership(s.leaseOwner); err != nil {
		return err
	}
	anchor, ok := config.Witness.(*LeaseAnchor)
	if !ok || anchor == nil || anchor.Domain() != LeaseDomainFile {
		return syscall.EINVAL
	}
	if err := nativelease.VerifyDatabaseOwner((*nativelease.Anchor)(anchor), s.leaseOwner); err != nil {
		return err
	}
	if config.StateID != anchor.StateID() || config.Initialize != anchor.Initializing() {
		return syscall.EIO
	}
	pending, err := s.FileLeaseInitializationPending(ctx)
	if err != nil {
		return err
	}
	if !pending && anchor.Initializing() {
		return fmt.Errorf("ready file recovery cannot initialize new evidence: %w", syscall.EIO)
	}
	config.RecoveryStart = s.leaseOwner.Acquired()
	r := &LeaseRecovery{file: true, store: s, witness: anchor, start: config.RecoveryStart, gate: newCommitGate()}
	if err := r.open(ctx, config); err != nil {
		return err
	}
	if pending && (r.state.Generation != 0 || r.state.MaxLease != 0 || !r.state.Quiescent) {
		return r.fail(fmt.Errorf("pending file initialization contains active leases: %w", syscall.EIO))
	}
	if err := anchor.Complete(); err != nil {
		return r.fail(err)
	}
	if pending {
		if err := s.mutate(ctx, func(tx *sql.Tx) error {
			result, err := tx.ExecContext(ctx, `UPDATE file_lease_initialization SET state=1 WHERE singleton=1 AND state=0`)
			if err != nil {
				return err
			}
			return requireRetirementRow(result, syscall.EIO)
		}); err != nil {
			return err
		}
	}
	s.fileLeaseRecovery = r
	return s.configureFileRecovery(ctx, r)
}

func (s *Store) RaiseFileMaxLease(ctx context.Context, ttl time.Duration) error {
	release, err := s.beginFileSession(ctx, ttl)
	if err != nil {
		return err
	}
	release()
	return nil
}

func validateFileLeaseOpening(ctx context.Context, db *sql.DB, owned bool) error {
	if owned {
		return nil
	}
	var present bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM file_lease_recovery)`).Scan(&present); err != nil {
		return err
	}
	if present {
		return fmt.Errorf("this volume requires its native file recovery owner: %w", syscall.EIO)
	}
	return nil
}

func validateFileLeaseMutation(ctx context.Context, tx *sql.Tx) error {
	var present bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM file_lease_recovery)`).Scan(&present); err != nil {
		return err
	}
	if present {
		return fmt.Errorf("volume mutation requires its file recovery authority: %w", syscall.EIO)
	}
	return nil
}

func OpenBoundDurableFileWithOptions(ctx context.Context, database, volume, storeID string, allowance int64, options Options, mode VolumeOpenMode, startup DurableStartup, witness CommitWitness) (*Store, error) {
	options.fileLeaseRecoveryOwner = true
	return OpenBoundDurableWithOptions(ctx, database, volume, storeID, allowance, options, mode, startup, witness)
}
