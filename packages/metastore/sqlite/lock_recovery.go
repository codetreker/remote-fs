package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// LeaseEvidence binds a monotonic maximum lease duration to one database. It contains no
// owner or grant capabilities. Generation and duration are validated before use.
type LeaseEvidence struct {
	DatabaseID string
	StateID    string
	Generation int64
	MaxLease   time.Duration
}

// LeaseWitness stores independent, workspace-anchored evidence. Advance must durably publish
// the exact next generation before returning. Repeating the same record is idempotent.
type LeaseWitness interface {
	Load() (LeaseEvidence, bool, error)
	Advance(LeaseEvidence) error
}

// LeaseRecoveryConfig is supplied by the holder of exclusive native workspace ownership.
// Initialize requires a matching durable initialization intent; a flag supplied by an
// ordinary reopen is insufficient. RecoveryStart is captured after ownership acquisition.
type LeaseRecoveryConfig struct {
	Witness       LeaseWitness
	RecoveryStart time.Time
	StateID       string
	Initialize    bool
}

type LeaseRecovery struct {
	gate    commitGate
	store   *Store
	witness LeaseWitness
	start   time.Time
	state   LeaseEvidence
	poison  error
}

type leaseRecord struct {
	accepted LeaseEvidence
	prepared *LeaseEvidence
}

func validLeaseID(id string) bool {
	return len(id) == 32 && strings.Trim(id, "0123456789abcdef") == ""
}

func (e LeaseEvidence) validate() error {
	if !validLeaseID(e.DatabaseID) || !validLeaseID(e.StateID) ||
		e.Generation < 0 || e.MaxLease < 0 {
		return fmt.Errorf("lease recovery evidence has an invalid identity or counter: %w", syscall.EIO)
	}
	return nil
}

// ConfigureLeaseRecovery validates both independent records before exposing lease control.
// Prepared raises are completed conservatively. Invalid or missing active evidence fails
// closed; neither configuration changes nor a fresh process can lower the recorded duration.
func (s *Store) ConfigureLeaseRecovery(ctx context.Context, config LeaseRecoveryConfig) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if err := s.verifyLeaseOwnership(); err != nil {
		return err
	}
	anchor, ok := config.Witness.(*LeaseAnchor)
	if !ok || anchor == nil {
		return fmt.Errorf("lease recovery requires a native anchored witness: %w", syscall.EINVAL)
	}
	if err := anchor.verifyDatabaseOwner(s.leaseOwner); err != nil {
		return err
	}
	if config.StateID != anchor.StateID() || config.Initialize != anchor.Initializing() {
		return fmt.Errorf("lease recovery configuration differs from its durable native intent: %w", syscall.EIO)
	}
	config.RecoveryStart = s.leaseOwner.acquired
	return s.configureLeaseRecoveryLocked(ctx, config)
}

func (s *Store) configureLeaseRecovery(ctx context.Context, config LeaseRecoveryConfig) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	return s.configureLeaseRecoveryLocked(ctx, config)
}

func (s *Store) configureLeaseRecoveryLocked(ctx context.Context, config LeaseRecoveryConfig) error {
	if s.closed || s.leaseRecovery != nil {
		return fmt.Errorf("lease recovery requires an open, unattached namespace: %w", syscall.EINVAL)
	}
	if config.Witness == nil || config.RecoveryStart.IsZero() || !validLeaseID(config.StateID) {
		return fmt.Errorf("lease recovery needs native ownership and a bound witness: %w", syscall.EINVAL)
	}
	if err := s.coordinator.healthy(); err != nil {
		return err
	}
	recovery := &LeaseRecovery{store: s, witness: config.Witness, start: config.RecoveryStart, gate: newCommitGate()}
	if err := recovery.open(ctx, config); err != nil {
		return err
	}
	s.leaseRecovery = recovery
	return nil
}

func (r *LeaseRecovery) open(ctx context.Context, config LeaseRecoveryConfig) error {
	witness, present, err := r.witness.Load()
	if err != nil {
		return fmt.Errorf("reading lease recovery witness: %w", errors.Join(syscall.EIO, err))
	}
	var record leaseRecord
	var exists bool
	err = r.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		record, exists, err = readLeaseRecord(ctx, tx)
		return err
	})
	if err != nil {
		return fmt.Errorf("reading lease recovery state: %w", errors.Join(syscall.EIO, err))
	}
	if !exists {
		if !config.Initialize || present {
			return fmt.Errorf("the database has no lease recovery state: %w", syscall.EIO)
		}
		err := r.store.mutate(ctx, func(tx *sql.Tx) error {
			state, err := readDurableState(ctx, tx)
			if err != nil {
				return err
			}
			record.accepted = LeaseEvidence{DatabaseID: state.DatabaseID, StateID: config.StateID}
			_, err = tx.ExecContext(ctx, `INSERT INTO lease_recovery
				(singleton, database_id, state_id, accepted_generation, accepted_nanos)
				VALUES (1, ?, ?, 0, 0)`, state.DatabaseID, config.StateID)
			return err
		})
		if err != nil {
			return err
		}
	}
	if record.accepted.StateID != config.StateID {
		return fmt.Errorf("lease recovery state does not match its native binding: %w", syscall.EIO)
	}
	if !present {
		if !config.Initialize || record.accepted.Generation != 0 || record.accepted.MaxLease != 0 || record.prepared != nil {
			return fmt.Errorf("active lease recovery state has no independent witness: %w", syscall.EIO)
		}
		if err := r.advanceWitness(record.accepted); err != nil {
			return err
		}
		witness = record.accepted
	}
	if err := witness.validate(); err != nil {
		return err
	}
	if record.prepared == nil {
		if record.accepted != witness {
			return fmt.Errorf("accepted lease recovery state and witness disagree: %w", syscall.EIO)
		}
		r.state = record.accepted
		return nil
	}
	if witness != record.accepted && witness != *record.prepared {
		return fmt.Errorf("prepared lease recovery state and witness disagree: %w", syscall.EIO)
	}
	if err := r.advanceWitness(*record.prepared); err != nil {
		return err
	}
	if err := r.finalize(ctx, *record.prepared); err != nil {
		return err
	}
	r.state = *record.prepared
	return nil
}

func readLeaseRecord(ctx context.Context, tx *sql.Tx) (leaseRecord, bool, error) {
	var rows int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT singleton FROM lease_recovery LIMIT 2)`).Scan(&rows); err != nil {
		return leaseRecord{}, false, err
	}
	if rows == 0 {
		return leaseRecord{}, false, nil
	}
	if rows != 1 {
		return leaseRecord{}, false, fmt.Errorf("lease recovery state is not a singleton: %w", syscall.EIO)
	}
	var record leaseRecord
	var preparedGeneration, preparedNanos sql.NullInt64
	var valid int
	err := tx.QueryRowContext(ctx, `SELECT
		CASE WHEN typeof(database_id) = 'text' AND length(database_id) = 32 THEN database_id ELSE '' END,
		CASE WHEN typeof(state_id) = 'text' AND length(state_id) = 32 THEN state_id ELSE '' END,
		CASE WHEN typeof(accepted_generation) = 'integer' THEN accepted_generation ELSE 0 END,
		CASE WHEN typeof(accepted_nanos) = 'integer' THEN accepted_nanos ELSE 0 END,
		CASE WHEN typeof(prepared_generation) = 'integer' THEN prepared_generation END,
		CASE WHEN typeof(prepared_nanos) = 'integer' THEN prepared_nanos END,
		CASE WHEN typeof(singleton) = 'integer' AND singleton = 1 AND typeof(database_id) = 'text'
		AND length(database_id) = 32 AND typeof(state_id) = 'text' AND length(state_id) = 32
		AND typeof(accepted_generation) = 'integer' AND typeof(accepted_nanos) = 'integer'
		AND typeof(prepared_generation) IN ('integer', 'null')
		AND typeof(prepared_nanos) IN ('integer', 'null') THEN 1 ELSE 0 END
		FROM lease_recovery`).Scan(
		&record.accepted.DatabaseID, &record.accepted.StateID,
		&record.accepted.Generation, &record.accepted.MaxLease, &preparedGeneration, &preparedNanos, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return leaseRecord{}, false, nil
	}
	if err != nil {
		return leaseRecord{}, false, err
	}
	if valid != 1 {
		return leaseRecord{}, false, fmt.Errorf("lease recovery state has invalid storage classes: %w", syscall.EIO)
	}
	if err := record.accepted.validate(); err != nil {
		return leaseRecord{}, false, err
	}
	state, err := readDurableState(ctx, tx)
	if err != nil {
		return leaseRecord{}, false, err
	}
	if record.accepted.DatabaseID != state.DatabaseID || preparedGeneration.Valid != preparedNanos.Valid {
		return leaseRecord{}, false, fmt.Errorf("lease recovery state has an invalid database binding or partial preparation: %w", syscall.EIO)
	}
	if preparedGeneration.Valid {
		prepared := record.accepted
		prepared.Generation, prepared.MaxLease = preparedGeneration.Int64, time.Duration(preparedNanos.Int64)
		if record.accepted.Generation == math.MaxInt64 || prepared.Generation != record.accepted.Generation+1 ||
			prepared.MaxLease < record.accepted.MaxLease {
			return leaseRecord{}, false, fmt.Errorf("prepared lease recovery state is not the next nondecreasing generation: %w", syscall.EIO)
		}
		record.prepared = &prepared
	}
	return record, true, nil
}

func (r *LeaseRecovery) advanceWitness(state LeaseEvidence) error {
	if err := r.witness.Advance(state); err != nil {
		return r.fail(fmt.Errorf("advancing lease recovery witness: %w", err))
	}
	return nil
}

func (r *LeaseRecovery) fail(err error) error {
	r.poison = errors.Join(syscall.EIO, err)
	r.store.coordinator.poisonWith(r.poison)
	return r.poison
}

func (r *LeaseRecovery) finalize(ctx context.Context, next LeaseEvidence) error {
	err := r.store.mutate(ctx, func(tx *sql.Tx) error {
		record, exists, err := readLeaseRecord(ctx, tx)
		if err != nil {
			return err
		}
		if !exists || record.prepared == nil || *record.prepared != next {
			return fmt.Errorf("lease recovery preparation changed before acceptance: %w", syscall.EIO)
		}
		_, err = tx.ExecContext(ctx, `UPDATE lease_recovery SET accepted_generation = ?,
			accepted_nanos = ?, prepared_generation = NULL, prepared_nanos = NULL WHERE singleton = 1`,
			next.Generation, int64(next.MaxLease))
		return err
	})
	if err != nil {
		return r.fail(fmt.Errorf("accepting prepared lease recovery state: %w", err))
	}
	return nil
}

// MaxLease returns the durable maximum, including an interrupted raise recovered on open.
func (s *Store) MaxLease(ctx context.Context) (time.Duration, error) {
	r, err := s.configuredLeaseRecovery()
	if err != nil {
		return 0, err
	}
	if err := r.gate.acquire(ctx); err != nil {
		return 0, err
	}
	defer r.gate.release()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.coordinator.healthy(); err != nil {
		return 0, err
	}
	return r.state.MaxLease, nil
}

// RaiseMaxLease durably prepares, witnesses, and accepts a nondecreasing maximum. A failure
// after preparation fences this Store; reopening reconciles its independent evidence.
func (s *Store) RaiseMaxLease(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("a lease duration must be positive: %w", syscall.EINVAL)
	}
	r, err := s.configuredLeaseRecovery()
	if err != nil {
		return err
	}
	if err := r.gate.acquire(ctx); err != nil {
		return err
	}
	defer r.gate.release()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.coordinator.healthy(); err != nil {
		return err
	}
	if ttl <= r.state.MaxLease {
		return nil
	}
	if r.state.Generation == math.MaxInt64 {
		return fmt.Errorf("lease recovery generation is exhausted: %w", syscall.ENOSPC)
	}
	next := r.state
	next.Generation++
	next.MaxLease = ttl
	err = s.mutate(ctx, func(tx *sql.Tx) error {
		record, exists, err := readLeaseRecord(ctx, tx)
		if err != nil {
			return err
		}
		if !exists || record.accepted != r.state || record.prepared != nil {
			return fmt.Errorf("lease recovery state changed before preparation: %w", syscall.EIO)
		}
		_, err = tx.ExecContext(ctx, `UPDATE lease_recovery SET prepared_generation = ?,
			prepared_nanos = ? WHERE singleton = 1`, next.Generation, int64(next.MaxLease))
		return err
	})
	if err != nil {
		return err
	}
	if err := r.advanceWitness(next); err != nil {
		return err
	}
	if err := r.finalize(ctx, next); err != nil {
		return err
	}
	r.state = next
	return nil
}

func (s *Store) configuredLeaseRecovery() (*LeaseRecovery, error) {
	if s.leaseRecovery == nil {
		return nil, fmt.Errorf("the namespace has no active lease recovery owner: %w", syscall.EIO)
	}
	return s.leaseRecovery, nil
}

// RecoveryStart is the new monotonic origin captured after exclusive native ownership.
func (s *Store) RecoveryStart() time.Time {
	return s.leaseRecovery.start
}

func validateLeaseOpening(ctx context.Context, db *sql.DB, owned bool) error {
	if owned {
		return nil
	}
	var present bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM lease_recovery)`).Scan(&present); err != nil {
		return err
	}
	if present {
		return fmt.Errorf("this namespace requires its native lease recovery owner: %w", syscall.EIO)
	}
	return nil
}

func validateNativeLeaseOpening(database string, owned bool) error {
	if strings.ContainsAny(database, "%?#\x00") {
		return fmt.Errorf("the SQLite database must be a native pathname without URI parameters or escapes: %w", syscall.EINVAL)
	}
	if owned {
		return nil
	}
	for _, name := range []string{database, filepath.Dir(database)} {
		_, err := unix.Getxattr(name, leaseBindingAttribute, nil)
		if err == nil {
			return fmt.Errorf("the native database requires its lease recovery owner: %w", syscall.EIO)
		}
		if !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.ENODATA) && !errors.Is(err, syscall.ENOTSUP) {
			return fmt.Errorf("checking native database lease binding: %w", err)
		}
	}
	return nil
}

func validateLeaseMutation(ctx context.Context, tx *sql.Tx) error {
	var present bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM lease_recovery)`).Scan(&present); err != nil {
		return err
	}
	if present {
		return fmt.Errorf("namespace mutation requires its lease authority: %w", syscall.EIO)
	}
	return nil
}

// OpenBoundDurableLeaseWithOptions reserves opening for a caller which holds native lease
// ownership and configures recovery before exposing this Store. Ordinary durable opens
// reject namespaces that already carry lease evidence.
func OpenBoundDurableLeaseWithOptions(
	ctx context.Context, database, namespace, storeID string, allowance int64, options Options,
	mode NamespaceOpenMode, startup DurableStartup, witness CommitWitness,
) (*Store, error) {
	options.leaseRecoveryOwner = true
	return OpenBoundDurableWithOptions(ctx, database, namespace, storeID, allowance, options, mode, startup, witness)
}
