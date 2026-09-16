package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
)

func readFileLeaseRecord(ctx context.Context, tx *sql.Tx) (leaseRecord, bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT singleton FROM file_lease_recovery LIMIT 2)`).Scan(&count); err != nil {
		return leaseRecord{}, false, err
	}
	if count == 0 {
		return leaseRecord{}, false, nil
	}
	if count != 1 {
		return leaseRecord{}, false, syscall.EIO
	}
	var r leaseRecord
	var q int64
	var pg, pn, pq sql.NullInt64
	var valid bool
	err := tx.QueryRowContext(ctx, `SELECT
 CASE WHEN typeof(database_id)='text' AND length(CAST(database_id AS BLOB))=32 THEN database_id ELSE '' END,
 CASE WHEN typeof(state_id)='text' AND length(CAST(state_id AS BLOB))=32 THEN state_id ELSE '' END,
 CASE WHEN typeof(accepted_generation)='integer' THEN accepted_generation ELSE 0 END,
 CASE WHEN typeof(accepted_nanos)='integer' THEN accepted_nanos ELSE 0 END,
 CASE WHEN typeof(accepted_quiescent)='integer' THEN accepted_quiescent ELSE 0 END,
 CASE WHEN typeof(prepared_generation)='integer' THEN prepared_generation END,
 CASE WHEN typeof(prepared_nanos)='integer' THEN prepared_nanos END,
 CASE WHEN typeof(prepared_quiescent)='integer' THEN prepared_quiescent END,
 typeof(singleton)='integer' AND singleton=1 AND typeof(accepted_generation)='integer' AND typeof(accepted_nanos)='integer'
 AND typeof(accepted_quiescent)='integer' AND typeof(prepared_generation) IN ('integer','null')
 AND typeof(prepared_nanos) IN ('integer','null') AND typeof(prepared_quiescent) IN ('integer','null')
 FROM file_lease_recovery`).Scan(&r.accepted.DatabaseID, &r.accepted.StateID, &r.accepted.Generation, &r.accepted.MaxLease, &q, &pg, &pn, &pq, &valid)
	if err != nil {
		return r, false, err
	}
	if !valid || (q != 0 && q != 1) || pg.Valid != pn.Valid || pg.Valid != pq.Valid {
		return r, false, syscall.EIO
	}
	r.accepted.Quiescent = q == 1
	if err := validateFileEvidence(r.accepted); err != nil {
		return r, false, err
	}
	state, err := dbstate.Read(ctx, tx)
	if err != nil {
		return r, false, err
	}
	if state.DatabaseID != r.accepted.DatabaseID {
		return r, false, syscall.EIO
	}
	if pg.Valid {
		next := r.accepted
		next.Generation = pg.Int64
		next.MaxLease = time.Duration(pn.Int64)
		next.Quiescent = pq.Int64 == 1
		if (pq.Int64 != 0 && pq.Int64 != 1) || r.accepted.Generation == math.MaxInt64 || next.Generation != r.accepted.Generation+1 || next.MaxLease < r.accepted.MaxLease || (next.Quiescent && next.MaxLease != r.accepted.MaxLease) {
			return r, false, syscall.EIO
		}
		if err := validateFileEvidence(next); err != nil {
			return r, false, err
		}
		r.prepared = &next
	}
	if r.accepted.Quiescent || (r.prepared != nil && r.prepared.Quiescent) {
		var obligations bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM removal_intents) OR EXISTS(SELECT 1 FROM entries WHERE draining=1)`).Scan(&obligations); err != nil {
			return r, false, err
		}
		if obligations {
			return r, false, fmt.Errorf("quiescent file evidence has outstanding removal obligations: %w", syscall.EIO)
		}
	}
	return r, true, nil
}

func validateFileEvidence(e LeaseEvidence) error {
	if err := nativelease.ValidateEvidence(nativelease.Evidence(e)); err != nil {
		return err
	}
	if (e.Generation == 0 && (e.MaxLease != 0 || !e.Quiescent)) || (!e.Quiescent && e.MaxLease <= 0) {
		return syscall.EIO
	}
	return nil
}

func (r *LeaseRecovery) fileMutation(ctx context.Context, ordered bool, f func(*sql.Tx) error) error {
	if ordered {
		return r.store.mutateTransactionLocked(ctx, ctx, nil, f)
	}
	return r.store.mutate(ctx, f)
}

func (r *LeaseRecovery) finalizeFile(ctx context.Context, next LeaseEvidence, ordered bool) error {
	err := r.fileMutation(ctx, ordered, func(tx *sql.Tx) error {
		record, exists, err := readFileLeaseRecord(ctx, tx)
		if err != nil {
			return err
		}
		if !exists || record.prepared == nil || *record.prepared != next {
			return syscall.EIO
		}
		_, err = tx.ExecContext(ctx, `UPDATE file_lease_recovery SET accepted_generation=?,accepted_nanos=?,accepted_quiescent=?,prepared_generation=NULL,prepared_nanos=NULL,prepared_quiescent=NULL WHERE singleton=1`, next.Generation, int64(next.MaxLease), next.Quiescent)
		return err
	})
	if err != nil {
		return r.fail(fmt.Errorf("accepting file recovery evidence: %w", err))
	}
	return nil
}

func (r *LeaseRecovery) refreshFileLocked(ctx context.Context) error {
	var record leaseRecord
	var exists bool
	if err := r.store.inspect(ctx, func(tx *sql.Tx) error { var err error; record, exists, err = readFileLeaseRecord(ctx, tx); return err }); err != nil {
		return r.fail(err)
	}
	witness, present, err := r.witness.Load()
	if err != nil {
		return r.fail(err)
	}
	if !exists || !present || record.prepared != nil || record.accepted != witness {
		return r.fail(fmt.Errorf("file recovery evidence changed outside its authority: %w", syscall.EIO))
	}
	r.state = record.accepted
	return nil
}

func (r *LeaseRecovery) transitionFileLocked(ctx context.Context, next LeaseEvidence) error {
	if err := validateFileEvidence(next); err != nil {
		return err
	}
	err := r.fileMutation(ctx, true, func(tx *sql.Tx) error {
		record, exists, err := readFileLeaseRecord(ctx, tx)
		if err != nil {
			return err
		}
		if !exists || record.prepared != nil || record.accepted != r.state {
			return syscall.EIO
		}
		_, err = tx.ExecContext(ctx, `UPDATE file_lease_recovery SET prepared_generation=?,prepared_nanos=?,prepared_quiescent=? WHERE singleton=1`, next.Generation, int64(next.MaxLease), next.Quiescent)
		return err
	})
	if err != nil {
		return err
	}
	if err := r.advanceWitness(next); err != nil {
		return err
	}
	if err := r.finalizeFile(ctx, next, true); err != nil {
		return err
	}
	r.state = next
	return nil
}

// Admission is serialized across every volume sharing the database. The caller
// keeps this gate until the newly admitted session is registered under commit.
func (s *Store) beginFileSession(ctx context.Context, ttl time.Duration) (func(), error) {
	if ttl <= 0 {
		return nil, syscall.EINVAL
	}
	if err := s.coordinator.fileAdmission.acquire(ctx); err != nil {
		return nil, err
	}
	release := s.coordinator.fileAdmission.release
	if err := s.activateFileLease(ctx, ttl); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func (s *Store) activateFileLease(ctx context.Context, ttl time.Duration) error {
	r := s.fileLeaseRecovery
	if r == nil {
		return syscall.EOPNOTSUPP
	}
	if err := r.gate.acquire(ctx); err != nil {
		return err
	}
	defer r.gate.release()
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileAuthority(ctx); err != nil {
		return err
	}
	if err := r.refreshFileLocked(ctx); err != nil {
		return err
	}
	if !r.state.Quiescent && ttl <= r.state.MaxLease {
		return nil
	}
	if r.state.Generation == math.MaxInt64 {
		return syscall.ENOSPC
	}
	next := r.state
	next.Generation++
	next.Quiescent = false
	next.MaxLease = max(ttl, next.MaxLease)
	return r.transitionFileLocked(ctx, next)
}

// Close holds fileAdmission before taking a lease or commit gate. No session can
// appear between the database-wide drain proof and the durable witness update.
func (s *Store) markFileQuiescent(ctx context.Context) error {
	r := s.fileLeaseRecovery
	if r == nil {
		return nil
	}
	if err := r.gate.acquire(ctx); err != nil {
		return err
	}
	defer r.gate.release()
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	if err := s.coordinator.healthy(); err != nil {
		return err
	}
	for _, d := range s.coordinator.domains {
		if len(d.sessions) != 0 || d.files != 0 || d.activeIO != 0 || len(d.memberships) != 0 || d.recoveryPending || time.Now().Before(d.recoveryUntil) {
			return nil
		}
	}
	if len(s.coordinator.pins) != 0 {
		return nil
	}
	var obligations bool
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM removal_intents) OR EXISTS(SELECT 1 FROM entries WHERE draining=1)`).Scan(&obligations)
	}); err != nil {
		return err
	}
	if obligations {
		return nil
	}
	if err := r.refreshFileLocked(ctx); err != nil {
		return err
	}
	if r.state.Quiescent {
		return nil
	}
	if r.state.Generation == math.MaxInt64 {
		return syscall.ENOSPC
	}
	next := r.state
	next.Generation++
	next.Quiescent = true
	return r.transitionFileLocked(ctx, next)
}
