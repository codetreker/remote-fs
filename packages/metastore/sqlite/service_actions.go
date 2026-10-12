package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	quotaAction  = 1
	importAction = 2
)

type nativeAction struct {
	fingerprint storage.ActionFingerprint
	outcome     storage.DurableActionOutcome
	failure     syscall.Errno
	result      []byte
	acceptance  []byte
}

func (s *Store) actionClock(ctx context.Context, tx *sql.Tx, subject string, advance bool) (time.Time, error) {
	var sec, nsec int64
	if err := tx.QueryRowContext(ctx, `SELECT logical_sec,logical_nsec FROM action_subjects WHERE volume=? AND subject=?`, s.volume, subject).Scan(&sec, &nsec); err != nil {
		return time.Time{}, err
	}
	now := time.Now()
	old := time.Unix(sec, nsec)
	if old.After(now) {
		now = old
	}
	if advance {
		if _, err := tx.ExecContext(ctx, `UPDATE action_subjects SET logical_sec=?,logical_nsec=? WHERE volume=? AND subject=?`, now.Unix(), now.Nanosecond(), s.volume, subject); err != nil {
			return time.Time{}, err
		}
	}
	return now, nil
}

func (s *Store) RegisterActionNamespace(ctx context.Context, n storage.ActionNamespace) error {
	if err := n.Check(); err != nil {
		return err
	}
	if n.Sequence > math.MaxInt64 {
		return syscall.EOVERFLOW
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	known := false
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		var sec, nsec int64
		err := tx.QueryRowContext(ctx, `SELECT accept_sec,accept_nsec FROM action_namespaces WHERE volume=? AND subject=? AND sequence=?`, s.volume, n.SubjectID, n.Sequence).Scan(&sec, &nsec)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		known = true
		if !time.Unix(sec, nsec).Equal(n.AcceptUntil) {
			return syscall.EINVAL
		}
		return nil
	}); err != nil {
		return err
	}
	if known {
		return nil
	}
	return s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error {
		var subjects, namespaces int
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM action_subjects WHERE volume=?),(SELECT count(*) FROM action_namespaces WHERE volume=?)`, s.volume, s.volume).Scan(&subjects, &namespaces); err != nil {
			return err
		}
		var floor, high int64
		err := tx.QueryRowContext(ctx, `SELECT floor,highwater FROM action_subjects WHERE volume=? AND subject=?`, s.volume, n.SubjectID).Scan(&floor, &high)
		if errors.Is(err, sql.ErrNoRows) {
			if subjects >= s.maxAuthorityActions {
				return syscall.EAGAIN
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO action_subjects(volume,subject) VALUES(?,?)`, s.volume, n.SubjectID); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if int64(n.Sequence) < floor {
			return syscall.ESTALE
		}
		var sec, nsec int64
		err = tx.QueryRowContext(ctx, `SELECT accept_sec,accept_nsec FROM action_namespaces WHERE volume=? AND subject=? AND sequence=?`, s.volume, n.SubjectID, n.Sequence).Scan(&sec, &nsec)
		if err == nil {
			if !time.Unix(sec, nsec).Equal(n.AcceptUntil) {
				return syscall.EINVAL
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if namespaces >= s.maxAuthorityActions {
			return syscall.EAGAIN
		}
		now, err := s.actionClock(ctx, tx, n.SubjectID, true)
		if err != nil {
			return err
		}
		if !n.AcceptUntil.After(now) {
			return syscall.ESTALE
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO action_namespaces(volume,subject,sequence,accept_sec,accept_nsec) VALUES(?,?,?,?,?)`, s.volume, n.SubjectID, n.Sequence, n.AcceptUntil.Unix(), n.AcceptUntil.Nanosecond()); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE action_subjects SET highwater=max(highwater,?) WHERE volume=? AND subject=?`, n.Sequence, s.volume, n.SubjectID)
		return err
	})
}

func (s *Store) RetireActionNamespaces(ctx context.Context, r storage.ActionNamespaceRetirement) error {
	if err := r.Check(); err != nil {
		return err
	}
	if r.Before > math.MaxInt64 {
		return syscall.EOVERFLOW
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	var observedFloor int64
	absent := false
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `SELECT floor FROM action_subjects WHERE volume=? AND subject=?`, s.volume, r.SubjectID).Scan(&observedFloor)
		if errors.Is(err, sql.ErrNoRows) {
			absent = true
			return nil
		}
		return err
	}); err != nil {
		return err
	}
	if int64(r.Before) <= observedFloor {
		return nil
	}
	return s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error {
		if absent {
			var subjects int64
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM action_subjects WHERE volume=?`, s.volume).Scan(&subjects); err != nil {
				return err
			}
			if subjects >= int64(s.maxAuthorityActions) {
				return syscall.EAGAIN
			}
			now := time.Now()
			_, err := tx.ExecContext(ctx, `INSERT INTO action_subjects(volume,subject,floor,highwater,logical_sec,logical_nsec) VALUES(?,?,?,?,?,?)`, s.volume, r.SubjectID, r.Before, r.Before-1, now.Unix(), now.Nanosecond())
			return err
		}
		var floor int64
		if err := tx.QueryRowContext(ctx, `SELECT floor FROM action_subjects WHERE volume=? AND subject=?`, s.volume, r.SubjectID).Scan(&floor); err != nil {
			return err
		}
		if int64(r.Before) <= floor {
			return nil
		}
		now, err := s.actionClock(ctx, tx, r.SubjectID, true)
		if err != nil {
			return err
		}
		var active int64
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM action_namespaces WHERE volume=? AND subject=? AND sequence<? AND (accept_sec>? OR (accept_sec=? AND accept_nsec>?))`, s.volume, r.SubjectID, r.Before, now.Unix(), now.Unix(), now.Nanosecond()).Scan(&active); err != nil {
			return err
		}
		if active != 0 {
			return syscall.EBUSY
		}

		var pending int64
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM authority_actions WHERE volume=? AND subject=? AND namespace<? AND (outcome NOT IN(?,?) OR created_sec>=?)`, s.volume, r.SubjectID, r.Before, storage.DurableActionCompleted, storage.DurableActionNotExecuted, r.RetainAfter.Unix()).Scan(&pending); err != nil {
			return err
		}
		if pending != 0 {
			return syscall.EBUSY
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM authority_actions WHERE volume=? AND subject=? AND namespace<?`, s.volume, r.SubjectID, r.Before); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM action_namespaces WHERE volume=? AND subject=? AND sequence<?`, s.volume, r.SubjectID, r.Before); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE action_subjects SET floor=? WHERE volume=? AND subject=?`, r.Before, s.volume, r.SubjectID)
		return err
	})
}

func (s *Store) readAction(ctx context.Context, tx *sql.Tx, key storage.ActionKey, kind int) (nativeAction, bool, error) {
	var record nativeAction
	var fingerprint []byte
	var actual int
	var failure, resultBytes, acceptanceBytes int64
	err := tx.QueryRowContext(ctx, `SELECT kind,fingerprint,outcome,failure,CASE WHEN typeof(result)='blob' AND length(result)<=16384 THEN result END,CASE WHEN typeof(acceptance)='blob' AND length(acceptance)<=16384 THEN acceptance END,CASE WHEN typeof(result)='blob' THEN length(result) ELSE -1 END,CASE WHEN acceptance IS NULL THEN 0 WHEN typeof(acceptance)='blob' THEN length(acceptance) ELSE -1 END FROM authority_actions WHERE volume=? AND subject=? AND namespace=? AND nonce=?`, s.volume, key.SubjectID, key.Namespace, key.Nonce).Scan(&actual, &fingerprint, &record.outcome, &failure, &record.result, &record.acceptance, &resultBytes, &acceptanceBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return record, false, nil
	}
	if err != nil {
		return record, false, err
	}
	if resultBytes < 0 || acceptanceBytes < 0 || resultBytes > 16384 || acceptanceBytes > 16384 || int64(len(record.result)) != resultBytes || int64(len(record.acceptance)) != acceptanceBytes {
		return record, true, syscall.EIO
	}
	if actual != kind {
		return record, true, syscall.EINVAL
	}
	if len(fingerprint) != 32 {
		return record, true, syscall.EIO
	}
	if failure < 0 || failure > math.MaxInt32 {
		return record, true, syscall.EIO
	}
	record.failure = syscall.Errno(failure)
	copy(record.fingerprint[:], fingerprint)
	if record.outcome < storage.DurableActionPending || record.outcome > storage.DurableActionHistoryExpired {
		return record, true, syscall.EIO
	}
	switch kind {
	case quotaAction:
		var receipt storage.QuotaReceipt
		if json.Unmarshal(record.result, &receipt) != nil || receipt.Check() != nil || receipt.Action != key || receipt.Fingerprint != record.fingerprint || receipt.Outcome != record.outcome || receipt.Failure != record.failure {
			return record, true, syscall.EIO
		}
	case importAction:
		var receipt storage.ImportReceipt
		if json.Unmarshal(record.result, &receipt) != nil || receipt.Check() != nil || receipt.Action != key || receipt.Fingerprint != record.fingerprint || receipt.Outcome != record.outcome || receipt.Failure != record.failure {
			return record, true, syscall.EIO
		}
	}
	return record, true, nil
}

func (s *Store) checkActionAdmission(ctx context.Context, tx *sql.Tx, key storage.ActionKey) error {
	if key.Namespace > math.MaxInt64 {
		return syscall.EOVERFLOW
	}
	var floor int64
	err := tx.QueryRowContext(ctx, `SELECT floor FROM action_subjects WHERE volume=? AND subject=?`, s.volume, key.SubjectID).Scan(&floor)
	if errors.Is(err, sql.ErrNoRows) {
		return syscall.ESTALE
	}
	if err != nil {
		return err
	}
	if int64(key.Namespace) < floor {
		return syscall.ESTALE
	}
	var sec, nsec, admission int64
	err = tx.QueryRowContext(ctx, `SELECT accept_sec,accept_nsec,admission FROM action_namespaces WHERE volume=? AND subject=? AND sequence=?`, s.volume, key.SubjectID, key.Namespace).Scan(&sec, &nsec, &admission)
	if errors.Is(err, sql.ErrNoRows) {
		return syscall.ESTALE
	}
	if err != nil {
		return err
	}
	if admission != 1 {
		return syscall.ESTALE
	}
	now, err := s.actionClock(ctx, tx, key.SubjectID, true)
	if err != nil {
		return err
	}
	if !time.Unix(sec, nsec).After(now) {
		return syscall.ESTALE
	}
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM authority_actions WHERE volume=?`, s.volume).Scan(&count); err != nil {
		return err
	}
	if count >= int64(s.maxAuthorityActions) {
		return syscall.EAGAIN
	}
	return nil
}

func (s *Store) saveAction(ctx context.Context, tx *sql.Tx, key storage.ActionKey, kind int, fingerprint storage.ActionFingerprint, outcome storage.DurableActionOutcome, failure syscall.Errno, result any) error {
	now, clockErr := s.actionClock(ctx, tx, key.SubjectID, true)
	if clockErr != nil {
		return clockErr
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO authority_actions(volume,subject,namespace,nonce,kind,fingerprint,outcome,failure,result,created_sec) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(volume,subject,namespace,nonce) DO UPDATE SET outcome=excluded.outcome,failure=excluded.failure,result=excluded.result,created_sec=excluded.created_sec`, s.volume, key.SubjectID, key.Namespace, key.Nonce, kind, fingerprint[:], outcome, int64(failure), encoded, now.Unix())
	return err
}

func (s *Store) SetQuota(ctx context.Context, c storage.QuotaCommand) (storage.QuotaReceipt, error) {
	if err := checkActionSubject(ctx, c.Action); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := c.Check(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := s.CheckQuotaControl(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	result := storage.QuotaReceipt{Action: c.Action, Fingerprint: c.Fingerprint, Outcome: storage.DurableActionUnknown}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return result, err
	}
	defer s.coordinator.commit.release()
	known := false
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		record, found, err := s.readAction(ctx, tx, c.Action, quotaAction)
		if err != nil {
			return err
		}
		known = found
		if !found {
			return nil
		}
		if record.fingerprint != c.Fingerprint {
			return syscall.EINVAL
		}
		if record.outcome == storage.DurableActionPending {
			proof, err := quotaAcceptance(record, c.Action)
			if err != nil {
				return err
			}
			if proof.Command != c {
				return syscall.EINVAL
			}
		}
		return json.Unmarshal(record.result, &result)
	})
	if err != nil {
		return result, err
	}
	if known && result.Outcome != storage.DurableActionPending {
		if result.Failure != 0 {
			return result, result.Failure
		}
		return result, nil
	}
	err = s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error {
		existing, found, err := s.readAction(ctx, tx, c.Action, quotaAction)
		if err != nil {
			return err
		}
		if found {
			if existing.fingerprint != c.Fingerprint {
				return syscall.EINVAL
			}
			if existing.outcome != storage.DurableActionPending {
				return json.Unmarshal(existing.result, &result)
			}
			proof, err := quotaAcceptance(existing, c.Action)
			if err != nil {
				return err
			}
			if proof.Command != c {
				return syscall.EINVAL
			}
		} else {
			if err := s.checkActionAdmission(ctx, tx, c.Action); err != nil {
				return err
			}
		}
		var revision int64
		if err := tx.QueryRowContext(ctx, `SELECT quota_revision FROM volumes WHERE id=?`, s.volume).Scan(&revision); err != nil {
			return err
		}
		if revision < 1 {
			return syscall.EIO
		}
		if uint64(revision) != c.ExpectedRevision {
			result.Outcome = storage.DurableActionNotExecuted
			result.Failure = syscall.ESTALE
		} else {
			if revision == math.MaxInt64 {
				return syscall.EOVERFLOW
			}
			if _, err := tx.ExecContext(ctx, `UPDATE volumes SET quota_bytes=?,quota_revision=quota_revision+1 WHERE id=?`, c.QuotaBytes, s.volume); err != nil {
				return err
			}
			result.Outcome = storage.DurableActionCompleted
			result.QuotaBytes = c.QuotaBytes
			result.QuotaRevision = uint64(revision + 1)
		}
		return s.saveAction(ctx, tx, c.Action, quotaAction, c.Fingerprint, result.Outcome, result.Failure, result)
	})
	if err != nil {
		result.Outcome = storage.DurableActionUnknown
		result.QuotaBytes = 0
		result.QuotaRevision = 0
		result.Failure = 0
		return result, err
	}
	if result.Failure != 0 {
		return result, result.Failure
	}
	return result, nil
}

func (s *Store) QueryQuotaAction(ctx context.Context, key storage.ActionKey) (storage.QuotaReceipt, error) {
	if err := checkActionSubject(ctx, key); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := key.Check(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := s.CheckQuotaControl(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	result := storage.QuotaReceipt{Action: key, Outcome: storage.DurableActionUnknown}
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		record, found, err := s.readAction(ctx, tx, key, quotaAction)
		if err != nil {
			return err
		}
		if found {
			return json.Unmarshal(record.result, &result)
		}
		return s.unknownAction(ctx, tx, key, &result.Outcome)
	})
	if err != nil {
		return result, err
	}
	if result.Failure != 0 {
		return result, result.Failure
	}
	return result, nil
}

func (s *Store) unknownAction(ctx context.Context, tx *sql.Tx, key storage.ActionKey, outcome *storage.DurableActionOutcome) error {
	var floor int64
	err := tx.QueryRowContext(ctx, `SELECT floor FROM action_subjects WHERE volume=? AND subject=?`, s.volume, key.SubjectID).Scan(&floor)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && key.Namespace <= math.MaxInt64 && int64(key.Namespace) < floor {
		*outcome = storage.DurableActionHistoryExpired
		return syscall.ESTALE
	}
	*outcome = storage.DurableActionUnknown
	return syscall.EIO
}
