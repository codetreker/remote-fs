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

var _ storage.QuotaAcceptance = (*Store)(nil)

func (s *Store) CheckQuotaAcceptance() error { return s.CheckQuotaControl() }

func sameQuotaAcceptance(a, b storage.AcceptedQuotaAction) bool {
	return a.Command == b.Command && a.Namespace.SubjectID == b.Namespace.SubjectID && a.Namespace.Sequence == b.Namespace.Sequence && a.Namespace.AcceptUntil.Equal(b.Namespace.AcceptUntil) && a.AcceptedAt.Equal(b.AcceptedAt)
}

func quotaAcceptance(record nativeAction, key storage.ActionKey) (storage.AcceptedQuotaAction, error) {
	var proof storage.AcceptedQuotaAction
	if len(record.acceptance) == 0 || json.Unmarshal(record.acceptance, &proof) != nil || proof.Check() != nil || proof.Command.Action != key || proof.Command.Fingerprint != record.fingerprint {
		return proof, syscall.EIO
	}
	return proof, nil
}

// The embedding controller supplies a durable authorization decision. A
// expired namespace admits exactly this retained key. A still-live original
// window keeps ordinary admission without extending its deadline.
func (s *Store) AcceptQuotaAction(ctx context.Context, a storage.AcceptedQuotaAction) (storage.QuotaReceipt, error) {
	if err := a.Check(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if a.Namespace.Sequence > math.MaxInt64 {
		return storage.QuotaReceipt{}, syscall.EOVERFLOW
	}
	if err := checkActionSubject(ctx, a.Command.Action); err != nil {
		return storage.QuotaReceipt{}, err
	}
	if err := s.CheckQuotaAcceptance(); err != nil {
		return storage.QuotaReceipt{}, err
	}
	result := storage.QuotaReceipt{Action: a.Command.Action, Fingerprint: a.Command.Fingerprint, Outcome: storage.DurableActionUnknown}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return result, err
	}
	defer s.coordinator.commit.release()
	known := false
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		record, found, err := s.readAction(ctx, tx, a.Command.Action, quotaAction)
		if err != nil {
			return err
		}
		known = found
		if !found {
			return nil
		}
		proof, err := quotaAcceptance(record, a.Command.Action)
		if err != nil {
			return err
		}
		if !sameQuotaAcceptance(a, proof) {
			return syscall.EINVAL
		}
		return json.Unmarshal(record.result, &result)
	})
	if err != nil {
		return result, err
	}
	if known {
		if result.Failure != 0 {
			return result, result.Failure
		}
		return result, nil
	}
	err = s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error {
		var floor int64
		subjectErr := tx.QueryRowContext(ctx, `SELECT floor FROM action_subjects WHERE volume=? AND subject=?`, s.volume, a.Namespace.SubjectID).Scan(&floor)
		if subjectErr != nil && !errors.Is(subjectErr, sql.ErrNoRows) {
			return subjectErr
		}
		if errors.Is(subjectErr, sql.ErrNoRows) {
			var count int64
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM action_subjects WHERE volume=?`, s.volume).Scan(&count); err != nil {
				return err
			}
			if count >= int64(s.maxAuthorityActions) {
				return syscall.EAGAIN
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO action_subjects(volume,subject) VALUES(?,?)`, s.volume, a.Namespace.SubjectID); err != nil {
				return err
			}
		}
		if int64(a.Namespace.Sequence) < floor {
			return syscall.ESTALE
		}
		now, err := s.actionClock(ctx, tx, a.Namespace.SubjectID, true)
		if err != nil {
			return err
		}
		if a.AcceptedAt.After(now) {
			now = a.AcceptedAt
		}
		var sec, nsec int64
		namespaceErr := tx.QueryRowContext(ctx, `SELECT accept_sec,accept_nsec FROM action_namespaces WHERE volume=? AND subject=? AND sequence=?`, s.volume, a.Namespace.SubjectID, a.Namespace.Sequence).Scan(&sec, &nsec)
		if namespaceErr != nil && !errors.Is(namespaceErr, sql.ErrNoRows) {
			return namespaceErr
		}
		if namespaceErr == nil && !time.Unix(sec, nsec).Equal(a.Namespace.AcceptUntil) {
			return syscall.EINVAL
		}
		if errors.Is(namespaceErr, sql.ErrNoRows) {
			var count int64
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM action_namespaces WHERE volume=?`, s.volume).Scan(&count); err != nil {
				return err
			}
			if count >= int64(s.maxAuthorityActions) {
				return syscall.EAGAIN
			}
			admission := 0
			if now.Before(a.Namespace.AcceptUntil) {
				admission = 1
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO action_namespaces(volume,subject,sequence,accept_sec,accept_nsec,admission) VALUES(?,?,?,?,?,?)`, s.volume, a.Namespace.SubjectID, a.Namespace.Sequence, a.Namespace.AcceptUntil.Unix(), a.Namespace.AcceptUntil.Nanosecond(), admission); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE action_subjects SET highwater=max(highwater,?),logical_sec=?,logical_nsec=? WHERE volume=? AND subject=?`, a.Namespace.Sequence, now.Unix(), now.Nanosecond(), s.volume, a.Namespace.SubjectID); err != nil {
			return err
		}
		var count int64
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM authority_actions WHERE volume=?`, s.volume).Scan(&count); err != nil {
			return err
		}
		if count >= int64(s.maxAuthorityActions) {
			return syscall.EAGAIN
		}
		result.Outcome = storage.DurableActionPending
		if err := s.saveAction(ctx, tx, a.Command.Action, quotaAction, a.Command.Fingerprint, result.Outcome, 0, result); err != nil {
			return err
		}
		proof, err := json.Marshal(a)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE authority_actions SET acceptance=? WHERE volume=? AND subject=? AND namespace=? AND nonce=?`, proof, s.volume, a.Command.Action.SubjectID, a.Command.Action.Namespace, a.Command.Action.Nonce)
		return err
	})
	if err != nil {
		return storage.QuotaReceipt{Action: a.Command.Action, Fingerprint: a.Command.Fingerprint, Outcome: storage.DurableActionUnknown}, err
	}
	return result, nil
}
