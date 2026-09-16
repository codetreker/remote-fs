package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
)

type fileRecoveryKey struct{}

func (s *Store) configureFileRecovery(ctx context.Context, recovery *LeaseRecovery) error {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	d := s.fileDomain
	if d == nil {
		return nil
	}
	d.recoveryUntil = recovery.start
	if !recovery.state.Quiescent {
		d.recoveryUntil = recovery.start.Add(recovery.state.MaxLease)
	}
	var obligations bool
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM removal_intents WHERE volume=? AND authority<>?) OR EXISTS(SELECT 1 FROM entries WHERE volume=? AND draining=1)`, s.volume, d.authority, s.volume).Scan(&obligations)
	}); err != nil {
		return err
	}
	if !obligations {
		return nil
	}
	d.recoveryPending = true
	d.recoveryStore = s
	d.recoveryContext = locking.WithScope(context.WithoutCancel(ctx), locking.MutationScope{})
	if !time.Now().Before(d.recoveryUntil) {
		return s.recoverFileEntriesLocked(ctx)
	}
	d.recoveryTimer = time.AfterFunc(time.Until(d.recoveryUntil), func() {
		cleanup, cancel := context.WithTimeout(d.recoveryContext, d.contentConfig.FileOperationTimeout)
		defer cancel()
		if err := s.coordinator.commit.acquire(cleanup); err != nil {
			if !d.recoveryStopped.Load() {
				s.coordinator.poisonWith(err)
			}
			return
		}
		defer s.coordinator.commit.release()
		if d.recoveryStopped.Load() || d.recoveryStore != s {
			return
		}
		if err := s.recoverFileEntriesLocked(cleanup); err != nil {
			s.coordinator.poisonWith(err)
		}
	})
	return nil
}

func (s *Store) recoverFileEntriesLocked(ctx context.Context) error {
	d := s.fileDomain
	if !d.recoveryPending {
		return nil
	}
	if time.Now().Before(d.recoveryUntil) {
		return syscall.EAGAIN
	}
	ctx = context.WithValue(ctx, fileRecoveryKey{}, true)
	ctx = context.WithValue(ctx, fileActorKey{}, fileActor{cleanup: true})
	var references []string
	var nodes []int64
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT CASE WHEN typeof(reference)='text' AND length(CAST(reference AS BLOB))<=128 THEN reference END FROM removal_intents WHERE volume=? AND authority<>?`, s.volume, d.authority)
		if err != nil {
			return err
		}
		for rows.Next() {
			if len(references) >= d.config.MaxPrepared {
				rows.Close()
				return syscall.EFBIG
			}
			var reference string
			if err := rows.Scan(&reference); err != nil {
				rows.Close()
				return err
			}
			references = append(references, reference)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		rows, err = tx.QueryContext(ctx, `SELECT DISTINCT e.node FROM entries e LEFT JOIN removal_intents r ON r.entry=e.id AND r.volume=e.volume WHERE e.volume=? AND (e.draining=1 OR r.authority<>?)`, s.volume, d.authority)
		if err != nil {
			return err
		}
		for rows.Next() {
			if len(nodes) >= d.config.MaxPrepared+d.config.MaxDrains {
				rows.Close()
				return syscall.EFBIG
			}
			var node int64
			if err := rows.Scan(&node); err != nil {
				rows.Close()
				return err
			}
			if s.coordinator.pins[retainedNode{s.volume, node}] != 0 {
				rows.Close()
				return syscall.EBUSY
			}
			nodes = append(nodes, node)
		}
		return errors.Join(rows.Err(), rows.Close())
	})
	if err != nil {
		return err
	}
	err = s.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.RemoveMutation, nodes: nodes, totalUsage: true}, func(tx *sql.Tx) error {
		for _, reference := range references {
			if _, err := s.activatePreparedRemoval(ctx, tx, reference); err != nil {
				return err
			}
		}
		rows, err := tx.QueryContext(ctx, `SELECT id FROM entries WHERE volume=? AND draining=1 ORDER BY id`, s.volume)
		if err != nil {
			return err
		}
		var entries []int64
		for rows.Next() {
			if len(entries) >= d.config.MaxDrains {
				rows.Close()
				return syscall.EFBIG
			}
			var entry int64
			if err := rows.Scan(&entry); err != nil {
				rows.Close()
				return err
			}
			entries = append(entries, entry)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		for _, id := range entries {
			entry, err := s.entryRetirementState(ctx, tx, id)
			if err != nil {
				return err
			}
			if err := s.detachDrainedEntry(ctx, tx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		d.recoveryPending = false
	}
	return err
}
