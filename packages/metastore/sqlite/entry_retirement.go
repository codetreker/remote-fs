package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"syscall"
)

// These transitions share the caller's transaction, commit gate, reference
// retirement, and publication admission. An error requires transaction rollback.
type entryRetirement struct {
	EntryID, NodeID int64
	Draining        bool
	Generation      uint64
	IfEmpty         bool
	Authority       string
}

func checkRetirementIdentity(value string) error {
	if value == "" || len(value) > 128 || strings.IndexByte(value, 0) >= 0 {
		return syscall.EINVAL
	}
	return nil
}

func (s *Store) entryRetirementState(ctx context.Context, tx *sql.Tx, entry int64) (entryRetirement, error) {
	if entry <= 0 {
		return entryRetirement{}, syscall.EINVAL
	}
	state := entryRetirement{EntryID: entry}
	err := tx.QueryRowContext(ctx, `SELECT node,draining,drain_generation,drain_if_empty,drain_authority FROM entries WHERE volume=? AND id=?`, s.volume, entry).
		Scan(&state.NodeID, &state.Draining, &state.Generation, &state.IfEmpty, &state.Authority)
	if errors.Is(err, sql.ErrNoRows) {
		return entryRetirement{}, syscall.ESTALE
	}
	if err != nil {
		return entryRetirement{}, err
	}
	if state.NodeID <= 0 || state.Generation > math.MaxInt64 || state.Draining && (state.Generation == 0 || checkRetirementIdentity(state.Authority) != nil) || !state.Draining && (state.IfEmpty || state.Authority != "") {
		return entryRetirement{}, syscall.EIO
	}
	return state, nil
}

func (s *Store) checkEntryActive(ctx context.Context, tx *sql.Tx, entry int64) error {
	state, err := s.entryRetirementState(ctx, tx, entry)
	if err != nil {
		return err
	}
	if state.Draining {
		return syscall.EAGAIN
	}
	return nil
}

// A retained parent does not exempt new child names from its entry's drain.
// The root has no entry; a detached parent has no publishable child namespace.
func (s *Store) checkParentEntriesActive(ctx context.Context, tx *sql.Tx, parent int64) error {
	if parent == s.root {
		return nil
	}
	var entry int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM entries WHERE volume=? AND node=?`, s.volume, parent).Scan(&entry); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return syscall.ESTALE
		}
		return err
	}
	return s.checkEntryActive(ctx, tx, entry)
}

func (s *Store) prepareEntryRemoval(ctx context.Context, tx *sql.Tx, reference, token string, entry int64, ifEmpty bool, authority string) error {
	for _, value := range []string{reference, token, authority} {
		if err := checkRetirementIdentity(value); err != nil {
			return err
		}
	}
	if err := s.checkEntryActive(ctx, tx, entry); err != nil {
		return err
	}
	var occupied bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM removal_intents WHERE reference=? OR token=?)`, reference, token).Scan(&occupied); err != nil {
		return err
	}
	if occupied {
		return syscall.EAGAIN
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO removal_intents(volume,reference,token,entry,if_empty,authority) VALUES(?,?,?,?,?,?)`, s.volume, reference, token, entry, ifEmpty, authority)
	return err
}

func (s *Store) cancelPreparedRemoval(ctx context.Context, tx *sql.Tx, reference, token string) error {
	if err := checkRetirementIdentity(reference); err != nil {
		return err
	}
	if err := checkRetirementIdentity(token); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM removal_intents WHERE volume=? AND reference=? AND token=?`, s.volume, reference, token)
	if err != nil {
		return err
	}
	return requireRetirementRow(result, syscall.ESTALE)
}

// Each newly admitted drain advances the generation. Action replay is handled
// by the enclosing receipt, so an old cancel cannot clear a later activation.
func (s *Store) drainEntry(ctx context.Context, tx *sql.Tx, entry int64, ifEmpty bool, authority string) (entryRetirement, error) {
	if err := checkRetirementIdentity(authority); err != nil {
		return entryRetirement{}, err
	}
	state, err := s.entryRetirementState(ctx, tx, entry)
	if err != nil {
		return entryRetirement{}, err
	}
	ifEmpty = ifEmpty || state.IfEmpty
	if ifEmpty {
		empty, err := s.isEmpty(ctx, tx, state.NodeID)
		if err != nil {
			return entryRetirement{}, err
		}
		if !empty {
			return entryRetirement{}, syscall.ENOTEMPTY
		}
	}
	if state.Generation == math.MaxInt64 {
		return entryRetirement{}, syscall.EOVERFLOW
	}
	state.Generation++
	state.Draining, state.IfEmpty, state.Authority = true, ifEmpty, authority
	result, err := tx.ExecContext(ctx, `UPDATE entries SET draining=1,drain_generation=?,drain_if_empty=?,drain_authority=? WHERE volume=? AND id=?`, state.Generation, state.IfEmpty, state.Authority, s.volume, entry)
	if err != nil {
		return entryRetirement{}, err
	}
	if err := requireRetirementRow(result, syscall.ESTALE); err != nil {
		return entryRetirement{}, err
	}
	return state, nil
}

func (s *Store) cancelEntryDrain(ctx context.Context, tx *sql.Tx, entry int64, generation uint64) error {
	if entry <= 0 || generation == 0 || generation > math.MaxInt64 {
		return syscall.EINVAL
	}
	state, err := s.entryRetirementState(ctx, tx, entry)
	if err != nil {
		return err
	}
	if !state.Draining || state.Generation != generation {
		return syscall.EAGAIN
	}
	result, err := tx.ExecContext(ctx, `UPDATE entries SET draining=0,drain_if_empty=0,drain_authority='' WHERE volume=? AND id=? AND draining=1 AND drain_generation=?`, s.volume, entry, generation)
	if err != nil {
		return err
	}
	return requireRetirementRow(result, syscall.EAGAIN)
}

// A zero result means this reference owns no drain activation: its intent was
// absent, its original entry was detached, or its IfEmpty condition was false.
// Another reference's active drain and Prepared intent remain independent.
func (s *Store) activatePreparedRemoval(ctx context.Context, tx *sql.Tx, reference string) (entryRetirement, error) {
	if err := checkRetirementIdentity(reference); err != nil {
		return entryRetirement{}, err
	}
	var entry int64
	var token, authority string
	var ifEmpty bool
	err := tx.QueryRowContext(ctx, `SELECT token,entry,if_empty,authority FROM removal_intents WHERE volume=? AND reference=?`, s.volume, reference).
		Scan(&token, &entry, &ifEmpty, &authority)
	if errors.Is(err, sql.ErrNoRows) {
		return entryRetirement{}, nil
	}
	if err != nil {
		return entryRetirement{}, err
	}
	if checkRetirementIdentity(token) != nil || checkRetirementIdentity(authority) != nil || entry <= 0 {
		return entryRetirement{}, syscall.EIO
	}
	state, err := s.entryRetirementState(ctx, tx, entry)
	if err != nil && !errors.Is(err, syscall.ESTALE) {
		return entryRetirement{}, err
	}
	activate := err == nil
	if activate && ifEmpty {
		activate, err = s.isEmpty(ctx, tx, state.NodeID)
		if err != nil {
			return entryRetirement{}, err
		}
	}
	if err := s.cancelPreparedRemoval(ctx, tx, reference, token); err != nil {
		return entryRetirement{}, err
	}
	if !activate {
		return entryRetirement{}, nil
	}
	return s.drainEntry(ctx, tx, entry, ifEmpty, authority)
}

// Pins and publication constraints belong to the caller. This check must run
// again in the final detach transaction, after all related references retire.
func (s *Store) entryReadyToDetach(ctx context.Context, tx *sql.Tx, entry int64, generation uint64) (entryRetirement, error) {
	state, err := s.entryRetirementState(ctx, tx, entry)
	if err != nil {
		return entryRetirement{}, err
	}
	if !state.Draining || state.Generation != generation {
		return entryRetirement{}, syscall.EAGAIN
	}
	if state.IfEmpty {
		empty, err := s.isEmpty(ctx, tx, state.NodeID)
		if err != nil {
			return entryRetirement{}, err
		}
		if !empty {
			return entryRetirement{}, syscall.ENOTEMPTY
		}
	}
	return state, nil
}

func requireRetirementRow(result sql.Result, missing error) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return missing
	}
	if count != 1 {
		return syscall.EIO
	}
	return nil
}
