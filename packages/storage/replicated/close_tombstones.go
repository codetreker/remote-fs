package replicated

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type closeTombstoneKey struct {
	owner   *retainedClose
	attempt storage.CloseAttempt
}

type closeTombstone struct {
	key      closeTombstoneKey
	actions  storage.ReferenceCloseActions
	epoch    uint64
	sequence uint64
}

func (s *fileSession) hasCloseTombstone(owner *retainedClose, attempt storage.CloseAttempt) bool {
	s.closeReservationMu.Lock()
	defer s.closeReservationMu.Unlock()
	_, ok := s.closeTombstoneRegistry[closeTombstoneKey{owner: owner, attempt: attempt}]
	return ok
}

func (s *fileSession) retireCloseTombstones() {
	s.closeReservationMu.Lock()
	s.closeTombstonesRetired = true
	s.closeTombstoneRegistry = nil
	s.closeTombstones = 0
	s.closeReservationMu.Unlock()
}

func (s *fileSession) reserveCloseTombstone(ctx context.Context, owner *retainedClose, actions storage.ReferenceCloseActions, attempt storage.CloseAttempt) error {
	key := closeTombstoneKey{owner: owner, attempt: attempt}
	s.closeReservationMu.Lock()
	if s.closeTombstonesRetired {
		s.closeReservationMu.Unlock()
		return syscall.ESTALE
	}
	if _, exists := s.closeTombstoneRegistry[key]; exists {
		s.closeReservationMu.Unlock()
		return nil
	}
	if s.maxCloseActions <= 0 {
		s.closeReservationMu.Unlock()
		return syscall.EAGAIN
	}
	if s.closeTombstones < s.maxCloseActions {
		s.addCloseTombstoneLocked(key, actions)
		s.closeReservationMu.Unlock()
		return nil
	}
	candidates := make([]*closeTombstone, 0, len(s.closeTombstoneRegistry))
	for _, candidate := range s.closeTombstoneRegistry {
		candidates = append(candidates, candidate)
	}
	s.closeReservationMu.Unlock()
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].epoch != candidates[j].epoch {
			return candidates[i].epoch < candidates[j].epoch
		}
		return candidates[i].sequence < candidates[j].sequence
	})

	var queryErr error
	for _, candidate := range candidates {
		receipt, err := candidate.actions.QueryCloseAttempt(ctx, candidate.key.attempt)
		if err != nil {
			queryErr = errors.Join(queryErr, err)
			continue
		}
		if receipt.Action != candidate.key.attempt.Action || receipt.Operation != storage.OpFileClose {
			queryErr = errors.Join(queryErr, fmt.Errorf("close history query changed reference identity: %w", syscall.EIO))
			continue
		}
		if receipt.Outcome != storage.FileActionRetired {
			continue
		}
		s.closeReservationMu.Lock()
		removed := false
		if s.closeTombstoneRegistry[candidate.key] == candidate {
			delete(s.closeTombstoneRegistry, candidate.key)
			s.closeTombstones--
			removed = true
		}
		s.closeReservationMu.Unlock()
		if removed {
			break
		}
	}

	s.closeReservationMu.Lock()
	defer s.closeReservationMu.Unlock()
	if s.closeTombstonesRetired {
		return syscall.ESTALE
	}
	if _, exists := s.closeTombstoneRegistry[key]; exists {
		return nil
	}
	if s.closeTombstones >= s.maxCloseActions {
		return errors.Join(syscall.EAGAIN, queryErr)
	}
	s.addCloseTombstoneLocked(key, actions)
	return nil
}

func (s *fileSession) addCloseTombstoneLocked(key closeTombstoneKey, actions storage.ReferenceCloseActions) {
	if s.closeTombstoneRegistry == nil {
		s.closeTombstoneRegistry = make(map[closeTombstoneKey]*closeTombstone)
	}
	epoch, err := key.attempt.Action.Epoch()
	if err != nil {
		panic("invalid close action admitted to tombstone registry")
	}
	s.closeTombstoneSequence++
	s.closeTombstoneRegistry[key] = &closeTombstone{key: key, actions: actions, epoch: epoch, sequence: s.closeTombstoneSequence}
	s.closeTombstones++
}
