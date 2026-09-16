package fuse

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
)

type advisoryOwnerKey struct {
	node   uint64
	owner  lockOwner
	family lockFamily
}

// A local generation serializes close-owner cleanup against acquisitions that
// are still waiting. The authoritative ranges remain in the file session.
type advisoryOwnerState struct {
	mu         sync.Mutex
	generation context.Context
	cancel     context.CancelFunc
	pid        atomic.Uint32
}

type advisoryOwnerRecord struct {
	state *advisoryOwnerState
	users int
	held  bool
}

type advisoryOwners struct {
	mu      sync.Mutex
	limit   int
	entries map[advisoryOwnerKey]*advisoryOwnerRecord
}

func (m *advisoryOwners) pinExisting(key advisoryOwnerKey) (*advisoryOwnerState, func(), error) {
	m.mu.Lock()
	exists := m.entries[key] != nil
	m.mu.Unlock()
	if !exists {
		return nil, nil, nil
	}
	return m.pin(key)
}

func (m *advisoryOwners) pin(key advisoryOwnerKey) (*advisoryOwnerState, func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record := m.entries[key]
	if record == nil {
		if len(m.entries) >= m.limit {
			return nil, nil, syscall.ENOLCK
		}
		if m.entries == nil {
			m.entries = make(map[advisoryOwnerKey]*advisoryOwnerRecord)
		}
		generation, cancel := context.WithCancel(context.Background())
		record = &advisoryOwnerRecord{state: &advisoryOwnerState{generation: generation, cancel: cancel}}
		m.entries[key] = record
	}
	record.users++
	return record.state, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		record.users--
		if record.users == 0 && !record.held {
			record.state.cancel()
			delete(m.entries, key)
		}
	}, nil
}

func (m *advisoryOwners) remember(key advisoryOwnerKey, held bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[key].held = held
}

func (m *advisoryOwners) process(key advisoryOwnerKey) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	record := m.entries[key]
	if record == nil {
		return 0
	}
	return record.state.pid.Load()
}

func (s *advisoryOwnerState) retireGeneration() {
	s.cancel()
	s.generation, s.cancel = context.WithCancel(context.Background())
}
