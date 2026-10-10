package smb

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestAuthorityFailureDrainsAcceptedTreeOwnersBeforeSession(t *testing.T) {
	server := &Server{config: Config{Limits: DefaultLimits()}}
	raw := newEndpointFileSession()
	export := &Export{server: server, refs: 1, trees: 1}
	s := &session{id: 4, trees: make(map[uint32]*tree), authorities: make(map[*Export]*authoritySession)}
	c := &connection{server: server, pending: make(map[uint64]*pendingRequest)}
	ready, done := make(chan struct{}), make(chan struct{})
	close(ready)
	close(done)
	a := &authoritySession{raw: raw, export: export, connection: c, smbSession: s, refs: 1, ready: ready, done: done}
	tree := &tree{id: 1, sessionID: s.id, export: export, authority: a, done: make(chan struct{})}
	s.trees[tree.id] = tree
	s.authorities[export] = a
	if !tree.beginFileWork(s) {
		t.Fatal("accepted work refused")
	}
	h, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	file := &ownerActionFile{status: func() (storage.CloseOwnerStatus, error) {
		raw.mu.Lock()
		closes := raw.closes
		raw.mu.Unlock()
		if closes != 0 {
			t.Fatal("raw session closed before own handle")
		}
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
	}, close: func(storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}}
	h.file = file
	h.published = true
	finished := make(chan struct{})
	go func() { a.retireAfterRenewalFailure(t.Context(), syscall.ESTALE, time.Second); close(finished) }()
	select {
	case <-tree.done:
	case <-time.After(time.Second):
		t.Fatal("retirement did not fence tree")
	}
	if tree.beginFileWork(s) {
		t.Fatal("retirement admitted another operation")
	}
	raw.mu.Lock()
	closes := raw.closes
	raw.mu.Unlock()
	if closes != 0 || tree.findFileHandle(h.id) == nil {
		t.Fatal("accepted work did not retain its owner")
	}
	tree.endFileWork()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("retirement did not drain")
	}
	raw.mu.Lock()
	closes = raw.closes
	raw.mu.Unlock()
	if closes != 1 || !tree.closed || len(s.trees) != 0 || len(s.authorities) != 0 || len(server.HandleOwners()) != 0 {
		t.Fatalf("retirement retained resources: closes=%d trees=%d authorities=%d", closes, len(s.trees), len(s.authorities))
	}
}

func TestHandleDiagnosticsRetainOpaqueOwnerThroughSettlement(t *testing.T) {
	server := &Server{config: Config{Limits: DefaultLimits()}}
	tree := &tree{id: 6, export: &Export{server: server}}
	s := &session{id: 5}
	if !tree.beginFileWork(s) {
		t.Fatal("admission refused")
	}
	h, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	h.action, err = storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	h.recordOpenDiagnostic()
	owners := server.HandleOwners()
	if len(owners) != 1 || owners[0].State != "opening" || owners[0].FileID != h.id || owners[0].OpenAction != h.action || owners[0].TreeID != tree.id {
		t.Fatalf("opening diagnostics: %+v", owners)
	}
	closes := 0
	h.file = &ownerActionFile{status: func() (storage.CloseOwnerStatus, error) {
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
	}, close: func(storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
		closes++
		if closes == 1 {
			return storage.ReferenceCloseResult{Released: true, Determined: true}, &storage.CloseSettlementError{State: storage.CloseSettlementPending, Cause: syscall.EIO}
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}}
	tree.endFileWork()
	if err := tree.closeFileHandle(t.Context(), h); !errors.Is(err, syscall.EIO) {
		t.Fatalf("pending %v", err)
	}
	owners = server.HandleOwners()
	if len(owners) != 1 || owners[0].State != "barrier-only" || owners[0].CloseAttempt.Action == "" || owners[0].LastStatus != statusIO {
		t.Fatalf("settlement diagnostics: %+v", owners)
	}
	if err := tree.closeFileHandle(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if len(server.HandleOwners()) != 0 || tree.findFileHandle(h.id) != nil {
		t.Fatal("settled owner still charged")
	}
}
