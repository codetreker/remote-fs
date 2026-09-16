package fuse

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
)

func TestLocalOwnerCloseGenerationsAndBounds(t *testing.T) {
	owners := advisoryOwners{limit: 1}
	key := advisoryOwnerKey{node: 1, owner: 2, family: posixFamily}
	state, release, err := owners.pin(key)
	if err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	before := state.generation
	state.pid.Store(37)
	state.retireGeneration()
	if !errors.Is(before.Err(), context.Canceled) || state.generation.Err() != nil {
		t.Fatal("close did not retire only the old generation")
	}
	state.mu.Unlock()
	owners.remember(key, true)
	release()
	if owners.process(key) != 37 {
		t.Fatal("lost local PID for held owner")
	}
	if _, _, err := owners.pin(advisoryOwnerKey{node: 2}); !errors.Is(err, syscall.ENOLCK) {
		t.Fatalf("owner overflow = %v", err)
	}
	_, release, err = owners.pin(key)
	if err != nil {
		t.Fatal(err)
	}
	owners.remember(key, false)
	release()
	if owners.process(key) != 0 || len(owners.entries) != 0 {
		t.Fatal("unlocked owner retained")
	}
	for range 32 {
		_, release, err := owners.pin(key)
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	if len(owners.entries) != 0 {
		t.Fatal("historical owners consume live capacity")
	}
}

func TestLocalOwnerPinsPreserveSharedState(t *testing.T) {
	owners := advisoryOwners{limit: 1}
	key := advisoryOwnerKey{node: 1, owner: 0, family: flockFamily}
	state, release, err := owners.pin(key)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			other, done, err := owners.pin(key)
			if err != nil {
				t.Error(err)
				return
			}
			defer done()
			if other != state {
				t.Error("duplicated local owner")
			}
			_ = owners.process(key)
		})
	}
	group.Wait()
	if len(owners.entries) != 1 {
		t.Fatal("live call lost owner state")
	}
	release()
	if len(owners.entries) != 0 {
		t.Fatal("last call did not release unheld owner")
	}
}
