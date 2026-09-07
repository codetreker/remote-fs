package sqlite

import (
	"errors"
	"testing"
)

func TestSQLitePublicationFenceDoesNotRetainClosedPoolBookkeeping(t *testing.T) {
	for _, abort := range []bool{false, true} {
		name := "close"
		if abort {
			name = "abort"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newPublicationFixture(t)
			fence := errors.New("publication outcome unavailable")
			*fixture.authorityFence = fence
			fixture.store.locks.Fence(fence)
			closeStore := fixture.store.Close
			if abort {
				closeStore = fixture.store.Abort
			}
			if err := closeStore(); !errors.Is(err, fence) {
				t.Fatalf("closing a fenced namespace returned %v, want original fence", err)
			}
			databaseCoordinators.Lock()
			retained := databaseCoordinators.byPath[fixture.store.coordinator.key]
			databaseCoordinators.Unlock()
			if retained != nil {
				t.Fatal("successfully closed SQL pools retained a coordinator reference")
			}
		})
	}
}
