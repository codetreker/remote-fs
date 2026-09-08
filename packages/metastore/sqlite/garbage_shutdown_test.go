package sqlite

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type observedMaintenanceForget struct {
	metastore.Store
	entered  chan context.Context
	finished chan error
}

func (m *observedMaintenanceForget) Forget(ctx context.Context, keys []metastore.Key) error {
	m.entered <- ctx
	err := m.Store.Forget(ctx, keys)
	m.finished <- err
	return err
}

func TestObjectstoreCloseDrainsAdmittedForgetAndCancelsQueuedForget(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		t.Run(fmt.Sprintf("finalization admitted %t", admitted), func(t *testing.T) {
			config := lockingTestConfig(t)
			opened, err := OpenLocking(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			s := opened.Store
			defer func() {
				if err := opened.Close(); err != nil {
					t.Error(err)
				}
			}()
			key, err := s.Reserve(t.Context(), "file", 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Abandon(t.Context(), key); err != nil {
				t.Fatal(err)
			}
			objects := memory.New()
			if _, err := objects.Put(t.Context(), string(key), []byte("x")); err != nil {
				t.Fatal(err)
			}
			var release func()
			if admitted {
				s.coordinator.health.RLock()
				release = sync.OnceFunc(s.coordinator.health.RUnlock)
			} else {
				if err := s.coordinator.commit.acquire(t.Context()); err != nil {
					t.Fatal(err)
				}
				release = sync.OnceFunc(s.coordinator.commit.release)
			}
			defer release()
			meta := &observedMaintenanceForget{Store: opened, entered: make(chan context.Context, 1), finished: make(chan error, 1)}
			backing := objectstore.New(objects, meta)
			defer func() {
				release()
				if err := backing.Close(); err != nil {
					t.Error(err)
				}
			}()
			var maintenance context.Context
			select {
			case maintenance = <-meta.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("maintenance did not reach native Forget")
			}
			if admitted {
				until := time.Now().Add(5 * time.Second)
				for s.coordinator.health.TryRLock() {
					s.coordinator.health.RUnlock()
					if time.Now().After(until) {
						t.Fatal("maintenance did not reach native finalization")
					}
					runtime.Gosched()
				}
			}
			closed := make(chan error, 1)
			go func() { closed <- backing.Close() }()
			select {
			case <-maintenance.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not cancel the maintenance lifetime")
			}
			select {
			case err := <-closed:
				t.Fatalf("Close completed before native drain: %v", err)
			default:
			}
			var forgetErr error
			if !admitted {
				select {
				case forgetErr = <-meta.finished:
				case <-time.After(5 * time.Second):
					t.Fatal("queued Forget did not honor Close cancellation")
				}
				if storage.ErrnoOf(forgetErr) != syscall.EINTR || !errors.Is(forgetErr, context.Canceled) {
					t.Errorf("queued Forget returned %v", forgetErr)
				}
			}
			release()
			if admitted {
				select {
				case forgetErr = <-meta.finished:
				case <-time.After(5 * time.Second):
					t.Fatal("admitted Forget did not finish")
				}
				if forgetErr != nil {
					t.Errorf("admitted Forget returned %v", forgetErr)
				}
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("Close after native drain: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not finish after native drain")
			}
			config.Initialize = false
			reopened, err := OpenLocking(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			remaining, err := reopened.Garbage(t.Context(), 1)
			want := 1
			if admitted {
				want = 0
			}
			if err != nil || len(remaining) != want || (!admitted && remaining[0] != key) {
				t.Errorf("reopened garbage=%v, error=%v, want %d retained records", remaining, err, want)
			}
		})
	}
}
