package httprest

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type registryTestBackend struct {
	storage.FileStorage
	open func(context.Context, storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error)
}

func (b registryTestBackend) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	return b.open(ctx, o)
}

type registryTestSession struct {
	storage.FileSession
	close func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
}

func (s *registryTestSession) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.close(ctx, id)
}

type registryIdentityKey struct{}

func registryTestEnrollment(t *testing.T, native *registryTestSession, status storage.FileSessionStatus) (*fileRegistry, *servedFileSession) {
	t.Helper()
	backend := registryTestBackend{open: func(context.Context, storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
		return native, status, nil
	}}
	registry := newFileRegistry(backend, DefaultFileLimits())
	ctx := context.WithValue(context.Background(), registryIdentityKey{}, "original principal")
	done, err := registry.begin()
	if err != nil {
		t.Fatal(err)
	}
	response, err := registry.enroll(ctx, storage.DefaultFileSessionOptions())
	done()
	if err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	session := registry.sessions[response.Session]
	registry.mu.Unlock()
	if session == nil {
		t.Fatal("enrollment did not retain its native session")
	}
	return registry, session
}
func registryTestStatus() storage.FileSessionStatus {
	return storage.FileSessionStatus{Epoch: "native-session", Revision: 1, ActionEpoch: 7, Remaining: 30 * time.Second, HistoryRemaining: time.Minute}
}
func registryTestClosed(id storage.FileActionID) storage.FileActionReceipt {
	return storage.FileActionReceipt{Action: id, Operation: storage.OpFileSessionClose, State: storage.FileActionCompleted, HistoryRemaining: time.Minute}
}
func registryTestWait(t *testing.T, r *fileRegistry) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.wait(ctx)
}

func TestFileRegistryCleanupRetriesOneOwnedAction(t *testing.T) {
	var mu sync.Mutex
	var actions []storage.FileActionID
	native := &registryTestSession{close: func(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		if ctx.Value(registryIdentityKey{}) != "original principal" {
			t.Error("cleanup lost the enrollment principal")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("cleanup has no deadline")
		}
		mu.Lock()
		defer mu.Unlock()
		actions = append(actions, id)
		if len(actions) == 1 {
			return storage.FileActionReceipt{}, syscall.EIO
		}
		return registryTestClosed(id), nil
	}}
	registry, _ := registryTestEnrollment(t, native, registryTestStatus())
	registry.stop()
	if err := registryTestWait(t, registry); !errors.Is(err, syscall.EIO) {
		t.Fatalf("first cleanup error = %v", err)
	}
	registry.mu.Lock()
	remaining := len(registry.sessions)
	registry.mu.Unlock()
	if remaining != 1 {
		t.Fatalf("failed cleanup retained %d sessions", remaining)
	}
	registry.stop()
	if err := registryTestWait(t, registry); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(actions) != 2 || actions[0] != actions[1] {
		t.Fatalf("cleanup action identities = %v", actions)
	}
	epoch, err := actions[0].Epoch()
	if err != nil || epoch != 7 {
		t.Fatalf("cleanup epoch = %d, %v", epoch, err)
	}
	registry.mu.Lock()
	remaining = len(registry.sessions)
	registry.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("confirmed cleanup retained %d sessions", remaining)
	}
}

func TestFileCallerCloseCannotPoisonOwnerCleanup(t *testing.T) {
	caller, err := storage.NewFileActionID(8)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var actions []storage.FileActionID
	native := &registryTestSession{close: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		mu.Lock()
		actions = append(actions, id)
		mu.Unlock()
		if id == caller {
			return storage.FileActionReceipt{}, syscall.ESTALE
		}
		return registryTestClosed(id), nil
	}}
	registry, session := registryTestEnrollment(t, native, registryTestStatus())
	handler := &Handler{files: registry}
	_, err = handler.performFile(context.Background(), session, fileRequest{Op: storage.OpFileSessionClose, Action: caller})
	if !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("future caller action error = %v", err)
	}
	session.mu.Lock()
	poisoned := session.closeAction != "" || session.retired || session.cleanupComplete
	session.mu.Unlock()
	if poisoned {
		t.Fatal("rejected caller action changed transport ownership")
	}
	registry.stop()
	if err := registryTestWait(t, registry); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(actions) != 2 || actions[0] != caller || actions[1] == caller {
		t.Fatalf("close actions = %v", actions)
	}
	epoch, err := actions[1].Epoch()
	if err != nil || epoch != 7 {
		t.Fatalf("owner epoch = %d, %v", epoch, err)
	}
}

func TestFileRegistryStopCancelsAndDrainsAdmittedCalls(t *testing.T) {
	cleaned := make(chan struct{}, 1)
	native := &registryTestSession{close: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		cleaned <- struct{}{}
		return registryTestClosed(id), nil
	}}
	registry, session := registryTestEnrollment(t, native, registryTestStatus())
	releaseRegistry, err := registry.begin()
	if err != nil {
		t.Fatal(err)
	}
	ctx, releaseSession, err := session.begin(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	registry.stop()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not cancel the admitted call")
	}
	select {
	case <-cleaned:
		t.Fatal("cleanup ran while an admitted call still owned the session")
	default:
	}
	releaseSession()
	releaseRegistry()
	if err := registryTestWait(t, registry); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cleaned:
	default:
		t.Fatal("cleanup did not run after the admitted call drained")
	}
	if _, err := registry.begin(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("stopped admission error = %v", err)
	}
}

func TestFileRegistryInvalidEnrollmentDoesNotInventCleanupEpoch(t *testing.T) {
	var mu sync.Mutex
	closes := 0
	native := &registryTestSession{close: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		mu.Lock()
		closes++
		mu.Unlock()
		return registryTestClosed(id), nil
	}}
	status := registryTestStatus()
	status.ActionEpoch = 0
	backend := registryTestBackend{open: func(context.Context, storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
		return native, status, nil
	}}
	registry := newFileRegistry(backend, DefaultFileLimits())
	release, err := registry.begin()
	if err != nil {
		t.Fatal(err)
	}
	response, err := registry.enroll(context.Background(), storage.DefaultFileSessionOptions())
	release()
	if !errors.Is(err, syscall.EIO) || response.Session != "" {
		t.Fatalf("invalid enrollment = %#v, %v", response, err)
	}
	registry.stop()
	if err := registryTestWait(t, registry); err == nil {
		t.Fatal("cleanup without a confirmed epoch succeeded")
	}
	mu.Lock()
	count := closes
	mu.Unlock()
	if count != 0 {
		t.Fatalf("native Close received %d invented identities", count)
	}
	registry.mu.Lock()
	if len(registry.sessions) != 1 {
		t.Fatalf("unknown native ownership was dropped: %d sessions", len(registry.sessions))
	}
	var retained *servedFileSession
	for _, session := range registry.sessions {
		retained = session
	}
	registry.mu.Unlock()
	// A confirmed status repairs the fixture's broken constructor contract; the
	// next cleanup must still use the same retained native owner.
	status.ActionEpoch = 7
	if _, err := retained.observe(status, time.Now()); err != nil {
		t.Fatal(err)
	}
	registry.stop()
	if err := registryTestWait(t, registry); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	count = closes
	mu.Unlock()
	if count != 1 {
		t.Fatalf("recovered cleanup calls = %d", count)
	}
}

func TestFileRegistryTerminalCleanupFactHasNoInventedHistory(t *testing.T) {
	for _, invalidFirst := range []bool{false, true} {
		name := "terminal fact"
		if invalidFirst {
			name = "reject historical claim"
		}
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var actions []storage.FileActionID
			native := &registryTestSession{close: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
				mu.Lock()
				defer mu.Unlock()
				actions = append(actions, id)
				result := storage.FileActionReceipt{Operation: storage.OpFileSessionClose, State: storage.FileActionRetired}
				if invalidFirst && len(actions) == 1 {
					result.Action = id
				}
				return result, nil
			}}
			registry, _ := registryTestEnrollment(t, native, registryTestStatus())
			registry.stop()
			err := registryTestWait(t, registry)
			if invalidFirst {
				if !errors.Is(err, syscall.EIO) {
					t.Fatalf("invented historical fact accepted: %v", err)
				}
				registry.mu.Lock()
				retained := len(registry.sessions)
				registry.mu.Unlock()
				if retained != 1 {
					t.Fatal("malformed terminal result dropped cleanup ownership")
				}
				registry.stop()
				err = registryTestWait(t, registry)
			}
			if err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if invalidFirst && (len(actions) != 2 || actions[0] != actions[1]) {
				t.Fatalf("terminal cleanup retry identities = %v", actions)
			}
			registry.mu.Lock()
			retained := len(registry.sessions)
			registry.mu.Unlock()
			if retained != 0 {
				t.Fatal("confirmed terminal identity retained cleanup ownership")
			}
		})
	}
}

func TestFileRegistryTinyValidLeaseCannotExhaustCleanupCapacity(t *testing.T) {
	cleaned := make(chan struct{}, 1)
	native := &registryTestSession{close: func(ctx context.Context, _ storage.FileActionID) (storage.FileActionReceipt, error) {
		if err := ctx.Err(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) < time.Second {
			t.Error("cleanup inherited the caller's tiny lease")
		}
		cleaned <- struct{}{}
		return storage.FileActionReceipt{Operation: storage.OpFileSessionClose, State: storage.FileActionRetired}, nil
	}}
	status := registryTestStatus()
	status.Remaining = time.Nanosecond
	status.HistoryRemaining = time.Nanosecond
	backend := registryTestBackend{open: func(context.Context, storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
		return native, status, nil
	}}
	limits := DefaultFileLimits()
	limits.MaxSessions = 1
	registry := newFileRegistry(backend, limits)
	t.Cleanup(func() {
		registry.stop()
		if err := registryTestWait(t, registry); err != nil {
			t.Error(err)
		}
	})
	options := storage.DefaultFileSessionOptions()
	options.Lease = time.Nanosecond
	options.History = time.Nanosecond
	if err := options.Check(); err != nil {
		t.Fatal(err)
	}
	done, err := registry.begin()
	if err != nil {
		t.Fatal(err)
	}
	response, err := registry.enroll(context.Background(), options)
	done()
	if err == nil || response.Session != "" {
		t.Fatalf("expired enrollment = %+v, %v", response, err)
	}
	select {
	case <-cleaned:
	case <-time.After(5 * time.Second):
		t.Fatal("expired enrollment never reached native cleanup with a live context")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		registry.mu.Lock()
		remaining := len(registry.sessions)
		registry.mu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("confirmed cleanup retained the only session slot")
		}
		time.Sleep(time.Millisecond)
	}
}
