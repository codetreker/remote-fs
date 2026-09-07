package objectstore_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type measuredObjects struct {
	objectstore.Objects
	available    int64
	availableErr error
	closeErr     error
	availableAsk atomic.Int64
	closed       func()
}

func (o *measuredObjects) Available(context.Context) (int64, error) {
	o.availableAsk.Add(1)
	return o.available, o.availableErr
}

func (o *measuredObjects) Close() error {
	if o.closed != nil {
		o.closed()
	}
	return errors.Join(o.Objects.Close(), o.closeErr)
}

type wrappedStore struct {
	metastore.Store
	space    storage.Space
	spaceErr error
	closeErr error
	closed   func()
}

func (s *wrappedStore) Space(context.Context) (storage.Space, error) {
	return s.space, s.spaceErr
}

func (s *wrappedStore) Close() error {
	if s.closed != nil {
		s.closed()
	}
	return errors.Join(s.Store.Close(), s.closeErr)
}

func composedStore(t *testing.T, allowance int64, objects objectstore.Objects) (*objectstore.Storage, *sqlite.Store) {
	t.Helper()
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", allowance, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	namespace := objectstore.New(objects, meta)
	t.Cleanup(func() {
		if err := namespace.Close(); err != nil {
			t.Errorf("closing the namespace: %v", err)
		}
	})
	return namespace, meta
}

func await(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

func TestSpaceUsesTheTighterMeasuredAvailability(t *testing.T) {
	for _, test := range []struct {
		name          string
		available     int64
		availableErr  error
		wantAvailable int64
	}{
		{name: "physical storage is tighter", available: 25, wantAvailable: 25},
		{name: "workspace quota is tighter", available: 10000, wantAvailable: limited.MinLimit - 30},
		{name: "object store has no figure", availableErr: syscall.ENOSYS, wantAvailable: limited.MinLimit - 30},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := &measuredObjects{
				Objects:      memory.New(),
				available:    test.available,
				availableErr: test.availableErr,
			}
			namespace, _ := composedStore(t, limited.MinLimit, objects)
			if err := namespace.Write(t.Context(), "f", make([]byte, 30)); err != nil {
				t.Fatalf("writing the measured content: %v", err)
			}

			space, err := namespace.Space(t.Context())
			if err != nil {
				t.Fatalf("space: %v", err)
			}
			want := storage.Space{Total: limited.MinLimit, Used: 30, Avail: test.wantAvailable}
			if space != want {
				t.Fatalf("space reported %+v, want %+v", space, want)
			}
		})
	}
}

func TestSpacePropagatesMeasurementFailures(t *testing.T) {
	for _, joinedUnsupported := range []bool{false, true} {
		physicalFailure := errors.New("the filesystem refused its capacity measurement")
		measurementErr := error(physicalFailure)
		if joinedUnsupported {
			measurementErr = errors.Join(syscall.ENOSYS, physicalFailure)
		}
		objects := &measuredObjects{Objects: memory.New(), availableErr: measurementErr}
		namespace, _ := composedStore(t, limited.MinLimit, objects)

		if _, err := namespace.Space(t.Context()); !errors.Is(err, physicalFailure) {
			t.Fatalf("space failed with %v, want the physical measurement failure", err)
		}
	}
}

func TestSpaceRejectsAnImpossiblePhysicalMeasurement(t *testing.T) {
	objects := &measuredObjects{Objects: memory.New(), available: -1}
	namespace, _ := composedStore(t, limited.MinLimit, objects)

	if _, err := namespace.Space(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("space failed with %v, want EIO", err)
	}
}

func TestSpaceDoesNotHideAMetastoreFailureBehindPhysicalCapacity(t *testing.T) {
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	metaFailure := errors.New("the quota ledger is unavailable")
	store := &wrappedStore{Store: meta, spaceErr: metaFailure}
	objects := &measuredObjects{Objects: memory.New(), available: 100}
	namespace := objectstore.New(objects, store)
	t.Cleanup(func() { _ = namespace.Close() })

	if _, err := namespace.Space(t.Context()); !errors.Is(err, metaFailure) {
		t.Fatalf("space failed with %v, want the metastore failure", err)
	}
	if calls := objects.availableAsk.Load(); calls != 0 {
		t.Fatalf("physical availability was measured %d times after the quota ledger had already failed", calls)
	}
}

type closeTrace struct {
	mu    sync.Mutex
	steps []string
}

func (t *closeTrace) add(step string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.steps = append(t.steps, step)
}

func (t *closeTrace) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.steps...)
}

func TestCloseReleasesBothHalvesInOrderAndReturnsBothFailures(t *testing.T) {
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	metaFailure := errors.New("metastore close failed")
	objectsFailure := errors.New("object-store close failed")
	trace := new(closeTrace)
	store := &wrappedStore{
		Store:    meta,
		closeErr: metaFailure,
		closed:   func() { trace.add("metastore") },
	}
	objects := &measuredObjects{
		Objects:  memory.New(),
		closeErr: objectsFailure,
		closed:   func() { trace.add("objects") },
	}
	namespace := objectstore.New(objects, store)

	const callers = 8
	results := make(chan error, callers)
	for range callers {
		go func() { results <- namespace.Close() }()
	}
	for range callers {
		err := <-results
		if !errors.Is(err, metaFailure) || !errors.Is(err, objectsFailure) {
			t.Errorf("close returned %v, want both close failures", err)
		}
	}

	steps := trace.snapshot()
	if fmt.Sprint(steps) != "[metastore objects]" {
		t.Fatalf("close order was %v, want metastore then object store exactly once", steps)
	}
}

type blockedPutObjects struct {
	objectstore.Objects
	started chan struct{}
	release chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (o *blockedPutObjects) Put(ctx context.Context, key string, content []byte) ([]byte, error) {
	close(o.started)
	select {
	case <-o.release:
		return o.Objects.Put(ctx, key, content)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (o *blockedPutObjects) Close() error {
	o.once.Do(func() { close(o.closed) })
	return o.Objects.Close()
}

func TestAnotherStorageCannotCollectALiveReservation(t *testing.T) {
	database := filepath.Join(t.TempDir(), "meta.db")
	writerMeta, err := sqlite.Open(t.Context(), database, "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	sweeperMeta, err := sqlite.Open(t.Context(), database, "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		writerMeta.Close()
		t.Fatal(err)
	}
	objects := memory.New()
	blocked := &blockedPutObjects{
		Objects: objects,
		started: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	writer := objectstore.New(blocked, writerMeta)
	sweeper := objectstore.New(objects, sweeperMeta)
	t.Cleanup(func() {
		_ = writer.Close()
		_ = sweeper.Close()
	})

	written := make(chan error, 1)
	go func() { written <- writer.Write(t.Context(), "f", []byte("long upload")) }()
	<-blocked.started
	if removed, err := sweeper.Sweep(t.Context(), 100); err != nil || removed != 0 {
		t.Fatalf("the other Storage swept %d live reservations (%v), want none", removed, err)
	}
	close(blocked.release)
	if err := <-written; err != nil {
		t.Fatalf("the long-running write failed after the other Storage swept: %v", err)
	}
	content, err := sweeper.Read(t.Context(), "f")
	if err != nil || string(content) != "long upload" {
		t.Fatalf("the completed long-running write reads as %q (%v)", content, err)
	}
}

func TestCloseDrainsAnAdmittedNamespaceOperation(t *testing.T) {
	objects := &blockedPutObjects{
		Objects: memory.New(),
		started: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	namespace, _ := composedStore(t, limited.MinLimit, objects)
	written := make(chan error, 1)
	go func() { written <- namespace.Write(t.Context(), "f", []byte("content")) }()
	<-objects.started

	closed := make(chan error, 1)
	go func() { closed <- namespace.Close() }()
	select {
	case <-objects.closed:
		t.Fatal("the object store closed while an admitted write was still in progress")
	case <-time.After(25 * time.Millisecond):
	}
	deadline := time.Now().Add(2 * time.Second)
	refused := false
	for !refused {
		late := make(chan error, 1)
		go func() {
			_, err := namespace.Space(t.Context())
			late <- err
		}()
		select {
		case err := <-late:
			if errors.Is(err, syscall.EIO) {
				refused = true
				continue
			}
			if err != nil {
				t.Fatalf("late operation failed with %v before close ownership was visible", err)
			}
			if time.Now().After(deadline) {
				t.Fatal("close never began refusing new namespace operations")
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("a namespace operation arriving during close blocked behind the close gate")
		}
	}
	close(objects.release)
	if err := <-written; err != nil {
		t.Fatalf("the admitted write failed during close: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-objects.closed:
	default:
		t.Fatal("the object store remained open after the admitted write completed")
	}
}

func TestClosedNamespaceOperationsFailWithEIO(t *testing.T) {
	namespace, _ := composedStore(t, limited.MinLimit, memory.New())
	if err := namespace.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	mode := storage.Attr{}.Mode
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{name: "stat", run: func() error { _, err := namespace.Stat(t.Context(), "f"); return err }},
		{name: "setattr", run: func() error { return namespace.SetAttr(t.Context(), "f", storage.AttrChange{Mode: &mode}) }},
		{name: "list", run: func() error { _, err := namespace.List(t.Context(), ""); return err }},
		{name: "read", run: func() error { _, err := namespace.Read(t.Context(), "f"); return err }},
		{name: "write", run: func() error { return namespace.Write(t.Context(), "f", nil) }},
		{name: "create", run: func() error { return namespace.Create(t.Context(), "f") }},
		{name: "mkdir", run: func() error { return namespace.Mkdir(t.Context(), "d") }},
		{name: "remove", run: func() error { return namespace.Remove(t.Context(), "f") }},
		{name: "removedir", run: func() error { return namespace.RemoveDir(t.Context(), "d") }},
		{name: "rename", run: func() error { return namespace.Rename(t.Context(), "from", "to") }},
		{name: "space", run: func() error { _, err := namespace.Space(t.Context()); return err }},
		{name: "sweep", run: func() error { _, err := namespace.Sweep(t.Context(), 1); return err }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.run(); !errors.Is(err, syscall.EIO) {
				t.Fatalf("operation returned %v, want EIO", err)
			}
		})
	}
}

type blockedMetastore struct {
	metastore.Store
	method  string
	started chan struct{}
	release chan struct{}
	once    sync.Once
	enabled atomic.Bool
}

func (s *blockedMetastore) block(ctx context.Context, method string) error {
	if !s.enabled.Load() || s.method != method {
		return nil
	}
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockedMetastore) Stat(ctx context.Context, path string) (metastore.Node, error) {
	if err := s.block(ctx, "stat"); err != nil {
		return metastore.Node{}, err
	}
	return s.Store.Stat(ctx, path)
}

func (s *blockedMetastore) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	if err := s.block(ctx, "setattr"); err != nil {
		return err
	}
	return s.Store.SetAttr(ctx, path, change)
}

func (s *blockedMetastore) List(ctx context.Context, path string) ([]metastore.Child, error) {
	if err := s.block(ctx, "list"); err != nil {
		return nil, err
	}
	return s.Store.List(ctx, path)
}

func (s *blockedMetastore) Create(ctx context.Context, path string) error {
	if err := s.block(ctx, "create"); err != nil {
		return err
	}
	return s.Store.Create(ctx, path)
}

func (s *blockedMetastore) Mkdir(ctx context.Context, path string) error {
	if err := s.block(ctx, "mkdir"); err != nil {
		return err
	}
	return s.Store.Mkdir(ctx, path)
}

func (s *blockedMetastore) Remove(ctx context.Context, path string) error {
	if err := s.block(ctx, "remove"); err != nil {
		return err
	}
	return s.Store.Remove(ctx, path)
}

func (s *blockedMetastore) RemoveDir(ctx context.Context, path string) error {
	if err := s.block(ctx, "removedir"); err != nil {
		return err
	}
	return s.Store.RemoveDir(ctx, path)
}

func (s *blockedMetastore) Rename(ctx context.Context, from, to string) error {
	if err := s.block(ctx, "rename"); err != nil {
		return err
	}
	return s.Store.Rename(ctx, from, to)
}

func (s *blockedMetastore) Reserve(ctx context.Context, path string, size int64) (metastore.Key, error) {
	if err := s.block(ctx, "reserve"); err != nil {
		return "", err
	}
	return s.Store.Reserve(ctx, path, size)
}

func (s *blockedMetastore) Space(ctx context.Context) (storage.Space, error) {
	if err := s.block(ctx, "space"); err != nil {
		return storage.Space{}, err
	}
	return s.Store.Space(ctx)
}

func (s *blockedMetastore) Garbage(ctx context.Context, limit int) ([]metastore.Key, error) {
	if err := s.block(ctx, "garbage"); err != nil {
		return nil, err
	}
	return s.Store.Garbage(ctx, limit)
}

func TestCloseDrainsEveryAdmittedNamespaceMethod(t *testing.T) {
	mode := storage.Attr{}.Mode
	for _, operation := range []struct {
		name   string
		method string
		run    func(*objectstore.Storage) error
	}{
		{name: "stat", method: "stat", run: func(s *objectstore.Storage) error { _, err := s.Stat(t.Context(), "missing"); return err }},
		{name: "setattr", method: "setattr", run: func(s *objectstore.Storage) error {
			return s.SetAttr(t.Context(), "missing", storage.AttrChange{Mode: &mode})
		}},
		{name: "list", method: "list", run: func(s *objectstore.Storage) error { _, err := s.List(t.Context(), ""); return err }},
		{name: "read", method: "stat", run: func(s *objectstore.Storage) error { _, err := s.Read(t.Context(), "missing"); return err }},
		{name: "write", method: "reserve", run: func(s *objectstore.Storage) error { return s.Write(t.Context(), "written", []byte("content")) }},
		{name: "create", method: "create", run: func(s *objectstore.Storage) error { return s.Create(t.Context(), "created") }},
		{name: "mkdir", method: "mkdir", run: func(s *objectstore.Storage) error { return s.Mkdir(t.Context(), "directory") }},
		{name: "remove", method: "remove", run: func(s *objectstore.Storage) error { return s.Remove(t.Context(), "missing") }},
		{name: "removedir", method: "removedir", run: func(s *objectstore.Storage) error { return s.RemoveDir(t.Context(), "missing") }},
		{name: "rename", method: "rename", run: func(s *objectstore.Storage) error { return s.Rename(t.Context(), "missing", "renamed") }},
		{name: "space", method: "space", run: func(s *objectstore.Storage) error { _, err := s.Space(t.Context()); return err }},
		{name: "sweep", method: "garbage", run: func(s *objectstore.Storage) error { _, err := s.Sweep(t.Context(), 1); return err }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
			if err != nil {
				t.Fatalf("opening the metastore: %v", err)
			}
			blocked := &blockedMetastore{
				Store:   meta,
				method:  operation.method,
				started: make(chan struct{}),
				release: make(chan struct{}),
			}
			objectClosed := make(chan struct{})
			objects := &measuredObjects{Objects: memory.New(), closed: func() { close(objectClosed) }}
			namespace := objectstore.New(objects, blocked)
			await(t, "the initial maintenance probe", func() bool {
				return !namespace.MaintenanceStatus().LastSweepTime.IsZero()
			})
			blocked.enabled.Store(true)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
			t.Cleanup(func() {
				release()
				_ = namespace.Close()
			})

			finished := make(chan error, 1)
			go func() { finished <- operation.run(namespace) }()
			select {
			case <-blocked.started:
			case <-time.After(2 * time.Second):
				t.Fatal("the namespace operation never reached its dependency")
			}
			closed := make(chan error, 1)
			go func() { closed <- namespace.Close() }()
			select {
			case <-objectClosed:
				t.Fatal("the object store closed while the namespace operation was admitted")
			case <-time.After(10 * time.Millisecond):
			}
			release()
			if err := <-finished; errors.Is(err, syscall.EIO) {
				t.Fatalf("the admitted operation collided with close: %v", err)
			}
			if err := <-closed; err != nil {
				t.Fatalf("close: %v", err)
			}
		})
	}
}

type maintenanceObjects struct {
	objectstore.Objects
	mu        sync.Mutex
	deleteErr error
}

func (o *maintenanceObjects) Delete(ctx context.Context, key string) error {
	o.mu.Lock()
	err := o.deleteErr
	o.mu.Unlock()
	if err != nil {
		return err
	}
	return o.Objects.Delete(ctx, key)
}

func (o *maintenanceObjects) failDeletes(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deleteErr = err
}

func TestMaintenanceStatusRetainsAndClearsSweepFailures(t *testing.T) {
	objects := &maintenanceObjects{Objects: memory.New()}
	namespace, _ := composedStore(t, limited.MinLimit, objects)
	ctx := t.Context()

	if err := namespace.Write(ctx, "f", []byte("first")); err != nil {
		t.Fatalf("writing the first contents: %v", err)
	}
	failure := errors.New("garbage deletion failed")
	objects.failDeletes(failure)
	if err := namespace.Write(ctx, "f", []byte("second")); err != nil {
		t.Fatalf("the committed replacement was changed by maintenance failure: %v", err)
	}

	var failed objectstore.MaintenanceStatus
	await(t, "the failed mutation-triggered sweep", func() bool {
		failed = namespace.MaintenanceStatus()
		return errors.Is(failed.LastSweepError, failure)
	})
	if failed.LastSweepTime.IsZero() || failed.LastSweepRemoved != 0 || !errors.Is(failed.LastSweepError, failure) {
		t.Fatalf("failed maintenance status is %+v, want the retained delete failure", failed)
	}
	if removed, err := namespace.Sweep(ctx, 0); err != nil || removed != 0 {
		t.Fatalf("zero-limit sweep removed %d objects (%v), want a no-op", removed, err)
	}
	if afterNoop := namespace.MaintenanceStatus(); afterNoop != failed {
		t.Fatalf("zero-limit sweep changed status from %+v to %+v", failed, afterNoop)
	}

	objects.failDeletes(nil)
	removed, err := namespace.Sweep(ctx, 10)
	if err != nil || removed != 1 {
		t.Fatalf("recovered sweep removed %d objects (%v), want one", removed, err)
	}
	recovered := namespace.MaintenanceStatus()
	if recovered.LastSweepTime.Before(failed.LastSweepTime) || recovered.LastSweepRemoved != 1 || recovered.LastSweepError != nil {
		t.Fatalf("recovered maintenance status is %+v, want one removal and no failure", recovered)
	}
}

func TestPeriodicMaintenanceRetriesATransientFailureWithoutAnotherMutation(t *testing.T) {
	objects := &maintenanceObjects{Objects: memory.New()}
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	namespace, err := objectstore.NewWithOptions(objects, meta, objectstore.Options{
		SweepInterval: 10 * time.Millisecond,
		SweepBatch:    1,
	})
	if err != nil {
		t.Fatalf("creating background maintenance: %v", err)
	}
	t.Cleanup(func() {
		if err := namespace.Close(); err != nil {
			t.Errorf("closing the namespace: %v", err)
		}
	})
	await(t, "the initial maintenance pass", func() bool {
		return !namespace.MaintenanceStatus().LastSweepTime.IsZero()
	})

	if err := namespace.Write(t.Context(), "f", []byte("first")); err != nil {
		t.Fatalf("writing initial contents: %v", err)
	}
	first, err := meta.Stat(t.Context(), "f")
	if err != nil {
		t.Fatalf("reading the initial object key: %v", err)
	}
	failure := errors.New("transient delete failure")
	objects.failDeletes(failure)
	if err := namespace.Write(t.Context(), "f", []byte("replacement")); err != nil {
		t.Fatalf("writing replacement contents: %v", err)
	}
	var failedAt time.Time
	await(t, "the transient maintenance failure", func() bool {
		status := namespace.MaintenanceStatus()
		if errors.Is(status.LastSweepError, failure) {
			failedAt = status.LastSweepTime
			return true
		}
		return false
	})

	objects.failDeletes(nil)
	await(t, "the periodic retry after backend recovery", func() bool {
		status := namespace.MaintenanceStatus()
		_, getErr := objects.Get(t.Context(), string(first.Content))
		return status.LastSweepTime.After(failedAt) && status.LastSweepError == nil && errors.Is(getErr, syscall.ENOENT)
	})
	content, err := namespace.Read(t.Context(), "f")
	if err != nil || string(content) != "replacement" {
		t.Fatalf("the recovered namespace reads %q (%v), want replacement", content, err)
	}
}

type secondDeleteFails struct {
	objectstore.Objects
	calls   int
	failure error
}

func (o *secondDeleteFails) Delete(ctx context.Context, key string) error {
	o.calls++
	if o.calls == 2 {
		return o.failure
	}
	return o.Objects.Delete(ctx, key)
}

type forgetFails struct {
	metastore.Store
	failure error
}

func (s *forgetFails) Forget(context.Context, []metastore.Key) error { return s.failure }

func putDirect(t *testing.T, meta metastore.Store, objects objectstore.Objects, path string, content []byte) {
	t.Helper()
	key, err := meta.Reserve(t.Context(), path, int64(len(content)))
	if err != nil {
		t.Fatalf("reserving %q: %v", path, err)
	}
	digest, err := objects.Put(t.Context(), string(key), content)
	if err != nil {
		t.Fatalf("storing %q: %v", path, err)
	}
	if err := meta.Commit(t.Context(), path, metastore.Object{
		Key: key, Size: int64(len(content)), Digest: digest, ModTime: time.Now(),
	}); err != nil {
		t.Fatalf("committing %q: %v", path, err)
	}
}

func TestSweepPreservesObjectAndMetastoreFailures(t *testing.T) {
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	deleteFailure := errors.New("the second object could not be deleted")
	forgetFailure := errors.New("the first deletion could not be recorded")
	objects := &secondDeleteFails{Objects: memory.New(), failure: deleteFailure}
	namespace := objectstore.New(objects, &forgetFails{Store: meta, failure: forgetFailure})
	t.Cleanup(func() { _ = namespace.Close() })
	ctx := t.Context()

	for _, name := range []string{"first", "second"} {
		putDirect(t, meta, objects, name, []byte(name))
	}
	for _, name := range []string{"first", "second"} {
		if err := meta.Remove(ctx, name); err != nil {
			t.Fatalf("removing %q directly from the tree: %v", name, err)
		}
	}

	removed, err := namespace.Sweep(ctx, 10)
	if removed != 1 || !errors.Is(err, deleteFailure) || !errors.Is(err, forgetFailure) {
		t.Fatalf("sweep removed %d objects with %v, want one removal and both failures", removed, err)
	}
	status := namespace.MaintenanceStatus()
	if status.LastSweepRemoved != 1 || !errors.Is(status.LastSweepError, deleteFailure) ||
		!errors.Is(status.LastSweepError, forgetFailure) {
		t.Fatalf("maintenance status is %+v, want one removal and both failures", status)
	}
}

func TestBackgroundMaintenanceOwnsItsLifetime(t *testing.T) {
	objects := &maintenanceObjects{Objects: memory.New()}
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	namespace, err := objectstore.NewWithOptions(objects, meta, objectstore.Options{
		SweepInterval: 25 * time.Millisecond,
		SweepBatch:    4,
	})
	if err != nil {
		t.Fatalf("creating background maintenance: %v", err)
	}
	t.Cleanup(func() {
		if err := namespace.Close(); err != nil {
			t.Errorf("closing the namespace: %v", err)
		}
	})

	ctx := t.Context()
	if err := namespace.Write(ctx, "f", []byte("garbage after removal")); err != nil {
		t.Fatalf("writing the file: %v", err)
	}
	node, err := meta.Stat(ctx, "f")
	if err != nil {
		t.Fatalf("reading the object key: %v", err)
	}
	if err := meta.Remove(ctx, "f"); err != nil {
		t.Fatalf("removing the file directly from the tree: %v", err)
	}

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		status := namespace.MaintenanceStatus()
		_, getErr := objects.Get(ctx, string(node.Content))
		if errors.Is(getErr, syscall.ENOENT) && !status.LastSweepTime.IsZero() && status.LastSweepError == nil {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("background maintenance did not remove the object; last status is %+v", status)
		case <-time.After(time.Millisecond):
		}
	}
}

type gatedGarbageStore struct {
	metastore.Store
	entered chan struct{}
	release chan struct{}
	enabled atomic.Bool
}

func (s *gatedGarbageStore) Garbage(ctx context.Context, _ int) ([]metastore.Key, error) {
	if !s.enabled.Load() {
		return s.Store.Garbage(ctx, 1)
	}
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestExplicitSweepsAreSerialized(t *testing.T) {
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	gated := &gatedGarbageStore{
		Store:   meta,
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	namespace := objectstore.New(memory.New(), gated)
	t.Cleanup(func() { _ = namespace.Close() })
	await(t, "the initial maintenance probe", func() bool {
		return !namespace.MaintenanceStatus().LastSweepTime.IsZero()
	})
	gated.enabled.Store(true)

	results := make(chan error, 2)
	go func() {
		_, err := namespace.Sweep(t.Context(), 1)
		results <- err
	}()
	<-gated.entered
	go func() {
		_, err := namespace.Sweep(t.Context(), 1)
		results <- err
	}()

	select {
	case <-gated.entered:
		t.Fatal("a second sweep entered the metastore while the first was still in progress")
	case <-time.After(25 * time.Millisecond):
	}
	close(gated.release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("serialized sweep failed: %v", err)
		}
	}
	if entered := len(gated.entered); entered != 1 {
		t.Fatalf("the second sweep entered %d times after the first left, want one", entered)
	}
}

type firstDeleteBlocks struct {
	objectstore.Objects
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *firstDeleteBlocks) Delete(ctx context.Context, key string) error {
	blocked := false
	o.once.Do(func() {
		blocked = true
		close(o.started)
	})
	if blocked {
		select {
		case <-o.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return o.Objects.Delete(ctx, key)
}

func TestConcurrentMutationsDoNotLoseAnEventDrivenSweep(t *testing.T) {
	objects := &firstDeleteBlocks{
		Objects: memory.New(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	namespace, meta := composedStore(t, limited.MinLimit, objects)
	ctx := t.Context()
	if err := namespace.Write(ctx, "f", []byte("initial")); err != nil {
		t.Fatalf("writing initial contents: %v", err)
	}

	first := make(chan error, 1)
	go func() { first <- namespace.Write(ctx, "f", []byte("first replacement")) }()
	<-objects.started
	second := make(chan error, 1)
	go func() { second <- namespace.Write(ctx, "f", []byte("second replacement")) }()
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("second replacement: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the second mutation waited for unrelated object deletion")
	}
	close(objects.release)
	if err := <-first; err != nil {
		t.Fatalf("first replacement: %v", err)
	}
	await(t, "both coalesced maintenance batches", func() bool {
		status, err := meta.ObjectStatus(ctx)
		return err == nil && status.GarbageCount == 0
	})
}

func TestEventDrivenMaintenanceDrainsMoreThanOneBatch(t *testing.T) {
	objects := memory.New()
	namespace, meta := composedStore(t, 4*limited.MinLimit, objects)
	await(t, "the initial maintenance probe", func() bool {
		return !namespace.MaintenanceStatus().LastSweepTime.IsZero()
	})

	const files = 20
	for i := range files {
		name := fmt.Sprintf("garbage-%d", i)
		putDirect(t, meta, objects, name, []byte(name))
		if err := meta.Remove(t.Context(), name); err != nil {
			t.Fatalf("removing %q directly from the tree: %v", name, err)
		}
	}
	if err := namespace.Write(t.Context(), "trigger", nil); err != nil {
		t.Fatalf("signaling event-driven maintenance: %v", err)
	}
	await(t, "every full maintenance batch to requeue its remainder", func() bool {
		status, err := meta.ObjectStatus(t.Context())
		return err == nil && status.GarbageCount == 0
	})
}

type cancellationGarbageStore struct {
	metastore.Store
	started chan struct{}
	once    sync.Once
	failure error
}

type closeObservedGarbageStore struct {
	metastore.Store
	started chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func (s *closeObservedGarbageStore) Garbage(ctx context.Context, _ int) ([]metastore.Key, error) {
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	close(s.stopped)
	return nil, ctx.Err()
}

func (s *cancellationGarbageStore) Garbage(ctx context.Context, _ int) ([]metastore.Key, error) {
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	if s.failure != nil {
		return nil, s.failure
	}
	return nil, ctx.Err()
}

func TestCloseCancelsAndWaitsForBackgroundMaintenance(t *testing.T) {
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	blocking := &cancellationGarbageStore{
		Store: meta, started: make(chan struct{}), failure: errors.Join(syscall.EIO, context.Canceled),
	}
	namespace, err := objectstore.NewWithOptions(memory.New(), blocking, objectstore.Options{
		SweepInterval: time.Millisecond,
		SweepBatch:    1,
	})
	if err != nil {
		t.Fatalf("creating background maintenance: %v", err)
	}
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background maintenance never started")
	}

	closed := make(chan error, 1)
	go func() { closed <- namespace.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not cancel the background sweep")
	}
	if _, err := namespace.Sweep(t.Context(), 1); !errors.Is(err, syscall.EIO) {
		t.Fatalf("sweep after close returned %v, want EIO", err)
	}
	if status := namespace.MaintenanceStatus(); status.LastSweepTime.IsZero() || !errors.Is(status.LastSweepError, syscall.EIO) {
		t.Fatalf("shutdown cancellation was not retained honestly: %+v", status)
	}
}

func TestCloseCancelsAndWaitsForEventDrivenMaintenance(t *testing.T) {
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	blocking := &cancellationGarbageStore{
		Store: meta, started: make(chan struct{}), failure: errors.Join(syscall.EIO, context.Canceled),
	}
	namespace := objectstore.New(memory.New(), blocking)
	if err := namespace.Write(t.Context(), "f", []byte("content")); err != nil {
		t.Fatalf("writing the event-driven trigger: %v", err)
	}
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("event-driven maintenance never started")
	}

	closed := make(chan error, 1)
	go func() { closed <- namespace.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not cancel the event-driven sweep")
	}
	if status := namespace.MaintenanceStatus(); status.LastSweepTime.IsZero() || !errors.Is(status.LastSweepError, syscall.EIO) {
		t.Fatalf("event-worker shutdown cancellation was not retained honestly: %+v", status)
	}
}

func TestCloseRetainsAnIndependentBackgroundFailure(t *testing.T) {
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	failure := errors.New("the maintenance store failed while shutdown began")
	blocking := &cancellationGarbageStore{Store: meta, started: make(chan struct{}), failure: failure}
	namespace, err := objectstore.NewWithOptions(memory.New(), blocking, objectstore.Options{
		SweepInterval: time.Millisecond,
		SweepBatch:    1,
	})
	if err != nil {
		t.Fatalf("creating background maintenance: %v", err)
	}
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background maintenance never started")
	}
	if err := namespace.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	status := namespace.MaintenanceStatus()
	if status.LastSweepTime.IsZero() || !errors.Is(status.LastSweepError, failure) {
		t.Fatalf("shutdown lost the independent maintenance failure: %+v", status)
	}
}

func TestCloseRetainsFailuresBeyondSQLiteShapedCancellation(t *testing.T) {
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	independent := errors.New("the object service also failed")
	failure := errors.Join(context.Canceled, syscall.EIO, independent)
	blocking := &cancellationGarbageStore{Store: meta, started: make(chan struct{}), failure: failure}
	namespace, err := objectstore.NewWithOptions(memory.New(), blocking, objectstore.Options{
		SweepInterval: time.Millisecond,
		SweepBatch:    1,
	})
	if err != nil {
		t.Fatalf("creating background maintenance: %v", err)
	}
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background maintenance never started")
	}
	if err := namespace.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	status := namespace.MaintenanceStatus()
	if status.LastSweepTime.IsZero() || !errors.Is(status.LastSweepError, independent) {
		t.Fatalf("shutdown hid the failure beyond SQLite-shaped cancellation: %+v", status)
	}
}

func TestClosePreservesAQueuedMutationForStartupMaintenance(t *testing.T) {
	database := filepath.Join(t.TempDir(), "meta.db")
	meta, err := sqlite.Open(t.Context(), database, "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	objects := memory.New()
	putDirect(t, meta, objects, "f", []byte("initial"))
	node, err := meta.Stat(t.Context(), "f")
	if err != nil {
		t.Fatalf("reading the initial object key: %v", err)
	}

	blocked := &blockedPutObjects{
		Objects: objects,
		started: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	observed := &closeObservedGarbageStore{
		Store: meta, started: make(chan struct{}), stopped: make(chan struct{}),
	}
	namespace := objectstore.New(blocked, observed)
	<-observed.started

	written := make(chan error, 1)
	go func() { written <- namespace.Write(t.Context(), "f", []byte("replacement")) }()
	<-blocked.started
	closed := make(chan error, 1)
	go func() { closed <- namespace.Close() }()
	select {
	case <-observed.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not stop maintenance before draining the admitted mutation")
	}
	close(blocked.release)
	if err := <-written; err != nil {
		t.Fatalf("admitted replacement: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("closing the first namespace: %v", err)
	}

	reopenedMeta, err := sqlite.Open(t.Context(), database, "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("reopening the metastore: %v", err)
	}
	reopened := objectstore.New(objects, reopenedMeta)
	t.Cleanup(func() { _ = reopened.Close() })
	await(t, "startup maintenance to collect the queued replacement garbage", func() bool {
		status, err := reopenedMeta.ObjectStatus(t.Context())
		if err != nil || status.GarbageCount != 0 {
			return false
		}
		_, err = objects.Get(t.Context(), string(node.Content))
		return errors.Is(err, syscall.ENOENT)
	})
	content, err := reopened.Read(t.Context(), "f")
	if err != nil || string(content) != "replacement" {
		t.Fatalf("reopened file reads as %q (%v), want the admitted replacement", content, err)
	}
}

func TestNewWithOptionsRejectsUnownedMaintenanceConfiguration(t *testing.T) {
	for name, options := range map[string]objectstore.Options{
		"interval":        {SweepInterval: 0, SweepBatch: 1},
		"batch":           {SweepInterval: time.Second, SweepBatch: 0},
		"unbounded batch": {SweepInterval: time.Second, SweepBatch: objectstore.MaxSweepBatch + 1},
	} {
		t.Run(name, func(t *testing.T) {
			meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
			if err != nil {
				t.Fatalf("opening the metastore: %v", err)
			}
			var metaCloses, objectCloses atomic.Int64
			store := &wrappedStore{Store: meta, closed: func() { metaCloses.Add(1) }}
			objects := &measuredObjects{Objects: memory.New(), closed: func() { objectCloses.Add(1) }}

			if _, err := objectstore.NewWithOptions(objects, store, options); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("NewWithOptions(%+v) returned %v, want EINVAL", options, err)
			}
			if metaCloses.Load() != 0 || objectCloses.Load() != 0 {
				t.Fatalf("failed construction closed caller-owned inputs: meta=%d objects=%d",
					metaCloses.Load(), objectCloses.Load())
			}
			if _, err := meta.Space(t.Context()); err != nil {
				t.Fatalf("caller-owned metastore was unusable after failed construction: %v", err)
			}
			if _, err := objects.Put(t.Context(), "still-owned", []byte("content")); err != nil {
				t.Fatalf("caller-owned object store was unusable after failed construction: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("closing the caller-owned metastore: %v", err)
			}
			if err := objects.Close(); err != nil {
				t.Fatalf("closing the caller-owned object store: %v", err)
			}
		})
	}
}

func TestDefaultOptionsBoundBackgroundMaintenance(t *testing.T) {
	if options := objectstore.DefaultOptions(); options.SweepInterval != time.Minute || options.SweepBatch != 64 {
		t.Fatalf("DefaultOptions returned %+v, want a one-minute interval and 64-object batch", options)
	}
}
