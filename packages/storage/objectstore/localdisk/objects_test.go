package localdisk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

var shardReadDeleteOperations = []struct {
	name string
	run  func(context.Context, *Objects, string) error
}{
	{"Get", func(ctx context.Context, o *Objects, key string) error { _, err := o.Get(ctx, key); return err }},
	{"GetBounded", func(ctx context.Context, o *Objects, key string) error {
		_, err := o.GetBounded(ctx, key, 1024)
		return err
	}},
	{"Delete", func(ctx context.Context, o *Objects, key string) error { return o.Delete(ctx, key) }},
}

// After shard validation, Done is evaluated first by take and then by the
// saturated promotion select. This observes that branch without replacing the
// real context's cancellation or claiming the goroutine is already parked.
type shardPromotionContext struct {
	context.Context
	armed   atomic.Bool
	calls   atomic.Int32
	reached chan struct{}
}

func (ctx *shardPromotionContext) Done() <-chan struct{} {
	if ctx.armed.Load() && ctx.calls.Add(1) == 2 {
		close(ctx.reached)
	}
	return ctx.Context.Done()
}

func TestShardDescriptorsCloseWhenPromotionIsCanceled(t *testing.T) {
	for _, operation := range shardReadDeleteOperations {
		t.Run(operation.name, func(t *testing.T) {
			objects := openForTest(t, Options{MaxInFlightOperations: 1})
			const key = "cancel-after-opening-shard"
			if _, err := objects.Put(t.Context(), key, []byte("payload")); err != nil {
				t.Fatal(err)
			}
			identity := shardIdentityForTest(t, objects, key)
			baseline := shardDescriptorsForTest(t, identity)
			defer closeLeakedShardDescriptors(t, identity, baseline)
			held := holdShardAdmission(t, objects)
			defer held.release()
			var probe atomic.Pointer[shardPromotionContext]
			observeShardPromotion(objects, identity, &probe)
			for attempt := range 3 {
				ctx, cancel := context.WithCancel(t.Context())
				observed := &shardPromotionContext{Context: ctx, reached: make(chan struct{})}
				probe.Store(observed)
				finished := make(chan error, 1)
				go func() { finished <- operation.run(observed, objects, key) }()
				select {
				case <-observed.reached:
				case <-time.After(5 * time.Second):
					cancel()
					<-finished
					t.Fatal("operation did not reach saturated promotion after opening its shard")
				}
				assertShardAdmission(t, objects, 1, 1)
				cancel()
				if err := <-finished; !errors.Is(err, syscall.EINTR) {
					t.Errorf("attempt %d: cancellation returned %v, want EINTR", attempt+1, err)
				}
				probe.Store(nil)
				if descriptors := shardDescriptorsForTest(t, identity); len(descriptors) != len(baseline) {
					t.Errorf("attempt %d: shard descriptors = %d, want baseline %d", attempt+1, len(descriptors), len(baseline))
				}
				assertShardAdmission(t, objects, 1, 0)
				assertShardLocksReleased(t, objects, key)
			}
			held.release()
			if err := operation.run(t.Context(), objects, key); err != nil {
				t.Errorf("operation after admission released: %v", err)
			}
			assertShardAdmission(t, objects, 0, 0)
			assertShardLocksReleased(t, objects, key)
		})
	}
}

func TestShardDescriptorsCloseWhenHealthFailsAfterOpen(t *testing.T) {
	for _, operation := range shardReadDeleteOperations {
		t.Run(operation.name, func(t *testing.T) {
			objects := openForTest(t, Options{MaxInFlightOperations: 1})
			const key = "poison-after-opening-shard"
			if _, err := objects.Put(t.Context(), key, []byte("payload")); err != nil {
				t.Fatal(err)
			}
			identity := shardIdentityForTest(t, objects, key)
			baseline := shardDescriptorsForTest(t, identity)
			defer closeLeakedShardDescriptors(t, identity, baseline)
			held := holdShardAdmission(t, objects)
			defer held.release()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			observed := &shardPromotionContext{Context: ctx, reached: make(chan struct{})}
			var probe atomic.Pointer[shardPromotionContext]
			probe.Store(observed)
			observeShardPromotion(objects, identity, &probe)
			finished := make(chan error, 1)
			go func() { finished <- operation.run(observed, objects, key) }()
			select {
			case <-observed.reached:
			case <-time.After(5 * time.Second):
				cancel()
				<-finished
				t.Fatal("operation did not reach saturated promotion after opening its shard")
			}
			objects.health.poison(errors.New("health changed while admission was occupied"))
			held.release()
			if err := <-finished; !errors.Is(err, syscall.EIO) {
				t.Errorf("operation after poison = %v, want EIO", err)
			}
			if descriptors := shardDescriptorsForTest(t, identity); len(descriptors) != len(baseline) {
				t.Errorf("shard descriptors after poison = %d, want baseline %d", len(descriptors), len(baseline))
			}
			assertShardAdmission(t, objects, 0, 0)
			assertShardLocksReleased(t, objects, key)
		})
	}
}

func shardIdentityForTest(t *testing.T, objects *Objects, key string) unix.Stat_t {
	t.Helper()
	location, err := locate(key)
	if err != nil {
		t.Fatal(err)
	}
	var identity unix.Stat_t
	if err := unix.Stat(filepath.Join(objects.rootPath, objectsDirectory, location.first), &identity); err != nil {
		t.Fatal(err)
	}
	return identity
}

func observeShardPromotion(objects *Objects, identity unix.Stat_t, probe *atomic.Pointer[shardPromotionContext]) {
	original := objects.ops.fstatfs
	objects.ops.fstatfs = func(fd int, result *unix.Statfs_t) error {
		if err := original(fd, result); err != nil {
			return err
		}
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			return err
		}
		if st.Dev == identity.Dev && st.Ino == identity.Ino {
			if ctx := probe.Load(); ctx != nil {
				ctx.armed.Store(true)
			}
		}
		return nil
	}
}

func holdShardAdmission(t *testing.T, objects *Objects) *ticket {
	t.Helper()
	waiting, err := objects.gate.acquireWaiting(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := waiting.promote(t.Context(), 0)
	waiting.release()
	if err != nil {
		t.Fatal(err)
	}
	return ticket
}

func assertShardAdmission(t *testing.T, objects *Objects, wantActive, wantWaiting int) {
	t.Helper()
	active, waiting, bytes, closing := objects.gate.snapshot()
	if active != wantActive || waiting != wantWaiting || bytes != 0 || closing {
		t.Errorf("admission = active %d, waiting %d, bytes %d, closing %v; want %d/%d/0/false", active, waiting, bytes, closing, wantActive, wantWaiting)
	}
}

func assertShardLocksReleased(t *testing.T, objects *Objects, key string) {
	t.Helper()
	objects.keys.mu.Lock()
	keys := len(objects.keys.locks)
	objects.keys.mu.Unlock()
	location, err := locate(key)
	if err != nil {
		t.Fatal(err)
	}
	if keys != 0 || len(objects.shards.tokens[location.shard]) != 1 {
		t.Errorf("locks remain: %d key locks, shard token count %d", keys, len(objects.shards.tokens[location.shard]))
	}
}

func shardDescriptorsForTest(t *testing.T, identity unix.Stat_t) map[int]struct{} {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	descriptors := make(map[int]struct{})
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			// ReadDir's descriptor has already closed after its entries were captured.
			if errors.Is(err, syscall.EBADF) {
				continue
			}
			t.Fatal(err)
		}
		if st.Dev == identity.Dev && st.Ino == identity.Ino {
			descriptors[fd] = struct{}{}
		}
	}
	return descriptors
}

// Reclaim only extra descriptors still identifying this test's private shard,
// after the baseline assertions, so a regression run does not leak into later tests.
func closeLeakedShardDescriptors(t *testing.T, identity unix.Stat_t, baseline map[int]struct{}) {
	t.Helper()
	for fd := range shardDescriptorsForTest(t, identity) {
		if _, existed := baseline[fd]; existed {
			continue
		}
		if err := unix.Close(fd); err != nil {
			t.Errorf("close leaked shard descriptor %d: %v", fd, err)
		}
	}
}
