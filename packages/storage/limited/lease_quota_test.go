package limited_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type leaseQuotaClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *leaseQuotaClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *leaseQuotaClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (c *leaseQuotaClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type heldShrinkObjects struct {
	*memory.Objects
	payload []byte
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (o *heldShrinkObjects) Put(ctx context.Context, key string, body []byte) ([]byte, error) {
	if bytes.Equal(body, o.payload) {
		o.once.Do(func() { close(o.entered) })
		select {
		case <-o.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return o.Objects.Put(ctx, key, body)
}

func TestExpiredStagedShrinkRetainsBytesAndQuota(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	clock := &leaseQuotaClock{now: time.Now().Add(time.Minute)}
	options := locking.DefaultOptions()
	options.Clock = clock
	meta, err := sqlite.OpenLocking(ctx, sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "namespace.db"), Namespace: "limited",
		Allowance: 0, SQLite: sqlite.DefaultOptions(), Locks: options, Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	objects := &heldShrinkObjects{
		Objects: memory.New(), payload: []byte("shrunk"),
		entered: make(chan struct{}), resume: make(chan struct{}),
	}
	backing := objectstore.New(objects, meta)
	t.Cleanup(func() {
		if err := backing.Close(); err != nil {
			t.Errorf("closing namespace: %v", err)
		}
	})
	// A zero metadata allowance leaves the outer decorator responsible for every charge.
	quota := newStorageOver(t, backing, limited.MinLimit)
	original := bytes.Repeat([]byte{'o'}, limited.MinLimit)
	if err := quota.Write(ctx, "one", original); err != nil {
		t.Fatal(err)
	}
	service := backing.LockService()
	if service == nil {
		t.Fatal("the namespace has no native lock service")
	}
	ticket, err := service.BeginEnrollment(ctx)
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.OpenSession(ctx, ticket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.CloseSession(cleanup, session.ID); err != nil {
			t.Errorf("closing lock session: %v", err)
		}
	})
	owner, err := service.CreateOwner(ctx, session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	resource, err := service.Resolve(ctx, owner.Ref, "one")
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := service.Acquire(ctx, locking.AcquireRequest{
		Owner: owner.Ref, Request: "shrink", Resource: resource,
		Mode: locking.Exclusive, TTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if acquired.Grant == nil || acquired.Receipt.Outcome != locking.Granted {
		t.Fatalf("acquire returned outcome %s, want a grant", acquired.Receipt.Outcome)
	}
	scope := locking.MutationScope{Owner: owner.Ref, Grants: []locking.GrantRef{acquired.Grant.Ref}}
	writeContext, cancelWrite := context.WithCancel(ctx)
	release := sync.OnceFunc(func() { close(objects.resume) })
	written := make(chan error, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancelWrite()
		release()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("shrinking write did not finish during cleanup")
		}
	})
	go func() {
		defer close(finished)
		written <- quota.Write(locking.WithScope(writeContext, scope), "one", objects.payload)
	}()
	select {
	case <-objects.entered:
	case <-ctx.Done():
		t.Fatalf("shrinking write did not reach object upload: %v", ctx.Err())
	}
	assertCharge := func(want int64) {
		t.Helper()
		space, err := quota.Space(ctx)
		if err != nil || space.Total != limited.MinLimit || space.Used != want || space.Avail != limited.MinLimit-want {
			t.Fatalf("quota = %+v, error %v; want total %d, used %d, available %d",
				space, err, limited.MinLimit, want, limited.MinLimit-want)
		}
	}
	assertFull := func() {
		t.Helper()
		assertCharge(limited.MinLimit)
		if err := quota.Write(ctx, "growth", []byte{'x'}); !errors.Is(err, syscall.EDQUOT) {
			t.Fatalf("growth against retained charge returned %v, want EDQUOT", err)
		}
	}
	assertFull()
	clock.advance(2 * time.Second)
	release()
	select {
	case err := <-written:
		if locking.CodeOf(err) != locking.StaleGrant {
			t.Fatalf("expired staged shrink returned %v, want stale grant", err)
		}
	case <-ctx.Done():
		t.Fatalf("expired staged shrink did not finish: %v", ctx.Err())
	}
	got, err := quota.Read(ctx, "one")
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("expired shrink changed committed bytes: length %d, error %v", len(got), err)
	}
	assertFull()
	if err := quota.Recount(ctx); err != nil {
		t.Fatal(err)
	}
	assertFull()
	if err := quota.Write(ctx, "one", objects.payload); err != nil {
		t.Fatalf("valid shrink after expiry: %v", err)
	}
	assertCharge(int64(len(objects.payload)))
	if err := quota.Write(ctx, "two", original[len(objects.payload):]); err != nil {
		t.Fatalf("using the released bytes: %v", err)
	}
	assertFull()
	if err := quota.Recount(ctx); err != nil {
		t.Fatal(err)
	}
	assertFull()
}
