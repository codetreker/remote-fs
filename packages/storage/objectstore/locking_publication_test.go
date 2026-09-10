package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract"
	"github.com/codetreker/remote-fs/packages/storage/locked"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/azblob"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type publicationClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *publicationClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *publicationClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (c *publicationClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type heldLockObjects struct {
	*memory.Objects
	putPayload []byte
	getPayload []byte
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
}

func (o *heldLockObjects) hold(ctx context.Context) error {
	o.once.Do(func() { close(o.entered) })
	select {
	case <-o.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *heldLockObjects) Put(ctx context.Context, key string, content []byte) ([]byte, error) {
	if bytes.Equal(content, o.putPayload) {
		if err := o.hold(ctx); err != nil {
			return nil, err
		}
	}
	return o.Objects.Put(ctx, key, content)
}

func (o *heldLockObjects) Get(ctx context.Context, key string) ([]byte, error) {
	content, err := o.Objects.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(content, o.getPayload) {
		if err := o.hold(ctx); err != nil {
			return nil, err
		}
	}
	return content, nil
}

func lockingObjectVolume(t *testing.T, options locking.Options, objects objectstore.Objects) (*objectstore.Storage, *sqlite.LockingStore, *locked.Storage) {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "meta.db"), Volume: "workspace",
		SQLite: sqlite.DefaultOptions(), Locks: options, Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(objects, meta)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("closing the locked volume: %v", err)
		}
	})
	if volume.LockService() == nil || volume.LockService() != meta.LockService() {
		t.Fatal("object storage did not preserve its native metastore authority")
	}
	if err := volume.CheckPublicationAccounting(); err != nil {
		t.Fatalf("native publication accounting was not forwarded: %v", err)
	}
	paired, err := locked.New(volume)
	if err != nil {
		t.Fatal(err)
	}
	return volume, meta, paired
}

func publicationOwner(t *testing.T, service locking.Service) locking.OwnerRef {
	t.Helper()
	ticket, err := service.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := service.CreateOwner(t.Context(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.CloseSession(context.Background(), session.ID); err != nil {
			t.Errorf("retiring the lock session: %v", err)
		}
	})
	return owner.Ref
}

func publicationGrant(t *testing.T, service locking.Service, owner locking.OwnerRef, path string, mode locking.Mode) locking.GrantRef {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	resource, err := service.Resolve(ctx, owner, path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Acquire(ctx, locking.AcquireRequest{
		Owner: owner, Request: locking.RequestID(path), Resource: resource, Mode: mode, TTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Grant == nil || result.Receipt.Outcome != locking.Granted {
		t.Fatalf("acquire returned outcome %s", result.Receipt.Outcome)
	}
	return result.Grant.Ref
}

func awaitPublication[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("the volume operation did not finish")
		var zero T
		return zero
	}
}

func TestObjectUploadDoesNotReservePublicationOrExtendAnExpiredGrant(t *testing.T) {
	clock := &publicationClock{now: time.Now().Add(time.Minute)}
	options := locking.DefaultOptions()
	options.Clock = clock
	objects := &heldLockObjects{
		Objects: memory.New(), putPayload: []byte("staged replacement"),
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	volume, meta, paired := lockingObjectVolume(t, options, objects)
	release := sync.OnceFunc(func() { close(objects.release) })
	defer release()
	if err := volume.Write(t.Context(), "f", []byte("original")); err != nil {
		t.Fatal(err)
	}
	before, err := meta.Stat(t.Context(), "f")
	if err != nil {
		t.Fatal(err)
	}
	owner := publicationOwner(t, volume.LockService())
	grant := publicationGrant(t, volume.LockService(), owner, "f", locking.Exclusive)
	scoped, err := paired.Scope(locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}})
	if err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() { written <- scoped.Write(t.Context(), "f", objects.putPayload) }()
	awaitPublication(t, objects.entered)
	other := make(chan error, 1)
	go func() { other <- volume.Write(t.Context(), "unrelated", []byte("other file")) }()
	if err := awaitPublication(t, other); err != nil {
		t.Fatalf("uploading one file blocked another publication: %v", err)
	}
	if body, err := volume.Read(t.Context(), "f"); err != nil || string(body) != "original" {
		t.Fatalf("staging became visible before publication: body=%q err=%v", body, err)
	}
	clock.advance(11 * time.Second)
	release()
	if err := awaitPublication(t, written); !errors.Is(err, &locking.Error{Code: locking.StaleGrant}) {
		t.Fatalf("publication after grant expiry returned %v, want stale grant without a successor", err)
	}
	after, err := meta.Stat(t.Context(), "f")
	if err != nil {
		t.Fatal(err)
	}
	if before.ID != after.ID || before.Content != after.Content || before.Size != after.Size {
		t.Fatal("rejected publication changed the file identity or content reference")
	}
	if body, err := volume.Read(t.Context(), "f"); err != nil || string(body) != "original" {
		t.Fatalf("rejected staged bytes replaced the file: body=%q err=%v", body, err)
	}
}

func TestAnAnonymousUploadIsCheckedAgainstGrantsAcquiredDuringStaging(t *testing.T) {
	objects := &heldLockObjects{
		Objects: memory.New(), putPayload: []byte("anonymous replacement"),
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	volume, _, _ := lockingObjectVolume(t, locking.DefaultOptions(), objects)
	release := sync.OnceFunc(func() { close(objects.release) })
	defer release()
	if err := volume.Write(t.Context(), "f", []byte("original")); err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() { written <- volume.Write(t.Context(), "f", objects.putPayload) }()
	awaitPublication(t, objects.entered)
	owner := publicationOwner(t, volume.LockService())
	publicationGrant(t, volume.LockService(), owner, "f", locking.Shared)
	release()
	if err := awaitPublication(t, written); !errors.Is(err, &locking.Error{Code: locking.Conflict}) {
		t.Fatalf("anonymous upload ignored the newly acquired shared grant: %v", err)
	}
	if body, err := volume.Read(t.Context(), "f"); err != nil || string(body) != "original" {
		t.Fatalf("shared-protected file changed: body=%q err=%v", body, err)
	}
}

func TestCapturedImmutableReadDoesNotHoldGrantOrPublicationAdmission(t *testing.T) {
	objects := &heldLockObjects{
		Objects: memory.New(), getPayload: []byte("captured version"),
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	volume, _, paired := lockingObjectVolume(t, locking.DefaultOptions(), objects)
	release := sync.OnceFunc(func() { close(objects.release) })
	defer release()
	if err := volume.Write(t.Context(), "f", objects.getPayload); err != nil {
		t.Fatal(err)
	}
	type readResult struct {
		body []byte
		err  error
	}
	read := make(chan readResult, 1)
	go func() {
		body, err := volume.Read(t.Context(), "f")
		read <- readResult{body, err}
	}()
	awaitPublication(t, objects.entered)
	owner := publicationOwner(t, volume.LockService())
	grant := publicationGrant(t, volume.LockService(), owner, "f", locking.Exclusive)
	scoped, err := paired.Scope(locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}})
	if err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() { written <- scoped.Write(t.Context(), "f", []byte("new version")) }()
	if err := awaitPublication(t, written); err != nil {
		t.Fatalf("a captured immutable read held publication admission: %v", err)
	}
	release()
	result := awaitPublication(t, read)
	if result.err != nil || string(result.body) != "captured version" {
		t.Fatalf("captured read returned %q, %v", result.body, result.err)
	}
	if body, err := volume.Read(t.Context(), "f"); err != nil || string(body) != "new version" {
		t.Fatalf("fresh read did not capture the published version: %q, %v", body, err)
	}
}

func TestAzureObjectStoreLockContract(t *testing.T) {
	lockcontract.Run(t, func(t *testing.T, options locking.Options) lockcontract.Fixture {
		objects, err := azblob.NewWithSharedKey(
			serviceURL()+"/"+containerFor(t), accountName, accountKey,
			fmt.Sprintf("locks/%d/", volumes.Add(1)),
		)
		if err != nil {
			t.Fatal(err)
		}
		volume, _, paired := lockingObjectVolume(t, options, objects)
		return lockcontract.Fixture{Storage: volume, Locks: volume.LockService(), Scope: paired.Scope}
	})
}
