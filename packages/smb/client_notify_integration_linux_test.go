package smb

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

type nativeNotificationStream struct {
	*notifyTestStream
	incarnation metastore.Incarnation
}

func (s *nativeNotificationStream) Incarnation() metastore.Incarnation { return s.incarnation }

type nativeNotifyFixture struct {
	store   *localstore.Store
	file    *clientFile
	ctx     context.Context
	manager *notificationManager
	stream  *nativeNotificationStream
	start   metastore.Position
	result  <-chan nativeNotifyResult
	release func()
}

type nativeNotifyResult struct {
	body   []byte
	status uint32
}

func newNativeNotifyFixture(t *testing.T, initialNames ...string) *nativeNotifyFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	locks := locking.DefaultOptions()
	store, err := localstore.Open(ctx, localstore.Config{Root: root, Volume: "notify", Quota: 1 << 20, Locks: &locks, InitializeLocks: true, Window: sqlite.DefaultWindow(), Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, name := range initialNames {
		if err := store.Create(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	limits := DefaultLimits()
	allow := authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
	backend := &clientBackend{source: store, limits: limits, authorize: allow, volume: "notify"}
	opened, err := backend.NewSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*clientSession)
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		if err := session.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	action, err := session.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := session.raw.Retain(ctx, storage.RetainRequest{NodeID: session.state.RootID}, action)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := session.raw.Reference(ctx, receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	file := &clientFile{session: session, raw: raw, access: windowsAllAccess}
	barrier, err := store.Log().Barrier(ctx, 1024)
	if err != nil {
		t.Fatal(err)
	}
	stream := &nativeNotificationStream{notifyTestStream: testNotifyStream(), incarnation: barrier.Incarnation}
	stream.at = barrier.Position
	source := ChangeSource{
		Subscribe: func(context.Context) (ChangeStream, error) { return stream, nil },
		Resume: func(context.Context, metastore.Incarnation, metastore.Position) (ChangeStream, error) {
			return nil, syscall.ESTALE
		},
		Checkpoint: func(context.Context) (metastore.LogBarrier, error) { return barrier, nil },
	}
	manager, err := newNotificationManager(ctx, source, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	id := wire.FileID{1}
	connection := &connection{server: &Server{config: Config{Authorize: allow, Limits: limits}}}
	dispatcher := newFileDispatcher(backend, session, 1, limits)
	dispatcher.handles[id] = &fileHandle{file: file, access: 1, identity: notificationIdentity{ID: session.state.RootID, Directory: true}}
	tree := &tree{export: &Export{share: Share{Volume: "notify"}, changes: manager}, files: dispatcher}
	body := make([]byte, 32)
	smbLE.PutUint16(body, 32)
	smbLE.PutUint32(body[4:], 4096)
	copy(body[8:24], id[:])
	smbLE.PutUint32(body[24:], 1)
	registered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	pending := context.WithValue(ctx, pendingKey{}, func(*signing.Session) error {
		close(registered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	result := make(chan nativeNotifyResult, 1)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		body, status := connection.notify(pending, tree, fileRequest(wire.ChangeNotify, body), nil)
		result <- nativeNotifyResult{body: body, status: status}
	}()
	t.Cleanup(func() {
		unblock()
		cancel()
		select {
		case <-completed:
		case <-time.After(time.Second):
			t.Error("notification callback did not retire after cancellation")
		}
	})
	select {
	case <-registered:
	case answer := <-result:
		t.Fatalf("notification failed before registration: %#x", answer.status)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return &nativeNotifyFixture{store: store, file: file, ctx: ctx, manager: manager, stream: stream, start: barrier.Position, result: result, release: unblock}
}

func (f *nativeNotifyFixture) changes(t *testing.T) []metastore.Change {
	t.Helper()
	page, err := metastore.NewChangeResult(1<<20, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
		return 512 + lengths.Name + lengths.FromName + lengths.Content + lengths.Metadata + lengths.Target + lengths.Notification, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	retention, err := f.store.Log().Since(f.ctx, f.start, 64, page)
	if err != nil {
		t.Fatal(err)
	}
	changes, err := page.Changes()
	if err != nil || len(changes) == 0 || changes[len(changes)-1].Position != retention.Tail {
		t.Fatalf("incomplete authoritative notification page: %v, %v, tail=%d", changes, err, retention.Tail)
	}
	return changes
}

func (f *nativeNotifyFixture) queue(t *testing.T, changes []metastore.Change) {
	t.Helper()
	for _, change := range changes {
		select {
		case f.stream.changes <- change:
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
	}
	position := changes[len(changes)-1].Position
	for {
		f.manager.mu.Lock()
		at, failure, wake := f.manager.position, f.manager.failure, f.manager.wake
		f.manager.mu.Unlock()
		if failure != nil {
			t.Fatal(failure)
		}
		if at == position {
			return
		}
		select {
		case <-wake:
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
	}
}

func (f *nativeNotifyFixture) finish(t *testing.T) nativeNotifyResult {
	t.Helper()
	f.release()
	select {
	case result := <-f.result:
		return result
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
		return nativeNotifyResult{}
	}
}

func TestNativeNotificationRejectsGenericCaseCollision(t *testing.T) {
	f := newNativeNotifyFixture(t, "Foo")
	if err := f.store.Create(f.ctx, "foo"); err != nil {
		t.Fatalf("generic authority rejected distinct legal names: %v", err)
	}
	upper, err := f.store.Stat(f.ctx, "Foo")
	if err != nil {
		t.Fatal(err)
	}
	lower, err := f.store.Stat(f.ctx, "foo")
	if err != nil || lower.ID == upper.ID {
		t.Fatalf("generic names did not retain distinct identities: %+v / %+v, %v", upper, lower, err)
	}
	changes := f.changes(t)
	if len(changes) != 2 || changes[0].Kind != metastore.Created || string(changes[0].Name) != "foo" || changes[1].Kind != metastore.Modified || changes[1].Notification.SubjectID != int64(f.file.raw.NodeID()) {
		t.Fatalf("unexpected authoritative creation: %+v", changes)
	}
	mapped, err := mapNotifications(changes[0], int64(f.file.raw.NodeID()), 1, false)
	if err != nil || len(mapped) != 1 || mapped[0].Name != "foo" {
		t.Fatalf("individual name was not representable: %v, %v", mapped, err)
	}
	if err := f.file.ValidateNotificationLocation(f.ctx, changes[0].Notification.After.Location); !errors.Is(err, ErrNotifyRescan) {
		t.Fatalf("current-revision sibling collision = %v; want rescan", err)
	}
	f.queue(t, changes)
	answer := f.finish(t)
	if answer.status != 0x10c || len(answer.body) != 0 {
		t.Fatalf("case collision returned partial success: status=%#x body=%x", answer.status, answer.body)
	}
}

func TestNativeNotificationRejectsWholeQueuedBatch(t *testing.T) {
	f := newNativeNotifyFixture(t, "Foo")
	for _, name := range []string{"ordinary", "foo"} {
		if err := f.store.Create(f.ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	changes := f.changes(t)
	if len(changes) != 4 || changes[0].Kind != metastore.Created || changes[2].Kind != metastore.Created || changes[1].Notification.SubjectID != int64(f.file.raw.NodeID()) || changes[3].Notification.SubjectID != int64(f.file.raw.NodeID()) {
		t.Fatalf("queued child and parent changes=%+v; want two creation transactions", changes)
	}
	f.queue(t, changes)
	f.manager.mu.Lock()
	groups, events := 0, 0
	for _, watcher := range f.manager.watchers {
		groups += len(watcher.groups)
		events += watcher.events
	}
	f.manager.mu.Unlock()
	if groups != 2 || events != 2 {
		t.Fatalf("delivery gate did not retain the whole batch: groups=%d events=%d", groups, events)
	}
	answer := f.finish(t)
	if answer.status != 0x10c || len(answer.body) != 0 {
		t.Fatalf("invalid batch returned a plausible prefix: status=%#x body=%x", answer.status, answer.body)
	}
	f.manager.mu.Lock()
	defer f.manager.mu.Unlock()
	for _, watcher := range f.manager.watchers {
		if len(watcher.groups) != 0 || watcher.bytes != 0 || watcher.events != 0 {
			t.Fatalf("failed group retained undelivered event storage: %+v", watcher)
		}
	}
}

func TestNativeNotificationUsesImmutableMetadataWithCurrentNameProof(t *testing.T) {
	f := newNativeNotifyFixture(t)
	if err := f.store.Create(f.ctx, "ordinary"); err != nil {
		t.Fatal(err)
	}
	created := f.changes(t)
	if len(created) != 2 || created[0].Kind != metastore.Created || created[1].Kind != metastore.Modified || created[1].Notification.SubjectID != int64(f.file.raw.NodeID()) {
		t.Fatalf("creation history=%+v", created)
	}
	canonical, err := metastore.EncodeNotification(created[0])
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.store.Stat(f.ctx, "ordinary")
	if err != nil {
		t.Fatal(err)
	}
	metadata := storage.Metadata{{Key: windowsMetadataKey, Version: windowsMetadataVersion + 1, Data: []byte{1}}}
	if err := f.store.SetAttr(f.ctx, "ordinary", storage.AttrChange{ExpectedRevision: current.MetadataRevision, Metadata: &metadata}); err != nil {
		t.Fatal(err)
	}
	current, err = f.store.Stat(f.ctx, "ordinary")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projectWindowsAttr(storage.FileObservation{Attr: current}, windowsMetadata{}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("future metadata did not require a different projection: %v", err)
	}
	history := f.changes(t)
	if len(history) != len(created)+1 || history[len(history)-1].Kind != metastore.Modified {
		t.Fatalf("metadata history=%+v", history)
	}
	again, err := metastore.EncodeNotification(history[0])
	if err != nil || !bytes.Equal(again, canonical) || len(history[0].Notification.After.Attr.Metadata) != 0 {
		t.Fatalf("stored event was reconstructed from current metadata: %v", err)
	}
	// Delivery is paused at the original creation while the authority already
	// contains the later metadata revision.
	f.queue(t, history[:len(created)])
	answer := f.finish(t)
	want := wire.BufferResponseBody(wire.NotifyInformation([]wire.Notification{{Action: 1, Name: "ordinary"}}))
	if answer.status != 0 || !bytes.Equal(answer.body, want) {
		t.Fatalf("valid captured event changed with later metadata: status=%#x body=%x; want %x", answer.status, answer.body, want)
	}
}
