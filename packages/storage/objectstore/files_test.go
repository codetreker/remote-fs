package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func fileVolume(t *testing.T, objects objectstore.Objects, allowance int64, configure func(*sqlite.Options)) (*objectstore.Storage, *sqlite.LockingStore) {
	t.Helper()
	options := sqlite.DefaultOptions()
	if configure != nil {
		configure(&options)
	}
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "files.db"), Volume: "files", Allowance: allowance,
		SQLite: options, Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(objects, meta)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("close volume: %v", err)
		}
	})
	return volume, meta
}

func fileSessionFor(t *testing.T, volume storage.FileStorage, options storage.FileSessionOptions) storage.FileSession {
	t.Helper()
	session, err := volume.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close session: %v", err)
		}
	})
	return session
}

func openFileFor(t *testing.T, session storage.FileSession, name string, options storage.FileOpenOptions) storage.File {
	t.Helper()
	f, err := session.OpenFile(t.Context(), name, options)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func retainedLockRequest(t *testing.T, session storage.FileSession) storage.LockRequestID {
	t.Helper()
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func readFileFor(t *testing.T, file storage.File, want string) storage.Attr {
	t.Helper()
	read, err := file.ReadAt(t.Context(), 0, len(want)+10)
	if err != nil || string(read.Data) != want || read.Attr.Size != int64(len(want)) {
		t.Fatalf("read = %q, size %d, %v; want %q", read.Data, read.Attr.Size, err, want)
	}
	return read.Attr
}

func TestRetainedFileReadsCurrentObjectThroughNameChanges(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	f := openFileFor(t, session, "first", storage.FileOpenOptions{Read: true, Write: true, Create: true, Mode: 0600})
	if _, err := f.WriteAt(t.Context(), 0, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	id := readFileFor(t, f, "initial").ID
	for _, content := range []string{"replacement grows", "x", ""} {
		if err := volume.Write(t.Context(), "first", []byte(content)); err != nil {
			t.Fatal(err)
		}
		if attr := readFileFor(t, f, content); attr.ID != id {
			t.Fatalf("identity changed: %+v", attr)
		}
	}
	if err := volume.Rename(t.Context(), "first", "moved"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(t.Context(), 2, []byte("old")); err != nil {
		t.Fatal(err)
	}
	readFileFor(t, f, "\x00\x00old")
	if err := volume.Write(t.Context(), "replacement", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := volume.Rename(t.Context(), "replacement", "moved"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Truncate(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	readFileFor(t, f, "\x00\x00o")
	if data, err := volume.Read(t.Context(), "moved"); err != nil || string(data) != "new" {
		t.Fatalf("replacement=%q %v", data, err)
	}
	mode := storage.AttrChange{Mode: newMode(0640)}
	attr, err := f.SetAttr(t.Context(), mode)
	if err != nil || attr.ID != id || attr.Mode.Perm() != 0640 {
		t.Fatalf("detached attributes=%+v %v", attr, err)
	}
	if err := f.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.StatNode(t.Context(), id); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("reclaimed identity=%v", err)
	}
}

func newMode(mode uint32) *fs.FileMode { value := fs.FileMode(mode); return &value }

type pausedFilePut struct {
	*memory.Objects
	pause   atomic.Bool
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func newPausedFilePut(t *testing.T) *pausedFilePut {
	p := &pausedFilePut{Objects: memory.New(), entered: make(chan struct{}), resume: make(chan struct{})}
	t.Cleanup(p.release)
	return p
}

func (p *pausedFilePut) release() { p.once.Do(func() { close(p.resume) }) }

func (p *pausedFilePut) Put(ctx context.Context, key string, content []byte) ([]byte, error) {
	digest, err := p.Objects.Put(ctx, key, content)
	if err == nil && p.pause.CompareAndSwap(true, false) {
		close(p.entered)
		<-p.resume
	}
	return digest, err
}

func TestRetainedRangeWriteRetriesAgainstLatestRevision(t *testing.T) {
	objects := newPausedFilePut(t)
	volume, _ := fileVolume(t, objects, 4096, nil)
	t.Cleanup(objects.release)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	t.Cleanup(objects.release)
	first := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true})
	second := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true})
	if _, err := first.WriteAt(t.Context(), 0, []byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	objects.pause.Store(true)
	result := make(chan error, 1)
	go func() { _, err := first.WriteAt(t.Context(), 1, []byte("XX")); result <- err }()
	select {
	case <-objects.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not stage")
	}
	if _, err := second.WriteAt(t.Context(), 6, []byte("YY")); err != nil {
		t.Fatal(err)
	}
	objects.release()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	readFileFor(t, first, "aXXdefYY")
	readFileFor(t, second, "aXXdefYY")
}

func TestRetainedRenewalHasAdmissionWhileTheOnlyDataSlotIsStaging(t *testing.T) {
	objects := newPausedFilePut(t)
	volume, _ := fileVolume(t, objects, 4096, nil)
	options := storage.DefaultFileSessionOptions()
	options.MaxOperations = 1
	session := fileSessionFor(t, volume, options)
	t.Cleanup(objects.release)
	file := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true})
	before, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	objects.pause.Store(true)
	written := make(chan error, 1)
	go func() { _, err := file.WriteAt(t.Context(), 0, []byte("published")); written <- err }()
	select {
	case <-objects.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not enter object staging")
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("data slot was not occupied: %v", err)
	}
	renewed, err := session.Renew(t.Context())
	if err != nil || renewed.Revision <= before.Revision || renewed.Remaining <= 0 {
		t.Fatalf("renewal under occupied data admission = %+v, %v", renewed, err)
	}
	status, err := session.Status(t.Context())
	if err != nil || status.Revision != renewed.Revision {
		t.Fatalf("status under occupied data admission = %+v, %v", status, err)
	}
	objects.release()
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	readFileFor(t, file, "published")
}

func TestRetainedPOSIXOwnerCleanupProgressesWhileDataAdmissionIsFull(t *testing.T) {
	for _, cleanup := range []string{"close owner", "explicit unlock"} {
		t.Run(cleanup, func(t *testing.T) {
			objects := newPausedFilePut(t)
			volume, _ := fileVolume(t, objects, 4096, nil)
			options := storage.DefaultFileSessionOptions()
			options.MaxOperations = 1
			first := fileSessionFor(t, volume, options)
			second := fileSessionFor(t, volume, options)
			t.Cleanup(objects.release)
			writer := openFileFor(t, first, "f", storage.FileOpenOptions{Write: true, Create: true})
			closing := openFileFor(t, first, "f", storage.FileOpenOptions{Write: true})
			waiter := openFileFor(t, second, "f", storage.FileOpenOptions{Write: true})
			lock := storage.FileLock{Family: storage.POSIX, Type: storage.Exclusive, End: math.MaxInt64}
			if result, err := closing.SetLock(t.Context(), 41, lock, retainedLockRequest(t, first)); err != nil || result.State != storage.LockGranted {
				t.Fatalf("owner grant = %+v, %v", result, err)
			}
			pending := retainedLockRequest(t, second)
			lock.Wait = true
			if result, err := waiter.SetLock(t.Context(), 72, lock, pending); err != nil || result.State != storage.LockPending {
				t.Fatalf("waiter = %+v, %v", result, err)
			}
			objects.pause.Store(true)
			written := make(chan error, 1)
			go func() { _, err := writer.WriteAt(t.Context(), 0, []byte("write")); written <- err }()
			select {
			case <-objects.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("write did not occupy data admission")
			}
			if _, err := writer.Stat(t.Context()); !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("data admission = %v", err)
			}
			if cleanup == "close owner" {
				if err := closing.DropLocks(t.Context(), 41, storage.POSIX); err != nil {
					t.Fatalf("owner cleanup under data saturation: %v", err)
				}
			} else {
				unlock := storage.FileLock{Family: storage.POSIX, Type: storage.Unlock, End: math.MaxInt64}
				if result, err := closing.SetLock(t.Context(), 41, unlock, retainedLockRequest(t, first)); err != nil || result.State != storage.LockReleased {
					t.Fatalf("explicit unlock under data saturation = %+v, %v", result, err)
				}
			}
			if err := closing.Close(t.Context()); err != nil {
				t.Fatalf("closing descriptor after owner cleanup: %v", err)
			}
			if result, err := waiter.QueryLock(t.Context(), 72, pending); err != nil || result.State != storage.LockGranted {
				t.Fatalf("closed POSIX owner left protection behind: %+v, %v", result, err)
			}
			if _, err := first.Renew(t.Context()); err != nil {
				t.Fatalf("cleanup consumed renewal admission: %v", err)
			}
			objects.release()
			if err := <-written; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRetainedPendingLockCancellationProgressesWhileDataAdmissionIsFull(t *testing.T) {
	objects := newPausedFilePut(t)
	volume, _ := fileVolume(t, objects, 4096, nil)
	options := storage.DefaultFileSessionOptions()
	options.MaxOperations = 1
	first := fileSessionFor(t, volume, options)
	second := fileSessionFor(t, volume, options)
	t.Cleanup(objects.release)
	writer := openFileFor(t, first, "f", storage.FileOpenOptions{Write: true, Create: true})
	closing := openFileFor(t, first, "f", storage.FileOpenOptions{Write: true})
	holder := openFileFor(t, second, "f", storage.FileOpenOptions{Write: true})
	lock := storage.FileLock{Family: storage.POSIX, Type: storage.Exclusive, End: math.MaxInt64}
	if result, err := holder.SetLock(t.Context(), 72, lock, retainedLockRequest(t, second)); err != nil || result.State != storage.LockGranted {
		t.Fatalf("blocking holder = %+v, %v", result, err)
	}
	pending := retainedLockRequest(t, first)
	lock.Wait = true
	if result, err := closing.SetLock(t.Context(), 41, lock, pending); err != nil || result.State != storage.LockPending {
		t.Fatalf("pending request = %+v, %v", result, err)
	}
	objects.pause.Store(true)
	written := make(chan error, 1)
	go func() { _, err := writer.WriteAt(t.Context(), 0, []byte("write")); written <- err }()
	select {
	case <-objects.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not occupy data admission")
	}
	if result, err := closing.QueryLock(t.Context(), 41, pending); err != nil || result.State != storage.LockPending {
		t.Fatalf("pending query under data saturation = %+v, %v", result, err)
	}
	if result, err := closing.CancelLock(t.Context(), 41, pending); err != nil || result.State != storage.LockCancelled || result.EverGranted {
		t.Fatalf("cancellation under data saturation = %+v, %v", result, err)
	}
	if err := holder.DropLocks(t.Context(), 72, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	if result, err := closing.QueryLock(t.Context(), 41, pending); err != nil || result.State != storage.LockCancelled || result.EverGranted {
		t.Fatalf("cancelled request acquired after handoff = %+v, %v", result, err)
	}
	if err := closing.DropLocks(t.Context(), 41, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	if err := closing.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	lock.Wait = false
	if conflict, err := holder.GetLock(t.Context(), 72, lock); err != nil || conflict.Found {
		t.Fatalf("closed cancelled owner left a ghost range: %+v, %v", conflict, err)
	}
	if _, err := first.Renew(t.Context()); err != nil {
		t.Fatalf("reconciliation consumed renewal admission: %v", err)
	}
	objects.release()
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

type pausedFileControl struct {
	*sqlite.LockingStore
	pause   atomic.Bool
	entered chan struct{}
}

type pausedLockNodes struct {
	*sqlite.LockingStore
	remaining atomic.Int64
	entered   chan struct{}
}

func (m *pausedLockNodes) OpenFile(ctx context.Context, name string, options storage.FileOpenOptions) (metastore.File, error) {
	file, err := m.LockingStore.OpenFile(ctx, name, options)
	if err != nil {
		return nil, err
	}
	return &pausedLockNode{File: file, authority: m}, nil
}

type pausedLockNode struct {
	metastore.File
	authority *pausedLockNodes
}

func (f *pausedLockNode) Node(ctx context.Context) (metastore.FileState, error) {
	for {
		remaining := f.authority.remaining.Load()
		if remaining == 0 {
			return f.File.Node(ctx)
		}
		if f.authority.remaining.CompareAndSwap(remaining, remaining-1) {
			f.authority.entered <- struct{}{}
			<-ctx.Done()
			return metastore.FileState{}, ctx.Err()
		}
	}
}

func TestRetainedLockAdmissionPartitionsAreBoundedAndPreserveRenewal(t *testing.T) {
	for _, partition := range []string{"acquisition", "reconciliation"} {
		t.Run(partition, func(t *testing.T) {
			objects := memory.New()
			_, meta := fileVolume(t, objects, 4096, nil)
			paused := &pausedLockNodes{LockingStore: meta, entered: make(chan struct{}, 2)}
			volume := objectstore.New(objects, paused)
			t.Cleanup(func() {
				if err := volume.Close(); err != nil {
					t.Error(err)
				}
			})
			options := storage.DefaultFileSessionOptions()
			options.MaxOperations = 1
			session := fileSessionFor(t, volume, options)
			file := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true})
			lock := storage.FileLock{Family: storage.POSIX, Type: storage.Exclusive, End: math.MaxInt64}
			request := retainedLockRequest(t, session)
			if result, err := file.SetLock(t.Context(), 41, lock, request); err != nil || result.State != storage.LockGranted {
				t.Fatalf("initial lock = %+v, %v", result, err)
			}
			operation := func(ctx context.Context) error {
				if partition == "acquisition" {
					_, err := file.GetLock(ctx, 72, lock)
					return err
				}
				_, err := file.QueryLock(ctx, 41, request)
				return err
			}
			paused.remaining.Store(2)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			results := make(chan error, 2)
			for range 2 {
				go func() { results <- operation(ctx) }()
			}
			for range 2 {
				select {
				case <-paused.entered:
				case <-time.After(3 * time.Second):
					t.Fatal("lock controls did not occupy two independent slots")
				}
			}
			if err := operation(t.Context()); !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("%s partition exceeded its bound: %v", partition, err)
			}
			if _, err := session.Renew(t.Context()); err != nil {
				t.Fatalf("saturated %s consumed heartbeat admission: %v", partition, err)
			}
			if _, err := file.Stat(t.Context()); err != nil {
				t.Fatalf("saturated %s consumed data admission: %v", partition, err)
			}
			if partition == "acquisition" {
				if err := file.DropLocks(t.Context(), 41, storage.POSIX); err != nil {
					t.Fatalf("acquisitions consumed release admission: %v", err)
				}
			} else {
				if _, err := file.GetLock(t.Context(), 72, lock); err != nil {
					t.Fatalf("reconciliation consumed acquisition admission: %v", err)
				}
			}
			cancel()
			for range 2 {
				if err := <-results; !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled lock control = %v", err)
				}
			}
			if err := file.DropLocks(t.Context(), 41, storage.POSIX); err != nil {
				t.Fatalf("cancelled controls retained release admission: %v", err)
			}
		})
	}
}

func (m *pausedFileControl) Usage(ctx context.Context) (int64, error) {
	if m.pause.Load() {
		m.entered <- struct{}{}
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return m.LockingStore.Usage(ctx)
}

func TestRetainedControlAdmissionIsBoundedAndCancellationReleasesIt(t *testing.T) {
	objects := memory.New()
	_, meta := fileVolume(t, objects, 4096, nil)
	control := &pausedFileControl{LockingStore: meta, entered: make(chan struct{}, 2)}
	volume := objectstore.New(objects, control)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Error(err)
		}
	})
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	control.pause.Store(true)
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	secondCtx, cancelSecond := context.WithCancel(t.Context())
	t.Cleanup(cancelFirst)
	t.Cleanup(cancelSecond)
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := session.Status(firstCtx); first <- err }()
	go func() { _, err := session.Status(secondCtx); second <- err }()
	for range 2 {
		select {
		case <-control.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("control calls did not occupy their independent slots")
		}
	}
	if _, err := session.Renew(t.Context()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("control admission exceeded its two-slot bound: %v", err)
	}
	cancelFirst()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled control = %v", err)
	}
	control.pause.Store(false)
	if _, err := session.Renew(t.Context()); err != nil {
		t.Fatalf("cancelled control retained admission: %v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- session.Close(t.Context()) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pending control call prevented session retirement")
	}
	cancelSecond()
	if err := <-second; !errors.Is(err, context.Canceled) {
		t.Fatalf("retired control = %v", err)
	}
}

type observedFileAuthority struct {
	*sqlite.LockingStore
	entered chan struct{}
	once    sync.Once
}

func (m *observedFileAuthority) Advisory(ctx context.Context) (*advisory.Coordinator, error) {
	m.once.Do(func() { close(m.entered) })
	return m.LockingStore.Advisory(ctx)
}

func TestRetainedEnrollmentCanCancelWhileNativePublicationIsPaused(t *testing.T) {
	objects := memory.New()
	base, meta := fileVolume(t, objects, 4096, nil)
	observed := &observedFileAuthority{LockingStore: meta, entered: make(chan struct{})}
	volume := objectstore.New(objects, observed)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Error(err)
		}
	})
	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(release)
	publication := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
		close(entered)
		<-resume
		return func(storage.PublicationResult) error { return nil }, nil
	})
	published := make(chan error, 1)
	go func() { published <- base.Create(publication, "held") }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("native publication did not enter accounting")
	}
	checked := make(chan error, 1)
	go func() { checked <- volume.CheckFileStorage() }()
	select {
	case err := <-checked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("capability probing waited for native publication")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	opened := make(chan error, 1)
	go func() {
		session, err := volume.NewFileSession(ctx, storage.DefaultFileSessionOptions())
		if err == nil {
			err = session.Close(context.Background())
		}
		opened <- err
	}()
	select {
	case <-observed.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("enrollment did not reach native authority acquisition")
	}
	closed := make(chan error, 1)
	go func() { closed <- volume.CloseFileSessions() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("enrollment held the session map while waiting for publication")
	}
	cancel()
	select {
	case err := <-opened:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled enrollment = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("native enrollment ignored cancellation")
	}
	release()
	if err := <-published; err != nil {
		t.Fatal(err)
	}
}

func TestRetainedSessionExpiryFencesUploadBeforeAdvisoryHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		objects := newPausedFilePut(t)
		volume, _ := fileVolume(t, objects, 4096, nil)
		options := storage.DefaultFileSessionOptions()
		options.Lease = 150 * time.Millisecond
		firstSession := fileSessionFor(t, volume, options)
		secondSession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
		t.Cleanup(objects.release)
		first := openFileFor(t, firstSession, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true})
		second := openFileFor(t, secondSession, "f", storage.FileOpenOptions{Read: true, Write: true})
		if _, err := first.WriteAt(t.Context(), 0, []byte("old")); err != nil {
			t.Fatal(err)
		}
		lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
		firstStatus, err := firstSession.Status(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		firstID, err := storage.NewLockRequestID(firstStatus.ActionEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if result, err := first.SetLock(t.Context(), 7, lock, firstID); err != nil || result.State != storage.LockGranted {
			t.Fatalf("first lock=%+v %v", result, err)
		}
		secondStatus, err := secondSession.Status(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		secondID, err := storage.NewLockRequestID(secondStatus.ActionEpoch)
		if err != nil {
			t.Fatal(err)
		}
		lock.Wait = true
		if result, err := second.SetLock(t.Context(), 7, lock, secondID); err != nil || result.State != storage.LockPending {
			t.Fatalf("second lock=%+v %v", result, err)
		}
		objects.pause.Store(true)
		writeDone := make(chan error, 1)
		go func() { _, err := first.WriteAt(t.Context(), 0, []byte("late")); writeDone <- err }()
		select {
		case <-objects.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("write did not stage")
		}
		await(t, "expiry advisory handoff", func() bool {
			result, err := second.QueryLock(t.Context(), 7, secondID)
			if err != nil {
				t.Fatal(err)
			}
			return result.State == storage.LockGranted
		})
		if _, err := second.WriteAt(t.Context(), 0, []byte("winner")); err != nil {
			t.Fatal(err)
		}
		objects.release()
		if err := <-writeDone; !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("retired upload=%v, want ESTALE", err)
		}
		readFileFor(t, second, "winner")
		if _, err := firstSession.Renew(t.Context()); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("expired renewal=%v", err)
		}
	})
}

func TestRetainedFileAdmissionAndSizeAreBounded(t *testing.T) {
	objects := newPausedFilePut(t)
	volume, _ := fileVolume(t, objects, 4096, func(options *sqlite.Options) {
		config := advisory.DefaultConfig()
		config.MaxMaterializedBytes = 16
		config.MaxFileBytes = 8
		config.MaxMaterializations = 1
		options.Advisory = config
	})
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session := fileSessionFor(t, volume, options)
	t.Cleanup(objects.release)
	f := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true})
	if _, err := session.OpenFile(t.Context(), "other", storage.FileOpenOptions{Read: true, Create: true}); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("file limit=%v", err)
	}
	if _, err := volume.Stat(t.Context(), "other"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused open created a name: %v", err)
	}
	if _, err := f.WriteAt(t.Context(), 0, bytes.Repeat([]byte("x"), 9)); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("size limit=%v", err)
	}
	if _, err := f.WriteAt(t.Context(), 0, []byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	objects.pause.Store(true)
	writeDone := make(chan error, 1)
	go func() { _, err := f.WriteAt(t.Context(), 0, []byte("z")); writeDone <- err }()
	select {
	case <-objects.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not stage")
	}
	if _, err := f.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("occupied materialization=%v", err)
	}
	objects.release()
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	readFileFor(t, f, "zbcdefgh")
}

func TestRetainedOpenChecksIdentityAndAccess(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	f := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true, Exclusive: true, Mode: 0600})
	attr, err := f.Stat(t.Context())
	if err != nil || attr.Mode.Perm() != 0600 {
		t.Fatalf("created attr=%+v %v", attr, err)
	}
	if _, err := session.OpenFile(t.Context(), "f", storage.FileOpenOptions{Read: true, Create: true, Exclusive: true}); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("exclusive open=%v", err)
	}
	if err := volume.Remove(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	if err := volume.Create(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.OpenFile(t.Context(), "f", storage.FileOpenOptions{Read: true, ExpectedID: attr.ID}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("replacement open=%v", err)
	}
	byID, err := session.OpenNode(t.Context(), attr.ID, storage.FileOpenOptions{Read: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := byID.WriteAt(t.Context(), 0, []byte("x")); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only write=%v", err)
	}
	if _, err := byID.Truncate(t.Context(), 0); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only truncate=%v", err)
	}
	if _, err := byID.ReadAt(t.Context(), -1, 1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative read=%v", err)
	}
	if _, err := byID.SetAttr(t.Context(), storage.AttrChange{Mode: newMode(0640)}); err != nil {
		t.Fatal(err)
	}
	if err := byID.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := byID.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := byID.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed stat=%v", err)
	}
}

func TestRetainedWritesPreserveStrongScopeAtFinalPublication(t *testing.T) {
	clock := &publicationClock{now: time.Now().Add(time.Minute)}
	options := locking.DefaultOptions()
	options.Clock = clock
	objects := newPausedFilePut(t)
	volume, _, _ := lockingObjectVolume(t, options, objects)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	t.Cleanup(objects.release)
	f := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true})
	if _, err := f.WriteAt(t.Context(), 0, []byte("original")); err != nil {
		t.Fatal(err)
	}
	owner := publicationOwner(t, volume.LockService())
	grant := publicationGrant(t, volume.LockService(), owner, "f", locking.Exclusive)
	if _, err := f.WriteAt(t.Context(), 0, []byte("bad")); err == nil {
		t.Fatal("anonymous write bypassed strong exclusive grant")
	}
	ctx := locking.WithScope(t.Context(), locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}})
	if _, err := f.WriteAt(ctx, 0, []byte("OK")); err != nil {
		t.Fatal(err)
	}
	readFileFor(t, f, "OKiginal")
	objects.pause.Store(true)
	result := make(chan error, 1)
	go func() { _, err := f.WriteAt(ctx, 0, []byte("late")); result <- err }()
	select {
	case <-objects.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not stage")
	}
	clock.advance(11 * time.Second)
	objects.release()
	if err := <-result; !errors.Is(err, &locking.Error{Code: locking.StaleGrant}) {
		t.Fatalf("expired strong proof=%v", err)
	}
	readFileFor(t, f, "OKiginal")
}

type changedFileGet struct {
	*memory.Objects
	before func()
	once   sync.Once
}

type measuredFileObjects struct {
	*memory.Objects
	gets        atomic.Int64
	puts        atomic.Int64
	rejectReads atomic.Bool
}

func (o *measuredFileObjects) GetBounded(ctx context.Context, key string, limit int64) ([]byte, error) {
	o.gets.Add(1)
	if o.rejectReads.Load() {
		return nil, syscall.EIO
	}
	return o.Objects.GetBounded(ctx, key, limit)
}

func (o *measuredFileObjects) Put(ctx context.Context, key string, body []byte) ([]byte, error) {
	o.puts.Add(1)
	return o.Objects.Put(ctx, key, body)
}

func TestRetainedCapturedRevisionHonorsSessionAndNativeFileLimits(t *testing.T) {
	for _, limits := range []struct {
		name            string
		session, native int64
	}{{"session ceiling", 4, 64}, {"native ceiling", 64, 4}} {
		t.Run(limits.name, func(t *testing.T) {
			objects := &measuredFileObjects{Objects: memory.New()}
			volume, _ := fileVolume(t, objects, 4096, func(options *sqlite.Options) {
				options.Advisory.MaxFileBytes = limits.native
			})
			if err := volume.Write(t.Context(), "f", []byte("ok")); err != nil {
				t.Fatal(err)
			}
			options := storage.DefaultFileSessionOptions()
			options.MaxFileSize = limits.session
			session := fileSessionFor(t, volume, options)
			file := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true})
			if attr, err := file.Stat(t.Context()); err != nil || attr.Size != 2 {
				t.Fatalf("initial stat = %+v, %v", attr, err)
			}
			if err := volume.Write(t.Context(), "f", []byte("too large")); err != nil {
				t.Fatal(err)
			}
			gets, puts := objects.gets.Load(), objects.puts.Load()
			for name, operation := range map[string]func() error{
				"read":     func() error { _, err := file.ReadAt(t.Context(), 0, 1); return err },
				"write":    func() error { _, err := file.WriteAt(t.Context(), 0, []byte("x")); return err },
				"truncate": func() error { _, err := file.Truncate(t.Context(), 1); return err },
				"sync":     func() error { return file.Sync(t.Context()) },
			} {
				if err := operation(); !errors.Is(err, syscall.EFBIG) {
					t.Fatalf("%s used a stale or larger size allowance: %v", name, err)
				}
				if objects.gets.Load() != gets || objects.puts.Load() != puts {
					t.Fatalf("%s materialized or staged the refused revision", name)
				}
			}
			objects.rejectReads.Store(true)
			attr, err := file.Truncate(t.Context(), 0)
			if err != nil || attr.Size != 0 {
				t.Fatalf("emptying oversized unreadable contents = %+v, %v", attr, err)
			}
			if objects.gets.Load() != gets || objects.puts.Load() != puts {
				t.Fatal("zero truncation read or staged discarded contents")
			}
			if used, err := volume.Usage(t.Context()); err != nil || used != 0 {
				t.Fatalf("zero truncation usage = %d, %v", used, err)
			}
		})
	}
}

func TestRetainedZeroTruncateStillChecksStrongPublicationPermission(t *testing.T) {
	objects := &measuredFileObjects{Objects: memory.New()}
	volume, _ := fileVolume(t, objects, 4096, nil)
	if err := volume.Write(t.Context(), "f", []byte("protected")); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFileSize = 4
	session := fileSessionFor(t, volume, options)
	file := openFileFor(t, session, "f", storage.FileOpenOptions{Write: true})
	owner := publicationOwner(t, volume.LockService())
	grant := publicationGrant(t, volume.LockService(), owner, "f", locking.Exclusive)
	objects.rejectReads.Store(true)
	gets := objects.gets.Load()
	if _, err := file.Truncate(t.Context(), 0); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("anonymous zero truncation bypassed strong protection: %v", err)
	}
	ctx := locking.WithScope(t.Context(), locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}})
	if attr, err := file.Truncate(ctx, 0); err != nil || attr.Size != 0 {
		t.Fatalf("authorized zero truncation = %+v, %v", attr, err)
	}
	if objects.gets.Load() != gets {
		t.Fatal("zero truncation fetched protected contents")
	}
}

func (o *changedFileGet) GetBounded(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	if o.before != nil {
		o.once.Do(o.before)
	}
	return o.Objects.GetBounded(ctx, key, maxBytes)
}

func TestRetainedReadRetriesCollectedRevisionAndRejectsMissingCurrentObject(t *testing.T) {
	objects := &changedFileGet{Objects: memory.New()}
	volume, meta := fileVolume(t, objects, 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	f := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true})
	if _, err := f.WriteAt(t.Context(), 0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	objects.before = func() {
		if err := volume.Write(t.Context(), "f", []byte("new revision")); err != nil {
			t.Fatal(err)
		}
		if _, err := volume.Sweep(t.Context(), 16); err != nil {
			t.Fatal(err)
		}
	}
	readFileFor(t, f, "new revision")
	node, err := meta.Stat(t.Context(), "f")
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Delete(t.Context(), string(node.Content)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENOENT) {
		t.Fatalf("missing current object=%v", err)
	}
	if err := f.Sync(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("missing object sync=%v", err)
	}
}

func TestRetainedSessionCloseCanRetryKnownAccountingRefusal(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	failure := errors.New("known cleanup refusal")
	var refuse atomic.Bool
	refuse.Store(true)
	ctx := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous > 0 && next == 0 && refuse.Load() {
			return nil, failure
		}
		return func(storage.PublicationResult) error { return nil }, nil
	})
	session, err := volume.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	f := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true})
	if _, err := f.WriteAt(t.Context(), 0, []byte("charge")); err != nil {
		t.Fatal(err)
	}
	if err := volume.Remove(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	if err := volume.Close(); !errors.Is(err, failure) {
		t.Fatalf("first close=%v", err)
	}
	refuse.Store(false)
	if err := volume.Close(); err != nil {
		t.Fatalf("retry close=%v", err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type delayedNativeOpen struct {
	*sqlite.LockingStore
	entered chan struct{}
	release chan struct{}
}

func (m *delayedNativeOpen) OpenFile(ctx context.Context, name string, options storage.FileOpenOptions) (metastore.File, error) {
	close(m.entered)
	<-m.release
	return m.LockingStore.OpenFile(ctx, name, options)
}

func TestRetainedExpiredAtomicOpenCannotCreateOrTruncate(t *testing.T) {
	for _, create := range []bool{true, false} {
		t.Run(map[bool]string{true: "create", false: "truncate"}[create], func(t *testing.T) {
			objects := memory.New()
			base, meta := fileVolume(t, objects, 4096, nil)
			if !create {
				if err := base.Write(t.Context(), "f", []byte("preserved")); err != nil {
					t.Fatal(err)
				}
			}
			delayed := &delayedNativeOpen{LockingStore: meta, entered: make(chan struct{}), release: make(chan struct{})}
			volume := objectstore.New(objects, delayed)
			t.Cleanup(func() {
				if err := volume.Close(); err != nil {
					t.Error(err)
				}
			})
			options := storage.DefaultFileSessionOptions()
			options.Lease = 60 * time.Millisecond
			session := fileSessionFor(t, volume, options)
			release := sync.OnceFunc(func() { close(delayed.release) })
			t.Cleanup(release)
			result := make(chan error, 1)
			go func() {
				_, err := session.OpenFile(t.Context(), "f", storage.FileOpenOptions{Read: true, Write: true, Create: create, Truncate: !create})
				result <- err
			}()
			select {
			case <-delayed.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("native open did not start")
			}
			await(t, "expired open session", func() bool { _, err := session.Status(t.Context()); return errors.Is(err, syscall.ESTALE) })
			release()
			if err := <-result; !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("late native open=%v", err)
			}
			if create {
				if _, err := base.Stat(t.Context(), "f"); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("expired open created name: %v", err)
				}
			} else if body, err := base.Read(t.Context(), "f"); err != nil || string(body) != "preserved" {
				t.Fatalf("expired open truncated: %q %v", body, err)
			}
		})
	}
}

func TestRetainedSessionsShareAdvisoryAuthorityAcrossObjectWrappers(t *testing.T) {
	objects := memory.New()
	firstVolume, meta := fileVolume(t, objects, 4096, nil)
	secondVolume := objectstore.New(objects, meta)
	t.Cleanup(func() {
		if err := secondVolume.Close(); err != nil {
			t.Error(err)
		}
	})
	firstSession := fileSessionFor(t, firstVolume, storage.DefaultFileSessionOptions())
	secondSession := fileSessionFor(t, secondVolume, storage.DefaultFileSessionOptions())
	first := openFileFor(t, firstSession, "f", storage.FileOpenOptions{Read: true, Create: true})
	second := openFileFor(t, secondSession, "f", storage.FileOpenOptions{Read: true, Write: true})
	status, err := firstSession.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
	if result, err := first.SetLock(t.Context(), 11, lock, request); err != nil || result.State != storage.LockGranted {
		t.Fatalf("read-only flock=%+v %v", result, err)
	}
	if conflict, err := second.GetLock(t.Context(), 11, lock); err != nil || !conflict.Found {
		t.Fatalf("cross-wrapper conflict=%+v %v", conflict, err)
	}
	if _, err := second.WriteAt(t.Context(), 0, []byte("advisory")); err != nil {
		t.Fatalf("advisory blocked ordinary IO: %v", err)
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if conflict, err := second.GetLock(t.Context(), 11, lock); err != nil || conflict.Found {
		t.Fatalf("closed flock=%+v %v", conflict, err)
	}
	readFileFor(t, second, "advisory")
}

func TestRetainedControlMethodsKeepIdentityAndKnownLockOutcomes(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	before, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	after, err := session.Renew(t.Context())
	if err != nil || after.Epoch != before.Epoch || after.Revision != before.Revision+1 || after.Remaining <= 0 {
		t.Fatalf("renew before=%+v after=%+v %v", before, after, err)
	}
	if err := volume.Mkdir(t.Context(), "dir"); err != nil {
		t.Fatal(err)
	}
	dir, err := volume.Stat(t.Context(), "dir")
	if err != nil {
		t.Fatal(err)
	}
	if err := volume.Rename(t.Context(), "dir", "renamed"); err != nil {
		t.Fatal(err)
	}
	changed, err := session.SetNodeAttr(t.Context(), dir.ID, storage.AttrChange{Mode: newMode(0700)})
	if err != nil || changed.ID != dir.ID || changed.Mode.Perm() != 0700 {
		t.Fatalf("node setattr=%+v %v", changed, err)
	}
	if got, err := session.StatNode(t.Context(), dir.ID); err != nil || got.ID != dir.ID || got.Mode.Perm() != 0700 {
		t.Fatalf("node stat=%+v %v", got, err)
	}
	first := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true})
	second := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true})
	request := func() storage.LockRequestID {
		id, err := storage.NewLockRequestID(after.ActionEpoch)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	lock := storage.FileLock{Family: storage.POSIX, Type: storage.Exclusive, Start: 2, End: 8}
	if result, err := first.SetLock(t.Context(), 1, lock, request()); err != nil || result.State != storage.LockGranted {
		t.Fatalf("first POSIX=%+v %v", result, err)
	}
	pending := request()
	lock.Wait = true
	if result, err := second.SetLock(t.Context(), 2, lock, pending); err != nil || result.State != storage.LockPending {
		t.Fatalf("pending POSIX=%+v %v", result, err)
	}
	if result, err := second.CancelLock(t.Context(), 2, pending); err != nil || result.State != storage.LockCancelled || result.EverGranted {
		t.Fatalf("cancel=%+v %v", result, err)
	}
	if err := first.DropLocks(t.Context(), 1, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	if result, err := second.QueryLock(t.Context(), 2, pending); err != nil || result.State != storage.LockCancelled || result.EverGranted {
		t.Fatalf("cancelled after release=%+v %v", result, err)
	}
	if _, err := first.WriteAt(t.Context(), 0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if used, err := volume.Usage(t.Context()); err != nil || used != 3 {
		t.Fatalf("usage=%d %v", used, err)
	}
}

type uncertainFilePut struct {
	*memory.Objects
	fail  atomic.Bool
	cause error
}

func (o *uncertainFilePut) Put(ctx context.Context, key string, body []byte) ([]byte, error) {
	digest, err := o.Objects.Put(ctx, key, body)
	if err == nil && o.fail.Load() {
		return nil, o.cause
	}
	return digest, err
}

func TestRetainedUncertainPutPreservesOldBytesAndQuarantinesStage(t *testing.T) {
	failure := errors.New("object result lost")
	objects := &uncertainFilePut{Objects: memory.New(), cause: failure}
	volume, meta := fileVolume(t, objects, 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	f := openFileFor(t, session, "f", storage.FileOpenOptions{Read: true, Write: true, Create: true})
	if _, err := f.WriteAt(t.Context(), 0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	objects.fail.Store(true)
	if _, err := f.WriteAt(t.Context(), 0, []byte("changed")); !errors.Is(err, failure) {
		t.Fatalf("uncertain put=%v", err)
	}
	readFileFor(t, f, "old")
	status, err := meta.ObjectStatus(t.Context())
	if err != nil || status.UnresolvedCount != 1 || status.UnresolvedBytes != 7 || status.ReservedCount != 0 {
		t.Fatalf("uncertain stage=%+v %v", status, err)
	}
	if _, err := volume.Sweep(t.Context(), 16); err != nil {
		t.Fatal(err)
	}
	status, err = meta.ObjectStatus(t.Context())
	if err != nil || status.UnresolvedCount != 1 {
		t.Fatalf("sweep lost uncertain stage=%+v %v", status, err)
	}
}
