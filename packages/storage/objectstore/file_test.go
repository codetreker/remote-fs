package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

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

func fileSessionFor(t *testing.T, volume storage.FileStorage, options storage.FileSessionOptions) *retainedTestSession {
	t.Helper()
	state, err := volume.FileState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, status, err := volume.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	closeID, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := session.Close(context.Background(), closeID); err != nil {
			t.Errorf("close session: %v", err)
		}
	})
	return &retainedTestSession{FileSession: session, rootID: state.RootID}
}

type retainedOpenOptions struct {
	Read, Write, Create, Truncate, Exclusive bool
	ExpectedID                               uint64
	Metadata                                 storage.Metadata
}

type retainedFile struct {
	storage.File
	session storage.FileSession
	closeID storage.FileActionID
}

func (f *retainedFile) action(ctx context.Context) (storage.FileActionID, error) {
	status, err := f.session.Status(ctx)
	if err != nil {
		return "", err
	}
	return storage.NewFileActionID(status.ActionEpoch)
}
func (f *retainedFile) Stat(ctx context.Context) (storage.Attr, error) {
	observation, err := f.File.Stat(ctx, storage.ObservationOptions{})
	return observation.Attr, err
}
func (f *retainedFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	return f.File.ReadAt(ctx, storage.FileReadRequest{Offset: offset, Length: length})
}
func (f *retainedFile) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	id, err := f.action(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	result, err := f.File.WriteAt(ctx, storage.FileWriteRequest{Offset: offset, Data: data}, id)
	return retainedAttrResult(result, err)
}
func (f *retainedFile) Truncate(ctx context.Context, size int64) (storage.Attr, error) {
	id, err := f.action(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	result, err := f.File.Truncate(ctx, storage.FileTruncateRequest{Size: size}, id)
	return retainedAttrResult(result, err)
}
func (f *retainedFile) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	id, err := f.action(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	result, err := f.File.SetAttr(ctx, change, id)
	return retainedAttrResult(result, err)
}
func retainedAttrResult(result storage.FileActionReceipt, err error) (storage.Attr, error) {
	if err != nil {
		return result.Observation.Attr, err
	}
	if result.State != storage.FileActionCompleted {
		return storage.Attr{}, fmt.Errorf("mutation returned nonterminal receipt: %+v", result)
	}
	return result.Observation.Attr, nil
}
func (f *retainedFile) Close(ctx context.Context) error {
	result, err := f.File.Close(ctx, f.closeID)
	if err != nil {
		return err
	}
	if result.State != storage.FileActionCompleted && result.State != storage.FileActionRetired {
		return fmt.Errorf("close returned nonterminal receipt: %+v", result)
	}
	return nil
}

func openFileFor(t *testing.T, session *retainedTestSession, name string, options retainedOpenOptions) *retainedFile {
	t.Helper()
	root := retainedRootFor(t, session)
	defer func() {
		if _, err := root.File.Close(t.Context(), retainedAction(t, session)); err != nil {
			t.Error(err)
		}
	}()
	target := retainedTarget(t, root.File, name)
	claim := storage.AccessClaim{}
	if options.Read {
		claim.Uses |= storage.ReadContent
	}
	if options.Write {
		claim.Uses |= storage.WriteContent
	}
	var result storage.FileActionReceipt
	var err error
	if options.ExpectedID != 0 {
		target.ExpectedNodeID = options.ExpectedID
	}
	switch {
	case options.Create && (target.ExpectedEntryID == 0 || options.Exclusive):
		result, err = session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular, Metadata: options.Metadata}, Claim: claim}, retainedAction(t, session))
	case options.Truncate:
		observation, statErr := session.StatNode(t.Context(), target.ExpectedNodeID, storage.ObservationOptions{})
		if statErr != nil {
			t.Fatal(statErr)
		}
		result, err = session.ResetAndRetainAt(t.Context(), storage.ResetAndRetainRequest{Target: target, ExpectedRevision: observation.Attr.MetadataRevision, Claim: claim}, retainedAction(t, session))
	default:
		result, err = session.RetainAt(t.Context(), storage.RetainAtRequest{Target: target, Claim: claim}, retainedAction(t, session))
	}
	opened := retainedResult(t, session, result, err)
	return &retainedFile{File: opened.File, session: session, closeID: retainedAction(t, session)}
}

func readFileFor(t *testing.T, file *retainedFile, want string) storage.Attr {
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
	f := openFileFor(t, session, "first", retainedOpenOptions{Read: true, Write: true, Create: true, Metadata: storage.Metadata{{Key: "app.tag", Version: 1, Data: []byte("original")}}})
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
	before, err := f.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	metadata := storage.Metadata{{Key: "app.tag", Version: 1, Data: []byte("updated")}}
	mode := storage.AttrChange{ExpectedRevision: before.MetadataRevision, Metadata: &metadata}
	attr, err := f.SetAttr(t.Context(), mode)
	if err != nil || attr.ID != id || !reflect.DeepEqual(attr.Metadata, metadata) {
		t.Fatalf("detached attributes=%+v %v", attr, err)
	}
	if err := f.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.StatNode(t.Context(), id, storage.ObservationOptions{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("reclaimed identity=%v", err)
	}
}

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
	first := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
	second := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true})
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
	file := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
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

type pausedFileControl struct {
	*sqlite.LockingStore
	pause   atomic.Bool
	entered chan struct{}
}

func (m *pausedFileControl) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (metastore.FileSession, storage.FileSessionStatus, error) {
	session, status, err := m.LockingStore.NewFileSession(ctx, options)
	if err != nil {
		return nil, status, err
	}
	return &pausedControlSession{FileSession: session, authority: m}, status, nil
}

type pausedControlSession struct {
	metastore.FileSession
	authority *pausedFileControl
}

func (s *pausedControlSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	if s.authority.pause.Load() {
		s.authority.entered <- struct{}{}
		<-ctx.Done()
		return storage.FileSessionStatus{}, ctx.Err()
	}
	return s.FileSession.Status(ctx)
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
	closeID := retainedAction(t, session)
	go func() { _, err := session.Close(t.Context(), closeID); closed <- err }()
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

func (m *observedFileAuthority) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (metastore.FileSession, storage.FileSessionStatus, error) {
	m.once.Do(func() { close(m.entered) })
	return m.LockingStore.NewFileSession(ctx, options)
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
		session, initialStatus, err := volume.NewFileSession(ctx, storage.DefaultFileSessionOptions())
		if err == nil {
			closeID, idErr := storage.NewFileActionID(initialStatus.ActionEpoch)
			if idErr != nil {
				err = idErr
			} else {
				_, err = session.Close(context.Background(), closeID)
			}
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

func TestRetainedFileAdmissionAndSizeAreBounded(t *testing.T) {
	objects := newPausedFilePut(t)
	volume, _ := fileVolume(t, objects, 4096, func(options *sqlite.Options) {
		config := storage.DefaultFileServiceOptions()
		config.MaxMaterializedBytes = 16
		config.MaxFileBytes = 8
		config.MaxMaterializations = 1
		options.Files = config
	})
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 2
	session := fileSessionFor(t, volume, options)
	t.Cleanup(objects.release)
	f := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
	root := retainedRootFor(t, session)
	target := retainedTarget(t, root.File, "other")
	if _, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, retainedAction(t, session)); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("reference limit: %v", err)
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
	metadata := storage.Metadata{{Key: "app.tag", Version: 1, Data: []byte("original")}}
	f := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true, Create: true, Exclusive: true, Metadata: metadata})
	attr, err := f.Stat(t.Context())
	if err != nil || !reflect.DeepEqual(attr.Metadata, metadata) {
		t.Fatalf("initial metadata = %+v, %v", attr, err)
	}
	root := retainedRootFor(t, session)
	target := retainedTarget(t, root.File, "f")
	target.ExpectedEntryID = 0
	target.ExpectedNodeID = 0
	target.ExpectedMetadataRevision = 0
	if _, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}}, retainedAction(t, session)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("creation with a false absence condition = %v", err)
	}
	if current, err := volume.Stat(t.Context(), "f"); err != nil || current.ID != attr.ID || !reflect.DeepEqual(current.Metadata, metadata) {
		t.Fatalf("rejected conditional creation changed existing node: %+v, %v", current, err)
	}
	if err := volume.Remove(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	if err := volume.Create(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	target = retainedTarget(t, root.File, "f")
	target.ExpectedNodeID = attr.ID
	if _, err := session.RetainAt(t.Context(), storage.RetainAtRequest{Target: target, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, retainedAction(t, session)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("replacement identity = %v", err)
	}
	receipt, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: attr.ID, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, retainedAction(t, session))
	opened := retainedResult(t, session, receipt, err)
	byID := &retainedFile{File: opened.File, session: session, closeID: retainedAction(t, session)}
	if _, err := byID.WriteAt(t.Context(), 0, []byte("x")); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only write = %v", err)
	}
	if _, err := byID.Truncate(t.Context(), 0); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only truncate = %v", err)
	}
	if _, err := byID.ReadAt(t.Context(), -1, 1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative read = %v", err)
	}
	metadata[0].Data = []byte("changed")
	current, err := byID.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := byID.SetAttr(t.Context(), storage.AttrChange{ExpectedRevision: current.MetadataRevision, Metadata: &metadata}); err != nil {
		t.Fatal(err)
	}
	if err := byID.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := byID.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := byID.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed stat = %v", err)
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
	f := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
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
				options.Files.MaxFileBytes = limits.native
			})
			if err := volume.Write(t.Context(), "f", []byte("ok")); err != nil {
				t.Fatal(err)
			}
			options := storage.DefaultFileSessionOptions()
			options.MaxFileSize = limits.session
			session := fileSessionFor(t, volume, options)
			file := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true})
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
	file := openFileFor(t, session, "f", retainedOpenOptions{Write: true})
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
	f := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
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
	rawSession, initialStatus, err := volume.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	state, err := volume.FileState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session := &retainedTestSession{FileSession: rawSession, rootID: state.RootID}
	closeID, err := storage.NewFileActionID(initialStatus.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	f := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
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
	if _, err := session.Close(context.Background(), closeID); err != nil {
		t.Fatal(err)
	}
}

type delayedNativeOpen struct {
	*sqlite.LockingStore
	entered chan struct{}
	release chan struct{}
}

func (m *delayedNativeOpen) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (metastore.FileSession, storage.FileSessionStatus, error) {
	session, status, err := m.LockingStore.NewFileSession(ctx, options)
	if err != nil {
		return nil, status, err
	}
	return &delayedRetainSession{FileSession: session, authority: m}, status, nil
}

type delayedRetainSession struct {
	metastore.FileSession
	authority *delayedNativeOpen
}

func (s *delayedRetainSession) CreateAndRetainAt(ctx context.Context, request storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	close(s.authority.entered)
	<-s.authority.release
	return s.FileSession.CreateAndRetainAt(ctx, request, id)
}
func (s *delayedRetainSession) ResetAndRetainAt(ctx context.Context, request storage.ResetAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	close(s.authority.entered)
	<-s.authority.release
	return s.FileSession.ResetAndRetainAt(ctx, request, id)
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
			root := retainedRootFor(t, session)
			target := retainedTarget(t, root.File, "f")
			action := retainedAction(t, session)
			var expected storage.NodeMetadataRevision
			if !create {
				observation, err := session.StatNode(t.Context(), target.ExpectedNodeID, storage.ObservationOptions{})
				if err != nil {
					t.Fatal(err)
				}
				expected = observation.Attr.MetadataRevision
			}
			result := make(chan error, 1)
			go func() {
				var err error
				if create {
					_, err = session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}, action)
				} else {
					_, err = session.ResetAndRetainAt(t.Context(), storage.ResetAndRetainRequest{Target: target, ExpectedRevision: expected, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}, action)
				}
				result <- err
			}()
			select {
			case <-delayed.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("native open did not start")
			}
			await(t, "expired open session", func() bool {
				status, err := session.Status(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				return status.Retired && status.Remaining == 0
			})
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
	f := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
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

func TestRetainedExactLookupObservationAndClaimChanges(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := retainedSessionFor(t, volume)
	root := retainedRootFor(t, session).File
	missing, err := root.LookupAt(t.Context(), []byte("file"))
	if err != nil || missing.Found || missing.ParentID != root.NodeID() || missing.DirectoryRevision == 0 || string(missing.Name) != "file" || missing.EntryID != 0 || missing.Attr.ID != 0 {
		t.Fatalf("absent lookup = %+v, %v", missing, err)
	}
	file := openFileFor(t, session, "file", retainedOpenOptions{Create: true, Read: true, Write: true})
	lookup, err := root.LookupAt(t.Context(), []byte("file"))
	if err != nil || !lookup.Found || lookup.Attr.ID != file.NodeID() || lookup.EntryID == 0 || lookup.DirectoryRevision <= missing.DirectoryRevision {
		t.Fatalf("present lookup = %+v, %v", lookup, err)
	}
	before, err := file.File.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil || before.Location == nil {
		t.Fatalf("observation = %+v, %v", before, err)
	}
	condition := storage.ObservationCondition{MetadataRevision: before.Attr.MetadataRevision, DirectoryRevision: before.Attr.DirectoryRevision, Location: *before.Location}
	if observed, err := file.CheckObservation(t.Context(), condition); err != nil || !reflect.DeepEqual(observed, before) {
		t.Fatalf("unchanged observation = %+v, %v", observed, err)
	}
	metadata := storage.Metadata{{Key: "future.client", Version: 19, Data: []byte{0, 0xff}}}
	if _, err := file.SetAttr(t.Context(), storage.AttrChange{ExpectedRevision: before.Attr.MetadataRevision, Metadata: &metadata}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.CheckObservation(t.Context(), condition); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("stale observation = %v", err)
	}
	claimID := retainedAction(t, session)
	claim := storage.AccessClaim{Uses: storage.ReadContent}
	result, err := file.ReplaceClaim(t.Context(), claim, claimID)
	if err != nil || result.State != storage.FileActionCompleted || result.Effects != storage.EffectClaimChanged {
		t.Fatalf("read-only claim = %+v, %v", result, err)
	}
	if _, err := file.WriteAt(t.Context(), 0, []byte("refused")); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("write without claim = %v", err)
	}
	result, err = file.ReplaceClaim(t.Context(), storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}, retainedAction(t, session))
	if err != nil || result.State != storage.FileActionCompleted {
		t.Fatalf("restored claim = %+v, %v", result, err)
	}
	if replay, err := file.ReplaceClaim(t.Context(), claim, claimID); err != nil || replay.Effects != storage.EffectClaimChanged {
		t.Fatalf("claim replay = %+v, %v", replay, err)
	}
	if _, err := file.WriteAt(t.Context(), 0, []byte("current")); err != nil {
		t.Fatalf("historical claim replay changed current access: %v", err)
	}
	if err := volume.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	moved, err := root.LookupAt(t.Context(), []byte("moved"))
	if err != nil || moved.EntryID != lookup.EntryID || moved.Attr.ID != file.NodeID() || !reflect.DeepEqual(moved.Attr.Metadata, metadata) {
		t.Fatalf("renamed lookup = %+v, %v", moved, err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if file.NodeID() != lookup.Attr.ID {
		t.Fatal("retirement changed immutable identity")
	}
	if _, err := file.CheckObservation(t.Context(), condition); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed observation = %v", err)
	}
	if _, err := file.ReplaceClaim(t.Context(), claim, retainedAction(t, session)); !errors.Is(err, syscall.EBADF) || !storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("closed claim admission = %v", err)
	}
	if _, err := root.Close(t.Context(), retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := root.LookupAt(t.Context(), []byte("moved")); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed lookup = %v", err)
	}
}

func TestRetainedAtomicReplacementPreservesDetachedContentAndReceipt(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := retainedSessionFor(t, volume)
	old := openFileFor(t, session, "file", retainedOpenOptions{Create: true, Read: true, Write: true})
	if _, err := old.WriteAt(t.Context(), 0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	root := retainedRootFor(t, session).File
	request := storage.CreateAndRetainRequest{
		Target:  retainedTarget(t, root, "file"),
		Initial: storage.NodeInitial{Kind: storage.NodeRegular, Metadata: storage.Metadata{{Key: "app.replacement", Version: 7, Data: []byte("new")}}},
		Claim:   storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent},
	}
	action := retainedAction(t, session)
	result, err := session.ReplaceAndRetainAt(t.Context(), request, action)
	opened := retainedResult(t, session, result, err)
	if result.Effects&storage.EffectEntryDetached == 0 || opened.File.NodeID() == old.NodeID() || !reflect.DeepEqual(opened.Attr.Metadata, request.Initial.Metadata) {
		t.Fatalf("replacement = %+v", result)
	}
	fresh := &retainedFile{File: opened.File, session: session, closeID: retainedAction(t, session)}
	readFileFor(t, old, "old")
	readFileFor(t, fresh, "")
	if _, err := fresh.WriteAt(t.Context(), 0, []byte("new!")); err != nil {
		t.Fatal(err)
	}
	if used, err := volume.Usage(t.Context()); err != nil || used != 7 {
		t.Fatalf("retained replacement usage = %d, %v", used, err)
	}
	replay, err := session.ReplaceAndRetainAt(t.Context(), request, action)
	if err != nil || replay.HistoryRemaining <= 0 || replay.HistoryRemaining > result.HistoryRemaining {
		t.Fatalf("replacement replay lifetime = %+v, %v", replay, err)
	}
	replay.HistoryRemaining = result.HistoryRemaining
	if !reflect.DeepEqual(replay, result) {
		t.Fatalf("replacement replay changed historical result: %+v", replay)
	}
	readFileFor(t, fresh, "new!")
	if data, err := volume.Read(t.Context(), "file"); err != nil || string(data) != "new!" {
		t.Fatalf("replacement namespace = %q, %v", data, err)
	}
	if err := old.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if used, err := volume.Usage(t.Context()); err != nil || used != 4 {
		t.Fatalf("retired replacement usage = %d, %v", used, err)
	}
	closeID := retainedAction(t, session)
	lateID := retainedAction(t, session)
	if _, err := session.Close(t.Context(), closeID); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ReplaceAndRetainAt(t.Context(), request, lateID); !errors.Is(err, syscall.ESTALE) || !storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("retired replacement admission = %v", err)
	}
}
