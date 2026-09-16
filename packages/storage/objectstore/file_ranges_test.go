package objectstore_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

var retainedAdvisoryScope = storage.RangeScope{Domain: 21}

func retainedRanges(t *testing.T, file *retainedFile, owner storage.RangeOwnerID, ranges []storage.RangeAcquisition) storage.FileActionReceipt {
	t.Helper()
	snapshot, err := file.File.RangeSnapshot(t.Context(), owner, retainedAdvisoryScope)
	if err != nil {
		t.Fatal(err)
	}
	result, err := file.File.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: owner, Scope: retainedAdvisoryScope, ExpectedRevision: snapshot.Revision, Ranges: ranges}, retainedAction(t, file.session))
	if err != nil || result.State != storage.FileActionCompleted {
		t.Fatalf("replace ranges = %+v, %v", result, err)
	}
	return result
}

func retainedWait(t *testing.T, file *retainedFile, owner storage.RangeOwnerID, ranges []storage.RangeAcquisition) storage.FileActionID {
	t.Helper()
	snapshot, err := file.File.RangeSnapshot(t.Context(), owner, retainedAdvisoryScope)
	if err != nil {
		t.Fatal(err)
	}
	action := retainedAction(t, file.session)
	ctx, cancel := context.WithCancel(t.Context())
	type outcome struct {
		result storage.FileActionReceipt
		err    error
	}
	finished := make(chan outcome, 1)
	go func() {
		result, err := file.File.WaitRanges(ctx, storage.RangeWaitRequest{Owner: owner, Scope: retainedAdvisoryScope, ExpectedRevision: snapshot.Revision, Ranges: ranges, DetectDeadlock: true}, action)
		finished <- outcome{result, err}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case got := <-finished:
			if got.result.State == storage.FileActionCompleted {
				if got.err != nil {
					t.Errorf("completed wait failed: %v", got.err)
				}
			} else if got.result.State != storage.FileActionNotApplied || !errors.Is(got.err, syscall.EINTR) {
				t.Errorf("wait did not reconcile: %+v, %v", got.result, got.err)
			}
		case <-time.After(3 * time.Second):
			t.Error("range wait did not drain after cancellation")
		}
	})
	await(t, "range wait enrollment", func() bool {
		result, err := file.session.QueryAction(t.Context(), action)
		if errors.Is(err, syscall.ESTALE) {
			return false
		}
		if err != nil {
			t.Fatal(err)
		}
		if result.State != storage.FileActionPending {
			t.Fatalf("wait enrollment = %+v", result)
		}
		return true
	})
	return action
}

func retainedWaitCompleted(t *testing.T, session storage.FileSession, action storage.FileActionID) {
	t.Helper()
	await(t, "range revision notification", func() bool {
		result, err := session.QueryAction(t.Context(), action)
		if err != nil {
			t.Fatal(err)
		}
		return result.State == storage.FileActionCompleted
	})
}

func TestRetainedRangeOwnerCleanupProgressesWhileDataAdmissionIsFull(t *testing.T) {
	for _, cleanup := range []string{"retire owner", "retire session owner", "replace empty"} {
		t.Run(cleanup, func(t *testing.T) {
			objects := newPausedFilePut(t)
			volume, _ := fileVolume(t, objects, 4096, nil)
			options := storage.DefaultFileSessionOptions()
			options.MaxOperations = 1
			first := fileSessionFor(t, volume, options)
			second := fileSessionFor(t, volume, options)
			t.Cleanup(objects.release)
			writer := openFileFor(t, first, "f", retainedOpenOptions{Write: true, Create: true})
			closing := openFileFor(t, first, "f", retainedOpenOptions{Write: true})
			waiter := openFileFor(t, second, "f", retainedOpenOptions{Write: true})
			ranges := []storage.RangeAcquisition{{ID: 1, End: math.MaxInt64, Exclusive: true}}
			retainedRanges(t, closing, 41, ranges)
			pending := retainedWait(t, waiter, 72, ranges)
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
			if cleanup == "retire owner" {
				if _, err := closing.File.RetireRangeOwner(t.Context(), 41, retainedAdvisoryScope, retainedAction(t, first)); err != nil {
					t.Fatal(err)
				}
			} else if cleanup == "retire session owner" {
				result, err := first.RetireRangeOwner(t.Context(), 41, retainedAction(t, first))
				if err != nil || result.State != storage.FileActionCompleted || result.Effects&storage.EffectRangesChanged == 0 {
					t.Fatalf("session owner cleanup = %+v, %v", result, err)
				}
			} else {
				retainedRanges(t, closing, 41, nil)
			}
			if err := closing.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			retainedWaitCompleted(t, second, pending)
			retainedRanges(t, waiter, 72, ranges)
			if _, err := first.Renew(t.Context()); err != nil {
				t.Fatalf("cleanup consumed heartbeat admission: %v", err)
			}
			objects.release()
			if err := <-written; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRetainedPendingRangeCancellationProgressesWhileDataAdmissionIsFull(t *testing.T) {
	objects := newPausedFilePut(t)
	volume, _ := fileVolume(t, objects, 4096, nil)
	options := storage.DefaultFileSessionOptions()
	options.MaxOperations = 1
	first := fileSessionFor(t, volume, options)
	second := fileSessionFor(t, volume, options)
	t.Cleanup(objects.release)
	writer := openFileFor(t, first, "f", retainedOpenOptions{Write: true, Create: true})
	closing := openFileFor(t, first, "f", retainedOpenOptions{Write: true})
	holder := openFileFor(t, second, "f", retainedOpenOptions{Write: true})
	ranges := []storage.RangeAcquisition{{ID: 1, End: math.MaxInt64, Exclusive: true}}
	retainedRanges(t, holder, 72, ranges)
	pending := retainedWait(t, closing, 41, ranges)
	objects.pause.Store(true)
	written := make(chan error, 1)
	go func() { _, err := writer.WriteAt(t.Context(), 0, []byte("write")); written <- err }()
	select {
	case <-objects.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("write did not occupy data admission")
	}
	if result, err := first.QueryAction(t.Context(), pending); err != nil || result.State != storage.FileActionPending {
		t.Fatalf("pending query = %+v, %v", result, err)
	}
	result, err := first.CancelAction(t.Context(), pending)
	if !errors.Is(err, syscall.EINTR) || result.State != storage.FileActionNotApplied || result.Effects != 0 {
		t.Fatalf("cancel = %+v, %v", result, err)
	}
	retainedRanges(t, holder, 72, nil)
	if result, err := first.QueryAction(t.Context(), pending); !errors.Is(err, syscall.EINTR) || result.State != storage.FileActionNotApplied || result.Effects != 0 {
		t.Fatalf("cancelled action after revision change = %+v, %v", result, err)
	}
	if _, err := closing.File.RetireRangeOwner(t.Context(), 41, retainedAdvisoryScope, retainedAction(t, first)); err != nil {
		t.Fatal(err)
	}
	if err := closing.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := holder.File.RangeSnapshot(t.Context(), 72, retainedAdvisoryScope)
	if err != nil || len(snapshot.Own) != 0 || len(snapshot.Other) != 0 {
		t.Fatalf("cancel left ghost ranges = %+v, %v", snapshot, err)
	}
	if _, err := first.Renew(t.Context()); err != nil {
		t.Fatal(err)
	}
	objects.release()
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func TestRetainedRangeAdmissionPartitionsAreBoundedAndPreserveRenewal(t *testing.T) {
	t.Run("wait lifetime and capacity", retainedWaitLifetimeAndCapacity)
	for _, partition := range []string{"acquisition", "reconciliation"} {
		t.Run(partition, func(t *testing.T) {
			objects := memory.New()
			_, meta := fileVolume(t, objects, 4096, nil)
			paused := &pausedRangeAuthority{LockingStore: meta, entered: make(chan struct{}, 2)}
			volume := objectstore.New(objects, paused)
			t.Cleanup(func() {
				if err := volume.Close(); err != nil {
					t.Error(err)
				}
			})
			options := storage.DefaultFileSessionOptions()
			options.MaxOperations = 1
			session := fileSessionFor(t, volume, options)
			file := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
			granted := retainedRanges(t, file, 41, []storage.RangeAcquisition{{ID: 1, End: math.MaxInt64, Exclusive: true}})
			operation := func(ctx context.Context) error {
				if partition == "acquisition" {
					_, err := file.File.RangeSnapshot(ctx, 72, retainedAdvisoryScope)
					return err
				}
				_, err := session.QueryAction(ctx, granted.Action)
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
					t.Fatal("range controls did not occupy two slots")
				}
			}
			if err := operation(t.Context()); !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("%s exceeded its bound: %v", partition, err)
			}
			if _, err := session.Renew(t.Context()); err != nil {
				t.Fatalf("%s consumed heartbeat: %v", partition, err)
			}
			if _, err := file.Stat(t.Context()); err != nil {
				t.Fatalf("%s consumed data admission: %v", partition, err)
			}
			if partition == "acquisition" {
				if _, err := file.File.RetireRangeOwner(t.Context(), 41, retainedAdvisoryScope, retainedAction(t, session)); err != nil {
					t.Fatalf("acquisitions consumed cleanup: %v", err)
				}
			} else {
				if _, err := file.File.RangeSnapshot(t.Context(), 72, retainedAdvisoryScope); err != nil {
					t.Fatalf("reconciliation consumed acquisition: %v", err)
				}
			}
			cancel()
			for range 2 {
				if err := <-results; !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled control = %v", err)
				}
			}
			if _, err := file.File.RetireRangeOwner(t.Context(), 41, retainedAdvisoryScope, retainedAction(t, session)); err != nil {
				t.Fatalf("cancelled controls retained cleanup admission: %v", err)
			}
		})
	}
}

func retainedWaitLifetimeAndCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		volume, _ := fileVolume(t, memory.New(), 4096, func(options *sqlite.Options) { options.Files.FileOperationTimeout = 100 * time.Millisecond })
		options := storage.DefaultFileSessionOptions()
		options.MaxWaiters = 1
		holderSession := fileSessionFor(t, volume, options)
		waiterSession := fileSessionFor(t, volume, options)
		holder := openFileFor(t, holderSession, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
		waiter := openFileFor(t, waiterSession, "f", retainedOpenOptions{Read: true, Write: true})
		ranges := []storage.RangeAcquisition{{ID: 1, End: math.MaxInt64, Exclusive: true}}
		retainedRanges(t, holder, 1, ranges)
		snapshot, err := waiter.File.RangeSnapshot(t.Context(), 2, retainedAdvisoryScope)
		if err != nil {
			t.Fatal(err)
		}
		action := retainedAction(t, waiterSession)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		type outcome struct {
			result storage.FileActionReceipt
			err    error
		}
		done := make(chan outcome, 1)
		finished := make(chan struct{})
		t.Cleanup(func() { cancel(); <-finished })
		go func() {
			defer close(finished)
			result, err := waiter.File.WaitRanges(ctx, storage.RangeWaitRequest{Owner: 2, Scope: retainedAdvisoryScope, ExpectedRevision: snapshot.Revision, Ranges: ranges}, action)
			done <- outcome{result, err}
		}()
		synctest.Wait()
		if result, err := waiterSession.QueryAction(t.Context(), action); err != nil || result.State != storage.FileActionPending {
			t.Fatalf("wait enrollment = %+v, %v", result, err)
		}
		time.Sleep(200 * time.Millisecond)
		if _, err := waiterSession.Renew(t.Context()); err != nil {
			t.Fatal(err)
		}
		if result, err := waiterSession.QueryAction(t.Context(), action); err != nil || result.State != storage.FileActionPending {
			t.Fatalf("wait clipped by operation timeout = %+v, %v", result, err)
		}
		snapshot, err = waiter.File.RangeSnapshot(t.Context(), 3, retainedAdvisoryScope)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := waiter.File.WaitRanges(t.Context(), storage.RangeWaitRequest{Owner: 3, Scope: retainedAdvisoryScope, ExpectedRevision: snapshot.Revision, Ranges: ranges}, retainedAction(t, waiterSession)); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("waiter admission exceeds bound = %v", err)
		}
		if _, err := waiterSession.CancelAction(t.Context(), action); !errors.Is(err, syscall.EINTR) {
			t.Fatalf("cancel pending wait = %v", err)
		}
		stopped := <-done
		if !errors.Is(stopped.err, syscall.EINTR) || stopped.result.State != storage.FileActionNotApplied {
			t.Fatalf("original wait cancellation = %+v, %v", stopped.result, stopped.err)
		}
		next := retainedWait(t, waiter, 3, ranges)
		retainedRanges(t, holder, 1, nil)
		retainedWaitCompleted(t, waiterSession, next)
	})
}

func TestRetainedSessionExpiryFencesUploadBeforeRangeHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		objects := newPausedFilePut(t)
		volume, _ := fileVolume(t, objects, 4096, nil)
		options := storage.DefaultFileSessionOptions()
		options.Lease = 150 * time.Millisecond
		firstSession := fileSessionFor(t, volume, options)
		secondSession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
		t.Cleanup(objects.release)
		first := openFileFor(t, firstSession, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
		second := openFileFor(t, secondSession, "f", retainedOpenOptions{Read: true, Write: true})
		if _, err := first.WriteAt(t.Context(), 0, []byte("old")); err != nil {
			t.Fatal(err)
		}
		ranges := []storage.RangeAcquisition{{ID: 1, End: math.MaxInt64, Exclusive: true}}
		retainedRanges(t, first, 7, ranges)
		pending := retainedWait(t, second, 7, ranges)
		objects.pause.Store(true)
		writeDone := make(chan error, 1)
		go func() { _, err := first.WriteAt(t.Context(), 0, []byte("late")); writeDone <- err }()
		select {
		case <-objects.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("write did not stage")
		}
		await(t, "expiry range handoff", func() bool {
			result, err := secondSession.QueryAction(t.Context(), pending)
			if err != nil {
				t.Fatal(err)
			}
			return result.State == storage.FileActionCompleted
		})
		retainedRanges(t, second, 7, ranges)
		if _, err := second.WriteAt(t.Context(), 0, []byte("winner")); err != nil {
			t.Fatal(err)
		}
		objects.release()
		if err := <-writeDone; !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("retired upload = %v", err)
		}
		readFileFor(t, second, "winner")
		if _, err := firstSession.Renew(t.Context()); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("expired renewal = %v", err)
		}
	})
}

func TestRetainedSessionsShareRangeAuthorityAcrossObjectWrappers(t *testing.T) {
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
	first := openFileFor(t, firstSession, "f", retainedOpenOptions{Read: true, Create: true})
	second := openFileFor(t, secondSession, "f", retainedOpenOptions{Read: true, Write: true})
	retainedRanges(t, first, 11, []storage.RangeAcquisition{{ID: 1, End: math.MaxInt64, Exclusive: true}})
	snapshot, err := second.File.RangeSnapshot(t.Context(), 11, retainedAdvisoryScope)
	if err != nil || len(snapshot.Other) != 1 || snapshot.Other[0].Owner.ID != 11 {
		t.Fatalf("cross-wrapper ranges = %+v, %v", snapshot, err)
	}
	if _, err := second.WriteAt(t.Context(), 0, []byte("advisory")); err != nil {
		t.Fatalf("advisory blocked anonymous IO: %v", err)
	}
	if _, err := first.File.RetireRangeOwner(t.Context(), 11, retainedAdvisoryScope, retainedAction(t, firstSession)); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	snapshot, err = second.File.RangeSnapshot(t.Context(), 11, retainedAdvisoryScope)
	if err != nil || len(snapshot.Other) != 0 {
		t.Fatalf("retired owner = %+v, %v", snapshot, err)
	}
	readFileFor(t, second, "advisory")
}

func TestRetainedControlMethodsKeepIdentityAndKnownRangeOutcomes(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	before, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	after, err := session.Renew(t.Context())
	if err != nil || after.Epoch != before.Epoch || after.Revision != before.Revision+1 || after.Remaining <= 0 {
		t.Fatalf("renew = %+v, %v", after, err)
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
	current, err := session.StatNode(t.Context(), dir.ID, storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	metadata := storage.Metadata{{Key: "app.tag", Version: 1, Data: []byte("retained-directory")}}
	changed, err := session.SetNodeAttr(t.Context(), dir.ID, storage.AttrChange{ExpectedRevision: current.Attr.MetadataRevision, Metadata: &metadata}, retainedAction(t, session))
	if err != nil || changed.Observation.Attr.ID != dir.ID || !reflect.DeepEqual(changed.Observation.Attr.Metadata, metadata) {
		t.Fatalf("node metadata = %+v, %v", changed, err)
	}
	if got, err := session.StatNode(t.Context(), dir.ID, storage.ObservationOptions{}); err != nil || got.Attr.ID != dir.ID || !reflect.DeepEqual(got.Attr.Metadata, metadata) {
		t.Fatalf("node observation = %+v, %v", got, err)
	}
	first := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true, Create: true})
	second := openFileFor(t, session, "f", retainedOpenOptions{Read: true, Write: true})
	ranges := []storage.RangeAcquisition{{ID: 1, Start: 2, End: 8, Exclusive: true}}
	retainedRanges(t, first, 1, ranges)
	pending := retainedWait(t, second, 2, ranges)
	if result, err := session.CancelAction(t.Context(), pending); !errors.Is(err, syscall.EINTR) || result.State != storage.FileActionNotApplied || result.Effects != 0 {
		t.Fatalf("cancel = %+v, %v", result, err)
	}
	retainedRanges(t, first, 1, nil)
	if result, err := session.QueryAction(t.Context(), pending); !errors.Is(err, syscall.EINTR) || result.State != storage.FileActionNotApplied || result.Effects != 0 {
		t.Fatalf("cancelled after release = %+v, %v", result, err)
	}
	if snapshot, err := second.File.RangeSnapshot(t.Context(), 2, retainedAdvisoryScope); err != nil || len(snapshot.Own) != 0 || len(snapshot.Other) != 0 {
		t.Fatalf("cancelled owner retained ranges = %+v, %v", snapshot, err)
	}
	if _, err := first.WriteAt(t.Context(), 0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if used, err := volume.Usage(t.Context()); err != nil || used != 3 {
		t.Fatalf("usage = %d, %v", used, err)
	}
}

type pausedRangeAuthority struct {
	*sqlite.LockingStore
	remaining atomic.Int64
	entered   chan struct{}
}

func (m *pausedRangeAuthority) pause(ctx context.Context) error {
	for {
		remaining := m.remaining.Load()
		if remaining == 0 {
			return nil
		}
		if m.remaining.CompareAndSwap(remaining, remaining-1) {
			m.entered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
	}
}

func (m *pausedRangeAuthority) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (metastore.FileSession, storage.FileSessionStatus, error) {
	session, status, err := m.LockingStore.NewFileSession(ctx, options)
	if err != nil {
		return nil, status, err
	}
	return &pausedRangeSession{FileSession: session, authority: m}, status, nil
}

type pausedRangeSession struct {
	metastore.FileSession
	authority *pausedRangeAuthority
}

func (s *pausedRangeSession) Reference(ctx context.Context, id storage.FileReferenceID) (metastore.File, bool, error) {
	file, live, err := s.FileSession.Reference(ctx, id)
	if err != nil {
		return nil, false, err
	}
	return &pausedRangeFile{File: file, authority: s.authority}, live, nil
}
func (s *pausedRangeSession) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := s.authority.pause(ctx); err != nil {
		return storage.FileActionReceipt{}, err
	}
	return s.FileSession.QueryAction(ctx, id)
}

type pausedRangeFile struct {
	metastore.File
	authority *pausedRangeAuthority
}

func (f *pausedRangeFile) RangeSnapshot(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope) (storage.RangeSnapshot, error) {
	if err := f.authority.pause(ctx); err != nil {
		return storage.RangeSnapshot{}, err
	}
	return f.File.RangeSnapshot(ctx, owner, scope)
}
