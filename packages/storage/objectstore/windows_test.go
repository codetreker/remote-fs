package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func windowsAction(t *testing.T, session storage.WindowsSession) storage.WindowsActionID {
	t.Helper()
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func windowsSessionFor(t *testing.T, volume storage.WindowsStorage) storage.WindowsSession {
	t.Helper()
	return windowsSessionWithOptionsFor(t, volume, storage.DefaultFileSessionOptions())
}

func windowsSessionWithOptionsFor(t *testing.T, volume storage.WindowsStorage, options storage.FileSessionOptions) storage.WindowsSession {
	t.Helper()
	state, err := volume.WindowsState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !state.Enabled {
		id, err := storage.NewLockRequestID(state.ActionEpoch)
		if err != nil {
			t.Fatal(err)
		}
		result, err := volume.EnableWindows(t.Context(), id)
		if err != nil || !result.Enabled {
			t.Fatalf("activation = %+v, %v", result, err)
		}
	}
	session, err := volume.NewWindowsSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close Windows session: %v", err)
		}
	})
	return session
}

func windowsOpenFor(t *testing.T, session storage.WindowsSession, request storage.WindowsOpenRequest) storage.WindowsOpenResult {
	t.Helper()
	result, err := session.Open(t.Context(), request, windowsAction(t, session))
	if err != nil || result.File == nil {
		t.Fatalf("open = %+v, %v", result, err)
	}
	return result
}

func windowsRootFor(t *testing.T, session storage.WindowsSession) storage.WindowsOpenResult {
	t.Helper()
	return windowsOpenFor(t, session, storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{
		Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen, Kind: storage.WindowsDirectory}})
}

func windowsCreateFor(t *testing.T, session storage.WindowsSession, parent uint64, name string, kind storage.WindowsKind) storage.WindowsOpenResult {
	t.Helper()
	return windowsOpenFor(t, session, storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{
		Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpenIf, Kind: kind},
		Lookup: storage.WindowsLookup{ParentID: parent, Name: name}, Mode: 0o600})
}

func TestWindowsObjectReferencesSupportDirectoriesAndMetadataOnlyAccess(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := windowsSessionFor(t, volume)
	root := windowsRootFor(t, session)
	dir := windowsCreateFor(t, session, root.Attr.ID, "dir", storage.WindowsDirectory)
	child := windowsCreateFor(t, session, dir.Attr.ID, "child", storage.WindowsRegularFile)
	if root.Attr.NameInfo.State != storage.WindowsNameRoot || !dir.Attr.IsDir() {
		t.Fatalf("root/directory attributes: %+v, %+v", root.Attr, dir.Attr)
	}
	if _, err := dir.File.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("directory read = %v", err)
	}
	if _, err := dir.File.WriteAt(t.Context(), 0, []byte("x"), windowsAction(t, session)); !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("directory write = %v", err)
	}
	meta := windowsOpenFor(t, session, storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{
		Access: storage.WindowsReadAttributes | storage.WindowsSynchronize, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen},
		Lookup: storage.WindowsLookup{ParentID: dir.Attr.ID, Name: "child", ExpectedID: child.Attr.ID}})
	if attr, err := meta.File.Stat(t.Context()); err != nil || attr.ID != child.Attr.ID {
		t.Fatalf("metadata reference stat = %+v, %v", attr, err)
	}
	if _, err := meta.File.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("metadata reference read = %v", err)
	}
	if err := meta.File.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := volume.Rename(t.Context(), "dir", "renamed"); err != nil {
		t.Fatal(err)
	}
	listing, err := storage.NewWindowsListResult(256, 0, func(_ int, nameBytes int64, _ storage.WindowsBasicAttr) (int64, error) { return nameBytes + 64, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.File.ListBounded(t.Context(), listing); err != nil {
		t.Fatal(err)
	}
	entries, err := listing.Entries()
	if err != nil || len(entries) != 1 || entries[0].Name != "child" || entries[0].Attr.ID != child.Attr.ID {
		t.Fatalf("retained directory listing = %+v, %v", entries, err)
	}
	if attr, err := meta.File.Stat(t.Context()); err != nil || attr.NameInfo.Path != "renamed/child" {
		t.Fatalf("renamed child metadata = %+v, %v", attr, err)
	}
}

func TestWindowsObjectLocksCheckOrdinaryLogicalRanges(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Write(t.Context(), "file", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	session := windowsSessionFor(t, volume)
	root := windowsRootFor(t, session)
	opened := windowsCreateFor(t, session, root.Attr.ID, "file", storage.WindowsRegularFile)
	lock := storage.WindowsLockBatch{Ranges: []storage.WindowsLockRange{{Offset: 4, Length: 2, Type: storage.Exclusive, FailImmediately: true}}}
	if _, err := opened.File.LockBatch(t.Context(), lock, windowsAction(t, session)); err != nil {
		t.Fatal(err)
	}
	ordinarySession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	ordinary := openFileFor(t, ordinarySession, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if _, err := ordinary.Stat(t.Context()); err != nil {
		t.Fatalf("metadata stat under range lock: %v", err)
	}
	if read, err := ordinary.ReadAt(t.Context(), 0, 2); err != nil || string(read.Data) != "01" {
		t.Fatalf("disjoint read = %q, %v", read.Data, err)
	}
	if _, err := ordinary.ReadAt(t.Context(), 4, 1); storage.WindowsFailureOf(err) != storage.WindowsLockConflict {
		t.Fatalf("overlapping read = %v", err)
	}
	if _, err := volume.Read(t.Context(), "file"); storage.WindowsFailureOf(err) != storage.WindowsLockConflict {
		t.Fatalf("whole-file read = %v", err)
	}
	if _, err := ordinary.WriteAt(t.Context(), 0, []byte("ab")); err != nil {
		t.Fatalf("disjoint patch = %v", err)
	}
	if _, err := ordinary.WriteAt(t.Context(), 4, []byte("x")); storage.WindowsFailureOf(err) != storage.WindowsLockConflict {
		t.Fatalf("overlapping patch = %v", err)
	}
	if err := volume.Write(t.Context(), "file", []byte("replacement")); storage.WindowsFailureOf(err) != storage.WindowsLockConflict {
		t.Fatalf("whole replacement = %v", err)
	}
	if _, err := ordinary.Truncate(t.Context(), 8); err != nil {
		t.Fatalf("disjoint EOF change = %v", err)
	}
	if _, err := ordinary.Truncate(t.Context(), 5); storage.WindowsFailureOf(err) != storage.WindowsLockConflict {
		t.Fatalf("overlapping EOF change = %v", err)
	}
	if read, err := opened.File.ReadAt(t.Context(), 4, 2); err != nil || string(read.Data) != "45" {
		t.Fatalf("owner read = %q, %v", read.Data, err)
	}
	lock.Ranges[0].Type, lock.Ranges[0].FailImmediately = storage.Unlock, false
	if _, err := opened.File.LockBatch(t.Context(), lock, windowsAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if body, err := volume.Read(t.Context(), "file"); err != nil || string(body) != "ab234567" {
		t.Fatalf("final content = %q, %v", body, err)
	}
}

type windowsPutProbe struct {
	*memory.Objects
	puts             atomic.Int64
	gate             atomic.Bool
	entered, release chan struct{}
}

func (p *windowsPutProbe) Put(ctx context.Context, key string, body []byte) ([]byte, error) {
	p.puts.Add(1)
	digest, err := p.Objects.Put(ctx, key, body)
	if err != nil {
		return nil, err
	}
	if p.gate.CompareAndSwap(true, false) {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return digest, nil
}

func TestWindowsObjectContentReplayKeepsOneUploadAndOriginalAction(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "cancelled"}[cancelled], func(t *testing.T) {
			objects := &windowsPutProbe{Objects: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
			volume, _ := fileVolume(t, objects, 4096, nil)
			session := windowsSessionFor(t, volume)
			root := windowsRootFor(t, session)
			file := windowsCreateFor(t, session, root.Attr.ID, "file", storage.WindowsRegularFile).File
			id := windowsAction(t, session)
			objects.gate.Store(true)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			type outcome struct {
				result storage.WindowsActionResult
				err    error
			}
			result := make(chan outcome, 1)
			finished := make(chan struct{})
			var once sync.Once
			resume := func() { once.Do(func() { close(objects.release) }) }
			go func() {
				defer close(finished)
				r, err := file.WriteAt(ctx, 0, []byte("payload"), id)
				result <- outcome{r, err}
			}()
			t.Cleanup(func() { resume(); cancel(); <-finished })
			select {
			case <-objects.entered:
			case <-ctx.Done():
				t.Fatal("write did not reach the actual Put gate")
			}
			if r, err := file.WriteAt(ctx, 0, []byte("payload"), id); err != nil || r.State != storage.WindowsActionPending {
				t.Fatalf("pending replay = %+v, %v", r, err)
			}
			if r, err := session.QueryAction(ctx, id); err != nil || r.State != storage.WindowsActionPending {
				t.Fatalf("pending query = %+v, %v", r, err)
			}
			if _, err := file.WriteAt(ctx, 0, []byte("different"), id); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("changed replay fingerprint = %v", err)
			}
			if objects.puts.Load() != 1 {
				t.Fatalf("replay uploaded %d objects, want 1", objects.puts.Load())
			}
			wantState := storage.WindowsActionCompleted
			wantBody := []byte("payload")
			if cancelled {
				wantState, wantBody = storage.WindowsActionCancelled, nil
				if r, err := session.CancelAction(ctx, id); err != nil || r.State != wantState {
					t.Fatalf("cancel = %+v, %v", r, err)
				}
			}
			resume()
			got := <-result
			if got.err != nil || got.result.State != wantState || got.result.Action != id {
				t.Fatalf("original write = %+v, %v", got.result, got.err)
			}
			if r, err := file.WriteAt(ctx, 0, []byte("payload"), id); err != nil || r.State != wantState || r.Action != id {
				t.Fatalf("settled replay = %+v, %v", r, err)
			}
			if objects.puts.Load() != 1 {
				t.Fatalf("settled replay uploaded %d objects, want 1", objects.puts.Load())
			}
			if read, err := file.ReadAt(ctx, 0, 20); err != nil || !bytes.Equal(read.Data, wantBody) {
				t.Fatalf("content = %q, %v; want %q", read.Data, err, wantBody)
			}
		})
	}
}

type windowsReadFailure struct {
	*memory.Objects
	fail atomic.Bool
}

func (o *windowsReadFailure) GetBounded(ctx context.Context, key string, size int64) ([]byte, error) {
	if o.fail.Load() {
		return nil, syscall.EIO
	}
	return o.Objects.GetBounded(ctx, key, size)
}

func TestWindowsObjectZeroWriteAndTruncateDoNotReadDiscardedBytes(t *testing.T) {
	objects := &windowsReadFailure{Objects: memory.New()}
	volume, _ := fileVolume(t, objects, 4096, nil)
	session := windowsSessionFor(t, volume)
	root := windowsRootFor(t, session)
	file := windowsCreateFor(t, session, root.Attr.ID, "file", storage.WindowsRegularFile).File
	initial, err := file.WriteAt(t.Context(), 0, []byte("stored"), windowsAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	objects.fail.Store(true)
	empty, err := file.WriteAt(t.Context(), 0, nil, windowsAction(t, session))
	if err != nil || empty.State != storage.WindowsActionCompleted || empty.Attr != initial.Attr {
		t.Fatalf("zero write = %+v, %v; want unchanged %+v", empty, err, initial.Attr)
	}
	truncated, err := file.Truncate(t.Context(), 0, windowsAction(t, session))
	if err != nil || truncated.State != storage.WindowsActionCompleted || truncated.Attr.Size != 0 {
		t.Fatalf("empty truncate = %+v, %v", truncated, err)
	}
}

func TestWindowsObjectRevisionRetryPreservesUntouchedCurrentBytes(t *testing.T) {
	objects := &windowsPutProbe{Objects: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
	volume, _ := fileVolume(t, objects, 4096, nil)
	session := windowsSessionFor(t, volume)
	root := windowsRootFor(t, session)
	file := windowsCreateFor(t, session, root.Attr.ID, "file", storage.WindowsRegularFile).File
	if _, err := file.WriteAt(t.Context(), 0, []byte("abcdef"), windowsAction(t, session)); err != nil {
		t.Fatal(err)
	}
	id := windowsAction(t, session)
	objects.gate.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	finished := make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(objects.release) }) }
	go func() {
		defer close(finished)
		_, err := file.WriteAt(ctx, 0, []byte("X"), id)
		result <- err
	}()
	t.Cleanup(func() { resume(); cancel(); <-finished })
	select {
	case <-objects.entered:
	case <-ctx.Done():
		t.Fatal("patch did not reach the actual Put gate")
	}
	if err := volume.Write(ctx, "file", []byte("abcdeY")); err != nil {
		t.Fatal(err)
	}
	resume()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if read, err := file.ReadAt(ctx, 0, 6); err != nil || string(read.Data) != "XbcdeY" {
		t.Fatalf("CAS retry content = %q, %v", read.Data, err)
	}
	before := objects.puts.Load()
	if r, err := file.WriteAt(ctx, 0, []byte("X"), id); err != nil || r.State != storage.WindowsActionCompleted {
		t.Fatalf("CAS receipt replay = %+v, %v", r, err)
	}
	if after := objects.puts.Load(); after != before {
		t.Fatalf("CAS receipt replay uploaded again: %d -> %d", before, after)
	}
}

type windowsFailedPut struct {
	*memory.Objects
	puts    atomic.Int64
	failure error
}

func (o *windowsFailedPut) Put(ctx context.Context, key string, body []byte) ([]byte, error) {
	o.puts.Add(1)
	if _, err := o.Objects.Put(ctx, key, body); err != nil {
		return nil, err
	}
	return nil, o.failure
}

func TestWindowsObjectFailedPutRetainsOneRejectedActionAndQuarantinedObject(t *testing.T) {
	failure := errors.New("lost object Put confirmation")
	objects := &windowsFailedPut{Objects: memory.New(), failure: failure}
	volume, meta := fileVolume(t, objects, 4096, nil)
	session := windowsSessionFor(t, volume)
	root := windowsRootFor(t, session)
	file := windowsCreateFor(t, session, root.Attr.ID, "file", storage.WindowsRegularFile).File
	id := windowsAction(t, session)
	for range 2 {
		result, err := file.WriteAt(t.Context(), 0, []byte("uncertain"), id)
		if !errors.Is(err, failure) || result.State != storage.WindowsActionRejected || result.Action != id {
			t.Fatalf("failed Put receipt = %+v, %v", result, err)
		}
	}
	if objects.puts.Load() != 1 {
		t.Fatalf("failed Put replay uploaded %d objects", objects.puts.Load())
	}
	if read, err := file.ReadAt(t.Context(), 0, 20); err != nil || len(read.Data) != 0 {
		t.Fatalf("failed Put published bytes = %q, %v", read.Data, err)
	}
	status, err := meta.ObjectStatus(t.Context())
	if err != nil || status.UnresolvedCount != 1 {
		t.Fatalf("quarantine status = %+v, %v", status, err)
	}
}

func TestWindowsObjectSessionRequiresExplicitActivation(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.CheckWindowsStorage(); err != nil {
		t.Fatal(err)
	}
	state, err := volume.WindowsState(t.Context())
	if err != nil || state.Enabled {
		t.Fatalf("initial Windows state = %+v, %v", state, err)
	}
	if _, err := volume.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("session before activation = %v", err)
	}
}

func TestWindowsObjectActionsRetainIdentityThroughMetadataLinksAndClose(t *testing.T) {
	volume, meta := fileVolume(t, memory.New(), 4096, nil)
	state, err := volume.WindowsState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	activation, err := storage.NewLockRequestID(state.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := volume.EnableWindows(t.Context(), activation); err != nil {
		t.Fatal(err)
	}
	if result, err := volume.QueryWindowsActivation(t.Context(), activation); err != nil || !result.Enabled || result.Action != activation {
		t.Fatalf("activation receipt = %+v, %v", result, err)
	}
	session := windowsSessionFor(t, volume)
	root := windowsRootFor(t, session)
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpenIf},
		Lookup: storage.WindowsLookup{ParentID: root.Attr.ID, Name: "file"}, Mode: 0o600}
	openID := windowsAction(t, session)
	opened, err := session.Open(t.Context(), request, openID)
	if err != nil {
		t.Fatal(err)
	}
	file := opened.File
	if file.Reference() == "" {
		t.Fatal("open returned an empty reference identity")
	}
	replayed, err := session.QueryAction(t.Context(), openID)
	if err != nil || replayed.File != file || replayed.File.Reference() != file.Reference() || replayed.Attr.ID != opened.Attr.ID {
		t.Fatalf("open replay = %+v, %v", replayed, err)
	}
	dos := uint32(storage.WindowsDOSHidden)
	if result, err := file.SetAttr(t.Context(), storage.WindowsAttrChange{DOSAttributes: &dos}, windowsAction(t, session)); err != nil || result.Attr.DOSAttributes != dos {
		t.Fatalf("attribute mutation = %+v, %v", result, err)
	}
	renamed, err := file.Rename(t.Context(), storage.WindowsRenameRequest{Source: storage.WindowsLookup{ParentID: root.Attr.ID, Name: "file", ExpectedID: opened.Attr.ID},
		Destination: storage.WindowsLookup{ParentID: root.Attr.ID, Name: "moved"}}, windowsAction(t, session))
	if err != nil || renamed.Attr.ID != opened.Attr.ID || renamed.Attr.NameInfo.Path != "moved" {
		t.Fatalf("identity rename = %+v, %v", renamed, err)
	}
	if _, err := file.SetDeletePending(t.Context(), true, windowsAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.SetDeletePending(t.Context(), false, windowsAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadLink(t.Context()); storage.WindowsFailureOf(err) != storage.WindowsNotReparsePoint {
		t.Fatalf("regular ReadLink = %v", err)
	}
	link, err := file.SetLink(t.Context(), "target", windowsAction(t, session))
	if err != nil || link.Attr.ID != opened.Attr.ID {
		t.Fatalf("link conversion = %+v, %v", link, err)
	}
	if info, err := file.ReadLink(t.Context()); err != nil || info.Target != "target" || info.Location.Path != "moved" || info.Location.State != storage.WindowsNameLinked || info.Unparsed != "" {
		t.Fatalf("atomic link observation = %+v, %v", info, err)
	}
	if _, err := volume.Read(t.Context(), "moved"); !errors.Is(err, syscall.ELOOP) {
		t.Fatalf("ordinary symlink read = %v", err)
	}
	if _, err := file.SetDeletePending(t.Context(), true, windowsAction(t, session)); err != nil {
		t.Fatal(err)
	}
	closeID := windowsAction(t, session)
	closed, err := file.Close(t.Context(), closeID)
	if err != nil || closed.State != storage.WindowsActionCompleted {
		t.Fatalf("close = %+v, %v", closed, err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed reference stat = %v", err)
	}
	if result, err := session.QueryAction(t.Context(), closeID); err != nil || result.State != storage.WindowsActionCompleted {
		t.Fatalf("close receipt = %+v, %v", result, err)
	}
	if _, err := volume.Stat(t.Context(), "moved"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("delete-on-close left name: %v", err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("final usage = %d, %v", used, err)
	}
}

func TestWindowsObjectControlAdmissionRemainsAvailableDuringStaging(t *testing.T) {
	objects := &windowsPutProbe{Objects: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
	volume, _ := fileVolume(t, objects, 4096, nil)
	options := storage.DefaultFileSessionOptions()
	options.MaxOperations = 1
	session := windowsSessionWithOptionsFor(t, volume, options)
	root := windowsRootFor(t, session)
	file := windowsCreateFor(t, session, root.Attr.ID, "file", storage.WindowsRegularFile).File
	id := windowsAction(t, session)
	objects.gate.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan struct{})
	result := make(chan error, 1)
	var once sync.Once
	resume := func() { once.Do(func() { close(objects.release) }) }
	go func() {
		defer close(finished)
		_, err := file.WriteAt(ctx, 0, []byte("payload"), id)
		result <- err
	}()
	t.Cleanup(func() { resume(); cancel(); <-finished })
	select {
	case <-objects.entered:
	case <-ctx.Done():
		t.Fatal("write did not reach Put gate")
	}
	if _, err := file.Stat(ctx); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("saturated data admission = %v", err)
	}
	before, err := session.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	after, err := session.Renew(ctx)
	if err != nil || after.Epoch != before.Epoch || after.Revision <= before.Revision || after.Remaining <= 0 {
		t.Fatalf("renew during staging = %+v, %v", after, err)
	}
	if pending, err := session.QueryAction(ctx, id); err != nil || pending.State != storage.WindowsActionPending {
		t.Fatalf("query during staging = %+v, %v", pending, err)
	}
	resume()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(ctx); err != nil {
		t.Fatalf("released data admission = %v", err)
	}
}

func TestWindowsObjectExpirySettlesDetachedLifetimeAccounting(t *testing.T) {
	volume, meta := fileVolume(t, memory.New(), 4096, nil)
	windowsSessionFor(t, volume)
	cleaned := make(chan struct{})
	var once sync.Once
	creation, cancel := context.WithCancel(storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error {
			if previous == 4 && next == 0 && result == storage.PublicationApplied {
				once.Do(func() { close(cleaned) })
			}
			return nil
		}, nil
	}))
	defer cancel()
	options := storage.DefaultFileSessionOptions()
	options.Lease = time.Second
	session, err := volume.NewWindowsSession(creation, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close expiring session: %v", err)
		}
	})
	root := windowsRootFor(t, session)
	file := windowsCreateFor(t, session, root.Attr.ID, "file", storage.WindowsRegularFile).File
	if _, err := file.WriteAt(t.Context(), 0, []byte("kept"), windowsAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if err := volume.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if read, err := file.ReadAt(t.Context(), 0, 4); err != nil || string(read.Data) != "kept" {
		t.Fatalf("detached read = %q, %v", read.Data, err)
	}
	cancel()
	select {
	case <-cleaned:
	case <-time.After(5 * time.Second):
		t.Fatal("expiry did not settle detached cleanup")
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired reference = %v", err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("expired usage = %d, %v", used, err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatalf("repeated close = %v", err)
	}
}

func TestWindowsObjectAlreadyElapsedLeaseCanClose(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	windowsSessionFor(t, volume)
	options := storage.DefaultFileSessionOptions()
	options.Lease = time.Nanosecond
	session, err := volume.NewWindowsSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Status(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("already elapsed lease = %v", err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsObjectDeletePendingRefusesNamedIOAndPreservesExistingReferences(t *testing.T) {
	volume, meta := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Write(t.Context(), "file", []byte("initial")); err != nil {
		t.Fatal(err)
	}
	session := windowsSessionFor(t, volume)
	root := windowsRootFor(t, session)
	opened := windowsCreateFor(t, session, root.Attr.ID, "file", storage.WindowsRegularFile)
	ordinarySession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	ordinary := openFileFor(t, ordinarySession, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if _, err := opened.File.SetDeletePending(t.Context(), true, windowsAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if body, err := volume.Read(t.Context(), "file"); body != nil || storage.WindowsFailureOf(err) != storage.WindowsDeletePending {
		t.Fatalf("named read under delete-pending = %q, %v", body, err)
	}
	if err := volume.Write(t.Context(), "file", []byte("wrong replacement")); storage.WindowsFailureOf(err) != storage.WindowsDeletePending {
		t.Fatalf("named write under delete-pending = %v", err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 7 {
		t.Fatalf("rejected named write changed usage = %d, %v", used, err)
	}
	for name, read := range map[string]func(context.Context, int64, int) (storage.FileRead, error){
		"Windows": opened.File.ReadAt, "ordinary": ordinary.ReadAt,
	} {
		if got, err := read(t.Context(), 0, 20); err != nil || string(got.Data) != "initial" || got.Attr.ID != opened.Attr.ID {
			t.Fatalf("%s retained read = %q, ID %d, %v", name, got.Data, got.Attr.ID, err)
		}
	}
	if _, err := opened.File.WriteAt(t.Context(), 0, []byte("W"), windowsAction(t, session)); err != nil {
		t.Fatalf("Windows retained write: %v", err)
	}
	if _, err := ordinary.WriteAt(t.Context(), 1, []byte("O")); err != nil {
		t.Fatalf("ordinary retained write: %v", err)
	}
	for name, read := range map[string]func(context.Context, int64, int) (storage.FileRead, error){
		"Windows": opened.File.ReadAt, "ordinary": ordinary.ReadAt,
	} {
		if got, err := read(t.Context(), 0, 20); err != nil || string(got.Data) != "WOitial" || got.Attr.ID != opened.Attr.ID {
			t.Fatalf("%s retained current bytes = %q, ID %d, %v", name, got.Data, got.Attr.ID, err)
		}
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 7 {
		t.Fatalf("retained writes changed usage = %d, %v", used, err)
	}
	if err := ordinary.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := opened.File.Close(t.Context(), windowsAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := volume.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("last close left the pending name: %v", err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("last close retained usage = %d, %v", used, err)
	}
}
