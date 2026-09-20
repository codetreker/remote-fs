package fuse

import (
	"context"
	"errors"
	iofs "io/fs"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/fuse/posix"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

type nodeReferenceFixture struct {
	attr                   storage.Attr
	scope                  storage.UseScope
	link                   []byte
	closes, stats, updates int
	clean                  bool
	closeErr               error
	checkErr, scopeErr     error
}

func (r *nodeReferenceFixture) Stat(context.Context) (storage.Attr, error) {
	r.stats++
	if r.closes != 0 {
		return storage.Attr{}, syscall.EBADF
	}
	return r.attr, nil
}
func (r *nodeReferenceFixture) SetAttr(_ context.Context, change storage.AttrChange) (storage.Attr, error) {
	if change.ModTime != nil {
		r.attr.ModTime = *change.ModTime
	}
	r.updates++
	return r.attr, nil
}
func (r *nodeReferenceFixture) Close(ctx context.Context) error {
	r.closes++
	_, deadline := ctx.Deadline()
	r.clean = ctx.Err() == nil && deadline
	return r.closeErr
}
func (r *nodeReferenceFixture) CheckScopedReference() error { return r.checkErr }
func (r *nodeReferenceFixture) Scope(context.Context) (storage.UseScope, error) {
	return r.scope, r.scopeErr
}
func (r *nodeReferenceFixture) CheckReferenceState() error { return nil }
func (r *nodeReferenceFixture) State(context.Context) (storage.ReferenceState, error) {
	return storage.ReferenceState{Attr: r.attr, LinkTarget: append([]byte(nil), r.link...)}, nil
}

type directorySessionFixture struct {
	*namespaceFixture
	reference      *nodeReferenceFixture
	openOptions    storage.NodeRefOptions
	openID         uint64
	opens, updates int
	queries        int
	checkErr       error
	open           func(context.Context, uint64, storage.NodeRefOptions) (storage.NodeOpenResult, error)
	receipt        storage.FileActionReceipt
	queryErr       error
}

func (s *directorySessionFixture) CheckNodeReferences() error { return s.checkErr }
func (s *directorySessionFixture) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.opens++
	s.openID = id
	s.openOptions = options
	if s.open != nil {
		return s.open(ctx, id, options)
	}
	return storage.NodeOpenResult{Reference: s.reference, Attr: s.reference.attr, Outcome: storage.Opened}, nil
}
func (s *directorySessionFixture) OpenChildRef(context.Context, storage.ChildName, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	panic("known inode was reopened through a parent name")
}
func (s *directorySessionFixture) CheckMetadataAccess() error { return nil }
func (s *directorySessionFixture) SetMetadata(_ context.Context, id uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	if id != s.reference.attr.ID || namespace != posix.Namespace {
		return storage.OpaquePayload{}, syscall.EINVAL
	}
	s.updates++
	payload := storage.OpaquePayload{Version: []byte{byte(s.updates)}, Data: append([]byte(nil), data...)}
	if s.reference.attr.Metadata == nil {
		s.reference.attr.Metadata = make(map[string]storage.OpaquePayload)
	}
	s.reference.attr.Metadata[namespace] = payload
	return payload, nil
}
func (s *directorySessionFixture) SetNodeAttr(_ context.Context, id uint64, change storage.AttrChange) (storage.Attr, error) {
	if id != s.reference.attr.ID {
		return storage.Attr{}, syscall.ESTALE
	}
	return s.reference.SetAttr(context.Background(), change)
}
func (s *directorySessionFixture) CheckFileActions() error { return nil }
func (s *directorySessionFixture) QueryFileAction(_ context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	s.queries++
	receipt := s.receipt
	receipt.Action = action
	return receipt, s.queryErr
}
func (s *directorySessionFixture) QueryDeleteIntent(context.Context, storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	panic("node reference open queried a delete intent")
}
func (s *directorySessionFixture) AcknowledgeDeleteIntent(context.Context, storage.AcknowledgeDeleteIntentCommand) error {
	panic("node reference open acknowledged a delete intent")
}

func TestReadOnlyDirectoryHandleRetainsIdentityWithoutRequestingWriteAccess(t *testing.T) {
	namespace := &namespaceFixture{}
	reference := &nodeReferenceFixture{
		attr:  storage.Attr{ID: 7, Kind: storage.NodeDirectory},
		scope: storage.UseScope{Token: "opened-directory"},
	}
	session := &directorySessionFixture{namespaceFixture: namespace, reference: reference}
	root := namespaceRoot(session)
	namespace.lookup = func(name storage.ChildName) (storage.Attr, error) {
		if name.Parent.NodeID != 7 || name.Parent.Scope == nil || *name.Parent.Scope != reference.scope {
			t.Fatalf("scoped lookup = %+v", name)
		}
		return storage.Attr{ID: 8, Kind: storage.NodeRegular}, nil
	}

	opened, _, errno := root.OpendirHandle(t.Context(), syscall.O_RDONLY|syscall.O_DIRECTORY)
	if errno != 0 {
		t.Fatal(errno)
	}
	if session.openID != 7 || session.openOptions.Use != (storage.UseClaim{Uses: storage.ReadEntries}) || session.openOptions.MetadataAccess != storage.ReadMetadata {
		t.Fatalf("read-only directory options = %+v", session.openOptions)
	}
	if epoch, err := session.openOptions.Action.Epoch(); err != nil || epoch != 7 {
		t.Fatalf("directory action = %q epoch=%d err=%v", session.openOptions.Action, epoch, err)
	}
	handle := opened.(*directoryHandle)
	if handle.scope != reference.scope {
		t.Fatalf("retained scope = %+v", handle.scope)
	}
	if _, errno := handle.Lookup(t.Context(), "child", &gofuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}
	var out gofuse.AttrOut
	if errno := root.Getattr(t.Context(), handle, &out); errno != 0 {
		t.Fatal(errno)
	}
	if errno := root.Setattr(t.Context(), handle, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{Valid: gofuse.FATTR_MODE, Mode: 0700}}, &out); errno != 0 {
		t.Fatal(errno)
	}
	mode, err := permissions(reference.attr)
	if err != nil || mode.Perm() != iofs.FileMode(0700) || session.updates != 1 {
		t.Fatalf("directory mode=%v updates=%d err=%v", mode, session.updates, err)
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	handle.Releasedir(canceled, 0)
	handle.Releasedir(canceled, 0)
	if reference.closes != 1 || !reference.clean || session.opens != 1 {
		t.Fatalf("closes=%d clean=%v opens=%d", reference.closes, reference.clean, session.opens)
	}
}

func TestInterruptedNodeReferenceOpenReplaysTheSameCompletedAction(t *testing.T) {
	reference := &nodeReferenceFixture{
		attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory},
	}
	session := &directorySessionFixture{
		namespaceFixture: &namespaceFixture{},
		reference:        reference,
		receipt: storage.FileActionReceipt{
			Operation: storage.OpFileOpenNodeRef,
			Outcome:   storage.FileActionCompleted,
		},
	}
	root := namespaceRoot(session)
	action, err := storage.NewFileActionID(7)
	if err != nil {
		t.Fatal(err)
	}
	var first storage.FileActionID
	session.open = func(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
		if session.opens == 1 {
			first = options.Action
			return storage.NodeOpenResult{}, context.Canceled
		}
		if id != 7 || options.Action != first || options.Action != action {
			t.Fatalf("replayed open id=%d action=%q first=%q", id, options.Action, first)
		}
		if _, bounded := ctx.Deadline(); !bounded || ctx.Err() != nil {
			t.Fatalf("replay context deadline=%v err=%v", bounded, ctx.Err())
		}
		return storage.NodeOpenResult{Reference: reference, Attr: reference.attr, Outcome: storage.Opened}, nil
	}

	result, err := root.openNodeReference(t.Context(), 7, storage.NodeRefOptions{Action: action})
	if err != nil || result.Reference != reference || session.opens != 2 || session.queries != 1 {
		t.Fatalf("reconciled open result=%+v err=%v opens=%d queries=%d", result, err, session.opens, session.queries)
	}
}

func TestNodeReferenceOpenRejectsUnavailableAndInvalidCapabilities(t *testing.T) {
	action, err := storage.NewFileActionID(7)
	if err != nil {
		t.Fatal(err)
	}
	options := storage.NodeRefOptions{Action: action}

	if _, err := namespaceRoot(&namespaceFixture{}).openNodeReference(t.Context(), 7, options); errnoOf(err) != syscall.EOPNOTSUPP {
		t.Fatalf("missing node-reference capability = %v", err)
	}

	preflight := errors.New("node-reference preflight failed")
	session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}, checkErr: preflight}
	if _, err := namespaceRoot(session).openNodeReference(t.Context(), 7, options); err != preflight || session.opens != 0 {
		t.Fatalf("failed preflight result=%v opens=%d", err, session.opens)
	}

	session.checkErr = nil
	session.open = func(context.Context, uint64, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
		return storage.NodeOpenResult{}, syscall.ENOENT
	}
	if _, err := namespaceRoot(session).openNodeReference(t.Context(), 7, options); err != syscall.ENOENT || session.queries != 0 {
		t.Fatalf("definite open failure result=%v queries=%d", err, session.queries)
	}
}

func TestFileActionReconciliationClassifiesAuthorityReceipts(t *testing.T) {
	action, err := storage.NewFileActionID(7)
	if err != nil {
		t.Fatal(err)
	}
	cause := context.Canceled
	for _, test := range []struct {
		name       string
		files      storage.FileSession
		wantReplay bool
		wantErrno  syscall.Errno
	}{
		{name: "capability unavailable", files: &namespaceFixture{}, wantErrno: syscall.EIO},
		{name: "query failed", files: &directorySessionFixture{namespaceFixture: &namespaceFixture{}, queryErr: syscall.ESTALE}, wantErrno: syscall.EIO},
		{name: "not executed", files: &directorySessionFixture{namespaceFixture: &namespaceFixture{}, receipt: storage.FileActionReceipt{Outcome: storage.FileActionNotExecuted}}, wantErrno: syscall.EINTR},
		{name: "pending", files: &directorySessionFixture{namespaceFixture: &namespaceFixture{}, receipt: storage.FileActionReceipt{Operation: storage.OpFileOpenNodeRef, Outcome: storage.FileActionPending}}, wantReplay: true},
		{name: "completed", files: &directorySessionFixture{namespaceFixture: &namespaceFixture{}, receipt: storage.FileActionReceipt{Operation: storage.OpFileOpenNodeRef, Outcome: storage.FileActionCompleted}}, wantReplay: true},
		{name: "wrong operation", files: &directorySessionFixture{namespaceFixture: &namespaceFixture{}, receipt: storage.FileActionReceipt{Operation: storage.OpFileOpenAt, Outcome: storage.FileActionCompleted}}, wantErrno: syscall.EIO},
		{name: "unknown", files: &directorySessionFixture{namespaceFixture: &namespaceFixture{}, receipt: storage.FileActionReceipt{Outcome: storage.FileActionUnknown}}, wantErrno: syscall.EIO},
		{name: "retired", files: &directorySessionFixture{namespaceFixture: &namespaceFixture{}, receipt: storage.FileActionReceipt{Outcome: storage.FileActionRetired}}, wantErrno: syscall.EIO},
		{name: "invalid receipt", files: &directorySessionFixture{namespaceFixture: &namespaceFixture{}, receipt: storage.FileActionReceipt{}}, wantErrno: syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			v := activeTestVolume(nil, 1024)
			v.files = test.files
			replay, err := v.reconcileFileAction(t.Context(), action, storage.OpFileOpenNodeRef, cause)
			if replay != test.wantReplay || errnoOf(err) != test.wantErrno {
				t.Fatalf("replay=%v err=%v", replay, err)
			}
			if err != nil && !errors.Is(err, cause) {
				t.Fatalf("reconciliation lost original failure: %v", err)
			}
		})
	}
}

func TestDirectoryOpenRejectsInvalidReferencesAndCleansReturnedOwnership(t *testing.T) {
	if handle, _, errno := namespaceRoot(&directorySessionFixture{
		namespaceFixture: &namespaceFixture{},
		reference:        &nodeReferenceFixture{attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}},
	}).OpendirHandle(t.Context(), syscall.O_WRONLY); errno != syscall.EISDIR || handle != nil {
		t.Fatalf("writable directory open = %v, %v", handle, errno)
	}

	for _, test := range []struct {
		name      string
		reference *nodeReferenceFixture
		outcome   storage.OpenOutcome
		openErr   error
		wantErrno syscall.Errno
	}{
		{
			name:      "reference returned with interrupted result",
			reference: &nodeReferenceFixture{attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "scope"}},
			outcome:   storage.Opened,
			openErr:   context.Canceled,
			wantErrno: syscall.EINTR,
		},
		{
			name:      "invalid attributes",
			reference: &nodeReferenceFixture{attr: storage.Attr{Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "scope"}},
			outcome:   storage.Opened,
		},
		{
			name: "invalid scoped reference",
			reference: &nodeReferenceFixture{attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "scope"},
				checkErr: storage.ErrInvalidScope},
			outcome: storage.Opened,
		},
		{
			name: "scope query failure",
			reference: &nodeReferenceFixture{attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "scope"},
				scopeErr: syscall.ESTALE},
			outcome: storage.Opened,
		},
		{
			name:      "malformed scope",
			reference: &nodeReferenceFixture{attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}},
			outcome:   storage.Opened,
		},
		{
			name:      "impossible outcome",
			reference: &nodeReferenceFixture{attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "scope"}},
			outcome:   storage.Created,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}, reference: test.reference}
			session.open = func(context.Context, uint64, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
				return storage.NodeOpenResult{Reference: test.reference, Attr: test.reference.attr, Outcome: test.outcome}, test.openErr
			}
			handle, _, errno := namespaceRoot(session).OpendirHandle(t.Context(), syscall.O_RDONLY)
			if handle != nil || errno == 0 || test.reference.closes != 1 || !test.reference.clean {
				t.Fatalf("invalid open handle=%v errno=%v closes=%d clean=%v", handle, errno, test.reference.closes, test.reference.clean)
			}
			if test.wantErrno != 0 && errno != test.wantErrno {
				t.Fatalf("open errno=%v, want %v", errno, test.wantErrno)
			}
		})
	}

	session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}}
	session.open = func(context.Context, uint64, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
		return storage.NodeOpenResult{Outcome: storage.Opened}, nil
	}
	if handle, _, errno := namespaceRoot(session).OpendirHandle(t.Context(), syscall.O_RDONLY); handle != nil || errno != syscall.EIO {
		t.Fatalf("missing returned reference handle=%v errno=%v", handle, errno)
	}
}

func TestDirectoryHandleAttributeMutationRevalidatesReferenceScope(t *testing.T) {
	reference := &nodeReferenceFixture{
		attr:  storage.Attr{ID: 7, Kind: storage.NodeDirectory},
		scope: storage.UseScope{Token: "directory"},
	}
	session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}, reference: reference}
	root := namespaceRoot(session)
	opened, _, errno := root.OpendirHandle(t.Context(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatal(errno)
	}
	handle := opened.(*directoryHandle)
	t.Cleanup(func() { handle.Releasedir(context.Background(), 0) })

	want := time.Unix(123456789, 0)
	in := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{Valid: gofuse.FATTR_MTIME, Mtime: uint64(want.Unix())}}
	if errno := root.Setattr(t.Context(), handle, in, &gofuse.AttrOut{}); errno != 0 || !reference.attr.ModTime.Equal(want) || reference.updates != 1 {
		t.Fatalf("directory setattr errno=%v modtime=%v updates=%d", errno, reference.attr.ModTime, reference.updates)
	}

	reference.scope = storage.UseScope{Token: "replacement"}
	if errno := root.Setattr(t.Context(), handle, in, &gofuse.AttrOut{}); errno != syscall.ESTALE || reference.updates != 1 {
		t.Fatalf("replaced directory setattr errno=%v updates=%d", errno, reference.updates)
	}
}

type fixedDirectoryStream struct {
	entries []gofuse.DirEntry
	index   int
	closes  int
}

func (s *fixedDirectoryStream) HasNext() bool { return s.index < len(s.entries) }
func (s *fixedDirectoryStream) Next() (gofuse.DirEntry, syscall.Errno) {
	entry := s.entries[s.index]
	s.index++
	return entry, 0
}
func (s *fixedDirectoryStream) Close() { s.closes++ }

type seekableDirectoryStream struct {
	*fixedDirectoryStream
	seeks []uint64
}

func (s *seekableDirectoryStream) Seekdir(_ context.Context, off uint64) syscall.Errno {
	s.seeks = append(s.seeks, off)
	if off > uint64(len(s.entries)) {
		return syscall.EINVAL
	}
	s.index = int(off)
	return 0
}

func TestDirectoryHandleSeekUsesTheOpenedStream(t *testing.T) {
	reference := &nodeReferenceFixture{attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "directory"}}
	session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}, reference: reference}
	root := namespaceRoot(session)
	stream := &seekableDirectoryStream{fixedDirectoryStream: &fixedDirectoryStream{entries: []gofuse.DirEntry{{Name: "first"}, {Name: "second"}}}}
	handle := &directoryHandle{node: root, reference: reference, scope: reference.scope, stream: stream}

	if errno := handle.Seekdir(t.Context(), 1); errno != 0 || stream.index != 1 || len(stream.seeks) != 1 {
		t.Fatalf("seek errno=%v index=%d seeks=%v", errno, stream.index, stream.seeks)
	}
	entry, errno := handle.Readdirent(t.Context())
	if errno != 0 || entry == nil || entry.Name != "second" {
		t.Fatalf("entry after seek=%+v errno=%v", entry, errno)
	}
	if errno := handle.Seekdir(t.Context(), 3); errno != syscall.EINVAL {
		t.Fatalf("out-of-range seek = %v", errno)
	}

	nonseekable := &directoryHandle{node: root, reference: reference, scope: reference.scope,
		stream: &fixedDirectoryStream{entries: []gofuse.DirEntry{{Name: "only"}}}}
	if errno := nonseekable.Seekdir(t.Context(), 0); errno != syscall.EOPNOTSUPP {
		t.Fatalf("non-seekable stream = %v", errno)
	}
}

func addObservedEntry(t *testing.T, result *storage.ListResult, name string, attr storage.Attr) {
	t.Helper()
	if err := result.Add(storage.Entry{Name: name, Attr: attr}); err != nil {
		t.Fatal(err)
	}
}

func TestDirectoryHandleEnumerationUsesOneBoundedExactScopeCapture(t *testing.T) {
	reference := &nodeReferenceFixture{
		attr:  storage.Attr{ID: 7, Kind: storage.NodeDirectory},
		scope: storage.UseScope{Token: "opened-directory"},
	}
	session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}, reference: reference}
	captures := 0
	session.readDirBounded = func(target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
		captures++
		if target.NodeID != reference.attr.ID || target.Scope == nil || *target.Scope != reference.scope {
			t.Fatalf("directory target = %+v", target)
		}
		addObservedEntry(t, result, "first", storage.Attr{ID: 8, Kind: storage.NodeRegular})
		addObservedEntry(t, result, "second", storage.Attr{ID: 9, Kind: storage.NodeDirectory})
		return storage.DirectoryObservation{ParentID: 7, Revision: []byte("capture-1")}, nil
	}
	root := namespaceRoot(session)
	root.id.child("stale", syscall.S_IFREG|0600, 10)
	opened, _, errno := root.OpendirHandle(t.Context(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatal(errno)
	}
	handle := opened.(*directoryHandle)
	t.Cleanup(func() { handle.Releasedir(context.Background(), 0) })

	entry, errno := handle.Readdirent(t.Context())
	if errno != 0 || entry == nil || entry.Name != "first" {
		t.Fatalf("first entry=%+v errno=%v", entry, errno)
	}
	if errno := handle.Seekdir(t.Context(), 0); errno != 0 {
		t.Fatalf("seek to capture start: %v", errno)
	}
	entry, errno = handle.Readdirent(t.Context())
	if errno != 0 || entry == nil || entry.Name != "first" {
		t.Fatalf("entry after seek=%+v errno=%v", entry, errno)
	}
	if captures != 1 || reference.stats != 0 || root.id.children["stale"] != nil {
		t.Fatalf("captures=%d reference stats=%d identities=%+v", captures, reference.stats, root.id.children)
	}
}

func TestDirectoryHandleRejectsFailedPartialAndMalformedCapturesBeforeChangingIdentities(t *testing.T) {
	for _, test := range []struct {
		name      string
		capture   func(*storage.ListResult) (storage.DirectoryObservation, error)
		wantErrno syscall.Errno
	}{
		{
			name: "failed partial result",
			capture: func(result *storage.ListResult) (storage.DirectoryObservation, error) {
				addObservedEntry(t, result, "uncommitted", storage.Attr{ID: 9, Kind: storage.NodeRegular})
				return storage.DirectoryObservation{}, syscall.ESTALE
			},
			wantErrno: syscall.ESTALE,
		},
		{
			name: "unfinished reservation",
			capture: func(result *storage.ListResult) (storage.DirectoryObservation, error) {
				if _, err := result.Reserve(9, 6, storage.Attr{ID: 9, Kind: storage.NodeRegular}); err != nil {
					t.Fatal(err)
				}
				return storage.DirectoryObservation{ParentID: 7, Revision: []byte("capture")}, nil
			},
			wantErrno: syscall.EIO,
		},
		{
			name: "wrong parent",
			capture: func(result *storage.ListResult) (storage.DirectoryObservation, error) {
				addObservedEntry(t, result, "replacement", storage.Attr{ID: 9, Kind: storage.NodeRegular})
				return storage.DirectoryObservation{ParentID: 70, Revision: []byte("capture")}, nil
			},
			wantErrno: syscall.EIO,
		},
		{
			name: "malformed entry",
			capture: func(result *storage.ListResult) (storage.DirectoryObservation, error) {
				addObservedEntry(t, result, "replacement", storage.Attr{Kind: storage.NodeRegular})
				return storage.DirectoryObservation{ParentID: 7, Revision: []byte("capture")}, nil
			},
			wantErrno: syscall.EIO,
		},
		{
			name: "malformed projected metadata after a valid entry",
			capture: func(result *storage.ListResult) (storage.DirectoryObservation, error) {
				addObservedEntry(t, result, "first-new", storage.Attr{ID: 9, Kind: storage.NodeRegular})
				addObservedEntry(t, result, "bad-mode", storage.Attr{
					ID: 10, Kind: storage.NodeRegular,
					Metadata: map[string]storage.OpaquePayload{
						posix.Namespace: {Version: []byte{1}, Data: []byte{1}},
					},
				})
				return storage.DirectoryObservation{ParentID: 7, Revision: []byte("capture")}, nil
			},
			wantErrno: syscall.EIO,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			reference := &nodeReferenceFixture{
				attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "directory"},
			}
			session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}, reference: reference}
			session.readDirBounded = func(target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
				if target.NodeID != 7 || target.Scope == nil || *target.Scope != reference.scope {
					t.Fatalf("directory target = %+v", target)
				}
				return test.capture(result)
			}
			root := namespaceRoot(session)
			known := root.id.child("known", syscall.S_IFREG|0600, 8)
			handle := &directoryHandle{node: root, reference: reference, scope: reference.scope}
			entry, errno := handle.Readdirent(t.Context())
			if entry != nil || errno != test.wantErrno {
				t.Fatalf("entry=%+v errno=%v, want %v", entry, errno, test.wantErrno)
			}
			if root.id.children["known"] != known || root.id.children["replacement"] != nil || root.id.children["uncommitted"] != nil ||
				root.id.children["first-new"] != nil || root.id.children["bad-mode"] != nil {
				t.Fatalf("failed capture changed identities: %+v", root.id.children)
			}
		})
	}
}

func TestReleasedDirectoryHandleDoesNotStartAnObservation(t *testing.T) {
	reference := &nodeReferenceFixture{
		attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "directory"},
	}
	session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}, reference: reference}
	captures := 0
	session.readDirBounded = func(storage.DirectoryTarget, *storage.ListResult) (storage.DirectoryObservation, error) {
		captures++
		return storage.DirectoryObservation{}, nil
	}
	handle := &directoryHandle{node: namespaceRoot(session), reference: reference, scope: reference.scope}
	handle.Releasedir(t.Context(), 0)
	if entry, errno := handle.Readdirent(t.Context()); entry != nil || errno != syscall.EBADF || captures != 0 {
		t.Fatalf("released read entry=%+v errno=%v captures=%d", entry, errno, captures)
	}
}

func TestDirectoryReleaseDiscardsAnInFlightObservationBeforeIdentityChanges(t *testing.T) {
	reference := &nodeReferenceFixture{
		attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "directory"},
	}
	session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}, reference: reference}
	started := make(chan struct{})
	proceed := make(chan struct{})
	session.readDirBounded = func(storage.DirectoryTarget, *storage.ListResult) (storage.DirectoryObservation, error) {
		close(started)
		<-proceed
		return storage.DirectoryObservation{ParentID: 7, Revision: []byte("capture")}, nil
	}
	root := namespaceRoot(session)
	known := root.id.child("known", syscall.S_IFREG|0600, 8)
	handle := &directoryHandle{node: root, reference: reference, scope: reference.scope}
	type readResult struct {
		entry *gofuse.DirEntry
		errno syscall.Errno
	}
	done := make(chan readResult, 1)
	go func() {
		entry, errno := handle.Readdirent(t.Context())
		done <- readResult{entry: entry, errno: errno}
	}()
	<-started
	handle.Releasedir(t.Context(), 0)
	close(proceed)
	result := <-done
	if result.entry != nil || result.errno != syscall.EBADF {
		t.Fatalf("released in-flight read entry=%+v errno=%v", result.entry, result.errno)
	}
	if handle.stream != nil || root.id.children["known"] != known || reference.closes != 1 {
		t.Fatalf("stream=%v identities=%+v closes=%d", handle.stream, root.id.children, reference.closes)
	}
}

func TestDirectoryReleaseClosesEveryResourceOnceAndFencesUnknownCleanup(t *testing.T) {
	closeErr := errors.New("reference cleanup result unavailable")
	reference := &nodeReferenceFixture{
		attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "directory"}, closeErr: closeErr,
	}
	session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}, reference: reference}
	root := namespaceRoot(session)
	stream := &fixedDirectoryStream{}
	handle := &directoryHandle{node: root, reference: reference, scope: reference.scope, stream: stream}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	handle.Releasedir(canceled, 0)
	handle.Releasedir(canceled, 0)
	if reference.closes != 1 || !reference.clean || stream.closes != 1 || handle.stream != nil {
		t.Fatalf("release reference closes=%d clean=%v stream closes=%d retained=%v", reference.closes, reference.clean, stream.closes, handle.stream)
	}
	if err := root.volume.check(); errnoOf(err) != syscall.EIO || !errors.Is(err, closeErr) {
		t.Fatalf("unknown cleanup did not fence the volume: %v", err)
	}
}

func TestReadlinkUsesRetainedSymlinkIdentityAndClosesIt(t *testing.T) {
	namespace := &namespaceFixture{}
	reference := &nodeReferenceFixture{
		attr: storage.Attr{ID: 9, Kind: storage.NodeSymlink, Size: 9},
		link: []byte("../target"), scope: storage.UseScope{Token: "symlink"},
	}
	session := &directorySessionFixture{namespaceFixture: namespace, reference: reference}
	root := namespaceRoot(session)
	link := root.child(t.Context(), "link", syscall.S_IFLNK|0777, 9).Operations().(*node)

	target, errno := link.Readlink(t.Context())
	if errno != 0 || string(target) != "../target" {
		t.Fatalf("readlink=%q errno=%v", target, errno)
	}
	if session.openID != 9 || session.openOptions.Kind != storage.NodeSymlink || session.openOptions.Target.State != storage.SameNode || session.openOptions.Target.NodeID != 9 || session.openOptions.MetadataAccess != storage.ReadMetadata {
		t.Fatalf("readlink options = %+v", session.openOptions)
	}
	if reference.closes != 1 || !reference.clean {
		t.Fatalf("closes=%d clean=%v", reference.closes, reference.clean)
	}
}

func TestReadlinkRejectsEmptyAuthorityTargetAndClosesReference(t *testing.T) {
	namespace := &namespaceFixture{}
	reference := &nodeReferenceFixture{
		attr:  storage.Attr{ID: 9, Kind: storage.NodeSymlink},
		scope: storage.UseScope{Token: "empty-symlink"},
	}
	session := &directorySessionFixture{namespaceFixture: namespace, reference: reference}
	root := namespaceRoot(session)
	link := root.child(t.Context(), "link", syscall.S_IFLNK|0777, 9).Operations().(*node)

	if target, errno := link.Readlink(t.Context()); errno != syscall.EIO || target != nil {
		t.Fatalf("readlink=%q errno=%v", target, errno)
	}
	if reference.closes != 1 || !reference.clean {
		t.Fatalf("closes=%d clean=%v", reference.closes, reference.clean)
	}
}

func TestDirectoryHandleLookupKeepsScopeAfterParentRenameAndNameReuse(t *testing.T) {
	namespace := &namespaceFixture{}
	reference := &nodeReferenceFixture{
		attr:  storage.Attr{ID: 8, Kind: storage.NodeDirectory},
		scope: storage.UseScope{Token: "renamed-directory"},
	}
	session := &directorySessionFixture{namespaceFixture: namespace, reference: reference}
	root := namespaceRoot(session)
	old := root.child(t.Context(), "parent", syscall.S_IFDIR|0755, 8)
	if !root.AddChild("parent", old, false) {
		t.Fatal("attaching parent")
	}
	handleValue, _, errno := old.Operations().(*node).OpendirHandle(t.Context(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatal(errno)
	}
	handle := handleValue.(*directoryHandle)
	t.Cleanup(func() { handle.Releasedir(context.Background(), 0) })
	if !root.MvChild("parent", &root.Inode, "moved", true) {
		t.Fatal("moving parent")
	}
	root.id.move("parent", root.id, "moved")
	replacement := root.child(t.Context(), "parent", syscall.S_IFDIR|0755, 10)
	if !root.AddChild("parent", replacement, false) {
		t.Fatal("attaching replacement parent")
	}
	namespace.lookup = func(name storage.ChildName) (storage.Attr, error) {
		if name.Parent.NodeID != 8 || name.Parent.Scope == nil || *name.Parent.Scope != reference.scope {
			t.Fatalf("lookup rebound to replacement: %+v", name)
		}
		return storage.Attr{ID: 11, Kind: storage.NodeRegular}, nil
	}
	if _, errno := handle.Lookup(t.Context(), "child", &gofuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}
}

func TestDirectoryEnumerationKeepsOpenedIdentityAcrossExternalRenameAndOldNameReuse(t *testing.T) {
	_, backing := memoryfixture.New(t, "directory-path-list", 0, locking.DefaultOptions())
	if err := backing.Mkdir(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	if err := backing.Write(t.Context(), "parent/original", []byte("old")); err != nil {
		t.Fatal(err)
	}
	parentAttr, err := backing.Stat(t.Context(), "parent")
	if err != nil {
		t.Fatal(err)
	}
	session, err := backing.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	v := activeTestVolume(backing, 1024)
	v.files = session
	rootAttr, err := backing.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	root := &node{volume: v, id: rootIdentity(rootAttr.ID)}
	fs.NewNodeFS(root, &fs.Options{})
	parent := root.child(t.Context(), "parent", syscall.S_IFDIR|0755, parentAttr.ID)
	if !root.AddChild("parent", parent, false) {
		t.Fatal("attaching parent")
	}
	handle, _, errno := parent.Operations().(*node).OpendirHandle(t.Context(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatal(errno)
	}
	directory := handle.(*directoryHandle)
	t.Cleanup(func() { directory.Releasedir(context.Background(), 0) })

	if err := backing.Rename(t.Context(), "parent", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := backing.Mkdir(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	if err := backing.Write(t.Context(), "parent/replacement", []byte("new")); err != nil {
		t.Fatal(err)
	}
	entry, errno := directory.Readdirent(t.Context())
	if errno != 0 || entry == nil || entry.Name != "original" {
		t.Fatalf("identity-bound enumeration = %+v, %v", entry, errno)
	}
}
