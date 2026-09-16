package httprest

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

type fileFlowBackend struct {
	storage.FileStorage
	check func() error
}

func (b fileFlowBackend) CheckFileStorage() error {
	if b.check != nil {
		return b.check()
	}
	return nil
}

type fileFlowLog struct {
	metastore.Log
	failure error
}

func (l fileFlowLog) Barrier(context.Context, int64) (metastore.LogBarrier, error) {
	if l.failure != nil {
		return metastore.LogBarrier{}, l.failure
	}
	return metastore.LogBarrier{Incarnation: "flow", Position: 1}, nil
}

type fileFlowSession struct {
	storage.FileSession
	file  storage.File
	query func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
}

func (s *fileFlowSession) Reference(context.Context, storage.FileReferenceID) (storage.File, error) {
	return s.file, nil
}
func (s *fileFlowSession) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.query(ctx, id)
}
func (s *fileFlowSession) Close(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
	return storage.FileActionReceipt{Operation: storage.OpFileSessionClose, State: storage.FileActionRetired}, nil
}

type fileFlowFile struct {
	lookup func(context.Context, []byte) (storage.EntryLookup, error)
	list   func(context.Context, storage.DirectoryPageRequest) (storage.DirectoryPage, error)
	storage.File
	wait    func(context.Context, storage.RangeWaitRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	replace func(context.Context, storage.RangeReplaceRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	write   func(context.Context, storage.FileWriteRequest, storage.FileActionID) (storage.FileActionReceipt, error)
}

func (f *fileFlowFile) LookupAt(ctx context.Context, name []byte) (storage.EntryLookup, error) {
	return f.lookup(ctx, name)
}
func (f *fileFlowFile) ListAt(ctx context.Context, r storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	return f.list(ctx, r)
}
func (*fileFlowFile) Reference() storage.FileReferenceID { return 1 }
func (*fileFlowFile) NodeID() uint64                     { return 11 }
func (f *fileFlowFile) WaitRanges(ctx context.Context, r storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.wait(ctx, r, id)
}
func (f *fileFlowFile) ReplaceRanges(ctx context.Context, r storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.replace(ctx, r, id)
}
func (f *fileFlowFile) WriteAt(ctx context.Context, r storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.write(ctx, r, id)
}

func fileFlowFixture(t *testing.T, native *fileFlowSession, log metastore.Log) (*Handler, *remoteFile, *httptest.Server) {
	t.Helper()
	life, cancel := context.WithCancelCause(context.Background())
	registry := newFileRegistry(fileFlowBackend{}, DefaultFileLimits())
	sessionLife, sessionCancel := context.WithCancelCause(context.Background())
	id := strings.Repeat("a", 64)
	registry.sessions[id] = &servedFileSession{native: native, options: storage.DefaultFileSessionOptions(), authority: "flow-session", revision: 1, actionEpoch: 1, expires: time.Now().Add(time.Minute), cleanup: context.Background(), lifetime: sessionLife, cancel: sessionCancel}
	const body = 2 << 20
	handler := &Handler{files: registry, log: log, lifetime: life, cancelLifetime: cancel, stopping: make(chan struct{}), maxBodyBytes: body, maxWriteBytes: body, maxIncarnationBytes: 128, maxFrameBytes: 4 << 20,
		responses: newBodyAdmission(1, retainedResponseMultiplier*body, 4), bodies: newBodyAdmission(1, body, 4), fileWaits: newBodyAdmission(1, retainedResponseMultiplier*body, 4), fileControls: newBodyAdmission(2, retainedResponseMultiplier*body, 4)}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		handler.Stop()
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	remote := &remoteFile{session: &remoteFileSession{storage: client, id: id}, id: 1, node: 11}
	return handler, remote, server
}
func fileFlowID(t *testing.T) storage.FileActionID {
	t.Helper()
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func fileFlowRequest(ctx context.Context, server *httptest.Server, request fileRequest) (int, fileResponse, fileErrorResponse, error) {
	body, err := marshalFileJSON(request)
	if err != nil {
		return 0, fileResponse{}, fileErrorResponse{}, err
	}
	op := OpFile
	if fileControl(request.Op) {
		op = OpFileControl
	}
	outgoing, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+Prefix+string(op), bytes.NewReader(body))
	if err != nil {
		return 0, fileResponse{}, fileErrorResponse{}, err
	}
	outgoing.Header.Set("Content-Type", contentJSON)
	response, err := server.Client().Do(outgoing)
	if err != nil {
		return 0, fileResponse{}, fileErrorResponse{}, err
	}
	defer response.Body.Close()
	data, err := readWhole(response, 4<<20)
	if err != nil {
		return response.StatusCode, fileResponse{}, fileErrorResponse{}, err
	}
	var result fileResponse
	var failure fileErrorResponse
	if response.StatusCode == http.StatusOK {
		err = decodeFileJSON(data, &result)
	} else {
		err = decodeFileJSON(data, &failure)
	}
	return response.StatusCode, result, failure, err
}

func TestFileWaitHandoffCannotBlockBulkRangeReplacement(t *testing.T) {
	started, unlocked := make(chan struct{}), make(chan struct{})
	var once sync.Once
	file := &fileFlowFile{}
	file.wait = func(ctx context.Context, _ storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		once.Do(func() { close(started) })
		select {
		case <-unlocked:
		case <-ctx.Done():
			return storage.FileActionReceipt{}, ctx.Err()
		}
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileWaitRanges, State: storage.FileActionCompleted, RangeRevision: 2}, nil
	}
	file.replace = func(_ context.Context, _ storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		close(unlocked)
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileReplaceRanges, State: storage.FileActionCompleted, Effects: storage.EffectRangesChanged, RangeRevision: 2}, nil
	}
	handler, remote, server := fileFlowFixture(t, &fileFlowSession{file: file}, fileFlowLog{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type answer struct {
		status  int
		failure fileErrorResponse
		err     error
	}
	send := func(request fileRequest, done chan<- answer) {
		status, _, failure, err := fileFlowRequest(ctx, server, request)
		done <- answer{status, failure, err}
	}
	request := fileRequest{Op: storage.OpFileWaitRanges, Session: remote.session.id, Reference: 1, Action: fileFlowID(t), Wait: &storage.RangeWaitRequest{ExpectedRevision: 1, Ranges: []storage.RangeAcquisition{{ID: 1, Start: 0, End: 1, Exclusive: true}}}}
	active := make(chan answer, 1)
	go send(request, active)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first wait did not enter native code")
	}
	request.Action = fileFlowID(t)
	queued := make(chan answer, 1)
	go send(request, queued)
	var queueAnswer *answer
	for queueAnswer == nil {
		select {
		case result := <-queued:
			queueAnswer = &result
		default:
		}
		handler.fileWaits.mu.Lock()
		waiting := handler.fileWaits.waiters
		handler.fileWaits.mu.Unlock()
		if waiting > 0 {
			break
		}
		if queueAnswer == nil {
			select {
			case <-ctx.Done():
				t.Fatal("second wait did not reach admission")
			case <-time.After(time.Millisecond):
			}
		}
	}
	replacement := make(chan answer, 1)
	go send(fileRequest{Op: storage.OpFileReplaceRanges, Session: remote.session.id, Reference: 1, Action: fileFlowID(t), Ranges: &storage.RangeReplaceRequest{ExpectedRevision: 1, Ranges: []storage.RangeAcquisition{}}}, replacement)
	select {
	case result := <-replacement:
		if result.err != nil || result.status != http.StatusOK {
			t.Fatalf("bulk replacement = %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("queued wait held the bulk slot needed to release the active wait")
	}
	if queueAnswer == nil {
		select {
		case result := <-queued:
			queueAnswer = &result
		case <-ctx.Done():
			t.Fatal("second wait stayed queued")
		}
	}
	if queueAnswer.err != nil || queueAnswer.status != StatusStorageError || queueAnswer.failure.Errno != "EAGAIN" {
		t.Fatalf("full wait lane result = %+v", queueAnswer)
	}
	select {
	case result := <-active:
		if result.err != nil || result.status != http.StatusOK {
			t.Fatalf("active wait result = %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("replacement did not wake the active wait")
	}
}

func TestFileCommittedReceiptSurvivesIndependentBarrierFailure(t *testing.T) {
	for _, failure := range []error{context.Canceled, syscall.ENOENT} {
		t.Run(failure.Error(), func(t *testing.T) {
			var applied storage.FileActionReceipt
			file := &fileFlowFile{write: func(_ context.Context, _ storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				applied = storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionCompleted, Effects: storage.EffectContentChanged, Observation: storage.FileObservation{Attr: storage.Attr{ID: 11, Kind: storage.NodeRegular, Size: 5, MetadataRevision: 2}}}
				return applied, nil
			}}
			_, remote, _ := fileFlowFixture(t, &fileFlowSession{file: file}, fileFlowLog{failure: failure})
			result, err := remote.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("known")}, fileFlowID(t))
			if storage.ErrnoOf(err) != syscall.EIO || result.Action != applied.Action || result.Effects != applied.Effects || result.Errno != 0 || result.Observation.Attr.Size != 5 {
				t.Fatalf("committed outcome with failed barrier = %+v, %v", result, err)
			}
		})
	}
}

func TestFileMalformedCommittedReceiptStaysUnknownUntilExplicitQuery(t *testing.T) {
	for _, kind := range []string{"missing", "wrong action", "invalid attributes", "omitted native error"} {
		t.Run(kind, func(t *testing.T) {
			var mu sync.Mutex
			writes, queries := 0, 0
			var known storage.FileActionReceipt
			file := &fileFlowFile{write: func(_ context.Context, _ storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				mu.Lock()
				defer mu.Unlock()
				writes++
				known = storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionCompleted, Effects: storage.EffectContentChanged, Observation: storage.FileObservation{Attr: storage.Attr{ID: 11, Kind: storage.NodeRegular, Size: 5, MetadataRevision: 2}}}
				malformed := known
				switch kind {
				case "missing":
					return storage.FileActionReceipt{}, errors.New("reply unavailable after commit")
				case "wrong action":
					malformed.Action = ""
				case "invalid attributes":
					malformed.Observation.Attr.Kind = 0
				case "omitted native error":
					malformed.Errno = syscall.EACCES
				}
				return malformed, nil
			}}
			native := &fileFlowSession{file: file, query: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
				mu.Lock()
				defer mu.Unlock()
				queries++
				if id != known.Action {
					t.Error("recovery changed the original action")
				}
				return known, nil
			}}
			_, remote, _ := fileFlowFixture(t, native, fileFlowLog{})
			id := fileFlowID(t)
			result, err := remote.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("known")}, id)
			if storage.ErrnoOf(err) != syscall.EIO || result.Action != id || result.State != storage.FileActionUnknown || result.Effects != 0 || storage.IsFileCallNotAdmitted(err) {
				t.Fatalf("malformed post-dispatch outcome = %+v, %v", result, err)
			}
			mu.Lock()
			beforeQueries := queries
			mu.Unlock()
			if beforeQueries != 0 {
				t.Fatalf("HTTP queried %d times without the caller's immutable plan", beforeQueries)
			}
			observed, err := remote.session.QueryAction(t.Context(), id)
			if err != nil || observed.Action != id || observed.State != storage.FileActionCompleted || observed.Effects != storage.EffectContentChanged || observed.Observation.Attr.Size != 5 {
				t.Fatalf("explicit original-action query = %+v, %v", observed, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if writes != 1 || queries != 1 {
				t.Fatalf("native writes=%d queries=%d", writes, queries)
			}
		})
	}
}

func TestFileDeclaredResultBoundsRefuseBeforeNativeLookup(t *testing.T) {
	native := &fileFlowSession{}
	handler, remote, server := fileFlowFixture(t, native, fileFlowLog{})
	var callsMu sync.Mutex
	calls := 0
	handler.files.backend = fileFlowBackend{check: func() error { callsMu.Lock(); calls++; callsMu.Unlock(); return nil }}
	// A native lookup would return a nil file and fail as a protocol fault. These
	// valid requests must instead fail the declared result budget before lookup.
	for _, request := range []fileRequest{
		{Op: storage.OpFileListAt, List: &storage.DirectoryPageRequest{MaxEntries: storage.MaxDirectoryPageEntries, MaxBytes: storage.MaxDirectoryPageBytes}},
		{Op: storage.OpFileRangeSnapshot, Owner: new(storage.RangeOwnerID), Scope: &storage.RangeScope{}},
	} {
		request.Session = remote.session.id
		request.Reference = 1
		status, _, failure, err := fileFlowRequest(t.Context(), server, request)
		if err != nil || status != StatusStorageError || failure.Errno != "EFBIG" {
			t.Fatalf("declared %s result beyond %d bytes = %d %+v, %v", request.Op, handler.maxBodyBytes, status, failure, err)
		}
		callsMu.Lock()
		count := calls
		callsMu.Unlock()
		if count != 0 {
			t.Fatalf("result budget reached native dispatch %d times", count)
		}
	}
}

func TestFileRangeSnapshotBoundCoversWorstLegalEntryShape(t *testing.T) {
	maximum := ^uint64(0)
	entry := storage.HeldRange{Owner: storage.RangeOwner{Session: strings.Repeat("&", storage.MaxFileSessionIDBytes), ID: storage.RangeOwnerID(maximum)}, Range: storage.RangeAcquisition{ID: storage.RangeAcquisitionID(maximum), Start: maximum, End: maximum}}
	if err := entry.Range.Check(); err != nil {
		t.Fatal(err)
	}
	snapshot := storage.RangeSnapshot{Revision: maximum, Own: []storage.RangeAcquisition{}, Other: []storage.HeldRange{entry}, Available: int(^uint(0) >> 1), OwnerAvailable: int(^uint(0) >> 1)}
	body, err := marshalFileJSON(fileResponse{Ranges: &snapshot})
	if err != nil {
		t.Fatal(err)
	}
	encodedEntry, err := marshalFileJSON(entry)
	if err != nil {
		t.Fatal(err)
	}
	fullSize := int64(len(body)) + int64(storage.MaxRangeSnapshotRanges-1)*int64(len(encodedEntry)+1)
	if fullSize > fileRangeSnapshotResponseLimit() {
		t.Fatalf("legal complete snapshot needs %d bytes above proof %d", fullSize, fileRangeSnapshotResponseLimit())
	}
}

type fileSetAttrAuthority struct{ locking.Service }
type fileSetAttrBackend struct {
	storage.BoundedStorage
	calls int
	last  storage.AttrChange
}

func (*fileSetAttrBackend) CheckBounded() error          { return nil }
func (*fileSetAttrBackend) LockService() locking.Service { return &fileSetAttrAuthority{} }
func (b *fileSetAttrBackend) SetAttr(_ context.Context, _ string, change storage.AttrChange) error {
	b.calls++
	b.last = change
	return nil
}

func TestFileOrdinarySetAttrRejectsMalformedChangesBeforeBackend(t *testing.T) {
	handler, _, _ := fileFlowFixture(t, &fileFlowSession{}, fileFlowLog{})
	backend := &fileSetAttrBackend{}
	paired, err := locked.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	handler.storage = paired
	for _, body := range []string{
		`{"change":{"expected_revision":0,"mode":420}}`,
		`{"change":{}}`,
		`{"change":{"expected_revision":0,"access_time":{"unix_sec":1}}}`,
		`{"change":null}`,
		`{"change":{"expected_revision":0,"unknown":1}}`,
		`{"change":{"expected_revision":0,"expected_revision":0}}`,
	} {
		request := httptest.NewRequest(http.MethodPost, Prefix+string(OpSetAttr)+"?path=file", strings.NewReader(body))
		request.Header.Set("Content-Type", contentJSON)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || backend.calls != 0 {
			t.Fatalf("malformed change reached backend: status=%d calls=%d body=%s", response.Code, backend.calls, response.Body.String())
		}
	}
	request := httptest.NewRequest(http.MethodPost, Prefix+string(OpSetAttr)+"?path=file", strings.NewReader(`{"change":{"expected_revision":0}}`))
	request.Header.Set("Content-Type", contentJSON)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || backend.calls != 1 || !backend.last.Empty() {
		t.Fatalf("explicit empty change = status %d calls %d change %+v", response.Code, backend.calls, backend.last)
	}
}

func TestFileLookupAtPreservesExactNameAndAuthoritativeAbsence(t *testing.T) {
	var mu sync.Mutex
	lookups, lists, decisions := 0, 0, 0
	wrongParent, denied := false, false
	wanted := []byte{0xff, 'A'}
	missing := []byte{0xff, 'B'}
	file := &fileFlowFile{
		lookup: func(_ context.Context, name []byte) (storage.EntryLookup, error) {
			mu.Lock()
			defer mu.Unlock()
			lookups++
			result := storage.EntryLookup{ParentID: 11, DirectoryRevision: 7, Name: bytes.Clone(name)}
			if bytes.Equal(name, wanted) {
				result.Found = true
				result.EntryID = 99
				result.Attr = storage.Attr{ID: 12, Kind: storage.NodeRegular, Size: 3, MetadataRevision: 2}
			}
			if wrongParent {
				result.ParentID = 17
			}
			return result, nil
		},
		list: func(context.Context, storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
			mu.Lock()
			lists++
			mu.Unlock()
			return storage.DirectoryPage{}, syscall.EIO
		},
	}
	handler, remote, _ := fileFlowFixture(t, &fileFlowSession{file: file}, fileFlowLog{})
	handler.authorizer = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		mu.Lock()
		defer mu.Unlock()
		decisions++
		if request.Operation != storage.OpFileLookupAt || request.Reference != 1 || request.Effects != 0 {
			t.Errorf("lookup authorization = %+v", request)
		}
		if denied {
			return authz.ErrDenied
		}
		return nil
	})
	found, err := remote.LookupAt(t.Context(), wanted)
	if err != nil || !found.Found || found.ParentID != 11 || found.EntryID != 99 || found.Attr.ID != 12 || !bytes.Equal(found.Name, wanted) {
		t.Fatalf("exact byte lookup = %+v, %v", found, err)
	}
	absent, err := remote.LookupAt(t.Context(), missing)
	if err != nil || absent.Found || absent.ParentID != 11 || absent.DirectoryRevision != 7 || absent.EntryID != 0 || absent.Attr.ID != 0 || !bytes.Equal(absent.Name, missing) {
		t.Fatalf("confirmed absent lookup = %+v, %v", absent, err)
	}
	mu.Lock()
	wrongParent = true
	mu.Unlock()
	if _, err := remote.LookupAt(t.Context(), wanted); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("lookup changed retained parent identity = %v", err)
	}
	mu.Lock()
	denied = true
	mu.Unlock()
	if result, err := remote.LookupAt(t.Context(), wanted); !errors.Is(err, syscall.EACCES) || result.Found || result.Attr.ID != 0 {
		t.Fatalf("denied lookup disclosed facts = %+v, %v", result, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if lookups != 3 || lists != 0 || decisions != 4 {
		t.Fatalf("lookup=%d list=%d policy=%d", lookups, lists, decisions)
	}
}

type fileFlowRoundTripper func(*http.Request) (*http.Response, error)

func (f fileFlowRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestFileLostNonAdmissionCannotPromoteAnOlderSuccessfulAction(t *testing.T) {
	var mu sync.Mutex
	writes, queries := 0, 0
	var known storage.FileActionReceipt
	file := &fileFlowFile{write: func(_ context.Context, request storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		mu.Lock()
		defer mu.Unlock()
		if writes != 0 {
			return storage.FileActionReceipt{}, &storage.FileError{Code: syscall.EINVAL, NotAdmitted: true, Cause: errors.New("action operands changed")}
		}
		if string(request.Data) != "first" {
			t.Error("unexpected initial content")
		}
		writes++
		known = storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionCompleted, Effects: storage.EffectContentChanged, Observation: storage.FileObservation{Attr: storage.Attr{ID: 11, Kind: storage.NodeRegular, Size: 5, MetadataRevision: 2}}}
		return known, nil
	}}
	native := &fileFlowSession{file: file, query: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		mu.Lock()
		defer mu.Unlock()
		queries++
		if id != known.Action {
			t.Error("query changed identity")
		}
		return known, nil
	}}
	_, remote, server := fileFlowFixture(t, native, fileFlowLog{})
	id := fileFlowID(t)
	if result, err := remote.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("first")}, id); err != nil || result.State != storage.FileActionCompleted {
		t.Fatalf("initial action = %+v, %v", result, err)
	}
	refused, err := remote.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("other")}, id)
	if !errors.Is(err, syscall.EINVAL) || !storage.IsFileCallNotAdmitted(err) || refused.State != 0 {
		t.Fatalf("changed operands were not a current-call refusal: %+v, %v", refused, err)
	}
	transport := server.Client().Transport
	if transport == nil {
		t.Fatal("fixture has no transport")
	}
	var dropMu sync.Mutex
	drop := true
	client := *remote.session.storage.http
	client.Transport = fileFlowRoundTripper(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		dropMu.Lock()
		lose := drop
		drop = false
		dropMu.Unlock()
		if lose {
			if response.StatusCode != StatusStorageError {
				t.Errorf("lost response was not a refusal: %d", response.StatusCode)
			}
			response.Body.Close()
			return nil, errors.New("refusal response was lost")
		}
		return response, nil
	})
	remote.session.storage.http = &client
	unknown, err := remote.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("other")}, id)
	if storage.ErrnoOf(err) != syscall.EIO || unknown.State != storage.FileActionUnknown || unknown.Effects != 0 || storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("lost rejection became a known action result: %+v, %v", unknown, err)
	}
	mu.Lock()
	beforeQueries, applied := queries, writes
	mu.Unlock()
	if beforeQueries != 0 || applied != 1 {
		t.Fatalf("lost refusal triggered %d implicit queries and %d writes", beforeQueries, applied)
	}
	observed, err := remote.session.QueryAction(t.Context(), id)
	if err != nil || observed.State != storage.FileActionCompleted || observed.Action != id || observed.Observation.Attr.Size != 5 {
		t.Fatalf("explicit historical query = %+v, %v", observed, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if writes != 1 || queries != 1 {
		t.Fatalf("writes=%d queries=%d", writes, queries)
	}
}

func TestFileNonAdmissionProofDoesNotHideAnIndependentFailure(t *testing.T) {
	rejection := &storage.FileError{Code: syscall.EAGAIN, NotAdmitted: true, Cause: errors.New("admission full")}
	file := &fileFlowFile{write: func(context.Context, storage.FileWriteRequest, storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{}, errors.Join(rejection, errors.New("independent outcome failure"))
	}}
	_, remote, _ := fileFlowFixture(t, &fileFlowSession{file: file}, fileFlowLog{})
	result, err := remote.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("unknown")}, fileFlowID(t))
	if storage.ErrnoOf(err) != syscall.EIO || result.State != storage.FileActionUnknown || storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("one proof hid an independent failure: %+v, %v", result, err)
	}
}

func TestFileAuthorizationSanitizesInternalProtocolFaults(t *testing.T) {
	handler, remote, server := fileFlowFixture(t, &fileFlowSession{}, fileFlowLog{})
	handler.authorizer = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		return nativeFileFault(&storage.FileError{Code: syscall.EACCES, NotAdmitted: true, Cause: errors.New("private policy detail")})
	})
	request := fileRequest{Op: storage.OpFileWrite, Session: remote.session.id, Reference: 1, Action: fileFlowID(t), Write: &fileWriteRequest{Data: []byte("refused")}}
	status, _, failure, err := fileFlowRequest(t.Context(), server, request)
	if err != nil || status != StatusStorageError || failure.Errno != "EIO" || failure.NotAdmitted || failure.Receipt != nil || failure.Conflict != nil || failure.LockCode != nil || strings.Contains(failure.Message, "private policy detail") {
		t.Fatalf("policy failure escaped authorization boundary = %d %+v, %v", status, failure, err)
	}
}
