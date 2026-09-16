package httprest

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func genericServerFixture(t *testing.T, limits FileLimits) (*Storage, *Handler, *objectstore.Storage) {
	t.Helper()
	meta, backend := memoryfixture.New(t, "generic-http", 1<<20, locking.DefaultOptions())
	options := DefaultHandlerOptions()
	options.Files = limits
	handler, err := NewHandlerWithOptions(backend, meta, options)
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := client.FileState(t.Context()); err != nil {
		t.Fatal(err)
	}
	return client, handler, backend
}
func genericServerSession(t *testing.T, client storage.FileStorage, options storage.FileSessionOptions) storage.FileSession {
	t.Helper()
	session, status, err := client.NewFileSession(t.Context(), options)
	if err != nil || session == nil || status.ActionEpoch == 0 {
		t.Fatalf("new session = %+v, %v", status, err)
	}
	return session
}
func genericServerAction(t *testing.T, s storage.FileSession) storage.FileActionID {
	t.Helper()
	status, err := s.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func genericServerRetain(t *testing.T, s storage.FileSession, node uint64, claim storage.AccessClaim) (storage.File, storage.FileActionReceipt) {
	t.Helper()
	receipt, err := s.Retain(t.Context(), storage.RetainRequest{NodeID: node, Claim: claim}, genericServerAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	file, err := s.Reference(t.Context(), receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	return file, receipt
}
func genericServerRoot(t *testing.T, s storage.FileSession, backend storage.Storage) storage.File {
	t.Helper()
	attr, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	file, _ := genericServerRetain(t, s, attr.ID, storage.AccessClaim{})
	return file
}
func genericServerTarget(t *testing.T, parent storage.File, name string) storage.EntryTarget {
	t.Helper()
	lookup, err := parent.LookupAt(t.Context(), []byte(name))
	if err != nil {
		t.Fatal(err)
	}
	target := storage.EntryTarget{Parent: parent.Reference(), ParentID: lookup.ParentID, Name: lookup.Name, DirectoryRevision: lookup.DirectoryRevision}
	if lookup.Found {
		target.ExpectedEntryID = lookup.EntryID
		target.ExpectedNodeID = lookup.Attr.ID
		target.ExpectedMetadataRevision = lookup.Attr.MetadataRevision
	}
	return target
}

func TestGenericFileServerRetainsIdentityAndReconcilesActions(t *testing.T) {
	client, _, backend := genericServerFixture(t, DefaultFileLimits())
	session := genericServerSession(t, client, storage.DefaultFileSessionOptions())
	root := genericServerRoot(t, session, backend)
	request := storage.CreateAndRetainRequest{Target: genericServerTarget(t, root, "file"), Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}
	action := genericServerAction(t, session)
	opened, err := session.CreateAndRetainAt(t.Context(), request, action)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := session.CreateAndRetainAt(t.Context(), request, action)
	if err != nil || replayed.Reference != opened.Reference || !reflect.DeepEqual(replayed.Observation, opened.Observation) {
		t.Fatalf("retain replay = %+v, %v", replayed, err)
	}
	changed := request
	changed.Claim = storage.AccessClaim{Uses: storage.ReadContent}
	if _, err := session.CreateAndRetainAt(t.Context(), changed, action); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed action operands = %v", err)
	}
	file, err := session.Reference(t.Context(), opened.Reference)
	if err != nil {
		t.Fatal(err)
	}
	writeAction := genericServerAction(t, session)
	first, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("first")}, writeAction)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("replacement")}, genericServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	replayed, err = file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("first")}, writeAction)
	if err != nil || replayed.Observation.Attr.Size != first.Observation.Attr.Size {
		t.Fatalf("write replay = %+v, %v", replayed, err)
	}
	read, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 32})
	if err != nil || string(read.Data) != "replacement" {
		t.Fatalf("live read = %+v, %v", read, err)
	}
	queried, err := session.QueryAction(t.Context(), action)
	if err != nil || queried.Reference != opened.Reference {
		t.Fatalf("query retain = %+v, %v", queried, err)
	}
	rename := storage.RenameRequest{Source: genericServerTarget(t, root, "file"), Destination: genericServerTarget(t, root, "renamed")}
	if _, err := file.Rename(t.Context(), rename, genericServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	observation, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil || observation.Attr.ID != opened.Observation.Attr.ID || observation.Location == nil || string(observation.Location.Ancestors[len(observation.Location.Ancestors)-1].Name) != "renamed" {
		t.Fatalf("renamed observation = %+v, %v", observation, err)
	}
	if body, err := backend.Read(t.Context(), "renamed"); err != nil || string(body) != "replacement" {
		t.Fatalf("authoritative bytes = %q, %v", body, err)
	}
	if _, err := file.Close(t.Context(), genericServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	queried, err = session.QueryAction(t.Context(), writeAction)
	if err != nil || queried.Observation.Attr.Size != first.Observation.Attr.Size {
		t.Fatalf("closed-reference history = %+v, %v", queried, err)
	}
	if _, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 1}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed-reference read = %v", err)
	}
}

func TestGenericFileServerCancelsRangeWaitWithoutChangingHeldRanges(t *testing.T) {
	client, _, backend := genericServerFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("contents")); err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	a, b := genericServerSession(t, client, storage.DefaultFileSessionOptions()), genericServerSession(t, client, storage.DefaultFileSessionOptions())
	fa, _ := genericServerRetain(t, a, attr.ID, storage.AccessClaim{Uses: storage.AllAccessUses})
	fb, _ := genericServerRetain(t, b, attr.ID, storage.AccessClaim{Uses: storage.AllAccessUses})
	scope := storage.RangeScope{Enforced: true}
	snapshot, err := fa.RangeSnapshot(t.Context(), 0, scope)
	if err != nil {
		t.Fatal(err)
	}
	ranges := []storage.RangeAcquisition{{ID: 1, Start: 0, End: 2, Exclusive: true}}
	if _, err := fa.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: ranges}, genericServerAction(t, a)); err != nil {
		t.Fatal(err)
	}
	other, err := fb.RangeSnapshot(t.Context(), 0, scope)
	if err != nil {
		t.Fatal(err)
	}
	action := genericServerAction(t, b)
	finished := make(chan error, 1)
	go func() {
		_, err := fb.WaitRanges(t.Context(), storage.RangeWaitRequest{Scope: scope, ExpectedRevision: other.Revision, Ranges: ranges}, action)
		finished <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		pending, err := b.QueryAction(t.Context(), action)
		if err == nil && pending.State == storage.FileActionPending {
			break
		}
		if !errors.Is(err, syscall.ESTALE) || time.Now().After(deadline) {
			t.Fatalf("wait admission = %+v, %v", pending, err)
		}
		time.Sleep(time.Millisecond)
	}
	cancelled, err := b.CancelAction(t.Context(), action)
	if !errors.Is(err, syscall.EINTR) || cancelled.State != storage.FileActionNotApplied || cancelled.Effects != 0 {
		t.Fatalf("cancelled wait = %+v, %v", cancelled, err)
	}
	select {
	case err := <-finished:
		if !errors.Is(err, syscall.EINTR) {
			t.Fatalf("wait result = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not release the wait lane")
	}
	held, err := fa.RangeSnapshot(t.Context(), 0, scope)
	if err != nil || !reflect.DeepEqual(held.Own, ranges) {
		t.Fatalf("held ranges after cancel = %+v, %v", held, err)
	}
	bad := storage.RangeReplaceRequest{Scope: scope, ExpectedRevision: held.Revision + 1, Ranges: []storage.RangeAcquisition{}}
	result, err := fa.ReplaceRanges(t.Context(), bad, genericServerAction(t, a))
	if err == nil || result.Effects != 0 {
		t.Fatalf("stale atomic replacement = %+v, %v", result, err)
	}
	held, err = fa.RangeSnapshot(t.Context(), 0, scope)
	if err != nil || !reflect.DeepEqual(held.Own, ranges) {
		t.Fatalf("failed replacement changed ranges = %+v, %v", held, err)
	}
}

func TestGenericFileServerClaimRefusalPreservesReferenceCapacity(t *testing.T) {
	client, _, backend := genericServerFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", nil); err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	a := genericServerSession(t, client, storage.DefaultFileSessionOptions())
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	b := genericServerSession(t, client, options)
	held, _ := genericServerRetain(t, a, attr.ID, storage.AccessClaim{Uses: storage.ReadContent, Excludes: storage.WriteContent})
	request := storage.RetainRequest{NodeID: attr.ID, Claim: storage.AccessClaim{Uses: storage.WriteContent}}
	for range 3 {
		id := genericServerAction(t, b)
		result, err := b.Retain(t.Context(), request, id)
		var failure *storage.FileError
		if err == nil || !errors.As(err, &failure) || failure.Conflict == nil || failure.Conflict.Kind != storage.ConflictClaim || result.Reference != 0 {
			t.Fatalf("claim refusal = %+v, %v", result, err)
		}
		queried, queryErr := b.QueryAction(t.Context(), id)
		if storage.ErrnoOf(queryErr) != storage.ErrnoOf(err) || queried.Errno != result.Errno || queried.Effects != 0 {
			t.Fatalf("claim history = %+v, %v", queried, queryErr)
		}
	}
	if _, err := held.Close(t.Context(), genericServerAction(t, a)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Retain(t.Context(), request, genericServerAction(t, b)); err != nil {
		t.Fatalf("released claim/reference capacity = %v", err)
	}
}

func TestGenericFileServerBoundsNativeActionsAndPreservesCleanup(t *testing.T) {
	limits := DefaultFileLimits()
	limits.MaxSessions = 1
	client, _, backend := genericServerFixture(t, limits)
	if err := backend.Write(t.Context(), "file", nil); err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxActions = 1
	session := genericServerSession(t, client, options)
	if _, _, err := client.NewFileSession(t.Context(), options); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("session admission = %v", err)
	}
	file, _ := genericServerRetain(t, session, attr.ID, storage.AccessClaim{Uses: storage.AllAccessUses})
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("refused")}, genericServerAction(t, session)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("full action history = %v", err)
	}
	if body, err := backend.Read(t.Context(), "file"); err != nil || len(body) != 0 {
		t.Fatalf("refused write changed content = %q, %v", body, err)
	}
	id := genericServerAction(t, session)
	if _, err := file.Close(t.Context(), id); err != nil {
		t.Fatalf("full action history blocked reference cleanup: %v", err)
	}
	if _, err := file.Close(t.Context(), id); err != nil {
		t.Fatalf("cleanup replay = %v", err)
	}
}

type genericLostRetainBackend struct {
	storage.FileStorage
	opened chan struct{}
}

func (b *genericLostRetainBackend) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	session, status, err := b.FileStorage.NewFileSession(ctx, o)
	if err != nil {
		return nil, status, err
	}
	return &genericLostRetainSession{FileSession: session, opened: b.opened}, status, nil
}

type genericLostRetainSession struct {
	storage.FileSession
	once   sync.Once
	opened chan struct{}
}

func (s *genericLostRetainSession) Retain(ctx context.Context, r storage.RetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	result, err := s.FileSession.Retain(ctx, r, id)
	if err != nil {
		return result, err
	}
	first := false
	s.once.Do(func() { first = true; close(s.opened) })
	if first {
		<-ctx.Done()
		return storage.FileActionReceipt{}, ctx.Err()
	}
	return result, nil
}
func TestGenericFileServerCancelledRetainReconcilesOriginalAction(t *testing.T) {
	client, handler, backend := genericServerFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", nil); err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan struct{})
	handler.files.backend = &genericLostRetainBackend{FileStorage: backend, opened: opened}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session := genericServerSession(t, client, options)
	request := storage.RetainRequest{NodeID: attr.ID, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}
	action := genericServerAction(t, session)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type retainedOutcome struct {
		receipt storage.FileActionReceipt
		err     error
	}
	finished := make(chan retainedOutcome, 1)
	go func() {
		receipt, err := session.Retain(ctx, request, action)
		finished <- retainedOutcome{receipt, err}
	}()
	select {
	case <-opened:
	case <-time.After(3 * time.Second):
		t.Fatal("native retain did not complete")
	}
	cancel()
	select {
	case outcome := <-finished:
		if storage.ErrnoOf(outcome.err) != syscall.EIO || outcome.receipt.Action != action || outcome.receipt.State != storage.FileActionUnknown || outcome.receipt.Effects != 0 {
			t.Fatalf("lost retain reply claimed a known result: %+v, %v", outcome.receipt, outcome.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled retain did not return its unknown outcome")
	}
	queried, err := session.QueryAction(t.Context(), action)
	if err != nil || queried.State != storage.FileActionCompleted || queried.Reference == 0 {
		t.Fatalf("native retained receipt = %+v, %v", queried, err)
	}
	replayed, err := session.Retain(t.Context(), request, action)
	if err != nil || replayed.Reference != queried.Reference {
		t.Fatalf("retain replay = %+v, %v", replayed, err)
	}
	file, err := session.Reference(t.Context(), queried.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("recovered")}, genericServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if body, err := backend.Read(t.Context(), "file"); err != nil || string(body) != "recovered" {
		t.Fatalf("reconciled content = %q, %v", body, err)
	}
}

func TestGenericFileServerSeparatesFileAndStreamBounds(t *testing.T) {
	for _, test := range []struct {
		name        string
		log         bool
		body, frame int64
		want        syscall.Errno
	}{
		{name: "small stream frame", log: true, frame: 1024},
		{name: "insufficient receipt body", log: true, body: fileControlResponseLimit() - 1, want: syscall.EFBIG},
	} {
		t.Run(test.name, func(t *testing.T) {
			meta, backend := memoryfixture.New(t, "generic-observation", 1<<20, locking.DefaultOptions())
			options := DefaultHandlerOptions()
			if test.body != 0 {
				options.MaxBodyBytes = test.body
			}
			if test.frame != 0 {
				options.MaxFrameBytes = test.frame
			}
			var handler *Handler
			var err error
			if test.log {
				handler, err = NewHandlerWithOptions(backend, meta, options)
			} else {
				handler, err = NewHandlerWithOptions(backend, nil, options)
			}
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			t.Cleanup(func() {
				handler.Stop()
				server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := handler.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			client, err := Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			if test.want == 0 {
				if err := client.CheckFileStorage(); err != nil {
					t.Fatalf("stream frame bound blocked native file capability: %v", err)
				}
				session := genericServerSession(t, client, storage.DefaultFileSessionOptions())
				if err := backend.Write(t.Context(), "file", nil); err != nil {
					t.Fatal(err)
				}
				attr, err := backend.Stat(t.Context(), "file")
				if err != nil {
					t.Fatal(err)
				}
				file, _ := genericServerRetain(t, session, attr.ID, storage.AccessClaim{Uses: storage.AllAccessUses})
				written, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("known")}, genericServerAction(t, session))
				if err != nil || written.Effects&storage.EffectContentChanged == 0 {
					t.Fatalf("stream frame bound blocked native write: %+v, %v", written, err)
				}
				return
			}
			if err := client.CheckFileStorage(); !errors.Is(err, test.want) {
				t.Fatalf("unobservable capability = %v", err)
			}
			if _, _, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, test.want) {
				t.Fatalf("unobservable enrollment = %v", err)
			}
			handler.files.mu.Lock()
			owned := len(handler.files.sessions) + handler.files.enrolling
			handler.files.mu.Unlock()
			if owned != 0 {
				t.Fatalf("unobservable enrollment acquired %d sessions", owned)
			}
		})
	}
}

func TestGenericFileServerSessionLimitRefusalPreservesEINVAL(t *testing.T) {
	limits := DefaultFileLimits()
	limits.Session.MaxFiles = 1
	client, handler, _ := genericServerFixture(t, limits)
	requested := storage.DefaultFileSessionOptions()
	requested.MaxFiles = 2
	if _, _, err := client.NewFileSession(t.Context(), requested); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("session limit error = %v", err)
	}
	handler.files.mu.Lock()
	defer handler.files.mu.Unlock()
	if handler.files.enrolling != 0 || len(handler.files.sessions) != 0 {
		t.Fatal("invalid session options acquired ownership")
	}
}

func TestGenericFileServerReconcilesLargeAuthoritativeLocation(t *testing.T) {
	client, _, backend := genericServerFixture(t, DefaultFileLimits())
	parent := ""
	segment := strings.Repeat("&", 240)
	for range 100 {
		parent = path.Join(parent, segment)
		if err := backend.Mkdir(t.Context(), parent); err != nil {
			t.Fatal(err)
		}
	}
	attr, err := backend.Stat(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	session := genericServerSession(t, client, storage.DefaultFileSessionOptions())
	id := genericServerAction(t, session)
	observation, err := session.StatNode(t.Context(), attr.ID, storage.ObservationOptions{IncludeLocation: true})
	if err != nil || observation.Location == nil {
		t.Fatalf("large parent location = %+v, %v", observation, err)
	}
	retained, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: attr.ID, Witness: observation.Location}, id)
	if err != nil {
		t.Fatal(err)
	}
	queried, err := session.QueryAction(t.Context(), id)
	if err != nil || queried.Reference != retained.Reference || !reflect.DeepEqual(queried.Observation.Location, retained.Observation.Location) || queried.Observation.Location == nil {
		t.Fatalf("large location reconciliation = reference %d, location %#v, %v", queried.Reference, queried.Observation.Location, err)
	}
	if len(queried.Observation.Location.Ancestors) != 100 {
		t.Fatalf("location depth = %d", len(queried.Observation.Location.Ancestors))
	}
}

func TestGenericFileServerControlsRemainAvailableUnderDataAndWaitPressure(t *testing.T) {
	client, handler, _ := genericServerFixture(t, DefaultFileLimits())
	session := genericServerSession(t, client, storage.DefaultFileSessionOptions())
	for _, admission := range []*bodyAdmission{client.fileRequests, client.fileWaits, handler.responses, handler.fileWaits} {
		release, err := admission.acquire(t.Context(), admission.maxBytes)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	status, err := session.Status(ctx)
	if err != nil || status.Retired || status.Remaining <= 0 {
		t.Fatalf("independent status = %+v, %v", status, err)
	}
	id, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Close(ctx, id); err != nil {
		t.Fatalf("independent cleanup = %v", err)
	}
}

func TestGenericFileServerReusesOnlyRetiredReferenceCapacity(t *testing.T) {
	client, _, backend := genericServerFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", nil); err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session := genericServerSession(t, client, options)
	file, first := genericServerRetain(t, session, attr.ID, storage.AccessClaim{Uses: storage.AllAccessUses})
	request := storage.RetainRequest{NodeID: attr.ID, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}
	if _, err := session.Retain(t.Context(), request, genericServerAction(t, session)); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("reference capacity = %v", err)
	}
	if _, err := file.Close(t.Context(), genericServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	_, second := genericServerRetain(t, session, attr.ID, storage.AccessClaim{Uses: storage.AllAccessUses})
	if second.Reference == first.Reference || second.Observation.Attr.ID != first.Observation.Attr.ID {
		t.Fatal("retired reference identity was reused")
	}
	if _, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 1}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("retired identity reached replacement = %v", err)
	}
}

func TestGenericFileServerRejectsInconsistentNativeActionOutcome(t *testing.T) {
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	result := storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionCompleted, Errno: syscall.EACCES}
	response, err := fileReceiptResponse(fileRequest{Op: storage.OpFileWrite, Action: id}, result, nil)
	if storage.ErrnoOf(err) != syscall.EIO || errors.Is(err, syscall.EACCES) || response.Receipt != nil {
		t.Fatalf("inconsistent native outcome = %+v, %v", response, err)
	}
}

type genericCapabilityProbe struct {
	storage.FileStorage
	calls int
}

func (p *genericCapabilityProbe) CheckFileStorage() error { p.calls++; return syscall.EIO }

func TestGenericFileServerRejectsInitialNanosecondsBeforeNativeDispatch(t *testing.T) {
	_, handler, backend := genericServerFixture(t, DefaultFileLimits())
	probe := &genericCapabilityProbe{FileStorage: backend}
	handler.files.backend = probe
	metadata, err := storage.EncodeMetadata(nil)
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"access", "modification", "creation", "change"} {
		for _, nanos := range []int32{-1, 1e9} {
			t.Run(field+"/"+time.Duration(nanos).String(), func(t *testing.T) {
				initial := fileInitial{Kind: storage.NodeRegular, Metadata: metadata, LinkTarget: []byte{}}
				instant := &Time{Nanos: nanos}
				switch field {
				case "access":
					initial.AccessTime = instant
				case "modification":
					initial.ModTime = instant
				case "creation":
					initial.CreationTime = instant
				case "change":
					initial.ChangeTime = instant
				}
				operand := fileRequest{Op: storage.OpFileCreateAndRetainAt, Session: strings.Repeat("a", 64), Action: action, Create: &fileCreateRequest{Target: fileEntryTarget{Parent: 1, ParentID: 1, Name: []byte("file"), DirectoryRevision: 1, Witness: &storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 1, NodeID: 1}}, Initial: initial}}
				body, err := marshalFileJSON(operand)
				if err != nil {
					t.Fatal(err)
				}
				request := httptest.NewRequest(http.MethodPost, Prefix+string(OpFile), bytes.NewReader(body))
				request.Header.Set("Content-Type", contentJSON)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusBadRequest {
					t.Fatalf("invalid nanoseconds status/body = %d %s", response.Code, response.Body.String())
				}
				if probe.calls != 0 {
					t.Fatalf("invalid nanoseconds reached native dispatch %d times", probe.calls)
				}
			})
		}
	}
}

func TestGenericFileServerWithoutLogKeepsNativeOperationsAndReceipts(t *testing.T) {
	_, backend := memoryfixture.New(t, "generic-no-log", 1<<20, locking.DefaultOptions())
	if err := backend.Write(t.Context(), "file", nil); err != nil {
		t.Fatal(err)
	}
	options := DefaultHandlerOptions()
	options.MaxFrameBytes = 1024
	handler, err := NewHandlerWithOptions(backend, nil, options)
	if err != nil {
		t.Fatal(err)
	}
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
	state, err := client.FileState(t.Context())
	if err != nil || state.RootID == 0 {
		t.Fatalf("file capability without log = %+v, %v", state, err)
	}
	session := genericServerSession(t, client, storage.DefaultFileSessionOptions())
	enhanced := session.(FileSessionWithBarrier)
	rootReceipt, barrier, err := enhanced.RetainWithBarrier(t.Context(), storage.RetainRequest{NodeID: state.RootID}, genericServerAction(t, session))
	if err != nil || barrier != nil || rootReceipt.Reference == 0 {
		t.Fatalf("root retain without log = %+v, %+v, %v", rootReceipt, barrier, err)
	}
	root, err := session.Reference(t.Context(), rootReceipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := root.LookupAt(t.Context(), []byte("file"))
	if err != nil || !lookup.Found {
		t.Fatalf("lookup without log = %+v, %v", lookup, err)
	}
	retained, barrier, err := enhanced.RetainWithBarrier(t.Context(), storage.RetainRequest{NodeID: lookup.Attr.ID, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, genericServerAction(t, session))
	if err != nil || barrier != nil || retained.Reference == 0 {
		t.Fatalf("retain without log = %+v, %+v, %v", retained, barrier, err)
	}
	file, err := session.Reference(t.Context(), retained.Reference)
	if err != nil {
		t.Fatal(err)
	}
	action := genericServerAction(t, session)
	written, barrier, err := file.(FileWithBarrier).WriteAtWithBarrier(t.Context(), storage.FileWriteRequest{Data: []byte("known")}, action)
	if err != nil || barrier != nil || written.Effects&storage.EffectContentChanged == 0 {
		t.Fatalf("write without log = %+v, %+v, %v", written, barrier, err)
	}
	if _, barrier, err := file.(FileWithBarrier).CloseWithBarrier(t.Context(), genericServerAction(t, session)); err != nil || barrier != nil {
		t.Fatalf("close without log = %+v, %v", barrier, err)
	}
	queried, barrier, err := enhanced.QueryActionWithBarrier(t.Context(), action)
	if err != nil || barrier != nil || queried.Action != action || queried.Observation.Attr.Size != 5 {
		t.Fatalf("history without log = %+v, %+v, %v", queried, barrier, err)
	}
	if _, barrier, err := enhanced.CloseWithBarrier(t.Context(), genericServerAction(t, session)); err != nil || barrier != nil {
		t.Fatalf("session close without log = %+v, %v", barrier, err)
	}
	if body, err := backend.Read(t.Context(), "file"); err != nil || string(body) != "known" {
		t.Fatalf("authoritative content without log = %q, %v", body, err)
	}
}
