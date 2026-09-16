package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

type fileAuthorizationIdentity struct{}

type fileAuthorizationPolicy struct {
	mu         sync.Mutex
	err        error
	requests   []authz.AccessRequest
	identities []any
}

func (p *fileAuthorizationPolicy) Authorize(ctx context.Context, request authz.AccessRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, request)
	p.identities = append(p.identities, ctx.Value(fileAuthorizationIdentity{}))
	return p.err
}

func (p *fileAuthorizationPolicy) reset(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
	p.requests = nil
	p.identities = nil
}

func fileAuthorizationFixture(t *testing.T, policy *fileAuthorizationPolicy, limits FileLimits) (*Handler, *objectstore.Storage) {
	t.Helper()
	meta, backend := memoryfixture.New(t, "backend-name-is-not-authority", 1<<20, locking.DefaultOptions())
	options := DefaultHandlerOptions()
	options.Volume = "trusted-volume"
	options.Authorizer = policy
	options.Files = limits
	h, err := NewHandlerWithOptions(backend, meta, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return h, backend
}

func fileAuthorizationRequest(t *testing.T, h *Handler, request fileRequest) *httptest.ResponseRecorder {
	t.Helper()
	return fileAuthorizationRequestContext(t, h, context.WithValue(t.Context(), fileAuthorizationIdentity{}, "member"), request)
}

func fileAuthorizationRequestContext(t *testing.T, h *Handler, ctx context.Context, request fileRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := marshalFileJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	op := OpFile
	if fileControl(request.Op) {
		op = OpFileControl
	}
	r := httptest.NewRequest(http.MethodPost, Prefix+string(op), bytes.NewReader(body))
	r.Header.Set("Content-Type", contentJSON)
	r = r.WithContext(ctx)
	answer := httptest.NewRecorder()
	h.ServeHTTP(answer, r)
	return answer
}

func fileAuthorizationSuccess(t *testing.T, h *Handler, request fileRequest) fileResponse {
	t.Helper()
	answer := fileAuthorizationRequest(t, h, request)
	if answer.Code != http.StatusOK {
		t.Fatalf("file %s returned %d: %s", request.Op, answer.Code, answer.Body.String())
	}
	var response fileResponse
	if err := json.Unmarshal(answer.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func fileAuthorizationDenied(t *testing.T, answer *httptest.ResponseRecorder, errno, message string) {
	t.Helper()
	if answer.Code != StatusStorageError {
		t.Fatalf("authorization status=%d body=%s", answer.Code, answer.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(answer.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response) != 2 || response["errno"] != errno || response["message"] != message {
		t.Fatalf("authorization exposed a capability or native receipt: %+v", response)
	}
}

func fileAuthorizationID(t *testing.T, epoch uint64) storage.FileActionID {
	t.Helper()
	id, err := storage.NewFileActionID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type fileAuthorizationCase struct {
	request fileRequest
	access  authz.AccessRequest
}

func fileAuthorizationCases(t *testing.T) []fileAuthorizationCase {
	t.Helper()
	options := storage.DefaultFileSessionOptions()
	root := storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 1, NodeID: 1}
	entry := storage.EntryCondition{ParentID: 1, DirectoryRevision: 1, EntryID: 3, NodeID: 7, Name: []byte("file")}
	location := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 7, Ancestors: []storage.EntryCondition{entry}}
	target := storage.EntryTarget{Parent: 2, ParentID: 1, Name: []byte("file"), DirectoryRevision: 1, ExpectedEntryID: 3, ExpectedNodeID: 7, ExpectedMetadataRevision: 1, Witness: &root}
	absent := target
	absent.Name, absent.ExpectedEntryID, absent.ExpectedNodeID, absent.ExpectedMetadataRevision = []byte("new"), 0, 0, 0
	claim := storage.AccessClaim{Uses: storage.AllAccessUses, Excludes: storage.RemoveEntry}
	prepared := storage.RemovalFile
	intent := storage.RemovalIntentID(1)
	owner := storage.RangeOwnerID(13)
	scope := storage.RangeScope{Domain: 9}
	metadata := []byte{'R', 'F', 'M', 1, 0, 0}
	initial := fileInitial{Kind: storage.NodeRegular, Metadata: metadata, LinkTarget: []byte{}}
	const retain = storage.EffectRetained | storage.EffectClaimChanged | storage.EffectPreparedChanged
	const content = storage.EffectContentChanged | storage.EffectMetadataChanged
	const cleanup = storage.EffectReferenceRetired | storage.EffectClaimChanged | storage.EffectRangesChanged | storage.EffectPreparedChanged | storage.EffectDrainChanged | storage.EffectEntryDetached
	cases := []fileAuthorizationCase{
		{fileRequest{Op: storage.OpFileState}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileSessionOpen, Options: &options}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileStatus}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileRenew}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileSessionClose}, authz.AccessRequest{Effects: cleanup}},
		{fileRequest{Op: storage.OpFileStatNode, Node: 7, Observation: &storage.ObservationOptions{}}, authz.AccessRequest{Node: 7}},
		{fileRequest{Op: storage.OpFileSetNodeAttr, Node: 7, Change: &AttrChange{}}, authz.AccessRequest{Node: 7, Effects: storage.EffectMetadataChanged}},
		{fileRequest{Op: storage.OpFileRetain, Retain: &fileRetainRequest{NodeID: 7, ExpectedMetadataRevision: 1, Claim: claim, Witness: &location, Prepared: &prepared}}, authz.AccessRequest{Node: 7, Claim: claim, Effects: retain}},
		{fileRequest{Op: storage.OpFileRetainAt, RetainAt: &fileRetainAtRequest{Target: fileTargetOf(target), Claim: claim, Prepared: &prepared}}, authz.AccessRequest{Parent: 2, Claim: claim, Effects: retain}},
		{fileRequest{Op: storage.OpFileCreateAndRetainAt, Create: &fileCreateRequest{Target: fileTargetOf(absent), Initial: initial, Claim: claim, Prepared: &prepared}}, authz.AccessRequest{Parent: 2, Claim: claim, Effects: retain | storage.EffectCreated}},
		{fileRequest{Op: storage.OpFileReplaceAndRetainAt, Create: &fileCreateRequest{Target: fileTargetOf(target), Initial: initial, Claim: claim, Prepared: &prepared}}, authz.AccessRequest{Parent: 2, Claim: claim, Effects: retain | storage.EffectCreated | storage.EffectEntryDetached}},
		{fileRequest{Op: storage.OpFileResetAndRetainAt, Reset: &fileResetRequest{Target: fileTargetOf(target), ExpectedRevision: 1, Change: AttrChange{}, Claim: claim, Prepared: &prepared}}, authz.AccessRequest{Parent: 2, Claim: claim, Effects: retain | content}},
		{fileRequest{Op: storage.OpFileReference, Reference: 5}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileStat, Reference: 5, Observation: &storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileCheckObservation, Reference: 5, Check: &storage.ObservationCondition{MetadataRevision: 1, Location: location}}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileRead, Reference: 5, Read: &fileReadRequest{Length: 8}}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileWrite, Reference: 5, Write: &fileWriteRequest{Data: []byte("patch")}}, authz.AccessRequest{Effects: content}},
		{fileRequest{Op: storage.OpFileTruncate, Reference: 5, Truncate: &fileTruncateRequest{Size: 2}}, authz.AccessRequest{Effects: content}},
		{fileRequest{Op: storage.OpFileSetAttr, Reference: 5, Change: &AttrChange{}}, authz.AccessRequest{Effects: storage.EffectMetadataChanged}},
		{fileRequest{Op: storage.OpFileSetKind, Reference: 5, Kind: &fileKindRequest{ExpectedRevision: 1, Kind: storage.NodeSymlink, LinkTarget: []byte("target"), Metadata: metadata}}, authz.AccessRequest{Effects: content}},
		{fileRequest{Op: storage.OpFileListAt, Reference: 5, List: &storage.DirectoryPageRequest{MaxEntries: 8, MaxBytes: 4096}}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileLookupAt, Reference: 5, Name: []byte{255, 'x'}}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileRename, Reference: 5, Rename: fileRenameOf(storage.RenameRequest{Source: target, Destination: absent})}, authz.AccessRequest{Parent: 2, Destination: 2, Effects: storage.EffectEntryMoved | storage.EffectEntryDetached}},
		{fileRequest{Op: storage.OpFileReplaceClaim, Reference: 5, Claim: &claim}, authz.AccessRequest{Claim: claim, Effects: storage.EffectClaimChanged}},
		{fileRequest{Op: storage.OpFilePrepareRemoval, Reference: 5, Prepare: &storage.PrepareRemovalRequest{ExpectedMetadataRevision: 1, Entry: entry, Witness: location, Condition: prepared}}, authz.AccessRequest{Effects: storage.EffectPreparedChanged}},
		{fileRequest{Op: storage.OpFileCancelPrepared, Reference: 5, Intent: &intent}, authz.AccessRequest{Effects: storage.EffectPreparedChanged}},
		{fileRequest{Op: storage.OpFileDrainEntry, Reference: 5, Drain: &storage.DrainEntryRequest{ExpectedMetadataRevision: 1, Entry: entry, Witness: location, Condition: prepared}}, authz.AccessRequest{Effects: storage.EffectDrainChanged}},
		{fileRequest{Op: storage.OpFileCancelDrain, Reference: 5, CancelDrain: &storage.CancelDrainRequest{EntryID: 3, Generation: 1}}, authz.AccessRequest{Effects: storage.EffectDrainChanged}},
		{fileRequest{Op: storage.OpFileRangeSnapshot, Reference: 5, Owner: &owner, Scope: &scope}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileReplaceRanges, Reference: 5, Ranges: &storage.RangeReplaceRequest{Owner: owner, Scope: scope, ExpectedRevision: 1, Ranges: []storage.RangeAcquisition{{ID: 1, End: math.MaxUint64, Exclusive: true}}}}, authz.AccessRequest{Effects: storage.EffectRangesChanged}},
		{fileRequest{Op: storage.OpFileWaitRanges, Reference: 5, Wait: &storage.RangeWaitRequest{Owner: owner, Scope: scope, ExpectedRevision: 1, Ranges: []storage.RangeAcquisition{{ID: 1, End: 9, Exclusive: true}}, DetectDeadlock: true}}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileRetireRanges, Reference: 5, Owner: &owner, Scope: &scope}, authz.AccessRequest{Effects: storage.EffectRangesChanged}},
		{fileRequest{Op: storage.OpFileRetireRangeOwner, Owner: &owner}, authz.AccessRequest{Effects: storage.EffectRangesChanged}},
		{fileRequest{Op: storage.OpFileSync, Reference: 5}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileClose, Reference: 5}, authz.AccessRequest{Effects: cleanup}},
		{fileRequest{Op: storage.OpFileQueryAction}, authz.AccessRequest{}},
		{fileRequest{Op: storage.OpFileCancelAction}, authz.AccessRequest{}},
	}
	for i := range cases {
		c := &cases[i]
		if c.request.Op != storage.OpFileState && c.request.Op != storage.OpFileSessionOpen {
			c.request.Session = strings.Repeat("a", 64)
		}
		if fileActionRequired(c.request.Op) {
			c.request.Action = fileAuthorizationID(t, 1)
		}
		c.access.Volume, c.access.Operation, c.access.Reference = "trusted-volume", c.request.Op, c.request.Reference
	}
	return cases
}

func TestEveryFileOperationAuthorizesBeforeCapabilityLookup(t *testing.T) {
	policy := &fileAuthorizationPolicy{err: authz.ErrDenied}
	h, _ := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	for _, c := range fileAuthorizationCases(t) {
		t.Run(string(c.request.Op), func(t *testing.T) {
			policy.reset(authz.ErrDenied)
			fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, c.request), "EACCES", "access denied")
			policy.mu.Lock()
			defer policy.mu.Unlock()
			if len(policy.requests) != 1 || policy.requests[0] != c.access || len(policy.identities) != 1 || policy.identities[0] != "member" {
				t.Fatalf("authorization=%+v identities=%v; want %+v", policy.requests, policy.identities, c.access)
			}
		})
	}
	h.files.mu.Lock()
	defer h.files.mu.Unlock()
	if len(h.files.sessions) != 0 || h.files.enrolling != 0 || h.files.running {
		t.Fatal("denied file requests allocated or started the registry")
	}
}

func TestInvalidFileArgumentsDoNotReachAuthorization(t *testing.T) {
	policy := &fileAuthorizationPolicy{err: authz.ErrDenied}
	h, _ := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	valid := map[storage.Operation]fileRequest{}
	for _, c := range fileAuthorizationCases(t) {
		valid[c.request.Op] = c.request
	}
	for _, c := range []struct {
		name   string
		op     storage.Operation
		change func(*fileRequest)
	}{
		{"missing options", storage.OpFileSessionOpen, func(r *fileRequest) { r.Options = nil }},
		{"oversized options", storage.OpFileSessionOpen, func(r *fileRequest) { o := *r.Options; o.MaxFiles++; r.Options = &o }},
		{"missing retain", storage.OpFileRetain, func(r *fileRequest) { r.Retain = nil }},
		{"invalid claim", storage.OpFileRetain, func(r *fileRequest) {
			r.Retain = &fileRetainRequest{NodeID: 7, Claim: storage.AccessClaim{Uses: 1 << 63}}
		}},
		{"escaping entry", storage.OpFileRetainAt, func(r *fileRequest) { v := *r.RetainAt; v.Target.Name = []byte("../outside"); r.RetainAt = &v }},
		{"create existing entry", storage.OpFileCreateAndRetainAt, func(r *fileRequest) {
			v := *r.Create
			v.Target.ExpectedEntryID, v.Target.ExpectedNodeID = 3, 7
			r.Create = &v
		}},
		{"missing node", storage.OpFileStatNode, func(r *fileRequest) { r.Node = 0 }},
		{"negative read", storage.OpFileRead, func(r *fileRequest) { r.Read = &fileReadRequest{Offset: -1, Length: 1} }},
		{"overflow write", storage.OpFileWrite, func(r *fileRequest) { r.Write = &fileWriteRequest{Offset: math.MaxInt64, Data: []byte("x")} }},
		{"negative truncate", storage.OpFileTruncate, func(r *fileRequest) { r.Truncate = &fileTruncateRequest{Size: -1} }},
		{"invalid metadata", storage.OpFileSetAttr, func(r *fileRequest) {
			b := []byte("invalid")
			r.Change = &AttrChange{ExpectedRevision: 1, Metadata: &b}
		}},
		{"invalid range scope", storage.OpFileRangeSnapshot, func(r *fileRequest) { r.Scope = &storage.RangeScope{Domain: 1, Enforced: true} }},
		{"invalid action", storage.OpFileReplaceRanges, func(r *fileRequest) { r.Action = "invalid" }},
		{"reversed range", storage.OpFileReplaceRanges, func(r *fileRequest) {
			v := *r.Ranges
			v.Ranges = []storage.RangeAcquisition{{ID: 1, Start: 2, End: 1}}
			r.Ranges = &v
		}},
		{"missing range revision", storage.OpFileReplaceRanges, func(r *fileRequest) { v := *r.Ranges; v.ExpectedRevision = 0; r.Ranges = &v }},
		{"invalid query action", storage.OpFileQueryAction, func(r *fileRequest) { r.Action = "invalid" }},
		{"missing range owner", storage.OpFileRetireRangeOwner, func(r *fileRequest) { r.Owner = nil }},
		{"unknown operation", storage.OpFileStatus, func(r *fileRequest) { r.Op = "unknown" }},
		{"unqualified operation", storage.OpFileRetain, func(r *fileRequest) { r.Op = "retain" }},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := valid[c.op]
			c.change(&r)
			answer := fileAuthorizationRequest(t, h, r)
			if answer.Code == http.StatusOK || strings.Contains(answer.Body.String(), "access denied") {
				t.Fatalf("invalid %s bypassed parameter validation: %d %s", r.Op, answer.Code, answer.Body)
			}
		})
	}
	policy.mu.Lock()
	defer policy.mu.Unlock()
	if len(policy.requests) != 0 {
		t.Fatalf("invalid requests reached policy: %+v", policy.requests)
	}
}

func TestFileAuthorizationUsesTrustedFailureFieldsWithoutNativeReceipts(t *testing.T) {
	secret := "private policy details"
	for _, c := range []struct {
		cause          error
		errno, message string
	}{
		{errors.Join(authz.ErrDenied, &locking.Error{Code: locking.Conflict, Recorded: true, Message: secret}), "EACCES", "access denied"},
		{fmt.Errorf("%s: %w", secret, syscall.ENOSPC), "EIO", "authorization failed"},
	} {
		policy := &fileAuthorizationPolicy{err: c.cause}
		h, _ := fileAuthorizationFixture(t, policy, DefaultFileLimits())
		for _, candidate := range fileAuthorizationCases(t) {
			if candidate.request.Op == storage.OpFileSessionOpen || candidate.request.Op == storage.OpFileStatus {
				fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, candidate.request), c.errno, c.message)
			}
		}
	}
}

func fileAuthorizationRetain(t *testing.T, h *Handler, backend *objectstore.Storage, options storage.FileSessionOptions, uses storage.AccessUse) (fileResponse, fileRequest, fileResponse, *servedFileSession) {
	t.Helper()
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session := fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileSessionOpen, Options: &options})
	request := fileRequest{Op: storage.OpFileRetain, Session: session.Session, Action: fileAuthorizationID(t, session.Status.ActionEpoch), Retain: &fileRetainRequest{NodeID: attr.ID, ExpectedMetadataRevision: attr.MetadataRevision, Claim: storage.AccessClaim{Uses: uses}}}
	opened := fileAuthorizationSuccess(t, h, request)
	if opened.Receipt == nil || opened.Receipt.Reference == 0 {
		t.Fatalf("retain did not return reference: %+v", opened)
	}
	h.files.mu.Lock()
	served := h.files.sessions[session.Session]
	h.files.mu.Unlock()
	return session, request, opened, served
}

func fileAuthorizationSameReceipt(t *testing.T, native storage.FileSession, action storage.FileActionID, before storage.FileActionReceipt) {
	t.Helper()
	after, err := native.QueryAction(t.Context(), action)
	if err != nil {
		t.Fatal(err)
	}
	before.HistoryRemaining, after.HistoryRemaining = 0, 0
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("denial changed receipt: %+v; want %+v", after, before)
	}
}

func TestDeniedFileActionsDoNotMutateOrExposeRetainedReceipts(t *testing.T) {
	policy := &fileAuthorizationPolicy{}
	h, backend := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	session, request, opened, served := fileAuthorizationRetain(t, h, backend, storage.DefaultFileSessionOptions(), storage.ReadContent|storage.WriteContent)
	reference := opened.Receipt.Reference
	before, err := served.native.QueryAction(t.Context(), request.Action)
	if err != nil {
		t.Fatal(err)
	}
	served.mu.Lock()
	expires, revision := served.expires, served.revision
	served.mu.Unlock()
	policy.reset(authz.ErrDenied)
	for _, denied := range []fileRequest{
		request,
		{Op: storage.OpFileReference, Session: session.Session, Reference: reference},
		{Op: storage.OpFileRenew, Session: session.Session},
		{Op: storage.OpFileStatus, Session: session.Session},
		{Op: storage.OpFileClose, Session: session.Session, Reference: reference, Action: fileAuthorizationID(t, session.Status.ActionEpoch)},
		{Op: storage.OpFileSessionClose, Session: session.Session, Action: fileAuthorizationID(t, session.Status.ActionEpoch)},
	} {
		fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, denied), "EACCES", "access denied")
	}
	served.mu.Lock()
	unchanged := !served.retired && served.revision == revision && served.expires.Equal(expires)
	served.mu.Unlock()
	if !unchanged {
		t.Fatal("denial changed session lifetime")
	}
	fileAuthorizationSameReceipt(t, served.native, request.Action, before)
	if _, err := served.native.Reference(t.Context(), reference); err != nil {
		t.Fatalf("denial released reference: %v", err)
	}
	policy.reset(nil)
	replayed := fileAuthorizationSuccess(t, h, request)
	if replayed.Receipt.Reference != reference {
		t.Fatalf("allowed replay allocated replacement: %+v", replayed)
	}
	write := fileRequest{Op: storage.OpFileWrite, Session: session.Session, Reference: reference, Action: fileAuthorizationID(t, session.Status.ActionEpoch), Write: &fileWriteRequest{Data: []byte("altered!")}}
	policy.reset(authz.ErrDenied)
	fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, write), "EACCES", "access denied")
	if receipt, err := served.native.QueryAction(t.Context(), write.Action); !errors.Is(err, syscall.ESTALE) || receipt.State != 0 {
		t.Fatalf("denied write reserved receipt: %+v %v", receipt, err)
	}
	content, err := backend.Read(t.Context(), "file")
	if err != nil || string(content) != "original" {
		t.Fatalf("denied write changed content: %q %v", content, err)
	}
	policy.reset(nil)
	fileAuthorizationSuccess(t, h, write)
	content, err = backend.Read(t.Context(), "file")
	if err != nil || string(content) != "altered!" {
		t.Fatalf("allowed retry did not execute original action: %q %v", content, err)
	}
}

func TestDeniedAdvisoryCleanupPreservesTheActualGrantAndActionHistory(t *testing.T) {
	policy := &fileAuthorizationPolicy{}
	h, backend := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	session, _, opened, served := fileAuthorizationRetain(t, h, backend, storage.DefaultFileSessionOptions(), storage.ReadContent)
	reference := opened.Receipt.Reference
	owner := storage.RangeOwnerID(17)
	scope := storage.RangeScope{Domain: 9}
	snapshotRequest := fileRequest{Op: storage.OpFileRangeSnapshot, Session: session.Session, Reference: reference, Owner: &owner, Scope: &scope}
	snapshot := fileAuthorizationSuccess(t, h, snapshotRequest)
	grant := storage.RangeAcquisition{ID: 1, End: math.MaxUint64, Exclusive: true}
	acquire := fileRequest{Op: storage.OpFileReplaceRanges, Session: session.Session, Reference: reference, Action: fileAuthorizationID(t, session.Status.ActionEpoch), Ranges: &storage.RangeReplaceRequest{Owner: owner, Scope: scope, ExpectedRevision: snapshot.Ranges.Revision, Ranges: []storage.RangeAcquisition{grant}}}
	granted := fileAuthorizationSuccess(t, h, acquire)
	if granted.Receipt == nil || granted.Receipt.State != storage.FileActionCompleted || granted.Receipt.Effects&storage.EffectRangesChanged == 0 {
		t.Fatalf("range acquisition failed: %+v", granted)
	}
	before, err := served.native.QueryAction(t.Context(), acquire.Action)
	if err != nil {
		t.Fatal(err)
	}
	unlock := fileRequest{Op: storage.OpFileReplaceRanges, Session: session.Session, Reference: reference, Action: fileAuthorizationID(t, session.Status.ActionEpoch), Ranges: &storage.RangeReplaceRequest{Owner: owner, Scope: scope, ExpectedRevision: granted.Receipt.RangeRevision, Ranges: []storage.RangeAcquisition{}}}
	retire := fileRequest{Op: storage.OpFileRetireRanges, Session: session.Session, Reference: reference, Action: fileAuthorizationID(t, session.Status.ActionEpoch), Owner: &owner, Scope: &scope}
	policy.reset(authz.ErrDenied)
	for _, request := range []fileRequest{
		acquire,
		{Op: storage.OpFileQueryAction, Session: session.Session, Action: acquire.Action},
		{Op: storage.OpFileCancelAction, Session: session.Session, Action: acquire.Action},
		unlock, retire,
	} {
		fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, request), "EACCES", "access denied")
	}
	policy.reset(nil)
	other := storage.RangeOwnerID(18)
	snapshotRequest.Owner = &other
	conflict := fileAuthorizationSuccess(t, h, snapshotRequest)
	if len(conflict.Ranges.Other) != 1 || conflict.Ranges.Other[0].Owner.ID != owner || conflict.Ranges.Other[0].Range != grant {
		t.Fatalf("denied cleanup released actual range: %+v", conflict.Ranges)
	}
	fileAuthorizationSameReceipt(t, served.native, acquire.Action, before)
	query := fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileQueryAction, Session: session.Session, Action: acquire.Action})
	if query.Receipt == nil || query.Receipt.State != storage.FileActionCompleted || query.Receipt.RangeRevision != granted.Receipt.RangeRevision {
		t.Fatalf("denial changed grant history: %+v", query.Receipt)
	}
	for _, id := range []storage.FileActionID{unlock.Action, retire.Action} {
		if receipt, err := served.native.QueryAction(t.Context(), id); !errors.Is(err, syscall.ESTALE) || receipt.State != 0 {
			t.Fatalf("denied cleanup reserved history: %+v %v", receipt, err)
		}
	}
}

func TestDeniedFileCloseStillAllowsInternalLeaseCleanup(t *testing.T) {
	policy := &fileAuthorizationPolicy{}
	limits := DefaultFileLimits()
	limits.Session.Lease = 500 * time.Millisecond
	h, backend := fileAuthorizationFixture(t, policy, limits)
	if err := backend.Write(t.Context(), "file", []byte("retained")); err != nil {
		t.Fatal(err)
	}
	session, _, opened, _ := fileAuthorizationRetain(t, h, backend, limits.Session, storage.ReadContent)
	if err := backend.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	policy.reset(authz.ErrDenied)
	fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, fileRequest{Op: storage.OpFileClose, Session: session.Session, Reference: opened.Receipt.Reference, Action: fileAuthorizationID(t, session.Status.ActionEpoch)}), "EACCES", "access denied")
	if used, err := backend.Usage(t.Context()); err != nil || used != 8 {
		t.Fatalf("denied close released retained bytes: %d %v", used, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		used, err := backend.Usage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if used == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease expiry retained %d bytes after close authorization was revoked", used)
		}
		time.Sleep(10 * time.Millisecond)
	}
	policy.mu.Lock()
	defer policy.mu.Unlock()
	if len(policy.requests) != 1 || policy.requests[0].Operation != storage.OpFileClose {
		t.Fatalf("internal cleanup asked for caller authorization: %+v", policy.requests)
	}
}

type fileAuthorizationBackend struct {
	storage.FileStorage
	calls int
}

func (b *fileAuthorizationBackend) CheckFileStorage() error {
	b.calls++
	return b.FileStorage.CheckFileStorage()
}
func (b *fileAuthorizationBackend) FileState(ctx context.Context) (storage.FileVolumeState, error) {
	b.calls++
	return b.FileStorage.FileState(ctx)
}
func (b *fileAuthorizationBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	b.calls++
	return b.FileStorage.NewFileSession(ctx, options)
}

func TestEveryRetainedOperationAuthorizesBeforeNativeStateOrCapabilityLookup(t *testing.T) {
	policy := &fileAuthorizationPolicy{err: authz.ErrDenied}
	h, backend := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	probe := &fileAuthorizationBackend{FileStorage: backend}
	h.files.backend = probe
	identity := &struct{ name string }{name: "member"}
	ctx := context.WithValue(t.Context(), fileAuthorizationIdentity{}, identity)
	for _, c := range fileAuthorizationCases(t) {
		t.Run(string(c.request.Op), func(t *testing.T) {
			policy.reset(authz.ErrDenied)
			fileAuthorizationDenied(t, fileAuthorizationRequestContext(t, h, ctx, c.request), "EACCES", "access denied")
			policy.mu.Lock()
			defer policy.mu.Unlock()
			if len(policy.requests) != 1 || policy.requests[0] != c.access || len(policy.identities) != 1 || policy.identities[0] != identity {
				t.Fatalf("authorization=%+v identities=%v; want %+v and original identity", policy.requests, policy.identities, c.access)
			}
		})
	}
	if probe.calls != 0 {
		t.Fatalf("denied requests made %d native calls", probe.calls)
	}
	h.files.mu.Lock()
	if len(h.files.sessions) != 0 || h.files.enrolling != 0 || h.files.running {
		t.Error("denied requests allocated or started registry")
	}
	h.files.mu.Unlock()
	if err := ctx.Err(); err != nil {
		t.Fatalf("request completion cancelled host context: %v", err)
	}
}

func TestFileAuthorizationRevocationPreservesReferencesReceiptsAndContent(t *testing.T) {
	policy := &fileAuthorizationPolicy{}
	h, backend := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	session, open, opened, served := fileAuthorizationRetain(t, h, backend, storage.DefaultFileSessionOptions(), storage.ReadContent|storage.WriteContent)
	reference := opened.Receipt.Reference
	before, err := served.native.QueryAction(t.Context(), open.Action)
	if err != nil {
		t.Fatal(err)
	}
	served.mu.Lock()
	expires, revision := served.expires, served.revision
	served.mu.Unlock()
	write := fileRequest{Op: storage.OpFileWrite, Session: session.Session, Reference: reference, Action: fileAuthorizationID(t, session.Status.ActionEpoch), Write: &fileWriteRequest{Data: []byte("altered!")}}
	requests := fileAuthorizationCases(t)
	for i := range requests {
		r := &requests[i].request
		if r.Session != "" {
			r.Session = session.Session
		}
		if r.Reference != 0 {
			r.Reference = reference
		}
		switch r.Op {
		case storage.OpFileRetain:
			*r = open
		case storage.OpFileWrite:
			*r = write
		case storage.OpFileQueryAction, storage.OpFileCancelAction:
			r.Action = open.Action
		default:
			if r.Action != "" {
				r.Action = fileAuthorizationID(t, session.Status.ActionEpoch)
			}
		}
	}
	policy.reset(authz.ErrDenied)
	for _, c := range requests {
		fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, c.request), "EACCES", "access denied")
	}
	policy.mu.Lock()
	calls := len(policy.requests)
	policy.mu.Unlock()
	if calls != len(requests) {
		t.Fatalf("revoked requests consulted current policy %d times, want %d", calls, len(requests))
	}
	served.mu.Lock()
	unchanged := !served.retired && served.expires.Equal(expires) && served.revision == revision
	served.mu.Unlock()
	if !unchanged {
		t.Fatal("denial changed retained session or lease")
	}
	fileAuthorizationSameReceipt(t, served.native, open.Action, before)
	if _, err := served.native.Reference(t.Context(), reference); err != nil {
		t.Fatalf("denial released retained reference: %v", err)
	}
	for _, c := range requests {
		if c.request.Action != "" && c.request.Action != open.Action {
			if receipt, err := served.native.QueryAction(t.Context(), c.request.Action); !errors.Is(err, syscall.ESTALE) || receipt.State != 0 {
				t.Fatalf("denied %s retained history: %+v %v", c.request.Op, receipt, err)
			}
		}
	}
	content, err := backend.Read(t.Context(), "file")
	if err != nil || string(content) != "original" {
		t.Fatalf("denied mutations changed content: %q %v", content, err)
	}
	policy.reset(nil)
	replayed := fileAuthorizationSuccess(t, h, open)
	if replayed.Receipt.Reference != reference {
		t.Fatalf("allowed replay replaced reference: %+v", replayed)
	}
	queried := fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileQueryAction, Session: session.Session, Action: open.Action})
	if queried.Receipt == nil || queried.Receipt.State != storage.FileActionCompleted || queried.Receipt.Reference != reference {
		t.Fatalf("denial changed native retain receipt: %+v", queried.Receipt)
	}
	written := fileAuthorizationSuccess(t, h, write)
	if written.Receipt == nil || written.Receipt.State != storage.FileActionCompleted || written.Receipt.Action != write.Action {
		t.Fatalf("allowed retry did not complete original write action: %+v", written.Receipt)
	}
	content, err = backend.Read(t.Context(), "file")
	if err != nil || string(content) != "altered!" {
		t.Fatalf("allowed write did not publish content: %q %v", content, err)
	}
}

func TestFileAuthorizationSanitizesNativeLookingPolicyFailures(t *testing.T) {
	private := "private policy details with a retained file capability"
	link := fmt.Errorf("private/target at private/link: %w", syscall.ELOOP)
	native := &storage.FileError{Code: syscall.EACCES, Conflict: &storage.FileConflict{Kind: storage.ConflictClaim, NodeID: 99, Claim: storage.AccessClaim{Excludes: storage.WriteContent}}, Cause: errors.New(private)}
	for _, c := range []struct {
		name, errno, message string
		cause                error
	}{
		{"denied link", "EACCES", "access denied", errors.Join(authz.ErrDenied, link)},
		{"failed link", "EIO", "authorization failed", link},
		{"denied native failure", "EACCES", "access denied", errors.Join(authz.ErrDenied, native)},
		{"failed native failure", "EIO", "authorization failed", native},
	} {
		t.Run(c.name, func(t *testing.T) {
			policy := &fileAuthorizationPolicy{err: c.cause}
			h, backend := fileAuthorizationFixture(t, policy, DefaultFileLimits())
			probe := &fileAuthorizationBackend{FileStorage: backend}
			h.files.backend = probe
			for _, candidate := range fileAuthorizationCases(t) {
				t.Run(string(candidate.request.Op), func(t *testing.T) {
					fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, candidate.request), c.errno, c.message)
				})
			}
			if probe.calls != 0 {
				t.Fatalf("policy failure made %d native calls", probe.calls)
			}
		})
	}
}

func TestFileAuthorizationCancellationPreventsNativeEffects(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
	}{
		{"callback allows", nil},
		{"callback denies", authz.ErrDenied},
		{"callback returns native facts", &storage.FileError{Code: syscall.ELOOP, Conflict: &storage.FileConflict{Kind: storage.ConflictIdentity, NodeID: 99}, Cause: errors.New("private/target at private/link")}},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, backend := fileAuthorizationFixture(t, &fileAuthorizationPolicy{}, DefaultFileLimits())
			probe := &fileAuthorizationBackend{FileStorage: backend}
			h.files.backend = probe
			identity := &struct{ name string }{name: "member"}
			host := context.WithValue(t.Context(), fileAuthorizationIdentity{}, identity)
			for _, candidate := range fileAuthorizationCases(t) {
				t.Run(string(candidate.request.Op), func(t *testing.T) {
					ctx, cancel := context.WithCancel(host)
					defer cancel()
					called := 0
					h.authorizer = authz.AuthorizerFunc(func(observed context.Context, access authz.AccessRequest) error {
						called++
						if observed.Value(fileAuthorizationIdentity{}) != identity || access != candidate.access {
							t.Fatalf("authorization changed identity or intent: %+v", access)
						}
						cancel()
						return c.err
					})
					fileAuthorizationDenied(t, fileAuthorizationRequestContext(t, h, ctx, candidate.request), "EINTR", context.Canceled.Error())
					if called != 1 {
						t.Fatalf("cancellation callback ran %d times, want 1", called)
					}
				})
			}
			if probe.calls != 0 {
				t.Fatalf("cancelled authorization made %d native calls", probe.calls)
			}
			if err := host.Err(); err != nil {
				t.Fatalf("request cancellation ended host identity context: %v", err)
			}
		})
	}
}

func TestFileLookupAuthorizationDoesNotRequireDirectoryListing(t *testing.T) {
	policy := &fileAuthorizationPolicy{}
	h, backend := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	name := []byte{255, 'x'}
	if err := backend.Write(t.Context(), string(name), []byte("data")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	session := fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileSessionOpen, Options: &options})
	retained := fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileRetain, Session: session.Session, Action: fileAuthorizationID(t, session.Status.ActionEpoch), Retain: &fileRetainRequest{NodeID: root.ID}})
	var operations []storage.Operation
	h.authorizer = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		operations = append(operations, request.Operation)
		if request.Operation == storage.OpFileListAt || request.Operation == storage.OpVolumeList {
			return authz.ErrDenied
		}
		return nil
	})
	response := fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileLookupAt, Session: session.Session, Reference: retained.Receipt.Reference, Name: name})
	if response.Lookup == nil || !response.Lookup.Found || string(response.Lookup.Name) != string(name) || response.Lookup.ParentID != root.ID || response.Lookup.DirectoryRevision == 0 {
		t.Fatalf("lookup=%+v", response.Lookup)
	}
	if len(operations) != 1 || operations[0] != storage.OpFileLookupAt {
		t.Fatalf("lookup required unrelated authorization: %v", operations)
	}
	fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, fileRequest{Op: storage.OpFileListAt, Session: session.Session, Reference: retained.Receipt.Reference, List: &storage.DirectoryPageRequest{MaxEntries: 1, MaxBytes: 4096}}), "EACCES", "access denied")
}
