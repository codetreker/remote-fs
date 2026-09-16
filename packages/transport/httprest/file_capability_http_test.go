package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type capabilityTestBackend struct {
	storage.FileStorage
	session *capabilityTestSession
}

func (b capabilityTestBackend) CheckFileStorage() error { return nil }
func (b capabilityTestBackend) NewFileSession(_ context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	b.session.options = options
	return b.session, nil
}

type capabilityTestSession struct {
	storage.FileSession
	options           storage.FileSessionOptions
	opens             atomic.Int32
	rangeMu           sync.Mutex
	lastRange         storage.RangeAttempt
	rangeCalls        atomic.Int32
	largeListing      bool
	openError         error
	nameError         error
	attrMetadataBytes int64
	budgetChecks      atomic.Int32
	loaded            atomic.Int32
	node              *capabilityTestReference
	observed          storage.ChildName
}

func (s *capabilityTestSession) Status(context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{Epoch: "test-authority", Revision: 1, ActionEpoch: 1, Remaining: s.options.Lease, HistoryRemaining: s.options.History}, nil
}
func (s *capabilityTestSession) Close(ctx context.Context) error {
	if s.node != nil {
		return s.node.Close(ctx)
	}
	return nil
}
func (s *capabilityTestSession) CheckAtomicFileOpen() error { return nil }
func (s *capabilityTestSession) CheckNodeReferences() error { return nil }
func (s *capabilityTestSession) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	scalar := s.node.attr
	scalar.Metadata = nil
	metadataBytes := s.attrMetadataBytes
	if metadataBytes == 0 {
		metadataBytes = 6
	}
	s.budgetChecks.Add(1)
	if err := storage.CheckAttrResultBudget(ctx, scalar, metadataBytes); err != nil {
		return storage.OpenResult{}, err
	}
	if metadataBytes > 6 {
		s.loaded.Add(1)
	}
	s.opens.Add(1)
	s.observed = name
	outcome := storage.Opened
	if options.Create {
		outcome = storage.Created
	}
	return storage.OpenResult{File: s.node, Attr: s.node.attr, Outcome: outcome}, s.openError
}
func (s *capabilityTestSession) OpenNodeRef(context.Context, uint64, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.opens.Add(1)
	return storage.NodeOpenResult{Reference: s.node, Attr: s.node.attr, Outcome: storage.Opened}, nil
}
func (s *capabilityTestSession) OpenChildRef(_ context.Context, name storage.ChildName, _ storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.opens.Add(1)
	s.observed = name
	return storage.NodeOpenResult{Reference: s.node, Attr: s.node.attr, Outcome: storage.Created}, nil
}

type capabilityTestReference struct {
	storage.File
	attr       storage.Attr
	mu         sync.Mutex
	closed     bool
	closeError error
	checkError error
	closes     int
}

func (f *capabilityTestReference) Stat(context.Context) (storage.Attr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return storage.Attr{}, syscall.EBADF
	}
	value := f.attr
	value.Size++
	return value, nil
}
func (f *capabilityTestReference) Close(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	if f.closeError != nil {
		err := f.closeError
		f.closeError = nil
		return err
	}
	f.closed = true
	return nil
}
func (f *capabilityTestReference) CheckScopedReference() error { return nil }
func (f *capabilityTestReference) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: "test-scope"}, nil
}

func capabilityHTTPFixture(t *testing.T, session *capabilityTestSession) (*Storage, *Handler) {
	t.Helper()
	options := DefaultHandlerOptions().settle()
	lifetime, cancel := context.WithCancelCause(context.Background())
	handler := &Handler{lifetime: lifetime, cancelLifetime: cancel, stopping: make(chan struct{}), maxBodyBytes: options.maxBodyBytes, maxWriteBytes: options.maxWriteBytes,
		bodies: newBodyAdmission(options.maxConcurrentBodies, options.maxInFlightBodyBytes, options.maxWaitingBodies), responses: newBodyAdmission(options.maxConcurrentResponses, options.maxInFlightResponseBytes, options.maxWaitingResponses), lockControls: configuredLockControlAdmission(options.maxConcurrentLockControls, options.maxWaitingLockControls)}
	handler.files = &fileRegistry{limits: DefaultFileLimits(), backend: capabilityTestBackend{session: session}, sessions: make(map[string]*servedFileSession), wake: make(chan struct{}, 1), done: make(chan struct{})}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client, handler
}

type loseCapabilityReply struct {
	next http.RoundTripper
	op   storage.Operation
	lost atomic.Bool
}

func (d *loseCapabilityReply) RoundTrip(request *http.Request) (*http.Response, error) {
	var operation storage.Operation
	if request.GetBody != nil {
		body, err := request.GetBody()
		if err == nil {
			var decoded fileRequest
			_ = json.NewDecoder(body).Decode(&decoded)
			operation = decoded.Op
			_ = body.Close()
		}
	}
	response, err := d.next.RoundTrip(request)
	if err == nil && operation == d.op && d.lost.CompareAndSwap(false, true) {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return nil, errors.New("lost capability reply")
	}
	return response, err
}
func TestAtomicOpenReusesJournalAcrossLostReply(t *testing.T) {
	native := &capabilityTestSession{node: &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: storage.NodeRegular, Size: 7}}}
	client, handler := capabilityHTTPFixture(t, native)
	client.http.Transport = &loseCapabilityReply{next: client.http.Transport, op: storage.OpFileOpenAt}
	session, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte{'x', 0xff}}
	options := storage.OpenAtOptions{Read: true, Create: true, Target: storage.ChildCondition{State: storage.Absent}, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.ReadData}}
	result, err := session.(storage.AtomicFileOpener).OpenAt(context.Background(), name, options)
	if err != nil {
		t.Fatal(err)
	}
	if native.opens.Load() != 1 || result.Attr.ID != 41 || result.Attr.Size != 7 || result.Outcome != storage.Created {
		t.Fatalf("lost reply changed result: opens=%d,result=%+v", native.opens.Load(), result)
	}
	current, err := result.File.Stat(context.Background())
	if err != nil || current.Size != 8 {
		t.Fatalf("current stat: %+v,%v", current, err)
	}
	handler.files.mu.Lock()
	defer handler.files.mu.Unlock()
	for _, served := range handler.files.sessions {
		served.mu.Lock()
		if len(served.files) != 1 {
			t.Errorf("replayed open retained %d refs", len(served.files))
		}
		for _, file := range served.files {
			if !file.pending.IsZero() {
				t.Error("returned ref remains pending ACK")
			}
		}
		served.mu.Unlock()
	}
}
func TestMetadataOpenUsesExistingRegistryAndRetryableClose(t *testing.T) {
	native := &capabilityTestSession{node: &capabilityTestReference{attr: storage.Attr{ID: 51, Kind: storage.NodeDirectory}, closeError: syscall.EIO}}
	client, handler := capabilityHTTPFixture(t, native)
	session, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.(storage.NodeReferences).OpenNodeRef(context.Background(), 51, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 51}, MetadataAccess: storage.ReadMetadata})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.Reference.(storage.File); ok {
		t.Fatal("node reference exposes byte methods")
	}
	if err := result.Reference.Close(context.Background()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("first cleanup: %v", err)
	}
	handler.files.mu.Lock()
	for _, served := range handler.files.sessions {
		served.mu.Lock()
		if len(served.files) != 1 {
			t.Error("failed native cleanup discarded registry entry")
		}
		served.mu.Unlock()
	}
	handler.files.mu.Unlock()
	if err := result.Reference.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	native.node.mu.Lock()
	defer native.node.mu.Unlock()
	if native.node.closes != 2 || !native.node.closed {
		t.Fatalf("native cleanup calls=%d,closed=%v", native.node.closes, native.node.closed)
	}
}

func (s *capabilityTestSession) CheckNamespaceAccess() error { return nil }
func (s *capabilityTestSession) LookupAt(_ context.Context, name storage.ChildName) (storage.Attr, error) {
	s.observed = name
	return s.node.attr, nil
}
func (s *capabilityTestSession) ReadDirNode(_ context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	return storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte{1}}, Entries: []storage.ObservedEntry{{RawLeaf: []byte{0xff}, Attr: s.node.attr}}}, nil
}
func (s *capabilityTestSession) MutateName(_ context.Context, command storage.NameCommand) (storage.NameResult, error) {
	s.observed = command.Name
	attr := s.node.attr
	return storage.NameResult{Attr: &attr}, s.nameError
}
func (s *capabilityTestSession) CheckMetadataAccess() error { return nil }
func (s *capabilityTestSession) SetMetadata(_ context.Context, _ uint64, _ string, _ []byte, data []byte) (storage.OpaquePayload, error) {
	return storage.OpaquePayload{Version: []byte{2}, Data: data}, nil
}
func (s *capabilityTestSession) CheckUseOwners() error { return nil }
func (s *capabilityTestSession) NewUseOwner(context.Context, uint64, storage.UseScope, storage.OwnerOptions) (storage.UseOwner, error) {
	return 3, nil
}
func (s *capabilityTestSession) RetireUseOwner(context.Context, storage.UseOwner) error { return nil }
func (s *capabilityTestSession) CheckRangeControl() error                               { return nil }
func (s *capabilityTestSession) GetConflict(context.Context, storage.UseOwner, storage.RangeCommand) (storage.RangeConflict, error) {
	return storage.RangeConflict{}, nil
}
func (s *capabilityTestSession) Apply(_ context.Context, _ storage.UseOwner, commands []storage.RangeCommand, id storage.LockRequestID) (storage.RangeAttempt, error) {
	result := capabilityTestAttempt(id, commands)
	s.rangeMu.Lock()
	s.lastRange = result.Clone()
	s.rangeMu.Unlock()
	s.rangeCalls.Add(1)
	return result, nil
}
func (s *capabilityTestSession) Query(_ context.Context, _ storage.UseOwner, id storage.LockRequestID) (storage.RangeAttempt, error) {
	s.rangeMu.Lock()
	defer s.rangeMu.Unlock()
	if s.lastRange.Request == id {
		return s.lastRange.Clone(), nil
	}
	return capabilityTestAttempt(id, []storage.RangeCommand{capabilityTestRange()}), nil
}
func (s *capabilityTestSession) Cancel(ctx context.Context, owner storage.UseOwner, id storage.LockRequestID) (storage.RangeAttempt, error) {
	return s.Query(ctx, owner, id)
}
func (s *capabilityTestSession) Drop(context.Context, storage.UseOwner, storage.ConflictDomain) error {
	return nil
}
func (f *capabilityTestReference) CheckReferenceState() error { return f.checkError }
func (f *capabilityTestReference) State(context.Context) (storage.ReferenceState, error) {
	return storage.ReferenceState{Attr: f.attr}, nil
}
func (f *capabilityTestReference) CheckMetadataAccess() error { return nil }
func (f *capabilityTestReference) SetMetadata(_ context.Context, _ string, _ []byte, data []byte) (storage.OpaquePayload, error) {
	return storage.OpaquePayload{Version: []byte{2}, Data: data}, nil
}
func (f *capabilityTestReference) CheckDeleteIntent() error { return nil }
func (f *capabilityTestReference) SetPendingUnlink(context.Context, storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return storage.ReferenceState{Attr: f.attr, PendingUnlink: true, PendingGeneration: []byte{1}}, nil
}
func (f *capabilityTestReference) ClearPendingUnlink(context.Context, storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return storage.ReferenceState{Attr: f.attr}, nil
}
func (f *capabilityTestReference) CheckConditionalFileMutation() error { return nil }
func (f *capabilityTestReference) MutateFile(_ context.Context, mutation storage.FileMutation) (storage.Attr, error) {
	value := f.attr
	value.Size = mutation.Size
	return value, nil
}
func capabilityTestRange() storage.RangeCommand {
	return storage.RangeCommand{Domain: storage.DomainRecord, Mode: storage.RangeShared, Range: storage.Range{Kind: storage.Bytes, Length: 1}, Edit: storage.Replace}
}

func TestNeutralCapabilitiesUseTheFileControlProtocol(t *testing.T) {
	native := &capabilityTestSession{node: &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: storage.NodeRegular, Size: 7}}}
	client, _ := capabilityHTTPFixture(t, native)
	opened, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*remoteFileSession)
	ctx := context.Background()
	child := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte{0xff}}
	attr, err := session.LookupAt(ctx, child)
	if err != nil || attr.ID != 41 {
		t.Fatalf("lookup: %+v,%v", attr, err)
	}
	directory, err := session.ReadDirNode(ctx, child.Parent)
	if err != nil || len(directory.Entries) != 1 {
		t.Fatalf("directory: %+v,%v", directory, err)
	}
	changed, err := session.MutateName(ctx, storage.NameCommand{Kind: storage.NameMkdir, Name: child, Target: storage.ChildCondition{State: storage.Absent}})
	if err != nil || changed.Attr == nil || changed.Attr.ID != 41 {
		t.Fatalf("name result: %+v,%v", changed, err)
	}
	payload, err := session.SetMetadata(ctx, 41, "test.v1", nil, []byte{0, 0xff})
	if err != nil || len(payload.Data) != 2 {
		t.Fatalf("metadata: %+v,%v", payload, err)
	}
	result, err := session.OpenAt(ctx, child, storage.OpenAtOptions{Read: true, Create: true, Target: storage.ChildCondition{State: storage.Absent}, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.ReadData}})
	if err != nil {
		t.Fatal(err)
	}
	file := result.File.(*remoteFile)
	state, err := file.State(ctx)
	if err != nil || state.Attr.ID != 41 {
		t.Fatalf("state: %+v,%v", state, err)
	}
	scope, err := file.Scope(ctx)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := session.NewUseOwner(ctx, 41, scope, storage.OwnerOptions{Lifetime: storage.OwnerReference})
	if err != nil || owner != 3 {
		t.Fatalf("owner: %d,%v", owner, err)
	}
	command := capabilityTestRange()
	id, _ := storage.NewLockRequestID(1)
	conflict, err := session.GetConflict(ctx, owner, command)
	if err != nil || conflict.Found {
		t.Fatalf("conflict: %+v,%v", conflict, err)
	}
	for _, call := range []func() (storage.RangeAttempt, error){func() (storage.RangeAttempt, error) {
		return session.Apply(ctx, owner, []storage.RangeCommand{command}, id)
	}, func() (storage.RangeAttempt, error) { return session.Query(ctx, owner, id) }, func() (storage.RangeAttempt, error) { return session.Cancel(ctx, owner, id) }} {
		attempt, err := call()
		if err != nil || attempt.State != storage.Granted {
			t.Fatalf("range: %+v,%v", attempt, err)
		}
	}
	if err := session.Drop(ctx, owner, storage.DomainRecord); err != nil {
		t.Fatal(err)
	}
	if err := session.RetireUseOwner(ctx, owner); err != nil {
		t.Fatal(err)
	}
	payload, err = file.SetMetadata(ctx, "test.v1", nil, []byte{0xff})
	if err != nil || len(payload.Data) != 1 {
		t.Fatalf("file metadata: %+v,%v", payload, err)
	}
	state, err = file.SetPendingUnlink(ctx, storage.PendingUnlinkCommand{Condition: storage.UnlinkFile})
	if err != nil || !state.PendingUnlink {
		t.Fatalf("pending: %+v,%v", state, err)
	}
	state, err = file.ClearPendingUnlink(ctx, storage.ClearPendingUnlinkCommand{Generation: state.PendingGeneration})
	if err != nil || state.PendingUnlink {
		t.Fatalf("clear: %+v,%v", state, err)
	}
	attr, err = file.MutateFile(ctx, storage.FileMutation{Kind: storage.MutateTruncate, Size: 2})
	if err != nil || attr.Size != 2 {
		t.Fatalf("conditional: %+v,%v", attr, err)
	}
}

func (s *capabilityTestSession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	if s.largeListing {
		_, err := result.Reserve(4096, 4096, storage.Attr{ID: 1, Kind: storage.NodeRegular})
		if err != nil {
			return storage.DirectoryObservation{}, err
		}
		s.loaded.Add(1)
		return storage.DirectoryObservation{}, syscall.EIO
	}
	observed, err := s.ReadDirNode(ctx, target)
	if err != nil {
		return storage.DirectoryObservation{}, result.Fail(err)
	}
	for _, entry := range observed.Entries {
		if err := result.Add(storage.Entry{Name: string(entry.RawLeaf), Attr: entry.Attr}); err != nil {
			return storage.DirectoryObservation{}, err
		}
	}
	return observed.Observation, nil
}
func capabilityTestAttempt(id storage.LockRequestID, commands []storage.RangeCommand) storage.RangeAttempt {
	result := storage.RangeAttempt{Request: id, State: storage.Granted, Commands: commands, EverGranted: true, HistoryRemaining: time.Minute}
	for i, command := range commands {
		effect := storage.RangeEffect{Command: command}
		if command.Edit == storage.AddExact {
			effect.Claim, _ = storage.NewClaimID(id, i)
			result.Claims = append(result.Claims, effect.Claim)
		}
		result.Effects = append(result.Effects, effect)
	}
	return result
}

func TestMaximumRangeReceiptSurvivesLostApplyResponse(t *testing.T) {
	native := &capabilityTestSession{node: &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: storage.NodeRegular}}}
	client, _ := capabilityHTTPFixture(t, native)
	lost := &loseCapabilityReply{next: client.http.Transport, op: storage.OpFileRangeApply}
	client.http.Transport = lost
	opened, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*remoteFileSession)
	id, _ := storage.NewLockRequestID(^uint64(0))
	commands := make([]storage.RangeCommand, storage.MaxRangeCommands)
	for i := range commands {
		commands[i] = storage.RangeCommand{Domain: storage.DomainEnforced, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Start: ^uint64(0) - uint64(i), Length: 1}, Edit: storage.AddExact, Policy: storage.RangePolicy{DenyOthers: storage.ReadData | storage.WriteData}}
	}
	if _, err := session.Apply(context.Background(), 3, commands, id); storage.ErrnoOf(err) != syscall.EIO || !lost.lost.Load() {
		t.Fatalf("lost response: %v", err)
	}
	receipt, err := session.Query(context.Background(), 3, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipt.Claims) != 64 || len(receipt.Effects) != 64 || native.rangeCalls.Load() != 1 {
		t.Fatalf("incomplete receipt: claims=%d,effects=%d,apply=%d", len(receipt.Claims), len(receipt.Effects), native.rangeCalls.Load())
	}
	encoded, _ := json.Marshal(fileResponse{Epoch: 1, Data: []byte{}, Attempt: &receipt})
	if len(encoded) <= int(DefaultMaxLockControlBytes) || int64(len(encoded)) > MaxFileControlBytes {
		t.Fatalf("receipt size did not exercise extended bound: %d", len(encoded))
	}
	if _, err := session.Cancel(context.Background(), 3, id); err != nil {
		t.Fatal(err)
	}
}

func TestRangeReplyBudgetRefusesBeforeNativeGrant(t *testing.T) {
	native := &capabilityTestSession{node: &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: storage.NodeRegular}}}
	client, handler := capabilityHTTPFixture(t, native)
	handler.maxBodyBytes = DefaultMaxLockControlBytes
	opened, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	commands := make([]storage.RangeCommand, storage.MaxRangeCommands)
	for i := range commands {
		commands[i] = storage.RangeCommand{Domain: storage.DomainEnforced, Mode: storage.RangeShared, Range: storage.Range{Kind: storage.Bytes, Start: uint64(i), Length: 1}, Edit: storage.AddExact, Policy: storage.RangePolicy{DenySelf: storage.WriteData, DenyOthers: storage.WriteData}}
	}
	id, _ := storage.NewLockRequestID(1)
	if _, err := opened.(storage.RangeControl).Apply(context.Background(), 3, commands, id); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("small response budget: %v", err)
	}
	if native.rangeCalls.Load() != 0 {
		t.Fatal("insufficient receipt budget reached native grant")
	}
}

func TestDirectoryResponseBudgetPrecedesPayloadMaterialization(t *testing.T) {
	native := &capabilityTestSession{node: &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: storage.NodeRegular}}, largeListing: true}
	client, handler := capabilityHTTPFixture(t, native)
	handler.maxBodyBytes = 1024
	opened, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	result, err := opened.(storage.NamespaceAccess).ReadDirNode(context.Background(), storage.DirectoryTarget{NodeID: 1})
	if err == nil || len(result.Entries) != 0 || native.loaded.Load() != 0 {
		t.Fatalf("oversized native payload materialized: loaded=%d,result=%+v,err=%v", native.loaded.Load(), result, err)
	}
}

func TestPartialOpenAndNameEffectsCannotBecomeSafeCancellation(t *testing.T) {
	for _, cleanupFails := range []bool{false, true} {
		t.Run(fmt.Sprint(cleanupFails), func(t *testing.T) {
			native := &capabilityTestSession{options: storage.DefaultFileSessionOptions(), openError: context.Canceled, node: &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: storage.NodeRegular}}}
			if cleanupFails {
				native.node.closeError = syscall.EIO
			}
			handler := &Handler{files: &fileRegistry{limits: DefaultFileLimits()}}
			served := &servedFileSession{native: native, options: native.options, files: make(map[string]*servedFile)}
			child := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("new")}
			_, err := handler.openReference(context.Background(), served, fileRequest{Op: storage.OpFileOpenAt, Child: &child, OpenAt: openAtOptionsOf(storage.OpenAtOptions{})})
			if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, context.Canceled) {
				t.Fatalf("partial open classification: %v", err)
			}
			if cleanupFails && len(served.files) != 1 {
				t.Fatal("failed partial cleanup lost its registry owner")
			}
			if !cleanupFails && len(served.files) != 0 {
				t.Fatal("completed partial cleanup retained its registry entry")
			}
			native.nameError = context.Canceled
			_, err = handler.performSessionCapability(context.Background(), native, fileRequest{Op: storage.OpFileMutateName, Name: nameCommandOf(storage.NameCommand{})})
			if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, context.Canceled) {
				t.Fatalf("partial name classification: %v", err)
			}
		})
	}
}

func TestPostOpenCapabilityFailureRetainsEffectClassification(t *testing.T) {
	native := &capabilityTestSession{options: storage.DefaultFileSessionOptions(), node: &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: storage.NodeRegular}, checkError: context.Canceled}}
	handler := &Handler{files: &fileRegistry{limits: DefaultFileLimits()}}
	served := &servedFileSession{native: native, options: native.options, files: make(map[string]*servedFile)}
	child := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("created")}
	_, err := handler.openReference(context.Background(), served, fileRequest{Op: storage.OpFileOpenAt, Child: &child, OpenAt: openAtOptionsOf(storage.OpenAtOptions{})})
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, context.Canceled) || len(served.files) != 1 {
		t.Fatalf("post-open probe failure: %v;refs=%d", err, len(served.files))
	}
}

func TestAtomicReplyBudgetRejectsBeforeNativePayloadAndEffect(t *testing.T) {
	for _, clientIsSmall := range []bool{false, true} {
		t.Run(fmt.Sprint(clientIsSmall), func(t *testing.T) {
			native := &capabilityTestSession{node: &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: storage.NodeRegular}}, attrMetadataBytes: storage.MaxMetadataBytes}
			client, handler := capabilityHTTPFixture(t, native)
			if clientIsSmall {
				client.maxBodyBytes = 1024
			} else {
				handler.maxBodyBytes = 1024
			}
			opened, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			_, err = opened.(storage.AtomicFileOpener).OpenAt(context.Background(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("new")}, storage.OpenAtOptions{Read: true, Create: true, Existing: storage.Keep, Target: storage.ChildCondition{State: storage.Absent}, Use: storage.UseClaim{Uses: storage.ReadData}})
			if !errors.Is(err, syscall.EFBIG) || native.opens.Load() != 0 || native.loaded.Load() != 0 || native.budgetChecks.Load() != 1 {
				t.Fatalf("oversized result passed native admission: %v;opens=%d,loads=%d,checks=%d", err, native.opens.Load(), native.loaded.Load(), native.budgetChecks.Load())
			}
		})
	}
}

func (f *capabilityTestReference) SetAttr(_ context.Context, change storage.AttrChange) (storage.Attr, error) {
	value := f.attr
	if change.ModTime != nil {
		value.ModTime = *change.ModTime
	}
	return value, nil
}
func TestMetadataReferenceFacetsRoundTripWithoutByteMethods(t *testing.T) {
	native := &capabilityTestSession{node: &capabilityTestReference{attr: storage.Attr{ID: 51, Kind: storage.NodeDirectory}}}
	client, _ := capabilityHTTPFixture(t, native)
	opened, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	result, err := opened.(storage.NodeReferences).OpenChildRef(context.Background(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte{0xff}}, storage.NodeRefOptions{Kind: storage.NodeDirectory, Create: true, Target: storage.ChildCondition{State: storage.Absent}, MetadataAccess: storage.ReadMetadata | storage.WriteMetadata})
	if err != nil {
		t.Fatal(err)
	}
	ref := result.Reference.(*remoteNodeReference)
	ctx := context.Background()
	if _, err := ref.Stat(ctx); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(123, 4)
	attr, err := ref.SetAttr(ctx, storage.AttrChange{ModTime: &stamp})
	if err != nil || !attr.ModTime.Equal(stamp) {
		t.Fatalf("metadata ref attrs: %+v,%v", attr, err)
	}
	for _, check := range []func() error{ref.CheckReferenceState, ref.CheckScopedReference, ref.CheckMetadataAccess, ref.CheckDeleteIntent} {
		if err := check(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ref.State(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ref.Scope(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ref.SetMetadata(ctx, "test.v1", nil, []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	state, err := ref.SetPendingUnlink(ctx, storage.PendingUnlinkCommand{Condition: storage.UnlinkIfEmpty})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ref.ClearPendingUnlink(ctx, storage.ClearPendingUnlinkCommand{Generation: state.PendingGeneration}); err != nil {
		t.Fatal(err)
	}
	if _, err := ref.CloseWithBarrier(ctx); err != nil {
		t.Fatal(err)
	}
}
