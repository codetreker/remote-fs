package httprest

import (
	"bytes"
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

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
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

type metadataConditionSession struct {
	*capabilityTestSession
	conditionCalls atomic.Int32
	conditionMu    sync.Mutex
	conditions     map[string][]byte
}

func (s *metadataConditionSession) checkConditions(conditions map[string][]byte) error {
	s.conditionCalls.Add(1)
	s.conditionMu.Lock()
	s.conditions = storage.CloneInitialMetadata(conditions)
	s.conditionMu.Unlock()
	for namespace, expected := range conditions {
		current, exists := s.node.attr.Metadata[namespace]
		if len(expected) == 0 && exists || len(expected) != 0 && (!exists || !bytes.Equal(current.Version, expected)) {
			return storage.ErrConditionConflict
		}
	}
	return nil
}
func (s *metadataConditionSession) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	if err := s.checkConditions(options.ExpectedMetadata); err != nil {
		return storage.OpenResult{}, err
	}
	return s.capabilityTestSession.OpenAt(ctx, name, options)
}
func (s *metadataConditionSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if err := s.checkConditions(options.ExpectedMetadata); err != nil {
		return storage.NodeOpenResult{}, err
	}
	return s.capabilityTestSession.OpenNodeRef(ctx, id, options)
}
func (s *metadataConditionSession) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if err := s.checkConditions(options.ExpectedMetadata); err != nil {
		return storage.NodeOpenResult{}, err
	}
	result, err := s.capabilityTestSession.OpenChildRef(ctx, name, options)
	result.Outcome = storage.Opened
	return result, err
}

type metadataConditionBackend struct {
	storage.FileStorage
	session *metadataConditionSession
}

func (b metadataConditionBackend) CheckFileStorage() error { return nil }
func (b metadataConditionBackend) NewFileSession(_ context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	b.session.options = options
	return b.session, nil
}
func metadataConditionHTTPFixture(t *testing.T) (*Storage, *Handler, *remoteFileSession, *metadataConditionSession) {
	t.Helper()
	base := &capabilityTestSession{node: &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: storage.NodeRegular, Size: 7, Metadata: map[string]storage.OpaquePayload{"test.attributes": {Version: []byte{1, 0, 0xff}, Data: []byte("payload")}}}}}
	client, handler := capabilityHTTPFixture(t, base)
	native := &metadataConditionSession{capabilityTestSession: base}
	handler.files.backend = metadataConditionBackend{session: native}
	opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	return client, handler, opened.(*remoteFileSession), native
}

func TestOpenMetadataConditionsHTTPRoundTripAndKnownConflict(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef} {
		t.Run(string(operation), func(t *testing.T) {
			client, handler, session, native := metadataConditionHTTPFixture(t)
			request := openMetadataRequest(t, operation, session.id)
			requestMetadataConditions(request)["test.attributes"] = []byte("stale")
			response, err := client.fileCall(t.Context(), request)
			if !errors.Is(err, storage.ErrConditionConflict) || !errors.Is(err, syscall.EAGAIN) || response.File != "" || response.Attr != nil || native.opens.Load() != 0 {
				t.Fatalf("known conflict changed outcome/effects: %+v %v opens=%d", response, err, native.opens.Load())
			}
			handler.files.mu.Lock()
			registered := handler.files.sessions[session.id]
			handler.files.mu.Unlock()
			registered.mu.Lock()
			files := len(registered.files)
			registered.mu.Unlock()
			if files != 0 {
				t.Fatalf("conflict retained %d references", files)
			}
			request = openMetadataRequest(t, operation, session.id)
			response, err = client.fileCall(t.Context(), request)
			if err != nil || response.File == "" || response.Outcome != storage.Opened || response.Attr == nil || response.Attr.ID != 41 || native.opens.Load() != 1 {
				t.Fatalf("matching conditions: %+v %v opens=%d", response, err, native.opens.Load())
			}
			native.conditionMu.Lock()
			captured := storage.CloneInitialMetadata(native.conditions)
			native.conditionMu.Unlock()
			absent, present := captured["test.absent"]
			if len(captured) != 2 || !present || len(absent) != 0 || !bytes.Equal(captured["test.attributes"], []byte{1, 0, 0xff}) {
				t.Fatalf("condition map changed before execution: %#v", captured)
			}
		})
	}
}

func TestOpenMetadataConditionsBindExistingActionDigest(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef} {
		t.Run(string(operation), func(t *testing.T) {
			client, _, session, native := metadataConditionHTTPFixture(t)
			request := openMetadataRequest(t, operation, session.id)
			original, err := client.fileCall(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := client.fileCall(t.Context(), request)
			if err != nil || replayed.File != original.File || native.conditionCalls.Load() != 1 || native.opens.Load() != 1 {
				t.Fatalf("unchanged replay reexecuted conditions/open: %v calls=%d opens=%d", err, native.conditionCalls.Load(), native.opens.Load())
			}
			requestMetadataConditions(request)["test.attributes"] = []byte("different")
			if _, err := client.fileCall(t.Context(), request); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("changed token on same action: %v", err)
			}
			requestMetadataConditions(request)["test.attributes"] = []byte{1, 0, 0xff}
			delete(requestMetadataConditions(request), "test.absent")
			if _, err := client.fileCall(t.Context(), request); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("removed absence condition on same action: %v", err)
			}
			if native.conditionCalls.Load() != 1 || native.opens.Load() != 1 {
				t.Fatal("changed replay reached native execution")
			}
		})
	}
}

func TestOpenMetadataConditionsMalformedWireStopsBeforeExecution(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef} {
		t.Run(string(operation), func(t *testing.T) {
			_, handler, session, native := metadataConditionHTTPFixture(t)
			tooMany := map[string][]byte{}
			for i := range storage.MaxMetadataNamespaces + 1 {
				tooMany[fmt.Sprintf("test.namespace.%d", i)] = []byte{1}
			}
			many, _ := json.Marshal(tooMany)
			large, _ := json.Marshal(map[string][]byte{"test.attributes": bytes.Repeat([]byte{1}, storage.MaxObservationTokenBytes+1)})
			for _, malformed := range []struct {
				name, raw string
				anyTarget bool
			}{
				{"null map", "null", false}, {"null token", `{"test.attributes":null}`, false}, {"duplicate namespace", `{"test.attributes":"AQ==","test.attributes":"Ag=="}`, false},
				{"invalid namespace", `{"":"AQ=="}`, false}, {"invalid base64", `{"test.attributes":"%%%"}`, false}, {"too many namespaces", string(many), false}, {"oversized token", string(large), false},
				{"unbound target", `{"test.absent":""}`, true},
			} {
				t.Run(malformed.name, func(t *testing.T) {
					request := openMetadataRequest(t, operation, session.id)
					if malformed.anyTarget {
						if request.OpenAt != nil {
							request.OpenAt.Target = storage.ChildCondition{State: storage.Any}
						} else {
							request.NodeRef.Target = storage.ChildCondition{State: storage.Any}
						}
					}
					body, err := json.Marshal(request)
					if err != nil {
						t.Fatal(err)
					}
					var envelope map[string]json.RawMessage
					if err := json.Unmarshal(body, &envelope); err != nil {
						t.Fatal(err)
					}
					field := "nodeRef"
					if request.OpenAt != nil {
						field = "openAt"
					}
					var options map[string]json.RawMessage
					if err := json.Unmarshal(envelope[field], &options); err != nil {
						t.Fatal(err)
					}
					options["expectedMetadata"] = json.RawMessage(malformed.raw)
					envelope[field], err = json.Marshal(options)
					if err != nil {
						t.Fatal(err)
					}
					body, err = json.Marshal(envelope)
					if err != nil {
						t.Fatal(err)
					}
					r := httptest.NewRequest(http.MethodPost, Prefix+string(OpFile), bytes.NewReader(body))
					r.Header.Set("Content-Type", contentJSON)
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, r)
					if w.Code < 400 || native.conditionCalls.Load() != 0 || native.opens.Load() != 0 {
						t.Fatalf("malformed conditions executed: status=%d calls=%d opens=%d", w.Code, native.conditionCalls.Load(), native.opens.Load())
					}
				})
			}
			body, err := json.Marshal(openMetadataRequest(t, operation, session.id))
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, Prefix+string(OpFile), bytes.NewReader(body))
			r.Header.Set("Content-Type", contentJSON)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != http.StatusOK || native.conditionCalls.Load() != 1 || native.opens.Load() != 1 {
				t.Fatalf("valid control failed: status=%d calls=%d opens=%d", w.Code, native.conditionCalls.Load(), native.opens.Load())
			}
		})
	}
}

func TestOpenMetadataConditionsNativeHTTPKeepEffectsAtomic(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef} {
		t.Run(string(operation), func(t *testing.T) {
			meta, backend := memoryfixture.New(t, "open-metadata", 1<<20, locking.DefaultOptions())
			if err := backend.Write(t.Context(), "file", []byte("keep")); err != nil {
				t.Fatal(err)
			}
			initial, err := backend.Stat(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			root, err := backend.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			handler, err := NewHandler(backend, meta)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			t.Cleanup(func() {
				server.Close()
				if err := handler.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			client, err := Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			session := opened.(*remoteFileSession)
			first, err := session.SetMetadata(t.Context(), initial.ID, "test.attributes", nil, []byte("first"))
			if err != nil {
				t.Fatal(err)
			}
			current, err := session.SetMetadata(t.Context(), initial.ID, "test.attributes", first.Version, []byte("current"))
			if err != nil {
				t.Fatal(err)
			}
			name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}
			target := storage.ChildCondition{State: storage.SameNode, NodeID: initial.ID}
			fileOptions := storage.OpenAtOptions{Read: true, Write: true, Target: target, Existing: storage.ResetContent, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}}
			nodeOptions := storage.NodeRefOptions{Kind: storage.NodeRegular, Target: target, MetadataAccess: storage.ReadMetadata}
			open := func(version []byte) (storage.NodeOpenResult, error) {
				conditions := map[string][]byte{"test.attributes": version, "test.absent": nil}
				if operation == storage.OpFileOpenAt {
					options := fileOptions
					options.ExpectedMetadata = conditions
					result, err := session.OpenAt(t.Context(), name, options)
					return storage.NodeOpenResult{Reference: result.File, Attr: result.Attr, Outcome: result.Outcome}, err
				}
				options := nodeOptions
				options.ExpectedMetadata = conditions
				if operation == storage.OpFileOpenNodeRef {
					return session.OpenNodeRef(t.Context(), initial.ID, options)
				}
				return session.OpenChildRef(t.Context(), name, options)
			}
			before, err := meta.Barrier(t.Context(), MaxIncarnationBytes)
			if err != nil {
				t.Fatal(err)
			}
			failed, err := open(first.Version)
			if !errors.Is(err, storage.ErrConditionConflict) || !errors.Is(err, syscall.EAGAIN) || failed.Reference != nil || failed.Attr.ID != 0 || failed.Outcome != 0 {
				t.Fatalf("stale metadata did not fail without a result: %+v %v", failed, err)
			}
			after, err := meta.Barrier(t.Context(), MaxIncarnationBytes)
			if err != nil || after.Position != before.Position {
				t.Fatalf("stale condition published: before=%+v after=%+v err=%v", before, after, err)
			}
			data, err := backend.Read(t.Context(), "file")
			if err != nil || string(data) != "keep" {
				t.Fatalf("stale condition changed bytes: %q %v", data, err)
			}
			unchanged, err := backend.Stat(t.Context(), "file")
			if err != nil || unchanged.ID != initial.ID || !bytes.Equal(unchanged.Metadata["test.attributes"].Version, current.Version) || string(unchanged.Metadata["test.attributes"].Data) != "current" {
				t.Fatalf("stale condition changed identity/metadata: %+v %v", unchanged, err)
			}
			if _, err := session.SetMetadata(t.Context(), initial.ID, "test.large", nil, bytes.Repeat([]byte{1}, 32<<10)); err != nil {
				t.Fatal(err)
			}
			before, err = meta.Barrier(t.Context(), MaxIncarnationBytes)
			if err != nil {
				t.Fatal(err)
			}
			request := openMetadataRequest(t, operation, session.id)
			request.ResultBytes = 1024
			if operation == storage.OpFileOpenAt {
				options := fileOptions
				options.ExpectedMetadata = map[string][]byte{"test.attributes": current.Version}
				request.OpenAt = openAtOptionsOf(options)
				request.Child = &name
			} else {
				options := nodeOptions
				options.ExpectedMetadata = map[string][]byte{"test.attributes": current.Version}
				request.NodeRef = nodeRefOptionsOf(options)
				if operation == storage.OpFileOpenNodeRef {
					request.Node = initial.ID
				} else {
					request.Child = &name
				}
			}
			response, err := session.call(t.Context(), request)
			if !errors.Is(err, syscall.EFBIG) || response.File != "" || response.Attr != nil {
				t.Fatalf("small returned-attribute budget admitted open: %+v %v", response, err)
			}
			after, err = meta.Barrier(t.Context(), MaxIncarnationBytes)
			if err != nil || after.Position != before.Position {
				t.Fatalf("budget failure published: before=%+v after=%+v err=%v", before, after, err)
			}
			data, err = backend.Read(t.Context(), "file")
			if err != nil || string(data) != "keep" {
				t.Fatalf("budget refusal changed bytes: %q %v", data, err)
			}
			result, err := open(current.Version)
			outcome, size := storage.Opened, int64(4)
			if operation == storage.OpFileOpenAt {
				outcome, size = storage.Reset, 0
			}
			if err != nil || result.Reference == nil || result.Attr.ID != initial.ID || result.Attr.Size != size || result.Outcome != outcome || !bytes.Equal(result.Attr.Metadata["test.attributes"].Version, current.Version) {
				t.Fatalf("current conditions changed capture: %+v %v", result, err)
			}
			if err := result.Reference.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMetadataCASFirstInsertPreservesSessionAndSibling(t *testing.T) {
	for _, replay := range []bool{false, true} {
		name := "nil version"
		if replay {
			name = "empty version replay"
		}
		t.Run(name, func(t *testing.T) {
			meta, backend := memoryfixture.New(t, "metadata-cas", 1<<20, locking.DefaultOptions())
			for _, path := range []string{"target", "sibling"} {
				if err := backend.Write(t.Context(), path, []byte("keep "+path)); err != nil {
					t.Fatal(err)
				}
			}
			handler, err := NewHandler(backend, meta)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			t.Cleanup(func() {
				server.Close()
				if err := handler.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			client, err := Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			var calls, badRequests, sessionCloses atomic.Int32
			next := client.http.Transport
			client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				body, err := request.GetBody()
				if err != nil {
					return nil, err
				}
				var requestValue struct {
					Op storage.Operation `json:"op"`
				}
				err = json.NewDecoder(body).Decode(&requestValue)
				_ = body.Close()
				if err != nil {
					return nil, err
				}
				if requestValue.Op == storage.OpFileMutate {
					calls.Add(1)
				}
				if requestValue.Op == storage.OpFileSessionClose {
					sessionCloses.Add(1)
				}
				response, err := next.RoundTrip(request)
				if response != nil && response.StatusCode == http.StatusBadRequest {
					badRequests.Add(1)
				}
				return response, err
			})
			opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			session := opened.(*remoteFileSession)
			target, err := session.OpenFile(t.Context(), "target", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
			if err != nil {
				t.Fatal(err)
			}
			sibling, err := session.OpenFile(t.Context(), "sibling", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			if err != nil {
				t.Fatal(err)
			}
			var version []byte
			if replay {
				version = []byte{}
				client.http.Transport = &loseCapabilityReply{next: client.http.Transport, op: storage.OpFileMutate}
			}
			command := storage.FileMutation{Kind: storage.MutateAttributes, Metadata: map[string]storage.OpaquePayload{"test.created": {Version: version, Data: []byte("first")}}}
			mutation := target.(storage.ConditionalFileMutation)
			created, err := mutation.MutateFile(t.Context(), command)
			wantCalls := int32(1)
			if replay {
				wantCalls = 2
			}
			payload, present := created.Metadata["test.created"]
			if err != nil || created.ID == 0 || !present || len(payload.Version) == 0 || string(payload.Data) != "first" || calls.Load() != wantCalls || badRequests.Load() != 0 || sessionCloses.Load() != 0 {
				t.Fatalf("first CAS: attr=%+v err=%v calls=%d badRequests=%d closes=%d", created, err, calls.Load(), badRequests.Load(), sessionCloses.Load())
			}
			assertUsable := func() {
				t.Helper()
				session.mu.Lock()
				failed, closed := session.failed, session.closed
				session.mu.Unlock()
				if failed != nil || closed {
					t.Fatalf("CAS fenced session: %v closed=%v", failed, closed)
				}
				read, err := sibling.ReadAt(t.Context(), 0, 32)
				if err != nil || string(read.Data) != "keep sibling" {
					t.Fatalf("sibling became unusable: %q %v", read.Data, err)
				}
				if _, err := session.Status(t.Context()); err != nil {
					t.Fatalf("session became unusable: %v", err)
				}
			}
			assertUsable()
			if result, err := mutation.MutateFile(t.Context(), command); !errors.Is(err, storage.ErrConditionConflict) || result.ID != 0 {
				t.Fatalf("absence CAS overwrote existing namespace: %+v %v", result, err)
			}
			command.Metadata["test.created"] = storage.OpaquePayload{Version: payload.Version}
			updated, err := mutation.MutateFile(t.Context(), command)
			current := updated.Metadata["test.created"]
			if err != nil || len(current.Version) == 0 || bytes.Equal(current.Version, payload.Version) || len(current.Data) != 0 {
				t.Fatalf("current-token CAS: %+v %v", updated, err)
			}
			if result, err := mutation.MutateFile(t.Context(), command); !errors.Is(err, storage.ErrConditionConflict) || result.ID != 0 {
				t.Fatalf("stale token accepted: %+v %v", result, err)
			}
			stamp := time.Unix(1234567890, 0)
			if _, err := mutation.MutateFile(t.Context(), storage.FileMutation{Kind: storage.MutateAttributes, Attr: storage.AttrChange{ModTime: &stamp}, ExpectedMetadata: map[string][]byte{"test.still_absent": nil}}); err != nil {
				t.Fatalf("independent absence predicate changed: %v", err)
			}
			assertUsable()
			stored, err := backend.Stat(t.Context(), "target")
			if err != nil || !bytes.Equal(stored.Metadata["test.created"].Version, current.Version) || len(stored.Metadata["test.created"].Data) != 0 {
				t.Fatalf("rejected CAS changed metadata: %+v %v", stored, err)
			}
			data, err := backend.Read(t.Context(), "target")
			if err != nil || string(data) != "keep target" {
				t.Fatalf("metadata CAS changed content: %q %v", data, err)
			}
		})
	}
}
