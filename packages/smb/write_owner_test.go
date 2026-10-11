package smb

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type ioOwnerFile struct {
	handleFileStub
	mutate   func(storage.FileMutation) (storage.Attr, error)
	syncCall func() error
}

func (f *ioOwnerFile) CheckConditionalFileMutation() error { return nil }
func (f *ioOwnerFile) MutateFile(_ context.Context, command storage.FileMutation) (storage.Attr, error) {
	return f.mutate(command)
}
func (f *ioOwnerFile) Sync(context.Context) error { return f.syncCall() }

type ioOwnerSession struct {
	*endpointFileSession
	query func(storage.FileActionID) (storage.FileActionReceipt, error)
}

func (s *ioOwnerSession) QueryFileAction(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.query(id)
}

func ioOwnerFixture(t *testing.T) (*Server, *tree, *fileHandle, *ioOwnerFile, *ioOwnerSession, storage.FileMutation, []storage.ContentMetadataEffect) {
	t.Helper()
	server := &Server{config: Config{Limits: DefaultLimits(), Authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })}}
	session := &ioOwnerSession{endpointFileSession: newEndpointFileSession()}
	file := &ioOwnerFile{}
	h := &fileHandle{file: file, nodeID: 41}
	export := &Export{server: server, share: Share{Volume: "v"}}
	tree := &tree{export: export, authority: &authoritySession{raw: session}}
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	effect := storage.ContentMetadataEffect{Namespace: windowsMetadataKey, PayloadBytes: 8, PresentPrefix: []byte("SMW\x01"), AbsentPayload: []byte{'S', 'M', 'W', 1, 0, 0, 0, 0}, ClearMask: []byte{0, 0, 0, 0, byte(dosNormal), 0, 0, 0}, SetMask: []byte{0, 0, 0, 0, byte(dosArchive), 0, 0, 0}}
	command := storage.FileMutation{Action: action, Kind: storage.MutateWriteAt, Offset: 2, Data: []byte("abc"), ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{windowsMetadataKey: nil}}
	return server, tree, h, file, session, command, []storage.ContentMetadataEffect{effect}
}

func ioOwnerAttr(t *testing.T) storage.Attr {
	t.Helper()
	data, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosArchive})
	if err != nil {
		t.Fatal(err)
	}
	return storage.Attr{ID: 41, Kind: storage.NodeRegular, Size: 5, AllocationKnown: true, Metadata: map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte("1"), Data: data}}}
}

func TestWriteOwnerCompletedReceiptRequiresSameTypedReplay(t *testing.T) {
	server, tree, h, file, session, command, effects := ioOwnerFixture(t)
	original := cloneWriteCommand(command)
	calls := 0
	file.mutate = func(got storage.FileMutation) (storage.Attr, error) {
		calls++
		if !reflect.DeepEqual(got, original) {
			t.Fatalf("changed command: %+v", got)
		}
		got.Data[0] = 'z'
		if calls == 1 {
			return storage.Attr{}, syscall.ENOSPC
		}
		return ioOwnerAttr(t), nil
	}
	session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileMutate, Outcome: storage.FileActionCompleted}, nil
	}
	owner, err := server.reserveWriteOwner(tree, h, 1, command, effects)
	if err != nil {
		t.Fatal(err)
	}
	command.Data[0] = 'x'
	effects[0].SetMask[4] = 0
	attr, err := owner.execute(t.Context(), func(context.Context) error { return nil }, nil, nil)
	if err != nil || attr.ID != 41 || calls != 2 || owner.pending() {
		t.Fatalf("replay result: %+v %v calls=%d", attr, err, calls)
	}
	owner.recordResponse(ResponseSent)
	if got := server.Status(); got.WriteOwners != 0 || got.RetainedWriteBytes != 0 || len(got.WriteFailures) != 0 {
		t.Fatalf("settled charge: %+v", got)
	}
}

func TestWriteOwnerUnknownRetainsPayloadUntilCompleteProof(t *testing.T) {
	for _, outcome := range []storage.FileActionOutcome{storage.FileActionUnknown, storage.FileActionRetired, storage.FileActionNotExecuted} {
		t.Run(string(rune(outcome+'0')), func(t *testing.T) {
			server, tree, h, file, session, command, effects := ioOwnerFixture(t)
			calls := 0
			file.mutate = func(storage.FileMutation) (storage.Attr, error) { calls++; return storage.Attr{}, syscall.ENOSPC }
			session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Outcome: outcome}, nil
			}
			owner, err := server.reserveWriteOwner(tree, h, 1, command, effects)
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.execute(t.Context(), func(context.Context) error { return nil }, func(context.Context) (map[string][]byte, error) {
				t.Fatal("unbound receipt reminted action")
				return nil, nil
			}, func() uint64 { return 1 })
			if !errors.Is(err, syscall.EIO) || !owner.pending() || calls != 1 {
				t.Fatalf("unknown result: %v pending=%v calls=%d", err, owner.pending(), calls)
			}
			before := server.Status().RetainedWriteBytes
			if before == 0 {
				t.Fatal("unknown payload uncharged")
			}
			owner.recordResponse(ResponseSent)
			_ = owner.retire(true, false)
			if server.Status().RetainedWriteBytes != before {
				t.Fatal("released before settlement")
			}
			if server.AcknowledgeWriteFailures([]WriteOwnerID{owner.id}) == nil {
				t.Fatal("acknowledged active failure")
			}
			_ = owner.retire(true, true)
			status := server.Status()
			if status.RetainedWriteBytes != 0 || len(status.WriteFailures) != 1 || status.WriteFailures[0].Execution != WriteExecutionUnknown || !status.WriteFailures[0].Terminal {
				t.Fatalf("terminal facts: %+v", status)
			}
			if server.writes.pendingError(tree.export) == nil {
				t.Fatal("failure did not block export stop")
			}
			if err := server.AcknowledgeWriteFailures([]WriteOwnerID{owner.id}); err != nil {
				t.Fatal(err)
			}
			if server.writes.pendingError(nil) != nil {
				t.Fatal("transferred facts still own stop")
			}
		})
	}
}

func TestWriteOwnerConditionRetriesRequireBoundRefusalAndRemainLimited(t *testing.T) {
	server, tree, h, file, session, command, effects := ioOwnerFixture(t)
	ids := make(map[storage.FileActionID]struct{})
	calls, observations := 0, 0
	file.mutate = func(got storage.FileMutation) (storage.Attr, error) {
		calls++
		ids[got.Action] = struct{}{}
		return storage.Attr{}, storage.ErrConditionConflict
	}
	session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileMutate, Outcome: storage.FileActionNotExecuted}, nil
	}
	owner, err := server.reserveWriteOwner(tree, h, 1, command, effects)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.execute(t.Context(), func(context.Context) error { return nil }, func(context.Context) (map[string][]byte, error) {
		observations++
		return map[string][]byte{windowsMetadataKey: []byte("new")}, nil
	}, func() uint64 { return 1 })
	if !errors.Is(err, storage.ErrConditionConflict) || calls != 3 || len(ids) != 3 || observations != 2 || owner.pending() {
		t.Fatalf("retry result: %v calls=%d ids=%d obs=%d", err, calls, len(ids), observations)
	}
	owner.recordResponse(ResponseSent)
	status := server.Status()
	if len(status.WriteFailures) != 1 || status.WriteFailures[0].Execution != WriteExecutionNotExecuted || status.RetainedWriteBytes != 0 {
		t.Fatalf("bound refusal facts: %+v", status)
	}
}

func TestWriteOwnerCompletedReceiptCannotHideTypedErrorOrMalformedResult(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "barrier", true: "wrong-object"}[malformed], func(t *testing.T) {
			server, tree, h, file, session, command, effects := ioOwnerFixture(t)
			calls := 0
			file.mutate = func(storage.FileMutation) (storage.Attr, error) {
				calls++
				attr := ioOwnerAttr(t)
				if malformed {
					attr.ID++
					return attr, nil
				}
				return attr, syscall.EIO
			}
			session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Operation: storage.OpFileMutate, Outcome: storage.FileActionCompleted}, nil
			}
			owner, err := server.reserveWriteOwner(tree, h, 1, command, effects)
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.execute(t.Context(), func(context.Context) error { return nil }, nil, nil)
			if !errors.Is(err, syscall.EIO) || !owner.pending() || calls != 2 || server.Status().RetainedWriteBytes == 0 {
				t.Fatalf("completed result accepted: %v calls=%d", err, calls)
			}
			if !malformed && (owner.captured.ID != 41 || owner.captured.Metadata != nil) {
				t.Fatal("partial captured facts missing or unbounded")
			}
		})
	}
}

func TestWriteOwnerRecoveryRechecksAuthorityBeforeQueryOrReplay(t *testing.T) {
	server, tree, h, file, session, command, effects := ioOwnerFixture(t)
	mutations, queries := 0, 0
	file.mutate = func(storage.FileMutation) (storage.Attr, error) { mutations++; return storage.Attr{}, syscall.EIO }
	session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		queries++
		return storage.FileActionReceipt{Action: id, Outcome: storage.FileActionUnknown}, nil
	}
	owner, err := server.reserveWriteOwner(tree, h, 1, command, effects)
	if err != nil {
		t.Fatal(err)
	}
	allowed := true
	authorize := func(context.Context) error {
		if !allowed {
			return authz.ErrDenied
		}
		return nil
	}
	_, _ = owner.execute(t.Context(), authorize, nil, nil)
	allowed = false
	_, err = owner.reconcile(t.Context())
	if !errors.Is(err, syscall.EIO) || !errors.Is(err, authz.ErrDenied) || mutations != 1 || queries != 1 || !owner.pending() {
		t.Fatalf("denied recovery dispatched: %v mutations=%d queries=%d", err, mutations, queries)
	}
}

func TestWriteOwnerPreDispatchRefusalHasNoMutationAndCanTransferFacts(t *testing.T) {
	server, tree, h, file, _, command, effects := ioOwnerFixture(t)
	file.mutate = func(storage.FileMutation) (storage.Attr, error) {
		t.Fatal("denied mutation executed")
		return storage.Attr{}, nil
	}
	owner, err := server.reserveWriteOwner(tree, h, 1, command, effects)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.execute(t.Context(), func(context.Context) error { return authz.ErrDenied }, nil, nil)
	if !errors.Is(err, authz.ErrDenied) || owner.pending() {
		t.Fatalf("denied admission: %v", err)
	}
	owner.recordResponse(ResponseSent)
	facts := server.Status().WriteFailures
	if len(facts) != 1 || facts[0].Execution != WriteExecutionNotExecuted || facts[0].ErrorCategory != WriteErrorDenied || server.Status().RetainedWriteBytes != 0 {
		t.Fatalf("denied facts: %+v", facts)
	}
	if err := server.AcknowledgeWriteFailures([]WriteOwnerID{owner.id}); err != nil {
		t.Fatal(err)
	}
}
