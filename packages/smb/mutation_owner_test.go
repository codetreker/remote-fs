package smb

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type mutationOwnerNode struct {
	storage.NodeReference
	mutate func(storage.FileMutation) (storage.Attr, error)
}

func (*mutationOwnerNode) CheckConditionalFileMutation() error { return nil }
func (n *mutationOwnerNode) MutateFile(_ context.Context, command storage.FileMutation) (storage.Attr, error) {
	return n.mutate(command)
}

func metadataOwnerFixture(t *testing.T, kind storage.NodeKind) (*Server, *tree, *fileHandle, *mutationOwnerNode, *ioOwnerSession, storage.FileMutation) {
	t.Helper()
	server, tree, h, _, session, command, _ := ioOwnerFixture(t)
	node := &mutationOwnerNode{}
	h.file, h.node, h.nodeKind = nil, node, kind
	stamp := time.Date(2026, 9, 28, 12, 30, 0, 0, time.UTC)
	data, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosHidden})
	if err != nil {
		t.Fatal(err)
	}
	command = storage.FileMutation{Action: command.Action, Kind: storage.MutateAttributes,
		Attr:     storage.AttrChange{BirthTime: &stamp, AccessTime: &stamp, ModTime: &stamp},
		Metadata: map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte("old"), Data: data}},
	}
	return server, tree, h, node, session, command
}

func metadataOwnerAttr(h *fileHandle, command storage.FileMutation) storage.Attr {
	return storage.Attr{ID: h.nodeID, Kind: h.nodeKind, BirthTime: command.Attr.BirthTime,
		AccessTime: *command.Attr.AccessTime, ModTime: *command.Attr.ModTime,
		Metadata: map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte("new"), Data: bytes.Clone(command.Metadata[windowsMetadataKey].Data)}},
	}
}

func TestMutationOwnerNodeReferenceReplaysExactDeepCopiedIntent(t *testing.T) {
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory} {
		t.Run(map[storage.NodeKind]string{storage.NodeRegular: "metadata-only", storage.NodeDirectory: "directory"}[kind], func(t *testing.T) {
			server, tree, h, node, session, command := metadataOwnerFixture(t, kind)
			original := cloneMutationCommand(command)
			calls := 0
			node.mutate = func(got storage.FileMutation) (storage.Attr, error) {
				calls++
				if !reflect.DeepEqual(got, original) {
					t.Fatalf("changed intent: %+v", got)
				}
				if calls == 1 {
					*got.Attr.BirthTime = time.Time{}
					got.Metadata[windowsMetadataKey].Data[4] = byte(dosArchive)
					got.Metadata[windowsMetadataKey].Version[0] = 'x'
					return storage.Attr{}, syscall.EIO
				}
				return metadataOwnerAttr(h, got), nil
			}
			session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Operation: storage.OpFileMutate, Outcome: storage.FileActionCompleted}, nil
			}
			owner, err := server.reserveMutationOwner(tree, h, 1, command, nil)
			if err != nil {
				t.Fatal(err)
			}
			*command.Attr.ModTime = time.Time{}
			command.Metadata[windowsMetadataKey].Data[4] = byte(dosSystem)
			command.Metadata[windowsMetadataKey].Version[0] = 'z'
			attr, err := owner.executeMutation(t.Context(), func(context.Context) error { return nil }, nil, nil)
			if err != nil || attr.ID != h.nodeID || attr.Kind != kind || calls != 2 {
				t.Fatalf("metadata replay: attr=%+v err=%v calls=%d", attr, err, calls)
			}
			if !owner.command.Attr.Empty() || owner.command.Metadata != nil || owner.command.ExpectedMetadata != nil || server.Status().RetainedWriteBytes != 0 {
				t.Fatal("settled metadata owner retained payload")
			}
			owner.recordResponse(ResponseSent)
			if server.Status().WriteOwners != 0 {
				t.Fatal("successful metadata owner retained diagnostic")
			}
		})
	}
}

func TestMutationOwnerBasicRetryRefreshesExistingMetadataCASVersion(t *testing.T) {
	server, tree, h, node, session, command := metadataOwnerFixture(t, storage.NodeDirectory)
	calls, observations := 0, 0
	priorAction := command.Action
	node.mutate = func(got storage.FileMutation) (storage.Attr, error) {
		calls++
		if calls == 1 {
			return storage.Attr{}, storage.ErrConditionConflict
		}
		if got.Action == priorAction || len(got.ExpectedMetadata) != 0 || !bytes.Equal(got.Metadata[windowsMetadataKey].Version, []byte("fresh")) {
			t.Fatalf("retry did not refresh metadata CAS version: %+v", got)
		}
		return metadataOwnerAttr(h, got), nil
	}
	session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileMutate, Outcome: storage.FileActionNotExecuted}, nil
	}
	owner, err := server.reserveMutationOwner(tree, h, 1, command, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.executeMutation(t.Context(), func(context.Context) error { return nil }, func(context.Context) (windowsMetadataCondition, error) {
		observations++
		return windowsMetadataCondition{Version: []byte("fresh")}, nil
	}, func() uint64 { return 1 })
	if err != nil || calls != 2 || observations != 1 || owner.pending() {
		t.Fatalf("retry failed: %v calls=%d observations=%d", err, calls, observations)
	}
}

func TestMutationOwnerBasicRetryKeepsCapturedIntent(t *testing.T) {
	server, tree, h, node, session, command := metadataOwnerFixture(t, storage.NodeDirectory)
	original := cloneMutationCommand(command)
	calls := 0
	node.mutate = func(got storage.FileMutation) (storage.Attr, error) {
		calls++
		if calls == 1 {
			return storage.Attr{}, storage.ErrConditionConflict
		}
		original.Action = got.Action
		update := original.Metadata[windowsMetadataKey]
		update.Version = []byte("fresh")
		original.Metadata[windowsMetadataKey] = update
		if !reflect.DeepEqual(got, original) {
			t.Fatalf("retry changed retained intent: %+v", got)
		}
		return metadataOwnerAttr(h, got), nil
	}
	session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileMutate, Outcome: storage.FileActionNotExecuted}, nil
	}
	owner, err := server.reserveMutationOwner(tree, h, 1, command, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.executeMutation(t.Context(), func(context.Context) error { return nil }, func(context.Context) (windowsMetadataCondition, error) {
		*command.Attr.ModTime = command.Attr.ModTime.Add(time.Second)
		command.Metadata[windowsMetadataKey].Data[4] = byte(dosArchive)
		command.Kind = storage.MutateTruncate
		return windowsMetadataCondition{Version: []byte("fresh")}, nil
	}, func() uint64 { return 1 })
	if err != nil || calls != 2 || owner.pending() {
		t.Fatalf("immutable retry err=%v calls=%d", err, calls)
	}
}

func TestMutationOwnerNodeUnknownPreservesAllPayloadUntilSettledRelease(t *testing.T) {
	server, tree, h, node, session, command := metadataOwnerFixture(t, storage.NodeDirectory)
	node.mutate = func(storage.FileMutation) (storage.Attr, error) { return storage.Attr{}, syscall.EIO }
	session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, Outcome: storage.FileActionUnknown}, nil
	}
	owner, err := server.reserveMutationOwner(tree, h, 1, command, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.executeMutation(t.Context(), func(context.Context) error { return nil }, nil, nil)
	if !errors.Is(err, syscall.EIO) || !owner.pending() {
		t.Fatalf("unknown was accepted: %v", err)
	}
	charged := server.Status().RetainedWriteBytes
	if charged < 72+storage.MaxObservationTokenBytes+len(command.Metadata[windowsMetadataKey].Data) {
		t.Fatalf("metadata retention undercharged: %d", charged)
	}
	owner.recordResponse(ResponseSent)
	_ = owner.retire(true, false)
	if server.Status().RetainedWriteBytes != charged || !reflect.DeepEqual(owner.command, command) {
		t.Fatal("partial release discarded metadata intent")
	}
	_ = owner.retire(true, true)
	if server.Status().RetainedWriteBytes != 0 || !owner.command.Attr.Empty() || owner.command.Metadata != nil || owner.command.ExpectedMetadata != nil {
		t.Fatal("terminal release kept metadata payload")
	}
	if len(server.Status().WriteFailures) != 1 {
		t.Fatal("unknown metadata was silently discarded")
	}
}

func TestMutationOwnerRejectsBadNodeResultsWithoutSettling(t *testing.T) {
	for _, flaw := range []string{"identity", "kind", "metadata", "birth", "access", "mod"} {
		t.Run(flaw, func(t *testing.T) {
			server, tree, h, node, session, command := metadataOwnerFixture(t, storage.NodeDirectory)
			node.mutate = func(got storage.FileMutation) (storage.Attr, error) {
				attr := metadataOwnerAttr(h, got)
				switch flaw {
				case "identity":
					attr.ID++
				case "kind":
					attr.Kind = storage.NodeRegular
				case "metadata":
					attr.Metadata[windowsMetadataKey].Data[4] = byte(dosSystem)
				case "birth":
					attr.BirthTime = nil
				case "access":
					attr.AccessTime = time.Time{}
				case "mod":
					attr.ModTime = time.Time{}
				}
				return attr, nil
			}
			session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Outcome: storage.FileActionUnknown}, nil
			}
			owner, err := server.reserveMutationOwner(tree, h, 1, command, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.executeMutation(t.Context(), func(context.Context) error { return nil }, nil, nil)
			if !errors.Is(err, syscall.EIO) || !owner.pending() || server.Status().RetainedWriteBytes == 0 {
				t.Fatalf("bad %s result settled: %v", flaw, err)
			}
		})
	}
}

func TestMutationOwnerBasicRootRetryPreservesObservedHiddenSystem(t *testing.T) {
	server, tree, h, node, session, command := metadataOwnerFixture(t, storage.NodeDirectory)
	h.identity.Backend.RootNodeID = h.nodeID
	owner, err := server.reserveMutationOwner(tree, h, 1, command, nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	node.mutate = func(got storage.FileMutation) (storage.Attr, error) {
		calls++
		if calls == 1 {
			return storage.Attr{}, storage.ErrConditionConflict
		}
		metadata, err := decodeWindowsMetadata(got.Metadata)
		if err != nil || metadata.Attributes != dosSystem || !bytes.Equal(got.Metadata[windowsMetadataKey].Version, []byte("fresh")) {
			t.Fatalf("root retry facts: %+v %v", got, err)
		}
		return metadataOwnerAttr(h, got), nil
	}
	session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileMutate, Outcome: storage.FileActionNotExecuted}, nil
	}
	_, err = owner.executeMutation(t.Context(), func(context.Context) error { return nil }, func(context.Context) (windowsMetadataCondition, error) {
		return windowsMetadataCondition{Version: []byte("fresh"), RootAttributes: dosSystem}, nil
	}, func() uint64 { return 1 })
	if err != nil || calls != 2 {
		t.Fatalf("root retry err=%v calls=%d", err, calls)
	}
	if owner.authorize != nil {
		t.Fatal("settled owner retained authorization capture")
	}
}

func TestMutationOwnerRejectsInvalidObservedRootAttributesAndToken(t *testing.T) {
	for _, test := range []struct {
		name      string
		root      bool
		condition windowsMetadataCondition
	}{
		{"nonroot-hidden", false, windowsMetadataCondition{RootAttributes: dosHidden}},
		{"root-archive", true, windowsMetadataCondition{RootAttributes: dosArchive}},
		{"oversized-version", true, windowsMetadataCondition{Version: make([]byte, storage.MaxObservationTokenBytes+1)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, tree, h, node, session, command := metadataOwnerFixture(t, storage.NodeDirectory)
			if test.root {
				h.identity.Backend.RootNodeID = h.nodeID
			}
			calls := 0
			node.mutate = func(storage.FileMutation) (storage.Attr, error) {
				calls++
				return storage.Attr{}, storage.ErrConditionConflict
			}
			session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Operation: storage.OpFileMutate, Outcome: storage.FileActionNotExecuted}, nil
			}
			owner, err := server.reserveMutationOwner(tree, h, 1, command, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.executeMutation(t.Context(), func(context.Context) error { return nil }, func(context.Context) (windowsMetadataCondition, error) {
				return test.condition, nil
			}, func() uint64 { return 1 })
			if !errors.Is(err, syscall.EIO) || calls != 1 || owner.pending() {
				t.Fatalf("invalid observation err=%v calls=%d", err, calls)
			}
		})
	}
}

func TestMutationOwnerTruncateRequiresExactSizeAndArchive(t *testing.T) {
	for _, flaw := range []string{"none", "size", "archive", "allocation"} {
		t.Run(flaw, func(t *testing.T) {
			server, tree, h, file, session, command, effects := ioOwnerFixture(t)
			h.nodeKind = storage.NodeRegular
			command.Kind, command.Data, command.Offset, command.Size = storage.MutateTruncate, nil, 0, 17
			file.mutate = func(storage.FileMutation) (storage.Attr, error) {
				attr := ioOwnerAttr(t)
				attr.Size = 17
				switch flaw {
				case "size":
					attr.Size++
				case "archive":
					attr.Metadata = nil
				case "allocation":
					attr.AllocationKnown = false
				}
				return attr, nil
			}
			session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Outcome: storage.FileActionUnknown}, nil
			}
			owner, err := server.reserveMutationOwner(tree, h, 1, command, effects)
			if err != nil {
				t.Fatal(err)
			}
			attr, err := owner.executeMutation(t.Context(), func(context.Context) error { return nil }, nil, nil)
			if flaw == "none" && (err != nil || owner.pending() || attr.Size != 17) || flaw != "none" && (!errors.Is(err, syscall.EIO) || !owner.pending()) {
				t.Fatalf("EOF %s accepted=%v pending=%v attr=%+v", flaw, err, owner.pending(), attr)
			}
		})
	}
}

func TestMutationOwnerMetadataReservationChargesBeforeDispatch(t *testing.T) {
	server, tree, h, node, _, command := metadataOwnerFixture(t, storage.NodeDirectory)
	node.mutate = func(storage.FileMutation) (storage.Attr, error) {
		t.Fatal("budget-refused mutation dispatched")
		return storage.Attr{}, nil
	}
	server.config.Limits.MaxRetainedWriteBytes = 72 + storage.MaxObservationTokenBytes + 7
	if _, err := server.reserveMutationOwner(tree, h, 1, command, nil); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("metadata payload budget refusal: %v", err)
	}
	if status := server.Status(); status.RetainedWriteBytes != 0 || status.WriteOwners != 0 {
		t.Fatalf("refused owner retained charge: %+v", status)
	}
}

func TestMutationOwnerRejectsUnsupportedKinds(t *testing.T) {
	for _, invalid := range []string{"directory-truncate", "unknown-kind", "uses", "effect-attrs", "other-namespace", "redundant-condition"} {
		t.Run(invalid, func(t *testing.T) {
			server, tree, h, _, _, command := metadataOwnerFixture(t, storage.NodeDirectory)
			switch invalid {
			case "directory-truncate":
				command.Kind = storage.MutateTruncate
			case "unknown-kind":
				h.nodeKind = storage.NodeSymlink
			case "uses":
				command.Uses = []storage.TargetUse{{}}
			case "effect-attrs":
				command.ContentEffects = []uint16{0}
			case "other-namespace":
				command.Metadata["other"] = command.Metadata[windowsMetadataKey]
				delete(command.Metadata, windowsMetadataKey)
			case "redundant-condition":
				command.ExpectedMetadata = map[string][]byte{windowsMetadataKey: []byte("old")}
			}
			if _, err := server.reserveMutationOwner(tree, h, 1, command, nil); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("invalid %s reservation: %v", invalid, err)
			}
		})
	}
}

func TestMutationOwnerRecoveryPreservesOriginalResultByteLimit(t *testing.T) {
	server, tree, h, node, session, command := metadataOwnerFixture(t, storage.NodeDirectory)
	limits := []int64{}
	node.mutate = nil
	boundNode := &boundedMutationOwnerNode{mutationOwnerNode: node, mutateBounded: func(ctx context.Context, got storage.FileMutation) (storage.Attr, error) {
		limit, ok := storage.AttrResultByteLimit(ctx)
		if !ok {
			t.Fatal("typed operation lost encoded result budget")
		}
		limits = append(limits, limit)
		attr := metadataOwnerAttr(h, got)
		scalar := attr.Clone()
		scalar.Metadata = nil
		if err := storage.CheckAttrResultBudget(ctx, scalar, 6); err != nil {
			return storage.Attr{}, err
		}
		if len(limits) == 1 {
			return storage.Attr{}, syscall.EIO
		}
		return attr, nil
	}}
	h.node = boundNode
	outcome := storage.FileActionUnknown
	session.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileMutate, Outcome: outcome}, nil
	}
	owner, err := server.reserveMutationOwner(tree, h, 1, command, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := storage.WithBoundedAttrResult(t.Context(), 2048, func(storage.Attr, int64) error { return nil })
	_, err = owner.executeMutation(ctx, func(context.Context) error { return nil }, nil, nil)
	if !errors.Is(err, syscall.EIO) || !owner.pending() {
		t.Fatalf("initial unknown: %v", err)
	}
	outcome = storage.FileActionCompleted
	recovery := storage.WithBoundedAttrResult(t.Context(), 1024, func(storage.Attr, int64) error { return syscall.EFBIG })
	if _, err := owner.reconcile(recovery); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(limits, []int64{2048, 2048}) || owner.pending() {
		t.Fatalf("recovery changed original byte ceiling: %v", limits)
	}
}

type boundedMutationOwnerNode struct {
	*mutationOwnerNode
	mutateBounded func(context.Context, storage.FileMutation) (storage.Attr, error)
}

func (n *boundedMutationOwnerNode) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	return n.mutateBounded(ctx, command)
}
