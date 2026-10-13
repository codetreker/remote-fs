package smb

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type typedMutationNode struct {
	storage.NodeReference
	set   func(storage.PendingUnlinkCommand) (storage.ReferenceState, error)
	clear func(storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error)
}

func (*typedMutationNode) CheckDeleteIntent() error { return nil }
func (n *typedMutationNode) SetPendingUnlink(_ context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return n.set(command)
}
func (n *typedMutationNode) ClearPendingUnlink(_ context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return n.clear(command)
}

type typedMutationSession struct {
	*ioOwnerSession
	mutate func(storage.NameCommand) (storage.NameResult, error)
}

func (*typedMutationSession) CheckNamespaceAccess() error { return nil }
func (*typedMutationSession) LookupAt(context.Context, storage.ChildName) (storage.Attr, error) {
	return storage.Attr{}, syscall.EOPNOTSUPP
}
func (s *typedMutationSession) MutateName(_ context.Context, command storage.NameCommand) (storage.NameResult, error) {
	return s.mutate(command)
}

func typedOwnerFixture(t *testing.T, variant string) (*Server, *tree, *fileHandle, *typedMutationSession, *typedMutationNode, mutationCommand) {
	t.Helper()
	server, tree, h, _, session, fileCommand, _ := ioOwnerFixture(t)
	node := &typedMutationNode{}
	h.file, h.node = nil, node
	raw := &typedMutationSession{ioOwnerSession: session}
	tree.authority.raw = raw
	uses := []storage.TargetUse{{NodeID: h.nodeID, Scope: storage.UseScope{Token: "reference-scope"}}}
	var command mutationCommand
	switch variant {
	case "rename", "remove":
		name := &storage.NameCommand{Action: fileCommand.Action, Kind: storage.NameRename,
			Name:    storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1, Scope: &storage.UseScope{Token: "source-parent-scope"}}, RawLeaf: []byte("old")},
			Initial: storage.InitialFields{Metadata: map[string][]byte{}},
			Target:  storage.ChildCondition{State: storage.SameNode, NodeID: h.nodeID, ExpectedMetadata: map[string][]byte{windowsMetadataKey: []byte("metadata-version")}},
			Uses:    uses, Guards: &storage.NamespaceGuards{RootID: 1,
				Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: []byte("revision")}},
				Edges:       []storage.ObservedEdge{{ParentID: 1, ChildID: h.nodeID, RawLeaf: []byte("old")}}},
			Destination: &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: 1, Scope: &storage.UseScope{Token: "target-parent-scope"}}, ObservedLeaf: []byte("new"), OutputLeaf: []byte("new"), Expected: storage.ChildCondition{State: storage.Absent}},
		}
		if variant == "remove" {
			name.Kind, name.Destination = storage.NameRemove, nil
		}
		command.name = name
	case "set":
		command.pending = &storage.PendingUnlinkCommand{Action: fileCommand.Action, Condition: storage.UnlinkFile, Uses: uses,
			ExpectedMetadata: map[string][]byte{windowsMetadataKey: []byte("metadata-version")}}
	case "clear":
		command.clear = &storage.ClearPendingUnlinkCommand{Action: fileCommand.Action, Generation: []byte("generation"), Uses: uses}
	default:
		t.Fatal(variant)
	}
	return server, tree, h, raw, node, command
}

func typedOwnerCalls(raw *typedMutationSession, node *typedMutationNode, fn func(mutationCommand) (storage.Attr, error)) {
	raw.mutate = func(command storage.NameCommand) (storage.NameResult, error) {
		attr, err := fn(mutationCommand{name: &command})
		if attr.ID == 0 {
			return storage.NameResult{}, err
		}
		return storage.NameResult{Attr: &attr}, err
	}
	node.set = func(command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
		attr, err := fn(mutationCommand{pending: &command})
		return storage.ReferenceState{Attr: attr, PendingUnlink: true, PendingGeneration: []byte("result-generation")}, err
	}
	node.clear = func(command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
		attr, err := fn(mutationCommand{clear: &command})
		return storage.ReferenceState{Attr: attr, PendingUnlink: true, PendingGeneration: []byte("close-intent-generation")}, err
	}
}

func mutateTypedPayload(command mutationCommand) {
	switch {
	case command.name != nil:
		command.name.Initial.Metadata["unused"] = []byte("changed-unused-field")
		command.name.Name.RawLeaf[0] = 'x'
		command.name.Name.Parent.Scope.Token = "changed-parent"
		command.name.Target.ExpectedMetadata[windowsMetadataKey][0] = 'x'
		command.name.Guards.Directories[0].Revision[0] = 'x'
		command.name.Guards.Edges[0].RawLeaf[0] = 'x'
		command.name.Uses[0].Scope.Token = "changed-scope"
		if command.name.Destination != nil {
			command.name.Destination.OutputLeaf[0] = 'x'
			command.name.Destination.Parent.Scope.Token = "changed-destination-parent"
		}
	case command.pending != nil:
		command.pending.ExpectedMetadata[windowsMetadataKey][0] = 'x'
		command.pending.Uses[0].Scope.Token = "changed-scope"
	case command.clear != nil:
		command.clear.Generation[0] = 'x'
		command.clear.Uses[0].Scope.Token = "changed-scope"
	}
}

func TestMutationOwnerTypedReplayRetainsExactCommandAndOperation(t *testing.T) {
	for _, variant := range []string{"rename", "remove", "set", "clear"} {
		t.Run(variant, func(t *testing.T) {
			server, tree, h, raw, node, command := typedOwnerFixture(t, variant)
			original := command.clone()
			calls := 0
			typedOwnerCalls(raw, node, func(got mutationCommand) (storage.Attr, error) {
				calls++
				if !reflect.DeepEqual(got, original) {
					t.Fatalf("changed typed replay: %+v", got)
				}
				mutateTypedPayload(got)
				if calls == 1 {
					return storage.Attr{}, syscall.EIO
				}
				if variant == "remove" {
					return storage.Attr{}, nil
				}
				return storage.Attr{ID: h.nodeID, Kind: h.nodeKind}, nil
			})
			raw.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Operation: original.operation(), Outcome: storage.FileActionCompleted}, nil
			}
			owner, err := server.reserveMutationCommand(tree, h, 1, command, nil)
			if err != nil {
				t.Fatal(err)
			}
			mutateTypedPayload(command)
			attr, err := owner.executeCommand(t.Context(), func(context.Context) error { return nil }, nil, nil)
			if err != nil || calls != 2 || owner.pending() || variant == "remove" && attr.ID != 0 || variant != "remove" && attr.ID != h.nodeID {
				t.Fatalf("typed result: %+v %v calls=%d", attr, err, calls)
			}
			owner.recordResponse(ResponseSent)
			if got := server.Status(); got.WriteOwners != 0 || got.RetainedWriteBytes != 0 || len(got.WriteFailures) != 0 {
				t.Fatalf("typed settlement: %+v", got)
			}
		})
	}
}

func TestMutationOwnerTypedUnknownUsesExistingCloseSettlement(t *testing.T) {
	for _, variant := range []string{"rename", "remove", "set", "clear"} {
		t.Run(variant, func(t *testing.T) {
			server, tree, h, raw, node, command := typedOwnerFixture(t, variant)
			original := command.clone()
			calls, queries := 0, 0
			recovery := false
			typedOwnerCalls(raw, node, func(got mutationCommand) (storage.Attr, error) {
				calls++
				if !reflect.DeepEqual(got, original) {
					t.Fatal("recovery changed original selection")
				}
				if !recovery {
					return storage.Attr{}, syscall.EIO
				}
				return storage.Attr{ID: h.nodeID, Kind: h.nodeKind}, nil
			})
			raw.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				queries++
				outcome := storage.FileActionRetired
				if recovery {
					outcome = storage.FileActionCompleted
				}
				return storage.FileActionReceipt{Action: id, Operation: original.operation(), Outcome: outcome}, nil
			}
			owner, err := server.reserveMutationCommand(tree, h, 1, command, nil)
			if err != nil {
				t.Fatal(err)
			}
			h.pendingWrite = owner
			_, err = owner.executeCommand(t.Context(), func(context.Context) error { return nil }, func(context.Context) (mutationCommand, error) {
				t.Fatal("retired receipt allowed reobservation")
				return mutationCommand{}, nil
			}, func() uint64 { return 1 })
			if !errors.Is(err, syscall.EIO) || !owner.pending() || calls != 1 {
				t.Fatalf("unknown result: %v calls=%d", err, calls)
			}
			owner.recordResponse(ResponseSent)
			before := server.Status().RetainedWriteBytes
			_ = retirePendingFileIO(h, true, false)
			if before == 0 || server.Status().RetainedWriteBytes != before || !owner.pending() {
				t.Fatal("native release discarded unsettled adapter payload")
			}
			if err := server.AcknowledgeWriteFailures([]WriteOwnerID{owner.id}); err == nil {
				t.Fatal("acknowledged unsettled responsibility")
			}
			recovery = true
			if err := settlePendingFileIO(t.Context(), h); err != nil {
				t.Fatal(err)
			}
			if calls != 2 || queries != 2 || owner.pending() || server.Status().RetainedWriteBytes != 0 {
				t.Fatal("close did not reconcile original typed command")
			}
			_ = retirePendingFileIO(h, true, true)
			facts := server.Status().WriteFailures
			if len(facts) != 1 || facts[0].Action != original.action() || facts[0].Operation != original.operation() ||
				facts[0].Execution != WriteExecutionCompleted || !facts[0].Terminal || !facts[0].ChainSettled {
				t.Fatalf("terminal typed facts: %+v", facts)
			}
			if err := server.AcknowledgeWriteFailures([]WriteOwnerID{owner.id}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMutationOwnerTypedRecoveryRequiresCurrentAuthorization(t *testing.T) {
	for _, variant := range []string{"rename", "remove", "set", "clear"} {
		t.Run(variant, func(t *testing.T) {
			server, tree, h, raw, node, command := typedOwnerFixture(t, variant)
			calls, queries := 0, 0
			typedOwnerCalls(raw, node, func(mutationCommand) (storage.Attr, error) {
				calls++
				return storage.Attr{}, syscall.EIO
			})
			raw.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				queries++
				return storage.FileActionReceipt{Action: id, Outcome: storage.FileActionUnknown}, nil
			}
			owner, err := server.reserveMutationCommand(tree, h, 1, command, nil)
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
			_, _ = owner.executeCommand(t.Context(), authorize, nil, nil)
			allowed = false
			_, err = owner.reconcile(t.Context())
			if !errors.Is(err, authz.ErrDenied) || !owner.pending() || calls != 1 || queries != 1 {
				t.Fatalf("revoked recovery: %v calls=%d queries=%d", err, calls, queries)
			}
			_ = owner.retire(true, true)
		})
	}
}

func TestMutationOwnerTypedCompletedReceiptRequiresValidResult(t *testing.T) {
	for _, variant := range []string{"rename", "remove", "set", "clear"} {
		for _, fault := range []string{"identity", "typed-contract", "barrier"} {
			t.Run(variant+"/"+fault, func(t *testing.T) {
				server, tree, h, raw, node, command := typedOwnerFixture(t, variant)
				calls := 0
				typedOwnerCalls(raw, node, func(mutationCommand) (storage.Attr, error) {
					calls++
					attr := storage.Attr{ID: h.nodeID, Kind: h.nodeKind}
					if fault == "identity" {
						attr.ID++
					}
					if fault == "barrier" {
						return attr, syscall.EIO
					}
					return attr, nil
				})
				if fault == "typed-contract" {
					switch variant {
					case "rename":
						raw.mutate = func(storage.NameCommand) (storage.NameResult, error) { calls++; return storage.NameResult{}, nil }
					case "remove":
						raw.mutate = func(storage.NameCommand) (storage.NameResult, error) {
							calls++
							return storage.NameResult{Attr: &storage.Attr{Kind: h.nodeKind}}, nil
						}
					case "set":
						node.set = func(storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
							calls++
							return storage.ReferenceState{Attr: storage.Attr{ID: h.nodeID, Kind: h.nodeKind}}, nil
						}
					case "clear":
						node.clear = func(storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
							calls++
							return storage.ReferenceState{Attr: storage.Attr{ID: h.nodeID, Kind: h.nodeKind}, PendingGeneration: []byte("unexpected")}, nil
						}
					}
				}
				raw.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
					return storage.FileActionReceipt{Action: id, Operation: command.operation(), Outcome: storage.FileActionCompleted}, nil
				}
				owner, err := server.reserveMutationCommand(tree, h, 1, command, nil)
				if err != nil {
					t.Fatal(err)
				}
				_, err = owner.executeCommand(t.Context(), func(context.Context) error { return nil }, nil, nil)
				if !errors.Is(err, syscall.EIO) || calls != 2 || !owner.pending() || server.Status().RetainedWriteBytes == 0 {
					t.Fatalf("invalid typed result settled: %v calls=%d pending=%v", err, calls, owner.pending())
				}
				_ = owner.retire(true, true)
			})
		}
	}
}

func largerOwnerNameCommand(command mutationCommand) mutationCommand {
	fresh := command.clone()
	fresh.name.Name.Parent.NodeID = 8
	fresh.name.Guards.Directories = nil
	fresh.name.Guards.Edges = nil
	for id := uint64(1); id <= 8; id++ {
		fresh.name.Guards.Directories = append(fresh.name.Guards.Directories, storage.DirectoryObservation{ParentID: id, Revision: bytes.Repeat([]byte{'r'}, storage.MaxObservationTokenBytes)})
		if id != 1 {
			fresh.name.Guards.Edges = append(fresh.name.Guards.Edges, storage.ObservedEdge{ParentID: id - 1, ChildID: id, RawLeaf: []byte("directory")})
		}
	}
	fresh.name.Guards.Edges = append(fresh.name.Guards.Edges, storage.ObservedEdge{ParentID: 8, ChildID: fresh.name.Target.NodeID, RawLeaf: bytes.Clone(fresh.name.Name.RawLeaf)})
	return fresh
}

func TestMutationOwnerNameRetryChargesOldAndFreshPeakBeforeMint(t *testing.T) {
	for _, oneByteShort := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact-peak", true: "one-byte-short"}[oneByteShort], func(t *testing.T) {
			server, tree, h, raw, node, command := typedOwnerFixture(t, "rename")
			fresh := largerOwnerNameCommand(command)
			if err := fresh.check(h, nil); err != nil {
				t.Fatal(err)
			}
			oldBytes, freshBytes := command.payloadBytes(nil), fresh.payloadBytes(nil)
			server.config.Limits.MaxRetainedWriteBytes = oldBytes + freshBytes
			if oneByteShort {
				server.config.Limits.MaxRetainedWriteBytes--
			}
			calls, mintCalls := 0, 0
			typedOwnerCalls(raw, node, func(got mutationCommand) (storage.Attr, error) {
				calls++
				if calls == 1 {
					return storage.Attr{}, storage.ErrConditionConflict
				}
				if got.action() == command.action() || got.name.Name.Parent.NodeID != 8 || server.Status().RetainedWriteBytes != freshBytes {
					t.Fatal("new dispatch used uncharged or unchanged selection")
				}
				return storage.Attr{ID: h.nodeID, Kind: h.nodeKind}, nil
			})
			raw.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Operation: command.operation(), Outcome: storage.FileActionNotExecuted}, nil
			}
			owner, err := server.reserveMutationCommand(tree, h, 1, command, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.executeCommand(t.Context(), func(context.Context) error { return nil }, func(context.Context) (mutationCommand, error) {
				return fresh, nil
			}, func() uint64 {
				mintCalls++
				if got := server.Status().RetainedWriteBytes; got != oldBytes+freshBytes {
					t.Fatalf("transition peak uncharged before mint: %d", got)
				}
				return 1
			})
			if owner.pending() || server.Status().RetainedWriteBytes != 0 {
				t.Fatal("retry left charge or active responsibility")
			}
			if oneByteShort {
				if !errors.Is(err, syscall.ENOMEM) || !errors.Is(err, storage.ErrConditionConflict) || calls != 1 || mintCalls != 0 {
					t.Fatalf("resource failure dispatched: %v calls=%d mints=%d", err, calls, mintCalls)
				}
				owner.recordResponse(ResponseSent)
				facts := server.Status().WriteFailures
				if len(facts) != 1 || facts[0].Action != command.action() || facts[0].Execution != WriteExecutionNotExecuted || facts[0].ErrorCategory != WriteErrorCondition {
					t.Fatalf("resource failure rewrote bound refusal: %+v", facts)
				}
			} else if err != nil || calls != 2 || mintCalls != 1 {
				t.Fatalf("exact peak rejected: %v calls=%d mints=%d", err, calls, mintCalls)
			}
		})
	}
}

func TestMutationOwnerNameRetryMintFailureReleasesPeakCharge(t *testing.T) {
	server, tree, h, raw, node, command := typedOwnerFixture(t, "rename")
	fresh := largerOwnerNameCommand(command)
	calls := 0
	typedOwnerCalls(raw, node, func(mutationCommand) (storage.Attr, error) {
		calls++
		return storage.Attr{}, storage.ErrConditionConflict
	})
	raw.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, Operation: command.operation(), Outcome: storage.FileActionNotExecuted}, nil
	}
	owner, err := server.reserveMutationCommand(tree, h, 1, command, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.executeCommand(t.Context(), func(context.Context) error { return nil }, func(context.Context) (mutationCommand, error) {
		return fresh, nil
	}, func() uint64 {
		if server.Status().RetainedWriteBytes != command.payloadBytes(nil)+fresh.payloadBytes(nil) {
			t.Fatal("mint attempted without peak admission")
		}
		return 0
	})
	if !errors.Is(err, syscall.EINVAL) || calls != 1 || owner.pending() || server.Status().RetainedWriteBytes != 0 || owner.command.action() != command.action() {
		t.Fatalf("mint failure did not preserve original action: %v calls=%d", err, calls)
	}
	owner.recordResponse(ResponseSent)
	facts := server.Status().WriteFailures
	if len(facts) != 1 || facts[0].Execution != WriteExecutionNotExecuted || facts[0].Action != command.action() {
		t.Fatalf("mint failure diagnostic: %+v", facts)
	}
}

func TestMutationOwnerTypedRetryRejectsChangedEffect(t *testing.T) {
	for _, variant := range []string{"rename", "remove", "set", "clear"} {
		t.Run(variant, func(t *testing.T) {
			server, tree, h, raw, node, command := typedOwnerFixture(t, variant)
			fresh := command.clone()
			switch variant {
			case "rename":
				fresh.name.Destination.OutputLeaf = []byte("other")
			case "remove":
				fresh.name.RequireNoOpenDescendants = true
			case "set":
				fresh.pending.Condition = storage.UnlinkIfEmpty
			case "clear":
				fresh.clear.Uses[0].Scope.Token = "another-reference"
			}
			calls := 0
			typedOwnerCalls(raw, node, func(mutationCommand) (storage.Attr, error) {
				calls++
				return storage.Attr{}, storage.ErrConditionConflict
			})
			raw.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Operation: command.operation(), Outcome: storage.FileActionNotExecuted}, nil
			}
			owner, err := server.reserveMutationCommand(tree, h, 1, command, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.executeCommand(t.Context(), func(context.Context) error { return nil }, func(context.Context) (mutationCommand, error) {
				return fresh, nil
			}, func() uint64 { t.Fatal("changed effect minted action"); return 1 })
			if !errors.Is(err, syscall.EINVAL) || calls != 1 || owner.pending() || owner.command.action() != command.action() {
				t.Fatalf("changed effect admitted: %v calls=%d", err, calls)
			}
		})
	}
}

func TestMutationOwnerUnionRequiresExactlyOneVariantBeforeReservation(t *testing.T) {
	server, tree, h, _, _, command := typedOwnerFixture(t, "rename")
	for _, invalid := range []mutationCommand{{}, {name: command.name, clear: &storage.ClearPendingUnlinkCommand{Action: command.action()}}} {
		if _, err := server.reserveMutationCommand(tree, h, 1, invalid, nil); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid union accepted: %v", err)
		}
	}
	if got := server.Status(); got.WriteOwners != 0 || got.RetainedWriteBytes != 0 {
		t.Fatalf("invalid union reserved responsibility: %+v", got)
	}
}

func TestMutationOwnerTypedReceiptCannotAuthorizeAnotherOperation(t *testing.T) {
	for _, variant := range []string{"rename", "remove", "set", "clear"} {
		t.Run(variant, func(t *testing.T) {
			server, tree, h, raw, node, command := typedOwnerFixture(t, variant)
			calls := 0
			typedOwnerCalls(raw, node, func(mutationCommand) (storage.Attr, error) {
				calls++
				return storage.Attr{}, storage.ErrConditionConflict
			})
			raw.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Operation: storage.OpFileMutate, Outcome: storage.FileActionNotExecuted}, nil
			}
			owner, err := server.reserveMutationCommand(tree, h, 1, command, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.executeCommand(t.Context(), func(context.Context) error { return nil }, func(context.Context) (mutationCommand, error) {
				t.Fatal("unrelated operation receipt allowed reobservation")
				return command, nil
			}, func() uint64 { return 1 })
			if !errors.Is(err, syscall.EIO) || calls != 1 || !owner.pending() || !reflect.DeepEqual(owner.command, command) {
				t.Fatalf("wrong operation receipt settled: %v calls=%d", err, calls)
			}
			_ = owner.retire(true, true)
		})
	}
}

func TestMutationOwnerPendingRetriesRefreshOnlyObservedConditions(t *testing.T) {
	for _, variant := range []string{"set", "clear"} {
		t.Run(variant, func(t *testing.T) {
			server, tree, h, raw, node, command := typedOwnerFixture(t, variant)
			fresh := command.clone()
			if fresh.pending != nil {
				fresh.pending.ExpectedMetadata[windowsMetadataKey] = []byte("fresh-metadata")
			} else {
				fresh.clear.Generation = []byte("fresh-generation")
			}
			calls := 0
			typedOwnerCalls(raw, node, func(got mutationCommand) (storage.Attr, error) {
				calls++
				if calls == 1 {
					return storage.Attr{}, storage.ErrConditionConflict
				}
				if got.action() == command.action() {
					t.Fatal("bound refusal reused the original action for new conditions")
				}
				copy := fresh.clone()
				copy.setAction(got.action())
				if !reflect.DeepEqual(got, copy) {
					t.Fatal("retry changed effect or reference authority")
				}
				return storage.Attr{ID: h.nodeID, Kind: h.nodeKind}, nil
			})
			raw.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Operation: command.operation(), Outcome: storage.FileActionNotExecuted}, nil
			}
			owner, err := server.reserveMutationCommand(tree, h, 1, command, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.executeCommand(t.Context(), func(context.Context) error { return nil }, func(context.Context) (mutationCommand, error) {
				return fresh, nil
			}, func() uint64 { return 1 })
			if err != nil || calls != 2 || owner.pending() || server.Status().RetainedWriteBytes != 0 {
				t.Fatalf("pending retry: %v calls=%d", err, calls)
			}
		})
	}
}

func TestMutationOwnerTypedTwoBoundRetriesAssignOwnerActions(t *testing.T) {
	for _, variant := range []string{"rename", "remove", "set", "clear"} {
		t.Run(variant, func(t *testing.T) {
			server, tree, h, raw, node, command := typedOwnerFixture(t, variant)
			fresh := command.clone()
			calls, observations := 0, 0
			ids := make(map[storage.FileActionID]struct{})
			typedOwnerCalls(raw, node, func(got mutationCommand) (storage.Attr, error) {
				calls++
				ids[got.action()] = struct{}{}
				if calls <= maxMutationConditionRetries {
					return storage.Attr{}, storage.ErrConditionConflict
				}
				return storage.Attr{ID: h.nodeID, Kind: h.nodeKind}, nil
			})
			raw.query = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, Operation: command.operation(), Outcome: storage.FileActionNotExecuted}, nil
			}
			owner, err := server.reserveMutationCommand(tree, h, 1, command, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.executeCommand(t.Context(), func(context.Context) error { return nil }, func(context.Context) (mutationCommand, error) {
				observations++
				return fresh, nil
			}, func() uint64 { return 1 })
			if err != nil || calls != maxMutationConditionRetries+1 || len(ids) != calls || observations != maxMutationConditionRetries || owner.pending() || server.Status().RetainedWriteBytes != 0 {
				t.Fatalf("typed retry actions: %v calls=%d ids=%d observations=%d", err, calls, len(ids), observations)
			}
		})
	}
}
