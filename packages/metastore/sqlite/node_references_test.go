package sqlite

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func retainNodeReferenceForTest(t *testing.T, s *Store, id uint64, permissions storage.MetadataPermissions) metastore.NodeReference {
	t.Helper()
	var capability metastore.NodeReferences = s
	result, err := capability.OpenNodeRef(t.Context(), id, storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.Any}, MetadataAccess: permissions,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := result.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return result.Reference
}

func TestNodeReferenceCapabilitiesForwardMetadataConditionalChangesAndRetirement(t *testing.T) {
	s := pendingUnlinkStore(t)
	var nodes metastore.NodeReferences = s.Store
	if err := nodes.CheckNodeReferences(); err != nil {
		t.Fatal(err)
	}
	opened, err := nodes.OpenChildRef(t.Context(), namespaceName(s.root, "metadata"), storage.NodeRefOptions{
		Kind: storage.NodeRegular, Create: true, Target: storage.ChildCondition{State: storage.Absent},
		MetadataAccess: storage.ReadMetadata | storage.WriteMetadata, Use: storage.UseClaim{Uses: storage.DeleteName},
		InitialState: storage.InitialState{OnCreate: storage.InitialFields{Metadata: map[string][]byte{"test.base": []byte("base")}}},
	})
	if err != nil || opened.Outcome != storage.Created {
		t.Fatalf("metadata reference open=%+v error=%v", opened, err)
	}
	reference := opened.Reference
	t.Cleanup(func() {
		if err := reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, exposed := reference.(metastore.File); exposed {
		t.Fatal("metadata reference exposes the byte-file interface")
	}
	scoped := reference.(storage.ScopedReference)
	if err := scoped.CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	scope, err := scoped.Scope(t.Context())
	if err != nil || scope.Check() != nil {
		t.Fatalf("reference scope=%+v error=%v", scope, err)
	}
	stateAccess := reference.(metastore.ReferenceStateAccess)
	if err := stateAccess.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	metadata := reference.(storage.ReferenceMetadataAccess)
	if err := metadata.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	first, err := metadata.SetMetadata(t.Context(), "test.reference", nil, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	var byID storage.MetadataAccess = s.Store
	if err := byID.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	if _, err := byID.SetMetadata(t.Context(), uint64(opened.State.ID), "test.store", nil, []byte("store")); err != nil {
		t.Fatal(err)
	}
	modified := time.Unix(300, 40)
	if state, err := reference.SetAttr(t.Context(), storage.AttrChange{ModTime: &modified}); err != nil || !state.ModTime.Equal(modified) {
		t.Fatalf("reference attributes=%+v error=%v", state, err)
	}
	conditional := reference.(metastore.ConditionalFileMutation)
	if err := conditional.CheckConditionalFileMutation(); err != nil {
		t.Fatal(err)
	}
	changed := time.Unix(500, 60)
	command := storage.FileMutation{
		Kind: storage.MutateAttributes, Attr: storage.AttrChange{ChangeTime: &changed},
		Metadata: map[string]storage.OpaquePayload{"test.reference": {Version: first.Version, Data: []byte("two")}},
		Uses:     []storage.TargetUse{{NodeID: uint64(opened.State.ID), Scope: scope}},
	}
	state, err := conditional.MutateFile(t.Context(), command)
	if err != nil || state.ID != opened.State.ID || state.Revision != opened.State.Revision || state.ChangeTime == nil || !state.ChangeTime.Equal(changed) || !bytes.Equal(state.Metadata["test.reference"].Data, []byte("two")) || !bytes.Equal(state.Metadata["test.base"].Data, []byte("base")) || !bytes.Equal(state.Metadata["test.store"].Data, []byte("store")) {
		t.Fatalf("conditional metadata forwarding=%+v error=%v", state, err)
	}
	if _, err := conditional.MutateFile(t.Context(), command); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale conditional metadata=%v", err)
	}
	if _, err := conditional.MutateFile(t.Context(), storage.FileMutation{Kind: storage.MutateWriteAt}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata reference accepted a conditional byte call=%v", err)
	}
	if _, err := conditional.CommitMutation(t.Context(), storage.FileMutation{Kind: storage.MutateTruncate}, state.Revision, metastore.Object{}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata reference accepted a byte publication=%v", err)
	}
	deletion := reference.(metastore.DeleteIntent)
	if err := deletion.CheckDeleteIntent(); err != nil {
		t.Fatal(err)
	}
	pending, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile})
	if err != nil || !pending.PendingUnlink {
		t.Fatalf("node-reference pending=%+v error=%v", pending, err)
	}
	if current, err := deletion.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: pending.PendingGeneration}); err != nil || current.PendingUnlink {
		t.Fatalf("node-reference clear=%+v error=%v", current, err)
	}
	if current, err := stateAccess.State(t.Context()); err != nil || current.State.ID != opened.State.ID || current.PendingUnlink {
		t.Fatalf("reference state=%+v error=%v", current, err)
	}
	ordered := reference.(interface {
		Order(context.Context, func() error) error
	})
	calls := 0
	transition := func() error { calls++; return nil }
	if err := ordered.Order(t.Context(), transition); err != nil || calls != 1 {
		t.Fatalf("reference ordering=%v calls=%d", err, calls)
	}
	if err := reference.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := reference.Node(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired metadata reference remained readable=%v", err)
	}
	if err := ordered.Order(t.Context(), transition); !errors.Is(err, syscall.ESTALE) || calls != 1 {
		t.Fatalf("retired reference transition=%v calls=%d", err, calls)
	}
}

func TestNodeReferenceMetadataPermissionsRemainIndependentOfCapabilities(t *testing.T) {
	s := pendingUnlinkStore(t)
	node := namespaceCreate(t, s.Store, s.root, "file", storage.NameCreate)
	for _, test := range []struct {
		name        string
		permissions storage.MetadataPermissions
	}{
		{"none", 0}, {"read", storage.ReadMetadata}, {"write", storage.WriteMetadata},
	} {
		t.Run(test.name, func(t *testing.T) {
			reference := retainNodeReferenceForTest(t, s.Store, node.ID, test.permissions)
			stateAccess := reference.(metastore.ReferenceStateAccess)
			if err := stateAccess.CheckReferenceState(); err != nil {
				t.Fatal(err)
			}
			_, nodeErr := reference.Node(t.Context())
			_, stateErr := stateAccess.State(t.Context())
			if test.permissions&storage.ReadMetadata == 0 {
				if !errors.Is(nodeErr, syscall.EBADF) || !errors.Is(stateErr, syscall.EBADF) {
					t.Fatalf("missing read permission: Node=%v State=%v", nodeErr, stateErr)
				}
			} else if nodeErr != nil || stateErr != nil {
				t.Fatalf("read permission: Node=%v State=%v", nodeErr, stateErr)
			}
			at := time.Unix(600, 70)
			_, attrErr := reference.SetAttr(t.Context(), storage.AttrChange{AccessTime: &at})
			_, metadataErr := reference.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), "test."+test.name, nil, []byte(test.name))
			_, mutationErr := reference.(metastore.ConditionalFileMutation).MutateFile(t.Context(), storage.FileMutation{Kind: storage.MutateAttributes, Attr: storage.AttrChange{ModTime: &at}})
			if test.permissions&storage.WriteMetadata == 0 {
				if !errors.Is(attrErr, syscall.EBADF) || !errors.Is(metadataErr, syscall.EBADF) || !errors.Is(mutationErr, syscall.EBADF) {
					t.Fatalf("missing write permission: SetAttr=%v metadata=%v conditional=%v", attrErr, metadataErr, mutationErr)
				}
			} else if attrErr != nil || metadataErr != nil || mutationErr != nil {
				t.Fatalf("write permission: SetAttr=%v metadata=%v conditional=%v", attrErr, metadataErr, mutationErr)
			}
		})
	}
}

func nodeReferenceSessionContext(t *testing.T, s *Store) context.Context {
	t.Helper()
	coordinator, err := s.Advisory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := coordinator.NewSession(storage.DefaultFileSessionOptions(), func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Retire(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return metastore.WithReferenceSession(t.Context(), session)
}

func TestNodeReferenceDirectoryLookupPreservesBytesAndRejectsForeignOrClosedScopes(t *testing.T) {
	s := pendingUnlinkStore(t)
	var namespace metastore.NamespaceAccess = s.Store
	if err := namespace.CheckNamespaceAccess(); err != nil {
		t.Fatal(err)
	}
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	other := namespaceCreate(t, s.Store, s.root, "other", storage.NameMkdir)
	leaf := []byte{0xff, 'x'}
	child, err := namespace.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameCreate, Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: directory.ID}, RawLeaf: leaf},
		Target: storage.ChildCondition{State: storage.Absent},
	})
	if err != nil || child.Attr == nil {
		t.Fatalf("byte-name child=%+v error=%v", child, err)
	}
	first := nodeReferenceSessionContext(t, s.Store)
	second := nodeReferenceSessionContext(t, s.Store)
	var nodes metastore.NodeReferences = s.Store
	opened, err := nodes.OpenNodeRef(first, directory.ID, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID},
		MetadataAccess: storage.ReadMetadata, Use: storage.UseClaim{Uses: storage.ReadEntries},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	scoped := opened.Reference.(storage.ScopedReference)
	if err := scoped.CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	scope, err := scoped.Scope(first)
	if err != nil {
		t.Fatal(err)
	}
	name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: directory.ID, Scope: &scope}, RawLeaf: leaf}
	if attr, err := namespace.LookupAt(first, name); err != nil || attr.ID != child.Attr.ID {
		t.Fatalf("scoped byte lookup=%+v error=%v", attr, err)
	}
	view, err := namespace.ReadDirNode(first, name.Parent)
	if err != nil || view.Observation.ParentID != directory.ID || len(view.Entries) != 1 || !bytes.Equal(view.Entries[0].RawLeaf, leaf) || view.Entries[0].Attr.ID != child.Attr.ID {
		t.Fatalf("scoped directory=%+v error=%v", view, err)
	}
	if _, err := namespace.LookupAt(second, name); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("foreign session borrowed directory scope=%v", err)
	}
	wrongParent := name
	wrongParent.Parent.NodeID = other.ID
	if _, err := namespace.LookupAt(first, wrongParent); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("scope transferred to another directory=%v", err)
	}
	missing := name
	missing.RawLeaf = []byte("missing")
	if _, err := namespace.LookupAt(first, missing); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("missing exact leaf=%v", err)
	}
	if err := opened.Reference.Close(first); err != nil {
		t.Fatal(err)
	}
	if _, err := namespace.LookupAt(first, name); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("closed directory scope became anonymous=%v", err)
	}
	name.Parent.Scope = nil
	if attr, err := namespace.LookupAt(second, name); err != nil || attr.ID != child.Attr.ID {
		t.Fatalf("live raw parent identity=%+v error=%v", attr, err)
	}
}

func TestNodeReferenceTargetUseRequiresActualDeleteScopeForSelfExemption(t *testing.T) {
	s := pendingUnlinkStore(t)
	var opener metastore.AtomicFileOpener = s.Store
	if err := opener.CheckAtomicFileOpen(); err != nil {
		t.Fatal(err)
	}
	open := func(name string, use storage.UseClaim) metastore.OpenResult {
		result, err := opener.OpenAt(t.Context(), namespaceName(s.root, name), storage.OpenAtOptions{
			Read: true, Create: true, Existing: storage.Keep, Target: storage.ChildCondition{State: storage.Any}, Use: use,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := result.File.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		return result
	}
	owner := open("owned", storage.UseClaim{Uses: storage.ReadData | storage.DeleteName, Deny: storage.DeleteName})
	other := open("other", storage.UseClaim{Uses: storage.ReadData | storage.DeleteName})
	reader := open("owned", storage.UseClaim{Uses: storage.ReadData})
	ownerScope, err := owner.File.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	otherScope, err := other.File.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	readerScope, err := reader.File.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var namespace metastore.NamespaceAccess = s.Store
	command := storage.NameCommand{Kind: storage.NameRemove, Name: namespaceName(s.root, "owned"), Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(owner.State.ID)}}
	if _, err := namespace.MutateName(t.Context(), command); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("anonymous removal bypassed deny-delete=%v", err)
	}
	for _, test := range []struct {
		name  string
		use   storage.TargetUse
		cause error
	}{
		{"other node scope", storage.TargetUse{NodeID: uint64(owner.State.ID), Scope: otherScope}, storage.ErrInvalidScope},
		{"unused other target", storage.TargetUse{NodeID: uint64(other.State.ID), Scope: otherScope}, syscall.EINVAL},
		{"same node without delete use", storage.TargetUse{NodeID: uint64(owner.State.ID), Scope: readerScope}, syscall.EBADF},
	} {
		t.Run(test.name, func(t *testing.T) {
			command.Uses = []storage.TargetUse{test.use}
			if _, err := namespace.MutateName(t.Context(), command); !errors.Is(err, test.cause) {
				t.Fatalf("target use=%v want=%v", err, test.cause)
			}
			if attr, err := namespace.LookupAt(t.Context(), command.Name); err != nil || attr.ID != uint64(owner.State.ID) {
				t.Fatalf("refused scope changed target=%+v error=%v", attr, err)
			}
		})
	}
	command.Uses = []storage.TargetUse{{NodeID: uint64(owner.State.ID), Scope: ownerScope}}
	if _, err := namespace.MutateName(t.Context(), command); err != nil {
		t.Fatalf("actual delete scope did not receive self exemption=%v", err)
	}
	if state, err := owner.File.Node(t.Context()); err != nil || state.ID != owner.State.ID || !state.Detached {
		t.Fatalf("own removal lost retained identity=%+v error=%v", state, err)
	}
	if _, err := namespace.LookupAt(t.Context(), command.Name); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("own deletion left a name=%v", err)
	}
	if attr, err := namespace.LookupAt(t.Context(), namespaceName(s.root, "other")); err != nil || attr.ID != uint64(other.State.ID) {
		t.Fatalf("other scope's object changed=%+v error=%v", attr, err)
	}
}

func TestNodeReferenceOpenKindRefusalsPreserveActualNodeKinds(t *testing.T) {
	s := pendingUnlinkStore(t)
	regular := namespaceCreate(t, s.Store, s.root, "regular", storage.NameCreate)
	directory := namespaceCreate(t, s.Store, s.root, "directory", storage.NameMkdir)
	linked, err := s.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameSymlink, Name: namespaceName(s.root, "link"), Target: storage.ChildCondition{State: storage.Absent},
		Initial: storage.InitialFields{LinkTarget: []byte("regular")},
	})
	if err != nil || linked.Attr == nil {
		t.Fatalf("symlink fixture=%+v error=%v", linked, err)
	}
	var capability metastore.NodeReferences = s.Store
	for _, test := range []struct {
		name      string
		id        uint64
		requested storage.NodeKind
		cause     error
	}{
		{"regular", regular.ID, storage.NodeDirectory, syscall.ENOTDIR},
		{"regular", regular.ID, storage.NodeSymlink, syscall.EINVAL},
		{"directory", directory.ID, storage.NodeRegular, syscall.EISDIR},
		{"link", linked.Attr.ID, storage.NodeRegular, syscall.ELOOP},
		{"link", linked.Attr.ID, storage.NodeDirectory, syscall.ELOOP},
	} {
		for _, route := range []string{"identity", "child"} {
			t.Run(test.name+"/"+route+"/"+test.cause.Error(), func(t *testing.T) {
				options := storage.NodeRefOptions{Kind: test.requested, Target: storage.ChildCondition{State: storage.Any}, MetadataAccess: storage.ReadMetadata}
				var result metastore.NodeOpenResult
				var err error
				if route == "identity" {
					result, err = capability.OpenNodeRef(t.Context(), test.id, options)
				} else {
					result, err = capability.OpenChildRef(t.Context(), namespaceName(s.root, test.name), options)
				}
				if result.Reference != nil {
					if closeErr := result.Reference.Close(context.Background()); closeErr != nil {
						t.Error(closeErr)
					}
					t.Fatal("kind refusal returned a retained reference")
				}
				if !errors.Is(err, test.cause) {
					t.Fatalf("kind refusal=%v want=%v", err, test.cause)
				}
			})
		}
	}
	opened, err := capability.OpenNodeRef(t.Context(), linked.Attr.ID, storage.NodeRefOptions{
		Kind: storage.NodeSymlink, Target: storage.ChildCondition{State: storage.SameNode, NodeID: linked.Attr.ID}, MetadataAccess: storage.ReadMetadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	state, err := opened.Reference.(metastore.ReferenceStateAccess).State(t.Context())
	if err != nil || state.State.Kind != storage.NodeSymlink || state.State.ID != int64(linked.Attr.ID) || !bytes.Equal(state.State.LinkTarget, []byte("regular")) {
		t.Fatalf("explicit symlink reference=%+v error=%v", state, err)
	}
}

func TestNodeReferenceNativeCapabilityDiscoveryAndOrderedOwnershipLifetime(t *testing.T) {
	for _, exclusive := range []bool{true, false} {
		name := "exclusive"
		if !exclusive {
			name = "shared"
		}
		t.Run(name, func(t *testing.T) {
			config := lockingTestConfig(t)
			var store *Store
			var closeStore func() error
			if exclusive {
				opened, err := OpenLocking(t.Context(), config)
				if err != nil {
					t.Fatal(err)
				}
				store, closeStore = opened.Store, opened.Close
			} else {
				opened, err := OpenWithOptions(t.Context(), config.Database, config.Volume, 0, DefaultOptions())
				if err != nil {
					t.Fatal(err)
				}
				store, closeStore = opened, opened.Close
			}
			t.Cleanup(func() {
				if err := closeStore(); err != nil {
					t.Error(err)
				}
			})
			var files metastore.FileStore = store
			owners := files.(interface{ CheckUseOwners() error })
			ranges := files.(interface{ CheckRangeControl() error })
			maintenance := files.(storage.MaintenanceAccounting)
			for _, capability := range []struct {
				name  string
				check func() error
			}{
				{"use owners", owners.CheckUseOwners}, {"range control", ranges.CheckRangeControl},
				{"maintenance accounting", maintenance.CheckMaintenanceAccounting},
			} {
				err := capability.check()
				if exclusive && err != nil || !exclusive && !errors.Is(err, syscall.EOPNOTSUPP) {
					t.Fatalf("%s capability under %s ownership=%v", capability.name, name, err)
				}
			}
			initialized := 0
			initialize := func(used int64) {
				initialized++
				if used != 0 {
					t.Errorf("empty volume accounting initialized with %d bytes", used)
				}
			}
			err := maintenance.BindMaintenanceAccounting(t.Context(), storage.PublicationAccountingChain{}, initialize)
			if exclusive {
				if err != nil || initialized != 1 {
					t.Fatalf("exclusive accounting bind=%v initializations=%d", err, initialized)
				}
				if coordinator, err := files.Advisory(t.Context()); err != nil || coordinator == nil {
					t.Fatalf("exclusive advisory access=%v", err)
				}
			} else if !errors.Is(err, syscall.EOPNOTSUPP) || initialized != 0 {
				t.Fatalf("shared accounting bind=%v initializations=%d", err, initialized)
			}
			if err := closeStore(); err != nil {
				t.Fatal(err)
			}
			before := initialized
			if err := maintenance.BindMaintenanceAccounting(t.Context(), storage.PublicationAccountingChain{}, initialize); !errors.Is(err, syscall.ESTALE) || initialized != before {
				t.Fatalf("retired accounting bind=%v initializations=%d", err, initialized)
			}
			if _, err := files.Advisory(t.Context()); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("retired advisory access=%v", err)
			}
			if file, err := files.OpenFile(t.Context(), "missing", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); !errors.Is(err, syscall.ESTALE) {
				if file != nil {
					file.Close(context.Background())
				}
				t.Fatalf("retired file access=%v", err)
			}
		})
	}
}

func TestNodeReferenceMetadataConditionsRejectConcurrentChangeBeforeClaimsAndIntent(t *testing.T) {
	for _, observation := range []struct {
		name    string
		present bool
	}{{"version", true}, {"absence", false}} {
		for _, route := range []string{"child", "identity"} {
			t.Run(observation.name+"/"+route, func(t *testing.T) {
				runStaleMetadataOpen(t, observation.present, func(ctx context.Context, s *Store, name storage.ChildName, capture metadataOpenObservation) (metadataConditionOpenResult, error) {
					options := storage.NodeRefOptions{
						Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: capture.attr.ID},
						ExpectedMetadata: capture.expected, Guards: &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{capture.directory}},
						MetadataAccess: storage.ReadMetadata, Use: storage.UseClaim{Uses: storage.DeleteName, Deny: storage.DeleteName},
						CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
					}
					var result metastore.NodeOpenResult
					var err error
					if route == "child" {
						options.Create = true
						result, err = s.OpenChildRef(ctx, name, options)
					} else {
						options.MetadataAccess = 0
						result, err = s.OpenNodeRef(ctx, capture.attr.ID, options)
					}
					return metadataConditionOpenResult{reference: result.Reference, state: result.State, outcome: result.Outcome}, err
				})
			})
		}
	}
}

func TestNodeReferenceCurrentMetadataConditionsAllowUnrelatedChanges(t *testing.T) {
	for _, route := range []string{"child", "identity"} {
		t.Run(route, func(t *testing.T) {
			s := pendingUnlinkStore(t)
			node := namespaceCreate(t, s.Store, s.root, "file", storage.NameCreate)
			version, err := s.SetMetadata(t.Context(), node.ID, "test.attributes", nil, []byte{})
			if err != nil {
				t.Fatal(err)
			}
			view, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
			if err != nil {
				t.Fatal(err)
			}
			options := storage.NodeRefOptions{
				Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node.ID},
				ExpectedMetadata: map[string][]byte{"test.attributes": bytes.Clone(version.Version), "test.absent": nil},
				Guards:           &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{view.Observation}}, MetadataAccess: storage.ReadMetadata,
			}
			if _, err := s.SetMetadata(t.Context(), node.ID, "test.unrelated", nil, []byte("allowed")); err != nil {
				t.Fatal(err)
			}
			var result metastore.NodeOpenResult
			if route == "child" {
				result, err = s.OpenChildRef(t.Context(), namespaceName(s.root, "file"), options)
			} else {
				options.MetadataAccess = 0
				result, err = s.OpenNodeRef(t.Context(), node.ID, options)
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Reference == nil {
				t.Fatal("current metadata condition returned no reference")
			}
			t.Cleanup(func() {
				if err := result.Reference.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if result.Outcome != storage.Opened || uint64(result.State.ID) != node.ID || !bytes.Equal(result.State.Metadata["test.attributes"].Version, version.Version) || len(result.State.Metadata["test.attributes"].Data) != 0 || !bytes.Equal(result.State.Metadata["test.unrelated"].Data, []byte("allowed")) {
				t.Fatalf("current metadata reference=%+v", result)
			}
			if route == "identity" {
				if _, err := result.Reference.Node(t.Context()); !errors.Is(err, syscall.EBADF) {
					t.Fatalf("metadata condition granted attribute-read access=%v", err)
				}
			}
		})
	}
}
