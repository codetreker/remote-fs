package sqlite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNodeReferenceClaimsConflictInBothAdmissionDirections(t *testing.T) {
	s := pendingUnlinkStore(t)
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory} {
		mutation := storage.NameCreate
		if kind == storage.NodeDirectory {
			mutation = storage.NameMkdir
		}
		node := namespaceCreate(t, s.Store, s.root, fmt.Sprintf("node-%d", kind), mutation)
		for _, uses := range []storage.Uses{storage.ReadData, storage.WriteData} {
			for _, denyFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("kind-%d/use-%d/deny-first-%t", kind, uses, denyFirst), func(t *testing.T) {
					first, second := storage.UseClaim{Uses: uses}, storage.UseClaim{Deny: uses}
					if denyFirst {
						first, second = second, first
					}
					options := storage.NodeRefOptions{Kind: kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node.ID}, Use: first}
					held, err := s.OpenNodeRef(t.Context(), node.ID, options)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := held.Reference.Close(context.Background()); err != nil {
							t.Error(err)
						}
					})
					count := s.fileDomain.files
					options.Use = second
					name := namespaceName(s.root, fmt.Sprintf("node-%d", kind))
					blocked, err := s.OpenChildRef(t.Context(), name, options)
					if !errors.Is(err, storage.ErrUseConflict) || blocked.Reference != nil || s.fileDomain.files != count {
						t.Fatalf("conflicting open=%+v err=%v refs=%d want=%d", blocked, err, s.fileDomain.files, count)
					}
					if err := held.Reference.Close(t.Context()); err != nil {
						t.Fatal(err)
					}
					if err := held.Reference.Close(t.Context()); err != nil {
						t.Fatal(err)
					}
					admitted, err := s.OpenChildRef(t.Context(), name, options)
					if err != nil {
						t.Fatalf("claim remained after close: %v", err)
					}
					if err := admitted.Reference.Close(t.Context()); err != nil {
						t.Fatal(err)
					}
					if s.fileDomain.files != count-1 {
						t.Fatalf("reference accounting after cleanup=%d", s.fileDomain.files)
					}
				})
			}
		}
	}
}

func TestNodeReferenceClaimsDoNotGrantByteOrMetadataPermissions(t *testing.T) {
	s := pendingUnlinkStore(t)
	node := namespaceCreate(t, s.Store, s.root, "file", storage.NameCreate)
	opened, err := s.OpenChildRef(t.Context(), namespaceName(s.root, "file"), storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node.ID},
		Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, ok := opened.Reference.(metastore.File); ok {
		t.Fatal("non-byte reference exposes File")
	}
	ref := opened.Reference.(*retainedNodeReference)
	scope, err := ref.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.StatNode(t.Context(), node.ID)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(20, 30)
	checks := map[string]func() error{
		"metadata read":  func() error { _, err := ref.Node(t.Context()); return err },
		"metadata write": func() error { _, err := ref.SetAttr(t.Context(), storage.AttrChange{ModTime: &at}); return err },
		"read capture": func() error {
			_, err := ref.file.Node(metastore.WithFileAccess(t.Context(), metastore.FileAccess{Uses: storage.ReadData, Length: 1}))
			return err
		},
		"write capture": func() error {
			_, err := ref.file.Node(metastore.WithFileAccess(t.Context(), metastore.FileAccess{Uses: storage.WriteData, Length: 1}))
			return err
		},
		"reserve": func() error { _, err := ref.file.Reserve(t.Context(), 1); return err },
		"commit": func() error {
			_, err := ref.file.Commit(t.Context(), opened.State.Revision, metastore.Object{Size: 1})
			return err
		},
		"scoped mutation": func() error {
			_, err := ref.MutateFile(t.Context(), storage.FileMutation{Kind: storage.MutateWriteAt, Uses: []storage.TargetUse{{NodeID: node.ID, Scope: scope}}})
			return err
		},
		"scoped publication": func() error {
			_, err := ref.CommitMutation(t.Context(), storage.FileMutation{Kind: storage.MutateTruncate, Size: 1, Uses: []storage.TargetUse{{NodeID: node.ID, Scope: scope}}}, opened.State.Revision, metastore.Object{Size: 1})
			return err
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("method permission bypass: %v", err)
			}
		})
	}
	after, err := s.StatNode(t.Context(), node.ID)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("denied methods changed node: %+v %v", after, err)
	}
}

func TestNodeReferenceClaimAdmissionFailureReleasesOnlyItsOwnClaim(t *testing.T) {
	s := pendingUnlinkStore(t)
	node := namespaceCreate(t, s.Store, s.root, "file", storage.NameCreate)
	options := storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node.ID}, Use: storage.UseClaim{Uses: storage.ReadData}}
	held, err := s.OpenNodeRef(t.Context(), node.ID, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := held.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	failure := errors.New("reference publication rejected")
	ctx := metastore.WithFilePublicationGuard(t.Context(), func() error { return failure })
	options.Use = storage.UseClaim{Uses: storage.WriteData}
	count := s.fileDomain.files
	failed, err := s.OpenNodeRef(ctx, node.ID, options)
	if !errors.Is(err, failure) || failed.Reference != nil || s.fileDomain.files != count {
		t.Fatalf("failed publication ownership=%+v error=%v refs=%d want=%d", failed, err, s.fileDomain.files, count)
	}
	options.Use = storage.UseClaim{Deny: storage.WriteData}
	allowed, err := s.OpenNodeRef(t.Context(), node.ID, options)
	if err != nil {
		t.Fatalf("failed claim leaked: %v", err)
	}
	if err := allowed.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	options.Use = storage.UseClaim{Deny: storage.ReadData}
	if denied, err := s.OpenNodeRef(t.Context(), node.ID, options); !errors.Is(err, storage.ErrUseConflict) || denied.Reference != nil {
		t.Fatalf("failure removed other reference's claim: %+v %v", denied, err)
	}
	if err := s.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	if denied, err := s.OpenNodeRef(t.Context(), node.ID, options); !errors.Is(err, storage.ErrUseConflict) || denied.Reference != nil {
		t.Fatalf("unlink lost retained claim: %+v %v", denied, err)
	}
	if err := held.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.fileDomain.files != count-1 {
		t.Fatalf("retired claim retained accounting=%d", s.fileDomain.files)
	}
	if _, err := s.StatNode(t.Context(), node.ID); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("last close left detached node: %v", err)
	}
}

func TestNodeReferenceDirectoryClaimsDoNotAuthorizeEnumerationOrImplicitTraversal(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	child := namespaceCreate(t, s.Store, int64(directory.ID), "existing", storage.NameCreate)
	options := storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID}, Use: storage.UseClaim{Deny: storage.ReadData | storage.ReadEntries | storage.WriteData}}
	held, err := s.OpenNodeRef(t.Context(), directory.ID, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := held.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	for _, uses := range []storage.Uses{storage.ReadData, storage.ReadEntries, storage.WriteData} {
		options.Use = storage.UseClaim{Uses: uses}
		if denied, err := s.OpenNodeRef(t.Context(), directory.ID, options); !errors.Is(err, storage.ErrUseConflict) || denied.Reference != nil {
			t.Fatalf("explicit claim %v=%+v %v", uses, denied, err)
		}
	}
	options.Use = storage.UseClaim{}
	metadata, err := s.OpenNodeRef(t.Context(), directory.ID, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	found, err := s.LookupAt(t.Context(), namespaceName(int64(directory.ID), "existing"))
	if err != nil || found.ID != child.ID {
		t.Fatalf("implicit traversal inferred claim: %+v %v", found, err)
	}
	namespaceCreate(t, s.Store, int64(directory.ID), "created", storage.NameCreate)
	if err := held.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	options.Use = storage.UseClaim{Uses: storage.ReadData | storage.WriteData}
	claimed, err := s.OpenNodeRef(t.Context(), directory.ID, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := claimed.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	scope, err := claimed.Reference.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: directory.ID, Scope: &scope}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("byte category claim granted enumeration: %v", err)
	}
}
