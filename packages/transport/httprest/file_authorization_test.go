package httprest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func TestCompoundFileAuthorizationChecksEveryIntent(t *testing.T) {
	stamp := time.Unix(4, 5)
	for _, test := range []struct {
		name    string
		request fileRequest
		deny    storage.Operation
	}{
		{"replace deletes old node", fileRequest{Op: storage.OpFileOpenAt, OpenAt: openAtOptionsOf(storage.OpenAtOptions{Existing: storage.ReplaceNode})}, storage.OpVolumeRemove},
		{"open arms deletion", fileRequest{Op: storage.OpFileOpenAt, OpenAt: openAtOptionsOf(storage.OpenAtOptions{CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose}})}, storage.OpFileSetPendingUnlink},
		{"reference arms deletion", fileRequest{Op: storage.OpFileOpenChildRef, NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose}})}, storage.OpFileSetPendingUnlink},
		{"reset changes attributes", fileRequest{Op: storage.OpFileOpenAt, OpenAt: openAtOptionsOf(storage.OpenAtOptions{Existing: storage.ResetContent, Initial: storage.InitialState{OnReset: storage.InitialFields{Attr: storage.AttrChange{ModTime: &stamp}}}})}, storage.OpFileSetAttr},
		{"reset changes metadata", fileRequest{Op: storage.OpFileOpenAt, OpenAt: openAtOptionsOf(storage.OpenAtOptions{Existing: storage.ResetContent, Initial: storage.InitialState{OnReset: storage.InitialFields{Metadata: map[string][]byte{"test.v1": {1}}}}})}, storage.OpFileSetMetadata},
		{"conditional truncates", fileRequest{Op: storage.OpFileMutate, Mutation: fileMutationOf(storage.FileMutation{Kind: storage.MutateTruncate})}, storage.OpFileTruncate},
		{"conditional metadata", fileRequest{Op: storage.OpFileMutate, Mutation: fileMutationOf(storage.FileMutation{Metadata: map[string]storage.OpaquePayload{"test.v1": {Version: []byte{1}, Data: []byte{2}}}})}, storage.OpFileSetMetadata},
		{"namespace canonical remove", fileRequest{Op: storage.OpFileMutateName, Name: nameCommandOf(storage.NameCommand{Kind: storage.NameRemove})}, storage.OpVolumeRemove},
		{"namespace canonical symlink", fileRequest{Op: storage.OpFileMutateName, Name: nameCommandOf(storage.NameCommand{Kind: storage.NameSymlink})}, storage.OpVolumeCreate},
	} {
		t.Run(test.name, func(t *testing.T) {
			seen := false
			handler := &Handler{volume: "trusted", stopping: make(chan struct{}), authorizer: authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
				if request.Volume != "trusted" {
					t.Fatalf("untrusted volume %q", request.Volume)
				}
				if request.Operation == test.deny {
					seen = true
					return authz.ErrDenied
				}
				return nil
			})}
			err := handler.authorizeFile(context.Background(), test.request)
			if !seen || !errors.Is(err, authz.ErrDenied) {
				t.Fatalf("compound intent was not refused: seen=%v, err=%v", seen, err)
			}
		})
	}
}

func TestNodeReferenceAuthorizationIncludesCompatibilityClaims(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef} {
		for _, test := range []struct {
			name        string
			use         storage.UseClaim
			metadata    storage.MetadataPermissions
			read, write bool
		}{
			{"data read", storage.UseClaim{Uses: storage.ReadData}, 0, true, false},
			{"enumeration", storage.UseClaim{Uses: storage.ReadEntries}, 0, true, false},
			{"data write", storage.UseClaim{Uses: storage.WriteData}, 0, false, true},
			{"metadata read", storage.UseClaim{}, storage.ReadMetadata, true, false},
			{"metadata write", storage.UseClaim{}, storage.WriteMetadata, false, true},
			{"combined", storage.UseClaim{Uses: storage.ReadData}, storage.WriteMetadata, true, true},
			{"deny only", storage.UseClaim{Deny: storage.ReadData | storage.WriteData | storage.ReadEntries}, 0, false, false},
			{"delete claim", storage.UseClaim{Uses: storage.DeleteName}, 0, false, false},
		} {
			t.Run(string(operation)+"/"+test.name, func(t *testing.T) {
				seen := 0
				h := &Handler{volume: "trusted", stopping: make(chan struct{}), authorizer: authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
					seen++
					want := storage.OpenAccess{Read: test.read, Write: test.write, Create: true, Exclusive: true}
					if request.Operation != operation || request.Open != want || request.Volume != "trusted" {
						t.Fatalf("open intent=%+v want=%+v", request, want)
					}
					return nil
				})}
				if err := h.authorizeFile(t.Context(), fileRequest{Op: operation, NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{Use: test.use, MetadataAccess: test.metadata, Create: true, Exclusive: true})}); err != nil || seen != 1 {
					t.Fatalf("authorization=%v calls=%d", err, seen)
				}
			})
		}
	}
}

type claimAuthorizationStorage struct {
	*objectstore.Storage
	opens atomic.Int64
}

type claimAuthorizationSession struct {
	storage.FileSession
	storage.NodeReferences
	opens *atomic.Int64
}

func (s *claimAuthorizationStorage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := s.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &claimAuthorizationSession{FileSession: session, NodeReferences: session.(storage.NodeReferences), opens: &s.opens}, nil
}

func (s *claimAuthorizationSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.opens.Add(1)
	return s.NodeReferences.OpenNodeRef(ctx, id, options)
}

func (s *claimAuthorizationSession) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.opens.Add(1)
	return s.NodeReferences.OpenChildRef(ctx, name, options)
}

func TestNodeReferenceHTTPDeniesClaimBeforeNativeOpen(t *testing.T) {
	_, backend := memoryfixture.New(t, "claims", 1<<20, locking.DefaultOptions())
	if err := backend.Mkdir(t.Context(), "dir"); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := backend.Stat(t.Context(), "dir")
	if err != nil {
		t.Fatal(err)
	}
	probe := &claimAuthorizationStorage{Storage: backend}
	var deny atomic.Bool
	options := DefaultHandlerOptions()
	options.Volume = "claims"
	options.Authorizer = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		if deny.Load() && (request.Open.Read || request.Open.Write) {
			return authz.ErrDenied
		}
		return nil
	})
	h, err := NewHandlerWithOptions(probe, nil, options)
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
	session := fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileSessionOpen, Options: storage.DefaultFileSessionOptions()})
	for _, operation := range []storage.Operation{storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef} {
		for _, uses := range []storage.Uses{storage.ReadData, storage.ReadEntries, storage.WriteData} {
			action, err := storage.NewLockRequestID(session.Epoch)
			if err != nil {
				t.Fatal(err)
			}
			req := fileRequest{Op: operation, Session: session.Session, Action: action, NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID}, Use: storage.UseClaim{Uses: uses}})}
			if operation == storage.OpFileOpenNodeRef {
				req.Node = directory.ID
			} else {
				req.Child = &storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("dir")}
			}
			before := probe.opens.Load()
			deny.Store(true)
			fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, req), "EACCES", "access denied")
			if probe.opens.Load() != before {
				t.Fatal("denied claim reached native open")
			}
			deny.Store(false)
			opened := fileAuthorizationSuccess(t, h, req)
			if probe.opens.Load() != before+1 || opened.Attr == nil || opened.Attr.ID != directory.ID {
				t.Fatalf("allowed claim failed native path: %+v count=%d", opened, probe.opens.Load())
			}
			fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileClose, Session: session.Session, File: opened.File})
		}
	}
	action, err := storage.NewLockRequestID(session.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	held := fileRequest{Op: storage.OpFileOpenNodeRef, Session: session.Session, Action: action, Node: directory.ID, NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID}, Use: storage.UseClaim{Uses: storage.ReadData}})}
	fileAuthorizationSuccess(t, h, held)
	fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileSessionClose, Session: session.Session})
	replacement := fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileSessionOpen, Options: storage.DefaultFileSessionOptions()})
	held.Session = replacement.Session
	held.Action, err = storage.NewLockRequestID(replacement.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	held.NodeRef.Use = storage.UseClaim{Deny: storage.ReadData}
	opened := fileAuthorizationSuccess(t, h, held)
	fileAuthorizationSuccess(t, h, fileRequest{Op: storage.OpFileClose, Session: replacement.Session, File: opened.File})
}
