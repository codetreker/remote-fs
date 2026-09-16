package httprest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
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
