package objectstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type inlineCloseReference interface {
	CloseWithResult(context.Context) (storage.ReferenceCloseResult, error)
}

func TestInlineNativeCloseSettlementCoversSessionAndBothReferenceKinds(t *testing.T) {
	volume, native := fileVolume(t, memory.New(), 8192, nil)
	if err := native.CheckInlineCloseSettlement(); err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	if err := session.(storage.InlineCloseSettlement).CheckInlineCloseSettlement(); err != nil {
		t.Fatal(err)
	}
	backend, err := volume.BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertSettled := func(result storage.ReferenceCloseResult, err error) {
		t.Helper()
		var marker *storage.CloseSettlementError
		if err != nil || !result.Released || errors.As(err, &marker) {
			t.Fatalf("native released close was unsettled: %+v, %v", result, err)
		}
	}
	for _, kind := range []string{"file", "node"} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%t", kind, explicit), func(t *testing.T) {
				var reference inlineCloseReference
				if kind == "file" {
					file, err := session.OpenFile(t.Context(), fmt.Sprintf("file-%t", explicit), storage.FileOpenOptions{
						OpenAccess: storage.OpenAccess{Read: true, Create: true},
					})
					if err != nil {
						t.Fatal(err)
					}
					reference = file
				} else {
					opened, err := session.(storage.NodeReferences).OpenNodeRef(t.Context(), backend.RootNodeID, storage.NodeRefOptions{
						Kind: storage.NodeDirectory, MetadataAccess: storage.ReadMetadata, Action: fileActionFor(t, session),
						Target: storage.ChildCondition{State: storage.SameNode, NodeID: backend.RootNodeID},
					})
					if err != nil {
						t.Fatal(err)
					}
					reference = opened.Reference
				}
				if !explicit {
					assertSettled(reference.CloseWithResult(t.Context()))
					return
				}
				closeActions := reference.(storage.ReferenceCloseActions)
				status, err := closeActions.CloseOwnerStatus(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				action, err := storage.NewFileActionID(status.CurrentEpoch)
				if err != nil {
					t.Fatal(err)
				}
				assertSettled(closeActions.CloseWithAction(t.Context(), storage.CloseAttempt{Action: action, Generation: status.NextGeneration}))
			})
		}
	}
	assertSettled(session.CloseWithResult(t.Context()))
}
