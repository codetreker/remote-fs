package httprest

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestHTTPSemanticActionsUseNativeEpochDespiteHTTPWindowSkew(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.History = 2 * time.Second
	value, err := client.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	session := value.(*remoteFileSession)
	t.Cleanup(func() {
		servedEpoch := uint64(1)
		session.mu.Lock()
		session.epoch = servedEpoch
		session.mu.Unlock()
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	handler.files.mu.Lock()
	served := handler.files.sessions[session.id]
	handler.files.mu.Unlock()
	t.Cleanup(func() { served.mu.Lock(); served.started = time.Now(); served.mu.Unlock() })
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The native authority exposes its exact epoch; wait for that observable
	// transition rather than deriving its clock from HTTP enrollment time.
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for status.ActionEpoch < 2 {
		select {
		case <-deadline.C:
			t.Fatal("native action epoch did not advance")
		case <-tick.C:
			status, err = session.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, test := range []struct {
		name    string
		advance time.Duration
	}{
		{"HTTP ahead", 3 * served.options.History},
		{"HTTP behind", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			current, err := session.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			served.mu.Lock()
			served.started = time.Now()
			if test.advance != 0 {
				served.started = time.Now().Add(-time.Duration(current.ActionEpoch+2) * served.options.History)
			}
			httpEpoch := served.epoch(time.Now())
			served.mu.Unlock()
			if httpEpoch == current.ActionEpoch {
				t.Fatalf("fixture did not skew epochs: HTTP=%d native=%d", httpEpoch, current.ActionEpoch)
			}
			openAction, err := storage.NewFileActionID(current.ActionEpoch)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := session.OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}, storage.OpenAtOptions{Read: true, Write: true, Target: storage.ChildCondition{State: storage.Any}, Existing: storage.Keep, MetadataAccess: storage.ReadMetadata | storage.WriteMetadata, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}, Action: openAction})
			if err != nil {
				t.Fatalf("native OpenAt rejected by HTTP epoch=%v", err)
			}
			file := opened.File.(*remoteFile)
			mutation := storage.FileMutation{Action: httpFileAction(t, session), Kind: storage.MutateTruncate, Size: 9}
			original, barrier, err := file.MutateFileWithBarrier(t.Context(), mutation)
			if err != nil || original.Size != 9 {
				t.Fatalf("native mutation=%+v err=%v", original, err)
			}
			served.mu.Lock()
			delete(served.actions, storage.LockRequestID(mutation.Action))
			served.dataActions--
			served.mu.Unlock()
			changed := time.Unix(234, 567).UTC()
			if err := backend.SetAttr(t.Context(), "file", storage.AttrChange{ModTime: &changed}); err != nil {
				t.Fatal(err)
			}
			replay, replayedBarrier, err := file.MutateFileWithBarrier(t.Context(), mutation)
			if err != nil || !reflect.DeepEqual(original, replay) || !reflect.DeepEqual(barrier, replayedBarrier) {
				t.Fatalf("native typed replay=%+v err=%v", replay, err)
			}
			current, err = session.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			mutation.Action, err = storage.NewFileActionID(current.ActionEpoch + 10)
			if err != nil {
				t.Fatal(err)
			}
			mutation.Size = 3
			if _, err := file.MutateFile(t.Context(), mutation); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("future native epoch=%v", err)
			}
			mutation.Action, err = storage.NewFileActionID(1)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.MutateFile(t.Context(), mutation); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("retired native epoch=%v", err)
			}
			receipt, err := session.QueryFileAction(t.Context(), mutation.Action)
			if err != nil || receipt.Outcome != storage.FileActionRetired {
				t.Fatalf("retired native receipt=%+v err=%v", receipt, err)
			}
			actual, err := backend.Stat(t.Context(), "file")
			if err != nil || actual.Size != 9 {
				t.Fatalf("invalid action mutated content=%+v err=%v", actual, err)
			}
			served.mu.Lock()
			served.started = time.Now()
			served.mu.Unlock()
			session.mu.Lock()
			session.epoch = 1
			session.mu.Unlock()
			if err := file.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
