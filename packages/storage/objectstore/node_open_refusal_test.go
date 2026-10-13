package objectstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type nodeOpenProofAuthority struct {
	*sqlite.LockingStore
	open  func(context.Context, uint64, storage.NodeRefOptions) (metastore.NodeOpenResult, error)
	calls int
}

func (a *nodeOpenProofAuthority) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (metastore.NodeOpenResult, error) {
	a.calls++
	if a.open != nil {
		return a.open(ctx, id, options)
	}
	return a.LockingStore.OpenNodeRef(ctx, id, options)
}

func nodeOpenProofVolume(t *testing.T) (*objectstore.Storage, *nodeOpenProofAuthority) {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "volume.db"), Volume: "open-proof", SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	authority := &nodeOpenProofAuthority{LockingStore: meta}
	volume := objectstore.New(memory.New(), authority)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("closing proof volume: %v", err)
		}
	})
	return volume, authority
}

func TestNodeOpenRefusalProofPreservesJournalAndReferenceOwnership(t *testing.T) {
	for _, test := range []struct {
		name       string
		proof      bool
		cause      error
		state      metastore.FileState
		outcome    storage.OpenOutcome
		reference  bool
		wantErrno  syscall.Errno
		wantRecord storage.FileActionOutcome
	}{
		{name: "canceled admission", proof: true, cause: context.Canceled, wantErrno: syscall.EINTR, wantRecord: storage.FileActionNotExecuted},
		{name: "deadline admission", proof: true, cause: context.DeadlineExceeded, wantErrno: syscall.EIO, wantRecord: storage.FileActionNotExecuted},
		{name: "resource admission", proof: true, cause: syscall.EAGAIN, wantErrno: syscall.EAGAIN, wantRecord: storage.FileActionNotExecuted},
		{name: "cancellation without proof", cause: context.Canceled, wantErrno: syscall.EINTR, wantRecord: storage.FileActionUnknown},
		{name: "known refusal without proof", cause: storage.ErrConditionConflict, wantErrno: syscall.EAGAIN, wantRecord: storage.FileActionNotExecuted},
		{name: "proof without error", proof: true, wantErrno: syscall.EIO, wantRecord: storage.FileActionUnknown},
		{name: "proof with state", proof: true, cause: context.Canceled, state: metastore.FileState{Node: metastore.Node{Kind: storage.NodeDirectory}}, wantErrno: syscall.EIO, wantRecord: storage.FileActionUnknown},
		{name: "invalid proof with condition conflict", proof: true, cause: storage.ErrConditionConflict, state: metastore.FileState{Node: metastore.Node{Kind: storage.NodeDirectory}}, wantErrno: syscall.EIO, wantRecord: storage.FileActionUnknown},
		{name: "invalid proof with use conflict", proof: true, cause: storage.ErrUseConflict, state: metastore.FileState{Node: metastore.Node{Kind: storage.NodeDirectory}}, wantErrno: syscall.EIO, wantRecord: storage.FileActionUnknown},
		{name: "invalid proof with range conflict", proof: true, cause: storage.ErrRangeConflict, state: metastore.FileState{Node: metastore.Node{Kind: storage.NodeDirectory}}, wantErrno: syscall.EIO, wantRecord: storage.FileActionUnknown},
		{name: "invalid proof with invalid scope", proof: true, cause: storage.ErrInvalidScope, state: metastore.FileState{Node: metastore.Node{Kind: storage.NodeDirectory}}, wantErrno: syscall.EIO, wantRecord: storage.FileActionUnknown},
		{name: "proof with outcome", proof: true, cause: context.Canceled, outcome: storage.Opened, wantErrno: syscall.EIO, wantRecord: storage.FileActionUnknown},
		{name: "proof with retained reference", proof: true, cause: context.Canceled, reference: true, wantErrno: syscall.EIO, wantRecord: storage.FileActionUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			volume, authority := nodeOpenProofVolume(t)
			meta := authority.LockingStore
			root, err := volume.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			authority.open = func(ctx context.Context, id uint64, options storage.NodeRefOptions) (metastore.NodeOpenResult, error) {
				result := metastore.NodeOpenResult{State: test.state, Outcome: test.outcome}
				if test.reference {
					var err error
					result, err = meta.OpenNodeRef(ctx, id, options)
					if err != nil {
						t.Fatal(err)
					}
				}
				result.NotExecuted = test.proof
				return result, test.cause
			}
			session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
			options := storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: root.ID}, MetadataAccess: storage.ReadMetadata, Use: storage.UseClaim{Uses: storage.ReadEntries}, Action: fileActionFor(t, session)}
			opener := session.(storage.NodeReferences)
			first, err := opener.OpenNodeRef(t.Context(), root.ID, options)
			if storage.ErrnoOf(err) != test.wantErrno || test.cause != nil && !errors.Is(err, test.cause) || (first.Reference != nil) != test.reference {
				t.Fatalf("open proof: result=%+v error=%T: %v", first, err, err)
			}
			originalText := err.Error()
			if test.wantRecord == storage.FileActionNotExecuted && originalText != test.cause.Error() {
				t.Fatalf("refusal diagnostic changed: %q want %q", originalText, test.cause.Error())
			}
			if first.Reference != nil {
				if err := first.Reference.Close(t.Context()); err != nil {
					t.Fatalf("closing returned uncertain reference: %v", err)
				}
			}
			replay, replayErr := opener.OpenNodeRef(t.Context(), root.ID, options)
			if storage.ErrnoOf(replayErr) != test.wantErrno || !reflect.DeepEqual(replay, first) || authority.calls != 1 {
				t.Fatalf("refusal replay: result=%+v error=%v calls=%d", replay, replayErr, authority.calls)
			}
			if replayErr.Error() != originalText {
				t.Fatalf("replayed diagnostic changed: %q want %q", replayErr.Error(), originalText)
			}
			actions := session.(storage.FileActions)
			receipt, err := actions.QueryFileAction(t.Context(), options.Action)
			if err != nil || receipt.Outcome != test.wantRecord {
				t.Fatalf("refusal journal: receipt=%+v error=%v", receipt, err)
			}
			changed := options
			changed.MetadataAccess = storage.WriteMetadata
			if _, err := opener.OpenNodeRef(t.Context(), root.ID, changed); storage.ErrnoOf(err) != syscall.EINVAL || authority.calls != 1 {
				t.Fatalf("changed action intent: error=%v calls=%d", err, authority.calls)
			}
			authority.open = nil
			options.Action = fileActionFor(t, session)
			options.Use = storage.UseClaim{Deny: storage.ReadEntries}
			fresh, err := opener.OpenNodeRef(t.Context(), root.ID, options)
			if err != nil || fresh.Reference == nil || fresh.Outcome != storage.Opened || authority.calls != 2 {
				t.Fatalf("fresh directory open: result=%+v error=%v calls=%d", fresh, err, authority.calls)
			}
			if err := fresh.Reference.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNodeOpenLocalAdmissionRefusalReplaysWithoutNativeDispatch(t *testing.T) {
	volume, authority := nodeOpenProofVolume(t)
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	limits := storage.DefaultFileSessionOptions()
	limits.MaxOperations = 1
	session := fileSessionFor(t, volume, limits)
	opener := session.(storage.NodeReferences)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	authority.open = func(ctx context.Context, id uint64, options storage.NodeRefOptions) (metastore.NodeOpenResult, error) {
		if authority.calls == 1 {
			close(entered)
			<-release
		}
		return authority.LockingStore.OpenNodeRef(ctx, id, options)
	}
	heldOptions := storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: root.ID}, MetadataAccess: storage.ReadMetadata, Use: storage.UseClaim{Uses: storage.ReadEntries}, Action: fileActionFor(t, session)}
	type completion struct {
		result storage.NodeOpenResult
		err    error
	}
	finished := make(chan completion, 1)
	go func() {
		result, err := opener.OpenNodeRef(t.Context(), root.ID, heldOptions)
		finished <- completion{result: result, err: err}
	}()
	select {
	case <-entered:
	case early := <-finished:
		t.Fatalf("first node open returned before native gate: %+v", early)
	case <-time.After(5 * time.Second):
		t.Fatal("first node open did not reach native dispatch")
	}
	refusedOptions := heldOptions
	refusedOptions.Action = fileActionFor(t, session)
	refused, refusal := opener.OpenNodeRef(t.Context(), root.ID, refusedOptions)
	if !errors.Is(refusal, syscall.EAGAIN) || refused.Reference != nil || refused.Attr.ID != 0 || refused.Outcome != 0 || authority.calls != 1 {
		t.Fatalf("local admission refusal: result=%+v error=%v native calls=%d", refused, refusal, authority.calls)
	}
	receipt, err := session.(storage.FileActions).QueryFileAction(t.Context(), refusedOptions.Action)
	if err != nil || receipt.Outcome != storage.FileActionNotExecuted {
		t.Fatalf("local refusal receipt=%+v error=%v", receipt, err)
	}
	unblock()
	var held completion
	select {
	case held = <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("first node open did not finish")
	}
	if held.err != nil || held.result.Reference == nil {
		t.Fatalf("first node open: %+v", held)
	}
	if err := held.result.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := opener.OpenNodeRef(t.Context(), root.ID, refusedOptions); !errors.Is(err, refusal) || authority.calls != 1 {
		t.Fatalf("refusal replay: error=%v native calls=%d", err, authority.calls)
	}
	refusedOptions.Action = fileActionFor(t, session)
	refusedOptions.Use = storage.UseClaim{Deny: storage.ReadEntries}
	fresh, err := opener.OpenNodeRef(t.Context(), root.ID, refusedOptions)
	if err != nil || fresh.Reference == nil || authority.calls != 2 {
		t.Fatalf("fresh node open: result=%+v error=%v native calls=%d", fresh, err, authority.calls)
	}
	if err := fresh.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
