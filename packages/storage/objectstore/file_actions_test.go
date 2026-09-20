package objectstore

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/storage"
)

func actionTestSession(t *testing.T, maximum int) *fileSession {
	t.Helper()
	config := advisory.DefaultConfig()
	domain, err := advisory.New(config)
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxLockActions = maximum
	locks, err := domain.NewSession(options, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return &fileSession{
		storage: &Storage{}, domain: domain, locks: locks, options: options,
		active: true, expires: time.Now().Add(options.Lease),
		actions: make(map[storage.FileActionID]*fileAction),
	}
}

func actionID(t *testing.T, session *fileSession) storage.FileActionID {
	t.Helper()
	epoch, err := session.locks.Epoch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewFileActionID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestFileActionJournalReplaysOneTypedResultAndRejectsChangedIntent(t *testing.T) {
	session := actionTestSession(t, 2)
	id := actionID(t, session)
	var calls atomic.Int32
	invoke := func(input string) (storage.NameResult, error) {
		return runFileAction(t.Context(), session, id, storage.OpFileMutateName, input, cloneNameResult,
			func(result storage.NameResult) bool { return result.Attr != nil }, func() (storage.NameResult, error) {
				calls.Add(1)
				attr := storage.Attr{ID: 9, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{
					"test.value": {Version: []byte{1}, Data: []byte("original")},
				}}
				return storage.NameResult{Attr: &attr}, nil
			})
	}
	first, err := invoke("same")
	if err != nil {
		t.Fatal(err)
	}
	first.Attr.Metadata["test.value"] = storage.OpaquePayload{Version: []byte{9}, Data: []byte("changed")}
	replayed, err := invoke("same")
	if err != nil || calls.Load() != 1 || string(replayed.Attr.Metadata["test.value"].Data) != "original" {
		t.Fatalf("replay=%+v error=%v calls=%d", replayed, err, calls.Load())
	}
	if _, err := invoke("different"); !errors.Is(err, syscall.EINVAL) || calls.Load() != 1 {
		t.Fatalf("changed action intent=%v calls=%d", err, calls.Load())
	}
	receipt, err := session.QueryFileAction(t.Context(), id)
	if err != nil || receipt.Operation != storage.OpFileMutateName || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("receipt=%+v error=%v", receipt, err)
	}
}

func TestFileActionJournalBoundsHistoryAndKeepsUnknownOutcomes(t *testing.T) {
	session := actionTestSession(t, 1)
	first := actionID(t, session)
	failure := errors.New("publication result unknown")
	result, err := runFileAction(t.Context(), session, first, storage.OpFileMutate, "first",
		func(attr storage.Attr) storage.Attr { return attr.Clone() },
		func(attr storage.Attr) bool { return attr.ID != 0 },
		func() (storage.Attr, error) { return storage.Attr{ID: 7, Kind: storage.NodeRegular}, failure })
	if !errors.Is(err, failure) || result.ID != 7 {
		t.Fatalf("unknown result=%+v error=%v", result, err)
	}
	receipt, err := session.QueryFileAction(t.Context(), first)
	if err != nil || receipt.Outcome != storage.FileActionUnknown {
		t.Fatalf("unknown receipt=%+v error=%v", receipt, err)
	}
	second := actionID(t, session)
	if _, err := runFileAction(t.Context(), session, second, storage.OpFileMutateName, "second", cloneNameResult,
		func(result storage.NameResult) bool { return result.Attr != nil },
		func() (storage.NameResult, error) { return storage.NameResult{}, nil }); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("full journal admitted another action: %v", err)
	}
	missing := actionID(t, session)
	receipt, err = session.QueryFileAction(t.Context(), missing)
	if err != nil || receipt.Outcome != storage.FileActionNotExecuted || receipt.Operation != "" {
		t.Fatalf("missing receipt=%+v error=%v", receipt, err)
	}
}

func TestFileActionJournalCancellationDoesNotRewriteConcurrentCompletion(t *testing.T) {
	session := actionTestSession(t, 2)
	id := actionID(t, session)
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := runFileAction(context.Background(), session, id, storage.OpFileMutateName, "same", cloneNameResult,
			func(result storage.NameResult) bool { return result.Attr != nil },
			func() (storage.NameResult, error) {
				close(entered)
				<-release
				attr := storage.Attr{ID: 5, Kind: storage.NodeRegular}
				return storage.NameResult{Attr: &attr}, nil
			})
		finished <- err
	}()
	<-entered
	waiting, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := runFileAction(waiting, session, id, storage.OpFileMutateName, "same", cloneNameResult,
		func(result storage.NameResult) bool { return result.Attr != nil },
		func() (storage.NameResult, error) {
			t.Fatal("duplicate action executed")
			return storage.NameResult{}, nil
		}); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting replay=%v", err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	receipt, err := session.QueryFileAction(t.Context(), id)
	if err != nil || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("completed receipt=%+v error=%v", receipt, err)
	}
}

func TestFileActionDigestCanonicalizesEquivalentConditionsAndTargetUses(t *testing.T) {
	instant := time.Date(2026, time.September, 20, 10, 30, 0, 123, time.FixedZone("west", -7*60*60))
	first := storage.FileMutation{
		Action: storage.FileActionID("1:00000000000000000000000000000000"), Kind: storage.MutateAttributes,
		Attr:             storage.AttrChange{ModTime: &instant},
		ExpectedMetadata: map[string][]byte{"absent": nil},
		Metadata:         map[string]storage.OpaquePayload{"empty": {Version: nil, Data: nil}},
		Uses:             []storage.TargetUse{{NodeID: 9, Scope: storage.UseScope{Token: "nine"}}, {NodeID: 3, Scope: storage.UseScope{Token: "three"}}},
	}
	second := first
	equivalent := instant.UTC()
	second.Attr.ModTime = &equivalent
	second.ExpectedMetadata = map[string][]byte{"absent": {}}
	second.Metadata = map[string]storage.OpaquePayload{"empty": {Version: []byte{}, Data: []byte{}}}
	second.Uses = []storage.TargetUse{{NodeID: 3, Scope: storage.UseScope{Token: "three"}}, {NodeID: 9, Scope: storage.UseScope{Token: "nine"}}}
	target := referenceActionTarget{NodeID: 7, Scope: storage.UseScope{Token: "scope-a"}}
	a, err := fileActionDigest(storage.OpFileMutate, fileMutationActionInput{Target: target, Command: first})
	if err != nil {
		t.Fatal(err)
	}
	b, err := fileActionDigest(storage.OpFileMutate, fileMutationActionInput{Target: target, Command: second})
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("semantically equal action inputs produced different digests")
	}
	second.Metadata["empty"] = storage.OpaquePayload{Data: []byte("changed")}
	c, err := fileActionDigest(storage.OpFileMutate, fileMutationActionInput{Target: target, Command: second})
	if err != nil {
		t.Fatal(err)
	}
	if a == c {
		t.Fatal("changed metadata payload retained the original action digest")
	}
	differentNode, err := fileActionDigest(storage.OpFileMutate, fileMutationActionInput{
		Target: referenceActionTarget{NodeID: 8, Scope: target.Scope}, Command: first,
	})
	if err != nil {
		t.Fatal(err)
	}
	if a == differentNode {
		t.Fatal("different receiver node retained the original action digest")
	}
	differentScope, err := fileActionDigest(storage.OpFileMutate, fileMutationActionInput{
		Target: referenceActionTarget{NodeID: target.NodeID, Scope: storage.UseScope{Token: "scope-b"}}, Command: first,
	})
	if err != nil {
		t.Fatal(err)
	}
	if a == differentScope {
		t.Fatal("different receiver scope retained the original action digest")
	}
}
