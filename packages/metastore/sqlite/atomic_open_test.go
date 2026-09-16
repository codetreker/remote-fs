package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func atomicOptions() storage.OpenAtOptions {
	return storage.OpenAtOptions{Read: true, Write: true, Create: true, Existing: storage.Keep,
		Target: storage.ChildCondition{State: storage.Any}, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}}
}

func atomicFile(t *testing.T, s *Store, name string, options storage.OpenAtOptions) metastore.OpenResult {
	t.Helper()
	result, err := s.OpenAt(t.Context(), namespaceName(s.root, name), options)
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

func commitNativeBytes(t *testing.T, file metastore.File, body []byte) metastore.FileState {
	t.Helper()
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key, err := file.Reserve(t.Context(), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	state, err := file.Commit(t.Context(), before.Revision, metastore.Object{Key: key, Size: int64(len(body)), Digest: digest[:], ModTime: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestAtomicOpenCapturesSelectedInitialStateAndPreservesReplacementIdentity(t *testing.T) {
	s := pendingUnlinkStore(t)
	options := atomicOptions()
	createdAt := time.Unix(100, 20)
	options.Initial.OnCreate = storage.InitialFields{Attr: storage.AttrChange{ModTime: &createdAt}, Metadata: map[string][]byte{"test.value": []byte("created"), "test.foreign": []byte("kept")}}
	created := atomicFile(t, s.Store, "file", options)
	if created.Outcome != storage.Created || created.State.ID == 0 || !created.State.ModTime.Equal(createdAt) || !bytes.Equal(created.State.Metadata["test.value"].Data, []byte("created")) {
		t.Fatalf("create = %+v", created)
	}
	options.Initial.OnCreate.Metadata["test.value"] = []byte("unused")
	kept := atomicFile(t, s.Store, "file", options)
	if kept.Outcome != storage.Opened || kept.State.ID != created.State.ID || !bytes.Equal(kept.State.Metadata["test.value"].Data, []byte("created")) {
		t.Fatalf("keep applied create fields: %+v", kept)
	}
	commitNativeBytes(t, created.File, []byte("old"))
	resetAt := time.Unix(200, 30)
	options.Existing = storage.ResetContent
	options.Initial.OnReset = storage.InitialFields{Attr: storage.AttrChange{ModTime: &resetAt}, Metadata: map[string][]byte{"test.value": []byte("reset")}}
	reset := atomicFile(t, s.Store, "file", options)
	if reset.Outcome != storage.Reset || reset.State.ID != created.State.ID || reset.State.Size != 0 || !reset.State.ModTime.Equal(resetAt) ||
		!bytes.Equal(reset.State.Metadata["test.value"].Data, []byte("reset")) || !bytes.Equal(reset.State.Metadata["test.foreign"].Data, []byte("kept")) {
		t.Fatalf("reset = %+v", reset)
	}
	if !bytes.Equal(created.State.Metadata["test.value"].Data, []byte("created")) || created.State.Size != 0 {
		t.Fatal("later changes rewrote the captured open result")
	}
	commitNativeBytes(t, reset.File, []byte("held"))
	options.Existing = storage.ReplaceNode
	options.Use.Uses |= storage.DeleteName
	options.Initial.OnReset = storage.InitialFields{}
	options.Initial.OnReplace.Metadata = map[string][]byte{"test.value": []byte("replacement")}
	replaced := atomicFile(t, s.Store, "file", options)
	if replaced.Outcome != storage.Replaced || replaced.State.ID == created.State.ID || replaced.State.Size != 0 || len(replaced.State.Metadata) != 1 {
		t.Fatalf("replace = %+v", replaced)
	}
	if old, err := reset.File.Node(t.Context()); err != nil || !old.Detached || old.ID != created.State.ID || old.Size != 4 {
		t.Fatalf("displaced retained state = %+v, %v", old, err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 4 {
		t.Fatalf("retained replacement usage = %d, %v", used, err)
	}
	for _, old := range []metastore.File{created.File, kept.File, reset.File} {
		if err := old.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("last displaced close usage = %d, %v", used, err)
	}
}

func TestAtomicOpenKnownBudgetRefusalRetainsNoReusedIdentity(t *testing.T) {
	s := pendingUnlinkStore(t)
	var refusedID uint64
	ctx := storage.WithAttrResultBudget(t.Context(), func(attr storage.Attr, _ int64) error {
		refusedID = attr.ID
		return syscall.EFBIG
	})
	options := atomicOptions()
	result, err := s.OpenAt(ctx, namespaceName(s.root, "refused"), options)
	if !errors.Is(err, syscall.EFBIG) || result.File != nil || result.State.ID != 0 || result.Outcome != 0 || refusedID == 0 || len(s.files) != 0 || len(s.coordinator.pins) != 0 {
		t.Fatalf("known refusal = %+v, %v; proposed=%d files=%d pins=%d", result, err, refusedID, len(s.files), len(s.coordinator.pins))
	}
	if _, err := s.Stat(t.Context(), "refused"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused open changed namespace: %v", err)
	}
	accepted := atomicFile(t, s.Store, "accepted", options)
	if uint64(accepted.State.ID) != refusedID || s.fileDomain.files != 1 {
		t.Fatalf("rollback retained an uncommitted allocation: id=%d refused=%d references=%d", accepted.State.ID, refusedID, s.fileDomain.files)
	}
}

func TestAtomicOpenUnknownAcceptanceRetainsClaimAndPreventsIdentityReuse(t *testing.T) {
	s, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	acceptErr := errors.New("open acceptance unavailable")
	s.witness = &retainedFailureWitness{failure: acceptErr}
	options := atomicOptions()
	options.Use.Deny = storage.WriteData
	result, err := s.OpenAt(t.Context(), namespaceName(s.root, "unknown"), options)
	if !errors.Is(err, acceptErr) || result.File == nil || result.State.ID != 0 || result.Outcome != 0 {
		t.Fatalf("unknown open = %+v, %v", result, err)
	}
	file := result.File.(*retainedFile)
	if file.active || file.closed || s.fileDomain.files != 1 || s.coordinator.pins[retainedNode{s.volume, file.id}] != 1 {
		t.Fatal("unknown open released or reactivated its native identity")
	}
	if err := s.fileDomain.coordinator.CheckUse(t.Context(), uint64(file.id), storage.UseScope{}, storage.WriteData); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("unknown open discarded its claim: %v", err)
	}
	var before, after int64
	if err := s.write.QueryRowContext(t.Context(), `SELECT max(id) FROM nodes`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if next, err := s.OpenAt(t.Context(), namespaceName(s.root, "later"), atomicOptions()); !errors.Is(err, acceptErr) || next.File != nil {
		t.Fatalf("poisoned authority accepted another identity: %+v, %v", next, err)
	}
	if err := s.write.QueryRowContext(t.Context(), `SELECT max(id) FROM nodes`).Scan(&after); err != nil || after != before {
		t.Fatalf("unknown authority allocated a replacement identity: %d -> %d, %v", before, after, err)
	}
	if err := result.File.Close(t.Context()); !errors.Is(err, acceptErr) {
		t.Fatalf("unknown reference close = %v", err)
	}
	if err := s.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("unknown owner released the database: %v", err)
	}
	// Public cleanup must retain the hold; the fixture then models process teardown.
	if err := s.locks.Close(); err != nil && !errors.Is(err, acceptErr) {
		t.Fatal(err)
	}
	s.locks, s.witness, s.files = nil, nil, nil
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
}
