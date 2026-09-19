package sqlite

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func nativeCandidate(t *testing.T, file metastore.File, body []byte) metastore.Object {
	t.Helper()
	key, err := file.Reserve(t.Context(), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	return metastore.Object{Key: key, Size: int64(len(body)), Digest: digest[:], ModTime: time.Now()}
}

func TestConditionalCommitChecksCurrentSizeMetadataAndContentRevisionSeparately(t *testing.T) {
	s, file := openPublicationFile(t)
	f := file.(*retainedFile)
	before := commitNativeBytes(t, f, []byte("base"))
	metadata, err := f.SetMetadata(t.Context(), "test.guard", nil, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	size := int64(4)
	command := storage.FileMutation{Kind: storage.MutateAppend, Data: []byte("xy"), ExpectedSize: &size,
		ExpectedMetadata: map[string][]byte{"test.guard": metadata.Version}}
	candidate := nativeCandidate(t, f, []byte("basexy"))
	current := commitNativeBytes(t, f, []byte("grown"))
	if _, err := f.CommitMutation(t.Context(), command, before.Revision, candidate); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("changed size was treated as retryable content competition: %v", err)
	}
	command.ExpectedSize = nil
	if _, err := f.CommitMutation(t.Context(), command, before.Revision, candidate); !errors.Is(err, syscall.EAGAIN) || errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("content-only competition = %v", err)
	}
	if err := s.Abandon(t.Context(), candidate.Key); err != nil {
		t.Fatal(err)
	}
	updated, err := f.SetMetadata(t.Context(), "test.guard", metadata.Version, []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	candidate = nativeCandidate(t, f, []byte("grownxy"))
	if _, err := f.CommitMutation(t.Context(), command, current.Revision, candidate); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("changed namespace accepted append: %v", err)
	}
	unchanged, err := f.Node(t.Context())
	if err != nil || unchanged.Content != current.Content || unchanged.Size != 5 || unchanged.Revision != current.Revision {
		t.Fatalf("refusal changed content state: %+v, %v", unchanged, err)
	}
	command.ExpectedMetadata["test.guard"] = updated.Version
	appended, err := f.CommitMutation(t.Context(), command, current.Revision, candidate)
	if err != nil || appended.Size != 7 || appended.Revision != current.Revision+1 || appended.Content != candidate.Key {
		t.Fatalf("current append = %+v, %v", appended, err)
	}
	modified := time.Unix(300, 40)
	attribute := storage.FileMutation{Kind: storage.MutateAttributes, Attr: storage.AttrChange{ModTime: &modified},
		Metadata: map[string]storage.OpaquePayload{"test.guard": {Version: updated.Version, Data: []byte("three")}}}
	changed, err := f.MutateFile(t.Context(), attribute)
	if err != nil || changed.Content != appended.Content || changed.Revision != appended.Revision || !changed.ModTime.Equal(modified) ||
		!bytes.Equal(changed.Metadata["test.guard"].Data, []byte("three")) {
		t.Fatalf("atomic attributes = %+v, %v", changed, err)
	}
	if _, err := f.MutateFile(t.Context(), attribute); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale namespace update = %v", err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 7 {
		t.Fatalf("conditional usage = %d, %v", used, err)
	}
}

func TestConditionalNoEffectStillAdmitsItsReturnedAttributes(t *testing.T) {
	s, file := openPublicationFile(t)
	f := file.(*retainedFile)
	before := commitNativeBytes(t, f, []byte("body"))
	position, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	size := before.Size
	command := storage.FileMutation{Kind: storage.MutateAppend, ExpectedSize: &size}
	state, err := f.MutateFile(t.Context(), command)
	if err != nil || state.Content != before.Content || state.Revision != before.Revision || !state.ModTime.Equal(before.ModTime) {
		t.Fatalf("empty append = %+v, %v", state, err)
	}
	ctx := storage.WithAttrResultBudget(t.Context(), func(storage.Attr, int64) error { return syscall.EFBIG })
	if _, err := f.MutateFile(ctx, command); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("no-effect result bypassed response admission: %v", err)
	}
	if after, err := s.CommittedPosition(t.Context()); err != nil || after != position {
		t.Fatalf("empty append published a change: %d -> %d, %v", position, after, err)
	}
}
