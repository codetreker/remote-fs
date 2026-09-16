package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNativeContentSizeConditionRejectsConcurrentGrowth(t *testing.T) {
	s, session := newFileAuthority(t)
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	f := retainRangeFile(t, session, uint64(node.ID))
	size := int64(0)
	id := fileActionID(t, session)
	first, fresh, err := f.BeginContent(t.Context(), id, [32]byte{1}, storage.FileIO{Write: true, Length: 1, ExpectedSize: &size})
	if err != nil || !fresh || first.State != storage.FileActionPending {
		t.Fatalf("begin=%+v,%v,%v", first, fresh, err)
	}
	state, err := f.Capture(t.Context(), storage.FileIO{Write: true, Length: 1, ExpectedSize: &size})
	if err != nil {
		t.Fatal(err)
	}
	oldKey, err := f.Reserve(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	other := fileActionID(t, session)
	if _, fresh, err := f.BeginContent(t.Context(), other, [32]byte{2}, storage.FileIO{Write: true, Length: 2}); err != nil || !fresh {
		t.Fatalf("other begin=%v,%v", fresh, err)
	}
	key, err := f.Reserve(t.Context(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.CommitContent(t.Context(), other, state.Revision, metastore.Object{Key: key, Size: 2, ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	size = 2
	r, err := f.CommitContent(t.Context(), id, state.Revision, metastore.Object{Key: oldKey, Size: 1, ModTime: time.Now()})
	if !errors.Is(err, syscall.EAGAIN) || r.State != storage.FileActionNotApplied || r.Effects != 0 || r.Conflict == nil || r.Conflict.Kind != storage.ConflictRevision {
		t.Fatalf("size refusal=%+v,%v", r, err)
	}
	q, err := session.QueryAction(t.Context(), id)
	if !errors.Is(err, syscall.EAGAIN) || q.State != storage.FileActionNotApplied {
		t.Fatalf("size receipt=%+v,%v", q, err)
	}
	current, err := s.Stat(t.Context(), "file")
	if err != nil || current.Size != 2 || current.Content != key {
		t.Fatalf("old write affected growth: %+v,%v", current, err)
	}
	if err := s.Abandon(t.Context(), oldKey); err != nil {
		t.Fatal(err)
	}
}

func TestNativeRejectContentPreservesTheDeterminedOutcome(t *testing.T) {
	s, session := newFileAuthority(t)
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	file := retainRangeFile(t, session, uint64(node.ID))
	id := fileActionID(t, session)
	if receipt, fresh, err := file.BeginContent(t.Context(), id, [32]byte{9}, storage.FileIO{Write: true, Length: 1}); err != nil || !fresh || receipt.State != storage.FileActionPending {
		t.Fatalf("begin=%+v,%v,%v", receipt, fresh, err)
	}
	captured, err := file.Capture(t.Context(), storage.FileIO{Write: true, Length: 1})
	if err != nil {
		t.Fatal(err)
	}
	key, err := file.Reserve(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.RejectContent(t.Context(), id, nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("missing failure=%v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := file.RejectContent(canceled, id, syscall.ENOSPC); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled rejection=%v", err)
	}
	if pending, err := session.QueryAction(t.Context(), id); err != nil || pending.State != storage.FileActionPending {
		t.Fatalf("invalid rejection changed action=%+v,%v", pending, err)
	}
	if _, err := file.RejectContent(t.Context(), fileActionID(t, session), syscall.ENOSPC); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("unknown action=%v", err)
	}
	diagnostic := errors.New("object staging refused")
	first, err := file.RejectContent(t.Context(), id, errors.Join(syscall.ENOSPC, diagnostic))
	if !errors.Is(err, syscall.ENOSPC) || !errors.Is(err, diagnostic) || first.State != storage.FileActionNotApplied || first.Effects != 0 {
		t.Fatalf("rejected=%+v,%v", first, err)
	}
	repeated, err := file.RejectContent(t.Context(), id, syscall.EIO)
	if !errors.Is(err, diagnostic) || repeated.State != first.State || repeated.Errno != first.Errno {
		t.Fatalf("rejection overwrote outcome=%+v,%v", repeated, err)
	}
	late, err := file.CommitContent(t.Context(), id, captured.Revision, metastore.Object{Key: key, Size: 1, ModTime: time.Now()})
	if !errors.Is(err, diagnostic) || late.State != storage.FileActionNotApplied || late.Effects != 0 {
		t.Fatalf("late commit changed rejection=%+v,%v", late, err)
	}
	current, err := s.Stat(t.Context(), "file")
	if err != nil || current.Size != 0 || current.Content != "" {
		t.Fatalf("rejected write published=%+v,%v", current, err)
	}
	if err := s.Abandon(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	completedID := fileActionID(t, session)
	completed, fresh, err := file.BeginContent(t.Context(), completedID, [32]byte{10}, storage.FileIO{Write: true})
	if err != nil || fresh || completed.State != storage.FileActionCompleted {
		t.Fatalf("zero write=%+v,%v,%v", completed, fresh, err)
	}
	result, err := file.RejectContent(t.Context(), completedID, syscall.EIO)
	if err != nil || result.State != storage.FileActionCompleted || result.Observation.Attr.ID != completed.Observation.Attr.ID {
		t.Fatalf("late rejection erased known completion=%+v,%v", result, err)
	}
	if _, err := file.Close(t.Context(), fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	result, err = file.RejectContent(t.Context(), id, syscall.EIO)
	if !errors.Is(err, diagnostic) || result.State != storage.FileActionNotApplied {
		t.Fatalf("closed reference lost content outcome=%+v,%v", result, err)
	}
}
