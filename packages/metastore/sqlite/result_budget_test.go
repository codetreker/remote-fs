package sqlite

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestOrdinaryPathOpenAdmitsOnlyItsResolvedResultBeforeRetention(t *testing.T) {
	s := pendingUnlinkStore(t)
	if err := s.Mkdir(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	parent, err := s.Stat(t.Context(), "parent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetMetadata(t.Context(), uint64(parent.ID), "test.parent", nil, bytes.Repeat([]byte("p"), 1024)); err != nil {
		t.Fatal(err)
	}
	created, err := s.OpenFile(t.Context(), "parent/file", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Create: true, Read: true}, InitialMetadata: map[string][]byte{"test.file": []byte("payload")},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := created.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	state, err := created.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	encodedBytes, err := storage.MetadataSize(state.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	var refusal error
	ctx := storage.WithAttrResultBudget(t.Context(), func(attr storage.Attr, size int64) error {
		calls++
		if attr.ID != uint64(state.ID) || attr.Metadata != nil || size != int64(encodedBytes) {
			t.Errorf("result admission included unrelated or loaded facts: %+v, bytes=%d", attr, size)
		}
		return refusal
	})
	opened, err := s.OpenFile(ctx, "parent/file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, ExpectedID: uint64(state.ID)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	got, err := opened.Node(t.Context())
	if err != nil || got.ID != state.ID || !bytes.Equal(got.Metadata["test.file"].Data, []byte("payload")) || calls == 0 {
		t.Fatalf("captured path result=%+v error=%v admissions=%d", got, err, calls)
	}
	if err := opened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, path string
		expected   uint64
		budgetErr  error
		want       error
		admissions int
	}{
		{"budget refusal", "parent/file", uint64(state.ID), syscall.EFBIG, syscall.EFBIG, 1},
		{"identity mismatch", "parent/file", uint64(state.ID) + 1, nil, syscall.ESTALE, 0},
		{"missing leaf", "parent/missing", 0, nil, syscall.ENOENT, 0},
		{"missing parent", "missing/file", 0, nil, syscall.ENOENT, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls, refusal = 0, test.budgetErr
			before := s.fileDomain.files
			file, err := s.OpenFile(ctx, test.path, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, ExpectedID: test.expected})
			if !errors.Is(err, test.want) || file != nil || calls != test.admissions {
				t.Fatalf("refused open=%v error=%v admissions=%d", file, err, calls)
			}
			if s.fileDomain.files != before || s.coordinator.pins[retainedNode{s.volume, state.ID}] != 1 {
				t.Fatal("refused result retained another reference or lost the existing pin")
			}
		})
	}
}
