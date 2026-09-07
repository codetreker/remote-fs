package replicated_test

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestBoundedEntrypointsPreserveReplicaAndDependencyFailures(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	write(t, s, "small", "small")
	write(t, s, "larger", "larger")
	mkdir(t, s, "directory")
	mounted, _ := mount(t, s)

	if err := mounted.CheckBounded(); err != nil {
		t.Fatalf("checking bounded dependencies: %v", err)
	}
	if content, err := mounted.ReadBounded(t.Context(), "small", 5); err != nil || string(content) != "small" {
		t.Fatalf("ReadBounded at the exact boundary returned %q, %v", content, err)
	}
	if content, err := mounted.ReadBounded(t.Context(), "larger", 5); content != nil || !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("ReadBounded above its boundary returned %q, %v; want EFBIG", content, err)
	}
	if _, err := mounted.ReadBounded(t.Context(), "missing", 5); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadBounded hid the remote dependency's ENOENT: %v", err)
	}

	result := listResult(t, 64)
	if err := mounted.ListBounded(t.Context(), "", result); err != nil {
		t.Fatalf("ListBounded: %v", err)
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatalf("reading the bounded listing: %v", err)
	}
	if len(entries) != 3 || entries[0].Name != "directory" || entries[1].Name != "larger" || entries[2].Name != "small" {
		t.Fatalf("bounded listing returned %+v", entries)
	}

	notDirectory := listResult(t, 64)
	if err := mounted.ListBounded(t.Context(), "small", notDirectory); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("ListBounded hid the replica dependency's ENOTDIR: %v", err)
	}
	assertInvalidListResult(t, notDirectory)

	overflow := listResult(t, 1)
	if err := mounted.ListBounded(t.Context(), "", overflow); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ListBounded above its boundary returned %v, want EIO", err)
	}
	assertInvalidListResult(t, overflow)

	s.events.cut()
	requireUnusable(t, mounted)
	if _, err := mounted.ReadBounded(t.Context(), "small", 5); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ReadBounded answered from an unusable replica: %v", err)
	}
	unusable := listResult(t, 64)
	if err := mounted.ListBounded(t.Context(), "", unusable); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ListBounded answered from an unusable replica: %v", err)
	}
	assertInvalidListResult(t, unusable)
}

func listResult(t *testing.T, maxBytes int64) *storage.ListResult {
	t.Helper()
	result, err := storage.NewListResult(maxBytes, 0, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes, nil
	})
	if err != nil {
		t.Fatalf("constructing a bounded listing: %v", err)
	}
	return result
}

func assertInvalidListResult(t *testing.T, result *storage.ListResult) {
	t.Helper()
	if entries, err := result.Entries(); entries != nil || err == nil {
		t.Fatalf("failed bounded listing exposed %+v, %v", entries, err)
	}
}
