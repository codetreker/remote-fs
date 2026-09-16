package objectstore_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func TestObjectMaintenanceBindingPreservesNestedLiveCleanupAccounting(t *testing.T) {
	volume, meta := fileVolume(t, memory.New(), 0, nil)
	if err := volume.Write(t.Context(), "file", []byte("one")); err != nil {
		t.Fatal(err)
	}
	inner, err := limited.New(t.Context(), volume, 4096)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := limited.New(t.Context(), inner, 8192)
	if err != nil {
		t.Fatal(err)
	}
	checkUsage := func(want int64) {
		t.Helper()
		for name, limiter := range map[string]*limited.Storage{"inner": inner, "outer": outer} {
			if space, err := limiter.Space(t.Context()); err != nil || space.Used != want {
				t.Fatalf("%s usage = %+v, %v; want %d", name, space, err, want)
			}
		}
		if used, err := meta.Usage(t.Context()); err != nil || used != want {
			t.Fatalf("native usage = %d, %v; want %d", used, err, want)
		}
	}
	checkUsage(3)
	session, err := outer.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close session: %v", err)
		}
	})
	file := openFileFor(t, session, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if _, err := file.WriteAt(t.Context(), 0, bytes.Repeat([]byte("x"), 512)); err != nil {
		t.Fatal(err)
	}
	if err := outer.Write(t.Context(), "kept", bytes.Repeat([]byte("y"), 256)); err != nil {
		t.Fatal(err)
	}
	checkUsage(768)
	if err := outer.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	checkUsage(768)
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkUsage(256)
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkUsage(256)
}
