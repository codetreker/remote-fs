package sqlite

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestWindowsReparseConversionFencesOrdinaryFinalContentPublication(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	key, err := s.Reserve(t.Context(), "file", 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.SetLink(t.Context(), "target", windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	for _, object := range []metastore.Object{{ModTime: time.Now()}, {Key: key, Size: 3, ModTime: time.Now()}} {
		if err := s.Commit(t.Context(), "file", object); !errors.Is(err, syscall.ELOOP) {
			t.Fatalf("reparsecommit=%v", err)
		}
		if info, err := f.ReadLink(t.Context()); err != nil || info.Target != "target" {
			t.Fatalf("retainedlink=%+v %v", info, err)
		}
	}
	if err := s.Abandon(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsReparseConversionRetiresZeroSizedImmutableContent(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	key, err := s.Reserve(t.Context(), "file", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(t.Context(), "file", metastore.Object{Key: key, ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.SetLink(t.Context(), "target", windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	var content any
	var objectState int
	if err := s.read.QueryRow(`SELECT content FROM nodes WHERE id=?`, f.id).Scan(&content); err != nil || content != nil {
		t.Fatalf("linkcontent=%v %v", content, err)
	}
	if err := s.read.QueryRow(`SELECT state FROM objects WHERE key=?`, string(key)).Scan(&objectState); err != nil || objectState != stateGarbage {
		t.Fatalf("priorobject=%d %v", objectState, err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 6 {
		t.Fatalf("usage=%d %v", used, err)
	}
	snapshot, _, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}
