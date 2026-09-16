package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func openLocationStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.Context(), t.TempDir()+"/meta.db", "location", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func observedLocation(t *testing.T, s *Store, path string) storage.EntryLocation {
	t.Helper()
	ctx := t.Context()
	var location storage.EntryLocation
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		node, err := s.resolve(ctx, tx, path)
		if err != nil {
			return err
		}
		location, err = s.captureLocation(ctx, tx, s.root, node.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return location
}

func TestEntryLocationCapturesExactAncestorFacts(t *testing.T) {
	s := openLocationStore(t)
	ctx := t.Context()
	if err := s.Mkdir(ctx, "dir"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"README", "readme", "CON", "bad?name", string([]byte{0xff})} {
		if err := s.Create(ctx, "dir/"+name); err != nil {
			t.Fatal(err)
		}
	}
	location := observedLocation(t, s, "dir/"+string([]byte{0xff}))
	if len(location.Ancestors) != 2 || !bytes.Equal(location.Ancestors[1].Name, []byte{0xff}) {
		t.Fatalf("location=%+v", location)
	}
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		node, err := s.nodeByID(ctx, tx, int64(location.NodeID))
		if err != nil {
			return err
		}
		captured, err := s.captureLocation(ctx, tx, s.root, node.ID)
		if err != nil {
			return err
		}
		if captured.NodeID != uint64(node.ID) {
			t.Fatal("capture changed subject")
		}
		return s.validateLocation(ctx, tx, captured)
	}); err != nil {
		t.Fatal(err)
	}
	root := observedLocation(t, s, "")
	if root.State != storage.LocationRoot || root.NodeID != uint64(s.root) || len(root.Ancestors) != 0 {
		t.Fatalf("root=%+v", root)
	}
}

func TestEntryLocationRejectsBoundsAndMissingContainment(t *testing.T) {
	s := openLocationStore(t)
	ctx := t.Context()
	for _, name := range []string{"a", "b"} {
		if err := s.Mkdir(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Create(ctx, "a/file"); err != nil {
		t.Fatal(err)
	}
	a, err := s.Stat(ctx, "a/file")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Stat(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		_, err := s.captureLocation(ctx, tx, b.ID, a.ID)
		if !errors.Is(err, syscall.EXDEV) {
			t.Fatalf("outside root=%v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	oldRecords, oldBytes := s.maxIntegrityRecords, s.maxIntegrityBytes
	for _, test := range []struct{ records, bytes int64 }{{1, oldBytes}, {oldRecords, 1}} {
		s.maxIntegrityRecords = test.records
		s.maxIntegrityBytes = test.bytes
		if err := s.inspect(ctx, func(tx *sql.Tx) error { _, err := s.captureLocation(ctx, tx, s.root, a.ID); return err }); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("bound=%v", err)
		}
	}
	s.maxIntegrityRecords = oldRecords
	s.maxIntegrityBytes = oldBytes
}

func TestEntryLocationDistinguishesDetachedObjects(t *testing.T) {
	s := openLocationStore(t)
	ctx := t.Context()
	if err := s.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.mutateTransaction(ctx, ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM entries WHERE volume=? AND node=?`, s.volume, node.ID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE nodes SET detached=1 WHERE volume=? AND id=?`, s.volume, node.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		location, err := s.captureLocation(ctx, tx, s.root, node.ID)
		if err != nil {
			return err
		}
		if location.State != storage.LocationDetached || len(location.Ancestors) != 0 {
			t.Fatalf("detached location=%+v", location)
		}
		return s.validateLocation(ctx, tx, location)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEntryLocationRejectsCyclicAncestors(t *testing.T) {
	s := openLocationStore(t)
	ctx := t.Context()
	if err := s.Mkdir(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Mkdir(ctx, "a/b"); err != nil {
		t.Fatal(err)
	}
	a, err := s.Stat(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Stat(ctx, "a/b")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.mutateTransaction(ctx, ctx, nil, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE entries SET parent=? WHERE volume=? AND node=?`, b.ID, s.volume, a.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.inspect(ctx, func(tx *sql.Tx) error { _, err := s.captureLocation(ctx, tx, s.root, b.ID); return err }); !errors.Is(err, syscall.EIO) {
		t.Fatalf("cyclic location=%v", err)
	}
}
