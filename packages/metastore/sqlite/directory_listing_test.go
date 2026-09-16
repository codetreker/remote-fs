package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func directoryPage(t *testing.T, s *Store, request storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	t.Helper()
	var page storage.DirectoryPage
	err := s.inspect(t.Context(), func(tx *sql.Tx) error {
		var err error
		page, err = s.listDirectoryPage(t.Context(), tx, s.root, request)
		return err
	})
	return page, err
}

func TestDirectoryPagesPreserveExactNamesAndRevision(t *testing.T) {
	s := openLocationStore(t)
	ctx := t.Context()
	names := [][]byte{[]byte("A"), []byte("CON"), []byte("a"), {0xff}}
	for _, name := range names {
		if err := s.Create(ctx, string(name)); err != nil {
			t.Fatal(err)
		}
	}
	request := storage.DirectoryPageRequest{MaxEntries: 2, MaxBytes: 4096}
	first, err := directoryPage(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || len(first.Entries) != 2 {
		t.Fatalf("first page=%+v", first)
	}
	request.Revision = first.Revision
	request.Cursor = first.Next
	second, err := directoryPage(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Done || len(second.Entries) != 2 || second.Revision != first.Revision {
		t.Fatalf("second page=%+v", second)
	}
	got := append(first.Entries, second.Entries...)
	for i, name := range names {
		if !bytes.Equal(name, got[i].Name) {
			t.Fatalf("name[%d]=%q", i, got[i].Name)
		}
	}
}

func TestDirectoryPaginationRejectsConcurrentMutation(t *testing.T) {
	for _, operation := range []string{"create", "rename"} {
		t.Run(operation, func(t *testing.T) {
			s := openLocationStore(t)
			ctx := t.Context()
			for _, name := range []string{"a", "c"} {
				if err := s.Create(ctx, name); err != nil {
					t.Fatal(err)
				}
			}
			first, err := directoryPage(t, s, storage.DirectoryPageRequest{MaxEntries: 1, MaxBytes: 4096})
			if err != nil {
				t.Fatal(err)
			}
			if operation == "create" {
				if err := s.Create(ctx, "b"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := s.Rename(ctx, "c", "b"); err != nil {
					t.Fatal(err)
				}
			}
			page, err := directoryPage(t, s, storage.DirectoryPageRequest{Revision: first.Revision, Cursor: first.Next, MaxEntries: 1, MaxBytes: 4096})
			if !errors.Is(err, syscall.EAGAIN) || len(page.Entries) != 0 {
				t.Fatalf("stale page=%+v, %v", page, err)
			}
		})
	}
}

func TestDirectoryPageReservesMetadataBeforeLoading(t *testing.T) {
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
		_, err := tx.ExecContext(ctx, `UPDATE nodes SET metadata=zeroblob(?) WHERE id=?`, storage.MaxMetadataBytes, node.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	_, err = directoryPage(t, s, storage.DirectoryPageRequest{MaxEntries: 1, MaxBytes: 1024})
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("small budget loaded malformed metadata: %v", err)
	}
	_, err = directoryPage(t, s, storage.DirectoryPageRequest{MaxEntries: 1, MaxBytes: storage.MaxDirectoryPageBytes})
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("adequate budget failed to expose malformed metadata: %v", err)
	}
}

func TestDirectoryPageByteBoundaryEndsWithoutLosingEntry(t *testing.T) {
	s := openLocationStore(t)
	ctx := t.Context()
	for _, name := range []string{"a", "b"} {
		if err := s.Create(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	charge, err := storage.DirectoryEntryBytes(1, 6)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.DirectoryPageRequest{MaxEntries: 10, MaxBytes: storage.DirectoryPageBaseBytes + charge}
	first, err := directoryPage(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || len(first.Entries) != 1 || string(first.Entries[0].Name) != "a" {
		t.Fatalf("first=%+v", first)
	}
	request.Revision = first.Revision
	request.Cursor = first.Next
	second, err := directoryPage(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Done || len(second.Entries) != 1 || string(second.Entries[0].Name) != "b" {
		t.Fatalf("second=%+v", second)
	}
}

func TestExactDirectoryLookupKeepsCaseAndAbsenceAtOneRevision(t *testing.T) {
	s := openLocationStore(t)
	ctx := t.Context()
	for _, name := range []string{"README", "readme", string([]byte{0xff})} {
		if err := s.Create(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		upper, err := s.lookupDirectoryEntry(ctx, tx, s.root, []byte("README"))
		if err != nil {
			return err
		}
		lower, err := s.lookupDirectoryEntry(ctx, tx, s.root, []byte("readme"))
		if err != nil {
			return err
		}
		absent, err := s.lookupDirectoryEntry(ctx, tx, s.root, []byte("Readme"))
		if err != nil {
			return err
		}
		raw, err := s.lookupDirectoryEntry(ctx, tx, s.root, []byte{0xff})
		if err != nil {
			return err
		}
		if !upper.Found || !lower.Found || !raw.Found || upper.Attr.ID == lower.Attr.ID || absent.Found || absent.EntryID != 0 {
			t.Fatal("exact lookup folded or invented an entry")
		}
		if upper.DirectoryRevision != lower.DirectoryRevision || upper.DirectoryRevision != absent.DirectoryRevision {
			t.Fatal("lookup mixed directory revisions")
		}
		return absent.Check([]byte("Readme"))
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExactDirectoryLookupDoesNotHideOrphanedEntries(t *testing.T) {
	s := openLocationStore(t)
	ctx := t.Context()
	if err := s.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}

	conn, err := s.write.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `UPDATE entries SET node=9223372036854775807 WHERE volume=? AND name=?`, s.volume, []byte("file")); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=ON`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.inspect(ctx, func(tx *sql.Tx) error { _, err := s.lookupDirectoryEntry(ctx, tx, s.root, []byte("file")); return err }); !errors.Is(err, syscall.EIO) {
		t.Fatalf("orphaned entry became absence: %v", err)
	}
}
