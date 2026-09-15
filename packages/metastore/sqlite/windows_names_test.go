package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"strings"
	"syscall"
	"testing"
)

func TestWindowsNameKey(t *testing.T) {
	for _, name := range []string{"file.txt", "README", "école", "中文", "😀", "COM0", "COM10", "console", "a..b", strings.Repeat("x", 255)} {
		if _, err := windowsNameKey([]byte(name)); err != nil {
			t.Errorf("valid %q: %v", name, err)
		}
	}
	for _, name := range []string{"", ".", "..", "a.", "a ", "a:b", "a/b", `a\b`, "a?b", "a*b", "a<b", "a>b", "a|b", `a"b`, "a\x00b", "a\x1fb", "\xff", "con", "NUL.txt", "com9.txt", "LPT¹.log", "CON .txt", "CONIN$", "CONOUT$", strings.Repeat("x", 256), strings.Repeat("😀", 128)} {
		if _, err := windowsNameKey([]byte(name)); err == nil {
			t.Errorf("invalid %q accepted", name)
		}
	}
	for _, pair := range [][2]string{{"README", "readme"}, {"École", "école"}, {"Σ", "ς"}} {
		a, err := windowsNameKey([]byte(pair[0]))
		if err != nil {
			t.Fatal(err)
		}
		b, err := windowsNameKey([]byte(pair[1]))
		if err != nil {
			t.Fatal(err)
		}
		if a != b {
			t.Errorf("%q and %q produce different keys", pair[0], pair[1])
		}
	}
}

func openWindowsNameStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.Context(), t.TempDir()+"/meta.db", "test", 0, DefaultWindow())
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

func enableWindowsNamePolicy(ctx context.Context, s *Store) error {
	return s.mutateTransaction(ctx, ctx, nil, func(tx *sql.Tx) error { return s.activateWindowsLocked(ctx, tx) })
}

func TestWindowsActivationRejectsWithoutChangingLegacyNames(t *testing.T) {
	for _, names := range [][]string{{"README", "readme"}, {"bad?name"}, {"\xff"}, {"CON.txt"}} {
		t.Run(names[0], func(t *testing.T) {
			s := openWindowsNameStore(t)
			for _, name := range names {
				if err := s.Create(t.Context(), name); err != nil {
					t.Fatal(err)
				}
			}
			if err := enableWindowsNamePolicy(t.Context(), s); err == nil {
				t.Fatal("activation succeeded")
			}
			var version uint32
			if err := s.inspect(t.Context(), func(tx *sql.Tx) error {
				var err error
				version, err = s.windowsNameVersion(t.Context(), tx)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if version != 0 {
				t.Fatal("failed activation changed policy")
			}
			for _, name := range names {
				if _, err := s.Stat(t.Context(), name); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Create(t.Context(), "still?legal"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWindowsActivatedPolicyCoversCreateRenameAndLookup(t *testing.T) {
	s := openWindowsNameStore(t)
	ctx := t.Context()
	if err := s.Create(ctx, "README"); err != nil {
		t.Fatal(err)
	}
	if err := s.Mkdir(ctx, "dir"); err != nil {
		t.Fatal(err)
	}
	if err := enableWindowsNamePolicy(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := enableWindowsNamePolicy(ctx, s); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"readme", "dir/read?me"} {
		if err := s.Create(ctx, name); err == nil {
			t.Fatalf("created %q", name)
		}
	}
	if err := s.Mkdir(ctx, "DIR"); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("directory collision: %v", err)
	}
	if err := s.Rename(ctx, "README", "readme"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, "another"); err != nil {
		t.Fatal(err)
	}
	if err := s.Rename(ctx, "another", "README"); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("rename collision: %v", err)
	}
	if _, err := s.Stat(ctx, "another"); err != nil {
		t.Fatal("failed rename changed source", err)
	}
	if err := s.Rename(ctx, "another", "readme"); err != nil {
		t.Fatal("exact replacement", err)
	}
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		n, found, err := s.windowsLookup(ctx, tx, s.root, []byte("README"))
		if err != nil {
			return err
		}
		if !found || n.ID == 0 {
			t.Fatal("case folded lookup missing")
		}
		_, found, err = s.windowsLookup(ctx, tx, s.root, []byte("absent"))
		if found || err != nil {
			t.Fatalf("absent lookup: %v %v", found, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsActivationBoundsCancellationAndPersistence(t *testing.T) {
	s := openWindowsNameStore(t)
	ctx := t.Context()
	for _, name := range []string{"one", "two"} {
		if err := s.Create(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	records, bytes := s.maxIntegrityRecords, s.maxIntegrityBytes
	s.maxIntegrityRecords = 1
	if err := enableWindowsNamePolicy(ctx, s); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("record limit: %v", err)
	}
	s.maxIntegrityRecords = records
	s.maxIntegrityBytes = 1
	if err := enableWindowsNamePolicy(ctx, s); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("byte limit: %v", err)
	}
	s.maxIntegrityBytes = bytes
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := enableWindowsNamePolicy(cancelled, s); err == nil {
		t.Fatal("cancelled activation succeeded")
	}
	if err := enableWindowsNamePolicy(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, s.databasePath, "test", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Create(ctx, "bad:name"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("policy not enforced: %v", err)
	}
}

func TestWindowsNameValidationIsPerDirectoryAndCoversSymlinkNames(t *testing.T) {
	s := openWindowsNameStore(t)
	ctx := t.Context()
	if err := s.Mkdir(ctx, "dir"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, "README"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, "dir/readme"); err != nil {
		t.Fatal(err)
	}
	if err := enableWindowsNamePolicy(ctx, s); err != nil {
		t.Fatal(err)
	}
	err := s.makeNode(ctx, "symlink", "AUX", fs.ModeSymlink|0777)
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("symlink name admission: %v", err)
	}
}

func TestWindowsPolicyRejectsUnknownVersionAndAmbiguousLookup(t *testing.T) {
	s := openWindowsNameStore(t)
	ctx := t.Context()
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		_, _, err := s.windowsLookup(ctx, tx, s.root, []byte("name"))
		if !errors.Is(err, syscall.ENOSYS) {
			t.Fatalf("inactive lookup: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"README", "readme"} {
		if err := s.Create(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.mutateTransaction(ctx, ctx, nil, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE volumes SET windows_name_version = 1 WHERE id = ?`, s.volume)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		_, _, err := s.windowsLookup(ctx, tx, s.root, []byte("ReadMe"))
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("ambiguous lookup: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.mutateTransaction(ctx, ctx, nil, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE volumes SET windows_name_version = 999 WHERE id = ?`, s.volume)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, "fresh"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown policy creation: %v", err)
	}
}
