package fuse_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func TestWindowsLinkConversionRefreshesCachedFUSEType(t *testing.T) {
	requireFUSE(t)
	for _, kind := range []struct {
		name string
		kind storage.WindowsKind
		mode fs.FileMode
	}{
		{"regular", storage.WindowsRegularFile, 0},
		{"directory", storage.WindowsDirectory, fs.ModeDir},
	} {
		t.Run(kind.name, func(t *testing.T) {
			_, backing := memoryfixture.New(t, "type-change", 0, locking.DefaultOptions())
			state, err := backing.WindowsState(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			activation, err := storage.NewLockRequestID(state.ActionEpoch)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := backing.EnableWindows(t.Context(), activation); err != nil {
				t.Fatal(err)
			}
			session, err := backing.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := session.Close(context.Background()); err != nil {
					t.Errorf("closing Windows session: %v", err)
				}
			})
			action := func() storage.WindowsActionID {
				status, err := session.Status(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				id, err := storage.NewLockRequestID(status.ActionEpoch)
				if err != nil {
					t.Fatal(err)
				}
				return id
			}
			root, err := backing.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			opened, err := session.Open(t.Context(), storage.WindowsOpenRequest{
				WindowsOpenIntent: storage.WindowsOpenIntent{
					Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll,
					Disposition: storage.WindowsCreate, Kind: kind.kind,
				},
				Lookup: storage.WindowsLookup{ParentID: root.ID, Name: "entry"}, Mode: 0o700,
			}, action())
			if err != nil || opened.File == nil {
				t.Fatalf("creating Windows reference: %+v, %v", opened, err)
			}
			mountpoint := mountStorage(t, backing, fuse.Options{Logger: testLogger(t)})
			path := filepath.Join(mountpoint, "entry")
			before, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if before.Mode().Type() != kind.mode {
				t.Fatalf("initial type = %v, want %v", before.Mode().Type(), kind.mode)
			}
			converted, err := opened.File.SetLink(t.Context(), "target", action())
			if err != nil || converted.State != storage.WindowsActionCompleted || converted.Attr.ID != opened.Attr.ID {
				t.Fatalf("same-object link conversion = %+v, %v", converted, err)
			}
			for range 2 {
				after, err := os.Lstat(path)
				if err != nil {
					t.Fatalf("stat after link conversion: %v", err)
				}
				if after.Mode().Type() != fs.ModeSymlink || after.Size() != int64(len("target")) {
					t.Fatalf("converted node type/size = %v/%d, want symlink/%d", after.Mode().Type(), after.Size(), len("target"))
				}
				if !os.SameFile(before, after) || after.Sys().(*syscall.Stat_t).Ino != opened.Attr.ID {
					t.Fatalf("link conversion changed inode: before=%d after=%d native=%d", before.Sys().(*syscall.Stat_t).Ino, after.Sys().(*syscall.Stat_t).Ino, opened.Attr.ID)
				}
			}
			if _, err := os.Readlink(path); !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("unsupported FUSE link resolution returned %v, want EOPNOTSUPP", err)
			}
		})
	}
}
