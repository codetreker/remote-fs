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

func TestKindConversionRefreshesCachedFUSEType(t *testing.T) {
	requireFUSE(t)
	for _, kind := range []struct {
		name string
		kind storage.NodeKind
		mode fs.FileMode
	}{
		{"regular", storage.NodeRegular, 0},
		{"directory", storage.NodeDirectory, fs.ModeDir},
	} {
		t.Run(kind.name, func(t *testing.T) {
			_, backing := memoryfixture.New(t, "type-change", 0, locking.DefaultOptions())
			session, _, err := backing.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			action := func() storage.FileActionID {
				status, err := session.Status(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				id, err := storage.NewFileActionID(status.ActionEpoch)
				if err != nil {
					t.Fatal(err)
				}
				return id
			}
			t.Cleanup(func() {
				if _, err := session.Close(context.Background(), action()); err != nil {
					t.Errorf("closing file session: %v", err)
				}
			})
			root, err := backing.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			retained, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: root.ID}, action())
			if err != nil {
				t.Fatal(err)
			}
			parent, err := session.Reference(t.Context(), retained.Reference)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := parent.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
			if err != nil {
				t.Fatal(err)
			}
			opened, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{
				Target:  storage.EntryTarget{Parent: parent.Reference(), ParentID: root.ID, Name: []byte("entry"), DirectoryRevision: observed.Attr.DirectoryRevision, Witness: observed.Location},
				Initial: storage.NodeInitial{Kind: kind.kind, Metadata: permissionMetadata(0700)},
				Claim:   storage.AccessClaim{Uses: storage.AllAccessUses},
			}, action())
			if err != nil {
				t.Fatal(err)
			}
			file, err := session.Reference(t.Context(), opened.Reference)
			if err != nil {
				t.Fatal(err)
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
			converted, err := file.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: opened.Observation.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("target"), Metadata: permissionMetadata(0777)}, action())
			if err != nil || converted.State != storage.FileActionCompleted || converted.Observation.Attr.ID != opened.Observation.Attr.ID {
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
				if !os.SameFile(before, after) || after.Sys().(*syscall.Stat_t).Ino != opened.Observation.Attr.ID {
					t.Fatalf("link conversion changed inode: before=%d after=%d native=%d", before.Sys().(*syscall.Stat_t).Ino, after.Sys().(*syscall.Stat_t).Ino, opened.Observation.Attr.ID)
				}
			}
			if _, err := os.Readlink(path); !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("unsupported FUSE link resolution returned %v, want EOPNOTSUPP", err)
			}
		})
	}
}
