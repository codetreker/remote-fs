package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/storage"
)

func seedWindowsPath(t *testing.T, s *Store, names []string) {
	t.Helper()
	ctx := t.Context()
	err := s.mutateTransaction(ctx, ctx, nil, func(tx *sql.Tx) error {
		parent := s.root
		for i, name := range names {
			node, err := dbstate.AllocateNodeID(ctx, tx)
			if err != nil {
				return err
			}
			mode := int64(fs.ModeDir | 0755)
			if i == len(names)-1 {
				mode = 0644
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO nodes(id,volume,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec) VALUES(?,?,?,0,0,0,0,0)`, node, s.volume, mode); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO entries(volume,parent,name,node) VALUES(?,?,?,?)`, s.volume, parent, []byte(name), node); err != nil {
				return err
			}
			parent = node
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWindowsPathMetricsBoundaries(t *testing.T) {
	max := storage.WindowsMaxPathUTF16Units
	if max != 32767 {
		t.Fatalf("SMB CREATE wire bound changed: %d", max)
	}
	for _, test := range []struct {
		name       string
		base, part windowsPathSize
		want       bool
	}{
		{"exact UTF16", windowsPathSize{units: max - 256, depth: 1}, windowsPathPart([]byte(strings.Repeat("x", 255))), true},
		{"UTF16 overflow", windowsPathSize{units: max - 255, depth: 1}, windowsPathPart([]byte(strings.Repeat("x", 255))), false},
		{"supplementary exact", windowsPathSize{units: max - 3, depth: 1}, windowsPathPart([]byte("😀")), true},
		{"supplementary overflow", windowsPathSize{units: max - 2, depth: 1}, windowsPathPart([]byte("😀")), false},
		{"exact bytes", windowsPathSize{bytes: metastore.MaxNotificationNameBytes - 4, depth: 1}, windowsPathPart([]byte("😀")), true},
		{"byte overflow", windowsPathSize{bytes: metastore.MaxNotificationNameBytes - 3, depth: 1}, windowsPathPart([]byte("😀")), false},
		{"exact depth", windowsPathSize{depth: metastore.MaxNotificationAncestors - 1}, windowsPathPart([]byte("x")), true},
		{"depth overflow", windowsPathSize{depth: metastore.MaxNotificationAncestors}, windowsPathPart([]byte("x")), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.base.child(test.part)
			if (err == nil) != test.want {
				t.Fatalf("child error=%v, want success=%v", err, test.want)
			}
		})
	}
}

func TestWindowsActivationRejectsUnaddressableFullPath(t *testing.T) {
	s := openWindowsNameStore(t)
	names := make([]string, 131)
	for i := range names {
		names[i] = strings.Repeat("x", 255)
	}
	seedWindowsPath(t, s, names)
	if err := enableWindowsNamePolicy(t.Context(), s); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("activation=%v", err)
	}
	if err := s.inspect(t.Context(), func(tx *sql.Tx) error {
		version, err := s.windowsNameVersion(t.Context(), tx)
		if version != 0 {
			t.Fatal("failed activation enabled policy")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(t.Context(), strings.Join(names, "/")); err != nil {
		t.Fatal("activation changed tree", err)
	}
}

func TestWindowsDirectoryRenameValidatesDescendantPathsAtomically(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "enabled"}[enabled], func(t *testing.T) {
			s := openWindowsNameStore(t)
			ctx := t.Context()
			names := []string{"a"}
			for range 127 {
				names = append(names, strings.Repeat("x", 255))
			}
			names = append(names, strings.Repeat("y", 253))
			seedWindowsPath(t, s, names)
			oldPath := strings.Join(names, "/")
			if len(oldPath) != storage.WindowsMaxPathUTF16Units {
				t.Fatal("fixture not at exact limit")
			}
			old, err := s.Stat(ctx, oldPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Mkdir(ctx, "aa"); err != nil {
				t.Fatal(err)
			}
			destination, err := s.Stat(ctx, "aa")
			if err != nil {
				t.Fatal(err)
			}
			if enabled {
				if err := enableWindowsNamePolicy(ctx, s); err != nil {
					t.Fatal(err)
				}
			}
			err = s.Rename(ctx, "a", "aa")
			if enabled {
				if !errors.Is(err, syscall.ENAMETOOLONG) {
					t.Fatalf("growing rename=%v", err)
				}
				after, err := s.Stat(ctx, oldPath)
				if err != nil || after.ID != old.ID {
					t.Fatalf("failed rename changed descendant: %v", err)
				}
				if after, err := s.Stat(ctx, "aa"); err != nil || after.ID != destination.ID {
					t.Fatalf("failed rename changed displaced destination: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal("inactive policy restricted rename", err)
				}
				if _, err := s.Stat(ctx, "aa"+oldPath[1:]); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestWindowsPathsRejectAncestorAndDescendantBudgetExhaustion(t *testing.T) {
	s := openWindowsNameStore(t)
	ctx := t.Context()
	seedWindowsPath(t, s, []string{"a", "child"})
	if err := enableWindowsNamePolicy(ctx, s); err != nil {
		t.Fatal(err)
	}
	s.maxIntegrityRecords = 1
	if err := s.Rename(ctx, "a", "longer"); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("walk record budget=%v", err)
	}
	s.maxIntegrityRecords = 100
	s.maxIntegrityBytes = 1
	if err := s.Rename(ctx, "a", "longer"); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("walk byte budget=%v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	graph := map[int64]*windowsPathNode{2: {parent: s.root, part: windowsPathPart([]byte("name"))}}
	if err := validateWindowsPaths(cancelled, s.root, graph); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled graph validation=%v", err)
	}
}

func TestWindowsPathsDetectCorruptGraph(t *testing.T) {
	for _, graph := range []map[int64]*windowsPathNode{
		{2: {parent: 3, part: windowsPathPart([]byte("a"))}},
		{2: {parent: 3, part: windowsPathPart([]byte("a"))}, 3: {parent: 2, part: windowsPathPart([]byte("b"))}},
	} {
		if err := validateWindowsPaths(t.Context(), 1, graph); !errors.Is(err, syscall.EIO) {
			t.Fatalf("corrupt graph=%v", err)
		}
	}
}

func TestWindowsActivationChecksFullPathBytesAndDepth(t *testing.T) {
	for _, test := range []struct {
		name, component string
		count           int
	}{{"bytes", strings.Repeat("漢", 255), 86}, {"depth", "x", 257}} {
		t.Run(test.name, func(t *testing.T) {
			s := openWindowsNameStore(t)
			names := make([]string, test.count)
			for i := range names {
				names[i] = test.component
			}
			seedWindowsPath(t, s, names)
			if err := enableWindowsNamePolicy(t.Context(), s); !errors.Is(err, syscall.ENAMETOOLONG) {
				t.Fatalf("activation=%v", err)
			}
		})
	}
}

func TestWindowsCreateChecksFullPathBeforePublication(t *testing.T) {
	s := openWindowsNameStore(t)
	ctx := t.Context()
	names := make([]string, 128)
	for i := range names {
		names[i] = strings.Repeat("x", 255)
	}
	seedWindowsPath(t, s, names)
	fullPath := strings.Join(names, "/")
	if len(fullPath) != storage.WindowsMaxPathUTF16Units {
		t.Fatal("fixture not at boundary")
	}
	node, err := s.Stat(ctx, fullPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.mutateTransaction(ctx, ctx, nil, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE nodes SET mode=? WHERE id=?`, int64(fs.ModeDir|0755), node.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := enableWindowsNamePolicy(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, fullPath+"/x"); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("unaddressable create=%v", err)
	}
	if _, err := s.Stat(ctx, fullPath+"/x"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed create published node=%v", err)
	}
}
