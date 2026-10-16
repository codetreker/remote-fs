package localstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

type readonlyRootEntry struct {
	info   fs.FileInfo
	digest [sha256.Size]byte
	uid    uint32
	gid    uint32
	links  uint64
}

type readonlyRootFixture struct {
	config   Config
	ack      witnessAcknowledgment
	owner    context.Context
	baseline map[string]readonlyRootEntry
	reusable bool
}

func newReadonlyRootFixture(t *testing.T) *readonlyRootFixture {
	t.Helper()
	config, ack := closedWitnessFixture(t)
	return &readonlyRootFixture{config: config, ack: ack, owner: t.Context(), baseline: snapshotReadonlyRoot(t, config.Root), reusable: true}
}

func snapshotReadonlyRoot(t *testing.T, root string) map[string]readonlyRootEntry {
	t.Helper()
	entries := make(map[string]readonlyRootEntry)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		stat := info.Sys().(*syscall.Stat_t)
		state := readonlyRootEntry{info: info, uid: stat.Uid, gid: stat.Gid, links: uint64(stat.Nlink)}
		if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			digest := sha256.New()
			_, readErr := io.Copy(digest, file)
			if err := errors.Join(readErr, file.Close()); err != nil {
				return err
			}
			copy(state.digest[:], digest.Sum(nil))
		} else if !info.IsDir() {
			return fmt.Errorf("unexpected fixture entry %s: %s", relative, info.Mode())
		}
		entries[relative] = state
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func readonlyRootDifference(before, after map[string]readonlyRootEntry) error {
	if len(before) != len(after) {
		return fmt.Errorf("entry count changed: got %d, want %d", len(after), len(before))
	}
	for path, original := range before {
		current, ok := after[path]
		if !ok {
			return fmt.Errorf("missing fixture entry %s", path)
		}
		// Stage cleanup changes directory timestamps, and reads can change access times.
		if !os.SameFile(original.info, current.info) || original.info.Mode() != current.info.Mode() || original.digest != current.digest ||
			original.uid != current.uid || original.gid != current.gid || original.links != current.links ||
			original.info.Mode().IsRegular() && original.info.Size() != current.info.Size() {
			return fmt.Errorf("fixture identity, mode, size or contents changed at %s", path)
		}
	}
	return nil
}

func (fixture *readonlyRootFixture) run(t *testing.T, name string, exercise, restore func(*testing.T)) {
	t.Helper()
	if !fixture.reusable {
		t.Fatal("readonly fixture restoration failed; refusing another child")
	}
	t.Run(name, func(t *testing.T) {
		fixture.reusable = false
		t.Cleanup(func() {
			restore(t)
			fixture.requireUnchanged(t)
			objects, err := localdisk.Open(fixture.owner, fixture.config.Root, fixture.config.LocalDisk)
			if err != nil {
				t.Fatalf("failed readonly Open retained object-store ownership: %v", err)
			}
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
			fixture.requireUnchanged(t)
			fixture.reusable = true
		})
		exercise(t)
	})
}

func (fixture *readonlyRootFixture) requireUnchanged(t *testing.T) {
	t.Helper()
	if err := readonlyRootDifference(fixture.baseline, snapshotReadonlyRoot(t, fixture.config.Root)); err != nil {
		t.Fatal(err)
	}
}

func (fixture *readonlyRootFixture) requireHealthy(t *testing.T) {
	t.Helper()
	if !fixture.reusable {
		t.Fatal("readonly fixture restoration failed; refusing healthy successor")
	}
	fixture.requireUnchanged(t)
	store, err := Open(t.Context(), fixture.config)
	if err != nil {
		t.Fatalf("healthy successor could not reopen restored root: %v", err)
	}
	requireWitnessAcknowledgment(t, store, fixture.ack)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func removeReadonlyArtifact(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

func TestMalformedCompletionMarkerFailsClosed(t *testing.T) {
	fixture := newReadonlyRootFixture(t)
	path := filepath.Join(fixture.config.Root, "LOCALSTORE")
	marker, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, corrupt := range map[string]func([]byte) []byte{
		"truncated": func(marker []byte) []byte { return marker[:len(marker)-1] },
		"magic": func(marker []byte) []byte {
			marker[0] ^= 0xff
			return marker
		},
		"total length": func(marker []byte) []byte {
			binary.BigEndian.PutUint32(marker[12:16], uint32(len(marker)+1))
			return marker
		},
		"volume length": func(marker []byte) []byte {
			binary.BigEndian.PutUint32(marker[16:20], MaxVolumeBytes+1)
			return marker
		},
		"checksum": func(marker []byte) []byte {
			marker[len(marker)-1] ^= 0xff
			return marker
		},
	} {
		fixture.run(t, name, func(t *testing.T) {
			damaged := corrupt(bytes.Clone(marker))
			if err := os.WriteFile(path, damaged, 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := Open(t.Context(), fixture.config)
			if store != nil {
				if closeErr := store.Close(); closeErr != nil {
					t.Errorf("closing unexpectedly accepted store: %v", closeErr)
				}
			}
			if store != nil || !errors.Is(err, syscall.EIO) {
				t.Fatalf("malformed completion marker returned store=%v error=%v, want EIO", store != nil, err)
			}
			requireWitnessStageBytes(t, path, damaged)
		}, func(t *testing.T) {
			if err := os.WriteFile(path, marker, 0o600); err != nil {
				t.Fatal(err)
			}
		})
	}
	fixture.requireHealthy(t)
}

func TestReadonlyRootFixtureDetectsResidueBeforeHealthySuccessor(t *testing.T) {
	fixture := newReadonlyRootFixture(t)
	path := filepath.Join(fixture.config.Root, "LOCALSTORE")
	marker, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	saved := filepath.Join(fixture.config.Root, "saved-completion-marker")
	for _, kind := range []string{"same length contents", "permissions", "replacement inode", "unexpected entry"} {
		fixture.run(t, kind, func(t *testing.T) {
			switch kind {
			case "same length contents":
				changed := bytes.Clone(marker)
				changed[len(changed)-1] ^= 0xff
				err = os.WriteFile(path, changed, 0o600)
			case "permissions":
				err = os.Chmod(path, 0o640)
			case "replacement inode":
				if err = os.Rename(path, saved); err == nil {
					err = os.WriteFile(path, marker, 0o600)
				}
			case "unexpected entry":
				err = os.WriteFile(saved, []byte("unrestored stage"), 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			snapshot := snapshotReadonlyRoot(t, fixture.config.Root)
			if kind == "replacement inode" {
				delete(snapshot, filepath.Base(saved))
			}
			if err := readonlyRootDifference(fixture.baseline, snapshot); err == nil {
				t.Fatalf("fixture accepted %s residue", kind)
			}
		}, func(t *testing.T) {
			if kind == "replacement inode" {
				removeReadonlyArtifact(t, path)
				if err := os.Rename(saved, path); err != nil {
					t.Fatal(err)
				}
			} else {
				removeReadonlyArtifact(t, saved)
				if err := os.WriteFile(path, marker, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Chmod(path, 0o600); err != nil {
				t.Fatal(err)
			}
		})
	}
	fixture.requireHealthy(t)
}
