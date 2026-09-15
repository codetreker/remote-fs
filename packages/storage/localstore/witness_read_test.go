package localstore

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"golang.org/x/sys/unix"
)

func TestWitnessInspectionIdentifiesAnOpenedInodeReplacedByCheckpoint(t *testing.T) {
	config := witnessTestConfig(privateTestRoot(t))
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	store.durable.stop()
	<-store.durable.done
	if err := store.Create(t.Context(), "checkpoint-probe"); err != nil {
		t.Fatal(err)
	}
	accepted, checkpointed := store.durable.witness.generations()
	if checkpointed >= accepted {
		t.Fatalf("probe has no pending checkpoint: accepted=%d checkpointed=%d", accepted, checkpointed)
	}
	anchor, err := openRootAnchor(config.Root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := anchor.Close(); err != nil {
			t.Error(err)
		}
	})
	var before, after, current unix.Stat_t
	opened := false
	openat := anchor.witnessOps.openat
	anchor.witnessOps.openat = func(dir int, name string, flags int, mode uint32) (int, error) {
		fd, err := openat(dir, name, flags, mode)
		if err != nil || name != metastoreWitnessFilename {
			return fd, err
		}
		opened = true
		if err := unix.Fstat(fd, &before); err != nil {
			return -1, errors.Join(err, unix.Close(fd))
		}
		result, err := store.meta.Checkpoint(t.Context(), sqlite.FullCheckpoint)
		if err != nil {
			return -1, errors.Join(err, unix.Close(fd))
		}
		if !result.Complete {
			return -1, errors.Join(fmt.Errorf("controlled checkpoint did not complete: %+v", result), unix.Close(fd))
		}
		if err := unix.Fstat(fd, &after); err != nil {
			return -1, errors.Join(err, unix.Close(fd))
		}
		if err := unix.Fstatat(anchor.fd, metastoreWitnessFilename, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return -1, errors.Join(err, unix.Close(fd))
		}
		return fd, nil
	}
	_, _, err = anchor.readMetastoreWitnessEntry(metastoreWitnessFilename, store.objects.ID(), config.Volume)
	t.Logf("METASTORE root_device=%d euid=%d before={dev:%d ino:%d mode:%#o uid:%d nlink:%d} opened_after_checkpoint={dev:%d ino:%d mode:%#o uid:%d nlink:%d} current_path={dev:%d ino:%d mode:%#o uid:%d nlink:%d}",
		anchor.device, unix.Geteuid(), before.Dev, before.Ino, before.Mode, before.Uid, before.Nlink,
		after.Dev, after.Ino, after.Mode, after.Uid, after.Nlink,
		current.Dev, current.Ino, current.Mode, current.Uid, current.Nlink)
	if !opened || !errors.Is(err, syscall.EIO) {
		t.Fatalf("replaced witness read returned %v (opened=%v), want EIO", err, opened)
	}
	if before.Nlink != 1 || after.Nlink != 0 || before.Ino != after.Ino || current.Ino == after.Ino || current.Nlink != 1 {
		t.Fatalf("checkpoint did not replace the opened inode at %s", filepath.Join(config.Root, metastoreWitnessFilename))
	}
	if before.Dev != after.Dev || uint64(after.Dev) != anchor.device || before.Mode != after.Mode ||
		after.Mode&unix.S_IFMT != unix.S_IFREG || after.Mode&0777 != 0600 ||
		before.Uid != after.Uid || after.Uid != uint32(unix.Geteuid()) {
		t.Fatal("a predicate other than the link count changed")
	}
	record := readWitnessRecord(t, store)
	if record.State.Generation != accepted || record.CheckpointedGeneration != accepted {
		t.Fatalf("replacement witness did not retain the completed checkpoint: %+v", record)
	}
}
