package localstore

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"golang.org/x/sys/unix"
)

func witnessReaderStore(t *testing.T) (*Store, int64, int64) {
	t.Helper()
	store, err := Open(t.Context(), Config{
		Root: privateTestRoot(t), Volume: "witness-reader", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	})
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
	if err := store.Create(t.Context(), "pending-checkpoint"); err != nil {
		t.Fatal(err)
	}
	accepted, checkpointed := store.durable.witness.generations()
	if accepted <= checkpointed {
		t.Fatalf("no pending checkpoint: accepted=%d checkpointed=%d", accepted, checkpointed)
	}
	return store, accepted, checkpointed
}

func TestWitnessInspectionRejectsAnInodeReplacedByCheckpoint(t *testing.T) {
	store, accepted, _ := witnessReaderStore(t)
	anchor, err := openRootAnchor(store.anchor.path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := anchor.Close(); err != nil {
			t.Error(err)
		}
	}()
	var before, after, current unix.Stat_t
	var beforeMount, afterMount uint64
	openat := anchor.witnessOps.openat
	anchor.witnessOps.openat = func(dir int, name string, flags int, mode uint32) (int, error) {
		fd, err := openat(dir, name, flags, mode)
		if err != nil || name != metastoreWitnessFilename {
			return fd, err
		}
		fail := func(err error) (int, error) { return -1, errors.Join(err, unix.Close(fd)) }
		if err := unix.Fstat(fd, &before); err != nil {
			return fail(err)
		}
		beforeMount, err = mountID(fd, anchor.statx)
		if err != nil {
			return fail(err)
		}
		result, err := store.durable.Store.Checkpoint(t.Context(), sqlite.FullCheckpoint)
		if err != nil {
			return fail(err)
		}
		if !result.Complete {
			return fail(fmt.Errorf("controlled checkpoint is incomplete: %+v", result))
		}
		if err := unix.Fstat(fd, &after); err != nil {
			return fail(err)
		}
		afterMount, err = mountID(fd, anchor.statx)
		if err != nil {
			return fail(err)
		}
		if err := unix.Fstatat(dir, metastoreWitnessFilename, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return fail(err)
		}
		return fd, nil
	}
	_, _, err = anchor.readMetastoreWitnessEntry(metastoreWitnessFilename, store.objects.ID(), store.volumeName)
	t.Logf("before dev=%d ino=%d mode=%#o uid=%d nlink=%d mount=%d; replaced fd dev=%d ino=%d mode=%#o uid=%d nlink=%d mount=%d; current ino=%d nlink=%d",
		before.Dev, before.Ino, before.Mode, before.Uid, before.Nlink, beforeMount, after.Dev, after.Ino, after.Mode, after.Uid, after.Nlink, afterMount, current.Ino, current.Nlink)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("replaced opened inode was accepted: %v", err)
	}
	if before.Nlink != 1 || after.Nlink != 0 || current.Nlink != 1 || before.Ino != after.Ino || current.Ino == after.Ino {
		t.Fatal("checkpoint did not replace the opened inode")
	}
	if before.Dev != after.Dev || uint64(after.Dev) != anchor.device || before.Mode != after.Mode || after.Mode&unix.S_IFMT != unix.S_IFREG || after.Mode&0o777 != 0o600 || before.Uid != after.Uid || after.Uid != uint32(unix.Geteuid()) || beforeMount != afterMount || afterMount != anchor.mount {
		t.Fatal("another private-file constraint changed")
	}
	record := readWitnessRecord(t, store)
	if record.State.Generation != accepted || record.CheckpointedGeneration != accepted {
		t.Fatalf("current witness lost completed checkpoint: %+v", record)
	}
}

func TestWitnessObservationHoldsPublisherLockAcrossDiskRead(t *testing.T) {
	store, accepted, checkpointed := witnessReaderStore(t)
	type checkpointOutcome struct {
		result sqlite.CheckpointResult
		err    error
	}
	started := make(chan struct{})
	finished := make(chan checkpointOutcome, 1)
	opened := false
	record := readWitnessRecordWithAnchor(t, store, func(path string) (*rootAnchor, error) {
		if store.durable.witness.mu.TryLock() {
			store.durable.witness.mu.Unlock()
			return nil, errors.New("witness observer opened its anchor without the publisher lock")
		}
		anchor, err := openRootAnchor(path)
		if err != nil {
			return nil, err
		}
		openat := anchor.witnessOps.openat
		anchor.witnessOps.openat = func(dir int, name string, flags int, mode uint32) (int, error) {
			fd, err := openat(dir, name, flags, mode)
			if err != nil || name != metastoreWitnessFilename {
				return fd, err
			}
			opened = true
			go func() {
				close(started)
				result, err := store.durable.Store.Checkpoint(t.Context(), sqlite.FullCheckpoint)
				finished <- checkpointOutcome{result, err}
			}()
			<-started
			return fd, nil
		}
		return anchor, nil
	})
	if !opened {
		t.Fatal("observer did not read the on-disk witness")
	}
	completed := <-finished
	if completed.err != nil || !completed.result.Complete {
		t.Fatalf("checkpoint after observation = %+v, %v", completed.result, completed.err)
	}
	if record.State.Generation != accepted || record.CheckpointedGeneration != checkpointed {
		t.Fatalf("observer did not retain the pre-publication disk record: %+v", record)
	}
	current := readWitnessRecord(t, store)
	if current.State.Generation != accepted || current.CheckpointedGeneration != accepted {
		t.Fatalf("subsequent observation did not read the replacement: %+v", current)
	}
}
