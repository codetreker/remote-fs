package localstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
	"golang.org/x/sys/unix"
)

const witnessCrashEnvironment = "REMOTE_FS_WITNESS_CRASH"
const witnessCrashRootEnvironment = "REMOTE_FS_WITNESS_CRASH_ROOT"
const witnessAcknowledgedName = "acknowledged"
const witnessAcknowledgedContents = "payload acknowledged before witness interruption"

type witnessAcknowledgment struct {
	Attr     storage.Attr
	Record   metastoreWitnessRecord
	WALBytes int64
	Phase    string
}

func witnessTestConfig(root string) Config {
	return Config{Root: root, Volume: "witness-recovery", Quota: 1 << 20,
		Window: sqlite.DefaultWindow(), Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8}}
}

func TestEmptyWitnessStageRecoversAcknowledgedWALAfterSIGKILL(t *testing.T) {
	if os.Getenv(witnessCrashEnvironment) == "baseline" {
		runWitnessAcknowledgedChild(t)
		return
	}
	config := witnessTestConfig(privateTestRoot(t))
	ack := killWitnessChild(t, config.Root, "baseline", "TestEmptyWitnessStageRecoversAcknowledgedWALAfterSIGKILL")
	if err := os.WriteFile(filepath.Join(config.Root, metastoreWitnessStage), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for attempt := range 2 {
		store, err := Open(t.Context(), config)
		if err != nil {
			t.Fatalf("reopen %d rejected acknowledged WAL beside an empty unpublished stage: %v", attempt+1, err)
		}
		requireWitnessAcknowledgment(t, store, ack)
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Lstat(filepath.Join(config.Root, metastoreWitnessStage)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted stage remains after successful reopen: %v", err)
	}
}

func runWitnessAcknowledgedChild(t *testing.T) {
	_, snapshot, ack := prepareWitnessChild(t)
	defer snapshot.Close()
	acknowledgeWitnessCut(t, ack)
}

func acknowledgeWitnessCut(t *testing.T, ack witnessAcknowledgment) {
	if err := json.NewEncoder(os.Stdout).Encode(ack); err != nil {
		t.Fatal(err)
	}
	select {}
}

func prepareWitnessChild(t *testing.T) (*Store, metastore.Snap, witnessAcknowledgment) {
	root := os.Getenv(witnessCrashRootEnvironment)
	if root == "" {
		t.Fatal("missing witness child root")
	}
	store, err := Open(t.Context(), witnessTestConfig(root))
	if err != nil {
		t.Fatal(err)
	}
	store.durable.stop()
	<-store.durable.done
	snapshot, _, err := store.Log().Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(t.Context(), witnessAcknowledgedName, []byte(witnessAcknowledgedContents)); err != nil {
		t.Fatal(err)
	}
	mode := fs.FileMode(0o640)
	atime, mtime := time.Unix(1700000011, 123456789).UTC(), time.Unix(1700000022, 987654321).UTC()
	if err := store.SetAttr(t.Context(), witnessAcknowledgedName, storage.AttrChange{Mode: &mode, AccessTime: &atime, ModTime: &mtime}); err != nil {
		t.Fatal(err)
	}
	attr, err := store.Stat(t.Context(), witnessAcknowledgedName)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, metastoreFilename+"-wal"))
	if err != nil {
		t.Fatal(err)
	}
	record := readWitnessRecord(t, store)
	if info.Size() <= 32 || record.State.Generation <= record.CheckpointedGeneration {
		t.Fatalf("child did not preserve acknowledged WAL: bytes=%d record=%+v", info.Size(), record)
	}
	ack := witnessAcknowledgment{Attr: attr, Record: record, WALBytes: info.Size(), Phase: "baseline"}
	return store, snapshot, ack
}

func requireWitnessAcknowledgment(t *testing.T, store *Store, ack witnessAcknowledgment) {
	t.Helper()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing recovered store: %v", err)
		}
	})
	attr, err := store.Stat(t.Context(), witnessAcknowledgedName)
	if err != nil {
		t.Fatal(err)
	}
	if attr.ID != ack.Attr.ID || attr.Mode != ack.Attr.Mode || attr.Size != ack.Attr.Size ||
		!attr.AccessTime.Equal(ack.Attr.AccessTime) || !attr.ModTime.Equal(ack.Attr.ModTime) {
		t.Fatalf("acknowledged attributes changed: got %+v, want %+v", attr, ack.Attr)
	}
	contents, err := store.Read(t.Context(), witnessAcknowledgedName)
	if err != nil || string(contents) != witnessAcknowledgedContents {
		t.Fatalf("acknowledged contents = %q, %v", contents, err)
	}
}

func killWitnessChild(t *testing.T, root, phase, testName string) witnessAcknowledgment {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+testName+"$", "-test.timeout=25s")
	command.Env = append(os.Environ(), witnessCrashEnvironment+"="+phase, witnessCrashRootEnvironment+"="+root)
	command.Stderr = os.Stderr
	command.WaitDelay = time.Second
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdout.Close()
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	readDone := make(chan struct{})
	type receipt struct {
		encoded []byte
		err     error
	}
	ready := make(chan receipt, 1)
	go func() {
		defer close(readDone)
		line, err := bufio.NewReaderSize(stdout, 4096).ReadSlice('\n')
		ready <- receipt{encoded: append([]byte(nil), line...), err: err}
	}()
	defer func() {
		if !waited {
			if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("kill witness child during cleanup: %v", err)
			}
			var exit *exec.ExitError
			if err := command.Wait(); err != nil && !errors.As(err, &exit) {
				t.Errorf("reap witness child: %v", err)
			}
		}
		<-readDone
	}()
	var ack witnessAcknowledgment
	select {
	case result := <-ready:
		if result.err != nil {
			t.Fatalf("child did not acknowledge %s: %v (%q)", phase, result.err, result.encoded)
		}
		if err := json.Unmarshal(result.encoded, &ack); err != nil {
			t.Fatalf("decode child receipt: %v (%q)", err, result.encoded)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for witness child %s: %v", phase, ctx.Err())
	}
	if ack.Phase != phase || ack.Attr.ID == 0 || ack.Attr.Size != int64(len(witnessAcknowledgedContents)) || ack.WALBytes <= 32 {
		t.Fatalf("invalid acknowledgement for %s: %+v", phase, ack)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("child did not terminate from SIGKILL: %v", err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child wait status = %v, want SIGKILL", exit.Sys())
	}
	info, err := os.Stat(filepath.Join(root, metastoreFilename+"-wal"))
	if err != nil || info.Size() <= 32 {
		t.Fatalf("killed child left no WAL frames: %v, %v", info, err)
	}
	t.Logf("verified SIGKILL after %s, acknowledged id=%d mode=%o size=%d A=%d C=%d WAL=%d bytes",
		phase, ack.Attr.ID, ack.Attr.Mode, ack.Attr.Size, ack.Record.State.Generation, ack.Record.CheckpointedGeneration, info.Size())
	return ack
}

func TestWitnessPublicationSIGKILLPreservesAcknowledgedData(t *testing.T) {
	if phase := os.Getenv(witnessCrashEnvironment); phase != "" {
		runWitnessPublicationChild(t, phase)
		return
	}
	for _, operation := range []string{"accept", "checkpoint"} {
		for _, cut := range []string{"create", "partial-write", "full-write", "file-fsync", "rename", "root-fsync"} {
			t.Run(operation+"/"+cut, func(t *testing.T) {
				config := witnessTestConfig(privateTestRoot(t))
				phase := operation + "/" + cut
				ack := killWitnessChild(t, config.Root, phase, "TestWitnessPublicationSIGKILLPreservesAcknowledgedData")
				requireWitnessPublicationCut(t, config, ack, operation, cut)
				for attempt := range 2 {
					store, err := Open(t.Context(), config)
					if err != nil {
						t.Fatalf("reopen %d after %s: %v", attempt+1, phase, err)
					}
					requireWitnessAcknowledgment(t, store, ack)
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func runWitnessPublicationChild(t *testing.T, phase string) {
	operation, cut, ok := strings.Cut(phase, "/")
	if !ok {
		t.Fatalf("invalid publication phase %q", phase)
	}
	store, snapshot, ack := prepareWitnessChild(t)
	ack.Phase = phase
	ops := store.anchor.witnessOps
	stageFD, written, expected := -1, 0, 0
	renamed := false
	after := func(reached string) {
		if reached == cut {
			acknowledgeWitnessCut(t, ack)
		}
	}
	store.anchor.witnessOps.openat = func(dir int, name string, flags int, mode uint32) (int, error) {
		fd, err := ops.openat(dir, name, flags, mode)
		if err == nil && name == metastoreWitnessStage && flags&unix.O_CREAT != 0 {
			stageFD = fd
			after("create")
		}
		return fd, err
	}
	store.anchor.witnessOps.write = func(fd int, content []byte) (int, error) {
		if fd != stageFD {
			return ops.write(fd, content)
		}
		if expected == 0 {
			expected = len(content)
		}
		if cut == "partial-write" && written == 0 {
			content = content[:len(content)/2]
		}
		n, err := ops.write(fd, content)
		written += n
		if err == nil && n > 0 {
			if cut == "partial-write" {
				after("partial-write")
			}
			if written == expected {
				after("full-write")
			}
		}
		return n, err
	}
	store.anchor.witnessOps.fsync = func(fd int) error {
		err := ops.fsync(fd)
		if err == nil {
			if fd == stageFD {
				after("file-fsync")
			}
			if fd == store.anchor.fd && renamed {
				after("root-fsync")
			}
		}
		return err
	}
	store.anchor.witnessOps.renameat = func(oldDir int, oldName string, newDir int, newName string) error {
		err := ops.renameat(oldDir, oldName, newDir, newName)
		if err == nil && oldName == metastoreWitnessStage && newName == metastoreWitnessFilename {
			renamed = true
			after("rename")
		}
		return err
	}
	switch operation {
	case "accept":
		defer snapshot.Close()
		if err := store.Create(t.Context(), "unacknowledged-publication"); err != nil {
			t.Fatal(err)
		}
	case "checkpoint":
		if err := snapshot.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := store.durable.Store.Checkpoint(t.Context(), sqlite.PassiveCheckpoint); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown witness operation %q", operation)
	}
	t.Fatalf("publication returned without reaching %s", phase)
}

func requireWitnessPublicationCut(t *testing.T, config Config, ack witnessAcknowledgment, operation, cut string) {
	t.Helper()
	final, err := os.ReadFile(filepath.Join(config.Root, metastoreWitnessFilename))
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeMetastoreWitness(final, ack.Record.StoreID, config.Volume)
	if err != nil {
		t.Fatal(err)
	}
	published := cut == "rename" || cut == "root-fsync"
	if !published && record != ack.Record {
		t.Fatalf("unpublished stage changed final witness: got %+v want %+v", record, ack.Record)
	}
	if published {
		if operation == "accept" && (record.State.Generation <= ack.Record.State.Generation || record.CheckpointedGeneration != ack.Record.CheckpointedGeneration) {
			t.Fatalf("accepted publication cut has wrong generations: %+v, previous %+v", record, ack.Record)
		}
		if operation == "checkpoint" && (record.State != ack.Record.State || record.CheckpointedGeneration != ack.Record.State.Generation) {
			t.Fatalf("checkpoint publication cut has wrong generations: %+v, accepted %+v", record, ack.Record)
		}
	}
	stage, err := os.ReadFile(filepath.Join(config.Root, metastoreWitnessStage))
	if published {
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rename cut retained stage: %v", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	switch cut {
	case "create":
		if len(stage) != 0 {
			t.Fatalf("post-create stage contains %d bytes", len(stage))
		}
	case "partial-write":
		if len(stage) == 0 || len(stage) >= len(final) {
			t.Fatalf("partial-write stage length = %d, complete length = %d", len(stage), len(final))
		}
	case "full-write", "file-fsync":
		if _, err := decodeMetastoreWitness(stage, ack.Record.StoreID, config.Volume); err != nil {
			t.Fatalf("completed stage at %s: %v", cut, err)
		}
	}
}

func closedWitnessFixture(t *testing.T) (Config, witnessAcknowledgment) {
	t.Helper()
	config := witnessTestConfig(privateTestRoot(t))
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(t.Context(), witnessAcknowledgedName, []byte(witnessAcknowledgedContents)); err != nil {
		store.Close()
		t.Fatal(err)
	}
	attr, err := store.Stat(t.Context(), witnessAcknowledgedName)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	id := store.objects.ID()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(filepath.Join(config.Root, metastoreWitnessFilename))
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeMetastoreWitness(encoded, id, config.Volume)
	if err != nil {
		t.Fatal(err)
	}
	return config, witnessAcknowledgment{Attr: attr, Record: record}
}

func TestValidFinalRecoversBoundedReadableTornWitnessStages(t *testing.T) {
	for _, test := range []struct {
		name   string
		damage func([]byte) []byte
	}{
		{"empty", func([]byte) []byte { return nil }},
		{"prefix", func(b []byte) []byte { return b[:len(b)/2] }},
		{"full checksum tear", func(b []byte) []byte { b[len(b)-1] ^= 0xff; return b }},
		{"identity bytes with torn checksum", func(b []byte) []byte { b[64] ^= 0xff; return b }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, ack := closedWitnessFixture(t)
			final, err := os.ReadFile(filepath.Join(config.Root, metastoreWitnessFilename))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(config.Root, metastoreWitnessStage), test.damage(final), 0o600); err != nil {
				t.Fatal(err)
			}
			for attempt := range 2 {
				store, err := Open(t.Context(), config)
				if err != nil {
					t.Fatalf("reopen %d with readable %s stage: %v", attempt+1, test.name, err)
				}
				requireWitnessAcknowledgment(t, store, ack)
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Lstat(filepath.Join(config.Root, metastoreWitnessStage)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("stage was not cleaned: %v", err)
			}
		})
	}
}

func TestValidForeignWitnessStagesRemainRejectedAndUntouched(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*metastoreWitnessRecord)
	}{
		{"object identity", func(r *metastoreWitnessRecord) { r.StoreID[0] ^= 0xff }},
		{"volume identity", func(r *metastoreWitnessRecord) { r.Volume = "foreign-volume" }},
		{"database identity", func(r *metastoreWitnessRecord) { r.State.DatabaseID = strings.Repeat("a", durableDatabaseIDBytes) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, ack := closedWitnessFixture(t)
			record := ack.Record
			test.change(&record)
			encoded, err := encodeMetastoreWitness(record)
			if err != nil {
				t.Fatal(err)
			}
			stage := filepath.Join(config.Root, metastoreWitnessStage)
			if err := os.WriteFile(stage, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := Open(t.Context(), config)
			if store != nil {
				store.Close()
			}
			if store != nil || !errors.Is(err, syscall.EIO) {
				t.Fatalf("foreign stage returned store=%v error=%v", store != nil, err)
			}
			requireWitnessStageBytes(t, stage, encoded)
		})
	}
}

func requireWitnessStageBytes(t *testing.T, stage string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(stage)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("failed recovery changed stage: got %x err=%v want %x", got, err, want)
	}
}

func TestWitnessStageRemainsUntilDatabaseAndWALAreValidated(t *testing.T) {
	for _, damage := range []string{"missing final", "torn final", "database identity", "missing WAL", "empty WAL", "header-only WAL", "corrupt WAL"} {
		t.Run(damage, func(t *testing.T) {
			config := witnessTestConfig(privateTestRoot(t))
			ack := killWitnessChild(t, config.Root, "baseline", "TestEmptyWitnessStageRecoversAcknowledgedWALAfterSIGKILL")
			stage := filepath.Join(config.Root, metastoreWitnessStage)
			residue := []byte("readable unfinished witness record")
			if err := os.WriteFile(stage, residue, 0o600); err != nil {
				t.Fatal(err)
			}
			final, wal := filepath.Join(config.Root, metastoreWitnessFilename), filepath.Join(config.Root, metastoreFilename+"-wal")
			var err error
			switch damage {
			case "missing final":
				err = os.Remove(final)
			case "torn final":
				err = os.WriteFile(final, []byte("invalid final"), 0o600)
			case "database identity":
				record := ack.Record
				if record.State.DatabaseID[0] == 'a' {
					record.State.DatabaseID = "b" + record.State.DatabaseID[1:]
				} else {
					record.State.DatabaseID = "a" + record.State.DatabaseID[1:]
				}
				var encoded []byte
				encoded, err = encodeMetastoreWitness(record)
				if err == nil {
					err = os.WriteFile(final, encoded, 0o600)
				}
			case "missing WAL":
				err = os.Remove(wal)
			case "empty WAL":
				err = os.Truncate(wal, 0)
			case "header-only WAL":
				err = os.Truncate(wal, 32)
			case "corrupt WAL":
				var encoded []byte
				encoded, err = os.ReadFile(wal)
				if err == nil {
					encoded[0] ^= 0xff
					err = os.WriteFile(wal, encoded, 0o600)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			store, err := Open(t.Context(), config)
			if store != nil {
				store.Close()
			}
			if store != nil || !errors.Is(err, syscall.EIO) {
				t.Fatalf("%s returned store=%v error=%v", damage, store != nil, err)
			}
			requireWitnessStageBytes(t, stage, residue)
		})
	}
}

func TestUnsafeWitnessStagesAreNotInterruptedRecords(t *testing.T) {
	for _, kind := range []string{"directory", "symlink", "hard link", "public permissions", "oversized", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			config, _ := closedWitnessFixture(t)
			stage := filepath.Join(config.Root, metastoreWitnessStage)
			var err error
			switch kind {
			case "directory":
				err = os.Mkdir(stage, 0o700)
			case "symlink":
				err = os.Symlink(metastoreWitnessFilename, stage)
			case "hard link":
				target := filepath.Join(config.Root, "separate-stage-inode")
				err = os.WriteFile(target, []byte("unfinished"), 0o600)
				if err == nil {
					err = os.Link(target, stage)
				}
			case "public permissions":
				err = os.WriteFile(stage, []byte("unfinished"), 0o600)
				if err == nil {
					err = os.Chmod(stage, 0o644)
				}
			case "oversized":
				err = os.WriteFile(stage, make([]byte, metastoreWitnessMaxBytes+1), 0o600)
			case "fifo":
				err = unix.Mkfifo(stage, 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.Lstat(stage)
			if err != nil {
				t.Fatal(err)
			}
			store, err := Open(t.Context(), config)
			if store != nil {
				store.Close()
			}
			if store != nil || err == nil {
				t.Fatalf("unsafe %s stage was accepted", kind)
			}
			after, err := os.Lstat(stage)
			if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() || before.Size() != after.Size() {
				t.Fatalf("unsafe %s stage changed after rejection: before=%v after=%v error=%v", kind, before, after, err)
			}
		})
	}
}

func TestWitnessStageIOFailuresAreNotParsedAsTornRecords(t *testing.T) {
	for _, phase := range []string{"open", "read", "short read", "close"} {
		t.Run(phase, func(t *testing.T) {
			config, ack := closedWitnessFixture(t)
			stage := filepath.Join(config.Root, metastoreWitnessStage)
			residue := []byte("readable partial witness")
			if err := os.WriteFile(stage, residue, 0o600); err != nil {
				t.Fatal(err)
			}
			cause := errors.New("injected stage " + phase + " failure")
			calls, stageFD := 0, -1
			hooks := openHooks{witnessOps: func(ops *witnessOperations) {
				real := *ops
				ops.openat = func(dir int, name string, flags int, mode uint32) (int, error) {
					if name == metastoreWitnessStage && phase == "open" {
						calls++
						return -1, cause
					}
					fd, err := real.openat(dir, name, flags, mode)
					if name == metastoreWitnessStage && err == nil {
						stageFD = fd
					}
					return fd, err
				}
				ops.pread = func(fd int, p []byte, offset int64) (int, error) {
					if fd == stageFD {
						if phase == "read" {
							calls++
							return 0, cause
						}
						if phase == "short read" {
							calls++
							return 0, nil
						}
					}
					return real.pread(fd, p, offset)
				}
				ops.close = func(fd int) error {
					err := real.close(fd)
					if fd == stageFD && phase == "close" {
						calls++
						return errors.Join(err, cause)
					}
					return err
				}
			}}
			store, err := open(t.Context(), config, hooks)
			if store != nil {
				store.Close()
			}
			if store != nil || !errors.Is(err, syscall.EIO) || calls != 1 || phase != "short read" && !errors.Is(err, cause) {
				t.Fatalf("stage %s failure: store=%v calls=%d err=%v", phase, store != nil, calls, err)
			}
			requireWitnessStageBytes(t, stage, residue)
			recovered, err := Open(t.Context(), config)
			if err != nil {
				t.Fatalf("retry after stage I/O failure: %v", err)
			}
			requireWitnessAcknowledgment(t, recovered, ack)
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWitnessRecoveryCleanupFailuresRetainCauseAndAllowRetry(t *testing.T) {
	for _, phase := range []string{"unlink", "directory sync"} {
		t.Run(phase, func(t *testing.T) {
			config, ack := closedWitnessFixture(t)
			stage := filepath.Join(config.Root, metastoreWitnessStage)
			residue := []byte("unfinished witness")
			if err := os.WriteFile(stage, residue, 0o600); err != nil {
				t.Fatal(err)
			}
			cause := errors.New("injected recovery " + phase + " failure")
			calls, unlinked := 0, false
			hooks := openHooks{witnessOps: func(ops *witnessOperations) {
				real := *ops
				ops.unlinkat = func(dir int, name string, flags int) error {
					if name == metastoreWitnessStage && phase == "unlink" {
						calls++
						return cause
					}
					err := real.unlinkat(dir, name, flags)
					if name == metastoreWitnessStage && err == nil {
						unlinked = true
					}
					return err
				}
				ops.fsync = func(fd int) error {
					if unlinked && phase == "directory sync" {
						calls++
						return cause
					}
					return real.fsync(fd)
				}
			}}
			store, err := open(t.Context(), config, hooks)
			if store != nil {
				store.Close()
			}
			if store != nil || calls != 1 || !errors.Is(err, syscall.EIO) || !errors.Is(err, cause) {
				t.Fatalf("cleanup %s failure: store=%v calls=%d err=%v", phase, store != nil, calls, err)
			}
			if phase == "unlink" {
				requireWitnessStageBytes(t, stage, residue)
			} else if _, err := os.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("completed unlink did not remove stage: %v", err)
			}
			recovered, err := Open(t.Context(), config)
			if err != nil {
				t.Fatalf("retry after cleanup error: %v", err)
			}
			requireWitnessAcknowledgment(t, recovered, ack)
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, phase := range []string{"unlink", "directory sync"} {
		t.Run("live checkpoint/"+phase, func(t *testing.T) { testLiveWitnessCheckpointFailure(t, phase) })
	}
}

func TestSameIdentityStageDoesNotSetAcceptedOrCheckpointedFloors(t *testing.T) {
	for _, direction := range []string{"ahead", "behind"} {
		t.Run(direction, func(t *testing.T) {
			config, ack := closedWitnessFixture(t)
			before := ack.Record.State
			control, err := Open(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err := control.Close(); err != nil {
				t.Fatal(err)
			}
			ack.Record = readWitnessRecord(t, control)
			startupAdvance := ack.Record.State.Generation - before.Generation
			if startupAdvance <= 0 || ack.Record.State.NodeHighWater != before.NodeHighWater || ack.Record.State.ChangeHighWater != before.ChangeHighWater {
				t.Fatalf("stage-free startup changed unexpected state: before=%+v after=%+v", before, ack.Record.State)
			}
			want := ack.Record.State
			want.Generation += startupAdvance
			record := ack.Record
			if direction == "ahead" {
				record.State.Generation += 100
				record.State.NodeHighWater += 100
				record.State.ChangeHighWater += 100
				record.CheckpointedGeneration = record.State.Generation
			} else {
				record.State.Generation, record.State.NodeHighWater, record.State.ChangeHighWater, record.CheckpointedGeneration = 0, 0, 0, 0
			}
			encoded, err := encodeMetastoreWitness(record)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(config.Root, metastoreWitnessStage), encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := Open(t.Context(), config)
			if err != nil {
				t.Fatalf("stage %s changed final witness authority: %v", direction, err)
			}
			requireWitnessAcknowledgment(t, store, ack)
			actual := readWitnessRecord(t, store)
			if actual.State != want {
				t.Fatalf("stage counters became accepted: got %+v want %+v", actual.State, want)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStageOnlyInitializationRequiresACompleteMatchingRecord(t *testing.T) {
	for _, kind := range []string{"valid", "torn", "foreign volume", "foreign database"} {
		t.Run(kind, func(t *testing.T) {
			config, ack := closedWitnessFixture(t)
			objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
			if err != nil {
				t.Fatal(err)
			}
			anchor, err := openRootAnchor(config.Root)
			if err != nil {
				objects.Close()
				t.Fatal(err)
			}
			if err := anchor.publishInitializationBinding(objects.ID(), config.Volume); err != nil {
				anchor.Close()
				objects.Close()
				t.Fatal(err)
			}
			if err := errors.Join(anchor.Close(), objects.Close()); err != nil {
				t.Fatal(err)
			}
			record := ack.Record
			if kind == "foreign volume" {
				record.Volume = "foreign-volume"
			}
			if kind == "foreign database" {
				if record.State.DatabaseID[0] == 'a' {
					record.State.DatabaseID = "b" + record.State.DatabaseID[1:]
				} else {
					record.State.DatabaseID = "a" + record.State.DatabaseID[1:]
				}
			}
			encoded, err := encodeMetastoreWitness(record)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "torn" {
				encoded = encoded[:len(encoded)/2]
			}
			stage := filepath.Join(config.Root, metastoreWitnessStage)
			if err := os.WriteFile(stage, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{metastoreWitnessFilename, completionFilename} {
				if err := os.Remove(filepath.Join(config.Root, name)); err != nil {
					t.Fatal(err)
				}
			}
			store, err := Open(t.Context(), config)
			if kind == "valid" {
				if err != nil {
					t.Fatalf("matching stage-only initialization did not recover: %v", err)
				}
				requireWitnessAcknowledgment(t, store, ack)
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if store != nil {
					store.Close()
				}
				if store != nil || !errors.Is(err, syscall.EIO) {
					t.Fatalf("%s stage-only initialization returned store=%v err=%v", kind, store != nil, err)
				}
				requireWitnessStageBytes(t, stage, encoded)
				if _, err := os.Lstat(filepath.Join(config.Root, metastoreWitnessFilename)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed initialization manufactured final witness: %v", err)
				}
			}
		})
	}
}

func TestWitnessIOHandlesInterruptionsAndPartialProgress(t *testing.T) {
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "witness-bytes"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	}()
	ops := defaultWitnessOperations()
	read, write := ops.pread, ops.write
	reads, writes := 0, 0
	ops.write = func(fd int, p []byte) (int, error) {
		writes++
		if writes == 1 {
			return -1, syscall.EINTR
		}
		if len(p) > 7 {
			p = p[:7]
		}
		return write(fd, p)
	}
	ops.pread = func(fd int, p []byte, offset int64) (int, error) {
		reads++
		if reads == 1 {
			return -1, syscall.EINTR
		}
		if len(p) > 5 {
			p = p[:5]
		}
		return read(fd, p, offset)
	}
	content := []byte("witness transfer retains every byte and offset")
	if err := ops.writeFull(int(file.Fd()), content); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(content))
	if err := ops.readFull(int(file.Fd()), got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) || reads <= 2 || writes <= 2 {
		t.Fatalf("partial I/O got %q, reads=%d writes=%d", got, reads, writes)
	}
	for _, failure := range []error{nil, syscall.EBADF} {
		ops.pread = func(int, []byte, int64) (int, error) { return 0, failure }
		ops.write = func(int, []byte) (int, error) { return 0, failure }
		want := failure
		if want == nil {
			want = syscall.EIO
		}
		if err := ops.readFull(int(file.Fd()), got); !errors.Is(err, want) {
			t.Fatalf("stalled/failed read = %v, want %v", err, want)
		}
		if err := ops.writeFull(int(file.Fd()), content); !errors.Is(err, want) {
			t.Fatalf("stalled/failed write = %v, want %v", err, want)
		}
	}
}

func testLiveWitnessCheckpointFailure(t *testing.T, phase string) {
	t.Helper()
	config := witnessTestConfig(privateTestRoot(t))
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	store.durable.stop()
	<-store.durable.done
	real := store.anchor.witnessOps
	t.Cleanup(func() {
		store.anchor.witnessOps = real
		if err := store.Close(); err != nil {
			t.Errorf("closing checkpoint fixture: %v", err)
		}
	})
	if err := store.Write(t.Context(), witnessAcknowledgedName, []byte(witnessAcknowledgedContents)); err != nil {
		t.Fatal(err)
	}
	attr, err := store.Stat(t.Context(), witnessAcknowledgedName)
	if err != nil {
		t.Fatal(err)
	}
	ack := witnessAcknowledgment{Attr: attr, Record: readWitnessRecord(t, store)}
	if ack.Record.CheckpointedGeneration >= ack.Record.State.Generation {
		t.Fatal("fixture has no pending checkpoint")
	}
	cause := errors.New("injected live checkpoint " + phase + " failure")
	calls := 0
	if phase == "unlink" {
		if err := os.WriteFile(filepath.Join(config.Root, metastoreWitnessStage), []byte("unfinished checkpoint witness"), 0o600); err != nil {
			t.Fatal(err)
		}
		store.anchor.witnessOps.unlinkat = func(dir int, name string, flags int) error {
			if name == metastoreWitnessStage {
				calls++
				return cause
			}
			return real.unlinkat(dir, name, flags)
		}
	} else {
		store.anchor.witnessOps.fsync = func(fd int) error {
			if fd == store.anchor.fd {
				calls++
				return cause
			}
			return real.fsync(fd)
		}
	}
	_, err = store.durable.Store.Checkpoint(t.Context(), sqlite.PassiveCheckpoint)
	if calls != 1 || !errors.Is(err, syscall.EIO) || !errors.Is(err, cause) {
		t.Fatalf("live checkpoint %s lost failure: calls=%d err=%v", phase, calls, err)
	}
	accepted, checkpointed := store.durable.witness.generations()
	if accepted != ack.Record.State.Generation || checkpointed != ack.Record.CheckpointedGeneration {
		t.Fatalf("failed checkpoint acknowledged its result: A=%d C=%d", accepted, checkpointed)
	}
	store.anchor.witnessOps = real
	result, err := store.durable.Store.Checkpoint(t.Context(), sqlite.PassiveCheckpoint)
	if err != nil || !result.Complete {
		t.Fatalf("same-instance checkpoint retry: %+v, %v", result, err)
	}
	record := readWitnessRecord(t, store)
	if record.State != ack.Record.State || record.CheckpointedGeneration != record.State.Generation {
		t.Fatalf("successful checkpoint retry recorded %+v, want %+v with C=A", record, ack.Record.State)
	}
	requireWitnessAcknowledgment(t, store, ack)
}
