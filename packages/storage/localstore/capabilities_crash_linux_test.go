package localstore

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"golang.org/x/sys/unix"
)

const capabilityCrashEnvironment = "REMOTE_FS_CAPABILITY_CRASH"
const capabilityCrashRootEnvironment = "REMOTE_FS_CAPABILITY_CRASH_ROOT"
const capabilityVictimName = "slot"
const capabilityMovedName = "moved"
const capabilityReplacementBody = "same-name replacement must survive old reference cleanup"

// The pipe receipt is emitted at a named process boundary. Scopes stay in the
// bounded pipe message; they are never written into the volume or test logs.
type capabilityCrashReceipt struct {
	Phase       string
	Victim      storage.Attr
	Replacement *storage.Attr
	VictimName  string
	Scope       storage.UseScope
	Record      metastoreWitnessRecord
	WALBytes    int64
}

type capabilityCrashFacts struct {
	Exists, Pending, Detached bool
	Intents                   int
	Name                      string
	Size                      int64
	Content                   string
}

func capabilityCrashConfig(root string, initialize bool) Config {
	config := witnessTestConfig(root)
	options := locking.DefaultOptions()
	options.MaxLease = 5 * time.Second
	config.Locks = &options
	config.InitializeLocks = initialize
	return config
}

func TestCapabilitiesPendingUnlinkSurvivesSIGKILL(t *testing.T) {
	if phase := os.Getenv(capabilityCrashEnvironment); phase != "" {
		runCapabilityCrashChild(t, phase)
		return
	}
	phases := []string{"ack/armed", "ack/renamed", "ack/replaced", "ack/now-strong"}
	for _, effect := range []string{"arm", "activate", "unlink"} {
		for _, cut := range []string{"before-commit", "after-sql", "after-witness-rename"} {
			phases = append(phases, effect+"/"+cut)
		}
	}
	for _, phase := range phases {
		t.Run(phase, func(t *testing.T) {
			config := capabilityCrashConfig(privateTestRoot(t), false)
			ack := killCapabilityChild(t, config.Root, phase)
			facts := readCapabilityCrashFacts(t, config.Root, ack.Victim.ID)
			requireCapabilityCut(t, config, ack, facts)
			recoverCapabilityCrash(t, config, ack, facts)
		})
	}
}

func killCapabilityChild(t *testing.T, root, phase string) capabilityCrashReceipt {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCapabilitiesPendingUnlinkSurvivesSIGKILL$", "-test.timeout=25s")
	command.Env = append(os.Environ(), capabilityCrashEnvironment+"="+phase, capabilityCrashRootEnvironment+"="+root)
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
	type pipeResult struct {
		data []byte
		err  error
	}
	ready := make(chan pipeResult, 1)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		line, err := bufio.NewReaderSize(stdout, 8192).ReadSlice('\n')
		ready <- pipeResult{bytes.Clone(line), err}
	}()
	waited := false
	defer func() {
		if !waited {
			if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("kill pending-unlink child: %v", err)
			}
			var exit *exec.ExitError
			if err := command.Wait(); err != nil && !errors.As(err, &exit) {
				t.Errorf("reap pending-unlink child: %v", err)
			}
		}
		<-readDone
	}()
	var ack capabilityCrashReceipt
	select {
	case result := <-ready:
		if result.err != nil {
			t.Fatalf("child %s reached no crash boundary: %v", phase, result.err)
		}
		if err := json.Unmarshal(result.data, &ack); err != nil {
			t.Fatalf("invalid bounded crash receipt: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", phase, ctx.Err())
	}
	if ack.Phase != phase || ack.Victim.ID == 0 || ack.Victim.Size != int64(len(witnessAcknowledgedContents)) || ack.WALBytes <= 32 {
		t.Fatal("child crash receipt did not identify acknowledged persistent content")
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("child did not exit from SIGKILL: %v", err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child status=%v, want SIGKILL", exit.Sys())
	}
	if info, err := os.Stat(filepath.Join(root, metastoreFilename+"-wal")); err != nil || info.Size() <= 32 {
		t.Fatalf("SIGKILL lost WAL boundary: %v", err)
	}
	t.Logf("SIGKILL reaped after %s; acknowledged victim NodeID=%d", phase, ack.Victim.ID)
	return ack
}

func runCapabilityCrashChild(t *testing.T, phase string) {
	root := os.Getenv(capabilityCrashRootEnvironment)
	if root == "" {
		t.Fatal("missing pending-unlink child root")
	}
	effect, cut, ok := strings.Cut(phase, "/")
	if !ok {
		t.Fatal("invalid pending-unlink crash phase")
	}
	store, err := Open(t.Context(), capabilityCrashConfig(root, true))
	if err != nil {
		t.Fatal(err)
	}
	store.durable.stop()
	<-store.durable.done
	snapshot, _, err := store.Log().Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err := store.Write(t.Context(), capabilityVictimName, []byte(witnessAcknowledgedContents)); err != nil {
		t.Fatal(err)
	}
	victim, err := store.Stat(t.Context(), capabilityVictimName)
	if err != nil {
		t.Fatal(err)
	}
	ack := capabilityCrashReceipt{Phase: phase, Victim: victim, VictimName: capabilityVictimName}
	var armed atomic.Bool
	signal := func() {
		if err := json.NewEncoder(os.Stdout).Encode(ack); err != nil {
			t.Fatal(err)
		}
		select {}
	}
	callContext := storage.WithPublicationAccounting(t.Context(), func(_, _ int64) (storage.PublicationSettlement, error) {
		if armed.Load() && cut == "before-commit" {
			signal()
		}
		return func(storage.PublicationResult) error { return nil }, nil
	})
	callContext = metastore.WithFilePublicationGuard(callContext, func() error {
		if armed.Load() && effect == "arm" && cut == "before-commit" {
			signal()
		}
		return nil
	})
	session, err := store.NewFileSession(callContext, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opener := session.(storage.AtomicFileOpener)
	rootAttr, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.OpenAtOptions{Read: true, Write: true, Existing: storage.Keep,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: victim.ID}, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName}}
	wantsArmed := effect == "arm" || effect == "activate" || effect == "ack" && cut != "now-strong"
	if wantsArmed {
		options.CloseIntent = &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile}
	}
	var file storage.File
	if effect != "arm" {
		opened, err := opener.OpenAt(callContext, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: rootAttr.ID}, RawLeaf: []byte(capabilityVictimName)}, options)
		if err != nil {
			t.Fatal(err)
		}
		file = opened.File
		ack.Scope, err = file.(storage.ScopedReference).Scope(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		state, err := file.(storage.ReferenceStateAccess).State(t.Context())
		if err != nil || state.PendingUnlink {
			t.Fatalf("ordinary/armed initial state=%+v,%v", state, err)
		}
	}
	if effect == "ack" && cut == "renamed" {
		if err := store.Rename(t.Context(), capabilityVictimName, capabilityMovedName); err != nil {
			t.Fatal(err)
		}
		ack.VictimName = capabilityMovedName
		if err := store.Write(t.Context(), capabilityVictimName, []byte(capabilityReplacementBody)); err != nil {
			t.Fatal(err)
		}
		replacement, err := store.Stat(t.Context(), capabilityVictimName)
		if err != nil || replacement.ID == victim.ID {
			t.Fatal("rename and name reuse did not create an independent replacement")
		}
		ack.Replacement = &replacement
	}
	if effect == "ack" && cut == "replaced" {
		if err := store.Write(t.Context(), "replacement-source", []byte(capabilityReplacementBody)); err != nil {
			t.Fatal(err)
		}
		replacement, err := store.Stat(t.Context(), "replacement-source")
		if err != nil {
			t.Fatal(err)
		}
		ack.Replacement = &replacement
		owner, grant := capabilityStrongGrant(t, store, capabilityVictimName)
		scoped := locking.WithScope(t.Context(), locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}})
		if err := store.Rename(scoped, "replacement-source", capabilityVictimName); err != nil {
			t.Fatal(err)
		}
		status, err := store.LockService().QueryGrant(t.Context(), owner, grant)
		if err != nil || status.State != locking.TargetGone {
			t.Fatalf("replaced Strong identity=%+v,%v", status, err)
		}
		state, err := file.(storage.ReferenceStateAccess).State(t.Context())
		if err != nil || !state.Detached || state.Attr.ID != victim.ID {
			t.Fatalf("replaced retained identity=%+v,%v", state, err)
		}
	}
	if effect == "unlink" || effect == "ack" && cut == "now-strong" {
		var reader storage.File
		if effect == "ack" {
			opened, err := opener.OpenAt(callContext, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: rootAttr.ID}, RawLeaf: []byte(capabilityVictimName)}, options)
			if err != nil {
				t.Fatal(err)
			}
			reader = opened.File
		}
		state, err := file.(storage.DeleteIntent).SetPendingUnlink(callContext, storage.PendingUnlinkCommand{Condition: storage.UnlinkFile})
		if err != nil || !state.PendingUnlink || state.Attr.ID != victim.ID {
			t.Fatalf("Now acknowledgement=%+v,%v", state, err)
		}
		if fresh, err := session.OpenFile(t.Context(), capabilityVictimName, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); !errors.Is(err, storage.ErrPendingDelete) {
			if fresh != nil {
				fresh.Close(t.Context())
			}
			t.Fatalf("pending fresh open=%v", err)
		}
		if reader != nil {
			read, err := reader.ReadAt(t.Context(), 0, len(witnessAcknowledgedContents))
			if err != nil || string(read.Data) != witnessAcknowledgedContents || read.Attr.ID != victim.ID {
				t.Fatalf("pending retained read=%+v,%v", read, err)
			}
			capabilityStrongGrant(t, store, capabilityVictimName)
		}
	}
	if file != nil {
		read, err := file.ReadAt(t.Context(), 0, len(witnessAcknowledgedContents))
		if err != nil || string(read.Data) != witnessAcknowledgedContents || read.Attr.ID != victim.ID {
			t.Fatalf("retained bytes before kill=%+v,%v", read, err)
		}
	}
	ack.Record = readWitnessRecord(t, store)
	info, err := os.Stat(filepath.Join(root, metastoreFilename+"-wal"))
	if err != nil {
		t.Fatal(err)
	}
	ack.WALBytes = info.Size()
	if effect == "ack" {
		signal()
	}
	ops := store.anchor.witnessOps
	store.anchor.witnessOps.openat = func(dir int, name string, flags int, mode uint32) (int, error) {
		fd, err := ops.openat(dir, name, flags, mode)
		if err == nil && armed.Load() && cut == "after-sql" && name == metastoreWitnessStage && flags&unix.O_CREAT != 0 {
			signal()
		}
		return fd, err
	}
	store.anchor.witnessOps.renameat = func(oldDir int, oldName string, newDir int, newName string) error {
		err := ops.renameat(oldDir, oldName, newDir, newName)
		if err == nil && armed.Load() && cut == "after-witness-rename" && oldName == metastoreWitnessStage && newName == metastoreWitnessFilename {
			signal()
		}
		return err
	}
	armed.Store(true)
	if effect == "arm" {
		_, err = opener.OpenAt(callContext, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: rootAttr.ID}, RawLeaf: []byte(capabilityVictimName)}, options)
	} else {
		err = file.Close(callContext)
	}
	t.Fatalf("%s returned without its crash boundary: %v", phase, err)
}

func capabilityStrongGrant(t *testing.T, store *Store, path string) (locking.OwnerRef, locking.GrantRef) {
	t.Helper()
	service := store.LockService()
	ticket, err := service.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := service.CreateOwner(t.Context(), session.ID, "crash-owner")
	if err != nil {
		t.Fatal(err)
	}
	resource, err := service.Resolve(t.Context(), owner.Ref, path)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := service.Acquire(t.Context(), locking.AcquireRequest{Owner: owner.Ref, Request: "crash-grant", Resource: resource, Mode: locking.Exclusive, TTL: 5 * time.Second})
	if err != nil || attempt.Grant == nil || attempt.Receipt.Outcome != locking.Granted {
		t.Fatalf("Strong grant=%+v,%v", attempt, err)
	}
	return owner.Ref, attempt.Grant.Ref
}

func readCapabilityCrashFacts(t *testing.T, root string, nodeID uint64) capabilityCrashFacts {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(root, metastoreFilename)+"?mode=ro&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var facts capabilityCrashFacts
	err = db.QueryRowContext(t.Context(), `SELECT size,content,detached,pending_unlink,
 (SELECT count(*) FROM close_intents WHERE node=n.id),
 COALESCE((SELECT name FROM entries WHERE node=n.id),'')
 FROM nodes n WHERE id=?`, nodeID).Scan(&facts.Size, &facts.Content, &facts.Detached, &facts.Pending, &facts.Intents, &facts.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return facts
	}
	if err != nil {
		t.Fatal(err)
	}
	facts.Exists = true
	return facts
}

func requireCapabilityCut(t *testing.T, config Config, ack capabilityCrashReceipt, facts capabilityCrashFacts) {
	t.Helper()
	effect, cut, _ := strings.Cut(ack.Phase, "/")
	exists, pending, intents := true, false, 0
	switch effect {
	case "ack":
		if cut == "now-strong" {
			pending = true
		} else {
			intents = 1
		}
	case "arm":
		if cut != "before-commit" {
			intents = 1
		}
	case "activate":
		if cut == "before-commit" {
			intents = 1
		} else {
			pending = true
		}
	case "unlink":
		if cut == "before-commit" {
			pending = true
		} else {
			exists = false
		}
	default:
		t.Fatal("unsupported crash effect")
	}
	if facts.Exists != exists || facts.Pending != pending || facts.Intents != intents {
		t.Fatalf("persistent %s boundary=%+v; want exists=%v pending=%v intents=%d", ack.Phase, facts, exists, pending, intents)
	}
	if exists {
		if facts.Size != ack.Victim.Size || facts.Content == "" {
			t.Fatal("persistent intent changed acknowledged content identity or size")
		}
		detached := effect == "ack" && cut == "replaced"
		if facts.Detached != detached || !detached && facts.Name != ack.VictimName {
			t.Fatalf("persistent node location=%+v", facts)
		}
	}
	encoded, err := os.ReadFile(filepath.Join(config.Root, metastoreWitnessFilename))
	if err != nil {
		t.Fatal(err)
	}
	witness, err := decodeMetastoreWitness(encoded, ack.Record.StoreID, config.Volume)
	if err != nil {
		t.Fatal(err)
	}
	if cut == "after-witness-rename" {
		if witness.State.Generation <= ack.Record.State.Generation || witness.CheckpointedGeneration != ack.Record.CheckpointedGeneration {
			t.Fatal("post-witness cut did not publish the new SQL generation")
		}
	} else if witness != ack.Record {
		t.Fatal("pre-witness boundary replaced the last acknowledged witness")
	}
}

func recoverCapabilityCrash(t *testing.T, config Config, ack capabilityCrashReceipt, before capabilityCrashFacts) {
	t.Helper()
	survives := ack.Phase == "arm/before-commit"
	replacementBytes := int64(0)
	if ack.Replacement != nil {
		replacementBytes = ack.Replacement.Size
	}
	beforeCleanupBytes := replacementBytes
	if before.Exists && !before.Detached {
		beforeCleanupBytes += ack.Victim.Size
	}
	observedStartup := false
	store, err := open(t.Context(), config, openHooks{afterDurableMetastore: func(d *durableMetastore) error {
		observedStartup = true
		if used, err := d.Store.Usage(t.Context()); err != nil || used != beforeCleanupBytes {
			return errors.Join(fmt.Errorf("startup charge=%d, want %d", used, beforeCleanupBytes), err)
		}
		if before.Exists && !before.Detached {
			node, err := d.Store.Stat(t.Context(), ack.VictimName)
			if err != nil || uint64(node.ID) != ack.Victim.ID || node.Content != metastore.Key(before.Content) {
				return fmt.Errorf("startup changed the retained node/content: %v", err)
			}
			if !survives {
				reference, err := d.Store.OpenFile(t.Context(), ack.VictimName, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
				if reference != nil {
					reference.Close(context.Background())
				}
				if !errors.Is(err, storage.ErrPendingDelete) {
					return fmt.Errorf("startup accepted a fresh reference past durable deletion: %v", err)
				}
			}
		}
		maximum, err := d.Store.MaxLease(t.Context())
		if err != nil {
			return err
		}
		if ack.Phase == "ack/now-strong" || ack.Phase == "ack/replaced" {
			if maximum != 5*time.Second {
				return fmt.Errorf("Strong recovery watermark=%v", maximum)
			}
		} else if maximum != 0 {
			return fmt.Errorf("ordinary file session changed Strong recovery watermark to %v", maximum)
		}
		if ack.Phase == "ack/now-strong" {
			status, err := d.Store.LockService().(locking.StatusService).Status(t.Context())
			if err != nil || !status.Recovering {
				return fmt.Errorf("reopened Strong recovery was not active: %v", err)
			}
			if err := d.Store.RetryPendingUnlinks(t.Context(), 8); !errors.Is(err, syscall.EAGAIN) {
				return fmt.Errorf("pending cleanup bypassed Strong recovery: %v", err)
			}
			if used, err := d.Store.Usage(t.Context()); err != nil || used != beforeCleanupBytes {
				return fmt.Errorf("blocked recovery released charge=%d: %v", used, err)
			}
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	if !observedStartup {
		t.Fatal("recovery skipped the native startup observation")
	}
	session, err := store.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if ack.Scope.Token != "" {
		owner, err := session.(storage.UseOwners).NewUseOwner(t.Context(), ack.Victim.ID, ack.Scope, storage.OwnerOptions{Lifetime: storage.OwnerReference})
		if !errors.Is(err, storage.ErrInvalidScope) || owner != 0 {
			t.Fatalf("recovered session reused a dead reference scope: %v", err)
		}
	}
	if ack.Phase == "ack/now-strong" {
		node, err := store.meta.StatNode(t.Context(), ack.Victim.ID)
		if err != nil {
			t.Fatal(err)
		}
		data, err := store.objects.Get(t.Context(), string(node.Content))
		if err != nil || string(data) != witnessAcknowledgedContents {
			t.Fatalf("recovered protected object bytes=%q,%v", data, err)
		}
		if fresh, err := session.OpenFile(t.Context(), ack.VictimName, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); !errors.Is(err, storage.ErrPendingDelete) {
			if fresh != nil {
				fresh.Close(t.Context())
			}
			t.Fatalf("Strong recovery allowed pending reference: %v", err)
		}
	}
	if !survives {
		deadline := time.Now().Add(10 * time.Second)
		for {
			err := store.meta.RetryPendingUnlinks(t.Context(), 8)
			if err != nil && !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("pending-unlink maintenance=%v", err)
			}
			_, stateErr := store.meta.StatNode(t.Context(), ack.Victim.ID)
			if errors.Is(stateErr, syscall.ESTALE) {
				break
			}
			if stateErr != nil {
				t.Fatal(stateErr)
			}
			if !time.Now().Before(deadline) {
				t.Fatal("recovered intent did not complete after Strong recovery")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	expectedBytes := replacementBytes
	if survives {
		expectedBytes += ack.Victim.Size
	}
	requireCapabilityFinalState(t, store, ack, survives, expectedBytes)
	for range 2 {
		if err := store.meta.RetryPendingUnlinks(t.Context(), 8); err != nil {
			t.Fatal(err)
		}
	}
	requireCapabilityFinalState(t, store, ack, survives, expectedBytes)
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	requireCapabilityFinalState(t, reopened, ack, survives, expectedBytes)
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func requireCapabilityFinalState(t *testing.T, store *Store, ack capabilityCrashReceipt, survives bool, expectedBytes int64) {
	t.Helper()
	if used, err := store.Usage(t.Context()); err != nil || used != expectedBytes {
		t.Fatalf("final quota=%d, want %d: %v", used, expectedBytes, err)
	}
	if survives {
		attr, err := store.Stat(t.Context(), ack.VictimName)
		if err != nil || attr.ID != ack.Victim.ID {
			t.Fatalf("uncommitted arm changed identity: %v", err)
		}
		data, err := store.Read(t.Context(), ack.VictimName)
		if err != nil || string(data) != witnessAcknowledgedContents {
			t.Fatalf("uncommitted arm changed bytes=%q,%v", data, err)
		}
	} else {
		if _, err := store.meta.StatNode(t.Context(), ack.Victim.ID); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("old NodeID remains after cleanup: %v", err)
		}
		if ack.Replacement == nil || ack.VictimName != capabilityVictimName {
			if _, err := store.Stat(t.Context(), ack.VictimName); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("deleted name remains: %v", err)
			}
		}
	}
	if ack.Replacement != nil {
		attr, err := store.Stat(t.Context(), capabilityVictimName)
		if err != nil || attr.ID != ack.Replacement.ID || attr.ID == ack.Victim.ID {
			t.Fatalf("cleanup retargeted the same-name replacement: %v", err)
		}
		data, err := store.Read(t.Context(), capabilityVictimName)
		if err != nil || string(data) != capabilityReplacementBody {
			t.Fatalf("replacement bytes changed=%q,%v", data, err)
		}
	}
}
