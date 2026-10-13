package localstore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

const deleteIntentCrashEnvironment = "REMOTE_FS_DELETE_INTENT_CRASH"
const deleteIntentCrashRootEnvironment = "REMOTE_FS_DELETE_INTENT_CRASH_ROOT"
const unlockedDeleteIntentCrashEnvironment = "REMOTE_FS_UNLOCKED_DELETE_INTENT_CRASH"

type deleteIntentCrashReceipt struct {
	Node   uint64
	Intent storage.DeleteIntentID
	Owner  storage.DeleteIntentOwner
}

func deleteIntentCrashConfig(root string, initialize bool) Config {
	config := witnessTestConfig(root)
	locks := locking.DefaultOptions()
	locks.MaxLease = 5 * time.Second
	config.Locks = &locks
	config.InitializeLocks = initialize
	return config
}

func TestDeleteIntentSurvivesSIGKILLAndRecovers(t *testing.T) {
	if os.Getenv(deleteIntentCrashEnvironment) != "" {
		runDeleteIntentCrashChild(t)
		return
	}
	root := privateTestRoot(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDeleteIntentSurvivesSIGKILLAndRecovers$", "-test.timeout=25s")
	command.Env = append(os.Environ(), deleteIntentCrashEnvironment+"=1", deleteIntentCrashRootEnvironment+"="+root)
	command.Stderr = os.Stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	var receipt deleteIntentCrashReceipt
	if err := json.NewDecoder(bufio.NewReader(stdout)).Decode(&receipt); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("child reached no durable-intent boundary: %v", err)
	}
	if receipt.Node == 0 || receipt.Intent.Check() != nil || receipt.Owner.Check() != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("invalid child receipt: %+v", receipt)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	var exit *exec.ExitError
	if err := command.Wait(); !errors.As(err, &exit) || !exit.Sys().(syscall.WaitStatus).Signaled() {
		t.Fatalf("child did not stop at SIGKILL boundary: %v", err)
	}

	store, err := Open(t.Context(), deleteIntentCrashConfig(root, false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	session, err := store.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	actions := session.(storage.FileActions)
	page, err := actions.ListDeleteIntents(t.Context(), receipt.Owner, 0, 1)
	if err != nil || len(page.Intents) != 1 || page.Intents[0].ID != receipt.Intent || page.Next == 0 {
		t.Fatalf("recovered owner discovery = %+v, %v", page, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, err := actions.QueryDeleteIntent(t.Context(), receipt.Owner, receipt.Intent)
		if err != nil {
			t.Fatal(err)
		}
		if status.Outcome == storage.DeleteIntentCompleted {
			if status.NodeID != receipt.Node {
				t.Fatalf("completed intent changed node: %+v", status)
			}
			break
		}
		if status.Outcome != storage.DeleteIntentArmed && status.Outcome != storage.DeleteIntentPending {
			t.Fatalf("recovered intent entered unexpected state: %+v", status)
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("recovered intent did not complete: %+v", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := store.Stat(t.Context(), "victim"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("recovered intent left its name: %v", err)
	}
	if used, err := store.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("recovered deletion left quota charged: used=%d error=%v", used, err)
	}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if err := actions.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Action: action, Owner: receipt.Owner, Intent: receipt.Intent}); err != nil {
		t.Fatal(err)
	}
	acknowledged, err := actions.QueryDeleteIntent(t.Context(), receipt.Owner, receipt.Intent)
	if err != nil || acknowledged.Outcome != storage.DeleteIntentUnknown || acknowledged.NodeID != 0 {
		t.Fatalf("acknowledged intent=%+v error=%v", acknowledged, err)
	}
}

func runDeleteIntentCrashChild(t *testing.T) {
	root := os.Getenv(deleteIntentCrashRootEnvironment)
	if root == "" {
		t.Fatal("missing crash root")
	}
	store, err := Open(t.Context(), deleteIntentCrashConfig(root, true))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(t.Context(), "victim", []byte("durable bytes")); err != nil {
		t.Fatal(err)
	}
	rootAttr, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	victim, err := store.Stat(t.Context(), "victim")
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := storage.NewDeleteIntentID()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := storage.NewDeleteIntentOwner()
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: rootAttr.ID}, RawLeaf: []byte("victim"),
	}}, storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: victim.ID},
		Action: action, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
		CloseIntent: &storage.CloseIntent{ID: intent, Owner: owner, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	})
	if err != nil || opened.File == nil {
		t.Fatalf("arm durable close intent: %+v %v", opened, err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(deleteIntentCrashReceipt{Node: victim.ID, Intent: intent, Owner: owner}); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestUnlockedDeleteIntentsClassifyBeforeBoundedMaintenance(t *testing.T) {
	if os.Getenv(unlockedDeleteIntentCrashEnvironment) != "" {
		runUnlockedDeleteIntentCrashChild(t)
		return
	}
	root := privateTestRoot(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUnlockedDeleteIntentsClassifyBeforeBoundedMaintenance$", "-test.timeout=25s")
	command.Env = append(os.Environ(), unlockedDeleteIntentCrashEnvironment+"=1", deleteIntentCrashRootEnvironment+"="+root)
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
	defer func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	type accepted struct {
		receipts []deleteIntentCrashReceipt
		err      error
	}
	boundary := make(chan accepted, 1)
	go func() {
		var receipts []deleteIntentCrashReceipt
		err := json.NewDecoder(bufio.NewReader(stdout)).Decode(&receipts)
		boundary <- accepted{receipts: receipts, err: err}
	}()
	var receipts []deleteIntentCrashReceipt
	select {
	case result := <-boundary:
		if result.err != nil || len(result.receipts) != 3 {
			t.Fatalf("accepted intents = %+v, %v", result.receipts, result.err)
		}
		receipts = result.receipts
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, receipt := range receipts {
		if receipt.Node == 0 || receipt.Intent.Check() != nil || receipt.Owner.Check() != nil {
			t.Fatalf("invalid acceptance receipt: %+v", receipt)
		}
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	var exit *exec.ExitError
	if err := command.Wait(); !errors.As(err, &exit) || !exit.Sys().(syscall.WaitStatus).Signaled() {
		t.Fatalf("child did not stop at SIGKILL boundary: %v", err)
	}
	config := witnessTestConfig(root)
	config.Maintenance.SweepBatch = 1
	injected := errors.New("unlocked intent classification witness failure")
	publications := 0
	failed, err := open(ctx, config, openHooks{witnessOps: func(ops *witnessOperations) {
		real := ops.renameat
		ops.renameat = func(oldDir int, oldName string, newDir int, newName string) error {
			if oldName == metastoreWitnessStage && newName == metastoreWitnessFilename {
				publications++
				if publications == 2 {
					return injected
				}
			}
			return real(oldDir, oldName, newDir, newName)
		}
	}})
	if failed != nil {
		_ = failed.Close()
	}
	if failed != nil || !errors.Is(err, injected) || publications != 2 {
		t.Fatalf("classification failure returned store=%v, error=%v, publications=%d", failed != nil, err, publications)
	}
	checkedBeforeMaintenance := false
	store, err := open(ctx, config, openHooks{afterDurableMetastore: func(meta *durableMetastore) error {
		for _, receipt := range receipts {
			status, err := meta.QueryDeleteIntent(ctx, receipt.Owner, receipt.Intent)
			if err != nil || status.Outcome != storage.DeleteIntentPending {
				return errors.Join(fmt.Errorf("pre-maintenance classification: %+v", status), err)
			}
			file, err := meta.OpenNode(ctx, receipt.Node, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			if file != nil {
				_ = file.Close(ctx)
			}
			if file != nil || !errors.Is(err, storage.ErrPendingDelete) {
				return fmt.Errorf("pre-maintenance admission: reference=%v error=%v", file != nil, err)
			}
		}
		checkedBeforeMaintenance = true
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if !checkedBeforeMaintenance {
		t.Fatal("constructor did not reach the pre-maintenance observation")
	}
	for store.MaintenanceStatus().LastSweepTime.IsZero() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if status := store.MaintenanceStatus(); status.LastSweepError != nil {
		t.Fatal(status.LastSweepError)
	}
	session, err := store.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	actions := session.(storage.FileActions)
	third, err := actions.QueryDeleteIntent(ctx, receipts[2].Owner, receipts[2].Intent)
	if err != nil || third.Outcome != storage.DeleteIntentPending {
		t.Fatalf("third intent after one bounded pass = %+v, %v", third, err)
	}
	if file, err := session.OpenFile(ctx, "victim2", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); file != nil || !errors.Is(err, storage.ErrPendingDelete) {
		if file != nil {
			_ = file.Close(ctx)
		}
		t.Fatalf("fresh unlocked OPEN = %v, %v", file, err)
	}
	for range receipts {
		if err := store.meta.RetryPendingUnlinks(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	for i, receipt := range receipts {
		status, err := actions.QueryDeleteIntent(ctx, receipt.Owner, receipt.Intent)
		if err != nil || status.Outcome != storage.DeleteIntentCompleted || status.NodeID != receipt.Node {
			t.Fatalf("completed intent = %+v, %v", status, err)
		}
		if _, err := store.Stat(ctx, fmt.Sprintf("victim%d", i)); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("completed name = %v", err)
		}
	}
}

func runUnlockedDeleteIntentCrashChild(t *testing.T) {
	root := os.Getenv(deleteIntentCrashRootEnvironment)
	if root == "" {
		t.Fatal("missing crash root")
	}
	config := witnessTestConfig(root)
	config.Maintenance.SweepBatch = 1
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	owner, err := storage.NewDeleteIntentOwner()
	if err != nil {
		t.Fatal(err)
	}
	var receipts []deleteIntentCrashReceipt
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("victim%d", i)
		// Empty objects keep the one-batch startup pass independent of garbage
		// reclamation, which can otherwise schedule another maintenance pass.
		if err := store.Write(t.Context(), name, nil); err != nil {
			t.Fatal(err)
		}
		node, err := store.Stat(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		intent, err := storage.NewDeleteIntentID()
		if err != nil {
			t.Fatal(err)
		}
		action, err := storage.NewFileActionID(status.ActionEpoch)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{
			Parent: storage.DirectoryTarget{NodeID: parent.ID}, RawLeaf: []byte(name),
		}}, storage.OpenAtOptions{
			Read: true, Action: action, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node.ID},
			Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
			CloseIntent: &storage.CloseIntent{ID: intent, Owner: owner, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
		})
		if err != nil || opened.File == nil {
			t.Fatalf("accept unlocked close intent: %+v, %v", opened, err)
		}
		receipts = append(receipts, deleteIntentCrashReceipt{Node: node.ID, Owner: owner, Intent: intent})
	}
	for _, receipt := range receipts {
		status, err := session.(storage.FileActions).QueryDeleteIntent(t.Context(), receipt.Owner, receipt.Intent)
		if err != nil || status.Outcome != storage.DeleteIntentArmed {
			t.Fatalf("live initiating reference = %+v, %v", status, err)
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(receipts); err != nil {
		t.Fatal(err)
	}
	select {}
}
