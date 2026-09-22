package localstore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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

type deleteIntentCrashReceipt struct {
	Node   uint64
	Intent storage.DeleteIntentID
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
	if receipt.Node == 0 || receipt.Intent.Check() != nil {
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
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, err := actions.QueryDeleteIntent(t.Context(), receipt.Intent)
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
	if err := actions.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Action: action, Intent: receipt.Intent}); err != nil {
		t.Fatal(err)
	}
	acknowledged, err := actions.QueryDeleteIntent(t.Context(), receipt.Intent)
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
	opened, err := session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: rootAttr.ID}, RawLeaf: []byte("victim"),
	}},

		storage.OpenAtOptions{
			Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: victim.ID},
			Action: action, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
			CloseIntent: &storage.CloseIntent{ID: intent, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
		})

	if err != nil || opened.File == nil {
		t.Fatalf("arm durable close intent: %+v %v", opened, err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(deleteIntentCrashReceipt{Node: victim.ID, Intent: intent}); err != nil {
		t.Fatal(err)
	}
	select {}
}
