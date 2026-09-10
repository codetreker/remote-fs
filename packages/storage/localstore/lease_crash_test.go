package localstore_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
)

const (
	leaseCrashHelperEnvironment = "REMOTE_FS_LOCALSTORE_LEASE_CRASH_HELPER"
	leaseCrashRootEnvironment   = "REMOTE_FS_LOCALSTORE_LEASE_CRASH_ROOT"
	leaseCrashAcknowledgment    = "lease-and-content-acknowledged\n"
	leaseCrashContents          = "content acknowledged before lease holder SIGKILL"
)

func TestLocalstoreLeaseSurvivesSIGKILL(t *testing.T) {
	if os.Getenv(leaseCrashHelperEnvironment) == "1" {
		runLeaseCrashHelper(t)
		return
	}
	config := testConfig(privateRoot(t))
	killLeaseAcknowledgedProcess(t, config.Root)
	if unsafe, err := localstore.Open(t.Context(), config); !errors.Is(err, syscall.EIO) {
		if unsafe != nil {
			closeStore(t, unsafe)
		}
		t.Fatalf("open crashed lease volume without protection = %v", err)
	}
	options := locking.DefaultOptions()
	options.MaxLease = time.Second
	config.Locks, config.InitializeLocks = &options, false
	reopened := open(t, config)
	defer closeStore(t, reopened)
	body, err := reopened.Read(t.Context(), "held")
	if err != nil || string(body) != leaseCrashContents {
		t.Fatalf("read acknowledged content after SIGKILL = %q, %v", body, err)
	}
	if err := reopened.Write(t.Context(), "held", []byte("unsafe replacement")); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("mutation during lease crash recovery = %v", err)
	}
	body, err = reopened.Read(t.Context(), "held")
	if err != nil || string(body) != leaseCrashContents {
		t.Fatalf("rejected recovery mutation changed acknowledged content = %q, %v", body, err)
	}
	status, err := reopened.LockService().(locking.StatusService).Status(t.Context())
	if err != nil || !status.Recovering || status.RecoveryRemainingMillis <= time.Second.Milliseconds() {
		t.Fatalf("crash recovery: recovering=%v, remaining_ms=%d, err=%v", status.Recovering, status.RecoveryRemainingMillis, err)
	}
	t.Logf("SIGKILL reaped; acknowledged content intact; mutation rejected; recovery_remaining_ms=%d", status.RecoveryRemainingMillis)
}

func runLeaseCrashHelper(t *testing.T) {
	root := os.Getenv(leaseCrashRootEnvironment)
	if root == "" {
		t.Fatal("lease crash helper has no root")
	}
	config := testConfig(root)
	options := locking.DefaultOptions()
	config.Locks, config.InitializeLocks = &options, true
	store := open(t, config)
	defer closeStore(t, store)
	if err := store.Create(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(t.Context(), "held", []byte(leaseCrashContents)); err != nil {
		t.Fatal(err)
	}
	acquireLocalstoreLease(t, store.LockService(), "held")
	if _, err := fmt.Fprint(os.Stdout, leaseCrashAcknowledgment); err != nil {
		t.Fatal(err)
	}
	select {}
}

func killLeaseAcknowledgedProcess(t *testing.T, root string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLocalstoreLeaseSurvivesSIGKILL$", "-test.timeout=25s")
	command.Env = append(os.Environ(), leaseCrashHelperEnvironment+"=1", leaseCrashRootEnvironment+"="+root)
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
	defer func() {
		if waited {
			return
		}
		if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("kill lease crash helper during cleanup: %v", err)
		}
		var exit *exec.ExitError
		if err := command.Wait(); err != nil && !errors.As(err, &exit) {
			t.Errorf("reap lease crash helper during cleanup: %v", err)
		}
	}()
	type acknowledgment struct {
		line string
		err  error
	}
	acknowledged := make(chan acknowledgment, 1)
	go func() {
		line, err := bufio.NewReaderSize(stdout, 128).ReadSlice('\n')
		acknowledged <- acknowledgment{line: string(line), err: err}
	}()
	select {
	case ack := <-acknowledged:
		if ack.err != nil || ack.line != leaseCrashAcknowledgment {
			t.Fatalf("lease crash helper did not acknowledge content and lease: %q, %v", ack.line, ack.err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for lease crash acknowledgment: %v", ctx.Err())
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL lease crash helper: %v", err)
	}
	err = command.Wait()
	waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("lease crash helper did not exit from SIGKILL: %v", err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("lease crash helper exit status = %v", exit.Sys())
	}
}
