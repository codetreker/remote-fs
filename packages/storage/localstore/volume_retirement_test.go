package localstore_test

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestDeletionReadinessObservesDurableWorkUnderReservedAuthority(t *testing.T) {
	config := transferConfig(privateRoot(t))
	locks := locking.DefaultOptions()
	config.Locks, config.InitializeLocks = &locks, true
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	registerTransferNamespace(t, store)
	authority, ok := store.LockService().(interface {
		ReserveRetirement(context.Context) error
		CancelRetirement()
	})
	if !ok {
		t.Fatal("localstore does not expose its native retirement authority")
	}
	before := transferBindingContext(t, transferTestSubject, "before-retirement")
	after := transferBindingContext(t, transferTestSubject, "retirement-cleanup")
	payload := []byte("accepted durable work")
	command := transferCommand(t, store, "artifact", "reserved-retirement", payload, 0)
	file, err := store.BeginImport(before, command)
	if err != nil {
		t.Fatal(err)
	}
	detached, settled := false, false
	t.Cleanup(func() {
		authority.CancelRetirement()
		if !detached {
			result, err := file.Cancel(context.WithoutCancel(before))
			if err != nil || !result.Released || !result.Determined {
				t.Errorf("cancel live import during cleanup = %+v, %v", result, err)
			}
		} else if !settled {
			receipt, err := store.CancelImport(context.WithoutCancel(after), command.Action)
			if err != nil || !receipt.Outcome.Terminal() {
				t.Errorf("settle durable import during cleanup = %+v, %v", receipt, err)
			}
		}
	})
	if _, err := file.Receive(before, bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if result, err := file.Detach(before); err != nil || !result.Released || !result.Determined {
		t.Fatalf("detach sealed import = %+v, %v", result, err)
	}
	detached = true
	if store.LiveTransferCount() != 0 {
		t.Fatal("sealed import retained a volatile handle")
	}
	if err := authority.ReserveRetirement(t.Context()); err != nil {
		t.Fatalf("quiescent native authority did not reserve retirement: %v", err)
	}
	if err := store.CheckVolumeRetirement(); err != nil {
		t.Fatalf("retirement reservation hid the capability: %v", err)
	}
	if err := store.CheckDeletionReady(before); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("reserved control read ignored durable import: %v", err)
	}
	if _, err := store.Stat(before, ""); locking.CodeOf(err) != locking.Retired {
		t.Fatalf("reserved authority admitted ordinary reads: %v", err)
	}
	if err := store.Write(before, "ordinary", []byte("blocked")); locking.CodeOf(err) != locking.Retired {
		t.Fatalf("reserved authority admitted ordinary mutation: %v", err)
	}
	authority.CancelRetirement()
	receipt, err := store.CancelImport(after, command.Action)
	if err != nil || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != syscall.ECANCELED {
		t.Fatalf("settle accepted durable import = %+v, %v", receipt, err)
	}
	settled = true
	if err := authority.ReserveRetirement(t.Context()); err != nil {
		t.Fatalf("settled authority did not reserve retirement: %v", err)
	}
	if err := store.CheckDeletionReady(after); err != nil {
		t.Fatalf("settled durable receipt blocked deletion readiness: %v", err)
	}
	authority.CancelRetirement()
}
