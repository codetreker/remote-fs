package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	modernsqlite "modernc.org/sqlite"
)

func serviceRetirementStatus(t *testing.T, store *Store, want error) {
	t.Helper()
	before, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CheckDeletionReady(t.Context()); !errors.Is(err, want) {
		t.Fatalf("deletion readiness = %v; want %v", err, want)
	}
	if after, err := store.DurableState(t.Context()); err != nil || after != before {
		t.Fatalf("readiness observation changed authority: %+v, %v; want %+v", after, err, before)
	}
}

func TestServiceRetirementPendingImportsBlockUntilPublishOrCancel(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	if err := store.CheckVolumeRetirement(); err != nil {
		t.Fatalf("native authority did not advertise retirement checks: %v", err)
	}
	servicePolicyNamespace(t, store.Store, 1)
	serviceRetirementStatus(t, store.Store, nil)
	command := nativeTransferCommand(t, store.Store, "publish", "published", "content", 0)
	prepared, err := store.PrepareImport(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	serviceRetirementStatus(t, store.Store, syscall.EBUSY)
	if err := store.SealImport(t.Context(), command.Action, metastore.Object{Key: prepared.Key, Size: command.DeclaredBytes, Digest: command.Digest[:]}); err != nil {
		t.Fatal(err)
	}
	serviceRetirementStatus(t, store.Store, syscall.EBUSY)
	if receipt, err := store.PublishImport(t.Context(), command.Action); err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("import publication = %+v, %v", receipt, err)
	}
	serviceRetirementStatus(t, store.Store, nil)
	for _, sealed := range []bool{false, true} {
		nonce := "cancel-prepared"
		if sealed {
			nonce = "cancel-sealed"
		}
		command := nativeTransferCommand(t, store.Store, nonce, nonce, "content", 0)
		if sealed {
			nativeTransferSeal(t, t.Context(), store.Store, command)
		} else if _, err := store.PrepareImport(t.Context(), command); err != nil {
			t.Fatal(err)
		}
		serviceRetirementStatus(t, store.Store, syscall.EBUSY)
		if err := store.AbandonImport(t.Context(), command.Action); err != nil {
			t.Fatal(err)
		}
		if receipt, err := store.QueryImportAction(t.Context(), command.Action); !errors.Is(err, syscall.ECANCELED) || receipt.Outcome != storage.DurableActionNotExecuted {
			t.Fatalf("abandoned import = %+v, %v", receipt, err)
		}
		serviceRetirementStatus(t, store.Store, nil)
	}
}

func TestServiceRetirementAcceptedQuotaBlocksUntilTerminal(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	now := time.Now()
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(-time.Hour)}
	for _, nonce := range []string{"complete", "refuse"} {
		proof := serviceAcceptedQuota(namespace, nonce, now.Add(-2*time.Hour), 1, 4096)
		serviceAcceptedQuotaPending(t, store.Store, proof)
		serviceRetirementStatus(t, store.Store, syscall.EBUSY)
		receipt, err := store.SetQuota(t.Context(), proof.Command)
		if nonce == "complete" {
			if err != nil || receipt.Outcome != storage.DurableActionCompleted {
				t.Fatalf("accepted quota completion = %+v, %v", receipt, err)
			}
		} else if !errors.Is(err, syscall.ESTALE) || receipt.Outcome != storage.DurableActionNotExecuted {
			t.Fatalf("accepted quota refusal = %+v, %v", receipt, err)
		}
		serviceRetirementStatus(t, store.Store, nil)
	}
}

func TestServiceRetirementClosePreservesPendingWorkAcrossReopen(t *testing.T) {
	config := servicePolicyConfig(t, 8192)
	store := servicePolicyOpen(t, config)
	namespace := servicePolicyNamespace(t, store.Store, 1)
	command := nativeTransferCommand(t, store.Store, "restart-import", "imported", "content", 0)
	prepared, err := store.PrepareImport(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	proof := serviceAcceptedQuota(namespace, "restart-quota", time.Now(), 1, 16384)
	pendingQuota := serviceAcceptedQuotaPending(t, store.Store, proof)
	serviceRetirementStatus(t, store.Store, syscall.EBUSY)
	if err := store.Close(); err != nil {
		t.Fatalf("ordinary close could not hand pending responsibility to restart: %v", err)
	}
	config.Initialize = false
	config.SQLite.InitializeQuota = false
	store = servicePolicyOpen(t, config)
	serviceRetirementStatus(t, store.Store, syscall.EBUSY)
	resumed, err := store.ResumeImportReservation(t.Context(), command.Action)
	if err != nil || resumed.Key != prepared.Key || resumed.Receipt.Outcome != storage.DurableActionPending {
		t.Fatalf("restart lost pending import: %+v, %v; want key %q", resumed, err, prepared.Key)
	}
	if receipt, err := store.QueryQuotaAction(t.Context(), proof.Command.Action); err != nil || receipt != pendingQuota {
		t.Fatalf("restart lost accepted quota: %+v, %v; want %+v", receipt, err, pendingQuota)
	}
	if err := store.SealImport(t.Context(), command.Action, metastore.Object{Key: resumed.Key, Size: command.DeclaredBytes, Digest: command.Digest[:]}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishImport(t.Context(), command.Action); err != nil {
		t.Fatal(err)
	}
	serviceRetirementStatus(t, store.Store, syscall.EBUSY)
	if _, err := store.SetQuota(t.Context(), proof.Command); err != nil {
		t.Fatal(err)
	}
	serviceRetirementStatus(t, store.Store, nil)
}

func TestServiceRetirementCancellationNeverProvesReadiness(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, pending := range []bool{false, true} {
		if pending {
			now := time.Now()
			namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(time.Hour)}
			serviceAcceptedQuotaPending(t, store.Store, serviceAcceptedQuota(namespace, "pending", now, 1, 4096))
		}
		before, err := store.DurableState(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CheckDeletionReady(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled retirement with pending=%t = %v; want canceled", pending, err)
		}
		if after, err := store.DurableState(t.Context()); err != nil || after != before {
			t.Fatalf("canceled retirement changed authority: %+v, %v; want %+v", after, err, before)
		}
	}
}

func TestServiceRetirementPreservesUnhealthyAuthorityError(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	serviceRetirementStatus(t, store.Store, nil)
	failure := errors.New("retirement authority durability unavailable")
	store.coordinator.poisonWith(failure)
	t.Cleanup(func() {
		store.coordinator.health.Lock()
		store.coordinator.poison = nil
		store.coordinator.health.Unlock()
	})
	if err := store.CheckDeletionReady(t.Context()); !errors.Is(err, failure) || storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("unhealthy retirement lost its cause or proved readiness: %v", err)
	}
}

func TestServiceRetirementMissingAuthorityTableNeverProvesReadiness(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	if _, err := store.write.ExecContext(t.Context(), `ALTER TABLE imports RENAME TO imports_unavailable`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := store.write.ExecContext(context.Background(), `ALTER TABLE imports_unavailable RENAME TO imports`); err != nil {
			t.Error(err)
		}
	})
	err := store.CheckDeletionReady(t.Context())
	var cause *modernsqlite.Error
	if !errors.As(err, &cause) || storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("missing retirement authority table lost SQLite failure or proved readiness: %v", err)
	}
}

func serviceReservedRetirementStatus(t *testing.T, store *Store, want error) {
	t.Helper()
	var before, after int64
	if err := store.write.QueryRowContext(t.Context(), `SELECT generation FROM database_state WHERE singleton=1`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckDeletionReady(t.Context()); !errors.Is(err, want) {
		t.Fatalf("reserved deletion readiness = %v; want %v", err, want)
	}
	if err := store.write.QueryRowContext(t.Context(), `SELECT generation FROM database_state WHERE singleton=1`).Scan(&after); err != nil || after != before {
		t.Fatalf("reserved control read changed generation: %d, %v; want %d", after, err, before)
	}
	if _, err := store.Stat(t.Context(), ""); locking.CodeOf(err) != locking.Retired {
		t.Fatalf("reserved authority admitted ordinary file reads: %v", err)
	}
}

func TestServiceRetirementReservedAuthorityChecksDurableResponsibilities(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	namespace := servicePolicyNamespace(t, store.Store, 1)
	proof := serviceAcceptedQuota(namespace, "reserved-quota", time.Now(), 1, 16384)
	serviceAcceptedQuotaPending(t, store.Store, proof)
	command := nativeTransferCommand(t, store.Store, "reserved-import", "imported", "content", 0)
	nativeTransferSeal(t, t.Context(), store.Store, command)
	if err := store.locks.ReserveRetirement(t.Context()); err != nil {
		t.Fatalf("quiescent authority did not reserve retirement: %v", err)
	}
	serviceReservedRetirementStatus(t, store.Store, syscall.EBUSY)
	store.locks.CancelRetirement()
	if _, err := store.SetQuota(t.Context(), proof.Command); err != nil {
		t.Fatal(err)
	}
	if err := store.locks.ReserveRetirement(t.Context()); err != nil {
		t.Fatal(err)
	}
	serviceReservedRetirementStatus(t, store.Store, syscall.EBUSY)
	store.locks.CancelRetirement()
	if _, err := store.PublishImport(t.Context(), command.Action); err != nil {
		t.Fatal(err)
	}
	if err := store.locks.ReserveRetirement(t.Context()); err != nil {
		t.Fatal(err)
	}
	serviceReservedRetirementStatus(t, store.Store, nil)
	if err := store.CheckVolumeRetirement(); err != nil {
		t.Fatalf("reservation hid native retirement capability: %v", err)
	}
}

func TestServiceRetirementReservedControlReadPreservesCancellationHealthAndClosure(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	if err := store.locks.ReserveRetirement(t.Context()); err != nil {
		t.Fatal(err)
	}
	serviceReservedRetirementStatus(t, store.Store, nil)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.CheckDeletionReady(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("reserved control read lost cancellation: %v", err)
	}
	failure := errors.New("reserved authority durability unavailable")
	store.coordinator.poisonWith(failure)
	t.Cleanup(func() {
		store.coordinator.health.Lock()
		store.coordinator.poison = nil
		store.coordinator.health.Unlock()
	})
	if err := store.CheckDeletionReady(t.Context()); !errors.Is(err, failure) || storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("reserved control read concealed unhealthy authority: %v", err)
	}
	store.coordinator.health.Lock()
	store.coordinator.poison = nil
	store.coordinator.health.Unlock()
	serviceReservedRetirementStatus(t, store.Store, nil)
	if err := store.Close(); err != nil {
		t.Fatalf("closing reserved authority failed: %v", err)
	}
	if err := store.CheckDeletionReady(t.Context()); locking.CodeOf(err) != locking.Retired {
		t.Fatalf("closed authority was mistaken for a healthy reservation: %v", err)
	}
}
