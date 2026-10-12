package sqlite

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func serviceAcceptedQuota(namespace storage.ActionNamespace, nonce string, acceptedAt time.Time, revision uint64, quota int64) storage.AcceptedQuotaAction {
	return storage.AcceptedQuotaAction{
		Namespace: namespace, AcceptedAt: acceptedAt,
		Command: servicePolicyCommand(namespace, nonce, revision, quota),
	}
}

func serviceAcceptedQuotaPending(t *testing.T, store *Store, proof storage.AcceptedQuotaAction) storage.QuotaReceipt {
	t.Helper()
	receipt, err := store.AcceptQuotaAction(t.Context(), proof)
	if err != nil || receipt.Outcome != storage.DurableActionPending || receipt.Action != proof.Command.Action || receipt.Fingerprint != proof.Command.Fingerprint {
		t.Fatalf("accepted pending quota = %+v, %v", receipt, err)
	}
	if err := receipt.Check(); err != nil {
		t.Fatalf("invalid pending receipt: %v", err)
	}
	return receipt
}

func TestServiceQuotaAcceptanceExpiredProofExecutesOnceAcrossReopen(t *testing.T) {
	config := servicePolicyConfig(t, 8192)
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	now := time.Now()
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(-time.Hour)}
	proof := serviceAcceptedQuota(namespace, "accepted", now.Add(-2*time.Hour), 1, 4096)
	pending := serviceAcceptedQuotaPending(t, store.Store, proof)
	if queried, err := store.QueryQuotaAction(t.Context(), proof.Command.Action); err != nil || queried != pending {
		t.Fatalf("pending quota query = %+v, %v", queried, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	config.SQLite.InitializeQuota = false
	config.Allowance = 1
	store, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := store.AcceptQuotaAction(t.Context(), proof); err != nil || replay != pending {
		t.Fatalf("pending acceptance reopen = %+v, %v; want %+v", replay, err, pending)
	}
	completed, err := store.SetQuota(t.Context(), proof.Command)
	if err != nil || completed.Outcome != storage.DurableActionCompleted || completed.QuotaBytes != 4096 || completed.QuotaRevision != 2 {
		t.Fatalf("expired accepted action execution = %+v, %v", completed, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if replay, err := store.AcceptQuotaAction(t.Context(), proof); err != nil || replay != completed {
			t.Fatalf("terminal acceptance reopen = %+v, %v; want %+v", replay, err, completed)
		}
		if replay, err := store.SetQuota(t.Context(), proof.Command); err != nil || replay != completed {
			t.Fatalf("terminal execution replay = %+v, %v; want %+v", replay, err, completed)
		}
	}
	if after, err := store.DurableState(t.Context()); err != nil || after != before {
		t.Fatalf("accepted replay executed again: %+v, %v; want %+v", after, err, before)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 4096, 2)
}

func TestServiceQuotaAcceptanceExpiredOutOfOrderNamespaceCannotAdmitUnrelatedWork(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	now := time.Now()
	newer := storage.ActionNamespace{SubjectID: "owner", Sequence: 2, AcceptUntil: now.Add(2 * time.Hour)}
	if err := store.RegisterActionNamespace(t.Context(), newer); err != nil {
		t.Fatal(err)
	}
	older := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(time.Hour)}
	servicePolicyExpireNamespace(t, store.Store, older)
	proof := serviceAcceptedQuota(older, "recovered", now.Add(-time.Minute), 1, 4096)
	serviceAcceptedQuotaPending(t, store.Store, proof)
	before, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := store.RegisterActionNamespace(t.Context(), older); err != nil {
			t.Fatalf("identical recovery-only registration: %v", err)
		}
		unknown := servicePolicyCommand(older, "unknown", 1, 16384)
		if _, err := store.SetQuota(t.Context(), unknown); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("recovery-only namespace admitted unrelated quota: %v", err)
		}
		importCommand := nativeTransferCommand(t, store.Store, "unrelated-import", "file", "content", 0)
		if _, err := store.PrepareImport(t.Context(), importCommand); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("recovery-only namespace admitted unrelated import: %v", err)
		}
	}
	if after, err := store.DurableState(t.Context()); err != nil || after != before {
		t.Fatalf("recovery-only registration upgraded admission: %+v, %v; want %+v", after, err, before)
	}
	if receipt, err := store.SetQuota(t.Context(), proof.Command); err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("specific recovered action did not execute: %+v, %v", receipt, err)
	}
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(newer, "ordinary", 2, 16384)); err != nil {
		t.Fatalf("ordinary newer namespace lost admission: %v", err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 16384, 3)
}

func TestServiceQuotaAcceptanceLiveWindowPreservesOrdinaryAdmission(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	now := time.Now()
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(time.Hour)}
	proof := serviceAcceptedQuota(namespace, "accepted", now, 1, 4096)
	serviceAcceptedQuotaPending(t, store.Store, proof)
	if receipt, err := store.SetQuota(t.Context(), proof.Command); err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("live accepted action did not execute: %+v, %v", receipt, err)
	}
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(namespace, "ordinary", 2, 16384)); err != nil {
		t.Fatalf("live bridge registration denied ordinary work: %v", err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 16384, 3)
}

func TestServiceQuotaAcceptanceSparseReverseNamespaceRegistration(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	now := time.Now()
	issuedEarlier := storage.ActionNamespace{SubjectID: "owner", Sequence: 3, AcceptUntil: now.Add(time.Hour)}
	usedFirst := storage.ActionNamespace{SubjectID: "owner", Sequence: 9, AcceptUntil: now.Add(2 * time.Hour)}
	if err := store.RegisterActionNamespace(t.Context(), usedFirst); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterActionNamespace(t.Context(), issuedEarlier); err != nil {
		t.Fatalf("live sparse namespace could not be registered after later first use: %v", err)
	}
	var highwater int64
	if err := store.read.QueryRowContext(t.Context(), `SELECT highwater FROM action_subjects WHERE volume=? AND subject=?`, store.volume, "owner").Scan(&highwater); err != nil || highwater != 9 {
		t.Fatalf("reverse registration changed highwater = %d, %v; want 9", highwater, err)
	}
	firstCommand := servicePolicyCommand(usedFirst, "first-use", 1, 4096)
	if _, err := store.SetQuota(t.Context(), firstCommand); err != nil {
		t.Fatal(err)
	}
	olderCommand := servicePolicyCommand(issuedEarlier, "later-use", 2, 16384)
	if _, err := store.SetQuota(t.Context(), olderCommand); err != nil {
		t.Fatalf("live lower sequence denied its first action: %v", err)
	}
	servicePolicyExpireNamespace(t, store.Store, issuedEarlier)
	retirement := storage.ActionNamespaceRetirement{SubjectID: "owner", Before: 4, RetainAfter: now.Add(time.Hour)}
	if err := store.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterActionNamespace(t.Context(), issuedEarlier); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("sparse registration revived a retired sequence: %v", err)
	}
	if _, err := store.SetQuota(t.Context(), olderCommand); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("sparse registration revived retired action history: %v", err)
	}
	if receipt, err := store.QueryQuotaAction(t.Context(), olderCommand.Action); !errors.Is(err, syscall.ESTALE) || receipt.Outcome != storage.DurableActionHistoryExpired {
		t.Fatalf("sparse retired history = %+v, %v", receipt, err)
	}
	withinFloor := storage.ActionNamespace{SubjectID: "owner", Sequence: 5, AcceptUntil: now.Add(2 * time.Hour)}
	if err := store.RegisterActionNamespace(t.Context(), withinFloor); err != nil {
		t.Fatalf("live unretired sparse namespace denied: %v", err)
	}
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(withinFloor, "in-range", 3, 32768)); err != nil {
		t.Fatal(err)
	}
	if err := store.read.QueryRowContext(t.Context(), `SELECT highwater FROM action_subjects WHERE volume=? AND subject=?`, store.volume, "owner").Scan(&highwater); err != nil || highwater != 9 {
		t.Fatalf("sparse admission rewound highwater = %d, %v; want 9", highwater, err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 32768, 4)
}

func TestServiceQuotaAcceptanceProofIsImmutableBeforeAndAfterExecution(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	now := time.Now()
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(time.Hour)}
	proof := serviceAcceptedQuota(namespace, "immutable", now.Add(-time.Minute), 1, 4096)
	serviceAcceptedQuotaPending(t, store.Store, proof)
	for _, phase := range []string{"pending", "completed"} {
		if phase == "completed" {
			if _, err := store.SetQuota(t.Context(), proof.Command); err != nil {
				t.Fatal(err)
			}
		}
		before, err := store.DurableState(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		variants := []storage.AcceptedQuotaAction{proof, proof, proof}
		variants[0].AcceptedAt = variants[0].AcceptedAt.Add(time.Second)
		variants[1].Namespace.AcceptUntil = variants[1].Namespace.AcceptUntil.Add(time.Second)
		variants[2].Command.QuotaBytes++
		variants[2].Command.Fingerprint = variants[2].Command.EffectFingerprint()
		for i, changed := range variants {
			if _, err := store.AcceptQuotaAction(t.Context(), changed); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("%s changed proof %d = %v; want EINVAL", phase, i, err)
			}
		}
		if after, err := store.DurableState(t.Context()); err != nil || after != before {
			t.Fatalf("%s rejected proof changed authority: %+v, %v; want %+v", phase, after, err, before)
		}
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 4096, 2)
}

func TestServiceQuotaAcceptancePendingResponsibilityBlocksPermanentRetirement(t *testing.T) {
	config := servicePolicyConfig(t, 8192)
	store := servicePolicyOpen(t, config)
	now := time.Now()
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(-time.Hour)}
	proof := serviceAcceptedQuota(namespace, "retired", now.Add(-2*time.Hour), 1, 4096)
	serviceAcceptedQuotaPending(t, store.Store, proof)
	retirement := storage.ActionNamespaceRetirement{SubjectID: "owner", Before: 2, RetainAfter: now.Add(time.Hour)}
	if err := store.RetireActionNamespaces(t.Context(), retirement); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retirement forgot pending accepted work: %v", err)
	}
	if _, err := store.SetQuota(t.Context(), proof.Command); err != nil {
		t.Fatal(err)
	}
	retirement.RetainAfter = now.Add(-time.Hour)
	if err := store.RetireActionNamespaces(t.Context(), retirement); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retirement discarded a recent settled proof: %v", err)
	}
	retirement.RetainAfter = now.Add(time.Hour)
	if err := store.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcceptQuotaAction(t.Context(), proof); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired proof resurrected its action: %v", err)
	}
	if _, err := store.SetQuota(t.Context(), proof.Command); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired quota executed again: %v", err)
	}
	if receipt, err := store.QueryQuotaAction(t.Context(), proof.Command.Action); !errors.Is(err, syscall.ESTALE) || receipt.Outcome != storage.DurableActionHistoryExpired {
		t.Fatalf("retired accepted history = %+v, %v", receipt, err)
	}
	if err := store.RegisterActionNamespace(t.Context(), namespace); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired namespace registered again: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	config.SQLite.InitializeQuota = false
	store = servicePolicyOpen(t, config)
	if _, err := store.AcceptQuotaAction(t.Context(), proof); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("reopen forgot the permanent acceptance floor: %v", err)
	}
	if receipt, err := store.QueryQuotaAction(t.Context(), proof.Command.Action); !errors.Is(err, syscall.ESTALE) || receipt.Outcome != storage.DurableActionHistoryExpired {
		t.Fatalf("reopen forgot retired history: %+v, %v", receipt, err)
	}
	next := storage.ActionNamespace{SubjectID: "owner", Sequence: 2, AcceptUntil: now.Add(time.Hour)}
	if err := store.RegisterActionNamespace(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(next, "next", 2, 16384)); err != nil {
		t.Fatal(err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 16384, 3)
}

func TestServiceQuotaAcceptanceAbsentSubjectRetirementCreatesPermanentFloor(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	now := time.Now()
	retirement := storage.ActionNamespaceRetirement{SubjectID: "owner", Before: 2, RetainAfter: now.Add(-time.Hour)}
	if err := store.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatal(err)
	}
	before, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatalf("absent-subject retirement did not replay: %v", err)
	}
	older := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(time.Hour)}
	proof := serviceAcceptedQuota(older, "too-old", now, 1, 4096)
	if _, err := store.AcceptQuotaAction(t.Context(), proof); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retirement floor admitted old accepted work: %v", err)
	}
	if err := store.RegisterActionNamespace(t.Context(), older); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retirement floor admitted an old namespace: %v", err)
	}
	if receipt, err := store.QueryQuotaAction(t.Context(), proof.Command.Action); !errors.Is(err, syscall.ESTALE) || receipt.Outcome != storage.DurableActionHistoryExpired {
		t.Fatalf("absent-subject retired history = %+v, %v", receipt, err)
	}
	if after, err := store.DurableState(t.Context()); err != nil || after != before {
		t.Fatalf("retired requests altered floor state: %+v, %v; want %+v", after, err, before)
	}
	next := storage.ActionNamespace{SubjectID: "owner", Sequence: 2, AcceptUntil: now.Add(time.Hour)}
	if err := store.RegisterActionNamespace(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(next, "next", 1, 16384)); err != nil {
		t.Fatalf("floor blocked its next namespace: %v", err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 16384, 2)
}

func TestServiceQuotaAcceptanceLogicalClockPreservesRecoveryWithoutReopeningAdmission(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	namespace := servicePolicyNamespace(t, store.Store, 1)
	proof := serviceAcceptedQuota(namespace, "accepted-before-expiry", time.Now(), 1, 4096)
	servicePolicyExpireNamespace(t, store.Store, namespace)
	serviceAcceptedQuotaPending(t, store.Store, proof)
	if err := store.RegisterActionNamespace(t.Context(), namespace); err != nil {
		t.Fatalf("known expired registration did not replay: %v", err)
	}
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(namespace, "new", 1, 16384)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("wall-clock rollback reopened ordinary admission: %v", err)
	}
	next := storage.ActionNamespace{SubjectID: "owner", Sequence: 2, AcceptUntil: time.Now().Add(time.Minute)}
	if err := store.RegisterActionNamespace(t.Context(), next); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("wall-clock rollback admitted an expired new window: %v", err)
	}
	if receipt, err := store.SetQuota(t.Context(), proof.Command); err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("logical-clock advance invalidated accepted work: %+v, %v", receipt, err)
	}
	if receipt, err := store.AcceptQuotaAction(t.Context(), proof); err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("advanced-clock terminal proof = %+v, %v", receipt, err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 4096, 2)
}

func TestServiceQuotaAcceptanceRejectsForeignSubjectBeforeEffects(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	now := time.Now()
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(time.Hour)}
	proof := serviceAcceptedQuota(namespace, "bound", now, 1, 4096)
	foreign, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "foreign", AccessID: "access"})
	if err != nil {
		t.Fatal(err)
	}
	for _, accepted := range []bool{false, true} {
		if accepted {
			serviceAcceptedQuotaPending(t, store.Store, proof)
		}
		before, err := store.DurableState(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.AcceptQuotaAction(foreign, proof); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("foreign subject used trusted quota bridge: %v", err)
		}
		if after, err := store.DurableState(t.Context()); err != nil || after != before {
			t.Fatalf("foreign bridge request changed authority: %+v, %v; want %+v", after, err, before)
		}
	}
	matching, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "rotated-access"})
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := store.AcceptQuotaAction(matching, proof); err != nil || receipt.Outcome != storage.DurableActionPending {
		t.Fatalf("matching subject lost accepted work after access rotation: %+v, %v", receipt, err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 8192, 1)
}

func TestServiceQuotaAcceptanceBoundedRecoveryPreservesExistingResponsibility(t *testing.T) {
	config := servicePolicyConfig(t, 8192)
	config.SQLite.MaxAuthorityActions = 1
	store := servicePolicyOpen(t, config)
	now := time.Now()
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(time.Hour)}
	proof := serviceAcceptedQuota(namespace, "retained", now, 1, 4096)
	pending := serviceAcceptedQuotaPending(t, store.Store, proof)
	before, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	variants := []storage.AcceptedQuotaAction{
		serviceAcceptedQuota(namespace, "new-action", now, 1, 16384),
		serviceAcceptedQuota(storage.ActionNamespace{SubjectID: "owner", Sequence: 2, AcceptUntil: namespace.AcceptUntil}, "new-namespace", now, 1, 16384),
		serviceAcceptedQuota(storage.ActionNamespace{SubjectID: "other", Sequence: 1, AcceptUntil: namespace.AcceptUntil}, "new-subject", now, 1, 16384),
	}
	for _, rejected := range variants {
		if _, err := store.AcceptQuotaAction(t.Context(), rejected); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("exhausted recovery admission = %v; want EAGAIN", err)
		}
	}
	if after, err := store.DurableState(t.Context()); err != nil || after != before {
		t.Fatalf("refused recovery allocated durable state: %+v, %v; want %+v", after, err, before)
	}
	if receipt, err := store.AcceptQuotaAction(t.Context(), proof); err != nil || receipt != pending {
		t.Fatalf("bounded recovery lost original pending work: %+v, %v", receipt, err)
	}
	if receipt, err := store.SetQuota(t.Context(), proof.Command); err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("bounded recovery could not settle accepted work: %+v, %v", receipt, err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 4096, 2)
}
