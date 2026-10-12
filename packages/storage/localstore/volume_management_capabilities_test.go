package localstore_test

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestLocalstoreVolumeManagementCapabilitiesAndRetirementPersistAcrossReopen(t *testing.T) {
	config := testConfig(privateRoot(t))
	config.QuotaPolicy, config.InitializeQuota = sqlite.DurableQuotaPolicy, true
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	for name, check := range map[string]func() error{
		"summary": store.CheckVolumeSummary, "quota": store.CheckQuotaControl,
		"action namespaces": store.CheckActionNamespaces,
	} {
		if err := check(); err != nil {
			t.Fatalf("durable localstore does not support %s: %v", name, err)
		}
	}
	summary, err := store.Summary(t.Context())
	if err != nil || summary.FileCount != 0 || summary.DirectoryCount != 0 || summary.AllocationUsed != 0 || summary.QuotaBytes != config.Quota {
		t.Fatalf("initial authoritative summary = %+v, %v", summary, err)
	}
	retirement := storage.ActionNamespaceRetirement{SubjectID: "retired-subject", Before: 2, RetainAfter: time.Now()}
	if err := store.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatalf("retire previously absent subject: %v", err)
	}
	old := storage.ActionNamespace{SubjectID: retirement.SubjectID, Sequence: 1, AcceptUntil: time.Now().Add(time.Hour)}
	if err := store.RegisterActionNamespace(t.Context(), old); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired sequence was registered: %v", err)
	}
	current := old
	current.Sequence = 2
	if err := store.RegisterActionNamespace(t.Context(), current); err != nil {
		t.Fatalf("sequence at retirement floor was rejected: %v", err)
	}
	command := storage.QuotaCommand{
		Action:           storage.ActionKey{SubjectID: current.SubjectID, Namespace: current.Sequence, Nonce: "quota-after-retirement"},
		ExpectedRevision: summary.QuotaRevision, QuotaBytes: config.Quota + 4096,
	}
	command.Fingerprint = command.EffectFingerprint()
	receipt, err := store.SetQuota(t.Context(), command)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted || receipt.QuotaBytes != command.QuotaBytes {
		t.Fatalf("quota action in admitted namespace = %+v, %v", receipt, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config.InitializeQuota = false
	reopened := open(t, config)
	t.Cleanup(func() { closeStore(t, reopened) })
	if err := reopened.RegisterActionNamespace(t.Context(), old); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("reopen lost retirement watermark: %v", err)
	}
	if err := reopened.RegisterActionNamespace(t.Context(), current); err != nil {
		t.Fatalf("reopen lost accepted namespace registration: %v", err)
	}
	if err := reopened.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatalf("idempotent retirement replay after reopen: %v", err)
	}
	queried, err := reopened.QueryQuotaAction(t.Context(), command.Action)
	if err != nil || queried != receipt {
		t.Fatalf("reopen lost original quota receipt: %+v, %v", queried, err)
	}
	summary, err = reopened.Summary(t.Context())
	if err != nil || summary.QuotaBytes != command.QuotaBytes || summary.QuotaRevision != receipt.QuotaRevision || summary.AllocationUsed != 0 {
		t.Fatalf("reopened authoritative policy = %+v, %v", summary, err)
	}
}
