package localstore_test

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestDurableZeroQuotaReopensWithoutChangingPolicy(t *testing.T) {
	config := testConfig(privateRoot(t))
	config.QuotaPolicy = sqlite.DurableQuotaPolicy
	config.InitializeQuota = true
	first := open(t, config)
	if err := first.Create(t.Context(), "existing"); err != nil {
		t.Fatal(err)
	}
	if err := first.Write(t.Context(), "existing", []byte("existing bytes")); err != nil {
		t.Fatal(err)
	}
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: time.Now().Add(time.Hour)}
	if err := first.RegisterActionNamespace(t.Context(), namespace); err != nil {
		t.Fatal(err)
	}
	summary, err := first.Summary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command := storage.QuotaCommand{Action: storage.ActionKey{SubjectID: "owner", Namespace: 1, Nonce: "zero-quota"}, ExpectedRevision: summary.QuotaRevision, QuotaBytes: 0}
	command.Fingerprint = command.EffectFingerprint()
	receipt, err := first.SetQuota(t.Context(), command)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted || receipt.QuotaBytes != 0 {
		t.Fatalf("zero quota receipt=%+v,error=%v", receipt, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	config.InitializeQuota = false
	config.Quota = 0
	second := open(t, config)
	t.Cleanup(func() { closeStore(t, second) })
	space, err := second.Space(t.Context())
	if err != nil || space.Total != 0 || space.Used == 0 || space.Avail != 0 {
		t.Fatalf("reopened zero quota space=%+v,error=%v", space, err)
	}
	data, err := second.Read(t.Context(), "existing")
	if err != nil || string(data) != "existing bytes" {
		t.Fatalf("existing data=%q,error=%v", data, err)
	}
	if err := second.Write(t.Context(), "existing", make([]byte, 8192)); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("growth beyond zero quota=%v", err)
	}
	replay, err := second.QueryQuotaAction(t.Context(), command.Action)
	if err != nil || replay != receipt {
		t.Fatalf("reopened action=%+v,error=%v", replay, err)
	}
}

func TestDurableAcceptedQuotaActionRetainsOriginalProofAcrossReopen(t *testing.T) {
	config := testConfig(privateRoot(t))
	config.QuotaPolicy = sqlite.DurableQuotaPolicy
	config.InitializeQuota = true
	first := open(t, config)
	if err := first.CheckQuotaAcceptance(); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	command := storage.QuotaCommand{Action: storage.ActionKey{SubjectID: "owner", Namespace: 1, Nonce: "accepted-quota"}, ExpectedRevision: 1, QuotaBytes: 0}
	command.Fingerprint = command.EffectFingerprint()
	proof := storage.AcceptedQuotaAction{Command: command, Namespace: storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: now.Add(-time.Hour)}, AcceptedAt: now.Add(-2 * time.Hour)}
	accepted, err := first.AcceptQuotaAction(t.Context(), proof)
	if err != nil || accepted.Outcome != storage.DurableActionPending {
		t.Fatalf("acceptance=%+v,error=%v", accepted, err)
	}
	receipt, err := first.SetQuota(t.Context(), command)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("publication=%+v,error=%v", receipt, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	config.InitializeQuota = false
	config.Quota = 0
	second := open(t, config)
	t.Cleanup(func() { closeStore(t, second) })
	replay, err := second.AcceptQuotaAction(t.Context(), proof)
	if err != nil || replay != receipt {
		t.Fatalf("reopened acceptance=%+v,error=%v", replay, err)
	}
	invalid := proof
	invalid.AcceptedAt = now
	if _, err := second.AcceptQuotaAction(t.Context(), invalid); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid proof accepted: %v", err)
	}
}
