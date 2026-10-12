package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func servicePolicyConfig(t *testing.T, quota int64) LockingConfig {
	t.Helper()
	config := lockingTestConfig(t)
	config.Allowance = quota
	config.SQLite.QuotaPolicy = DurableQuotaPolicy
	config.SQLite.InitializeQuota = true
	return config
}

func servicePolicyOpen(t *testing.T, config LockingConfig) *LockingStore {
	t.Helper()
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func servicePolicyNamespace(t *testing.T, store *Store, sequence uint64) storage.ActionNamespace {
	t.Helper()
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: sequence, AcceptUntil: time.Now().Add(time.Hour)}
	if err := store.RegisterActionNamespace(t.Context(), namespace); err != nil {
		t.Fatal(err)
	}
	return namespace
}

func servicePolicyCommand(namespace storage.ActionNamespace, nonce string, revision uint64, quota int64) storage.QuotaCommand {
	command := storage.QuotaCommand{Action: storage.ActionKey{SubjectID: namespace.SubjectID, Namespace: namespace.Sequence, Nonce: nonce}, ExpectedRevision: revision, QuotaBytes: quota}
	command.Fingerprint = command.EffectFingerprint()
	return command
}

func servicePolicyExpireNamespace(t *testing.T, store *Store, namespace storage.ActionNamespace) {
	t.Helper()
	if err := store.mutate(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `UPDATE action_subjects SET logical_sec=?,logical_nsec=? WHERE volume=? AND subject=?`, namespace.AcceptUntil.Unix()+1, namespace.AcceptUntil.Nanosecond(), store.volume, namespace.SubjectID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func servicePolicySummary(t *testing.T, store *Store, files, dirs uint64, used, quota int64, revision uint64) {
	t.Helper()
	want := storage.VolumeSummaryResult{FileCount: files, DirectoryCount: dirs, AllocationUsed: used, QuotaBytes: quota, QuotaRevision: revision}
	got, err := store.Summary(t.Context())
	if err != nil || got != want {
		t.Fatalf("summary = %+v, %v; want %+v", got, err, want)
	}
	if err := got.Check(); err != nil {
		t.Fatalf("invalid summary: %v", err)
	}
}

func TestServicePolicySummaryTracksNamedNodesAndDetachedAllocation(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 16384))
	servicePolicySummary(t, store.Store, 0, 0, 0, 16384, 1)
	for _, path := range []string{"left", "right"} {
		if err := store.Mkdir(t.Context(), path); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"left/empty", "right/replaced"} {
		if err := store.Create(t.Context(), path); err != nil {
			t.Fatal(err)
		}
	}
	key, err := store.Reserve(t.Context(), "left/data", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(t.Context(), "left/data", metastore.Object{Key: key, Size: 1, ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameSymlink, Action: fileAction(t),
		Name:   storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("link")},
		Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{LinkTarget: []byte("left")},
	}); err != nil {
		t.Fatal(err)
	}
	servicePolicySummary(t, store.Store, 4, 2, 4096, 16384, 1)
	if err := store.Rename(t.Context(), "left/empty", "right/replaced"); err != nil {
		t.Fatal(err)
	}
	if err := store.Rename(t.Context(), "left", "moved"); err != nil {
		t.Fatal(err)
	}
	servicePolicySummary(t, store.Store, 3, 2, 4096, 16384, 1)
	file, err := store.OpenFile(t.Context(), "moved/data", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := store.Remove(t.Context(), "moved/data"); err != nil {
		t.Fatal(err)
	}
	servicePolicySummary(t, store.Store, 2, 2, 4096, 16384, 1)
	if err := store.RemoveDir(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(t.Context(), "link"); err != nil {
		t.Fatal(err)
	}
	servicePolicySummary(t, store.Store, 1, 1, 4096, 16384, 1)
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	servicePolicySummary(t, store.Store, 1, 1, 0, 16384, 1)
}

func TestServicePolicyQuotaReceiptsSurviveReopenAndNeverReexecute(t *testing.T) {
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
	namespace := servicePolicyNamespace(t, store.Store, 1)
	first := servicePolicyCommand(namespace, "first", 1, 4096)
	receipt, err := store.SetQuota(t.Context(), first)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted || receipt.QuotaRevision != 2 || receipt.QuotaBytes != 4096 {
		t.Fatalf("first quota = %+v, %v", receipt, err)
	}
	stale := servicePolicyCommand(namespace, "stale", 1, 100)
	refusal, err := store.SetQuota(t.Context(), stale)
	if !errors.Is(err, syscall.ESTALE) || refusal.Outcome != storage.DurableActionNotExecuted || refusal.Failure != syscall.ESTALE {
		t.Fatalf("stale quota = %+v, %v", refusal, err)
	}
	second := servicePolicyCommand(namespace, "second", 2, 16384)
	if _, err := store.SetQuota(t.Context(), second); err != nil {
		t.Fatal(err)
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
	servicePolicySummary(t, store.Store, 0, 0, 0, 16384, 3)
	for _, action := range []storage.QuotaCommand{first, stale} {
		want := receipt
		wantErr := syscall.Errno(0)
		if action == stale {
			want, wantErr = refusal, syscall.ESTALE
		}
		got, err := store.SetQuota(t.Context(), action)
		if got != want || wantErr == 0 && err != nil || wantErr != 0 && !errors.Is(err, wantErr) {
			t.Fatalf("replayed quota = %+v, %v; want %+v, %v", got, err, want, wantErr)
		}
		queried, err := store.QueryQuotaAction(t.Context(), action.Action)
		if queried != want || wantErr == 0 && err != nil || wantErr != 0 && !errors.Is(err, wantErr) {
			t.Fatalf("queried quota = %+v, %v; want %+v, %v", queried, err, want, wantErr)
		}
	}
	changed := first
	changed.QuotaBytes = 8192
	changed.Fingerprint = changed.EffectFingerprint()
	if _, err := store.SetQuota(t.Context(), changed); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed action input = %v; want EINVAL", err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 16384, 3)
}

func TestServicePolicyQuotaChangeRevalidatesPreparedPublication(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	namespace := servicePolicyNamespace(t, store.Store, 1)
	key, err := store.Reserve(t.Context(), "prepared", 4097)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(namespace, "lower", 1, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(t.Context(), "prepared", metastore.Object{Key: key, Size: 4097, ModTime: time.Now()}); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("publication after quota reduction = %v; want EDQUOT", err)
	}
	if _, err := store.Stat(t.Context(), "prepared"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused publication exposed a file: %v", err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 4096, 2)
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(namespace, "raise", 2, 8192)); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(t.Context(), "prepared", metastore.Object{Key: key, Size: 4097, ModTime: time.Now()}); err != nil {
		t.Fatalf("refused publication did not preserve reservation: %v", err)
	}
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(namespace, "zero", 3, 0)); err != nil {
		t.Fatal(err)
	}
	servicePolicySummary(t, store.Store, 1, 0, 8192, 0, 4)
	if got, err := store.Space(t.Context()); err != nil || got != (storage.Space{Used: 8192}) {
		t.Fatalf("durable zero quota = %+v, %v", got, err)
	}
	if _, err := store.Reserve(t.Context(), "other", 1); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("durable zero quota admitted growth: %v", err)
	}
	if err := store.Commit(t.Context(), "prepared", metastore.Object{ModTime: time.Now()}); err != nil {
		t.Fatalf("over-quota volume could not release allocation: %v", err)
	}
	servicePolicySummary(t, store.Store, 1, 0, 0, 0, 4)
}

func TestServicePolicyNamespacesKeepReceiptsAfterAdmissionExpires(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 4096))
	if err := store.CheckActionNamespaces(); err != nil {
		t.Fatalf("exclusive authority did not advertise namespace admission: %v", err)
	}
	namespace := servicePolicyNamespace(t, store.Store, 1)
	if err := store.RegisterActionNamespace(t.Context(), namespace); err != nil {
		t.Fatalf("identical registration: %v", err)
	}
	changed := namespace
	changed.AcceptUntil = changed.AcceptUntil.Add(time.Second)
	if err := store.RegisterActionNamespace(t.Context(), changed); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed namespace deadline = %v; want EINVAL", err)
	}
	command := servicePolicyCommand(namespace, "known", 1, 8192)
	want, err := store.SetQuota(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	servicePolicyExpireNamespace(t, store.Store, namespace)
	if got, err := store.SetQuota(t.Context(), command); err != nil || got != want {
		t.Fatalf("known action after deadline = %+v, %v; want %+v", got, err, want)
	}
	unknown := servicePolicyCommand(namespace, "new", 2, 4096)
	if _, err := store.SetQuota(t.Context(), unknown); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("new action in expired namespace = %v; want ESTALE", err)
	}
	if got, err := store.QueryQuotaAction(t.Context(), unknown.Action); !errors.Is(err, syscall.EIO) || got.Outcome != storage.DurableActionUnknown {
		t.Fatalf("missing action query = %+v, %v; want unknown EIO", got, err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 8192, 2)
}

func TestServicePolicyNamespaceRetirementProtectsRecentAndPendingReceipts(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 4096))
	namespace := servicePolicyNamespace(t, store.Store, 1)
	command := servicePolicyCommand(namespace, "retained", 1, 8192)
	if _, err := store.SetQuota(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	retirement := storage.ActionNamespaceRetirement{SubjectID: namespace.SubjectID, Before: 2, RetainAfter: time.Now().Add(time.Hour)}
	if err := store.RetireActionNamespaces(t.Context(), retirement); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retiring an active namespace = %v; want EBUSY", err)
	}
	servicePolicyExpireNamespace(t, store.Store, namespace)
	retirement.RetainAfter = time.Unix(1, 0)
	if err := store.RetireActionNamespaces(t.Context(), retirement); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retiring a recent receipt = %v; want EBUSY", err)
	}
	retirement.RetainAfter = time.Now().Add(time.Hour)
	if err := store.mutate(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `UPDATE authority_actions SET outcome=? WHERE volume=? AND subject=? AND namespace=? AND nonce=?`, storage.DurableActionPending, store.volume, command.Action.SubjectID, command.Action.Namespace, command.Action.Nonce)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RetireActionNamespaces(t.Context(), retirement); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retiring pending work = %v; want EBUSY", err)
	}
	if err := store.mutate(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `UPDATE authority_actions SET outcome=? WHERE volume=? AND subject=? AND namespace=? AND nonce=?`, storage.DurableActionCompleted, store.volume, command.Action.SubjectID, command.Action.Namespace, command.Action.Nonce)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatal(err)
	}
	if err := store.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatalf("repeated retirement: %v", err)
	}
	if err := store.RegisterActionNamespace(t.Context(), namespace); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired namespace registered again: %v", err)
	}
	if _, err := store.SetQuota(t.Context(), command); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired action executed again: %v", err)
	}
	if got, err := store.QueryQuotaAction(t.Context(), command.Action); !errors.Is(err, syscall.ESTALE) || got.Outcome != storage.DurableActionHistoryExpired {
		t.Fatalf("retired history query = %+v, %v", got, err)
	}
	next := storage.ActionNamespace{SubjectID: namespace.SubjectID, Sequence: 2, AcceptUntil: namespace.AcceptUntil.Add(time.Hour)}
	if err := store.RegisterActionNamespace(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(next, "next", 2, 16384)); err != nil {
		t.Fatalf("next namespace could not execute: %v", err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 16384, 3)
}

func TestServicePolicyNamespacesRejectInvalidAndOverflowingAdmission(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 4096))
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: time.Unix(1, 0)}
	if err := store.RegisterActionNamespace(t.Context(), namespace); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("already expired namespace = %v; want ESTALE", err)
	}
	namespace.Sequence = math.MaxUint64
	if err := store.RegisterActionNamespace(t.Context(), namespace); !errors.Is(err, syscall.EOVERFLOW) {
		t.Fatalf("overflowed namespace = %v; want EOVERFLOW", err)
	}
	namespace.Sequence = 1
	command := servicePolicyCommand(namespace, "unregistered", 1, 8192)
	if _, err := store.SetQuota(t.Context(), command); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("unregistered namespace = %v; want ESTALE", err)
	}
	command.Fingerprint[0] ^= 1
	if _, err := store.SetQuota(t.Context(), command); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid fingerprint = %v; want EINVAL", err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 4096, 1)
}

func TestServicePolicyConcurrentQuotaCompareAndSwapCommitsOneReceipt(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 4096))
	namespace := servicePolicyNamespace(t, store.Store, 1)
	commands := []storage.QuotaCommand{
		servicePolicyCommand(namespace, "first", 1, 8192),
		servicePolicyCommand(namespace, "second", 1, 16384),
	}
	type result struct {
		receipt storage.QuotaReceipt
		err     error
	}
	results := make([]result, len(commands))
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i, command := range commands {
		workers.Go(func() {
			<-start
			results[i].receipt, results[i].err = store.SetQuota(t.Context(), command)
		})
	}
	close(start)
	workers.Wait()
	completed := 0
	var quota int64
	for i, result := range results {
		if result.receipt.Outcome == storage.DurableActionCompleted {
			completed++
			quota = commands[i].QuotaBytes
			if result.err != nil || result.receipt.QuotaRevision != 2 || result.receipt.QuotaBytes != quota {
				t.Fatalf("concurrent quota winner = %+v, %v", result.receipt, result.err)
			}
		} else if result.receipt.Outcome != storage.DurableActionNotExecuted || !errors.Is(result.err, syscall.ESTALE) {
			t.Fatalf("concurrent quota loser = %+v, %v", result.receipt, result.err)
		}
		queried, err := store.QueryQuotaAction(t.Context(), commands[i].Action)
		if queried != result.receipt || !errors.Is(err, result.err) {
			t.Fatalf("concurrent result not durably queryable = %+v, %v; want %+v, %v", queried, err, result.receipt, result.err)
		}
	}
	if completed != 1 {
		t.Fatalf("concurrent quota changes completed %d times; want 1", completed)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, quota, 2)
}

func TestServicePolicySummaryAndConstructorPolicyPersistAcrossReopen(t *testing.T) {
	config := servicePolicyConfig(t, 8192)
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Mkdir(t.Context(), "dir"); err != nil {
		store.Close()
		t.Fatal(err)
	}
	key, err := store.Reserve(t.Context(), "dir/file", 1)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Commit(t.Context(), "dir/file", metastore.Object{Key: key, Size: 1, ModTime: time.Now()}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	config.SQLite.InitializeQuota = false
	fixed := config
	fixed.SQLite.QuotaPolicy = FixedAllowancePolicy
	if rejected, err := OpenLocking(t.Context(), fixed); !errors.Is(err, syscall.EINVAL) {
		if rejected != nil {
			rejected.Close()
		}
		t.Fatalf("fixed policy reopened durable authority = %v; want EINVAL", err)
	}
	config.Allowance = 1
	store = servicePolicyOpen(t, config)
	servicePolicySummary(t, store.Store, 1, 1, 4096, 8192, 1)
	if err := store.Remove(t.Context(), "dir/file"); err != nil {
		t.Fatal(err)
	}
	servicePolicySummary(t, store.Store, 0, 1, 0, 8192, 1)
}

func TestServicePolicyBoundedAuthorityRejectsAdmissionWithoutEffects(t *testing.T) {
	config := servicePolicyConfig(t, 4096)
	config.SQLite.MaxAuthorityActions = 1
	store := servicePolicyOpen(t, config)
	namespace := servicePolicyNamespace(t, store.Store, 1)
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(namespace, "accepted", 1, 8192)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetQuota(t.Context(), servicePolicyCommand(namespace, "rejected", 2, 16384)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("exhausted receipt admission = %v; want EAGAIN", err)
	}
	other := namespace
	other.SubjectID = "other"
	if err := store.RegisterActionNamespace(t.Context(), other); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("exhausted subject admission = %v; want EAGAIN", err)
	}
	other = namespace
	other.Sequence++
	if err := store.RegisterActionNamespace(t.Context(), other); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("exhausted namespace admission = %v; want EAGAIN", err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 8192, 2)
}

func TestServicePolicyFixedZeroAllowanceDoesNotInventFiniteSummary(t *testing.T) {
	store := servicePolicyOpen(t, lockingTestConfig(t))
	if err := store.CheckVolumeSummary(); !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("fixed unlimited allowance advertised finite summary: %v", err)
	}
	if summary, err := store.Summary(t.Context()); !errors.Is(err, syscall.ENOSYS) || summary != (storage.VolumeSummaryResult{}) {
		t.Fatalf("fixed unlimited summary = %+v, %v; want unsupported with no result", summary, err)
	}
	if space, err := store.Space(t.Context()); !errors.Is(err, syscall.ENOSYS) || space != (storage.Space{}) {
		t.Fatalf("fixed unlimited space = %+v, %v; want unsupported with no result", space, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if summary, err := store.Summary(canceled); !errors.Is(err, context.Canceled) || summary != (storage.VolumeSummaryResult{}) {
		t.Fatalf("canceled unlimited summary = %+v, %v; want canceled with no result", summary, err)
	}
	key, err := store.Reserve(t.Context(), "file", 1)
	if err != nil {
		t.Fatalf("fixed zero allowance lost unlimited admission: %v", err)
	}
	if err := store.Abandon(t.Context(), key); err != nil {
		t.Fatal(err)
	}
}

func TestServicePolicyDurableZeroQuotaIsAuthoritative(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 0))
	if err := store.CheckVolumeSummary(); err != nil {
		t.Fatalf("durable zero quota did not advertise summary: %v", err)
	}
	if err := store.CheckQuotaControl(); err != nil {
		t.Fatalf("durable zero quota did not advertise control: %v", err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 0, 1)
	if space, err := store.Space(t.Context()); err != nil || space != (storage.Space{}) {
		t.Fatalf("authoritative zero-quota space = %+v, %v", space, err)
	}
	if _, err := store.Reserve(t.Context(), "file", 1); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("durable zero quota admitted allocation: %v", err)
	}
	servicePolicySummary(t, store.Store, 0, 0, 0, 0, 1)
}
