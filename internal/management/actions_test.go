package management

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"
)

func actionTestNamespace(t *testing.T, s *Store, actor ID) ActionNamespace {
	t.Helper()
	namespace, err := s.IssueNamespace(context.Background(), actor)
	if err != nil {
		t.Fatal(err)
	}
	return namespace
}

func actionTestFinishCreate(t *testing.T, s *Store, operation Operation) Operation {
	t.Helper()
	for _, change := range []OperationTransition{
		{State: OperationRunning, Phase: PhaseInitializing},
		{State: OperationRunning, Phase: PhaseBound, VerifiedRootProof: []byte("verified-native-binding-and-parent-sync")},
		{State: OperationSucceeded, Phase: PhaseCompleted, Settled: true, Result: []byte(`{"initialized":true}`)},
	} {
		var err error
		operation, err = s.TransitionOperation(context.Background(), operation.ID, operation.Revision, change)
		if err != nil {
			t.Fatal(err)
		}
	}
	return operation
}

func actionTestRetire(t *testing.T, s *Store, actor ID) (int64, error) {
	t.Helper()
	candidate, err := s.RetirementCandidate(context.Background(), actor)
	if err != nil {
		return 0, err
	}
	if candidate.Floor == candidate.ExpectedRetiredBefore {
		return candidate.Floor, nil
	}
	return s.ConfirmRetirement(context.Background(), actor, candidate, candidate.NativeRevision, []byte("confirmed-native-watermarks"))
}

func TestActionReplayConcurrentAndReopen(t *testing.T) {
	s, path := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	key := ActionKey{Namespace: namespace.Sequence, Nonce: "create-once"}
	var group sync.WaitGroup
	results := make(chan Operation, 16)
	errorsOut := make(chan error, 16)
	for range 16 {
		group.Go(func() {
			op, err := s.CreateVolume(context.Background(), actor.ID, key, "data", 4096)
			results <- op
			errorsOut <- err
		})
	}
	group.Wait()
	close(results)
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
	var accepted Operation
	for op := range results {
		if accepted.ID != "" && op.ID != accepted.ID {
			t.Fatalf("duplicate effects: %s != %s", accepted.ID, op.ID)
		}
		accepted = op
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, key, "changed", 4096); !errors.Is(err, ErrConflict) {
		t.Fatalf("fingerprint reuse: %v", err)
	}
	accepted = actionTestFinishCreate(t, s, accepted)
	reset, err := s.ResetAdminPassword(context.Background(), actor.ID, "changed-hash")
	if err != nil || reset.AuthEpoch != actor.AuthEpoch+1 {
		t.Fatalf("credential epoch reset: %#v %v", reset, err)
	}
	if known, err := s.Operation(context.Background(), reset.ID, accepted.ID); err != nil || known.ID != accepted.ID {
		t.Fatalf("credential epoch changed original actor receipt: %#v %v", known, err)
	}
	s.now = func() time.Time { return namespace.AcceptUntil.Add(time.Hour) }
	lookup, err := s.GetNamespace(context.Background(), actor.ID, namespace.Sequence)
	if err != nil || lookup != namespace {
		t.Fatalf("expired namespace window changed: %#v %v", lookup, err)
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, key, "data", 4096); err != nil {
		t.Fatalf("known receipt after expiry: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	options := s.options
	options.Create = false
	opened, err := Open(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	known, err := opened.FindOperation(context.Background(), actor.ID, key)
	if err != nil || known.ID != accepted.ID || known.State != OperationSucceeded {
		t.Fatalf("durable receipt: %#v, %v", known, err)
	}
	lookup, err = opened.GetNamespace(context.Background(), actor.ID, namespace.Sequence)
	if err != nil || lookup != namespace {
		t.Fatalf("namespace window changed after reopen: %#v %v", lookup, err)
	}
}

func TestNamespaceExpiryPersistsAcrossClockRollback(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	s.now = func() time.Time { return namespace.AcceptUntil }
	key := ActionKey{Namespace: namespace.Sequence, Nonce: "late"}
	if _, err := s.CreateVolume(context.Background(), actor.ID, key, "late", 4096); !errors.Is(err, ErrExpired) {
		t.Fatal(err)
	}
	s.now = func() time.Time { return namespace.AcceptUntil.Add(-time.Hour) }
	if _, err := s.CreateVolume(context.Background(), actor.ID, key, "late", 4096); !errors.Is(err, ErrExpired) {
		t.Fatalf("clock rollback reopened admission: %v", err)
	}
	issued := actionTestNamespace(t, s, actor.ID)
	if issued.AcceptUntil.Before(namespace.AcceptUntil.Add(s.options.NamespaceLifetime)) {
		t.Fatalf("issued expiry moved backward: %v", issued.AcceptUntil)
	}
}

func TestNamespaceRetirementPreservesUnsettledContiguousResponsibility(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	first := actionTestNamespace(t, s, actor.ID)
	one, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{first.Sequence, "first"}, "first", 4096)
	if err != nil {
		t.Fatal(err)
	}
	second := actionTestNamespace(t, s, actor.ID)
	two, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{second.Sequence, "second"}, "second", 4096)
	if err != nil {
		t.Fatal(err)
	}
	_ = actionTestFinishCreate(t, s, two)
	age := s.now().Add(8 * 24 * time.Hour)
	s.now = func() time.Time { return age }
	if floor, err := actionTestRetire(t, s, actor.ID); err != nil || floor != 0 {
		t.Fatalf("unfinished prefix skipped: %d %v", floor, err)
	}
	one, err = s.TransitionOperation(context.Background(), one.ID, one.Revision, OperationTransition{State: OperationFailed, Phase: PhaseAccepted, ErrorCode: "initialize_failed"})
	if err != nil {
		t.Fatal(err)
	}
	age = age.Add(8 * 24 * time.Hour)
	if floor, err := actionTestRetire(t, s, actor.ID); err != nil || floor != 0 {
		t.Fatalf("unsettled failed prefix skipped: %d %v", floor, err)
	}
	if _, err := s.SettleOperation(context.Background(), one.ID, one.Revision, []byte("confirmed-no-root-or-spool")); err != nil {
		t.Fatal(err)
	}
	age = age.Add(8 * 24 * time.Hour)
	if floor, err := actionTestRetire(t, s, actor.ID); err != nil || floor != second.Sequence {
		t.Fatalf("settled prefix did not retire: %d %v", floor, err)
	}
	if _, err := s.FindOperation(context.Background(), actor.ID, ActionKey{first.Sequence, "first"}); !errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("retired lookup: %v", err)
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{first.Sequence, "first"}, "first", 4096); !errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("retired action admitted: %v", err)
	}
}

func TestOperationTransitionsRequireProofAndCAS(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	op, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "create"}, "data", 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransitionOperation(context.Background(), op.ID, op.Revision, OperationTransition{State: OperationSucceeded, Phase: PhaseCompleted, Settled: true, Result: []byte("guess")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("skipped initialization: %v", err)
	}
	op, err = s.TransitionOperation(context.Background(), op.ID, op.Revision, OperationTransition{State: OperationRunning, Phase: PhaseInitializing})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransitionOperation(context.Background(), op.ID, op.Revision, OperationTransition{State: OperationRunning, Phase: PhaseBound}); !errors.Is(err, ErrConflict) {
		t.Fatalf("bound without proof: %v", err)
	}
	if _, err := s.TransitionOperation(context.Background(), op.ID, op.Revision-1, OperationTransition{State: OperationRunning, Phase: PhaseBound, VerifiedRootProof: []byte("proof")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale CAS: %v", err)
	}
	op, err = s.TransitionOperation(context.Background(), op.ID, op.Revision, OperationTransition{State: OperationRunning, Phase: PhaseBound, VerifiedRootProof: []byte("proof")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransitionOperation(context.Background(), op.ID, op.Revision, OperationTransition{State: OperationRunning, Phase: PhaseBound, VerifiedRootProof: []byte("different")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("proof rewritten: %v", err)
	}
	for _, state := range []OperationState{OperationSucceeded, OperationRunning} {
		_, err := s.TransitionOperation(context.Background(), op.ID, op.Revision, OperationTransition{State: state, Phase: PhaseCompleted, Result: []byte(fmt.Sprint(state))})
		if err == nil {
			t.Fatalf("incomplete settlement accepted for %s", state)
		}
	}
}

func TestOperationCapacityRetainsFailedResponsibilityAndReceiptSlot(t *testing.T) {
	s, _ := authTestStore(t, func(options *Options) {
		options.MaxOperations = 1
		options.MaxTerminalReceipts = 2
	})
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	op, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "first"}, "first", 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "second"}, "second", 4096); !errors.Is(err, ErrCapacity) {
		t.Fatalf("pending operation budget exceeded: %v", err)
	}
	op, err = s.TransitionOperation(context.Background(), op.ID, op.Revision, OperationTransition{State: OperationFailed, Phase: PhaseAccepted, ErrorCode: "not_initialized"})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingOperations(context.Background(), "", 1)
	if err != nil || len(pending) != 1 || pending[0].ID != op.ID {
		t.Fatalf("failed responsibility missing from recovery: %#v %v", pending, err)
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "second"}, "second", 4096); !errors.Is(err, ErrCapacity) {
		t.Fatalf("failed responsibility budget freed early: %v", err)
	}
	if _, err := s.SettleOperation(context.Background(), op.ID, op.Revision, []byte("confirmed-no-effects")); err != nil {
		t.Fatal(err)
	}
	op, err = s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "second"}, "second", 4096)
	if err != nil {
		t.Fatal(err)
	}
	_ = actionTestFinishCreate(t, s, op)
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "third"}, "third", 4096); !errors.Is(err, ErrCapacity) {
		t.Fatalf("retained receipt budget exceeded: %v", err)
	}
}

func TestRetirementRequiresNativeEvidenceAndExactPlan(t *testing.T) {
	s, path := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	s.now = func() time.Time { return namespace.AcceptUntil.Add(8 * 24 * time.Hour) }
	candidate, err := s.RetirementCandidate(context.Background(), actor.ID)
	if err != nil || candidate.Floor != namespace.Sequence {
		t.Fatalf("eligible candidate: %#v %v", candidate, err)
	}
	for _, check := range []struct {
		plan     NamespaceRetirement
		revision int64
		proof    []byte
	}{
		{candidate, candidate.NativeRevision, nil},
		{candidate, candidate.NativeRevision + 1, []byte("native")},
		{func() NamespaceRetirement { altered := candidate; altered.Floor++; return altered }(), candidate.NativeRevision, []byte("native")},
	} {
		if _, err := s.ConfirmRetirement(context.Background(), actor.ID, check.plan, check.revision, check.proof); !errors.Is(err, ErrConflict) {
			t.Fatalf("unconfirmed/mismatched retirement accepted: %v", err)
		}
	}
	if _, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "unknown"}, "unknown", 4096); !errors.Is(err, ErrExpired) {
		t.Fatalf("candidate removed history before native confirmation: %v", err)
	}
	if floor, err := s.ConfirmRetirement(context.Background(), actor.ID, candidate, candidate.NativeRevision, []byte("native-confirmed-before-floor-plus-one")); err != nil || floor != candidate.Floor {
		t.Fatalf("confirmed retirement: %d %v", floor, err)
	}
	if _, err := s.ConfirmRetirement(context.Background(), actor.ID, candidate, candidate.NativeRevision, []byte("native-confirmed-before-floor-plus-one")); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale plan replay: %v", err)
	}
	identity := s.DatabaseID()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	options := s.options
	options.Create, options.ExpectedDatabaseID = false, identity
	opened, err := Open(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	opened.now = func() time.Time { return namespace.AcceptUntil.Add(-time.Hour) }
	if _, err := opened.GetNamespace(context.Background(), actor.ID, namespace.Sequence); !errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("retired namespace registration returned a window: %v", err)
	}
	if _, err := opened.CreateVolume(context.Background(), actor.ID, ActionKey{namespace.Sequence, "unknown"}, "unknown", 4096); !errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("restart/rollback reopened retired namespace: %v", err)
	}
	var nativeRevision int64
	var proof []byte
	if err := opened.read.QueryRowContext(context.Background(), "SELECT retired_native_revision,retired_native_proof FROM users WHERE id=?", actor.ID).Scan(&nativeRevision, &proof); err != nil || nativeRevision != candidate.NativeRevision || len(proof) <= 32 {
		t.Fatalf("native evidence not durable: %d %q %v", nativeRevision, proof, err)
	}
	floor, revision, evidence, err := opened.ActorRetirement(context.Background(), actor.ID)
	if err != nil || floor != candidate.Floor || revision != candidate.NativeRevision || string(evidence) != string(proof) {
		t.Fatalf("native retirement getter after reopen: %d %d %q %v", floor, revision, evidence, err)
	}
}

func TestNamespaceLookupsDistinguishAbsentFromUnavailable(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	if _, err := s.GetNamespace(context.Background(), actor.ID, namespace.Sequence+1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unissued namespace: %v", err)
	}
	absent, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetNamespace(context.Background(), absent, namespace.Sequence); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown actor namespace: %v", err)
	}
	if _, _, _, err := s.ActorRetirement(context.Background(), absent); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown actor retirement: %v", err)
	}
	if _, err := s.GetNamespace(context.Background(), actor.ID, 0); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid namespace: %v", err)
	}
	floor, revision, proof, err := s.ActorRetirement(context.Background(), actor.ID)
	if err != nil || floor != 0 || revision != 0 || len(proof) != 0 {
		t.Fatalf("initial retirement: %d %d %q %v", floor, revision, proof, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetNamespace(context.Background(), actor.ID, namespace.Sequence); err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("unavailable namespace became absence: %v", err)
	}
	if _, _, _, err := s.ActorRetirement(context.Background(), actor.ID); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("unavailable retirement became absence: %v", err)
	}
}

func TestNamespaceActorSweepIsUniqueAndBounded(t *testing.T) {
	s, _ := authTestStore(t, nil)
	owners := []User{authTestAdmin(t, s, "first"), authTestOrdinaryUser(t, s, "second"), authTestAdmin(t, s, "third")}
	var expected []ID
	for _, owner := range owners {
		expected = append(expected, owner.ID)
		_ = actionTestNamespace(t, s, owner.ID)
		_ = actionTestNamespace(t, s, owner.ID)
	}
	_ = authTestAdmin(t, s, "without-namespace")
	slices.Sort(expected)
	var actors []ID
	var after ID
	for {
		page, err := s.ListNamespaceActors(context.Background(), after, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		if len(page) != 1 || page[0] <= after {
			t.Fatalf("unbounded or repeated actor page: %v after %s", page, after)
		}
		actors = append(actors, page[0])
		after = page[0]
	}
	if !slices.Equal(actors, expected) {
		t.Fatalf("namespace actor sweep: %v, want %v", actors, expected)
	}
	for _, limit := range []int{0, s.options.MaxPageSize + 1} {
		if _, err := s.ListNamespaceActors(context.Background(), "", limit); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid actor page size %d: %v", limit, err)
		}
	}
	if _, err := s.write.ExecContext(context.Background(), "UPDATE action_namespaces SET actor_user_id=? WHERE actor_user_id=?", "malformed", owners[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListNamespaceActors(context.Background(), "", s.options.MaxPageSize); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("noncanonical actor published: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListNamespaceActors(context.Background(), "", 1); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("closed database became an empty sweep: %v", err)
	}
}

func TestNamespacesAllocateIndependentlyAndRetireIssuedPrefix(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "first")
	other := authTestOrdinaryUser(t, s, "second")
	first := actionTestNamespace(t, s, actor.ID)
	otherFirst := actionTestNamespace(t, s, other.ID)
	second := actionTestNamespace(t, s, actor.ID)
	otherSecond := actionTestNamespace(t, s, other.ID)
	third := actionTestNamespace(t, s, actor.ID)
	if first.Sequence != 1 || otherFirst.Sequence != 1 || second.Sequence != 2 || otherSecond.Sequence != 2 || third.Sequence != 3 {
		t.Fatalf("actors did not receive independent sequences: %v %v %v %v %v", first, otherFirst, second, otherSecond, third)
	}
	pending, err := s.CreateVolume(context.Background(), actor.ID, ActionKey{second.Sequence, "pending"}, "pending", 0)
	if err != nil {
		t.Fatal(err)
	}
	age := s.now().Add(8 * 24 * time.Hour)
	s.now = func() time.Time { return age }
	plan, err := s.RetirementCandidate(context.Background(), actor.ID)
	if err != nil || plan.Floor != first.Sequence {
		t.Fatalf("issued prefix crossed pending namespace: %#v %v", plan, err)
	}
	if floor, err := s.ConfirmRetirement(context.Background(), actor.ID, plan, plan.NativeRevision, []byte("native-before-2")); err != nil || floor != first.Sequence {
		t.Fatalf("first retirement: %d %v", floor, err)
	}
	_ = actionTestFinishCreate(t, s, pending)
	age = age.Add(8 * 24 * time.Hour)
	plan, err = s.RetirementCandidate(context.Background(), actor.ID)
	if err != nil || plan.ExpectedRetiredBefore != first.Sequence || plan.Floor != third.Sequence {
		t.Fatalf("issued prefix did not advance: %#v %v", plan, err)
	}
	if floor, err := s.ConfirmRetirement(context.Background(), actor.ID, plan, plan.NativeRevision, []byte("native-before-4")); err != nil || floor != third.Sequence {
		t.Fatalf("remaining retirement: %d %v", floor, err)
	}
	if _, err := s.GetNamespace(context.Background(), other.ID, otherSecond.Sequence); err != nil {
		t.Fatalf("another actor's issued namespace removed: %v", err)
	}
	fresh := actionTestNamespace(t, s, actor.ID)
	if fresh.Sequence != third.Sequence+1 {
		t.Fatalf("receipt GC reset the actor's issuer: %v", fresh)
	}
}

func TestNamespaceIssuerSerializesConcurrentAllocations(t *testing.T) {
	s, path := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	results := make(chan ActionNamespace, 16)
	errorsOut := make(chan error, 16)
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			namespace, err := s.IssueNamespace(context.Background(), actor.ID)
			results <- namespace
			errorsOut <- err
		})
	}
	group.Wait()
	close(results)
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
	var sequences []int64
	for namespace := range results {
		if namespace.Actor != actor.ID {
			t.Fatalf("issuance changed actor: %v", namespace)
		}
		sequences = append(sequences, namespace.Sequence)
	}
	slices.Sort(sequences)
	for i, sequence := range sequences {
		if sequence != int64(i+1) {
			t.Fatalf("duplicate or missing issuance: %v", sequences)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	options := s.options
	options.Create = false
	opened, err := Open(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	namespace, err := opened.IssueNamespace(context.Background(), actor.ID)
	if err != nil || namespace.Sequence != 17 {
		t.Fatalf("issuer counter did not survive reopen: %v %v", namespace, err)
	}
}

func TestNamespaceIssuerBoundsPreserveAllocationCounter(t *testing.T) {
	s, _ := authTestStore(t, func(options *Options) {
		options.MaxNamespaces = 2
		options.MaxNamespacesPerUser = 1
	})
	actor := authTestAdmin(t, s, "actor")
	other := authTestAdmin(t, s, "other")
	third := authTestAdmin(t, s, "third")
	_ = actionTestNamespace(t, s, actor.ID)
	if _, err := s.IssueNamespace(context.Background(), actor.ID); !errors.Is(err, ErrCapacity) {
		t.Fatalf("per-actor namespace bound exceeded: %v", err)
	}
	_ = actionTestNamespace(t, s, other.ID)
	if _, err := s.IssueNamespace(context.Background(), third.ID); !errors.Is(err, ErrCapacity) {
		t.Fatalf("global namespace bound exceeded: %v", err)
	}
	var next int64
	if err := s.read.QueryRowContext(context.Background(), "SELECT next_action_namespace FROM users WHERE id=?", third.ID).Scan(&next); err != nil || next != 1 {
		t.Fatalf("refused issuance consumed a sequence: %d %v", next, err)
	}
}

func TestNamespaceIssuerExhaustionAndUnavailableStateFailExplicitly(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	if _, err := s.write.ExecContext(context.Background(), "UPDATE users SET next_action_namespace=? WHERE id=?", math.MaxInt64, actor.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IssueNamespace(context.Background(), actor.ID); !errors.Is(err, ErrCapacity) {
		t.Fatalf("namespace counter overflowed: %v", err)
	}
	if _, err := s.IssueNamespace(context.Background(), ID("00000000000000000000000000000000")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unregistered actor issued a namespace: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.IssueNamespace(ctx, actor.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled issuer became capacity: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IssueNamespace(context.Background(), actor.ID); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed issuer returned success: %v", err)
	}
}

func participationTestVolume(t *testing.T, s *Store, actor ID, namespace ActionNamespace, nonce string) Volume {
	t.Helper()
	operation, err := s.CreateVolume(context.Background(), actor, ActionKey{namespace.Sequence, nonce}, nonce, 0)
	if err != nil {
		t.Fatal(err)
	}
	operation = actionTestFinishCreate(t, s, operation)
	volume, err := s.GetVolume(context.Background(), actor, operation.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	return volume
}

func TestNamespaceParticipationIsBoundedOwnedAndImmutable(t *testing.T) {
	s, path := authTestStore(t, func(options *Options) { options.MaxNamespaceParticipants = 1 })
	actor := authTestAdmin(t, s, "actor")
	other := authTestAdmin(t, s, "other")
	namespace := actionTestNamespace(t, s, actor.ID)
	first := participationTestVolume(t, s, actor.ID, namespace, "first")
	second := participationTestVolume(t, s, actor.ID, namespace, "second")
	if err := s.RegisterNamespaceParticipation(context.Background(), actor.ID, namespace.Sequence, first.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterNamespaceParticipation(context.Background(), actor.ID, namespace.Sequence, second.ID); !errors.Is(err, ErrCapacity) {
		t.Fatalf("participant bound exceeded: %v", err)
	}
	if err := s.RegisterNamespaceParticipation(context.Background(), other.ID, namespace.Sequence, first.ID); !errors.Is(err, ErrDenied) {
		t.Fatalf("administrator registered another owner's authority: %v", err)
	}
	var created int64
	if err := s.read.QueryRowContext(context.Background(), "SELECT created_at FROM namespace_volume_participants WHERE actor_user_id=? AND sequence=? AND volume_id=?", actor.ID, namespace.Sequence, first.ID).Scan(&created); err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return namespace.AcceptUntil }
	if err := s.RegisterNamespaceParticipation(context.Background(), actor.ID, namespace.Sequence, first.ID); err != nil {
		t.Fatalf("known expired participant failed: %v", err)
	}
	if err := s.RegisterNamespaceParticipation(context.Background(), actor.ID, namespace.Sequence, second.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("unknown expired participant registered: %v", err)
	}
	s.now = func() time.Time { return namespace.AcceptUntil.Add(-time.Hour) }
	if err := s.RegisterNamespaceParticipation(context.Background(), actor.ID, namespace.Sequence, second.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("clock rollback reopened participant registration: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	options := s.options
	options.Create = false
	opened, err := Open(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if err := opened.RegisterNamespaceParticipation(context.Background(), actor.ID, namespace.Sequence, first.ID); err != nil {
		t.Fatalf("known participant lost after reopen: %v", err)
	}
	var after int64
	if err := opened.read.QueryRowContext(context.Background(), "SELECT created_at FROM namespace_volume_participants WHERE actor_user_id=? AND sequence=? AND volume_id=?", actor.ID, namespace.Sequence, first.ID).Scan(&after); err != nil || after != created {
		t.Fatalf("idempotent registration rewrote immutable timestamp: %d != %d %v", after, created, err)
	}
	window, err := opened.GetNamespace(context.Background(), actor.ID, namespace.Sequence)
	if err != nil || !window.AcceptUntil.Equal(namespace.AcceptUntil) {
		t.Fatalf("registration renewed immutable window: %#v %v", window, err)
	}
}

func TestNamespaceRetirementRequiresExactParticipantMembership(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	creation := actionTestNamespace(t, s, actor.ID)
	volume := participationTestVolume(t, s, actor.ID, creation, "data")
	first := actionTestNamespace(t, s, actor.ID)
	second := actionTestNamespace(t, s, actor.ID)
	if err := s.RegisterNamespaceParticipation(context.Background(), actor.ID, first.Sequence, volume.ID); err != nil {
		t.Fatal(err)
	}
	age := s.now().Add(8 * 24 * time.Hour)
	s.now = func() time.Time { return age }
	plan, err := s.RetirementCandidate(context.Background(), actor.ID)
	if err != nil || plan.Floor != second.Sequence || len(plan.VolumeIDs) != 1 || plan.VolumeIDs[0] != volume.ID || len(plan.Participants) != 1 || plan.Participants[0] != (NamespaceParticipation{Sequence: first.Sequence, VolumeID: volume.ID}) {
		t.Fatalf("participant-aware candidate: %#v %v", plan, err)
	}
	// This simulates a late registration acknowledged outside the plan snapshot.
	// The authority volume set stays equal, but the native evidence is stale.
	if _, err := s.write.ExecContext(context.Background(), "INSERT INTO namespace_volume_participants(actor_user_id,sequence,volume_id,created_at) VALUES(?,?,?,?)", actor.ID, second.Sequence, volume.ID, age.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ConfirmRetirement(context.Background(), actor.ID, plan, plan.NativeRevision, []byte("old-native-proof")); !errors.Is(err, ErrConflict) {
		t.Fatalf("unchanged volume union hid a late participant: %v", err)
	}
	fresh, err := s.RetirementCandidate(context.Background(), actor.ID)
	if err != nil || !slices.Equal(fresh.VolumeIDs, plan.VolumeIDs) || len(fresh.Participants) != 2 || fresh.Digest == plan.Digest {
		t.Fatalf("participant membership not included in digest: %#v %v", fresh, err)
	}
	if _, err := s.ConfirmRetirement(context.Background(), actor.ID, fresh, fresh.NativeRevision, []byte("fresh-native-proof")); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.read.QueryRowContext(context.Background(), "SELECT count(*) FROM namespace_volume_participants WHERE actor_user_id=?", actor.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("retired participant details were not reclaimed atomically: %d %v", count, err)
	}
	if err := s.RegisterNamespaceParticipation(context.Background(), actor.ID, first.Sequence, volume.ID); !errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("retired participant was recreated: %v", err)
	}
}

func TestNamespaceParticipationAloneAddsRetirementAuthority(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	creation := actionTestNamespace(t, s, actor.ID)
	volume := participationTestVolume(t, s, actor.ID, creation, "data")
	age := s.now().Add(8 * 24 * time.Hour)
	s.now = func() time.Time { return age }
	if _, err := actionTestRetire(t, s, actor.ID); err != nil {
		t.Fatal(err)
	}
	namespace, err := s.IssueNamespace(context.Background(), actor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterNamespaceParticipation(context.Background(), actor.ID, namespace.Sequence, volume.ID); err != nil {
		t.Fatal(err)
	}
	age = age.Add(8 * 24 * time.Hour)
	plan, err := s.RetirementCandidate(context.Background(), actor.ID)
	if err != nil || plan.Floor != namespace.Sequence || len(plan.VolumeIDs) != 1 || plan.VolumeIDs[0] != volume.ID {
		t.Fatalf("registration without a management operation omitted its authority: %#v %v", plan, err)
	}
}

func TestNativeNamespaceAdmissionPreservesExpiryAcrossRollback(t *testing.T) {
	s, path := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	if err := s.CheckNamespaceAdmission(context.Background(), actor.ID, namespace.Sequence); err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return namespace.AcceptUntil }
	if err := s.CheckNamespaceAdmission(context.Background(), actor.ID, namespace.Sequence); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired native admission: %v", err)
	}
	s.now = func() time.Time { return namespace.AcceptUntil.Add(-time.Hour) }
	if err := s.CheckNamespaceAdmission(context.Background(), actor.ID, namespace.Sequence); !errors.Is(err, ErrExpired) {
		t.Fatalf("wall rollback reopened native admission: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	options := s.options
	options.Create = false
	opened, err := Open(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	opened.now = s.now
	if err := opened.CheckNamespaceAdmission(context.Background(), actor.ID, namespace.Sequence); !errors.Is(err, ErrExpired) {
		t.Fatalf("reopen lost logical expiry frontier: %v", err)
	}
}

func TestNativeNamespaceAdmissionPreservesCancellationAndDatabaseErrors(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	for _, invalid := range []int64{0, -1, math.MaxInt64} {
		if err := s.CheckNamespaceAdmission(context.Background(), actor.ID, invalid); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid native namespace sequence %d: %v", invalid, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.CheckNamespaceAdmission(ctx, actor.ID, namespace.Sequence); !errors.Is(err, context.Canceled) || errors.Is(err, ErrExpired) || errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("canceled admission became expiry: %v", err)
	}
	if err := s.CheckNamespaceAdmission(context.Background(), actor.ID, namespace.Sequence+1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unissued namespace: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckNamespaceAdmission(context.Background(), actor.ID, namespace.Sequence); !errors.Is(err, ErrClosed) || errors.Is(err, ErrExpired) {
		t.Fatalf("closed database became expiry: %v", err)
	}
}

func TestKnownNamespaceParticipationRequiresNamespaceEvidence(t *testing.T) {
	s, _ := authTestStore(t, nil)
	actor := authTestAdmin(t, s, "actor")
	namespace := actionTestNamespace(t, s, actor.ID)
	volume := participationTestVolume(t, s, actor.ID, namespace, "data")
	if err := s.RegisterNamespaceParticipation(t.Context(), actor.ID, namespace.Sequence, volume.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.write.ExecContext(t.Context(), "DELETE FROM action_namespaces WHERE actor_user_id=? AND sequence=?", actor.ID, namespace.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterNamespaceParticipation(t.Context(), actor.ID, namespace.Sequence, volume.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("orphan participant replayed without namespace evidence: %v", err)
	}
}
