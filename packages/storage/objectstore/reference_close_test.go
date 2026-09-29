package objectstore

import (
	"context"
	"errors"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func closeAttempt(t *testing.T, generation uint64) storage.CloseAttempt {
	t.Helper()
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	return storage.CloseAttempt{Action: id, Generation: generation}
}

func closeEpochSession(t *testing.T) *fileSession {
	t.Helper()
	domain, err := advisory.New(advisory.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.History = 100 * time.Millisecond
	locks, err := domain.NewSession(options, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return &fileSession{storage: &Storage{}, domain: domain, locks: locks, options: options,
		cleanup: context.Background(), active: true, expires: time.Now().Add(options.Lease),
		actions: make(map[storage.FileActionID]*fileAction), closeActions: make(map[storage.FileActionID]*referenceCloseReceipt)}
}

func closeIDAtEpoch(t *testing.T, epoch, generation uint64) storage.CloseAttempt {
	t.Helper()
	id, err := storage.NewFileActionID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return storage.CloseAttempt{Action: id, Generation: generation}
}

func awaitNextCloseEpoch(t *testing.T, session *fileSession, prior uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		epoch, err := session.locks.Epoch(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if epoch > prior {
			return epoch
		}
		runtime.Gosched()
	}
	t.Fatal("native action epoch did not advance")
	return 0
}

func awaitNextCleanupEpoch(t *testing.T, session *fileSession, prior uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		epoch, err := session.locks.CloseEpoch(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if epoch > prior {
			return epoch
		}
		runtime.Gosched()
	}
	t.Fatal("retired cleanup epoch did not advance")
	return 0
}

func runCurrentClose(t *testing.T, state *referenceCloseState, session *fileSession, generation uint64,
	closeNative func() (storage.ReferenceCloseResult, error),
) (storage.CloseAttempt, storage.ReferenceCloseResult, error) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		epoch, err := session.locks.Epoch(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		attempt := closeIDAtEpoch(t, epoch, generation)
		result, err := state.run(t.Context(), session, attempt, closeNative)
		var missed *storage.CloseActionNotExecutedError
		if errors.As(err, &missed) {
			continue
		}
		return attempt, result, err
	}
	t.Fatal("could not admit a current native close action")
	return storage.CloseAttempt{}, storage.ReferenceCloseResult{}, nil
}

func TestReferenceCloseRetainsFailureReceiptAndAdvancesOnlyOnConfirmedFalse(t *testing.T) {
	session := actionTestSession(t, 1)
	state := referenceCloseState{next: 1}
	first, second := closeAttempt(t, 1), closeAttempt(t, 2)
	failure := errors.New("known finalization refusal")
	calls := 0
	closeNative := func() (storage.ReferenceCloseResult, error) {
		calls++
		if calls == 1 {
			return storage.ReferenceCloseResult{Determined: true}, failure
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	result, err := state.run(t.Context(), session, first, closeNative)
	if result.Released || !result.Determined || !errors.Is(err, failure) {
		t.Fatalf("first close = %+v, %v", result, err)
	}
	result, err = state.run(t.Context(), session, first, closeNative)
	if result.Released || !result.Determined || !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("same action replay = %+v, %v, calls %d", result, err, calls)
	}
	result, err = state.run(t.Context(), session, second, closeNative)
	if !result.Released || err != nil || calls != 2 {
		t.Fatalf("second attempt = %+v, %v, calls %d", result, err, calls)
	}
	session.mu.Lock()
	if retained := session.closeActions[first.Action]; retained != nil {
		retained.expires = time.Now().Add(-time.Second)
	}
	if retained := session.closeActions[second.Action]; retained != nil {
		retained.expires = time.Now().Add(-time.Second)
	}
	session.mu.Unlock()
	receipt, err := state.query(t.Context(), session, first)
	if err != nil || receipt.Outcome != storage.FileActionRetired {
		t.Fatalf("expired old generation = %+v, %v", receipt, err)
	}
	if _, err := state.run(t.Context(), session, first, closeNative); !errors.Is(err, syscall.ESTALE) || calls != 2 {
		t.Fatalf("expired action replay = %v, calls %d", err, calls)
	}
	if result, err := state.runImplicit(t.Context(), session, closeNative); !result.Released || err != nil || calls != 2 {
		t.Fatalf("legacy close after receipt expiry = %+v, %v, calls %d", result, err, calls)
	}
}

func TestUnknownCloseKeepsGenerationAndCanRecoverWithSameAction(t *testing.T) {
	session := actionTestSession(t, 1)
	state := referenceCloseState{next: 1}
	attempt := closeAttempt(t, 1)
	calls := 0
	closeNative := func() (storage.ReferenceCloseResult, error) {
		calls++
		if calls == 1 {
			return storage.ReferenceCloseResult{}, context.DeadlineExceeded
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	result, err := state.run(t.Context(), session, attempt, closeNative)
	if result.Determined || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unknown close = %+v, %v", result, err)
	}
	session.mu.Lock()
	session.closeActions[attempt.Action].expires = time.Now().Add(-time.Second)
	session.mu.Unlock()
	receipt, err := state.query(t.Context(), session, attempt)
	if err != nil || receipt.Outcome != storage.FileActionUnknown {
		t.Fatalf("unknown expired receipt = %+v, %v", receipt, err)
	}
	if _, err := state.run(t.Context(), session, closeAttempt(t, 2), closeNative); !errors.Is(err, syscall.ESTALE) || calls != 1 {
		t.Fatalf("new action before unknown recovery = %v, calls %d", err, calls)
	}
	result, err = state.runImplicit(t.Context(), session, closeNative)
	if !result.Released || err != nil || calls != 2 || state.implicit.Action != attempt.Action {
		t.Fatalf("internal recovery = %+v, %v, calls %d, implicit %+v", result, err, calls, state.implicit)
	}
}

func TestCloseRejectsExpiredActionIDReusedAtNewGeneration(t *testing.T) {
	session := closeEpochSession(t)
	state := referenceCloseState{next: 1}
	calls := 0
	closeNative := func() (storage.ReferenceCloseResult, error) {
		calls++
		if calls < 3 {
			return storage.ReferenceCloseResult{Determined: true}, syscall.EBUSY
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	first, result, err := runCurrentClose(t, &state, session, 1, closeNative)
	if !result.Determined || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("first result = %+v, %v", result, err)
	}
	firstEpoch, _ := first.Action.Epoch()
	awaitNextCloseEpoch(t, session, firstEpoch)
	_, result, err = runCurrentClose(t, &state, session, 2, closeNative)
	if !result.Determined || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("second result = %+v, %v", result, err)
	}
	session.mu.Lock()
	if retained := session.closeActions[first.Action]; retained != nil {
		retained.expires = time.Now().Add(-time.Second)
	}
	session.mu.Unlock()
	reused := storage.CloseAttempt{Action: first.Action, Generation: 3}
	_, err = state.run(t.Context(), session, reused, closeNative)
	var refused *storage.CloseActionNotExecutedError
	if !errors.As(err, &refused) || refused.CurrentEpoch <= firstEpoch || calls != 2 {
		t.Fatalf("old ID at next generation = %v, calls %d", err, calls)
	}
	if receipt, err := state.query(t.Context(), session, reused); err != nil || receipt.Outcome != storage.FileActionRetired {
		t.Fatalf("unadmitted reused ID = %+v, %v", receipt, err)
	}
	_, result, err = runCurrentClose(t, &state, session, 3, closeNative)
	if !result.Released || err != nil || calls != 3 {
		t.Fatalf("fresh current-epoch action = %+v, %v, calls %d", result, err, calls)
	}
}

func TestCloseEpochMissProvesNoEffectAndCanRemintSameGeneration(t *testing.T) {
	session := closeEpochSession(t)
	state := referenceCloseState{next: 1}
	oldEpoch, err := session.locks.Epoch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	current := awaitNextCloseEpoch(t, session, oldEpoch)
	stale := closeIDAtEpoch(t, oldEpoch, 1)
	calls := 0
	closeNative := func() (storage.ReferenceCloseResult, error) {
		calls++
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	_, err = state.run(t.Context(), session, stale, closeNative)
	var refused *storage.CloseActionNotExecutedError
	if !errors.As(err, &refused) || refused.CurrentEpoch < current || calls != 0 {
		t.Fatalf("missed epoch = %v, calls %d", err, calls)
	}
	if receipt, err := state.query(t.Context(), session, stale); err != nil || receipt.Outcome != storage.FileActionRetired {
		t.Fatalf("missed action query = %+v, %v", receipt, err)
	}
	_, result, err := runCurrentClose(t, &state, session, 1, closeNative)
	if !result.Released || err != nil || calls != 1 {
		t.Fatalf("same-generation remint = %+v, %v, calls %d", result, err, calls)
	}
	if receipt, err := state.query(t.Context(), session, stale); err != nil || receipt.Outcome != storage.FileActionRetired {
		t.Fatalf("missed action after remint = %+v, %v", receipt, err)
	}
}

func TestFutureCloseEpochCannotConsumeCleanupHistory(t *testing.T) {
	for _, retired := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "retired"}[retired], func(t *testing.T) {
			session := actionTestSession(t, 1)
			if retired {
				if err := session.locks.Retire(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			state := referenceCloseState{next: 1}
			current, err := session.locks.CloseEpoch(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			closeNative := func() (storage.ReferenceCloseResult, error) {
				calls++
				return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
			}
			for i := 0; i < 10; i++ {
				future := closeIDAtEpoch(t, current+1, 1)
				_, err := state.run(t.Context(), session, future, closeNative)
				var missed *storage.CloseActionNotExecutedError
				if !errors.Is(err, syscall.EINVAL) || errors.As(err, &missed) {
					t.Fatalf("future admission %d = %v", i, err)
				}
				if receipt, err := state.query(t.Context(), session, future); !errors.Is(err, syscall.EINVAL) || receipt.Outcome != 0 {
					t.Fatalf("future query %d = %+v, %v", i, receipt, err)
				}
			}
			if len(session.closeActions) != 0 || calls != 0 {
				t.Fatalf("future IDs consumed cleanup capacity: actions %d, calls %d", len(session.closeActions), calls)
			}
			valid := closeIDAtEpoch(t, current, 1)
			if result, err := state.run(t.Context(), session, valid, closeNative); !result.Released || err != nil || calls != 1 {
				t.Fatalf("valid close after future IDs = %+v, %v, calls %d", result, err, calls)
			}
		})
	}
}

func TestRepeatedStaleCloseProofsRetireWithoutConsumingCapacity(t *testing.T) {
	session := closeEpochSession(t)
	state := referenceCloseState{next: 1}
	oldEpoch, err := session.locks.CloseEpoch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := session.locks.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitNextCleanupEpoch(t, session, oldEpoch)
	calls := 0
	closeNative := func() (storage.ReferenceCloseResult, error) {
		calls++
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	for i := 0; i < 10; i++ {
		stale := closeIDAtEpoch(t, oldEpoch, 1)
		_, err := state.run(t.Context(), session, stale, closeNative)
		var missed *storage.CloseActionNotExecutedError
		if !errors.As(err, &missed) || missed.CurrentEpoch <= oldEpoch {
			t.Fatalf("stale attempt %d = %v", i, err)
		}
		receipt, err := state.query(t.Context(), session, stale)
		if err != nil || receipt.Outcome != storage.FileActionRetired {
			t.Fatalf("stale proof %d did not retire = %+v, %v", i, receipt, err)
		}
	}
	if len(session.closeActions) != 0 || calls != 0 {
		t.Fatalf("stale proof consumed cleanup capacity: actions %d, calls %d", len(session.closeActions), calls)
	}
	status, err := state.status(t.Context(), session)
	if err != nil || !status.Ready || status.NextGeneration != 1 {
		t.Fatalf("owner after stale proofs = %+v, %v", status, err)
	}
	valid := closeIDAtEpoch(t, status.CurrentEpoch, status.NextGeneration)
	if result, err := state.run(t.Context(), session, valid, closeNative); !result.Released || err != nil || calls != 1 {
		t.Fatalf("valid close after stale proofs = %+v, %v, calls %d", result, err, calls)
	}
}

type retireOnceNative struct {
	metastore.File
	calls int
}

type refuseCloseOnceNative struct {
	metastore.File
	closes int
}

func (*refuseCloseOnceNative) Retire(context.Context) error { return nil }

func (f *refuseCloseOnceNative) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	f.closes++
	if f.closes == 1 {
		return storage.ReferenceCloseResult{Determined: true}, syscall.EBUSY
	}
	return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
}

func (f *retireOnceNative) Retire(ctx context.Context) error {
	f.calls++
	if f.calls == 1 {
		return context.DeadlineExceeded
	}
	return nil
}

func (f *retireOnceNative) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
}

func TestSessionRetirementRecoversExplicitCloseAfterPreEffectRetireTimeout(t *testing.T) {
	session := actionTestSession(t, 2)
	session.cleanup = context.Background()
	session.files = make(map[retainedReference]struct{})
	native := &retireOnceNative{}
	reference := &openFile{session: session, native: native, active: true, closing: referenceCloseState{next: 1}}
	session.files[reference] = struct{}{}
	attempt := closeAttempt(t, 1)
	result, err := reference.CloseWithAction(t.Context(), attempt)
	if result.Determined || !errors.Is(err, context.DeadlineExceeded) || native.calls != 1 {
		t.Fatalf("pre-effect retire timeout = %+v, %v, calls %d", result, err, native.calls)
	}
	if result, err := session.CloseWithResult(t.Context()); !result.Released || err != nil {
		t.Fatalf("session retirement = %+v, %v", result, err)
	}
	if native.calls != 2 {
		t.Fatalf("native retire calls = %d", native.calls)
	}
	receipt, err := reference.QueryCloseAttempt(t.Context(), attempt)
	if err != nil || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("same-attempt recovery receipt = %+v, %v", receipt, err)
	}
}

func TestRetiredSessionMintsInternalCloseAfterLaterNativeEpoch(t *testing.T) {
	session := closeEpochSession(t)
	session.files = make(map[retainedReference]struct{})
	native := &refuseCloseOnceNative{}
	reference := &openFile{session: session, native: native, active: true, closing: referenceCloseState{next: 1}}
	session.files[reference] = struct{}{}
	firstEpoch, err := session.locks.Epoch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	secondEpoch := awaitNextCloseEpoch(t, session, firstEpoch)
	currentEpoch := awaitNextCloseEpoch(t, session, secondEpoch)
	_, result, err := runCurrentClose(t, &reference.closing, session, 1, reference.performClose)
	if !result.Determined || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("known first failure = %+v, %v", result, err)
	}
	if result, err := session.CloseWithResult(t.Context()); !result.Released || err != nil {
		t.Fatalf("retired-session internal cleanup = %+v, %v", result, err)
	}
	if native.closes != 2 || reference.closing.implicit == nil || reference.closing.implicit.Generation != 2 {
		t.Fatalf("cleanup attempt = %+v, native closes %d", reference.closing.implicit, native.closes)
	}
	if epoch, err := reference.closing.implicit.Action.Epoch(); err != nil || epoch < currentEpoch {
		t.Fatalf("internal cleanup epoch = %d, %v; prior %d", epoch, err, currentEpoch)
	}
}

func TestRetiredSessionAdmitsNextExplicitCloseAfterDeterminedFailure(t *testing.T) {
	session := closeEpochSession(t)
	state := referenceCloseState{next: 1}
	calls := 0
	closeNative := func() (storage.ReferenceCloseResult, error) {
		calls++
		if calls == 1 {
			return storage.ReferenceCloseResult{Determined: true}, syscall.EBUSY
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	first, result, err := runCurrentClose(t, &state, session, 1, closeNative)
	if !result.Determined || result.Released || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("first close = %+v, %v", result, err)
	}
	if err := session.locks.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := session.locks.History(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("ordinary history reopened after retirement: %v", err)
	}
	status, err := state.status(t.Context(), session)
	if err != nil || status.Released || !status.Ready || status.Current != nil || status.NextGeneration != 2 || status.CurrentEpoch == 0 {
		t.Fatalf("retired close owner status = %+v, %v", status, err)
	}
	second := closeIDAtEpoch(t, status.CurrentEpoch, status.NextGeneration)
	result, err = state.run(t.Context(), session, second, closeNative)
	if !result.Released || err != nil || calls != 2 {
		t.Fatalf("retired explicit retry = %+v, %v, calls %d", result, err, calls)
	}
	if receipt, err := state.query(t.Context(), session, first); err != nil || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("first failure receipt = %+v, %v", receipt, err)
	}
}

func TestFileAndNodeReferenceExposeBoundCloseStatusAfterRetirement(t *testing.T) {
	session := actionTestSession(t, 2)
	if err := session.locks.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	file := &openFile{session: session, closing: referenceCloseState{next: 1}}
	node := &nodeReference{session: session, closing: referenceCloseState{next: 1}}
	for name, reference := range map[string]storage.ReferenceCloseActions{"file": file, "node": node} {
		status, err := reference.CloseOwnerStatus(t.Context())
		if err != nil || !status.Ready || status.Released || status.Current != nil || status.NextGeneration != 1 || status.CurrentEpoch == 0 {
			t.Fatalf("%s close status = %+v, %v", name, status, err)
		}
	}
}

func TestRetiredInternalFailurePublishesNextGenerationToExternalOwner(t *testing.T) {
	session := closeEpochSession(t)
	state := referenceCloseState{next: 1}
	calls := 0
	closeNative := func() (storage.ReferenceCloseResult, error) {
		calls++
		if calls < 3 {
			return storage.ReferenceCloseResult{Determined: true}, syscall.EBUSY
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	if _, result, err := runCurrentClose(t, &state, session, 1, closeNative); !result.Determined || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("initial explicit failure = %+v, %v", result, err)
	}
	if err := session.locks.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if result, err := state.runImplicit(t.Context(), session, closeNative); !result.Determined || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("automatic retired cleanup = %+v, %v", result, err)
	}
	status, err := state.status(t.Context(), session)
	if err != nil || status.Released || status.Current != nil || status.NextGeneration != 3 || status.Ready {
		t.Fatalf("owner cursor after internal failure = %+v, %v", status, err)
	}
	session.mu.Lock()
	for _, receipt := range session.closeActions {
		if receipt.attempt.Generation == 1 {
			receipt.expires = time.Now().Add(-time.Second)
		}
	}
	session.mu.Unlock()
	status, err = state.status(t.Context(), session)
	if err != nil || !status.Ready || status.NextGeneration != 3 {
		t.Fatalf("owner cursor after oldest receipt expiry = %+v, %v", status, err)
	}
	third := closeIDAtEpoch(t, status.CurrentEpoch, status.NextGeneration)
	if result, err := state.run(t.Context(), session, third, closeNative); !result.Released || err != nil || calls != 3 {
		t.Fatalf("external continuation = %+v, %v, calls %d", result, err, calls)
	}
}

func TestRetiredUnknownInternalAttemptIsAdoptable(t *testing.T) {
	session := closeEpochSession(t)
	state := referenceCloseState{next: 1}
	calls := 0
	closeNative := func() (storage.ReferenceCloseResult, error) {
		calls++
		switch calls {
		case 1:
			return storage.ReferenceCloseResult{Determined: true}, syscall.EBUSY
		case 2:
			return storage.ReferenceCloseResult{}, context.DeadlineExceeded
		default:
			return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
		}
	}
	if _, result, err := runCurrentClose(t, &state, session, 1, closeNative); !result.Determined || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("initial failure = %+v, %v", result, err)
	}
	if err := session.locks.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if result, err := state.runImplicit(t.Context(), session, closeNative); result.Determined || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("internal unknown attempt = %+v, %v", result, err)
	}
	status, err := state.status(t.Context(), session)
	if err != nil || status.Ready || status.Current == nil || status.CurrentOutcome != storage.FileActionUnknown || status.NextGeneration != 2 {
		t.Fatalf("adoptable internal attempt = %+v, %v", status, err)
	}
	if result, err := state.run(t.Context(), session, *status.Current, closeNative); !result.Released || err != nil || calls != 3 {
		t.Fatalf("external same-ID adoption = %+v, %v, calls %d", result, err, calls)
	}
}

func TestRetiredCleanupEpochRejectsOldIDAfterReceiptExpiry(t *testing.T) {
	session := closeEpochSession(t)
	state := referenceCloseState{next: 1}
	calls := 0
	closeNative := func() (storage.ReferenceCloseResult, error) {
		calls++
		if calls < 3 {
			return storage.ReferenceCloseResult{Determined: true}, syscall.EBUSY
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	first, result, err := runCurrentClose(t, &state, session, 1, closeNative)
	if !result.Determined || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("first close = %+v, %v", result, err)
	}
	if err := session.locks.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	firstEpoch, _ := first.Action.Epoch()
	secondEpoch := awaitNextCleanupEpoch(t, session, firstEpoch)
	second := closeIDAtEpoch(t, secondEpoch, 2)
	if result, err := state.run(t.Context(), session, second, closeNative); !result.Determined || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retired second close = %+v, %v", result, err)
	}
	session.mu.Lock()
	if retained := session.closeActions[first.Action]; retained != nil {
		retained.expires = time.Now().Add(-time.Second)
	}
	session.mu.Unlock()
	reused := storage.CloseAttempt{Action: first.Action, Generation: 3}
	_, err = state.run(t.Context(), session, reused, closeNative)
	var missed *storage.CloseActionNotExecutedError
	if !errors.As(err, &missed) || missed.CurrentEpoch <= firstEpoch || calls != 2 {
		t.Fatalf("retired old-ID reuse = %v, calls %d", err, calls)
	}
	status, err := state.status(t.Context(), session)
	if err != nil || status.NextGeneration != 3 {
		t.Fatalf("retired cursor after old-ID refusal = %+v, %v", status, err)
	}
	third := closeIDAtEpoch(t, status.CurrentEpoch, status.NextGeneration)
	if result, err := state.run(t.Context(), session, third, closeNative); !result.Released || err != nil || calls != 3 {
		t.Fatalf("fresh retired action = %+v, %v, calls %d", result, err, calls)
	}
}

func TestCloseHistoryReservationReclaimsExpiredReleasedOwners(t *testing.T) {
	session := actionTestSession(t, 1)
	session.options.MaxFiles = 1
	session.options.MaxCloseActions = 3
	attempt := closeAttempt(t, 1)
	state := &referenceCloseState{released: true, next: 1}
	receipt := &referenceCloseReceipt{
		attempt: attempt, owner: state, outcome: storage.FileActionCompleted,
		expires: time.Now().Add(time.Hour),
	}
	state.receipts = map[uint64]*referenceCloseReceipt{1: receipt}
	session.closeRefs = map[*referenceCloseState]struct{}{state: {}}
	session.closeActions = map[storage.FileActionID]*referenceCloseReceipt{attempt.Action: receipt}
	if done, err := session.beginOpen(t.Context()); done != nil || !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("retained receipt allowed a new effectful open: %v", err)
	}
	session.mu.Lock()
	receipt.expires = time.Now().Add(-time.Second)
	session.mu.Unlock()
	done, err := session.beginOpen(t.Context())
	if err != nil {
		t.Fatalf("expired receipt did not free cleanup reservation: %v", err)
	}
	done()
}
