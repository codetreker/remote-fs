package replicated

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type actionFileProbe struct {
	*fileAuthorityStub
	attempts []storage.CloseAttempt
	result   func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error)
	status   func() (storage.CloseOwnerStatus, error)
	query    func(storage.CloseAttempt) (storage.FileActionReceipt, error)
}

func (p *actionFileProbe) CloseWithActionAndBarrier(_ context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	p.attempts = append(p.attempts, attempt)
	return p.result(len(p.attempts))
}

func (p *actionFileProbe) CloseWithAction(_ context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	result, _, err := p.CloseWithActionAndBarrier(context.Background(), attempt)
	return result, err
}

func (p *actionFileProbe) QueryCloseAttempt(_ context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	if p.query != nil {
		return p.query(attempt)
	}
	return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}, nil
}

func (p *actionFileProbe) CloseOwnerStatus(context.Context) (storage.CloseOwnerStatus, error) {
	if p.status != nil {
		return p.status()
	}
	next := uint64(1)
	for _, attempt := range p.attempts {
		if attempt.Generation >= next {
			next = attempt.Generation + 1
		}
	}
	return storage.CloseOwnerStatus{Ready: true, NextGeneration: next, CurrentEpoch: 1}, nil
}

type actionReferenceProbe struct {
	*barrierReferenceStub
	attempts []storage.CloseAttempt
	result   func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error)
	status   func() (storage.CloseOwnerStatus, error)
	query    func(storage.CloseAttempt) (storage.FileActionReceipt, error)
}

type confirmedSessionCloseProbe struct{ *fileSessionStub }

func (*confirmedSessionCloseProbe) CloseWithBarrier(context.Context) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
}

type determinedImplicitFileProbe struct{ *actionFileProbe }

type recoverableCloseSessionProbe struct {
	*fileSessionStub
	err error
}

func (p *recoverableCloseSessionProbe) CheckRecoverableReferenceClose() error { return p.err }

func (*determinedImplicitFileProbe) CloseWithBarrier(context.Context) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
}

func (p *actionReferenceProbe) CloseWithActionAndBarrier(_ context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	p.attempts = append(p.attempts, attempt)
	return p.result(len(p.attempts))
}

func (p *actionReferenceProbe) CloseWithAction(_ context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	result, _, err := p.CloseWithActionAndBarrier(context.Background(), attempt)
	return result, err
}

func (p *actionReferenceProbe) QueryCloseAttempt(_ context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	if p.query != nil {
		return p.query(attempt)
	}
	return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}, nil
}

func (p *actionReferenceProbe) CloseOwnerStatus(context.Context) (storage.CloseOwnerStatus, error) {
	if p.status != nil {
		return p.status()
	}
	next := uint64(1)
	for _, attempt := range p.attempts {
		if attempt.Generation >= next {
			next = attempt.Generation + 1
		}
	}
	return storage.CloseOwnerStatus{Ready: true, NextGeneration: next, CurrentEpoch: 1}, nil
}

func closeAttemptForTest(t *testing.T) storage.CloseAttempt {
	t.Helper()
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	return storage.CloseAttempt{Action: action, Generation: 1}
}

func TestRecoverableCloseCapabilityFollowsAuthoritySession(t *testing.T) {
	if err := (&fileSession{remote: &fileSessionStub{}}).CheckRecoverableReferenceClose(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing authority capability = %v", err)
	}
	probe := &recoverableCloseSessionProbe{fileSessionStub: &fileSessionStub{}}
	session := &fileSession{remote: probe}
	if err := session.CheckRecoverableReferenceClose(); err != nil {
		t.Fatalf("authority capability = %v", err)
	}
	probe.err = syscall.EIO
	if err := session.CheckRecoverableReferenceClose(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("authority capability failure = %v", err)
	}
}

func TestNodeCloseAttemptQueryIsReferenceBound(t *testing.T) {
	attempt := closeAttemptForTest(t)
	queries := 0
	probe := &actionReferenceProbe{barrierReferenceStub: &barrierReferenceStub{}}
	probe.query = func(requested storage.CloseAttempt) (storage.FileActionReceipt, error) {
		queries++
		if requested != attempt {
			t.Fatalf("query changed attempt: %+v", requested)
		}
		return storage.FileActionReceipt{Action: requested.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionUnknown}, nil
	}
	reference := &nodeReference{remote: probe}
	if receipt, err := reference.QueryCloseAttempt(t.Context(), attempt); err != nil || receipt.Action != attempt.Action || receipt.Outcome != storage.FileActionUnknown || queries != 1 {
		t.Fatalf("bound node query = %+v, %v, calls=%d", receipt, err, queries)
	}
	if _, err := reference.QueryCloseAttempt(t.Context(), storage.CloseAttempt{}); !errors.Is(err, syscall.EINVAL) || queries != 1 {
		t.Fatalf("invalid node query = %v, calls=%d", err, queries)
	}
	missing := &nodeReference{remote: &barrierReferenceStub{}}
	if _, err := missing.QueryCloseAttempt(t.Context(), attempt); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing bound node query = %v", err)
	}
}

func TestRetiredSessionDoesNotInventFileRelease(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.mu.Lock()
	session.closed = true
	session.mu.Unlock()
	unknown := errors.New("close response lost")
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		switch call {
		case 1:
			return storage.ReferenceCloseResult{}, nil, unknown
		case 2:
			return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
		default:
			return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
		}
	}
	file := &retainedFile{session: session, remote: probe}
	first, next := closeAttemptForTest(t), closeAttemptForTest(t)
	next.Generation = 2
	if result, err := file.CloseWithAction(t.Context(), first); result.Released || result.Determined || !errors.Is(err, unknown) || file.closed {
		t.Fatalf("unknown close = %+v, %v, closed=%t", result, err, file.closed)
	}
	if result, err := file.CloseWithAction(t.Context(), next); result.Released || !errors.Is(err, syscall.EBUSY) || len(probe.attempts) != 1 {
		t.Fatalf("new action before settlement = %+v, %v, calls=%d", result, err, len(probe.attempts))
	}
	if result, err := file.CloseWithAction(t.Context(), first); result.Released || !result.Determined || !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("determined retained close = %+v, %v", result, err)
	}
	if result, err := file.CloseWithAction(t.Context(), next); !result.Released || !result.Determined || err != nil || !file.closed {
		t.Fatalf("next close = %+v, %v, closed=%t", result, err, file.closed)
	}
	if len(probe.attempts) != 3 || probe.attempts[0] != first || probe.attempts[1] != first || probe.attempts[2] != next {
		t.Fatalf("close attempt sequence = %+v", probe.attempts)
	}
	if receipt, err := file.QueryCloseAttempt(t.Context(), first); err != nil || receipt.Action != first.Action {
		t.Fatalf("bound close query = %+v, %v", receipt, err)
	}
}

func TestRetiredSessionDoesNotInventNodeReferenceRelease(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.mu.Lock()
	session.closed = true
	session.mu.Unlock()
	unknown := errors.New("reference response lost")
	probe := &actionReferenceProbe{barrierReferenceStub: &barrierReferenceStub{}}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if call == 1 {
			return storage.ReferenceCloseResult{}, nil, unknown
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	reference := &nodeReference{session: session, remote: probe}
	attempt := closeAttemptForTest(t)
	if result, err := reference.CloseWithAction(t.Context(), attempt); result.Released || result.Determined || !errors.Is(err, unknown) || reference.closed {
		t.Fatalf("unknown reference close = %+v, %v, closed=%t", result, err, reference.closed)
	}
	if result, err := reference.CloseWithAction(t.Context(), attempt); !result.Released || !result.Determined || err != nil || !reference.closed {
		t.Fatalf("settled reference close = %+v, %v, closed=%t", result, err, reference.closed)
	}
	if len(probe.attempts) != 2 || probe.attempts[0] != attempt || probe.attempts[1] != attempt {
		t.Fatalf("reference attempts = %+v", probe.attempts)
	}
}

func TestFileOldCloseAttemptReplaysWhileNewAttemptIsUnknown(t *testing.T) {
	session := retainedTestSession(t, nil)
	first := closeAttemptForTest(t)
	second := closeAttemptForTest(t)
	second.Generation = 2
	third := closeAttemptForTest(t)
	third.Generation = 3
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		attempt := probe.attempts[call-1]
		if attempt == first {
			return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
		}
		if call == 2 {
			return storage.ReferenceCloseResult{}, nil, syscall.EIO
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	if result, err := file.CloseWithAction(t.Context(), first); result.Released || !result.Determined || !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("first close = %+v, %v", result, err)
	}
	if result, err := file.CloseWithAction(t.Context(), second); result.Released || result.Determined || !errors.Is(err, syscall.EIO) {
		t.Fatalf("second unknown close = %+v, %v", result, err)
	}
	if result, err := file.CloseWithAction(t.Context(), first); result.Released || !result.Determined || !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("historical replay = %+v, %v", result, err)
	}
	if file.closeAttempt != second || file.closeResult.Determined {
		t.Fatalf("historical replay replaced active close: attempt=%+v result=%+v", file.closeAttempt, file.closeResult)
	}
	if _, err := file.CloseWithAction(t.Context(), third); !errors.Is(err, syscall.EBUSY) || len(probe.attempts) != 3 {
		t.Fatalf("new attempt during unknown second close = %v, calls=%d", err, len(probe.attempts))
	}
	if result, err := file.CloseWithAction(t.Context(), second); !result.Released || !result.Determined || err != nil {
		t.Fatalf("second close reconciliation = %+v, %v", result, err)
	}
}

func TestNodeOldCloseAttemptReplaysWhileNewAttemptIsUnknown(t *testing.T) {
	session := retainedTestSession(t, nil)
	first := closeAttemptForTest(t)
	second := closeAttemptForTest(t)
	second.Generation = 2
	probe := &actionReferenceProbe{barrierReferenceStub: &barrierReferenceStub{}}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if probe.attempts[call-1] == first {
			return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
		}
		return storage.ReferenceCloseResult{}, nil, syscall.EIO
	}
	reference := &nodeReference{session: session, remote: probe}
	if _, err := reference.CloseWithAction(t.Context(), first); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	if _, err := reference.CloseWithAction(t.Context(), second); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if result, err := reference.CloseWithAction(t.Context(), first); result.Released || !result.Determined || !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("historical reference replay = %+v, %v", result, err)
	}
	if reference.closeAttempt != second || reference.closeResult.Determined {
		t.Fatalf("historical replay replaced active reference close: attempt=%+v result=%+v", reference.closeAttempt, reference.closeResult)
	}
}

func TestParentSessionReleaseProofForLegacyFileClose(t *testing.T) {
	for _, test := range []struct {
		name   string
		parent storage.ReferenceCloseResult
		proof  bool
	}{
		{name: "confirmed", parent: storage.ReferenceCloseResult{Released: true, Determined: true}, proof: true},
		{name: "unknown", parent: storage.ReferenceCloseResult{Released: true}},
		{name: "unreleased", parent: storage.ReferenceCloseResult{Determined: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := retainedTestSession(t, nil)
			session.mu.Lock()
			session.closed = true
			session.closeResult = test.parent
			session.mu.Unlock()
			calls := 0
			file := &retainedFile{session: session, remote: &fileAuthorityStub{close: func(context.Context) error {
				calls++
				return syscall.EIO
			}}}
			result, err := file.CloseWithResult(t.Context())
			if test.proof {
				if !result.Released || !result.Determined || err != nil || calls != 0 {
					t.Fatalf("confirmed parent close = %+v, %v, remote calls=%d", result, err, calls)
				}
			} else if result.Released || result.Determined || !errors.Is(err, syscall.EIO) || calls != 1 {
				t.Fatalf("unproved parent close = %+v, %v, remote calls=%d", result, err, calls)
			}
		})
	}
}

func TestParentSessionReleaseProofForLegacyNodeClose(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.mu.Lock()
	session.closed = true
	session.closeResult = storage.ReferenceCloseResult{Released: true, Determined: true}
	session.mu.Unlock()
	remote := &barrierReferenceStub{closeErr: syscall.EIO}
	reference := &nodeReference{session: session, remote: remote}
	result, err := reference.CloseWithResult(t.Context())
	if !result.Released || !result.Determined || err != nil || remote.closes != 0 {
		t.Fatalf("confirmed parent reference close = %+v, %v, remote calls=%d", result, err, remote.closes)
	}
}

func TestRecoverableCloseRejectsIncompleteRemoteCapabilityBeforeEffect(t *testing.T) {
	session := retainedTestSession(t, nil)
	attempt := closeAttemptForTest(t)
	fileCalls := 0
	file := &retainedFile{session: session, remote: &fileAuthorityStub{close: func(context.Context) error {
		fileCalls++
		return nil
	}}}
	if _, err := file.CloseWithAction(t.Context(), attempt); !errors.Is(err, syscall.EOPNOTSUPP) || fileCalls != 0 {
		t.Fatalf("file incomplete capability = %v, calls=%d", err, fileCalls)
	}
	referenceRemote := &barrierReferenceStub{}
	reference := &nodeReference{session: session, remote: referenceRemote}
	if _, err := reference.CloseWithAction(t.Context(), attempt); !errors.Is(err, syscall.EOPNOTSUPP) || referenceRemote.closes != 0 {
		t.Fatalf("reference incomplete capability = %v, calls=%d", err, referenceRemote.closes)
	}
}

func closeEpochAttemptsForTest(t *testing.T) (storage.CloseAttempt, storage.CloseAttempt, storage.CloseAttempt) {
	t.Helper()
	old := closeAttemptForTest(t)
	currentAction, err := storage.NewFileActionID(2)
	if err != nil {
		t.Fatal(err)
	}
	otherAction, err := storage.NewFileActionID(2)
	if err != nil {
		t.Fatal(err)
	}
	return old,
		storage.CloseAttempt{Action: currentAction, Generation: old.Generation},
		storage.CloseAttempt{Action: otherAction, Generation: old.Generation}
}

func TestFutureCloseIDsDoNotConsumeWrapperHistory(t *testing.T) {
	for _, kind := range []string{"file", "reference"} {
		t.Run(kind, func(t *testing.T) {
			session := retainedTestSession(t, nil)
			session.maxCloseActions = 1
			status := func() (storage.CloseOwnerStatus, error) {
				return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
			}
			queries := 0
			query := func(attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
				queries++
				return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}, nil
			}
			result := func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
				return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
			}
			var closer storage.ReferenceCloseActions
			var state *retainedClose
			var attempts func() []storage.CloseAttempt
			if kind == "file" {
				probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}, status: status, query: query, result: result}
				file := &retainedFile{session: session, remote: probe}
				closer, state = file, &file.retainedClose
				attempts = func() []storage.CloseAttempt { return probe.attempts }
			} else {
				probe := &actionReferenceProbe{barrierReferenceStub: &barrierReferenceStub{}, status: status, query: query, result: result}
				reference := &nodeReference{session: session, remote: probe}
				closer, state = reference, &reference.retainedClose
				attempts = func() []storage.CloseAttempt { return probe.attempts }
			}
			for epoch := uint64(2); epoch < 10; epoch++ {
				action, err := storage.NewFileActionID(epoch)
				if err != nil {
					t.Fatal(err)
				}
				future := storage.CloseAttempt{Action: action, Generation: 1}
				if response, err := closer.CloseWithAction(t.Context(), future); response.Released || !errors.Is(err, syscall.EINVAL) {
					t.Fatalf("future action at epoch %d = %+v, %v", epoch, response, err)
				}
				if state.closeAttemptSet || session.closeReservations != 0 || session.closeTombstones != 0 || len(attempts()) != 0 || queries != 0 {
					t.Fatalf("future action consumed state: attempt=%t active=%d old=%d calls=%d queries=%d", state.closeAttemptSet, session.closeReservations, session.closeTombstones, len(attempts()), queries)
				}
			}
			valid := closeAttemptForTest(t)
			if response, err := closer.CloseWithAction(t.Context(), valid); !response.Released || !response.Determined || err != nil || len(attempts()) != 1 || queries != 1 {
				t.Fatalf("valid close after future IDs = %+v, %v, calls=%d, queries=%d", response, err, len(attempts()), queries)
			}
		})
	}
}

func TestFileCloseEpochProofAllowsOnlyCallerReplacement(t *testing.T) {
	session := retainedTestSession(t, nil)
	old, replacement, unproven := closeEpochAttemptsForTest(t)
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 2}, nil
	}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if probe.attempts[call-1] == old && call == 2 {
			return storage.ReferenceCloseResult{}, nil, syscall.EIO
		}
		if probe.attempts[call-1] == old {
			return storage.ReferenceCloseResult{}, nil, &storage.CloseActionNotExecutedError{CurrentEpoch: 2}
		}
		if call == 3 {
			return storage.ReferenceCloseResult{}, nil, syscall.EIO
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	if result, err := file.CloseWithAction(t.Context(), old); result.Released || !session.hasCloseTombstone(&file.retainedClose, old) {
		t.Fatalf("old action proof = %+v, %v, tombstone=%t", result, err, session.hasCloseTombstone(&file.retainedClose, old))
	} else {
		var proof *storage.CloseActionNotExecutedError
		if !errors.As(err, &proof) || proof.CurrentEpoch != 2 {
			t.Fatalf("typed close proof = %v", err)
		}
	}
	if _, err := file.CloseWithAction(t.Context(), old); !errors.Is(err, syscall.EIO) || file.closeCurrentEpoch != 2 {
		t.Fatalf("old action transport failure lost prior proof: %v, epoch=%d", err, file.closeCurrentEpoch)
	}
	if result, err := file.CloseWithAction(t.Context(), replacement); result.Released || !errors.Is(err, syscall.EIO) || file.closeAttempt != replacement {
		t.Fatalf("caller replacement = %+v, %v, active=%+v", result, err, file.closeAttempt)
	}
	if result, err := file.CloseWithAction(t.Context(), old); result.Released || file.closeAttempt != replacement {
		t.Fatalf("old action replay = %+v, %v, active=%+v", result, err, file.closeAttempt)
	} else {
		var proof *storage.CloseActionNotExecutedError
		if !errors.As(err, &proof) {
			t.Fatalf("old action lost proof: %v", err)
		}
	}
	if _, err := file.CloseWithAction(t.Context(), unproven); !errors.Is(err, syscall.EINVAL) || len(probe.attempts) != 4 {
		t.Fatalf("unproven same-generation action = %v, calls=%d", err, len(probe.attempts))
	}
	if result, err := file.CloseWithAction(t.Context(), replacement); !result.Released || !result.Determined || err != nil {
		t.Fatalf("replacement reconciliation = %+v, %v", result, err)
	}
	if len(probe.attempts) != 5 {
		t.Fatalf("action count = %d", len(probe.attempts))
	}
}

func TestNodeCloseEpochProofAllowsOnlyCallerReplacement(t *testing.T) {
	session := retainedTestSession(t, nil)
	old, replacement, unproven := closeEpochAttemptsForTest(t)
	probe := &actionReferenceProbe{barrierReferenceStub: &barrierReferenceStub{}}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 2}, nil
	}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if probe.attempts[call-1] == old {
			return storage.ReferenceCloseResult{}, nil, &storage.CloseActionNotExecutedError{CurrentEpoch: 2}
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	reference := &nodeReference{session: session, remote: probe}
	if _, err := reference.CloseWithAction(t.Context(), old); err == nil || !session.hasCloseTombstone(&reference.retainedClose, old) {
		t.Fatalf("node proof = %v, tombstone=%t", err, session.hasCloseTombstone(&reference.retainedClose, old))
	}
	if result, err := reference.CloseWithAction(t.Context(), replacement); !result.Released || !result.Determined || err != nil {
		t.Fatalf("node caller replacement = %+v, %v", result, err)
	}
	if result, err := reference.CloseWithAction(t.Context(), old); result.Released || err == nil || reference.closeAttempt != replacement {
		t.Fatalf("node old proof replay = %+v, %v, active=%+v", result, err, reference.closeAttempt)
	}
	if _, err := reference.CloseWithAction(t.Context(), unproven); !errors.Is(err, syscall.EINVAL) || len(probe.attempts) != 3 {
		t.Fatalf("node unproven action = %v, calls=%d", err, len(probe.attempts))
	}
}

func TestExplicitCloseCannotAdoptImplicitRelease(t *testing.T) {
	session := retainedTestSession(t, nil)
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{close: func(context.Context) error { return nil }}}
	probe.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		t.Fatal("explicit action reached authority after implicit close")
		return storage.ReferenceCloseResult{}, nil, nil
	}
	file := &retainedFile{session: session, remote: probe}
	if result, err := file.CloseWithResult(t.Context()); !result.Released || err != nil {
		t.Fatalf("implicit close = %+v, %v", result, err)
	}
	if result, err := file.CloseWithAction(t.Context(), closeAttemptForTest(t)); result.Released || !errors.Is(err, syscall.EINVAL) || len(probe.attempts) != 0 {
		t.Fatalf("unexecuted explicit close = %+v, %v, calls=%d", result, err, len(probe.attempts))
	}
}

func TestCloseEpochTombstonesRespectSessionBound(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.maxCloseActions = 1
	newProbe := func() *actionFileProbe {
		probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
		probe.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
			return storage.ReferenceCloseResult{}, nil, &storage.CloseActionNotExecutedError{CurrentEpoch: 2}
		}
		return probe
	}
	firstProbe := newProbe()
	first := &retainedFile{session: session, remote: firstProbe}
	firstAction := closeAttemptForTest(t)
	if _, err := first.CloseWithAction(t.Context(), firstAction); err == nil || session.closeReservations != 0 || session.closeTombstones != 1 {
		t.Fatalf("first tombstone = %v, active=%d, old=%d", err, session.closeReservations, session.closeTombstones)
	}
	secondProbe := newProbe()
	second := &retainedFile{session: session, remote: secondProbe}
	old, _, _ := closeEpochAttemptsForTest(t)
	if _, err := second.CloseWithAction(t.Context(), old); !errors.Is(err, syscall.EAGAIN) || session.hasCloseTombstone(&second.retainedClose, old) || session.closeReservations != 0 || session.closeTombstones != 1 || len(secondProbe.attempts) != 1 {
		t.Fatalf("overflow tombstone = %v, local=%t, active=%d, old=%d, remote=%d", err, session.hasCloseTombstone(&second.retainedClose, old), session.closeReservations, session.closeTombstones, len(secondProbe.attempts))
	}
	if _, err := second.CloseWithAction(t.Context(), old); !errors.Is(err, syscall.EAGAIN) || len(secondProbe.attempts) != 2 {
		t.Fatalf("unreserved same-ID replay = %v, calls=%d", err, len(secondProbe.attempts))
	}
	if _, err := first.CloseWithAction(t.Context(), firstAction); err == nil || len(firstProbe.attempts) != 2 || session.closeReservations != 0 {
		t.Fatalf("same-ID replay under saturation = %v, calls=%d, active=%d", err, len(firstProbe.attempts), session.closeReservations)
	}
	unrelated := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	unrelated.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	third := &retainedFile{session: session, remote: unrelated}
	if result, err := third.CloseWithAction(t.Context(), closeAttemptForTest(t)); !result.Released || err != nil || session.closeReservations != 0 {
		t.Fatalf("unrelated close under tombstone saturation = %+v, %v, active=%d", result, err, session.closeReservations)
	}
}

func TestCloseAdmissionReleasesAfterDeterminedOutcome(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.maxCloseActions = 1
	completed := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	completed.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	first := &retainedFile{session: session, remote: completed}
	if result, err := first.CloseWithAction(t.Context(), closeAttemptForTest(t)); !result.Released || err != nil || session.closeReservations != 0 {
		t.Fatalf("completed close = %+v, %v, reserved=%d", result, err, session.closeReservations)
	}
	secondProbe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	secondProbe.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		return storage.ReferenceCloseResult{}, nil, &storage.CloseActionNotExecutedError{CurrentEpoch: 2}
	}
	second := &retainedFile{session: session, remote: secondProbe}
	if _, err := second.CloseWithAction(t.Context(), closeAttemptForTest(t)); err == nil || session.closeReservations != 0 || session.closeTombstones != 1 || len(secondProbe.attempts) != 1 {
		t.Fatalf("reused admission = %v, active=%d, old=%d, remote=%d", err, session.closeReservations, session.closeTombstones, len(secondProbe.attempts))
	}
}

func TestRepeatedCloseEpochMissesKeepActiveCapacity(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.maxCloseActions = 1
	var previous *retainedFile
	var previousAttempt storage.CloseAttempt
	for epoch := uint64(1); epoch <= 4; epoch++ {
		action, err := storage.NewFileActionID(epoch)
		if err != nil {
			t.Fatal(err)
		}
		attempt := storage.CloseAttempt{Action: action, Generation: 1}
		advanced := false
		probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
		probe.status = func() (storage.CloseOwnerStatus, error) {
			currentEpoch := epoch
			if advanced {
				currentEpoch++
			}
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: currentEpoch}, nil
		}
		probe.query = func(requested storage.CloseAttempt) (storage.FileActionReceipt, error) {
			outcome := storage.FileActionNotExecuted
			if advanced {
				outcome = storage.FileActionRetired
			}
			return storage.FileActionReceipt{Action: requested.Action, Operation: storage.OpFileClose, Outcome: outcome}, nil
		}
		probe.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
			if epoch == 4 {
				return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
			}
			advanced = true
			return storage.ReferenceCloseResult{}, nil, &storage.CloseActionNotExecutedError{CurrentEpoch: epoch + 1}
		}
		file := &retainedFile{session: session, remote: probe}
		result, err := file.CloseWithAction(t.Context(), attempt)
		if epoch < 4 {
			if result.Released || session.closeReservations != 0 || session.closeTombstones != 1 || !session.hasCloseTombstone(&file.retainedClose, attempt) {
				t.Fatalf("epoch %d proof = %+v, %v, active=%d, old=%d", epoch, result, err, session.closeReservations, session.closeTombstones)
			}
			if previous != nil && session.hasCloseTombstone(&previous.retainedClose, previousAttempt) {
				t.Fatalf("epoch %d did not reclaim retired close proof", epoch)
			}
			var proof *storage.CloseActionNotExecutedError
			if !errors.As(err, &proof) || proof.CurrentEpoch != epoch+1 {
				t.Fatalf("epoch %d typed proof = %v", epoch, err)
			}
			previous, previousAttempt = file, attempt
		} else if !result.Released || err != nil || session.closeReservations != 0 || session.closeTombstones != 1 {
			t.Fatalf("terminal close = %+v, %v, active=%d, old=%d", result, err, session.closeReservations, session.closeTombstones)
		}
	}
}

func TestUnknownCloseAdmissionBlocksNewActionButNotReplay(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.maxCloseActions = 1
	firstProbe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	firstProbe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if call == 1 {
			return storage.ReferenceCloseResult{}, nil, syscall.EIO
		}
		return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
	}
	first := &retainedFile{session: session, remote: firstProbe}
	firstAction := closeAttemptForTest(t)
	if _, err := first.CloseWithAction(t.Context(), firstAction); !errors.Is(err, syscall.EIO) || session.closeReservations != 1 {
		t.Fatalf("unknown first attempt = %v, active=%d", err, session.closeReservations)
	}
	secondProbe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	secondProbe.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	second := &retainedFile{session: session, remote: secondProbe}
	secondAction := closeAttemptForTest(t)
	if status, err := second.CloseOwnerStatus(t.Context()); err != nil || status.Ready || status.Current != nil || status.NextGeneration != 1 || status.CurrentEpoch != 1 {
		t.Fatalf("local capacity was omitted from owner status = %+v, %v", status, err)
	}
	if _, err := second.CloseWithAction(t.Context(), secondAction); !errors.Is(err, syscall.EAGAIN) || len(secondProbe.attempts) != 0 {
		t.Fatalf("new attempt exceeded active cap = %v, remote=%d", err, len(secondProbe.attempts))
	}
	if result, err := first.CloseWithAction(t.Context(), firstAction); result.Released || !result.Determined || !errors.Is(err, syscall.EAGAIN) || session.closeReservations != 0 {
		t.Fatalf("old attempt replay = %+v, %v, active=%d", result, err, session.closeReservations)
	}
	if status, err := second.CloseOwnerStatus(t.Context()); err != nil || !status.Ready {
		t.Fatalf("released local capacity stayed unready = %+v, %v", status, err)
	}
	if result, err := second.CloseWithAction(t.Context(), secondAction); !result.Released || err != nil || session.closeReservations != 0 {
		t.Fatalf("new attempt after release = %+v, %v, active=%d", result, err, session.closeReservations)
	}
}

func TestTombstoneSaturationRecoversAfterBoundHistoryRetires(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.maxCloseActions = 1
	firstAction := closeAttemptForTest(t)
	retired := false
	firstProbe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	firstProbe.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if retired {
			return storage.ReferenceCloseResult{}, nil, syscall.ESTALE
		}
		return storage.ReferenceCloseResult{}, nil, &storage.CloseActionNotExecutedError{CurrentEpoch: 2}
	}
	firstProbe.query = func(attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
		outcome := storage.FileActionNotExecuted
		if retired {
			outcome = storage.FileActionRetired
		}
		return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: outcome}, nil
	}
	first := &retainedFile{session: session, remote: firstProbe}
	if _, err := first.CloseWithAction(t.Context(), firstAction); err == nil || !session.hasCloseTombstone(&first.retainedClose, firstAction) {
		t.Fatalf("first retained proof = %v, tombstone=%t", err, session.hasCloseTombstone(&first.retainedClose, firstAction))
	}
	old, replacement, _ := closeEpochAttemptsForTest(t)
	secondProbe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	secondProbe.status = func() (storage.CloseOwnerStatus, error) {
		if len(secondProbe.attempts) == 0 {
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
		}
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 2}, nil
	}
	secondProbe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if secondProbe.attempts[call-1] == old {
			return storage.ReferenceCloseResult{}, nil, &storage.CloseActionNotExecutedError{CurrentEpoch: 2}
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	second := &retainedFile{session: session, remote: secondProbe}
	if _, err := second.CloseWithAction(t.Context(), old); !errors.Is(err, syscall.EAGAIN) || !second.closeProofUnstored || session.closeReservations != 0 {
		t.Fatalf("saturated proof = %v, unstored=%t, active=%d", err, second.closeProofUnstored, session.closeReservations)
	}
	if _, err := second.CloseWithAction(t.Context(), replacement); !errors.Is(err, syscall.EAGAIN) || len(secondProbe.attempts) != 1 {
		t.Fatalf("replacement before expiry = %v, calls=%d", err, len(secondProbe.attempts))
	}
	retired = true
	if _, err := second.CloseWithAction(t.Context(), old); err == nil || errors.Is(err, syscall.EAGAIN) || second.closeProofUnstored || !session.hasCloseTombstone(&second.retainedClose, old) || session.hasCloseTombstone(&first.retainedClose, firstAction) {
		t.Fatalf("same-ID recovery = %v, unstored=%t, new=%t, retired=%t", err, second.closeProofUnstored, session.hasCloseTombstone(&second.retainedClose, old), session.hasCloseTombstone(&first.retainedClose, firstAction))
	}
	if result, err := second.CloseWithAction(t.Context(), replacement); !result.Released || !result.Determined || err != nil || session.closeTombstones != 1 {
		t.Fatalf("replacement after expiry = %+v, %v, tombstones=%d", result, err, session.closeTombstones)
	}
}

func TestSessionRetirementReleasesWrapperTombstones(t *testing.T) {
	session := retainedTestSession(t, &confirmedSessionCloseProbe{fileSessionStub: &fileSessionStub{}})
	old, replacement, _ := closeEpochAttemptsForTest(t)
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 2}, nil
	}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if probe.attempts[call-1] == old {
			return storage.ReferenceCloseResult{}, nil, &storage.CloseActionNotExecutedError{CurrentEpoch: 2}
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	if _, err := file.CloseWithAction(t.Context(), old); err == nil || session.closeTombstones != 1 {
		t.Fatalf("pre-retirement tombstone = %v, count=%d", err, session.closeTombstones)
	}
	if result, err := file.CloseWithAction(t.Context(), replacement); !result.Released || err != nil {
		t.Fatalf("file release = %+v, %v", result, err)
	}
	if result, err := session.CloseWithResult(t.Context()); !result.Released || !result.Determined || err != nil || session.closeTombstones != 0 || !session.closeTombstonesRetired {
		t.Fatalf("session retirement = %+v, %v, count=%d, retired=%t", result, err, session.closeTombstones, session.closeTombstonesRetired)
	}
	if result, err := file.CloseWithAction(t.Context(), old); result.Released || !errors.Is(err, syscall.ESTALE) || len(probe.attempts) != 2 {
		t.Fatalf("old ID after owner retirement = %+v, %v, calls=%d", result, err, len(probe.attempts))
	}
}

func TestRetiringFileAdoptsInternalCloseOwner(t *testing.T) {
	session := retainedTestSession(t, nil)
	first, internal := closeAttemptForTest(t), closeAttemptForTest(t)
	internal.Generation = 2
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if call == 1 {
			return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		if len(probe.attempts) == 0 {
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
		}
		return storage.CloseOwnerStatus{Current: &internal, CurrentOutcome: storage.FileActionPending, NextGeneration: 2, CurrentEpoch: 1}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	if _, err := file.CloseWithAction(t.Context(), first); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	session.mu.Lock()
	session.closing = true
	session.mu.Unlock()
	status, err := file.CloseOwnerStatus(t.Context())
	if err != nil || status.Released || status.Ready || status.Current == nil || *status.Current != internal || status.CurrentOutcome != storage.FileActionPending || status.NextGeneration != 2 || status.CurrentEpoch != 1 {
		t.Fatalf("retired file owner status = %+v, %v", status, err)
	}
	if result, err := file.CloseWithAction(t.Context(), *status.Current); !result.Released || !result.Determined || err != nil || len(probe.attempts) != 2 || probe.attempts[1] != internal {
		t.Fatalf("adopted internal close = %+v, %v, attempts=%+v", result, err, probe.attempts)
	}
}

func TestRetiringNodeAdoptsInternalCloseOwner(t *testing.T) {
	session := retainedTestSession(t, nil)
	first, internal := closeAttemptForTest(t), closeAttemptForTest(t)
	internal.Generation = 2
	probe := &actionReferenceProbe{barrierReferenceStub: &barrierReferenceStub{}}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if call == 1 {
			return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		if len(probe.attempts) == 0 {
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
		}
		return storage.CloseOwnerStatus{Current: &internal, CurrentOutcome: storage.FileActionUnknown, NextGeneration: 2, CurrentEpoch: 1}, nil
	}
	reference := &nodeReference{session: session, remote: probe}
	if _, err := reference.CloseWithAction(t.Context(), first); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	session.mu.Lock()
	session.closing = true
	session.mu.Unlock()
	status, err := reference.CloseOwnerStatus(t.Context())
	if err != nil || status.Released || status.Ready || status.Current == nil || *status.Current != internal || status.CurrentOutcome != storage.FileActionUnknown || status.NextGeneration != 2 || status.CurrentEpoch != 1 {
		t.Fatalf("retired node owner status = %+v, %v", status, err)
	}
	if result, err := reference.CloseWithAction(t.Context(), *status.Current); !result.Released || !result.Determined || err != nil || len(probe.attempts) != 2 || probe.attempts[1] != internal {
		t.Fatalf("adopted internal node close = %+v, %v, attempts=%+v", result, err, probe.attempts)
	}
}

func TestReadyCloseOwnerStatusIsForwarded(t *testing.T) {
	session := retainedTestSession(t, nil)
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: 3, CurrentEpoch: 7}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	status, err := file.CloseOwnerStatus(t.Context())
	if err != nil || !status.Ready || status.Released || status.Current != nil || status.NextGeneration != 3 || status.CurrentEpoch != 7 {
		t.Fatalf("ready owner status = %+v, %v", status, err)
	}
}

func TestDeterminedCloseWaitsForOwnerReady(t *testing.T) {
	session := retainedTestSession(t, nil)
	first, next := closeAttemptForTest(t), closeAttemptForTest(t)
	next.Generation = 2
	ready := false
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if call == 1 {
			return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		if len(probe.attempts) == 0 {
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
		}
		return storage.CloseOwnerStatus{Ready: ready, NextGeneration: 2, CurrentEpoch: 1}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	if _, err := file.CloseWithAction(t.Context(), first); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	if _, err := file.CloseWithAction(t.Context(), next); !errors.Is(err, syscall.EAGAIN) || len(probe.attempts) != 1 || file.closeAttempt != first || session.closeReservations != 0 {
		t.Fatalf("unready close = %v, attempts=%d, current=%+v, reserved=%d", err, len(probe.attempts), file.closeAttempt, session.closeReservations)
	}
	ready = true
	if result, err := file.CloseWithAction(t.Context(), next); !result.Released || !result.Determined || err != nil || len(probe.attempts) != 2 {
		t.Fatalf("ready close = %+v, %v, attempts=%d", result, err, len(probe.attempts))
	}
}

func TestUnknownCloseAdoptsInternalSameGenerationAttempt(t *testing.T) {
	session := retainedTestSession(t, nil)
	old, internal, _ := closeEpochAttemptsForTest(t)
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if probe.attempts[call-1] == old {
			if call == 1 {
				return storage.ReferenceCloseResult{}, nil, syscall.EIO
			}
			return storage.ReferenceCloseResult{}, nil, &storage.CloseActionNotExecutedError{CurrentEpoch: 2}
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		if len(probe.attempts) == 0 {
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
		}
		return storage.CloseOwnerStatus{Current: &internal, CurrentOutcome: storage.FileActionUnknown, NextGeneration: 1, CurrentEpoch: 2}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	if result, err := file.CloseWithAction(t.Context(), old); result.Released || result.Determined || !errors.Is(err, syscall.EIO) || session.closeReservations != 1 {
		t.Fatalf("unknown old close = %+v, %v, active=%d", result, err, session.closeReservations)
	}
	session.mu.Lock()
	session.closing = true
	session.mu.Unlock()
	status, err := file.CloseOwnerStatus(t.Context())
	if err != nil || status.Current == nil || *status.Current != internal || status.Ready {
		t.Fatalf("internal owner status = %+v, %v", status, err)
	}
	if result, err := file.CloseWithAction(t.Context(), *status.Current); !result.Released || !result.Determined || err != nil || session.closeReservations != 0 || session.closeTombstones != 1 || len(probe.attempts) != 2 {
		t.Fatalf("same-generation adoption = %+v, %v, active=%d, old=%d, attempts=%+v", result, err, session.closeReservations, session.closeTombstones, probe.attempts)
	}
	if result, err := file.CloseWithAction(t.Context(), old); result.Released || err == nil || len(probe.attempts) != 3 || probe.attempts[2] != old {
		t.Fatalf("old exact replay after adoption = %+v, %v, attempts=%+v", result, err, probe.attempts)
	}
}

func TestFirstExplicitCloseRejectsGenerationHoleBeforeDispatch(t *testing.T) {
	session := retainedTestSession(t, nil)
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	valid := closeAttemptForTest(t)
	hole := valid
	hole.Generation = 100
	if result, err := file.CloseWithAction(t.Context(), hole); result.Released || !errors.Is(err, syscall.EINVAL) || file.closeAttemptSet || len(probe.attempts) != 0 || session.closeReservations != 0 {
		t.Fatalf("first generation hole = %+v, %v, current=%t, remote=%d, reserved=%d", result, err, file.closeAttemptSet, len(probe.attempts), session.closeReservations)
	}
	if result, err := file.CloseWithAction(t.Context(), valid); !result.Released || !result.Determined || err != nil || len(probe.attempts) != 1 {
		t.Fatalf("valid first close after hole = %+v, %v, remote=%d", result, err, len(probe.attempts))
	}
}

func TestReplacementCloseRejectsGenerationHoleBeforeDispatch(t *testing.T) {
	session := retainedTestSession(t, nil)
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if call == 1 {
			return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	first, valid := closeAttemptForTest(t), closeAttemptForTest(t)
	valid.Generation = 2
	hole := valid
	hole.Generation = 100
	if _, err := file.CloseWithAction(t.Context(), first); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	if result, err := file.CloseWithAction(t.Context(), hole); result.Released || !errors.Is(err, syscall.EINVAL) || file.closeAttempt != first || len(probe.attempts) != 1 {
		t.Fatalf("replacement generation hole = %+v, %v, current=%+v, remote=%d", result, err, file.closeAttempt, len(probe.attempts))
	}
	if result, err := file.CloseWithAction(t.Context(), valid); !result.Released || !result.Determined || err != nil || len(probe.attempts) != 2 {
		t.Fatalf("valid replacement after hole = %+v, %v, remote=%d", result, err, len(probe.attempts))
	}
}

func TestImplicitUnknownAdoptsExactNativeCurrent(t *testing.T) {
	session := retainedTestSession(t, nil)
	native := closeAttemptForTest(t)
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{close: func(context.Context) error { return syscall.EIO }}}
	probe.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		return storage.CloseOwnerStatus{Current: &native, CurrentOutcome: storage.FileActionUnknown, NextGeneration: 1, CurrentEpoch: 1}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	if result, err := file.CloseWithResult(t.Context()); result.Released || !errors.Is(err, syscall.EIO) || !file.implicitClose || file.closeAttemptSet {
		t.Fatalf("implicit unknown close = %+v, %v, implicit=%t, explicit=%t", result, err, file.implicitClose, file.closeAttemptSet)
	}
	if result, err := file.CloseWithAction(t.Context(), native); !result.Released || !result.Determined || err != nil || len(probe.attempts) != 1 || probe.attempts[0] != native || file.implicitClose {
		t.Fatalf("native current adoption = %+v, %v, attempts=%+v, implicit=%t", result, err, probe.attempts, file.implicitClose)
	}
}

func TestImplicitCleanupAdvancesAfterDeterminedExplicitFailure(t *testing.T) {
	session := retainedTestSession(t, nil)
	first := closeAttemptForTest(t)
	ready := false
	implicitCalls := 0
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{close: func(context.Context) error {
		implicitCalls++
		return nil
	}}}
	probe.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
	}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		if len(probe.attempts) == 0 {
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
		}
		return storage.CloseOwnerStatus{Ready: ready, NextGeneration: 2, CurrentEpoch: 1}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	if _, err := file.CloseWithAction(t.Context(), first); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	if result, err := file.CloseWithResult(t.Context()); result.Released || !errors.Is(err, syscall.EAGAIN) || implicitCalls != 0 || file.closeAttempt != first {
		t.Fatalf("unready implicit cleanup = %+v, %v, implicit=%d, current=%+v", result, err, implicitCalls, file.closeAttempt)
	}
	ready = true
	if result, err := file.CloseWithResult(t.Context()); !result.Released || err != nil || implicitCalls != 1 || len(probe.attempts) != 1 {
		t.Fatalf("advanced implicit cleanup = %+v, %v, implicit=%d, explicit=%+v", result, err, implicitCalls, probe.attempts)
	}
	if result, err := file.CloseWithAction(t.Context(), first); result.Released || !result.Determined || !errors.Is(err, syscall.EAGAIN) || len(probe.attempts) != 2 || !file.closeResult.Released {
		t.Fatalf("original A replay after implicit successor = %+v, %v, attempts=%+v, summary=%+v", result, err, probe.attempts, file.closeResult)
	}
}

func TestNodeOriginalActionSurvivesImplicitSuccessor(t *testing.T) {
	session := retainedTestSession(t, nil)
	first := closeAttemptForTest(t)
	probe := &actionReferenceProbe{barrierReferenceStub: &barrierReferenceStub{}}
	probe.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
	}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		if len(probe.attempts) == 0 {
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
		}
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: 2, CurrentEpoch: 1}, nil
	}
	reference := &nodeReference{session: session, remote: probe}
	if _, err := reference.CloseWithAction(t.Context(), first); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	if result, err := reference.CloseWithResult(t.Context()); !result.Released || err != nil {
		t.Fatalf("implicit node successor = %+v, %v", result, err)
	}
	if result, err := reference.CloseWithAction(t.Context(), first); result.Released || !result.Determined || !errors.Is(err, syscall.EAGAIN) || len(probe.attempts) != 2 || !reference.closeResult.Released {
		t.Fatalf("original node A replay = %+v, %v, attempts=%+v, summary=%+v", result, err, probe.attempts, reference.closeResult)
	}
}

func TestExplicitActionIDCannotBeReusedAtHigherGeneration(t *testing.T) {
	session := retainedTestSession(t, nil)
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if call == 1 {
			return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	first, valid := closeAttemptForTest(t), closeAttemptForTest(t)
	valid.Generation = 2
	reused := storage.CloseAttempt{Action: first.Action, Generation: 2}
	if _, err := file.CloseWithAction(t.Context(), first); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	if result, err := file.CloseWithAction(t.Context(), reused); result.Released || !errors.Is(err, syscall.EINVAL) || file.closeAttempt != first || len(probe.attempts) != 1 {
		t.Fatalf("reused action ID = %+v, %v, current=%+v, attempts=%d", result, err, file.closeAttempt, len(probe.attempts))
	}
	if result, err := file.CloseWithAction(t.Context(), valid); !result.Released || err != nil || len(probe.attempts) != 2 {
		t.Fatalf("valid next ID after reuse rejection = %+v, %v, attempts=%d", result, err, len(probe.attempts))
	}
}

func TestOlderSameGenerationCloseQueriesBoundHistory(t *testing.T) {
	for _, kind := range []string{"file", "reference"} {
		for _, scenario := range []struct {
			name       string
			outcome    storage.FileActionOutcome
			queryErr   error
			wrongOwner bool
			want       error
		}{
			{name: "retired", outcome: storage.FileActionRetired, want: syscall.ESTALE},
			{name: "not-executed", outcome: storage.FileActionNotExecuted, want: syscall.EINVAL},
			{name: "unknown", outcome: storage.FileActionUnknown, want: syscall.EIO},
			{name: "query-failure", queryErr: syscall.EAGAIN, want: syscall.EAGAIN},
			{name: "wrong-reference", outcome: storage.FileActionRetired, wrongOwner: true, want: syscall.EIO},
		} {
			t.Run(kind+"/"+scenario.name, func(t *testing.T) {
				session := retainedTestSession(t, nil)
				old, current, _ := closeEpochAttemptsForTest(t)
				status := func() (storage.CloseOwnerStatus, error) {
					return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 2}, nil
				}
				query := func(requested storage.CloseAttempt) (storage.FileActionReceipt, error) {
					if requested == current {
						return storage.FileActionReceipt{Action: requested.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}, nil
					}
					if requested != old {
						t.Fatalf("query changed attempt: %+v", requested)
					}
					if scenario.queryErr != nil {
						return storage.FileActionReceipt{}, scenario.queryErr
					}
					if scenario.wrongOwner {
						return storage.FileActionReceipt{Action: current.Action, Operation: storage.OpFileClose, Outcome: scenario.outcome}, nil
					}
					return storage.FileActionReceipt{Action: requested.Action, Operation: storage.OpFileClose, Outcome: scenario.outcome}, nil
				}
				result := func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
					return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
				}
				var closer storage.ReferenceCloseActions
				var attempts func() []storage.CloseAttempt
				var active func() storage.CloseAttempt
				if kind == "file" {
					probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}, status: status, query: query, result: result}
					file := &retainedFile{session: session, remote: probe}
					closer = file
					attempts = func() []storage.CloseAttempt { return probe.attempts }
					active = func() storage.CloseAttempt { return file.closeAttempt }
				} else {
					probe := &actionReferenceProbe{barrierReferenceStub: &barrierReferenceStub{}, status: status, query: query, result: result}
					reference := &nodeReference{session: session, remote: probe}
					closer = reference
					attempts = func() []storage.CloseAttempt { return probe.attempts }
					active = func() storage.CloseAttempt { return reference.closeAttempt }
				}
				if response, err := closer.CloseWithAction(t.Context(), current); response.Released || !response.Determined || !errors.Is(err, syscall.EAGAIN) {
					t.Fatalf("current close = %+v, %v", response, err)
				}
				if response, err := closer.CloseWithAction(t.Context(), old); response.Released || !errors.Is(err, scenario.want) || len(attempts()) != 1 || active() != current {
					t.Fatalf("old close = %+v, %v, calls=%+v, active=%+v", response, err, attempts(), active())
				}
			})
		}
	}
}

func TestRejectedHistoricalIDDoesNotAdvanceLocalGeneration(t *testing.T) {
	session := retainedTestSession(t, nil)
	first, second, valid := closeAttemptForTest(t), closeAttemptForTest(t), closeAttemptForTest(t)
	second.Generation = 2
	valid.Generation = 3
	reused := storage.CloseAttempt{Action: first.Action, Generation: 3}
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		next := uint64(1)
		if len(probe.attempts) >= 1 {
			next = 2
		}
		if len(probe.attempts) >= 2 {
			next = 3
		}
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: next, CurrentEpoch: 1}, nil
	}
	probe.query = func(attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}, nil
	}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if call <= 2 {
			return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
		}
		if call == 3 {
			return storage.ReferenceCloseResult{}, nil, syscall.EINVAL
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	for _, attempt := range []storage.CloseAttempt{first, second} {
		if result, err := file.CloseWithAction(t.Context(), attempt); result.Released || !result.Determined || !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("known failure %+v = %+v, %v", attempt, result, err)
		}
	}
	if result, err := file.CloseWithAction(t.Context(), reused); result.Released || !errors.Is(err, syscall.EINVAL) || file.closeAttempt != second {
		t.Fatalf("rejected old ID = %+v, %v, current=%+v", result, err, file.closeAttempt)
	}
	if result, err := file.CloseWithAction(t.Context(), valid); !result.Released || err != nil || len(probe.attempts) != 4 {
		t.Fatalf("valid ID after rejection = %+v, %v, attempts=%+v", result, err, probe.attempts)
	}
}

func TestHistoricalIDRejectedByBoundQueryBeforeLocalMutation(t *testing.T) {
	for _, kind := range []string{"file", "reference"} {
		t.Run(kind, func(t *testing.T) {
			session := retainedTestSession(t, nil)
			first, second, valid := closeAttemptForTest(t), closeAttemptForTest(t), closeAttemptForTest(t)
			second.Generation = 2
			valid.Generation = 3
			reused := storage.CloseAttempt{Action: first.Action, Generation: 3}
			var attempts []storage.CloseAttempt
			status := func() (storage.CloseOwnerStatus, error) {
				return storage.CloseOwnerStatus{Ready: true, NextGeneration: uint64(len(attempts) + 1), CurrentEpoch: 1}, nil
			}
			query := func(attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
				if attempt == reused {
					return storage.FileActionReceipt{}, syscall.EINVAL
				}
				return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}, nil
			}
			result := func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
				if call < 3 {
					return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
				}
				return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
			}
			var closer storage.ReferenceCloseActions
			var current func() storage.CloseAttempt
			if kind == "file" {
				probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}, status: status, query: query}
				probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
					attempts = probe.attempts
					return result(call)
				}
				file := &retainedFile{session: session, remote: probe}
				closer = file
				current = func() storage.CloseAttempt { return file.closeAttempt }
			} else {
				probe := &actionReferenceProbe{barrierReferenceStub: &barrierReferenceStub{}, status: status, query: query}
				probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
					attempts = probe.attempts
					return result(call)
				}
				reference := &nodeReference{session: session, remote: probe}
				closer = reference
				current = func() storage.CloseAttempt { return reference.closeAttempt }
			}
			for _, attempt := range []storage.CloseAttempt{first, second} {
				if response, err := closer.CloseWithAction(t.Context(), attempt); response.Released || !response.Determined || !errors.Is(err, syscall.EAGAIN) {
					t.Fatalf("known failure %+v = %+v, %v", attempt, response, err)
				}
			}
			if response, err := closer.CloseWithAction(t.Context(), reused); response.Released || !errors.Is(err, syscall.EINVAL) || current() != second || len(attempts) != 2 {
				t.Fatalf("bound query rejection = %+v, %v, current=%+v, attempts=%+v", response, err, current(), attempts)
			}
			if response, err := closer.CloseWithAction(t.Context(), valid); !response.Released || err != nil || len(attempts) != 3 {
				t.Fatalf("valid next action = %+v, %v, attempts=%+v", response, err, attempts)
			}
		})
	}
}

func TestRejectedCandidateWithoutBoundProofKeepsUnknownCursor(t *testing.T) {
	session := retainedTestSession(t, nil)
	first, rejected, valid := closeAttemptForTest(t), closeAttemptForTest(t), closeAttemptForTest(t)
	rejected.Generation = 2
	valid.Generation = 2
	probe := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	probe.status = func() (storage.CloseOwnerStatus, error) {
		generation := uint64(1)
		if len(probe.attempts) > 0 {
			generation = 2
		}
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: generation, CurrentEpoch: 1}, nil
	}
	probe.result = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if call == 1 {
			return storage.ReferenceCloseResult{Determined: true}, nil, syscall.EAGAIN
		}
		if call == 2 {
			return storage.ReferenceCloseResult{}, nil, syscall.EINVAL
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	queryFailed := false
	probe.query = func(attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
		if attempt == rejected && len(probe.attempts) == 2 && !queryFailed {
			queryFailed = true
			return storage.FileActionReceipt{}, syscall.EIO
		}
		return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}, nil
	}
	file := &retainedFile{session: session, remote: probe}
	if _, err := file.CloseWithAction(t.Context(), first); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	if response, err := file.CloseWithAction(t.Context(), rejected); response.Determined || !errors.Is(err, syscall.EINVAL) || file.closeAttempt != rejected {
		t.Fatalf("unproven rejection = %+v, %v, current=%+v", response, err, file.closeAttempt)
	}
	if response, err := file.CloseWithAction(t.Context(), valid); response.Released || err == nil || len(probe.attempts) != 2 {
		t.Fatalf("unproven candidate replacement = %+v, %v, attempts=%+v", response, err, probe.attempts)
	}
	if response, err := file.CloseWithAction(t.Context(), rejected); response.Released || !errors.Is(err, syscall.EINVAL) || file.closeAttempt != first || len(probe.attempts) != 2 {
		t.Fatalf("replayed bound proof = %+v, %v, current=%+v, attempts=%+v", response, err, file.closeAttempt, probe.attempts)
	}
	if response, err := file.CloseWithAction(t.Context(), valid); !response.Released || err != nil || len(probe.attempts) != 3 {
		t.Fatalf("fresh action after proof = %+v, %v, attempts=%+v", response, err, probe.attempts)
	}
}

func TestDeterminedImplicitFailureAllowsExplicitNextGeneration(t *testing.T) {
	session := retainedTestSession(t, nil)
	inner := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}}
	inner.result = func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	inner.status = func() (storage.CloseOwnerStatus, error) {
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: 2, CurrentEpoch: 1}, nil
	}
	file := &retainedFile{session: session, remote: &determinedImplicitFileProbe{actionFileProbe: inner}}
	if result, err := file.CloseWithResult(t.Context()); result.Released || !result.Determined || !errors.Is(err, syscall.EAGAIN) || file.implicitClose {
		t.Fatalf("determined implicit failure = %+v, %v, implicit=%t", result, err, file.implicitClose)
	}
	if status, err := file.CloseOwnerStatus(t.Context()); err != nil || !status.Ready || status.NextGeneration != 2 {
		t.Fatalf("next-generation owner status = %+v, %v", status, err)
	}
	next := closeAttemptForTest(t)
	next.Generation = 2
	if result, err := file.CloseWithAction(t.Context(), next); !result.Released || !result.Determined || err != nil || len(inner.attempts) != 1 || inner.attempts[0] != next {
		t.Fatalf("explicit next generation = %+v, %v, attempts=%+v", result, err, inner.attempts)
	}
}
