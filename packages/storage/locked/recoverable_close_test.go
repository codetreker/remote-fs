package locked

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type closeActionFileProbe struct {
	storage.File
	attempt storage.CloseAttempt
	result  storage.ReferenceCloseResult
	err     error
	calls   int
	ready   bool
}

func (p *closeActionFileProbe) CloseWithAction(_ context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	p.calls++
	p.attempt = attempt
	return p.result, p.err
}

func (p *closeActionFileProbe) QueryCloseAttempt(_ context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	p.attempt = attempt
	return storage.FileActionReceipt{Action: attempt.Action}, p.err
}

func (p *closeActionFileProbe) CloseOwnerStatus(context.Context) (storage.CloseOwnerStatus, error) {
	if p.ready {
		return storage.CloseOwnerStatus{Ready: true, NextGeneration: p.attempt.Generation + 1, CurrentEpoch: 2}, p.err
	}
	return storage.CloseOwnerStatus{Current: &p.attempt, CurrentOutcome: storage.FileActionPending, NextGeneration: p.attempt.Generation, CurrentEpoch: 2}, p.err
}

type closeActionSessionProbe struct {
	storage.FileSession
	err error
}

func (p *closeActionSessionProbe) CheckRecoverableReferenceClose() error { return p.err }

func TestRecoverableCloseForwardsTypedOutcomeAndAction(t *testing.T) {
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	attempt := storage.CloseAttempt{Action: action, Generation: 1}
	failure := errors.New("authority close failed")
	probe := &closeActionFileProbe{err: failure}
	wrapped := (&Storage{}).wrapFile(probe).(storage.ReferenceCloseActions)
	for _, expected := range []storage.ReferenceCloseResult{
		{},
		{Determined: true},
		{Released: true, Determined: true},
	} {
		probe.result = expected
		result, err := wrapped.CloseWithAction(t.Context(), attempt)
		if result != expected || !errors.Is(err, failure) || probe.attempt != attempt {
			t.Fatalf("close result = %+v, %v, attempt=%+v", result, err, probe.attempt)
		}
	}
	if probe.calls != 3 {
		t.Fatalf("close calls = %d", probe.calls)
	}
	if receipt, err := wrapped.QueryCloseAttempt(t.Context(), attempt); receipt.Action != attempt.Action || !errors.Is(err, failure) || probe.attempt != attempt {
		t.Fatalf("query = %+v, %v, attempt=%+v", receipt, err, probe.attempt)
	}
	if status, err := wrapped.CloseOwnerStatus(t.Context()); status.Ready || status.Current == nil || *status.Current != attempt || status.CurrentOutcome != storage.FileActionPending || status.CurrentEpoch != 2 || !errors.Is(err, failure) {
		t.Fatalf("owner status = %+v, %v", status, err)
	}
	probe.ready = true
	if status, err := wrapped.CloseOwnerStatus(t.Context()); !status.Ready || status.Current != nil || status.NextGeneration != attempt.Generation+1 || !errors.Is(err, failure) {
		t.Fatalf("ready owner status = %+v, %v", status, err)
	}
	unsupported := (&Storage{}).wrapFile(&struct{ storage.File }{}).(storage.ReferenceCloseActions)
	if _, err := unsupported.CloseWithAction(t.Context(), attempt); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported close = %v", err)
	}
}

func TestRecoverableCloseCapabilityFollowsSession(t *testing.T) {
	failure := errors.New("no receipt reservation")
	session := &fileSession{FileSession: &closeActionSessionProbe{err: failure}, storage: &Storage{}}
	if err := session.CheckRecoverableReferenceClose(); !errors.Is(err, failure) {
		t.Fatalf("capability failure = %v", err)
	}
	session.FileSession = &struct{ storage.FileSession }{}
	if err := session.CheckRecoverableReferenceClose(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing capability = %v", err)
	}
}
