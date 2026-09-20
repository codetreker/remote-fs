package replicated

import (
	"context"
	"errors"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type rangeSessionStub struct {
	httprest.FileSessionWithBarrier
	failure error
	attempt storage.RangeAttempt
}

func (*rangeSessionStub) CheckRangeControl() error { return nil }
func (*rangeSessionStub) GetConflict(context.Context, storage.UseOwner, storage.RangeCommand) (storage.RangeConflict, error) {
	return storage.RangeConflict{}, nil
}
func (s *rangeSessionStub) Apply(context.Context, storage.UseOwner, []storage.RangeCommand, storage.LockRequestID) (storage.RangeAttempt, error) {
	return s.attempt, s.failure
}
func (s *rangeSessionStub) Query(context.Context, storage.UseOwner, storage.LockRequestID) (storage.RangeAttempt, error) {
	return s.attempt, s.failure
}
func (s *rangeSessionStub) Cancel(context.Context, storage.UseOwner, storage.LockRequestID) (storage.RangeAttempt, error) {
	return s.attempt, s.failure
}
func (*rangeSessionStub) Drop(context.Context, storage.UseOwner, storage.ConflictDomain) error {
	return nil
}

func TestRangeForwardingPreservesPartialReceiptAndOriginalError(t *testing.T) {
	failure := errors.New("response ended after the receipt")
	request, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	remote := &rangeSessionStub{failure: failure, attempt: storage.RangeAttempt{
		Request: request, State: storage.Granted, EverGranted: true,
		Effects: []storage.RangeEffect{{Released: true}},
	}}
	session := retainedTestSession(t, remote)
	for name, call := range map[string]func() (storage.RangeAttempt, error){
		"apply":  func() (storage.RangeAttempt, error) { return session.Apply(t.Context(), 7, nil, request) },
		"query":  func() (storage.RangeAttempt, error) { return session.Query(t.Context(), 7, request) },
		"cancel": func() (storage.RangeAttempt, error) { return session.Cancel(t.Context(), 7, request) },
	} {
		t.Run(name, func(t *testing.T) {
			got, err := call()
			if !errors.Is(err, failure) || got.Request != request || !got.EverGranted || len(got.Effects) != 1 || !got.Effects[0].Released {
				t.Fatalf("receipt=%+v,error=%v", got, err)
			}
		})
	}
}
