package replicated

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type rangeSessionStub struct {
	httprest.FileSessionWithBarrier
	checkErr error
	failure  error
	attempt  storage.RangeAttempt
}

func (s *rangeSessionStub) CheckUseOwners() error { return s.checkErr }
func (s *rangeSessionStub) NewUseOwner(context.Context, uint64, storage.UseScope, storage.OwnerOptions) (storage.UseOwner, error) {
	return 7, s.failure
}
func (s *rangeSessionStub) RetireUseOwner(context.Context, storage.UseOwner) error {
	return s.failure
}
func (s *rangeSessionStub) CheckRangeControl() error { return s.checkErr }
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

type referenceCapabilityStub struct {
	httprest.FileWithBarrier
	checkErr error
	failure  error
}

func (s *referenceCapabilityStub) CheckScopedReference() error { return s.checkErr }
func (s *referenceCapabilityStub) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: "scope"}, s.failure
}
func (s *referenceCapabilityStub) CheckMetadataAccess() error { return s.checkErr }
func (s *referenceCapabilityStub) SetMetadata(context.Context, string, []byte, []byte) (storage.OpaquePayload, error) {
	return storage.OpaquePayload{Version: []byte{1}, Data: []byte("value")}, s.failure
}
func (s *referenceCapabilityStub) SetMetadataWithBarrier(context.Context, string, []byte, []byte) (storage.OpaquePayload, *httprest.MutationBarrier, error) {
	return storage.OpaquePayload{Version: []byte{1}, Data: []byte("value")}, nil, s.failure
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
	if err := session.CheckUseOwners(); err != nil {
		t.Fatal(err)
	}
	if err := session.CheckRangeControl(); err != nil {
		t.Fatal(err)
	}
	if owner, err := session.NewUseOwner(t.Context(), 3, storage.UseScope{Token: "scope"}, storage.OwnerOptions{Lifetime: storage.OwnerExplicit}); !errors.Is(err, failure) || owner != 7 {
		t.Fatalf("owner=%d,error=%v", owner, err)
	}
	if err := session.RetireUseOwner(t.Context(), 7); !errors.Is(err, failure) {
		t.Fatalf("retire owner=%v", err)
	}
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

	missing := retainedTestSession(t, &fileSessionStub{})
	if err := missing.CheckUseOwners(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing owner capability=%v", err)
	}
	if err := missing.CheckRangeControl(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing range capability=%v", err)
	}
	rejectedRemote := &rangeSessionStub{checkErr: failure, failure: errors.New("dispatch should not occur")}
	rejected := retainedTestSession(t, rejectedRemote)
	for name, call := range map[string]func() error{
		"new owner": func() error {
			_, err := rejected.NewUseOwner(t.Context(), 3, storage.UseScope{Token: "scope"}, storage.OwnerOptions{Lifetime: storage.OwnerExplicit})
			return err
		},
		"retire owner": func() error { return rejected.RetireUseOwner(t.Context(), 7) },
		"query": func() error {
			_, err := rejected.Query(t.Context(), 7, request)
			return err
		},
		"cancel": func() error {
			_, err := rejected.Cancel(t.Context(), 7, request)
			return err
		},
		"drop": func() error { return rejected.Drop(t.Context(), 7, storage.DomainEnforced) },
	} {
		if err := call(); !errors.Is(err, failure) {
			t.Fatalf("refused %s=%v", name, err)
		}
	}
}

func TestReferenceCapabilityChecksAndErrorsArePreserved(t *testing.T) {
	failure := errors.New("remote capability failed")
	session := retainedTestSession(t, nil)
	remote := &referenceCapabilityStub{failure: failure}
	file := &retainedFile{session: session, remote: remote}
	if err := file.CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	if scope, err := file.Scope(t.Context()); !errors.Is(err, failure) || scope.Token != "scope" {
		t.Fatalf("scope=%+v,error=%v", scope, err)
	}
	if err := file.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	if payload, err := file.SetMetadata(t.Context(), "test.value", nil, nil); !errors.Is(err, failure) || len(payload.Version) != 0 || len(payload.Data) != 0 {
		t.Fatalf("metadata=%+v,error=%v", payload, err)
	}

	missing := &retainedFile{session: session, remote: &fileAuthorityStub{}}
	if err := missing.CheckScopedReference(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing scope capability=%v", err)
	}
	if err := missing.CheckMetadataAccess(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing metadata capability=%v", err)
	}
	remote.checkErr = failure
	remote.failure = errors.New("dispatch should not occur")
	if _, err := file.Scope(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("refused scope=%v", err)
	}
	if _, err := file.SetMetadata(t.Context(), "test.value", nil, nil); !errors.Is(err, failure) {
		t.Fatalf("refused metadata=%v", err)
	}
}
