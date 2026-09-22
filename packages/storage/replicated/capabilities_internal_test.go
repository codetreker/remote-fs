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

type cleanupFileStub struct {
	storage.File
	closeErr error
	closes   int
}

func (f *cleanupFileStub) Close(context.Context) error {
	f.closes++
	return f.closeErr
}

type cleanupReferenceStub struct {
	storage.NodeReference
	closeErr error
	closes   int
}

func (r *cleanupReferenceStub) Close(context.Context) error {
	r.closes++
	return r.closeErr
}

type barrierReferenceStub struct {
	storage.NodeReference
	closeErr      error
	closes        int
	checkErr      error
	mutation      storage.Attr
	mutationErr   error
	barrier       *httprest.MutationBarrier
	lastMutation  storage.FileMutation
	mutationCalls int
}

func (r *barrierReferenceStub) Close(context.Context) error {
	r.closes++
	return r.closeErr
}

func (r *barrierReferenceStub) CloseWithBarrier(context.Context) (*httprest.MutationBarrier, error) {
	r.closes++
	return &httprest.MutationBarrier{Incarnation: "log"}, r.closeErr
}

func (*barrierReferenceStub) SetAttrWithBarrier(context.Context, storage.AttrChange) (storage.Attr, *httprest.MutationBarrier, error) {
	return storage.Attr{}, &httprest.MutationBarrier{Incarnation: "log"}, nil
}

func (r *barrierReferenceStub) CheckConditionalFileMutation() error { return r.checkErr }

func (r *barrierReferenceStub) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	result, _, err := r.MutateFileWithBarrier(ctx, command)
	return result, err
}

func (r *barrierReferenceStub) MutateFileWithBarrier(_ context.Context, command storage.FileMutation) (storage.Attr, *httprest.MutationBarrier, error) {
	r.lastMutation = command
	r.mutationCalls++
	return r.mutation, r.barrier, r.mutationErr
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

func TestAtomicOpenNormalizesTypedNilPartialReference(t *testing.T) {
	session := retainedTestSession(t, nil)
	failure := errors.New("open failed before retaining a reference")
	var typedNil *fileAuthorityStub
	result, err := session.wrapOpenResult(storage.OpenResult{File: typedNil}, failure)
	if !errors.Is(err, failure) || result.File != nil {
		t.Fatalf("typed-nil result=%+v error=%v", result, err)
	}
}

func TestIdentityCapabilityChecksRejectAnAuthorityThatDoesNotAdvertiseThem(t *testing.T) {
	session := retainedTestSession(t, &fileSessionStub{})
	for name, check := range map[string]func() error{
		"atomic open": session.CheckAtomicFileOpen,
		"namespace":   session.CheckNamespaceAccess,
		"references":  session.CheckNodeReferences,
		"actions":     session.CheckFileActions,
	} {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("%s check=%v", name, err)
		}
	}
	if _, err := session.OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{}}, storage.OpenAtOptions{}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("atomic open dispatch=%v", err)
	}
	if _, err := session.LookupAt(t.Context(), storage.ChildName{}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("namespace dispatch=%v", err)
	}
	if _, err := session.OpenNodeRef(t.Context(), 1, storage.NodeRefOptions{}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("reference dispatch=%v", err)
	}
	if _, err := session.QueryFileAction(t.Context(), storage.FileActionID("1:00000000000000000000000000000000")); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("action dispatch=%v", err)
	}
}

func TestAtomicOpenRetainsAReferenceWhenFailureCleanupIsUnknown(t *testing.T) {
	session := retainedTestSession(t, nil)
	openFailure := errors.New("open response reported failure")
	cleanupFailure := errors.New("close response was lost")

	plain := &cleanupFileStub{closeErr: cleanupFailure}
	partial, err := session.wrapOpenResult(storage.OpenResult{
		File: plain, Attr: storage.Attr{ID: 17}, Outcome: storage.Opened,
	}, openFailure)
	if partial.File == nil || partial.Attr.ID != 17 || !errors.Is(err, openFailure) || !errors.Is(err, cleanupFailure) || plain.closes != 1 {
		t.Fatalf("partial open=%+v error=%v closes=%d", partial, err, plain.closes)
	}
	failed := partial.File
	for name, call := range map[string]func() error{
		"stat": func() error { _, err := failed.Stat(t.Context()); return err },
		"setattr": func() error {
			_, err := failed.SetAttr(t.Context(), storage.AttrChange{})
			return err
		},
		"read":  func() error { _, err := failed.ReadAt(t.Context(), 0, 1); return err },
		"write": func() error { _, err := failed.WriteAt(t.Context(), 0, nil); return err },
		"truncate": func() error {
			_, err := failed.Truncate(t.Context(), 0)
			return err
		},
		"sync": func() error { return failed.Sync(t.Context()) },
		"scope check": func() error {
			return failed.(storage.ScopedReference).CheckScopedReference()
		},
		"scope": func() error {
			_, err := failed.(storage.ScopedReference).Scope(t.Context())
			return err
		},
		"state check": func() error {
			return failed.(storage.ReferenceStateAccess).CheckReferenceState()
		},
		"state": func() error {
			_, err := failed.(storage.ReferenceStateAccess).State(t.Context())
			return err
		},
	} {
		if err := call(); !errors.Is(err, openFailure) {
			t.Fatalf("failed-open %s=%v", name, err)
		}
	}
	plain.closeErr = nil
	if err := failed.Close(t.Context()); err != nil || plain.closes != 2 {
		t.Fatalf("cleanup retry=%v closes=%d", err, plain.closes)
	}

	barrierFile := &fileAuthorityStub{close: func(context.Context) error { return cleanupFailure }}
	partial, err = session.wrapOpenResult(storage.OpenResult{File: barrierFile, Attr: storage.Attr{ID: 18}}, openFailure)
	if _, ok := partial.File.(*retainedFile); !ok || !errors.Is(err, openFailure) || !errors.Is(err, cleanupFailure) {
		t.Fatalf("barrier-capable partial open=%T error=%v", partial.File, err)
	}
}

func TestNodeOpenRetainsAReferenceWhenFailureCleanupIsUnknown(t *testing.T) {
	session := retainedTestSession(t, nil)
	openFailure := errors.New("node open response reported failure")
	cleanupFailure := errors.New("node close response was lost")

	plain := &cleanupReferenceStub{closeErr: cleanupFailure}
	result, err := session.openReference(t.Context(), func(context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
		return storage.NodeOpenResult{Reference: plain, Attr: storage.Attr{ID: 23}, Outcome: storage.Opened}, nil, openFailure
	})
	if result.Reference == nil || result.Attr.ID != 23 || !errors.Is(err, openFailure) || !errors.Is(err, cleanupFailure) || plain.closes != 1 {
		t.Fatalf("partial node open=%+v error=%v closes=%d", result, err, plain.closes)
	}
	failed := result.Reference
	for name, call := range map[string]func() error{
		"stat": func() error { _, err := failed.Stat(t.Context()); return err },
		"setattr": func() error {
			_, err := failed.SetAttr(t.Context(), storage.AttrChange{})
			return err
		},
		"scope check": failed.CheckScopedReference,
		"scope":       func() error { _, err := failed.Scope(t.Context()); return err },
		"state check": failed.CheckReferenceState,
		"state":       func() error { _, err := failed.State(t.Context()); return err },
	} {
		if err := call(); !errors.Is(err, openFailure) {
			t.Fatalf("failed node-open %s=%v", name, err)
		}
	}
	plain.closeErr = nil
	if err := failed.Close(t.Context()); err != nil || plain.closes != 2 {
		t.Fatalf("node cleanup retry=%v closes=%d", err, plain.closes)
	}

	barrierReference := &barrierReferenceStub{closeErr: cleanupFailure}
	result, err = session.openReference(t.Context(), func(context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
		return storage.NodeOpenResult{Reference: barrierReference, Attr: storage.Attr{ID: 24}}, nil, openFailure
	})
	if _, ok := result.Reference.(*nodeReference); !ok || !errors.Is(err, openFailure) || !errors.Is(err, cleanupFailure) || barrierReference.closes != 1 {
		t.Fatalf("barrier-capable partial node open=%T error=%v closes=%d", result.Reference, err, barrierReference.closes)
	}
}

func TestNodeOpenRejectsAReferenceWithoutReplicationBarriers(t *testing.T) {
	session := retainedTestSession(t, nil)
	plain := &cleanupReferenceStub{}
	result, err := session.openReference(t.Context(), func(context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
		return storage.NodeOpenResult{Reference: plain, Attr: storage.Attr{ID: 25}}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	})
	if result.Reference != nil || !errors.Is(err, syscall.EIO) || plain.closes != 1 {
		t.Fatalf("unsupported node open=%+v error=%v closes=%d", result, err, plain.closes)
	}

	cleanupFailure := errors.New("unsupported node cleanup failed")
	plain = &cleanupReferenceStub{closeErr: cleanupFailure}
	result, err = session.openReference(t.Context(), func(context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
		return storage.NodeOpenResult{Reference: plain, Attr: storage.Attr{ID: 26}}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	})
	if result.Reference == nil || result.Attr.ID != 26 || !errors.Is(err, syscall.EIO) || !errors.Is(err, cleanupFailure) || plain.closes != 1 {
		t.Fatalf("retained unsupported node open=%+v error=%v closes=%d", result, err, plain.closes)
	}
	plain.closeErr = nil
	if err := result.Reference.Close(t.Context()); err != nil || plain.closes != 2 {
		t.Fatalf("unsupported node cleanup retry=%v closes=%d", err, plain.closes)
	}
}

func TestNodeReferenceConditionalMutationPreservesKindsAndPartialAuthorityResults(t *testing.T) {
	for _, test := range []struct {
		name string
		kind storage.NodeKind
	}{
		{name: "directory", kind: storage.NodeDirectory},
		{name: "symlink", kind: storage.NodeSymlink},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := retainedTestSession(t, nil)
			failure := errors.New("mutation response ended after authority publication")
			remote := &barrierReferenceStub{
				mutation:    storage.Attr{ID: 31, Kind: test.kind, Metadata: map[string]storage.OpaquePayload{"test.atomic": {Data: []byte("value")}}},
				mutationErr: failure,
			}
			reference := &nodeReference{session: session, remote: remote}
			if err := reference.CheckConditionalFileMutation(); err != nil {
				t.Fatal(err)
			}
			command := storage.FileMutation{
				Action: storage.FileActionID("1:00000000000000000000000000000000"), Kind: storage.MutateAttributes,
				Metadata: map[string]storage.OpaquePayload{"test.atomic": {Data: []byte("value")}},
			}
			result, err := reference.MutateFile(t.Context(), command)
			if !errors.Is(err, failure) || result.ID != 31 || result.Kind != test.kind || string(result.Metadata["test.atomic"].Data) != "value" {
				t.Fatalf("partial mutation=%+v error=%v", result, err)
			}
			if remote.mutationCalls != 1 || remote.lastMutation.Action != command.Action || remote.lastMutation.Kind != storage.MutateAttributes {
				t.Fatalf("forwarded mutation=%+v calls=%d", remote.lastMutation, remote.mutationCalls)
			}

			remote.mutationErr = nil
			remote.barrier = &httprest.MutationBarrier{Incarnation: "log"}
			result, err = reference.MutateFile(t.Context(), command)
			if err != nil || result.ID != 31 || result.Kind != test.kind || remote.mutationCalls != 2 {
				t.Fatalf("confirmed mutation=%+v error=%v calls=%d", result, err, remote.mutationCalls)
			}
		})
	}
}

func TestAtomicOpenRejectsAFileWithoutReplicationBarriers(t *testing.T) {
	session := retainedTestSession(t, nil)
	plain := &cleanupFileStub{}
	result, err := session.wrapOpenResult(storage.OpenResult{File: plain, Attr: storage.Attr{ID: 19}}, nil)
	if result.File != nil || !errors.Is(err, syscall.EIO) || plain.closes != 1 {
		t.Fatalf("unsupported open=%+v error=%v closes=%d", result, err, plain.closes)
	}

	cleanupFailure := errors.New("unsupported file cleanup failed")
	plain = &cleanupFileStub{closeErr: cleanupFailure}
	result, err = session.wrapOpenResult(storage.OpenResult{File: plain, Attr: storage.Attr{ID: 20}}, nil)
	if result.File == nil || result.Attr.ID != 20 || !errors.Is(err, syscall.EIO) || !errors.Is(err, cleanupFailure) || plain.closes != 1 {
		t.Fatalf("retained unsupported open=%+v error=%v closes=%d", result, err, plain.closes)
	}
	plain.closeErr = nil
	if err := result.File.Close(t.Context()); err != nil || plain.closes != 2 {
		t.Fatalf("unsupported cleanup retry=%v closes=%d", err, plain.closes)
	}
}
