package replicated

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type contentMetadataAuthority struct {
	*fileAuthorityStub
	id                             uint64
	identityErr, checkErr, callErr error
	observed                       storage.ContentMetadataObservation
	calls                          int
	index                          uint16
}

func (p *contentMetadataAuthority) ReferenceNodeID() (uint64, error) { return p.id, p.identityErr }
func (p *contentMetadataAuthority) CheckContentMetadata() error      { return p.checkErr }
func (p *contentMetadataAuthority) ObserveContentMetadata(_ context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	p.calls++
	p.index = index
	return p.observed, p.callErr
}
func (p *contentMetadataAuthority) Stat(context.Context) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}

func TestContentMetadataPreservesByteOnlyAuthorityAndOwnsObservation(t *testing.T) {
	want := storage.ContentMetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{'M', 0xff, 0}}}
	authority := &contentMetadataAuthority{fileAuthorityStub: &fileAuthorityStub{}, id: 19, observed: want}
	file := &retainedFile{session: retainedTestSession(t, &fileSessionStub{}), remote: authority}
	if err := file.CheckContentMetadata(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("public metadata access = %v", err)
	}
	got, err := file.ObserveContentMetadata(t.Context(), 3)
	if err != nil || !reflect.DeepEqual(got, want) || authority.index != 3 || authority.calls != 1 {
		t.Fatalf("observation=%+v, %v; calls=%d index=%d", got, err, authority.calls, authority.index)
	}
	got.Value.Version[0] = 9
	got.Value.Data[0] = 'X'
	if authority.observed.Value.Version[0] != 1 || authority.observed.Value.Data[0] != 'M' {
		t.Fatal("observation aliases authority payload")
	}
	authority.observed = storage.ContentMetadataObservation{NodeID: 19}
	if got, err := file.ObserveContentMetadata(t.Context(), 0); err != nil || got.NodeID != 19 || got.Value != nil {
		t.Fatalf("confirmed absence=%+v, %v", got, err)
	}
	authority.observed = storage.ContentMetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}}}
	if got, err := file.ObserveContentMetadata(t.Context(), 0); err != nil || got.Value == nil {
		t.Fatalf("present empty value became absent: %+v, %v", got, err)
	}
}

func TestContentMetadataRejectsAuthorityFailuresAndLocalRetirement(t *testing.T) {
	failure := errors.New("authority content metadata unavailable")
	for _, scenario := range []string{"missing capability", "missing identity", "check failure", "identity failure", "zero identity", "wrong identity", "malformed", "capture failure", "unhealthy replica", "retired session"} {
		t.Run(scenario, func(t *testing.T) {
			authority := &contentMetadataAuthority{fileAuthorityStub: &fileAuthorityStub{}, id: 19, observed: storage.ContentMetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{1}}}}
			session := retainedTestSession(t, &fileSessionStub{})
			var remote httprest.FileWithBarrier = authority
			switch scenario {
			case "missing capability":
				remote = &fileAuthorityStub{}
			case "missing identity":
				remote = &struct {
					httprest.FileWithBarrier
					storage.ReferenceContentMetadata
				}{FileWithBarrier: &fileAuthorityStub{}, ReferenceContentMetadata: authority}
			case "check failure":
				authority.checkErr = failure
			case "identity failure":
				authority.identityErr = failure
			case "zero identity":
				authority.id = 0
			case "wrong identity":
				authority.observed.NodeID = 20
			case "malformed":
				authority.observed.Value.Version = nil
			case "capture failure":
				authority.callErr = failure
			case "unhealthy replica":
				session.base.failure = failure
			case "retired session":
				session.closing = true
			}
			file := &retainedFile{session: session, remote: remote}
			got, err := file.ObserveContentMetadata(t.Context(), 0)
			if err == nil || !reflect.DeepEqual(got, storage.ContentMetadataObservation{}) {
				t.Fatalf("invalid observation=%+v, %v", got, err)
			}
			preflight := scenario == "missing capability" || scenario == "missing identity" || scenario == "check failure" || scenario == "identity failure" || scenario == "zero identity" || scenario == "unhealthy replica" || scenario == "retired session"
			if preflight && authority.calls != 0 {
				t.Fatalf("failed admission dispatched %d calls", authority.calls)
			}
			if (scenario == "check failure" || scenario == "identity failure" || scenario == "capture failure") && !errors.Is(err, failure) {
				t.Fatalf("lost failure cause: %v", err)
			}
		})
	}
}

type contentMutationAuthority struct {
	*fileAuthorityStub
	mutation *barrierReferenceStub
}

func (a *contentMutationAuthority) CheckConditionalFileMutation() error {
	return a.mutation.CheckConditionalFileMutation()
}

func (a *contentMutationAuthority) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	return a.mutation.MutateFile(ctx, command)
}

func (a *contentMutationAuthority) MutateFileWithBarrier(ctx context.Context, command storage.FileMutation) (storage.Attr, *httprest.MutationBarrier, error) {
	return a.mutation.MutateFileWithBarrier(ctx, command)
}

func TestContentEffectMutationRetainsOriginalCommandAndRequiresBarrier(t *testing.T) {
	command := storage.FileMutation{Action: storage.FileActionID("1:00000000000000000000000000000000"), Kind: storage.MutateWriteAt, Data: []byte("content"), ExpectedMetadata: map[string][]byte{"app.flags": {1}}, ContentEffects: []uint16{0}}
	for _, scenario := range []string{"confirmed", "missing barrier", "wrong log", "unknown publication"} {
		t.Run(scenario, func(t *testing.T) {
			remote := &contentMutationAuthority{fileAuthorityStub: &fileAuthorityStub{}, mutation: &barrierReferenceStub{
				mutation: storage.Attr{ID: 19, Kind: storage.NodeRegular, Size: int64(len(command.Data))}, barrier: &httprest.MutationBarrier{Incarnation: "log"},
			}}
			switch scenario {
			case "missing barrier":
				remote.mutation.barrier = nil
			case "wrong log":
				remote.mutation.barrier.Incarnation = "unrelated"
			case "unknown publication":
				remote.mutation.mutationErr = syscall.EIO
			}
			file := &retainedFile{session: retainedTestSession(t, &fileSessionStub{}), remote: remote}
			result, err := file.MutateFile(t.Context(), command)
			if scenario == "confirmed" {
				if err != nil || result.ID != 19 {
					t.Fatalf("confirmed effect write = %+v, %v", result, err)
				}
			} else if !errors.Is(err, syscall.EIO) {
				t.Fatalf("unsettled effect write reported success: %+v, %v", result, err)
			}
			if remote.mutation.mutationCalls != 1 || !reflect.DeepEqual(remote.mutation.lastMutation, command) {
				t.Fatalf("effect mutation changed original action or condition: %+v, calls=%d", remote.mutation.lastMutation, remote.mutation.mutationCalls)
			}
		})
	}
}

type contentSyncAuthority struct {
	*fileAuthorityStub
	calls   int
	err     error
	barrier *httprest.MutationBarrier
}

func (a *contentSyncAuthority) Sync(context.Context) error {
	panic("replicated Sync must consume the returned authority barrier")
}

func (a *contentSyncAuthority) SyncWithBarrier(context.Context) (*httprest.MutationBarrier, error) {
	a.calls++
	return a.barrier, a.err
}

func TestContentFlushForwardsEachConfirmationAndFailsOnReplicaRetirement(t *testing.T) {
	failure := errors.New("authority flush unavailable")
	authority := &contentSyncAuthority{fileAuthorityStub: &fileAuthorityStub{}, err: failure, barrier: &httprest.MutationBarrier{Incarnation: "log"}}
	session := retainedTestSession(t, &fileSessionStub{})
	file := &retainedFile{session: session, remote: authority}
	if err := file.Sync(t.Context()); !errors.Is(err, failure) || authority.calls != 1 {
		t.Fatalf("failed flush = %v, calls=%d", err, authority.calls)
	}
	authority.err = nil
	if err := file.Sync(t.Context()); err != nil || authority.calls != 2 {
		t.Fatalf("repeated confirmation = %v, calls=%d", err, authority.calls)
	}
	session.base.failure = failure
	if err := file.Sync(t.Context()); !errors.Is(err, syscall.EIO) || authority.calls != 2 {
		t.Fatalf("unhealthy replica flush = %v, calls=%d", err, authority.calls)
	}
	session.base.failure = nil
	session.closing = true
	if err := file.Sync(t.Context()); !errors.Is(err, syscall.ESTALE) || authority.calls != 2 {
		t.Fatalf("retired reference flush = %v, calls=%d", err, authority.calls)
	}
}

func TestContentFlushRequiresAuthorityBarrierAndExactReplicaConfirmation(t *testing.T) {
	for _, scenario := range []string{"confirm", "cancel", "missing barrier", "wrong log", "missing capability"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				authority := &contentSyncAuthority{fileAuthorityStub: &fileAuthorityStub{}, barrier: &httprest.MutationBarrier{Incarnation: "log", Position: 1}}
				session := retainedTestSession(t, &fileSessionStub{})
				file := &retainedFile{session: session, remote: authority}
				if scenario == "missing capability" {
					file.remote = &fileAuthorityStub{}
					if err := file.Sync(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) || authority.calls != 0 {
						t.Fatalf("missing sync confirmation = %v, calls=%d", err, authority.calls)
					}
					return
				}
				if scenario == "missing barrier" {
					authority.barrier = nil
				}
				if scenario == "wrong log" {
					authority.barrier.Incarnation = "different"
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- file.Sync(ctx) }()
				synctest.Wait()
				if scenario == "missing barrier" || scenario == "wrong log" {
					if err := <-done; !errors.Is(err, syscall.EIO) {
						t.Fatalf("unconfirmed flush = %v", err)
					}
				} else {
					select {
					case err := <-done:
						t.Fatalf("flush returned before replica held its barrier: %v", err)
					default:
					}
					if scenario == "cancel" {
						cancel()
					} else {
						session.base.mu.Lock()
						session.base.at = 1
						session.base.wake()
						session.base.mu.Unlock()
					}
					synctest.Wait()
					err := <-done
					if scenario == "cancel" && !errors.Is(err, syscall.EIO) || scenario == "confirm" && err != nil {
						t.Fatalf("flush confirmation = %v", err)
					}
				}
				if authority.calls != 1 || session.base.activeConfirmations != 0 {
					t.Fatalf("flush dispatched %d times and left %d confirmations", authority.calls, session.base.activeConfirmations)
				}
			})
		})
	}
}

type contentEnrollmentSessionProbe struct {
	*identitySessionProbe
	enrollmentErr error
}

func (p *contentEnrollmentSessionProbe) CheckOpenContentMetadata() error { return p.enrollmentErr }

func contentEnrollmentEffects() []storage.ContentMetadataEffect {
	return []storage.ContentMetadataEffect{{Namespace: "app.flags", PayloadBytes: 3, PresentPrefix: []byte{'M'}, AbsentPayload: []byte{'M', 0, 0}, ClearMask: []byte{0, 1, 0}, SetMask: []byte{0, 2, 0}}}
}

func TestOpenAtPreflightsAndOwnsContentEnrollment(t *testing.T) {
	probe := &contentEnrollmentSessionProbe{identitySessionProbe: &identitySessionProbe{opened: storage.OpenResult{File: &openIdentityFileProbe{id: 19}, Attr: storage.Attr{ID: 19, Kind: storage.NodeRegular}, Outcome: storage.Opened}}}
	wrapper := retainedTestSession(t, probe)
	options := storage.OpenAtOptions{Write: true, Existing: storage.Keep, ContentMetadataEffects: contentEnrollmentEffects()}
	if err := wrapper.CheckOpenContentMetadata(); err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options); err != nil {
		t.Fatal(err)
	}
	if probe.options.MetadataAccess != 0 || !probe.options.Write || !reflect.DeepEqual(probe.options.ContentMetadataEffects, options.ContentMetadataEffects) {
		t.Fatalf("enrollment changed public rights or descriptor: %+v", probe.options)
	}
	options.ContentMetadataEffects[0].Namespace = "app.other"
	options.ContentMetadataEffects[0].PresentPrefix[0] = 'X'
	options.ContentMetadataEffects[0].AbsentPayload[0] = 'X'
	options.ContentMetadataEffects[0].ClearMask[1] = 0xff
	options.ContentMetadataEffects[0].SetMask[1] = 0xff
	if !reflect.DeepEqual(probe.options.ContentMetadataEffects, contentEnrollmentEffects()) {
		t.Fatal("open retained caller descriptor aliases")
	}
	failure := errors.New("enrollment cannot be enforced")
	probe.enrollmentErr = failure
	options.ContentMetadataEffects = contentEnrollmentEffects()
	if result, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options); !reflect.DeepEqual(result, storage.OpenResult{}) || !errors.Is(err, failure) || probe.openCalls != 1 {
		t.Fatalf("failed enrollment=%+v, %v; calls=%d", result, err, probe.openCalls)
	}
	wrapper.remote = probe.identitySessionProbe
	if result, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options); !reflect.DeepEqual(result, storage.OpenResult{}) || !errors.Is(err, syscall.EOPNOTSUPP) || probe.openCalls != 1 {
		t.Fatalf("missing enrollment=%+v, %v; calls=%d", result, err, probe.openCalls)
	}
	options.ContentMetadataEffects = nil
	if _, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options); err != nil || probe.openCalls != 2 {
		t.Fatalf("unenrolled byte-only open=%v; calls=%d", err, probe.openCalls)
	}
	options.ContentMetadataEffects = contentEnrollmentEffects()
	options.ContentMetadataEffects[0].SetMask = nil
	if _, err := wrapper.OpenAt(t.Context(), storage.ChildSelection{}, options); !errors.Is(err, syscall.EINVAL) || probe.openCalls != 2 {
		t.Fatalf("malformed descriptor=%v; calls=%d", err, probe.openCalls)
	}
}
