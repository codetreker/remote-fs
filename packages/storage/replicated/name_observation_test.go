package replicated

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type metadataObserverAuthority struct {
	*fileSessionStub
	checkError error
	observe    func(context.Context, storage.DirectoryTarget, storage.DirectoryMetadataOptions, *storage.ListResult) (storage.DirectoryMetadataObservation, error)
}

func (p *metadataObserverAuthority) CheckDirectoryMetadataObservation() error { return p.checkError }
func (p *metadataObserverAuthority) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	return p.observe(ctx, target, options, result)
}

type nameObserverAuthority struct {
	*fileAuthorityStub
	identity      uint64
	identityError error
	checkError    error
	observe       func(context.Context, *storage.NamespaceGuards) (storage.NameObservation, error)
}

func (p *nameObserverAuthority) CheckReferenceNameObservation() error { return p.checkError }
func (p *nameObserverAuthority) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	return p.observe(ctx, guards)
}
func (p *nameObserverAuthority) ReferenceNodeID() (uint64, error) { return p.identity, p.identityError }

type nodeNameObserverAuthority struct {
	storage.NodeReference
	*nameObserverAuthority
}

type nodeAuthorityWithoutName struct {
	storage.NodeReference
}

func (*nodeAuthorityWithoutName) SetAttrWithBarrier(context.Context, storage.AttrChange) (storage.Attr, *httprest.MutationBarrier, error) {
	return storage.Attr{}, nil, syscall.EIO
}
func (*nodeAuthorityWithoutName) CloseWithBarrier(context.Context) (*httprest.MutationBarrier, error) {
	return nil, syscall.EIO
}
func (*nodeAuthorityWithoutName) CloseWithResultAndBarrier(context.Context) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	return storage.ReferenceCloseResult{}, nil, syscall.EIO
}

type nameObserverContextKey struct{}

type substitutedDirectoryAuthority struct {
	*fileSessionStub
}

type namespaceOnlyAuthority struct{ *fileSessionStub }

func (*namespaceOnlyAuthority) CheckNamespaceAccess() error { return nil }
func (*namespaceOnlyAuthority) LookupAt(context.Context, storage.ChildName) (storage.Attr, error) {
	return storage.Attr{ID: 11, Kind: storage.NodeRegular}, nil
}
func (*namespaceOnlyAuthority) MutateName(context.Context, storage.NameCommand) (storage.NameResult, error) {
	return storage.NameResult{}, nil
}
func (*namespaceOnlyAuthority) MutateNameWithBarrier(context.Context, storage.NameCommand) (storage.NameResult, *httprest.MutationBarrier, error) {
	return storage.NameResult{}, nil, nil
}

func (*substitutedDirectoryAuthority) CheckDirectoryRead() error { return nil }
func (*substitutedDirectoryAuthority) ReadDirNode(_ context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	return storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: target.NodeID + 1, Revision: []byte{1}}}, nil
}
func (*substitutedDirectoryAuthority) ReadDirNodeBounded(_ context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	if err := result.Add(storage.Entry{Name: "entry", Attr: storage.Attr{ID: target.NodeID + 2, Kind: storage.NodeRegular}}); err != nil {
		return storage.DirectoryObservation{}, err
	}
	return storage.DirectoryObservation{ParentID: target.NodeID + 1, Revision: []byte{1}}, nil
}

func TestReplicatedNamespaceRejectsSubstitutedAuthorityDirectory(t *testing.T) {
	remote := &substitutedDirectoryAuthority{fileSessionStub: &fileSessionStub{}}
	session := retainedTestSession(t, remote)
	if err := session.CheckDirectoryRead(); err != nil {
		t.Fatal(err)
	}
	if err := session.CheckNamespaceAccess(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("directory-only authority exposed namespace mutation: %v", err)
	}
	target := storage.DirectoryTarget{NodeID: 9}
	if observed, err := session.ReadDirNode(t.Context(), target); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(observed, storage.ObservedDirectory{}) {
		t.Fatalf("substituted directory = %+v, %v", observed, err)
	}
	result, err := storage.NewListResult(4096, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + metadataBytes + 64, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed, err := session.ReadDirNodeBounded(t.Context(), target, result); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(observed, storage.DirectoryObservation{}) {
		t.Fatalf("substituted bounded directory = %+v, %v", observed, err)
	}
	if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("substituted bounded directory exposed %+v, %v", entries, err)
	}
}

func TestReplicatedNamespaceCapabilityIsIndependentOfDirectoryRead(t *testing.T) {
	session := retainedTestSession(t, &namespaceOnlyAuthority{fileSessionStub: &fileSessionStub{}})
	if err := session.CheckNamespaceAccess(); err != nil {
		t.Fatal(err)
	}
	if err := session.CheckDirectoryRead(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("namespace-only authority exposed directory read: %v", err)
	}
	if attr, err := session.LookupAt(t.Context(), storage.ChildName{}); err != nil || attr.ID != 11 {
		t.Fatalf("namespace-only lookup = %+v, %v", attr, err)
	}
}

func TestReplicatedDirectoryMetadataUsesOrdinaryAdmissionAndOnePrefix(t *testing.T) {
	for _, scenario := range []string{"plain", "named", "budget refusal", "capture failure", "malformed", "check failure", "unsupported", "unhealthy replica", "retired session"} {
		t.Run(scenario, func(t *testing.T) {
			failure := errors.New("authority metadata unavailable")
			probe := &metadataObserverAuthority{fileSessionStub: &fileSessionStub{}}
			session := retainedTestSession(t, probe)
			if scenario == "unsupported" {
				session.remote = &fileSessionStub{}
			}
			if scenario == "unhealthy replica" {
				session.base.failure = failure
			}
			if scenario == "retired session" {
				session.closing = true
			}
			if scenario == "check failure" {
				probe.checkError = failure
			}
			guards := &storage.NamespaceGuards{RootID: 7, Directories: []storage.DirectoryObservation{{ParentID: 7, Revision: []byte{1}}}, Edges: []storage.ObservedEdge{{ParentID: 7, RawLeaf: []byte("dir"), ChildID: 9}}}
			target := storage.DirectoryTarget{NodeID: 9, Scope: &storage.UseScope{Token: "directory"}}
			options := storage.DirectoryMetadataOptions{Guards: guards, IncludeName: scenario != "plain"}
			calls, budgets, loaded := 0, 0, false
			ctx := context.WithValue(t.Context(), nameObserverContextKey{}, "request")
			ctx = storage.WithNameObservationBudget(ctx, func(scalar storage.NameObservation, n int64) (int64, error) {
				budgets++
				if scalar.NodeID != 9 || scalar.RawLeaf != nil || n != 3 {
					t.Fatalf("prefix header=%+v,%d", scalar, n)
				}
				return storage.NameObservationRetentionBytes(n)
			})
			prefix, _ := storage.NameObservationRetentionBytes(3)
			bound := int64(81)
			if options.IncludeName {
				bound += prefix
			}
			if scenario == "budget refusal" {
				bound = 10 + prefix - 1
			}
			result, err := storage.NewListResult(bound, 10, func(_ int, n, m int64, _ storage.Attr) (int64, error) { return 64 + n + m, nil })
			if err != nil {
				t.Fatal(err)
			}
			probe.observe = func(call context.Context, got storage.DirectoryTarget, opts storage.DirectoryMetadataOptions, collector *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
				calls++
				if call.Value(nameObserverContextKey{}) != "request" || !reflect.DeepEqual(got, target) || !reflect.DeepEqual(opts, options) || collector != result {
					t.Fatal("authority inputs changed")
				}
				session.mu.Lock()
				active := session.active
				session.mu.Unlock()
				session.base.mu.Lock()
				confirmations := session.base.activeConfirmations
				session.base.mu.Unlock()
				if active != 1 || confirmations != 0 {
					t.Fatalf("observer admission active=%d,mutation confirmations=%d", active, confirmations)
				}
				out := storage.DirectoryMetadataObservation{Observation: storage.DirectoryObservation{ParentID: 9, Revision: []byte{2}}}
				if opts.IncludeName {
					scalar := storage.NameObservation{NodeID: 9, ParentID: 7, State: storage.NameLinked}
					cost, err := storage.CheckNameObservationBudget(call, scalar, 3)
					if err != nil {
						return out, err
					}
					if err := collector.ReservePrefix(cost); err != nil {
						return out, err
					}
					scalar.RawLeaf = []byte("dir")
					out.Name = &scalar
				}
				loaded = true
				if err := collector.Add(storage.Entry{Name: "f", Attr: storage.Attr{ID: 31, Kind: storage.NodeRegular}}); err != nil {
					return out, err
				}
				if scenario == "capture failure" {
					return out, failure
				}
				if scenario == "malformed" {
					out.Name = nil
				}
				return out, nil
			}
			check := session.CheckDirectoryMetadataObservation()
			out, err := session.ObserveDirectoryMetadata(ctx, target, options, result)
			if scenario == "plain" || scenario == "named" {
				if check != nil || err != nil || out.Observation.ParentID != 9 || options.IncludeName != (out.Name != nil) {
					t.Fatalf("observation=%+v,%v,check=%v", out, err, check)
				}
				if entries, err := result.Entries(); err != nil || len(entries) != 1 {
					t.Fatalf("entries=%+v,%v", entries, err)
				}
			} else {
				if err == nil || !reflect.DeepEqual(out, storage.DirectoryMetadataObservation{}) {
					t.Fatalf("failed observation usable: %+v,%v", out, err)
				}
				if entries, e := result.Entries(); entries != nil || !errors.Is(e, err) {
					t.Fatalf("partial failed entries=%+v,%v", entries, e)
				}
				if scenario == "unsupported" && (!errors.Is(err, syscall.EOPNOTSUPP) || !errors.Is(check, syscall.EOPNOTSUPP)) {
					t.Fatalf("unsupported=%v,%v", check, err)
				}
				if scenario == "unhealthy replica" && !errors.Is(err, syscall.EIO) {
					t.Fatal(err)
				}
				if scenario == "retired session" && !errors.Is(err, syscall.ESTALE) {
					t.Fatal(err)
				}
				if (scenario == "capture failure" || scenario == "check failure") && !errors.Is(err, failure) {
					t.Fatal(err)
				}
			}
			if scenario == "unsupported" || scenario == "unhealthy replica" || scenario == "retired session" || scenario == "check failure" {
				if calls != 0 {
					t.Fatal("refused read reached authority")
				}
			} else if calls != 1 {
				t.Fatalf("authority calls=%d", calls)
			}
			if scenario == "budget refusal" && loaded {
				t.Fatal("over-budget observation loaded payload")
			}
			if scenario == "plain" && budgets != 0 {
				t.Fatal("unrequested own name was charged")
			}
			if session.active != 0 || session.base.activeConfirmations != 0 {
				t.Fatal("observation retained operation or mutation record")
			}
		})
	}
}

func TestReplicatedNameObservationPreservesIdentityAndOrdinaryReadAdmission(t *testing.T) {
	for _, kind := range []string{"file", "node"} {
		for _, scenario := range []string{"linked", "root", "detached", "capture failure", "malformed", "unsupported", "check failure", "identity failure", "zero identity", "wrong identity", "unhealthy replica", "retired session", "cancelled"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				failure := errors.New("authority name unavailable")
				session := retainedTestSession(t, nil)
				probe := &nameObserverAuthority{fileAuthorityStub: &fileAuthorityStub{}, identity: 31}
				if scenario == "check failure" {
					probe.checkError = failure
				}
				if scenario == "identity failure" {
					probe.identityError = failure
				}
				if scenario == "zero identity" {
					probe.identity = 0
				}
				if scenario == "wrong identity" {
					probe.identity = 99
				}
				if scenario == "unhealthy replica" {
					session.base.failure = failure
				}
				if scenario == "retired session" {
					session.closing = true
				}
				ctx := context.WithValue(t.Context(), nameObserverContextKey{}, "name-request")
				if scenario == "cancelled" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				calls, budgets := 0, 0
				ctx = storage.WithNameObservationBudget(ctx, func(_ storage.NameObservation, n int64) (int64, error) {
					budgets++
					return storage.NameObservationRetentionBytes(n)
				})
				guards := &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{{ParentID: 9, Revision: []byte{1}}}}
				want := storage.NameObservation{NodeID: 31, State: storage.NameLinked, ParentID: 9, RawLeaf: []byte{'f', 0xff}}
				if scenario == "root" {
					want = storage.NameObservation{NodeID: 31, State: storage.NameRoot}
				}
				if scenario == "detached" {
					want = storage.NameObservation{NodeID: 31, State: storage.NameDetached}
				}
				probe.observe = func(call context.Context, got *storage.NamespaceGuards) (storage.NameObservation, error) {
					calls++
					if got != guards || call.Value(nameObserverContextKey{}) != "name-request" {
						t.Fatal("authority name guards/context changed")
					}
					session.mu.Lock()
					active := session.active
					session.mu.Unlock()
					if active != 1 || session.base.activeConfirmations != 0 {
						t.Fatalf("name did not use ordinary reference operation admission: %d", active)
					}
					scalar := want
					scalar.RawLeaf = nil
					if _, err := storage.CheckNameObservationBudget(call, scalar, int64(len(want.RawLeaf))); err != nil {
						return want, err
					}
					if scenario == "capture failure" {
						return want, failure
					}
					if scenario == "malformed" {
						out := want
						out.ParentID = 0
						return out, nil
					}
					return want, nil
				}
				var observer storage.ReferenceNameObserver
				if kind == "file" {
					file := &retainedFile{session: session, remote: probe}
					if scenario == "unsupported" {
						file.remote = &fileAuthorityStub{}
					}
					observer = file
				} else {
					ref := &nodeReference{session: session, remote: &nodeNameObserverAuthority{nameObserverAuthority: probe}}
					if scenario == "unsupported" {
						ref.remote = &nodeAuthorityWithoutName{}
					}
					observer = ref
				}
				check := observer.CheckReferenceNameObservation()
				id, idErr := observer.(storage.ReferenceIdentity).ReferenceNodeID()
				if scenario == "unsupported" {
					if id != 0 || !errors.Is(idErr, syscall.EOPNOTSUPP) {
						t.Fatalf("missing identity=%d,%v", id, idErr)
					}
				} else if scenario == "identity failure" {
					if id != 0 || !errors.Is(idErr, failure) {
						t.Fatalf("identity=%d,%v", id, idErr)
					}
				} else if scenario == "zero identity" {
					if id != 0 || !errors.Is(idErr, syscall.EIO) {
						t.Fatalf("zero identity=%d,%v", id, idErr)
					}
				} else if idErr != nil || id != probe.identity {
					t.Fatalf("identity=%d,%v", id, idErr)
				}
				got, err := observer.ObserveName(ctx, guards)
				if scenario == "linked" || scenario == "root" || scenario == "detached" {
					if check != nil || err != nil || !reflect.DeepEqual(got, want) || budgets != 1 {
						t.Fatalf("name=%+v,%v,check=%v,budgets=%d", got, err, check, budgets)
					}
				} else {
					if err == nil || !reflect.DeepEqual(got, storage.NameObservation{}) {
						t.Fatalf("failed name usable: %+v,%v", got, err)
					}
					if (scenario == "capture failure" || scenario == "check failure" || scenario == "identity failure") && !errors.Is(err, failure) {
						t.Fatal(err)
					}
					if scenario == "unsupported" && (!errors.Is(check, syscall.EOPNOTSUPP) || !errors.Is(err, syscall.EOPNOTSUPP)) {
						t.Fatalf("unsupported=%v,%v", check, err)
					}
					if scenario == "unhealthy replica" && !errors.Is(err, syscall.EIO) {
						t.Fatal(err)
					}
					if scenario == "retired session" && !errors.Is(err, syscall.ESTALE) {
						t.Fatal(err)
					}
					if scenario == "cancelled" && !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				}
				if scenario == "unsupported" || scenario == "check failure" || scenario == "identity failure" || scenario == "zero identity" || scenario == "unhealthy replica" || scenario == "retired session" {
					if calls != 0 {
						t.Fatal("refused name read reached authority")
					}
				} else if calls != 1 {
					t.Fatalf("name calls=%d", calls)
				}
				if session.active != 0 || session.base.activeConfirmations != 0 {
					t.Fatal("name observation retained operation or mutation confirmation")
				}
			})
		}
	}
}
