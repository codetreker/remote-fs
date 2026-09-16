package limited

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type directoryObserverProbe struct {
	storage.FileSession
	checkError error
	observe    func(context.Context, storage.DirectoryTarget, storage.DirectoryMetadataOptions, *storage.ListResult) (storage.DirectoryMetadataObservation, error)
}

func (p *directoryObserverProbe) CheckDirectoryMetadataObservation() error { return p.checkError }
func (p *directoryObserverProbe) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	return p.observe(ctx, target, options, result)
}

type referenceObserverProbe struct {
	storage.File
	identity      uint64
	identityError error
	checkError    error
	observe       func(context.Context, *storage.NamespaceGuards) (storage.NameObservation, error)
}

func (p *referenceObserverProbe) ReferenceNodeID() (uint64, error) {
	return p.identity, p.identityError
}
func (p *referenceObserverProbe) CheckReferenceNameObservation() error { return p.checkError }
func (p *referenceObserverProbe) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	return p.observe(ctx, guards)
}

type observerContextKey struct{}

func TestDirectoryMetadataForwardingKeepsActualPrefixBudgetAndErrors(t *testing.T) {
	for _, scenario := range []string{"plain", "named", "budget refusal", "capture failure", "wrong target", "omitted name", "check failure", "unsupported"} {
		t.Run(scenario, func(t *testing.T) {
			failure := errors.New("metadata capture unavailable")
			guards := &storage.NamespaceGuards{RootID: 7, Directories: []storage.DirectoryObservation{{ParentID: 7, Revision: []byte{1}}}, Edges: []storage.ObservedEdge{{ParentID: 7, RawLeaf: []byte("dir"), ChildID: 9}}}
			target := storage.DirectoryTarget{NodeID: 9, Scope: &storage.UseScope{Token: "directory"}}
			options := storage.DirectoryMetadataOptions{Guards: guards, IncludeName: scenario != "plain"}
			budgetCalls, loaded, calls := 0, false, 0
			ctx := context.WithValue(t.Context(), observerContextKey{}, "request")
			ctx = storage.WithPublicationAccounting(ctx, func(int64, int64) (storage.PublicationSettlement, error) {
				t.Fatal("read observation prepared publication")
				return nil, nil
			})
			chain := storage.PublicationAccountingFrom(ctx)
			ctx = storage.WithNameObservationBudget(ctx, func(scalar storage.NameObservation, n int64) (int64, error) {
				budgetCalls++
				if scalar.NodeID != 9 || scalar.RawLeaf != nil || n != 3 {
					t.Fatalf("budget header=%+v,length=%d", scalar, n)
				}
				cost, err := storage.NameObservationRetentionBytes(n)
				return cost + 32, err
			})
			prefix, _ := storage.NameObservationRetentionBytes(3)
			bound := int64(10 + 71)
			if options.IncludeName {
				bound += prefix + 32
			}
			if scenario == "budget refusal" {
				bound = 10 + prefix + 31
			}
			result, err := storage.NewListResult(bound, 10, func(_ int, name, metadata int64, _ storage.Attr) (int64, error) { return 64 + name + metadata, nil })
			if err != nil {
				t.Fatal(err)
			}
			probe := &directoryObserverProbe{}
			probe.observe = func(call context.Context, got storage.DirectoryTarget, opts storage.DirectoryMetadataOptions, collector *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
				calls++
				if call.Value(observerContextKey{}) != "request" || storage.PublicationAccountingFrom(call) != chain || !reflect.DeepEqual(got, target) || !reflect.DeepEqual(opts, options) || collector != result {
					t.Fatal("request scope, guards, collector, or accounting changed")
				}
				out := storage.DirectoryMetadataObservation{Observation: storage.DirectoryObservation{ParentID: 9, Revision: []byte{2}}}
				if opts.IncludeName {
					scalar := storage.NameObservation{NodeID: 9, State: storage.NameLinked, ParentID: 7}
					charge, err := storage.CheckNameObservationBudget(call, scalar, 3)
					if err != nil {
						return out, err
					}
					if err := collector.ReservePrefix(charge); err != nil {
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
				if scenario == "wrong target" {
					out.Observation.ParentID = 11
				}
				if scenario == "omitted name" {
					out.Name = nil
				}
				return out, nil
			}
			var backing storage.FileSession = probe
			if scenario == "check failure" {
				probe.checkError = failure
			}
			if scenario == "unsupported" {
				backing = &struct{ storage.FileSession }{}
			}
			session := &fileSession{FileSession: backing, storage: &Storage{limit: MinLimit}}
			check := session.CheckDirectoryMetadataObservation()
			out, err := session.ObserveDirectoryMetadata(ctx, target, options, result)
			if scenario == "plain" || scenario == "named" {
				if check != nil || err != nil || out.Observation.ParentID != 9 || options.IncludeName != (out.Name != nil) {
					t.Fatalf("observation=%+v,%v,check=%v", out, err, check)
				}
				entries, err := result.Entries()
				if err != nil || len(entries) != 1 {
					t.Fatalf("entries=%+v,%v", entries, err)
				}
			} else {
				if err == nil || !reflect.DeepEqual(out, storage.DirectoryMetadataObservation{}) {
					t.Fatalf("failed observation remained usable: %+v,%v", out, err)
				}
				if entries, entryErr := result.Entries(); entries != nil || !errors.Is(entryErr, err) {
					t.Fatalf("failed collector=%+v,%v", entries, entryErr)
				}
				if (scenario == "capture failure" || scenario == "check failure") && !errors.Is(err, failure) {
					t.Fatal(err)
				}
				if scenario == "unsupported" && (!errors.Is(err, syscall.EOPNOTSUPP) || !errors.Is(check, syscall.EOPNOTSUPP)) {
					t.Fatalf("unsupported=%v,%v", check, err)
				}
			}
			if scenario == "unsupported" || scenario == "check failure" {
				if calls != 0 {
					t.Fatal("refused capability reached producer")
				}
			} else if calls != 1 {
				t.Fatalf("producer calls=%d", calls)
			}
			if scenario == "budget refusal" && loaded {
				t.Fatal("unfunded name/entry payload was loaded")
			}
			if (scenario == "plain" || scenario == "unsupported" || scenario == "check failure") && budgetCalls != 0 {
				t.Fatal("unrequested prefix was charged")
			}
		})
	}
}

func TestReferenceNameForwardingPreservesOnlyConfirmedBindingFacts(t *testing.T) {
	for _, referenceKind := range []string{"file", "node"} {
		for _, scenario := range []string{"linked", "root", "detached", "capture failure", "cancelled", "malformed", "check failure", "unsupported", "identity error", "zero identity", "missing identity", "wrong identity"} {
			t.Run(referenceKind+"/"+scenario, func(t *testing.T) {
				failure := errors.New("name capture unavailable")
				guards := &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{{ParentID: 9, Revision: []byte{1}}}}
				ctx := context.WithValue(t.Context(), observerContextKey{}, "name-request")
				if scenario == "cancelled" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				budgets, calls := 0, 0
				ctx = storage.WithNameObservationBudget(ctx, func(scalar storage.NameObservation, n int64) (int64, error) {
					budgets++
					return storage.NameObservationRetentionBytes(n)
				})
				want := storage.NameObservation{NodeID: 31, State: storage.NameLinked, ParentID: 9, RawLeaf: []byte{'f', 0xff}}
				if scenario == "root" {
					want = storage.NameObservation{NodeID: 31, State: storage.NameRoot}
				}
				if scenario == "detached" {
					want = storage.NameObservation{NodeID: 31, State: storage.NameDetached}
				}
				probe := &referenceObserverProbe{identity: 31}
				probe.observe = func(call context.Context, got *storage.NamespaceGuards) (storage.NameObservation, error) {
					calls++
					if got != guards || call.Value(observerContextKey{}) != "name-request" {
						t.Fatal("name guards or request context changed")
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
						bad := want
						bad.ParentID = 0
						return bad, nil
					}
					return want, nil
				}
				if scenario == "check failure" {
					probe.checkError = failure
				}
				if scenario == "identity error" {
					probe.identityError = failure
				}
				if scenario == "zero identity" {
					probe.identity = 0
				}
				if scenario == "wrong identity" {
					probe.identity = 99
				}
				var native storage.File = probe
				if scenario == "missing identity" {
					native = &struct {
						storage.File
						storage.ReferenceNameObserver
					}{ReferenceNameObserver: probe}
				}
				if scenario == "unsupported" {
					native = &struct{ storage.File }{}
				}
				var reference storage.NodeReference
				if referenceKind == "file" {
					reference = wrapFile(&Storage{limit: MinLimit}, native)
				} else {
					reference = wrapNodeReference(&Storage{limit: MinLimit}, native)
				}
				observer := reference.(storage.ReferenceNameObserver)
				check := observer.CheckReferenceNameObservation()
				id, identityErr := reference.(storage.ReferenceIdentity).ReferenceNodeID()
				if scenario == "unsupported" || scenario == "missing identity" {
					if id != 0 || !errors.Is(identityErr, syscall.EOPNOTSUPP) {
						t.Fatalf("missing identity=%d,%v", id, identityErr)
					}
				} else if scenario == "identity error" {
					if id != 0 || !errors.Is(identityErr, failure) {
						t.Fatalf("identity failure=%d,%v", id, identityErr)
					}
				} else if scenario == "zero identity" {
					if id != 0 || !errors.Is(identityErr, syscall.EIO) {
						t.Fatalf("zero identity=%d,%v", id, identityErr)
					}
				} else if identityErr != nil || id != probe.identity {
					t.Fatalf("identity changed=%d,%v", id, identityErr)
				}
				got, err := observer.ObserveName(ctx, guards)
				if scenario == "linked" || scenario == "root" || scenario == "detached" {
					if check != nil || err != nil || !reflect.DeepEqual(got, want) || budgets != 1 {
						t.Fatalf("name=%+v,%v,check=%v,budgets=%d", got, err, check, budgets)
					}
				} else {
					if err == nil || !reflect.DeepEqual(got, storage.NameObservation{}) {
						t.Fatalf("unconfirmed name exposed: %+v,%v", got, err)
					}
					if scenario == "cancelled" && !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
					if (scenario == "capture failure" || scenario == "check failure" || scenario == "identity error") && !errors.Is(err, failure) {
						t.Fatal(err)
					}
					if scenario == "unsupported" && (!errors.Is(err, syscall.EOPNOTSUPP) || !errors.Is(check, syscall.EOPNOTSUPP)) {
						t.Fatalf("unsupported=%v,%v", check, err)
					}
				}
				if scenario == "unsupported" || scenario == "check failure" || scenario == "identity error" || scenario == "zero identity" || scenario == "missing identity" {
					if calls != 0 {
						t.Fatal("refused name capability reached producer")
					}
				} else if calls != 1 {
					t.Fatalf("name calls=%d", calls)
				}
			})
		}
	}
}
