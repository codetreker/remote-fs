package locked

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type directoryObservationProbe struct {
	storage.FileSession
	checkErr error
	observe  func(context.Context, storage.DirectoryTarget, storage.DirectoryMetadataOptions, *storage.ListResult) (storage.DirectoryMetadataObservation, error)
}

func (p *directoryObservationProbe) CheckDirectoryMetadataObservation() error { return p.checkErr }
func (p *directoryObservationProbe) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	return p.observe(ctx, target, options, result)
}

type nameObservationProbe struct {
	storage.File
	identity    uint64
	identityErr error
	checkErr    error
	observe     func(context.Context, *storage.NamespaceGuards) (storage.NameObservation, error)
}

func (p *nameObservationProbe) ReferenceNodeID() (uint64, error) { return p.identity, p.identityErr }

func (p *nameObservationProbe) CheckReferenceNameObservation() error { return p.checkErr }
func (p *nameObservationProbe) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	return p.observe(ctx, guards)
}

func TestDirectoryMetadataObservationPreservesReadAdmissionAndWholeResults(t *testing.T) {
	for _, scenario := range []string{"success", "no name", "prefix refusal", "backend failure", "malformed", "missing", "declined"} {
		t.Run(scenario, func(t *testing.T) {
			view, _, ctx := capabilityView()
			failure := errors.New("observation refused")
			guards := &storage.NamespaceGuards{RootID: 1}
			target := storage.DirectoryTarget{NodeID: 7, Scope: &storage.UseScope{Token: "original scope"}}
			options := storage.DirectoryMetadataOptions{Guards: guards, IncludeName: scenario != "no name"}
			name := storage.NameObservation{NodeID: 7, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte{0xff}}
			observation := storage.DirectoryMetadataObservation{Observation: storage.DirectoryObservation{ParentID: 7, Revision: []byte("revision")}}
			if options.IncludeName {
				observation.Name = &name
			}
			var budgetCalls, backendCalls, loaded int
			ctx = storage.WithNameObservationBudget(ctx, func(scalar storage.NameObservation, length int64) (int64, error) {
				budgetCalls++
				if scalar.RawLeaf != nil || scalar.NodeID != 7 || length != 1 || loaded != 0 {
					t.Fatal("name budget lost unloaded header or ran after payload loading")
				}
				return 520, nil
			})
			maximum := int64(521)
			if scenario == "prefix refusal" {
				maximum = 519
			}
			result, err := storage.NewListResult(maximum, 0, func(_ int, _, _ int64, _ storage.Attr) (int64, error) { return 1, nil })
			if err != nil {
				t.Fatal(err)
			}
			backend := &directoryObservationProbe{}
			backend.observe = func(callCtx context.Context, got storage.DirectoryTarget, gotOptions storage.DirectoryMetadataOptions, supplied *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
				backendCalls++
				if !reflect.DeepEqual(locking.ScopeFromContext(callCtx), locking.MutationScope{}) || callCtx.Value(probeContextKey{}) != "request trace" || !reflect.DeepEqual(got, target) || gotOptions.Guards != guards || gotOptions.IncludeName != options.IncludeName || supplied != result {
					t.Fatal("directory observation changed context, scope, guards, output selection or collector")
				}
				if options.IncludeName {
					scalar := name
					scalar.RawLeaf = nil
					charge, err := storage.CheckNameObservationBudget(callCtx, scalar, 1)
					if err != nil {
						return observation, err
					}
					if err := supplied.ReservePrefix(charge); err != nil {
						return observation, err
					}
					loaded++
				}
				if err := supplied.Add(storage.Entry{Name: "x", Attr: storage.Attr{ID: 9, Kind: storage.NodeRegular}}); err != nil {
					return observation, err
				}
				if scenario == "backend failure" {
					return observation, failure
				}
				if scenario == "malformed" {
					observation.Observation.ParentID = 99
				}
				return observation, nil
			}
			session := &fileSession{FileSession: backend, storage: view}
			checkError := error(nil)
			if scenario == "missing" {
				session.FileSession = &struct{ storage.FileSession }{}
				checkError = syscall.EOPNOTSUPP
			} else if scenario == "declined" {
				backend.checkErr, checkError = failure, failure
			}
			if err := session.CheckDirectoryMetadataObservation(); err != checkError {
				t.Fatalf("check = %v, want %v", err, checkError)
			}
			if checkError != nil {
				if err := result.Add(storage.Entry{Name: "x", Attr: storage.Attr{Kind: storage.NodeRegular}}); err != nil {
					t.Fatal(err)
				}
			}
			got, err := session.ObserveDirectoryMetadata(ctx, target, options, result)
			entries, entryErr := result.Entries()
			if scenario == "success" || scenario == "no name" {
				if err != nil || entryErr != nil || len(entries) != 1 || !reflect.DeepEqual(got, observation) {
					t.Fatalf("observation = %+v, %v; entries = %+v, %v", got, err, entries, entryErr)
				}
				wantBudget := 1
				if scenario == "no name" {
					wantBudget = 0
				}
				if budgetCalls != wantBudget || loaded != wantBudget {
					t.Fatalf("budget calls %d, loaded %d", budgetCalls, loaded)
				}
			} else {
				if err == nil || !reflect.DeepEqual(got, storage.DirectoryMetadataObservation{}) || entryErr != err || entries != nil {
					t.Fatalf("failure leaked observation: %+v, %v; entries %+v, %v", got, err, entries, entryErr)
				}
				if checkError != nil && (err != checkError || backendCalls != 0) {
					t.Fatal("capability refusal was lost or dispatched")
				}
				if scenario == "backend failure" && err != failure {
					t.Fatalf("lost backend cause: %v", err)
				}
				if scenario == "prefix refusal" {
					if loaded != 0 || budgetCalls != 1 {
						t.Fatalf("prefix refusal loaded %d payloads after %d budget calls", loaded, budgetCalls)
					}
					if !errors.Is(err, syscall.EFBIG) {
						t.Fatalf("prefix refusal = %v, want EFBIG", err)
					}
				}
			}
		})
	}
}

func TestReferenceNameObservationPreservesReadContextAndRejectsUnknownFacts(t *testing.T) {
	for _, nodeReference := range []bool{false, true} {
		for _, scenario := range []string{"linked", "root", "detached", "backend failure", "budget refusal", "malformed", "substitution", "missing identity", "identity failure", "missing", "declined"} {
			t.Run(scenario+map[bool]string{false: "/file", true: "/node"}[nodeReference], func(t *testing.T) {
				view, _, ctx := capabilityView()
				failure := errors.New("name observation failed")
				guards := &storage.NamespaceGuards{RootID: 1}
				value := storage.NameObservation{NodeID: 7, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte{0xff, 'x'}}
				if scenario == "root" {
					value = storage.NameObservation{NodeID: 1, State: storage.NameRoot}
				}
				if scenario == "detached" {
					value = storage.NameObservation{NodeID: 7, State: storage.NameDetached}
				}
				var calls, budgetCalls int
				ctx = storage.WithNameObservationBudget(ctx, func(scalar storage.NameObservation, length int64) (int64, error) {
					budgetCalls++
					if scalar.RawLeaf != nil || scalar.NodeID != value.NodeID || length != int64(len(value.RawLeaf)) {
						t.Fatal("name budget lost scalar identity/length")
					}
					if scenario == "budget refusal" {
						return 0, failure
					}
					return storage.NameObservationRetentionBytes(length)
				})
				backend := &nameObservationProbe{identity: value.NodeID}
				backend.observe = func(callCtx context.Context, gotGuards *storage.NamespaceGuards) (storage.NameObservation, error) {
					calls++
					if gotGuards != guards || !reflect.DeepEqual(locking.ScopeFromContext(callCtx), locking.MutationScope{}) || callCtx.Value(probeContextKey{}) != "request trace" {
						t.Fatal("reference observation changed guards or read context")
					}
					scalar := value
					scalar.RawLeaf = nil
					if _, err := storage.CheckNameObservationBudget(callCtx, scalar, int64(len(value.RawLeaf))); err != nil {
						return value, err
					}
					if scenario == "backend failure" {
						return value, failure
					}
					if scenario == "malformed" {
						value.State = storage.NameRoot
					}
					return value, nil
				}
				checkError := error(nil)
				var raw storage.File = backend
				if scenario == "missing" {
					raw = &struct{ storage.File }{}
					checkError = syscall.EOPNOTSUPP
				} else if scenario == "declined" {
					backend.checkErr, checkError = failure, failure
				} else if scenario == "missing identity" {
					raw = &struct {
						storage.File
						storage.ReferenceNameObserver
					}{ReferenceNameObserver: backend}
					checkError = syscall.EOPNOTSUPP
				} else if scenario == "identity failure" {
					backend.identityErr, checkError = failure, failure
				} else if scenario == "substitution" {
					backend.identity = 99
				}
				var observer storage.ReferenceNameObserver
				if nodeReference {
					wrapped := view.wrapReference(raw)
					if _, ok := wrapped.(storage.File); ok {
						t.Fatal("node reference gained byte I/O")
					}
					observer = wrapped.(storage.ReferenceNameObserver)
				} else {
					observer = view.wrapFile(raw).(storage.ReferenceNameObserver)
				}
				if err := observer.CheckReferenceNameObservation(); err != checkError {
					t.Fatalf("check = %v, want %v", err, checkError)
				}
				got, err := observer.ObserveName(ctx, guards)
				if checkError != nil || scenario == "backend failure" || scenario == "budget refusal" || scenario == "malformed" || scenario == "substitution" {
					want := failure
					if checkError != nil {
						want = checkError
					}
					if (scenario != "malformed" && scenario != "substitution" && err != want) || err == nil || !reflect.DeepEqual(got, storage.NameObservation{}) {
						t.Fatalf("failed observation = %+v, %v", got, err)
					}
					if checkError != nil && (calls != 0 || budgetCalls != 0) {
						t.Fatal("refused observer dispatched")
					}
				} else if err != nil || !reflect.DeepEqual(got, value) || calls != 1 || budgetCalls != 1 {
					t.Fatalf("name = %+v, %v; calls %d/%d", got, err, calls, budgetCalls)
				}
			})
		}
	}
}
