package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"slices"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func nativeTransferCommand(t *testing.T, store *Store, nonce, name, content string, replaced uint64) storage.ImportCommand {
	t.Helper()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	command := storage.ImportCommand{
		Action:   storage.ActionKey{SubjectID: "owner", Namespace: 1, Nonce: nonce},
		Target:   storage.ChildSelection{Name: storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte(name)}},
		Expected: storage.ChildCondition{State: storage.Absent}, Mode: storage.ImportCreate,
		DeclaredBytes: int64(len(content)), Digest: sha256.Sum256([]byte(content)),
	}
	if replaced != 0 {
		command.Mode = storage.ImportReplaceName
		command.Expected = storage.ChildCondition{State: storage.SameNode, NodeID: replaced}
	}
	command.Fingerprint = command.EffectFingerprint()
	return command
}

func nativeTransferSeal(t *testing.T, ctx context.Context, store *Store, command storage.ImportCommand) metastore.ImportReservation {
	t.Helper()
	reservation, err := store.PrepareImport(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SealImport(ctx, command.Action, metastore.Object{Key: reservation.Key, Size: command.DeclaredBytes, Digest: command.Digest[:]}); err != nil {
		t.Fatal(err)
	}
	return reservation
}

func nativeTransferPublish(t *testing.T, store *Store, command storage.ImportCommand) storage.ImportReceipt {
	t.Helper()
	nativeTransferSeal(t, t.Context(), store, command)
	receipt, err := store.PublishImport(t.Context(), command.Action)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted || receipt.Attr.Size != command.DeclaredBytes || receipt.Attr.ID == 0 {
		t.Fatalf("publication = %+v, %v", receipt, err)
	}
	if err := receipt.Check(); err != nil {
		t.Fatalf("invalid publication receipt: %v", err)
	}
	return receipt
}

func nativeTransferGarbage(t *testing.T, store *Store, key metastore.Key, eligible bool) {
	t.Helper()
	keys, err := store.Garbage(t.Context(), 16)
	if err != nil || slices.Contains(keys, key) != eligible {
		t.Fatalf("garbage = %v, %v; key %q eligible %v", keys, err, key, eligible)
	}
}

func TestNativeTransferReplacementPreservesOpenedNodeAndAtomicReceipt(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	servicePolicyNamespace(t, store.Store, 1)
	initial := nativeTransferPublish(t, store.Store, nativeTransferCommand(t, store.Store, "initial", "file", "old", 0))
	opened, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	before, err := opened.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command := nativeTransferCommand(t, store.Store, "replace", "file", "replacement", initial.Attr.ID)
	nativeTransferSeal(t, t.Context(), store.Store, command)
	if named, err := store.Stat(t.Context(), "file"); err != nil || named.ID != before.ID || named.Content != before.Content {
		t.Fatalf("staging changed named file = %+v, %v", named, err)
	}
	servicePolicySummary(t, store.Store, 1, 0, 4096, 8192, 1)
	receipt, err := store.PublishImport(t.Context(), command.Action)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted || receipt.Attr.ID == initial.Attr.ID {
		t.Fatalf("replacement = %+v, %v", receipt, err)
	}
	named, err := store.Stat(t.Context(), "file")
	if err != nil || !reflect.DeepEqual(named.Attr(), receipt.Attr) || named.Content == before.Content {
		t.Fatalf("named replacement = %+v, %v; receipt %+v", named, err, receipt)
	}
	retained, err := opened.Node(t.Context())
	if err != nil || retained.ID != before.ID || retained.Content != before.Content || retained.Size != before.Size || !retained.Detached {
		t.Fatalf("old reference was redirected = %+v, %v", retained, err)
	}
	servicePolicySummary(t, store.Store, 1, 0, 8192, 8192, 1)
	position, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		replayed, err := store.PublishImport(t.Context(), command.Action)
		if err != nil || !reflect.DeepEqual(replayed, receipt) {
			t.Fatalf("replay = %+v, %v; want %+v", replayed, err, receipt)
		}
	}
	if after, err := store.CommittedPosition(t.Context()); err != nil || after != position {
		t.Fatalf("receipt replay changed namespace log = %d, %v; want %d", after, err, position)
	}
	if err := opened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	servicePolicySummary(t, store.Store, 1, 0, 4096, 8192, 1)
}

func TestNativeTransferReplacementChargesRetainedAllocation(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 4096))
	servicePolicyNamespace(t, store.Store, 1)
	initial := nativeTransferPublish(t, store.Store, nativeTransferCommand(t, store.Store, "initial", "file", "old", 0))
	opened, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	command := nativeTransferCommand(t, store.Store, "replace", "file", "new", initial.Attr.ID)
	reservation := nativeTransferSeal(t, t.Context(), store.Store, command)
	receipt, err := store.PublishImport(t.Context(), command.Action)
	if !errors.Is(err, syscall.EDQUOT) || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != syscall.EDQUOT {
		t.Fatalf("retained quota rejection = %+v, %v", receipt, err)
	}
	if node, err := store.Stat(t.Context(), "file"); err != nil || node.Attr().ID != initial.Attr.ID {
		t.Fatalf("quota rejection replaced name = %+v, %v", node, err)
	}
	servicePolicySummary(t, store.Store, 1, 0, 4096, 4096, 1)
	nativeTransferGarbage(t, store.Store, reservation.Key, true)
	if err := opened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if replay, err := store.PublishImport(t.Context(), command.Action); !errors.Is(err, syscall.EDQUOT) || !reflect.DeepEqual(replay, receipt) {
		t.Fatalf("quota refusal changed after pin release = %+v, %v", replay, err)
	}
	nativeTransferPublish(t, store.Store, nativeTransferCommand(t, store.Store, "retry", "file", "new", initial.Attr.ID))
	servicePolicySummary(t, store.Store, 1, 0, 4096, 4096, 1)
}

func TestNativeTransferNamespaceConflictHasOneDurableRefusal(t *testing.T) {
	for _, guarded := range []bool{false, true} {
		t.Run(map[bool]string{false: "ObservedChild", true: "DirectoryRevision"}[guarded], func(t *testing.T) {
			store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
			servicePolicyNamespace(t, store.Store, 1)
			command := nativeTransferCommand(t, store.Store, "staged", "file", "staged", 0)
			if guarded {
				observed := observedDirectory(t, store.Store, command.Target.Name.Parent.NodeID)
				command.Target.Guards = &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{observed.Observation}}
				command.Fingerprint = command.EffectFingerprint()
			}
			reservation := nativeTransferSeal(t, t.Context(), store.Store, command)
			other := "file"
			if guarded {
				other = "other"
			}
			if err := store.Create(t.Context(), other); err != nil {
				t.Fatal(err)
			}
			before, err := store.List(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			position, err := store.CommittedPosition(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := store.PublishImport(t.Context(), command.Action)
			if !errors.Is(err, syscall.EAGAIN) || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != syscall.EAGAIN {
				t.Fatalf("namespace refusal = %+v, %v", receipt, err)
			}
			for range 2 {
				replay, err := store.PublishImport(t.Context(), command.Action)
				if !errors.Is(err, syscall.EAGAIN) || !reflect.DeepEqual(replay, receipt) {
					t.Fatalf("refusal replay = %+v, %v; want %+v", replay, err, receipt)
				}
			}
			if after, err := store.List(t.Context(), ""); err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("refusal changed tree = %+v, %v", after, err)
			}
			if after, err := store.CommittedPosition(t.Context()); err != nil || after != position {
				t.Fatalf("refusal published changes = %d, %v; want %d", after, err, position)
			}
			nativeTransferGarbage(t, store.Store, reservation.Key, true)
			changed := command
			changed.Digest = sha256.Sum256([]byte("different"))
			changed.Fingerprint = changed.EffectFingerprint()
			if _, err := store.PrepareImport(t.Context(), changed); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("changed action intent accepted = %v", err)
			}
		})
	}
}

func TestNativeTransferUnsealedAndMismatchedObjectsNeverBecomeNamed(t *testing.T) {
	for _, mismatch := range []string{"unsealed", "key", "length", "digest"} {
		t.Run(mismatch, func(t *testing.T) {
			store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
			servicePolicyNamespace(t, store.Store, 1)
			command := nativeTransferCommand(t, store.Store, "import", "file", "abc", 0)
			reservation, err := store.PrepareImport(t.Context(), command)
			if err != nil {
				t.Fatal(err)
			}
			object := metastore.Object{Key: reservation.Key, Size: command.DeclaredBytes, Digest: command.Digest[:]}
			switch mismatch {
			case "key":
				object.Key = "different-key"
			case "length":
				object.Size++
			case "digest":
				object.Digest = make([]byte, sha256.Size)
			}
			if mismatch != "unsealed" {
				if err := store.SealImport(t.Context(), command.Action, object); !errors.Is(err, syscall.EINVAL) {
					t.Fatalf("mismatched seal = %v", err)
				}
			}
			if receipt, err := store.PublishImport(t.Context(), command.Action); !errors.Is(err, syscall.EBUSY) || receipt.Outcome == storage.DurableActionCompleted {
				t.Fatalf("unsealed publication = %+v, %v", receipt, err)
			}
			if _, err := store.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("unsealed object acquired a name: %v", err)
			}
			servicePolicySummary(t, store.Store, 0, 0, 0, 8192, 1)
			nativeTransferGarbage(t, store.Store, reservation.Key, false)
		})
	}
}

func TestNativeTransferSealedReservationAndReceiptSurviveReopen(t *testing.T) {
	config := servicePolicyConfig(t, 8192)
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	servicePolicyNamespace(t, store.Store, 1)
	oldContext, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "old-login"})
	if err != nil {
		t.Fatal(err)
	}
	command := nativeTransferCommand(t, store.Store, "survives", "file", "durable", 0)
	reservation := nativeTransferSeal(t, oldContext, store.Store, command)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	config.Initialize = false
	config.SQLite.InitializeQuota = false
	store, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	newContext, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: "owner", AccessID: "new-login"})
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := store.ResumeImportReservation(newContext, command.Action)
	if err != nil || !resumed.Sealed || resumed.Key != reservation.Key || !reflect.DeepEqual(resumed.Command, command) {
		t.Fatalf("sealed resume = %+v, %v; original %+v", resumed, err, reservation)
	}
	receipt, err := store.PublishImport(newContext, command.Action)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("sealed publication after credential rotation = %+v, %v", receipt, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	store, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []func(context.Context, storage.ActionKey) (storage.ImportReceipt, error){store.QueryImportAction, store.PublishImport} {
		if replay, err := query(newContext, command.Action); err != nil || !reflect.DeepEqual(replay, receipt) {
			t.Fatalf("durable publication replay = %+v, %v; want %+v", replay, err, receipt)
		}
	}
	servicePolicySummary(t, store.Store, 1, 0, 4096, 8192, 1)
}

func TestNativeTransferAbandonPreservesUnresolvedAndSealedCleanup(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		t.Run(map[bool]string{false: "Unsealed", true: "Sealed"}[sealed], func(t *testing.T) {
			store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
			servicePolicyNamespace(t, store.Store, 1)
			command := nativeTransferCommand(t, store.Store, "canceled", "file", "abc", 0)
			reservation, err := store.PrepareImport(t.Context(), command)
			if err != nil {
				t.Fatal(err)
			}
			if sealed {
				nativeTransferSeal(t, t.Context(), store.Store, command)
			}
			for range 2 {
				if err := store.AbandonImport(t.Context(), command.Action); err != nil {
					t.Fatal(err)
				}
			}
			if receipt, err := store.QueryImportAction(t.Context(), command.Action); !errors.Is(err, syscall.ECANCELED) || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != syscall.ECANCELED {
				t.Fatalf("abandon receipt = %+v, %v", receipt, err)
			}
			nativeTransferGarbage(t, store.Store, reservation.Key, sealed)
			if _, err := store.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("abandoned object is named: %v", err)
			}
			servicePolicySummary(t, store.Store, 0, 0, 0, 8192, 1)
		})
	}
}

func TestNativeTransferExportPinsExactKeyAcrossOverwriteAndRetryableClose(t *testing.T) {
	for _, failure := range []string{"canceled", "cleanup"} {
		t.Run(failure, func(t *testing.T) {
			store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
			servicePolicyNamespace(t, store.Store, 1)
			initial := nativeTransferPublish(t, store.Store, nativeTransferCommand(t, store.Store, "initial", "file", "old", 0))
			capture, err := store.CaptureExport(t.Context(), storage.ExportCommand{NodeID: initial.Attr.ID})
			if err != nil || capture.Key == "" || capture.State.Size != 3 || capture.DigestAvailable {
				t.Fatalf("captured export = %+v, %v", capture, err)
			}
			t.Cleanup(func() {
				if _, err := capture.Reference.CloseWithResult(context.Background()); err != nil {
					t.Error(err)
				}
			})
			newKey, err := store.Reserve(t.Context(), "file", 4)
			if err != nil {
				t.Fatal(err)
			}
			newDigest := sha256.Sum256([]byte("next"))
			if err := store.Commit(t.Context(), "file", metastore.Object{Key: newKey, Size: 4, Digest: newDigest[:]}); err != nil {
				t.Fatal(err)
			}
			current, err := store.Stat(t.Context(), "file")
			if err != nil || current.Content != newKey || current.Attr().ID != initial.Attr.ID || capture.Key == current.Content || capture.State.Size != 3 {
				t.Fatalf("overwrite changed captured version: current %+v, captured %+v, error %v", current, capture, err)
			}
			nativeTransferGarbage(t, store.Store, capture.Key, false)
			if err := store.Forget(t.Context(), []metastore.Key{capture.Key}); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("exact-key pin allowed Forget = %v", err)
			}
			closeContext := t.Context()
			wantFailure := error(context.Canceled)
			if failure == "canceled" {
				var cancel context.CancelFunc
				closeContext, cancel = context.WithCancel(t.Context())
				cancel()
			} else {
				if err := store.Remove(t.Context(), "file"); err != nil {
					t.Fatal(err)
				}
				wantFailure = errors.New("detached export cleanup unavailable")
				closeContext = storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
					return nil, wantFailure
				})
			}
			result, err := capture.Reference.CloseWithResult(closeContext)
			if result.Released || !errors.Is(err, wantFailure) {
				t.Fatalf("failed close = %+v, %v; want %v", result, err, wantFailure)
			}
			nativeTransferGarbage(t, store.Store, capture.Key, false)
			if count := store.coordinator.objectPins[capture.Key]; count != 1 || store.exportPins != 1 {
				t.Fatalf("failed close lost key ownership: pins %d, exports %d", count, store.exportPins)
			}
			for range 2 {
				result, err := capture.Reference.CloseWithResult(t.Context())
				if err != nil || !result.Released || !result.Determined {
					t.Fatalf("settled close replay = %+v, %v", result, err)
				}
			}
			nativeTransferGarbage(t, store.Store, capture.Key, true)
			if store.coordinator.objectPins[capture.Key] != 0 || store.exportPins != 0 {
				t.Fatal("confirmed close retained export resources")
			}
		})
	}
}

func TestNativeTransferImportRespectsSharingAndStrongProtection(t *testing.T) {
	for _, protection := range []string{"share", "stable", "exclusive"} {
		t.Run(protection, func(t *testing.T) {
			store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
			servicePolicyNamespace(t, store.Store, 1)
			initial := nativeTransferPublish(t, store.Store, nativeTransferCommand(t, store.Store, "initial", "file", "old", 0))
			command := nativeTransferCommand(t, store.Store, "replace", "file", "new", initial.Attr.ID)
			reservation := nativeTransferSeal(t, t.Context(), store.Store, command)
			if protection == "share" {
				file, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{
					OpenAccess: storage.OpenAccess{Read: true}, Use: storage.UseClaim{Uses: storage.ReadData, Deny: storage.DeleteName},
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := file.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
			} else {
				fixture := publicationFixture{store: store.Store, service: store.LockService()}
				mode := locking.Shared
				if protection == "exclusive" {
					mode = locking.Exclusive
				}
				fixture.grant(t, fixture.owner(t), "file", mode)
			}
			wantFailure := syscall.EAGAIN
			if protection != "share" {
				wantFailure = locking.Errno(locking.Conflict)
			}
			receipt, err := store.PublishImport(t.Context(), command.Action)
			if !errors.Is(err, wantFailure) || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != wantFailure {
				t.Fatalf("protected replacement = %+v, %v", receipt, err)
			}
			if current, err := store.Stat(t.Context(), "file"); err != nil || current.Attr().ID != initial.Attr.ID || current.Size != initial.Attr.Size {
				t.Fatalf("protection refusal modified target = %+v, %v", current, err)
			}
			nativeTransferGarbage(t, store.Store, reservation.Key, true)
			servicePolicySummary(t, store.Store, 1, 0, 4096, 8192, 1)
		})
	}
}

func TestNativeTransferExportRespectsSharingAndEnforcedRanges(t *testing.T) {
	for _, protection := range []string{"share", "range"} {
		t.Run(protection, func(t *testing.T) {
			store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
			servicePolicyNamespace(t, store.Store, 1)
			initial := nativeTransferPublish(t, store.Store, nativeTransferCommand(t, store.Store, "initial", "file", "content", 0))
			options := storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, Use: storage.UseClaim{Uses: storage.ReadData}}
			if protection == "share" {
				options.Use.Deny = storage.ReadData
			}
			file, err := store.OpenFile(t.Context(), "file", options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := file.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			wantFailure := storage.ErrUseConflict
			if protection == "range" {
				scope, err := file.Scope(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				session, err := store.fileDomain.coordinator.NewSession(storage.DefaultFileSessionOptions(), func() error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := session.Retire(context.Background()); err != nil {
						t.Error(err)
					}
				})
				owner, err := session.NewOwner(t.Context(), initial.Attr.ID, scope, storage.OwnerOptions{Lifetime: storage.OwnerExplicit})
				if err != nil {
					t.Fatal(err)
				}
				epoch, err := session.Epoch(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				request, err := storage.NewLockRequestID(epoch)
				if err != nil {
					t.Fatal(err)
				}
				command := storage.RangeCommand{
					Domain: storage.DomainEnforced, Mode: storage.RangeExclusive, Edit: storage.AddExact,
					Range: storage.Range{Kind: storage.Bytes, Start: 1, Length: 1}, Policy: storage.RangePolicy{DenyOthers: storage.ReadData},
				}
				attempt, err := session.Apply(t.Context(), initial.Attr.ID, owner, []storage.RangeCommand{command}, request, file.Order)
				if err != nil || attempt.State != storage.Granted {
					t.Fatalf("range protection = %+v, %v", attempt, err)
				}
				wantFailure = storage.ErrRangeConflict
			}
			capture, err := store.CaptureExport(t.Context(), storage.ExportCommand{NodeID: initial.Attr.ID})
			if !errors.Is(err, wantFailure) || capture.Reference != nil || store.exportPins != 0 || len(store.coordinator.objectPins) != 0 {
				t.Fatalf("protected export = %+v, %v; exports %d, keys %v", capture, err, store.exportPins, store.coordinator.objectPins)
			}
		})
	}
}

func TestNativeTransferConcurrentPublicationReplaysOneNode(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	servicePolicyNamespace(t, store.Store, 1)
	command := nativeTransferCommand(t, store.Store, "shared-action", "file", "content", 0)
	nativeTransferSeal(t, t.Context(), store.Store, command)
	before, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		receipt storage.ImportReceipt
		err     error
	}
	const publishers = 8
	results := make(chan result, publishers)
	start := make(chan struct{})
	for range publishers {
		go func() {
			<-start
			receipt, err := store.PublishImport(t.Context(), command.Action)
			results <- result{receipt, err}
		}()
	}
	close(start)
	var first storage.ImportReceipt
	for index := range publishers {
		got := <-results
		if got.err != nil || got.receipt.Outcome != storage.DurableActionCompleted {
			t.Errorf("publisher %d = %+v, %v", index, got.receipt, got.err)
		}
		if index == 0 {
			first = got.receipt
		} else if !reflect.DeepEqual(got.receipt, first) {
			t.Errorf("publisher %d received a second effect: %+v; first %+v", index, got.receipt, first)
		}
	}
	if after, err := store.CommittedPosition(t.Context()); err != nil || after != before+2 {
		t.Fatalf("concurrent publication changed the creation and parent-update count = %d, %v; want %d", after, err, before+2)
	}
	servicePolicySummary(t, store.Store, 1, 0, 4096, 8192, 1)
}

func TestNativeTransferEmptyExportAndExactScopeValidation(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	for _, name := range []string{"empty", "other"} {
		if err := store.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	file, err := store.Stat(t.Context(), "empty")
	if err != nil {
		t.Fatal(err)
	}
	capture, err := store.CaptureExport(t.Context(), storage.ExportCommand{NodeID: file.Attr().ID})
	if err != nil || capture.Key != "" || capture.State.Size != 0 || !capture.DigestAvailable || capture.Digest != sha256.Sum256(nil) {
		t.Fatalf("empty capture = %+v, %v", capture, err)
	}
	if result, err := capture.Reference.CloseWithResult(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("empty close = %+v, %v", result, err)
	}
	other, err := store.OpenFile(t.Context(), "other", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := other.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	scope, err := other.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if captured, err := store.CaptureExport(t.Context(), storage.ExportCommand{NodeID: file.Attr().ID, Scope: &scope}); !errors.Is(err, storage.ErrInvalidScope) || captured.Reference != nil {
		t.Fatalf("scope from different node was accepted = %+v, %v", captured, err)
	}
	directory, err := store.Stat(t.Context(), "directory")
	if err != nil {
		t.Fatal(err)
	}
	if captured, err := store.CaptureExport(t.Context(), storage.ExportCommand{NodeID: directory.Attr().ID}); !errors.Is(err, syscall.EISDIR) || captured.Reference != nil {
		t.Fatalf("directory export = %+v, %v", captured, err)
	}
	if store.exportPins != 0 || len(store.coordinator.objectPins) != 0 {
		t.Fatal("failed or empty export leaked key ownership")
	}
}

func TestNativeTransferReplacementSelectsSymlinkWithoutFollowingItsTarget(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	servicePolicyNamespace(t, store.Store, 1)
	targetReceipt := nativeTransferPublish(t, store.Store, nativeTransferCommand(t, store.Store, "target", "target", "target contents", 0))
	targetBefore, err := store.Stat(t.Context(), "target")
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	link, err := store.OpenChildRef(t.Context(), selectChild(storage.ChildName{Parent: directoryTarget(root), RawLeaf: []byte("link")}), storage.NodeRefOptions{
		Kind: storage.NodeSymlink, Target: storage.ChildCondition{State: storage.Absent}, Action: fileAction(t),
		Create: true, Exclusive: true, MetadataAccess: storage.ReadMetadata,
		InitialState: storage.InitialState{OnCreate: storage.InitialFields{LinkTarget: []byte("target")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := link.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	servicePolicySummary(t, store.Store, 2, 0, 4096, 8192, 1)
	command := nativeTransferCommand(t, store.Store, "replace-link", "link", "independent contents", link.State.Attr().ID)
	receipt := nativeTransferPublish(t, store.Store, command)
	if receipt.Attr.Kind != storage.NodeRegular || receipt.Attr.ID == link.State.Attr().ID || receipt.Attr.ID == targetReceipt.Attr.ID {
		t.Fatalf("symlink replacement did not create a distinct regular node = %+v", receipt)
	}
	newNamed, err := store.Stat(t.Context(), "link")
	if err != nil || !reflect.DeepEqual(newNamed.Attr(), receipt.Attr) || newNamed.Content == targetBefore.Content {
		t.Fatalf("replacement name = %+v, %v", newNamed, err)
	}
	targetAfter, err := store.Stat(t.Context(), "target")
	if err != nil || !reflect.DeepEqual(targetAfter, targetBefore) {
		t.Fatalf("replacement followed symlink target: before %+v, after %+v, error %v", targetBefore, targetAfter, err)
	}
	oldLink, err := link.Reference.State(t.Context(), storage.StateFull)
	if err != nil || !oldLink.State.Detached || oldLink.State.Attr().ID != link.State.Attr().ID || oldLink.State.Kind != storage.NodeSymlink || !bytes.Equal(oldLink.LinkTarget, []byte("target")) {
		t.Fatalf("retained symlink was redirected = %+v, %v", oldLink, err)
	}
	capture, err := store.CaptureExport(t.Context(), storage.ExportCommand{NodeID: receipt.Attr.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := capture.Reference.CloseWithResult(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if capture.Key != newNamed.Content || capture.State.Size != command.DeclaredBytes || capture.DigestAvailable {
		t.Fatalf("replacement does not own its independent content = %+v", capture)
	}
	var storedDigest []byte
	if err := store.read.QueryRowContext(t.Context(), `SELECT digest FROM objects WHERE volume=? AND key=?`, store.volume, string(capture.Key)).Scan(&storedDigest); err != nil || !bytes.Equal(storedDigest, command.Digest[:]) {
		t.Fatalf("replacement lost its verified import content seal = %x, %v", storedDigest, err)
	}
	servicePolicySummary(t, store.Store, 2, 0, 8192, 8192, 1)
	if err := link.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	servicePolicySummary(t, store.Store, 2, 0, 8192, 8192, 1)
	if _, err := store.StatNode(t.Context(), link.State.Attr().ID); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("released symlink identity remains = %v", err)
	}
}

func TestNativeTransferPendingImportOwnsItsReservedObjectAgainstMaintenance(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		t.Run(map[bool]string{false: "Unsealed", true: "Sealed"}[sealed], func(t *testing.T) {
			store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
			servicePolicyNamespace(t, store.Store, 1)
			command := nativeTransferCommand(t, store.Store, "import", "file", "content", 0)
			victim, err := store.OpenFile(t.Context(), "victim", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := victim.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			victimBefore, err := victim.Node(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			reservation, err := store.PrepareImport(t.Context(), command)
			if err != nil {
				t.Fatal(err)
			}
			if sealed {
				nativeTransferSeal(t, t.Context(), store.Store, command)
			}
			for _, cleanup := range []func() error{
				func() error { return store.Abandon(t.Context(), reservation.Key) },
				func() error { return store.Forget(t.Context(), []metastore.Key{reservation.Key}) },
				func() error { return store.Quarantine(t.Context(), reservation.Key) },
				func() error {
					return store.Commit(t.Context(), "stolen", metastore.Object{Key: reservation.Key, Size: command.DeclaredBytes, Digest: command.Digest[:]})
				},
				func() error {
					_, err := victim.Commit(t.Context(), victimBefore.Revision, metastore.Object{Key: reservation.Key, Size: command.DeclaredBytes, Digest: command.Digest[:]})
					return err
				},
			} {
				if err := cleanup(); !errors.Is(err, syscall.EBUSY) {
					t.Fatalf("object maintenance stole pending import ownership = %v", err)
				}
			}
			if _, err := store.Stat(t.Context(), "stolen"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("ordinary publication named pending import object = %v", err)
			}
			if after, err := victim.Node(t.Context()); err != nil || !reflect.DeepEqual(after, victimBefore) {
				t.Fatalf("retained publication stole pending import content = %+v, %v", after, err)
			}
			resumed, err := store.ResumeImportReservation(t.Context(), command.Action)
			if err != nil || resumed.Key != reservation.Key || resumed.Sealed != sealed || resumed.Receipt.Outcome != storage.DurableActionPending {
				t.Fatalf("maintenance modified pending import = %+v, %v", resumed, err)
			}
			nativeTransferGarbage(t, store.Store, reservation.Key, false)
			ordinary, err := store.Reserve(t.Context(), "ordinary", 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Abandon(t.Context(), ordinary); err != nil {
				t.Fatalf("ordinary reservation cannot be abandoned = %v", err)
			}
			nativeTransferGarbage(t, store.Store, ordinary, true)
			if err := store.Forget(t.Context(), []metastore.Key{ordinary}); err != nil {
				t.Fatalf("ordinary garbage cannot be forgotten = %v", err)
			}
			if !sealed {
				nativeTransferSeal(t, t.Context(), store.Store, command)
			}
			receipt, err := store.PublishImport(t.Context(), command.Action)
			if err != nil || receipt.Outcome != storage.DurableActionCompleted {
				t.Fatalf("owned import cannot publish after rejected maintenance = %+v, %v", receipt, err)
			}
			if named, err := store.Stat(t.Context(), "file"); err != nil || named.Content != reservation.Key || named.Attr().ID != receipt.Attr.ID {
				t.Fatalf("publication lost reserved object = %+v, %v", named, err)
			}
			servicePolicySummary(t, store.Store, 2, 0, 4096, 8192, 1)
		})
	}
}

func TestNativeTransferReplacementRejectsSelectedDirectory(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	servicePolicyNamespace(t, store.Store, 1)
	if err := store.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	before, err := store.Stat(t.Context(), "directory")
	if err != nil {
		t.Fatal(err)
	}
	command := nativeTransferCommand(t, store.Store, "replace-directory", "directory", "file contents", before.Attr().ID)
	reservation := nativeTransferSeal(t, t.Context(), store.Store, command)
	receipt, err := store.PublishImport(t.Context(), command.Action)
	if !errors.Is(err, syscall.EISDIR) || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != syscall.EISDIR {
		t.Fatalf("directory replacement = %+v, %v", receipt, err)
	}
	if after, err := store.Stat(t.Context(), "directory"); err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("refused replacement changed directory = %+v, %v", after, err)
	}
	nativeTransferGarbage(t, store.Store, reservation.Key, true)
	servicePolicySummary(t, store.Store, 0, 1, 0, 8192, 1)
}

func TestNativeTransferScopedExportRequiresMetadataRead(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	servicePolicyNamespace(t, store.Store, 1)
	initial := nativeTransferPublish(t, store.Store, nativeTransferCommand(t, store.Store, "initial", "file", "content", 0))
	for _, permissions := range []storage.MetadataPermissions{0, storage.ReadMetadata} {
		name := "ReadDataOnly"
		if permissions != 0 {
			name = "ReadDataAndMetadata"
		}
		t.Run(name, func(t *testing.T) {
			opened, err := store.OpenAt(t.Context(), selectChild(storage.ChildName{
				Parent: storage.DirectoryTarget{NodeID: uint64(store.root)}, RawLeaf: []byte("file"),
			}), storage.OpenAtOptions{
				Read: true, MetadataAccess: permissions, Existing: storage.Keep, Action: fileAction(t),
				Target: storage.ChildCondition{State: storage.SameNode, NodeID: initial.Attr.ID}, Use: storage.UseClaim{Uses: storage.ReadData},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := opened.File.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			scope, err := opened.File.Scope(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			files, pins, exports, objectPins := store.fileDomain.files, len(store.coordinator.pins), store.exportPins, len(store.coordinator.objectPins)
			capture, err := store.CaptureExport(t.Context(), storage.ExportCommand{NodeID: initial.Attr.ID, Scope: &scope})
			if permissions == 0 {
				if !errors.Is(err, syscall.EBADF) || capture.Reference != nil {
					t.Fatalf("read-data scope exposed metadata = %+v, %v", capture, err)
				}
				if store.fileDomain.files != files || len(store.coordinator.pins) != pins || store.exportPins != exports || len(store.coordinator.objectPins) != objectPins {
					t.Fatal("metadata permission refusal changed native or export retention")
				}
				return
			}
			if err != nil || capture.Reference == nil || capture.State.Attr().ID != initial.Attr.ID {
				t.Fatalf("metadata-authorized scoped export = %+v, %v", capture, err)
			}
			if result, err := capture.Reference.CloseWithResult(t.Context()); err != nil || !result.Released || !result.Determined {
				t.Fatalf("authorized export close = %+v, %v", result, err)
			}
			if store.fileDomain.files != files || len(store.coordinator.pins) != pins || store.exportPins != exports || len(store.coordinator.objectPins) != objectPins {
				t.Fatal("settled authorized export retained resources")
			}
		})
	}
}

func TestNativeTransferImportRejectsCorruptStoredCommandAndBindingWithoutEffects(t *testing.T) {
	for _, corruption := range []struct {
		name   string
		column string
		value  []byte
	}{
		{"OversizedCommand", "command", bytes.Repeat([]byte("x"), 262145)},
		{"OversizedBinding", "access_binding", bytes.Repeat([]byte("x"), 16385)},
		{"IncompleteBinding", "access_binding", []byte(`{"SubjectID":"owner"}`)},
	} {
		t.Run(corruption.name, func(t *testing.T) {
			store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
			servicePolicyNamespace(t, store.Store, 1)
			command := nativeTransferCommand(t, store.Store, "corruptible", "file", "content", 0)
			reservation := nativeTransferSeal(t, t.Context(), store.Store, command)
			var original []byte
			if err := store.read.QueryRowContext(t.Context(), `SELECT `+corruption.column+` FROM imports WHERE volume=? AND subject=? AND namespace=? AND nonce=?`, store.volume, command.Action.SubjectID, command.Action.Namespace, command.Action.Nonce).Scan(&original); err != nil {
				t.Fatal(err)
			}
			restore := func(ctx context.Context) error {
				_, err := store.write.ExecContext(ctx, `UPDATE imports SET `+corruption.column+`=? WHERE volume=? AND subject=? AND namespace=? AND nonce=?`, original, store.volume, command.Action.SubjectID, command.Action.Namespace, command.Action.Nonce)
				return err
			}
			t.Cleanup(func() {
				if err := restore(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if _, err := store.write.ExecContext(t.Context(), `UPDATE imports SET `+corruption.column+`=? WHERE volume=? AND subject=? AND namespace=? AND nonce=?`, corruption.value, store.volume, command.Action.SubjectID, command.Action.Namespace, command.Action.Nonce); err != nil {
				t.Fatal(err)
			}
			before, err := store.DurableState(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ResumeImportReservation(t.Context(), command.Action); !errors.Is(err, syscall.EIO) {
				t.Fatalf("corrupt reservation resume = %v", err)
			}
			if receipt, err := store.PublishImport(t.Context(), command.Action); !errors.Is(err, syscall.EIO) || receipt.Attr.ID != 0 {
				t.Fatalf("corrupt reservation publication = %+v, %v", receipt, err)
			}
			if after, err := store.DurableState(t.Context()); err != nil || after != before {
				t.Fatalf("corrupt reservation read changed durable state = %+v, %v; before %+v", after, err, before)
			}
			if _, err := store.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("corrupt reservation acquired a name = %v", err)
			}
			if err := restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			resumed, err := store.ResumeImportReservation(t.Context(), command.Action)
			if err != nil || !resumed.Sealed || resumed.Key != reservation.Key || resumed.Receipt.Outcome != storage.DurableActionPending || !reflect.DeepEqual(resumed.Command, command) {
				t.Fatalf("restored reservation changed = %+v, %v", resumed, err)
			}
			if receipt, err := store.PublishImport(t.Context(), command.Action); err != nil || receipt.Outcome != storage.DurableActionCompleted {
				t.Fatalf("restored reservation cannot publish = %+v, %v", receipt, err)
			}
			servicePolicySummary(t, store.Store, 1, 0, 4096, 8192, 1)
		})
	}
}

func TestNativeTransferExportDoesNotAdvertiseOpaqueThirtyTwoByteDigestAsSHA256(t *testing.T) {
	store := servicePolicyOpen(t, servicePolicyConfig(t, 8192))
	key, err := store.Reserve(t.Context(), "file", 3)
	if err != nil {
		t.Fatal(err)
	}
	opaque := bytes.Repeat([]byte{0xa5}, sha256.Size)
	if err := store.Commit(t.Context(), "file", metastore.Object{Key: key, Size: 3, Digest: opaque}); err != nil {
		t.Fatal(err)
	}
	var storedDigest []byte
	if err := store.read.QueryRowContext(t.Context(), `SELECT digest FROM objects WHERE volume=? AND key=?`, store.volume, string(key)).Scan(&storedDigest); err != nil || !bytes.Equal(storedDigest, opaque) {
		t.Fatalf("opaque digest fixture was not persisted = %x, %v", storedDigest, err)
	}
	node, err := store.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	capture, err := store.CaptureExport(t.Context(), storage.ExportCommand{NodeID: node.Attr().ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := capture.Reference.CloseWithResult(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if capture.Key != key || capture.State.Size != 3 || capture.DigestAvailable {
		t.Fatalf("opaque native digest was advertised as SHA-256 = %+v", capture)
	}
}
