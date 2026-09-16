package sqlite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func openPublicationFile(t *testing.T) (*LockingStore, *fileReference) {
	t.Helper()
	config := lockingTestConfig(t)
	config.Allowance = 100
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "file"); err != nil {
		s.Close()
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "file")
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	native, _, err := s.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	session := native.(*fileSession)
	f := retainRangeFile(t, session, uint64(node.ID))
	t.Cleanup(func() {
		if err := session.Dispose(context.Background()); err != nil {
			t.Error(err)
		}
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, f
}

func startPublication(t *testing.T, f *fileReference, operation storage.FileIO) storage.FileActionID {
	t.Helper()
	id := fileActionID(t, f.session)
	_, fresh, err := f.BeginContent(t.Context(), id, [32]byte{}, operation)
	if err != nil || !fresh {
		t.Fatalf("content admission=%v %v", fresh, err)
	}
	return id
}

func publicationTarget(t *testing.T, s *LockingStore, session *fileSession, name string) storage.EntryTarget {
	t.Helper()
	receipt, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: uint64(s.root)}, fileActionID(t, session))
	if err != nil {
		t.Fatal(err)
	}
	parent, live, err := session.Reference(t.Context(), receipt.Reference)
	if err != nil || !live {
		t.Fatalf("parent reference=%v %v", live, err)
	}
	entry, err := parent.LookupAt(t.Context(), []byte(name))
	if err != nil {
		t.Fatal(err)
	}
	return storage.EntryTarget{Parent: receipt.Reference, ParentID: uint64(s.root), Name: []byte(name), DirectoryRevision: entry.DirectoryRevision, ExpectedEntryID: entry.EntryID, ExpectedNodeID: entry.Attr.ID, ExpectedMetadataRevision: entry.Attr.MetadataRevision}
}

func TestRetiredReferenceCannotPublishAPreviouslyReservedObject(t *testing.T) {
	s, f := openPublicationFile(t)
	before, err := f.Capture(t.Context(), storage.FileIO{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := f.Reserve(t.Context(), 7)
	if err != nil {
		t.Fatal(err)
	}
	action := startPublication(t, f, storage.FileIO{Write: true, Length: 7})
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := f.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.CommitContent(t.Context(), action, before.Revision, metastore.Object{Key: key, Size: 7, ModTime: time.Now()}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("retired commit=%v", err)
	}
	var nodes, reserved int
	if err := s.read.QueryRow(`SELECT (SELECT count(*) FROM nodes WHERE id=? AND detached=1),(SELECT state FROM objects WHERE key=?)`, before.ID, string(key)).Scan(&nodes, &reserved); err != nil {
		t.Fatal(err)
	}
	if nodes != 1 || reserved != stateReserved {
		t.Fatalf("physical node=%d object state=%d", nodes, reserved)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("usage=%d %v", used, err)
	}
	if err := s.Abandon(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Close(t.Context(), fileActionID(t, f.session)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.session.StatNode(t.Context(), uint64(before.ID), storage.ObservationOptions{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("released node=%v", err)
	}
}

func TestRetirementOrdersAfterAnAdmittedFinalPublication(t *testing.T) {
	s, f := openPublicationFile(t)
	before, err := f.Capture(t.Context(), storage.FileIO{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := f.Reserve(t.Context(), 9)
	if err != nil {
		t.Fatal(err)
	}
	action := startPublication(t, f, storage.FileIO{Write: true, Length: 9})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	ctx := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous != 0 || next != 9 {
			return nil, fmt.Errorf("wrong accounting %d -> %d", previous, next)
		}
		close(entered)
		<-release
		return func(result storage.PublicationResult) error {
			if result != storage.PublicationApplied {
				return fmt.Errorf("result=%v", result)
			}
			return nil
		}, nil
	})
	committed := make(chan error, 1)
	go func() {
		_, err := f.CommitContent(ctx, action, before.Revision, metastore.Object{Key: key, Size: 9, ModTime: time.Now()})
		committed <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("publication did not reach final gate")
	}
	retired := make(chan error, 1)
	go func() { retired <- f.Retire(t.Context()) }()
	select {
	case err := <-retired:
		t.Fatalf("retirement passed active publication=%v", err)
	default:
	}
	unblock()
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	if err := <-retired; err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "file")
	if err != nil || node.Size != 9 {
		t.Fatalf("published node=%+v %v", node, err)
	}
	if _, _, err := f.BeginContent(t.Context(), fileActionID(t, f.session), [32]byte{}, storage.FileIO{Write: true, Truncate: true}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("later publication=%v", err)
	}
}

func TestRetainedCleanupKnownRefusalPreservesThePinForRetry(t *testing.T) {
	s, f := openPublicationFile(t)
	state, err := f.Capture(t.Context(), storage.FileIO{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := f.Reserve(t.Context(), 5)
	if err != nil {
		t.Fatal(err)
	}
	action := startPublication(t, f, storage.FileIO{Write: true, Length: 5})
	if _, err := f.CommitContent(t.Context(), action, state.Revision, metastore.Object{Key: key, Size: 5, ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	refused := errors.New("cleanup charge refused")
	ctx := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous != 5 || next != 0 {
			t.Fatalf("cleanup accounting %d -> %d", previous, next)
		}
		return nil, refused
	})
	if _, err := f.Close(ctx, fileActionID(t, f.session)); !errors.Is(err, refused) {
		t.Fatalf("cleanup refusal=%v", err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 5 {
		t.Fatalf("refused cleanup usage=%d %v", used, err)
	}
	if _, err := f.Close(t.Context(), fileActionID(t, f.session)); err != nil {
		t.Fatal(err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("retried cleanup usage=%d %v", used, err)
	}
}

type retainedFailureWitness struct{ failure error }

func (w *retainedFailureWitness) Accept(DurableState) error     { return w.failure }
func (w *retainedFailureWitness) Checkpoint(DurableState) error { return nil }

func TestRetainedCleanupUnknownAcceptanceKeepsPhysicalOwnership(t *testing.T) {
	s, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	native, _, err := s.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	session := native.(*fileSession)
	f := retainRangeFile(t, session, uint64(node.ID))
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	acceptErr := errors.New("retained cleanup acceptance unavailable")
	s.witness = &retainedFailureWitness{failure: acceptErr}
	var settlements atomic.Int32
	ctx := storage.WithPublicationAccounting(t.Context(), func(_, _ int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error {
			settlements.Add(1)
			if result != storage.PublicationUnknown {
				return fmt.Errorf("settlement=%v", result)
			}
			return nil
		}, nil
	})
	id := fileActionID(t, session)
	_, err = f.Close(ctx, id)
	if !errors.Is(err, acceptErr) || !storage.IsPublicationAccountingUncertain(err) {
		t.Fatalf("unknown cleanup=%v", err)
	}
	if _, again := f.Close(ctx, id); !errors.Is(again, acceptErr) || settlements.Load() != 1 {
		t.Fatalf("repeated cleanup=%v settlements=%d", again, settlements.Load())
	}
	if s.coordinator.pins[retainedNode{s.volume, f.id}] != 1 || len(s.files) != 1 || s.leaseOwner.FD() < 0 {
		t.Fatal("unknown cleanup released native retention")
	}
	if err := s.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("store close with unresolved reference=%v", err)
	}
	// Process teardown follows the assertion that ordinary cleanup kept ownership.
	if err := s.locks.Close(); err != nil && !errors.Is(err, acceptErr) {
		t.Fatal(err)
	}
	s.locks = nil
	s.witness = nil
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestStrongGrantsRetireTheNameWhileOrdinaryReferencesKeepTheNode(t *testing.T) {
	s, file := openPublicationFile(t)
	fixture := publicationFixture{store: s.Store, service: s.LockService()}
	fixture.put(t, t.Context(), "file", 5)
	owner := fixture.owner(t)
	grant := fixture.grant(t, owner, "file", locking.Exclusive)
	node, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := file.session.Retain(t.Context(), storage.RetainRequest{NodeID: uint64(node.ID), Claim: storage.AccessClaim{Uses: storage.ReadContent}}, fileActionID(t, file.session))
	if err != nil {
		t.Fatalf("ordinary retain while strongly protected=%v", err)
	}
	ref, _, err := file.session.Reference(t.Context(), opened.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ref.Close(t.Context(), fileActionID(t, file.session)); err != nil {
		t.Fatal(err)
	}
	requirePublicationCode(t, s.Remove(t.Context(), "file"), locking.Conflict)
	state, err := file.Capture(t.Context(), storage.FileIO{})
	if err != nil {
		t.Fatal(err)
	}
	accounted := 0
	ctx := storage.WithPublicationAccounting(publicationScope(t.Context(), owner, grant), func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous != 5 || next != 5 {
			return nil, fmt.Errorf("detach refunded retained bytes: %d -> %d", previous, next)
		}
		return func(result storage.PublicationResult) error {
			if result != storage.PublicationApplied {
				return fmt.Errorf("detach outcome=%v", result)
			}
			accounted++
			return nil
		}, nil
	})
	if err := s.Remove(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	if accounted != 1 {
		t.Fatalf("detach settlements=%d", accounted)
	}
	status, err := s.locks.QueryGrant(t.Context(), owner, grant)
	if err != nil || status.State != locking.TargetGone {
		t.Fatalf("retired grant=%+v %v", status, err)
	}
	err = (sqliteNative{s.Store}).Guard(t.Context(), s.backendKey(state.ID), func() error { return errors.New("detached node passed named guard") })
	requirePublicationCode(t, err, locking.StaleResource)
	action := startPublication(t, file, storage.FileIO{Write: true, Truncate: true})
	updated, err := file.CommitContent(t.Context(), action, state.Revision, metastore.Object{ModTime: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := file.Capture(t.Context(), storage.FileIO{})
	if err != nil || !observed.Detached || observed.ID != state.ID || updated.Observation.Attr.ID != uint64(state.ID) {
		t.Fatalf("retained write after strong retirement=%+v %v", observed, err)
	}
}

func TestSharedDatabaseOwnershipRefusesNativeRetentionBeforeMutation(t *testing.T) {
	config := lockingTestConfig(t)
	s, err := OpenWithOptions(t.Context(), config.Database, config.Volume, 0, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := s.CheckFileStore(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("shared native capability=%v", err)
	}
	if _, err := s.FileState(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("shared capability=%v", err)
	}
	if _, _, err := s.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("shared session=%v", err)
	}
	if _, err := s.Stat(t.Context(), "forbidden"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused retention created name=%v", err)
	}
}

func TestFilePublicationGuardRefusesEveryIdentityMutationAtFinalAdmission(t *testing.T) {
	for _, operation := range []string{"create", "reset-and-retain", "truncate-reference", "commit", "file-attributes", "node-attributes"} {
		t.Run(operation, func(t *testing.T) {
			s, file := openPublicationFile(t)
			fixture := publicationFixture{store: s.Store, service: s.LockService()}
			fixture.put(t, t.Context(), "file", 5)
			before, err := file.Capture(t.Context(), storage.FileIO{})
			if err != nil {
				t.Fatal(err)
			}
			var target storage.EntryTarget
			if operation == "create" {
				target = publicationTarget(t, s, file.session, "new")
			} else if operation == "reset-and-retain" {
				target = publicationTarget(t, s, file.session, "file")
			}
			var reserved metastore.Key
			if operation == "commit" {
				reserved, err = file.Reserve(t.Context(), 5)
				if err != nil {
					t.Fatal(err)
				}
			}
			position, err := s.CommittedPosition(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			ctx := metastore.WithFilePublicationGuard(t.Context(), func() error { calls++; return syscall.ESTALE })
			at := time.Now()
			switch operation {
			case "create":
				_, err = file.session.CreateAndRetainAt(ctx, storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.WriteContent}}, fileActionID(t, file.session))
			case "reset-and-retain":
				_, err = file.session.ResetAndRetainAt(ctx, storage.ResetAndRetainRequest{Target: target, ExpectedRevision: before.MetadataRevision, Claim: storage.AccessClaim{Uses: storage.WriteContent}}, fileActionID(t, file.session))
			case "truncate-reference":
				action := startPublication(t, file, storage.FileIO{Write: true, Truncate: true})
				_, err = file.CommitContent(ctx, action, before.Revision, metastore.Object{ModTime: at})
			case "commit":
				action := startPublication(t, file, storage.FileIO{Write: true, Length: 5})
				_, err = file.CommitContent(ctx, action, before.Revision, metastore.Object{Key: reserved, Size: 5, ModTime: at})
			case "file-attributes":
				_, err = file.SetAttr(ctx, storage.AttrChange{ExpectedRevision: before.MetadataRevision, ModTime: &at}, fileActionID(t, file.session))
			case "node-attributes":
				_, err = file.session.SetNodeAttr(ctx, uint64(before.ID), storage.AttrChange{ExpectedRevision: before.MetadataRevision, ModTime: &at}, fileActionID(t, file.session))
			}
			if !errors.Is(err, syscall.ESTALE) || calls != 1 {
				t.Fatalf("guard result=%v calls=%d", err, calls)
			}
			after, err := file.Capture(t.Context(), storage.FileIO{})
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("guard changed state=%+v %v; before=%+v", after, err, before)
			}
			afterPosition, err := s.CommittedPosition(t.Context())
			if err != nil || afterPosition != position {
				t.Fatalf("guard advanced position=%v %v; before=%v", afterPosition, err, position)
			}
			if operation == "create" {
				if _, err := s.Stat(t.Context(), "new"); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("guarded create left name=%v", err)
				}
			}
			if reserved != "" {
				if err := s.Abandon(t.Context(), reserved); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Remove(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			if _, err := file.Close(ctx, fileActionID(t, file.session)); err != nil {
				t.Fatalf("expired publication guard blocked cleanup=%v", err)
			}
		})
	}
}

func TestRangeAuthorityIsSharedAndRetiredWithItsStore(t *testing.T) {
	s, firstFile := openPublicationFile(t)
	ctx := t.Context()
	first := firstFile.session
	native, _, err := s.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	second := native.(*fileSession)
	t.Cleanup(func() {
		if err := second.Dispose(context.Background()); err != nil {
			t.Error(err)
		}
	})
	state, err := firstFile.Capture(ctx, storage.FileIO{})
	if err != nil {
		t.Fatal(err)
	}
	secondFile := retainRangeFile(t, second, uint64(state.ID))
	scope := storage.RangeScope{Domain: 7}
	snapshot, err := firstFile.RangeSnapshot(ctx, 1, scope)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.RangeReplaceRequest{Owner: 1, Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: []storage.RangeAcquisition{{ID: 1, Start: 0, End: 7, Exclusive: true}}}
	if result, err := firstFile.ReplaceRanges(ctx, request, fileActionID(t, first)); err != nil || result.State != storage.FileActionCompleted {
		t.Fatalf("first range=%+v %v", result, err)
	}
	other, err := secondFile.RangeSnapshot(ctx, 2, scope)
	if err != nil || len(other.Other) != 1 {
		t.Fatalf("shared conflict=%+v %v", other, err)
	}
	if _, err := firstFile.RetireRangeOwner(ctx, 1, scope, fileActionID(t, first)); err != nil {
		t.Fatal(err)
	}
	other, err = secondFile.RangeSnapshot(ctx, 2, scope)
	if err != nil || len(other.Other) != 0 {
		t.Fatalf("released conflict=%+v %v", other, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := firstFile.RangeSnapshot(canceled, 1, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled authority access=%v", err)
	}
	if err := second.Dispose(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Dispose(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.NewFileSession(ctx, storage.DefaultFileSessionOptions()); err == nil {
		t.Fatal("closed store admitted another range authority")
	}
}

func TestNodeFacadesKeepIdentityAcrossNameReplacement(t *testing.T) {
	s, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	original, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	replacement, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	current, err := s.Store.StatNode(t.Context(), uint64(original.ID))
	if err != nil || current.ID != original.ID {
		t.Fatalf("identity lookup=%+v %v", current, err)
	}
	metadata := storage.Metadata{{Key: "facade.test", Version: 27, Data: []byte{0, 0xff, 7}}}
	modified := time.Unix(1234, 56).UTC()
	updated, err := s.Store.SetNodeAttr(t.Context(), uint64(original.ID), storage.AttrChange{ExpectedRevision: current.MetadataRevision, Metadata: &metadata, ModTime: &modified})
	if err != nil || updated.ID != original.ID || updated.MetadataRevision != current.MetadataRevision+1 || !reflect.DeepEqual(updated.Metadata, metadata) || !updated.ModTime.Equal(modified) {
		t.Fatalf("identity metadata update=%+v %v", updated, err)
	}
	moved, err := s.Stat(t.Context(), "moved")
	if err != nil || !reflect.DeepEqual(moved, updated) {
		t.Fatalf("moved name disagrees with retained identity: %+v %v", moved, err)
	}
	untouched, err := s.Stat(t.Context(), "file")
	if err != nil || !reflect.DeepEqual(untouched, replacement) {
		t.Fatalf("identity mutation reached replacement: %+v %v", untouched, err)
	}
	if _, err := s.Store.SetNodeAttr(t.Context(), uint64(original.ID), storage.AttrChange{ExpectedRevision: current.MetadataRevision, Metadata: &metadata}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("stale metadata facade=%v", err)
	}
	unchanged, err := s.Store.StatNode(t.Context(), uint64(original.ID))
	if err != nil || !reflect.DeepEqual(unchanged, updated) {
		t.Fatalf("refused update changed identity=%+v %v", unchanged, err)
	}
	if err := s.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Store.StatNode(t.Context(), uint64(original.ID)); !errors.Is(err, syscall.ESTALE) || got.ID != 0 {
		t.Fatalf("removed identity=%+v %v", got, err)
	}
	if got, err := s.Store.SetNodeAttr(t.Context(), uint64(original.ID), storage.AttrChange{ModTime: &modified}); !errors.Is(err, syscall.ESTALE) || got.ID != 0 {
		t.Fatalf("removed identity update=%+v %v", got, err)
	}
}

func TestNodeFacadesRejectInvalidRequestsAndClosedStore(t *testing.T) {
	s, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, id := range []uint64{0, 1 << 63, ^uint64(0)} {
		if got, err := s.Store.StatNode(t.Context(), id); !errors.Is(err, syscall.ESTALE) || got.ID != 0 {
			t.Fatalf("invalid identity %d returned %+v %v", id, got, err)
		}
		if got, err := s.Store.SetNodeAttr(t.Context(), id, storage.AttrChange{}); !errors.Is(err, syscall.ESTALE) || got.ID != 0 {
			t.Fatalf("invalid identity mutation %d returned %+v %v", id, got, err)
		}
	}
	id := uint64(s.root)
	bad := storage.Metadata{{Key: "bad key", Version: 1}}
	for _, change := range []storage.AttrChange{{Metadata: &bad}, {ExpectedRevision: 1, Metadata: &bad}} {
		if got, err := s.Store.SetNodeAttr(t.Context(), id, change); !errors.Is(err, syscall.EINVAL) || got.ID != 0 {
			t.Fatalf("malformed metadata facade=%+v %v", got, err)
		}
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := s.Store.StatNode(cancelled, id); storage.ErrnoOf(err) != syscall.EINTR || got.ID != 0 {
		t.Fatalf("cancelled identity=%+v %v", got, err)
	}
	if got, err := s.Store.SetNodeAttr(cancelled, id, storage.AttrChange{}); storage.ErrnoOf(err) != syscall.EINTR || got.ID != 0 {
		t.Fatalf("cancelled identity mutation=%+v %v", got, err)
	}
	if err := s.Store.CheckFileStore(); err != nil {
		t.Fatalf("exclusive capability=%v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Capability is immutable; live operations still have to reject the closed owner.
	if err := s.Store.CheckFileStore(); err != nil {
		t.Fatalf("closed store changed static capability=%v", err)
	}
	if got, err := s.Store.StatNode(t.Context(), id); err == nil || errors.Is(err, syscall.ENOENT) || got.ID != 0 {
		t.Fatalf("closed identity lookup=%+v %v", got, err)
	}
	if got, err := s.Store.SetNodeAttr(t.Context(), id, storage.AttrChange{}); err == nil || errors.Is(err, syscall.ENOENT) || got.ID != 0 {
		t.Fatalf("closed identity mutation=%+v %v", got, err)
	}
	if err := (&Store{}).CheckFileStore(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("absent native ownership capability=%v", err)
	}
}
