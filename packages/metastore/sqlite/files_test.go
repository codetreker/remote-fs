package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func openPublicationFile(t *testing.T) (*LockingStore, metastore.File) {
	t.Helper()
	config := lockingTestConfig(t)
	config.Allowance = 100
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	f, err := s.OpenFile(t.Context(), "file", storage.FileOpenOptions{Read: true, Write: true, Create: true, Mode: 0o600})
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(context.Background()); err != nil {
			t.Error(err)
		}
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, f
}

func TestRetiredReferenceCannotPublishAPreviouslyReservedObject(t *testing.T) {
	s, f := openPublicationFile(t)
	before, err := f.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key, err := f.Reserve(t.Context(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := f.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Commit(t.Context(), before.Revision, metastore.Object{Key: key, Size: 7, ModTime: time.Now()}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired commit: %v", err)
	}
	var nodes, reserved int
	if err := s.read.QueryRow(`SELECT (SELECT count(*) FROM nodes WHERE id=? AND detached=1),(SELECT state FROM objects WHERE key=?)`, before.ID, string(key)).Scan(&nodes, &reserved); err != nil {
		t.Fatal(err)
	}
	if nodes != 1 || reserved != stateReserved {
		t.Fatalf("physical node=%d object state=%d", nodes, reserved)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("usage=%d error=%v", used, err)
	}
	if err := s.Abandon(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StatNode(t.Context(), uint64(before.ID)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("released node: %v", err)
	}
}

func TestRetirementOrdersAfterAnAdmittedFinalPublication(t *testing.T) {
	s, f := openPublicationFile(t)
	before, err := f.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key, err := f.Reserve(t.Context(), 9)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
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
		_, err := f.Commit(ctx, before.Revision, metastore.Object{Key: key, Size: 9, ModTime: time.Now()})
		committed <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("publication did not reach its final gate")
	}
	retired := make(chan error, 1)
	go func() { retired <- f.Retire(t.Context()) }()
	select {
	case err := <-retired:
		t.Fatalf("retirement passed an active final publication: %v", err)
	default:
	}
	close(release)
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	if err := <-retired; err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "file")
	if err != nil || node.Size != 9 {
		t.Fatalf("published node=%+v error=%v", node, err)
	}
	if _, err := f.Commit(t.Context(), before.Revision+1, metastore.Object{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("later commit: %v", err)
	}
}

func TestRetainedCleanupKnownRefusalPreservesThePinForRetry(t *testing.T) {
	s, f := openPublicationFile(t)
	state, err := f.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key, err := f.Reserve(t.Context(), 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Commit(t.Context(), state.Revision, metastore.Object{Key: key, Size: 5, ModTime: time.Now()}); err != nil {
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
	if err := f.Close(ctx); !errors.Is(err, refused) {
		t.Fatalf("cleanup refusal: %v", err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 5 {
		t.Fatalf("refused cleanup usage=%d error=%v", used, err)
	}
	if err := f.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("retried cleanup usage=%d error=%v", used, err)
	}
}

type retainedFailureWitness struct{ failure error }

func (w *retainedFailureWitness) Accept(DurableState) error     { return w.failure }
func (w *retainedFailureWitness) Checkpoint(DurableState) error { return nil }

func TestRetainedCleanupUnknownAcceptanceKeepsPhysicalOwnership(t *testing.T) {
	config := lockingTestConfig(t)
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	f, err := s.OpenFile(t.Context(), "file", storage.FileOpenOptions{Read: true, Write: true, Create: true})
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	ref := f.(*retainedFile)
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
	err = f.Close(ctx)
	if !errors.Is(err, acceptErr) || !storage.IsPublicationAccountingUncertain(err) {
		t.Fatalf("unknown cleanup: %v", err)
	}
	if again := f.Close(ctx); !errors.Is(again, acceptErr) || settlements.Load() != 1 {
		t.Fatalf("repeated cleanup=%v settlements=%d", again, settlements.Load())
	}
	if s.coordinator.pins[retainedNode{s.volume, ref.id}] != 1 || len(s.files) != 1 || s.leaseOwner.FD() < 0 {
		t.Fatal("unknown cleanup released native retention")
	}
	if err := s.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("store close with unresolved reference: %v", err)
	}
	// Model process teardown after observing that public cleanup kept every hold.
	if err := s.locks.Close(); err != nil && !errors.Is(err, acceptErr) {
		t.Fatal(err)
	}
	s.locks = nil
	s.witness = nil
	s.files = nil
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestStrongGrantsRetireTheNameWhileOrdinaryReferencesKeepTheNode(t *testing.T) {
	s, file := openPublicationFile(t)
	f := publicationFixture{store: s.Store, service: s.LockService()}
	f.put(t, t.Context(), "file", 5)
	owner := f.owner(t)
	grant := f.grant(t, owner, "file", locking.Exclusive)
	opened, err := s.OpenFile(t.Context(), "file", storage.FileOpenOptions{Read: true, Create: true, Mode: 0o777})
	if err != nil {
		t.Fatalf("ordinary open while strongly protected: %v", err)
	}
	if err := opened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	requirePublicationCode(t, s.Remove(t.Context(), "file"), locking.Conflict)
	state, err := file.Node(t.Context())
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
		t.Fatalf("retired grant=%+v error=%v", status, err)
	}
	err = (sqliteNative{s.Store}).Guard(t.Context(), s.backendKey(state.ID), func() error { return errors.New("detached node passed named guard") })
	requirePublicationCode(t, err, locking.StaleResource)
	updated, err := file.Commit(t.Context(), state.Revision, metastore.Object{ModTime: time.Now()})
	if err != nil || !updated.Detached || updated.ID != state.ID {
		t.Fatalf("retained write after strong retirement=%+v error=%v", updated, err)
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
		t.Fatalf("shared capability=%v", err)
	}
	if _, err := s.OpenFile(t.Context(), "forbidden", storage.FileOpenOptions{Write: true, Create: true}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("shared create/open=%v", err)
	}
	if _, err := s.Stat(t.Context(), "forbidden"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused retention created a name: %v", err)
	}
}

func TestFilePublicationGuardRefusesEveryIdentityMutationAtFinalAdmission(t *testing.T) {
	for _, operation := range []string{"create", "truncate-open", "truncate-node", "commit", "file-attributes", "node-attributes"} {
		t.Run(operation, func(t *testing.T) {
			s, file := openPublicationFile(t)
			f := publicationFixture{store: s.Store, service: s.LockService()}
			f.put(t, t.Context(), "file", 5)
			before, err := file.Node(t.Context())
			if err != nil {
				t.Fatal(err)
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
				_, err = s.OpenFile(ctx, "new", storage.FileOpenOptions{Write: true, Create: true})
			case "truncate-open":
				_, err = s.OpenFile(ctx, "file", storage.FileOpenOptions{Write: true, Truncate: true})
			case "truncate-node":
				_, err = s.OpenNode(ctx, uint64(before.ID), storage.FileOpenOptions{Write: true, Truncate: true})
			case "commit":
				_, err = file.Commit(ctx, before.Revision, metastore.Object{ModTime: at})
			case "file-attributes":
				_, err = file.SetAttr(ctx, storage.AttrChange{ModTime: &at})
			case "node-attributes":
				_, err = s.SetNodeAttr(ctx, uint64(before.ID), storage.AttrChange{ModTime: &at})
			}
			if !errors.Is(err, syscall.ESTALE) || calls != 1 {
				t.Fatalf("guard result=%v calls=%d", err, calls)
			}
			after, err := file.Node(t.Context())
			if err != nil || after != before {
				t.Fatalf("guarded mutation changed state=%+v error=%v; before=%+v", after, err, before)
			}
			afterPosition, err := s.CommittedPosition(t.Context())
			if err != nil || afterPosition != position {
				t.Fatalf("guarded mutation advanced position=%v error=%v; before=%v", afterPosition, err, position)
			}
			if operation == "create" {
				if _, err := s.Stat(t.Context(), "new"); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("guarded create left name: %v", err)
				}
			}
			if err := s.Remove(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(ctx); err != nil {
				t.Fatalf("expired guard prevented reference cleanup: %v", err)
			}
		})
	}
}

func TestAdvisoryAuthorityIsSharedAndRetiredWithItsStore(t *testing.T) {
	s, f := openPublicationFile(t)
	ctx := t.Context()
	a, err := s.Advisory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Advisory(ctx)
	if err != nil || a != b {
		t.Fatalf("volume authority changed: %p, %p, %v", a, b, err)
	}
	first, err := a.NewSession(storage.DefaultFileSessionOptions(), func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := first.Retire(context.Background()); err != nil {
			t.Error(err)
		}
	})
	second, err := b.NewSession(storage.DefaultFileSessionOptions(), func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := second.Retire(context.Background()); err != nil {
			t.Error(err)
		}
	})
	node, err := f.Node(ctx)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := first.Epoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.POSIX, Type: storage.Exclusive, Start: 0, End: 7}
	if attempt, err := first.Set(ctx, uint64(node.ID), 1, lock, id); err != nil || attempt.State != storage.LockGranted {
		t.Fatalf("first lock = %+v, %v", attempt, err)
	}
	if conflict, err := second.Get(ctx, uint64(node.ID), 2, lock); err != nil || !conflict.Found {
		t.Fatalf("shared conflict = %+v, %v", conflict, err)
	}
	if err := first.Drop(ctx, uint64(node.ID), 1, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	if conflict, err := second.Get(ctx, uint64(node.ID), 2, lock); err != nil || conflict.Found {
		t.Fatalf("released conflict = %+v, %v", conflict, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Advisory(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled authority access = %v", err)
	}
	if err := f.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advisory(ctx); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired authority access = %v", err)
	}
}
