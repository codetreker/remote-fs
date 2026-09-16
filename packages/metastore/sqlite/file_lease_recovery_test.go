package sqlite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileLeaseQuiescentCloseAndReactivation(t *testing.T) {
	config := lockingTestConfig(t)
	opened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	initial := opened.fileLeaseRecovery.state
	if initial.Generation != 0 || initial.MaxLease != 0 || !initial.Quiescent {
		t.Fatalf("initial=%+v", initial)
	}
	strong := opened.leaseRecovery.state
	session, _, err := opened.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	active := opened.fileLeaseRecovery.state
	if active.Generation != 1 || active.Quiescent || active.MaxLease != storage.DefaultFileSessionOptions().Lease || opened.leaseRecovery.state != strong {
		t.Fatalf("active=%+v strong=%+v", active, opened.leaseRecovery.state)
	}
	if err := session.Dispose(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	reopened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	clean := reopened.fileLeaseRecovery.state
	if !clean.Quiescent || clean.Generation != 2 || clean.MaxLease != active.MaxLease {
		t.Fatalf("clean=%+v", clean)
	}
	if _, err := reopened.FileState(t.Context()); err != nil {
		t.Fatalf("clean reopen gated: %v", err)
	}
	next, _, err := reopened.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	if reopened.fileLeaseRecovery.state.Quiescent || reopened.fileLeaseRecovery.state.Generation != 3 {
		t.Fatalf("reactivation=%+v", reopened.fileLeaseRecovery.state)
	}
	if err := next.Dispose(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestFileLeaseCrashKeepsIndependentRecoveryBarrier(t *testing.T) {
	config := lockingTestConfig(t)
	opened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	_, _, err = opened.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	active := opened.fileLeaseRecovery.state
	if err := opened.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	reopened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if reopened.fileLeaseRecovery.state != active || !reopened.fileDomain.recoveryUntil.Equal(reopened.fileLeaseRecovery.start.Add(active.MaxLease)) {
		t.Fatalf("lost recovery evidence: %+v", reopened.fileLeaseRecovery.state)
	}
	reopened.fileDomain.recoveryUntil = time.Now().Add(time.Hour)
	if _, err := reopened.Stat(metastore.WithFileIO(t.Context(), storage.FileIO{Length: 1}), "file"); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("future barrier=%v", err)
	}
	reopened.fileDomain.recoveryUntil = time.Now().Add(-time.Second)
	if _, err := reopened.Stat(metastore.WithFileIO(t.Context(), storage.FileIO{Length: 1}), "file"); err != nil {
		t.Fatalf("expired barrier=%v", err)
	}
}

func TestFileLeaseQuiescenceChecksEveryDatabaseDomain(t *testing.T) {
	opened, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := opened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := opened.RaiseFileMaxLease(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}
	active := opened.fileLeaseRecovery.state
	other := &fileDomain{sessions: map[*fileSession]struct{}{}, memberships: map[*fileIOMembership]struct{}{}}
	opened.coordinator.domains[999] = other
	defer delete(opened.coordinator.domains, 999)
	for _, blocked := range []struct {
		name  string
		set   func()
		clear func()
	}{
		{"session", func() { other.sessions[&fileSession{}] = struct{}{} }, func() { clear(other.sessions) }},
		{"reference", func() { other.files = 1 }, func() { other.files = 0 }},
		{"io", func() { other.activeIO = 1 }, func() { other.activeIO = 0 }},
		{"membership", func() { other.memberships[&fileIOMembership{}] = struct{}{} }, func() { clear(other.memberships) }},
		{"recovery", func() { other.recoveryPending = true }, func() { other.recoveryPending = false }},
		{"prior lease", func() { other.recoveryUntil = time.Now().Add(time.Hour) }, func() { other.recoveryUntil = time.Time{} }},
	} {
		t.Run(blocked.name, func(t *testing.T) {
			blocked.set()
			defer blocked.clear()
			if err := opened.coordinator.fileAdmission.acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			err := opened.markFileQuiescent(t.Context())
			opened.coordinator.fileAdmission.release()
			if err != nil || opened.fileLeaseRecovery.state != active {
				t.Fatalf("quiesced occupied domain: %+v,%v", opened.fileLeaseRecovery.state, err)
			}
		})
	}
	if err := opened.coordinator.fileAdmission.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	err = opened.markFileQuiescent(t.Context())
	opened.coordinator.fileAdmission.release()
	if err != nil || !opened.fileLeaseRecovery.state.Quiescent {
		t.Fatalf("drained=%+v,%v", opened.fileLeaseRecovery.state, err)
	}
}

type failingFileWitness struct {
	LeaseWitness
	failure error
}

func (w failingFileWitness) Advance(next LeaseEvidence) error {
	if err := w.LeaseWitness.Advance(next); err != nil {
		return err
	}
	if next.Quiescent {
		return w.failure
	}
	return nil
}

func TestFileLeaseInterruptedQuiescentWitnessReconciles(t *testing.T) {
	config := lockingTestConfig(t)
	opened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.RaiseFileMaxLease(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("witness reply interrupted")
	opened.fileLeaseRecovery.witness = failingFileWitness{opened.fileLeaseRecovery.witness, sentinel}
	if err := opened.Close(); !errors.Is(err, sentinel) {
		t.Fatalf("close=%v", err)
	}
	if opened.fileLeaseRecovery.state.Quiescent {
		t.Fatal("uncertain witness acknowledged quiescence")
	}
	if err := opened.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	reopened, err := OpenLocking(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if !reopened.fileLeaseRecovery.state.Quiescent {
		t.Fatal("prepared quiescence not reconciled")
	}
	if _, err := reopened.FileState(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestFileLeasePreparedCrashRetainsDrainUntilRecovery(t *testing.T) {
	config := lockingTestConfig(t)
	opened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	native, _, err := opened.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := native.(*fileSession)
	root := nativeRootReference(t, session)
	condition := storage.RemovalFile
	result, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{
		Target: nativeTarget(t, root, []byte("prepared")), Initial: storage.NodeInitial{Kind: storage.NodeRegular},
		Claim: storage.AccessClaim{Uses: storage.AllAccessUses}, Prepared: &condition,
	}, fileActionID(t, session))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Removal.Prepared {
		t.Fatalf("prepared=%+v", result)
	}
	if err := opened.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	reopened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if !reopened.fileDomain.recoveryPending || reopened.fileLeaseRecovery.state.Quiescent {
		t.Fatal("lost crash removal obligation")
	}
	if err := reopened.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("premature close=%v", err)
	}
	if err := reopened.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened.fileDomain.recoveryUntil = time.Now().Add(-time.Second)
	err = reopened.recoverFileEntriesLocked(t.Context())
	reopened.coordinator.commit.release()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Stat(t.Context(), "prepared"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("recovered name=%v", err)
	}
	if usage, err := reopened.Usage(t.Context()); err != nil || usage != 0 {
		t.Fatalf("usage=%d,%v", usage, err)
	}
}

func TestFileLeaseAdmissionCancellationDoesNotCrossDrainGate(t *testing.T) {
	opened, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := opened.Close(); err != nil {
			t.Error(err)
		}
	}()
	initial := opened.fileLeaseRecovery.state
	if err := opened.coordinator.fileAdmission.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		session, _, err := opened.NewFileSession(ctx, storage.DefaultFileSessionOptions())
		if session != nil {
			err = errors.Join(err, session.Dispose(context.Background()))
		}
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("enrollment=%v", err)
		}
	case <-time.After(time.Second):
		t.Error("canceled enrollment held behind drain gate")
	}
	opened.coordinator.fileAdmission.release()
	if opened.fileLeaseRecovery.state != initial || len(opened.fileDomain.sessions) != 0 {
		t.Fatal("canceled enrollment changed file evidence or ownership")
	}
}

func TestFileLeaseReopenAnotherVolumeCannotEraseInheritedLease(t *testing.T) {
	config := lockingTestConfig(t)
	for _, volume := range []string{"first", "second"} {
		store, err := OpenWithOptions(t.Context(), config.Database, volume, 0, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	config.Volume = "first"
	first, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = first.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	active := first.fileLeaseRecovery.state
	if err := first.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	config.Volume = "second"
	second, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.coordinator.domains) != 1 || !second.fileDomain.recoveryUntil.Equal(second.fileLeaseRecovery.start.Add(active.MaxLease)) {
		t.Fatal("new volume did not inherit database lease deadline")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	config.Volume = "first"
	again, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := again.Close(); err != nil {
			t.Error(err)
		}
	}()
	if again.fileLeaseRecovery.state != active {
		t.Fatalf("unopened old volume protection erased: %+v", again.fileLeaseRecovery.state)
	}
	if _, err := again.FileState(t.Context()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("inherited barrier=%v", err)
	}
	again.fileDomain.recoveryUntil = time.Now().Add(-time.Second)
	if err := again.coordinator.fileAdmission.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	err = again.markFileQuiescent(t.Context())
	again.coordinator.fileAdmission.release()
	if err != nil || !again.fileLeaseRecovery.state.Quiescent {
		t.Fatalf("expired inherited lease not drained: %+v,%v", again.fileLeaseRecovery.state, err)
	}
}

func TestFileLeaseReaderRejectsUnboundedStoredScalars(t *testing.T) {
	opened, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := opened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, column := range []string{"accepted_generation", "accepted_nanos", "accepted_quiescent", "prepared_generation", "prepared_nanos", "prepared_quiescent", "database_id", "state_id"} {
		t.Run(column, func(t *testing.T) {
			if err := opened.coordinator.commit.acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			defer opened.coordinator.commit.release()
			tx, err := opened.write.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var value any = make([]byte, 1<<20)
			if column == "database_id" || column == "state_id" {
				value = "0123456789abcdef0123456789abcdef\x00" + string(make([]byte, 1<<20))
			}
			if _, err := tx.ExecContext(t.Context(), `UPDATE file_lease_recovery SET `+column+`=?`, value); err != nil {
				t.Fatal(err)
			}
			if _, _, err := readFileLeaseRecord(t.Context(), tx); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid scalar=%v", err)
			}
		})
	}
}

type cancelAfterStrongClose struct {
	context.Context
	cancel context.CancelFunc
	closed func() bool
}

func (c cancelAfterStrongClose) Done() <-chan struct{} {
	if c.closed() {
		c.cancel()
	}
	return c.Context.Done()
}

func TestFileLeaseCloseRetryAfterStrongRetirement(t *testing.T) {
	config := lockingTestConfig(t)
	opened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !opened.Terminal() {
			if err := opened.Abort(); err != nil {
				t.Error(err)
			}
		}
	})
	if err := opened.RaiseFileMaxLease(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := cancelAfterStrongClose{Context: base, cancel: cancel, closed: func() bool { return opened.locks.Check(context.Background()) != nil }}
	if err := opened.CloseContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-retirement close=%v", err)
	}
	if !opened.fileClosePrepared || !opened.fileLeaseRecovery.state.Quiescent || opened.coordinator.closing || opened.locks.Check(context.Background()) == nil {
		t.Fatal("cancellation did not occur between Strong retirement and final commit admission")
	}
	proof := opened.fileLeaseRecovery.state
	if err := opened.Close(); err != nil {
		t.Fatalf("retry=%v", err)
	}
	config.Initialize = false
	reopened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if reopened.fileLeaseRecovery.state != proof {
		t.Fatal("close retry rewrote file evidence")
	}
}

func TestFileOnlyBoundDurableOpenPreservesIndependentEvidence(t *testing.T) {
	ctx := t.Context()
	database := filepath.Join(t.TempDir(), "file-only.db")
	witness := &checkpointStateWitness{}
	open := func(mode VolumeOpenMode, startup DurableStartup, initialize bool) (*Store, *LeaseAnchor) {
		t.Helper()
		store, err := OpenBoundDurableFileWithOptions(ctx, database, "volume", "objects", 0, DefaultOptions(), mode, startup, witness)
		if err != nil {
			t.Fatal(err)
		}
		var anchor *LeaseAnchor
		t.Cleanup(func() {
			if err := store.Abort(); err != nil {
				t.Error(err)
			}
			if anchor != nil {
				if err := anchor.Close(); err != nil {
					t.Error(err)
				}
			}
		})
		if store.leaseOwner == nil || !store.leaseOwner.Exclusive() {
			t.Fatal("File-only durable opener did not acquire exclusive native ownership")
		}
		if store.LockService() != nil || store.leaseRecovery != nil {
			t.Fatal("File-only durable opener attached Strong recovery")
		}
		var strongRecords int
		if err := store.read.QueryRowContext(ctx, `SELECT count(*) FROM lease_recovery`).Scan(&strongRecords); err != nil || strongRecords != 0 {
			t.Fatalf("Strong recovery records=%d: %v", strongRecords, err)
		}
		if _, _, err := store.NewFileSession(ctx, storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("session before File witness binding=%v", err)
		}
		pending, err := store.FileLeaseInitializationPending(ctx)
		if err != nil || pending != initialize {
			t.Fatalf("File initialization pending=%v, want=%v: %v", pending, initialize, err)
		}
		anchor, err = OpenLeaseAnchor(LeaseAnchorConfig{
			Domain: LeaseDomainFile, Directory: filepath.Dir(database), Name: ".file-leases",
			Identity: "file-only-volume", BindingFD: store.leaseOwner.FD(),
			RecoveryStart: store.leaseOwner.Acquired(), Initialize: pending,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.ConfigureFileLeaseRecovery(ctx, LeaseRecoveryConfig{
			Witness: anchor, StateID: anchor.StateID(), Initialize: anchor.Initializing(), RecoveryStart: store.leaseOwner.Acquired(),
		}); err != nil {
			t.Fatal(err)
		}
		if pending, err := store.FileLeaseInitializationPending(ctx); err != nil || pending {
			t.Fatalf("File binding did not complete initialization: %v, %v", pending, err)
		}
		return store, anchor
	}
	store, anchor := open(CreateVolumeIfMissing, DurableStartup{}, true)
	stateID := anchor.StateID()
	native, _, err := store.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := native.(*fileSession)
	root := nativeRootReference(t, session)
	created, err := session.CreateAndRetainAt(ctx, storage.CreateAndRetainRequest{
		Target: nativeTarget(t, root, []byte("persisted")), Initial: storage.NodeInitial{Kind: storage.NodeRegular},
		Claim: storage.AccessClaim{Uses: storage.ReadContent},
	}, fileActionID(t, session))
	if err != nil || created.State != storage.FileActionCompleted || created.Reference == 0 {
		t.Fatalf("native create=%+v: %v", created, err)
	}
	if got, err := store.Stat(ctx, "persisted"); err != nil || got.Kind != storage.NodeRegular {
		t.Fatalf("created node=%+v: %v", got, err)
	}
	if err := session.Dispose(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	clean := store.fileLeaseRecovery.state
	if !clean.Quiescent || clean.MaxLease != storage.DefaultFileSessionOptions().Lease {
		t.Fatalf("closed File lease evidence=%+v", clean)
	}
	if witness.accepted.DatabaseID == "" || witness.checkpointed != witness.accepted {
		t.Fatalf("close witness mismatch: accepted=%+v checkpointed=%+v", witness.accepted, witness.checkpointed)
	}
	if observed, err := InspectDurableState(ctx, database); err != nil || observed != witness.accepted {
		t.Fatalf("closed database=%+v: %v; accepted=%+v", observed, err, witness.accepted)
	}
	if err := anchor.Close(); err != nil {
		t.Fatal(err)
	}
	startup := DurableStartup{Accepted: witness.accepted, CheckpointedGeneration: witness.checkpointed.Generation}
	if wal, err := os.Stat(database + "-wal"); err == nil {
		startup.WALPresent = true
		startup.WALNonEmpty = wal.Size() != 0
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	reopened, nextAnchor := open(RequireExistingVolume, startup, false)
	if nextAnchor.StateID() != stateID || reopened.fileLeaseRecovery.state != clean {
		t.Fatalf("reopened File evidence changed identity: state=%+v id=%q", reopened.fileLeaseRecovery.state, nextAnchor.StateID())
	}
	if evidence, found, err := nextAnchor.Load(); err != nil || !found || evidence != clean {
		t.Fatalf("File witness=%+v found=%v: %v; expected=%+v", evidence, found, err, clean)
	}
	if observed, err := reopened.Stat(ctx, "persisted"); err != nil || uint64(observed.ID) != created.Observation.Attr.ID {
		t.Fatalf("reopened node=%+v: %v; created=%+v", observed, err, created.Observation)
	}
	next, _, err := reopened.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatalf("clean reopen imposed a lease wait: %v", err)
	}
	if err := next.Dispose(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
