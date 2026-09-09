package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
)

type testLeaseWitness struct {
	state   LeaseEvidence
	present bool
	before  func(LeaseEvidence) error
	after   func(LeaseEvidence) error
}

func (w *testLeaseWitness) Load() (LeaseEvidence, bool, error) { return w.state, w.present, nil }
func (w *testLeaseWitness) Advance(next LeaseEvidence) error {
	if w.before != nil {
		if err := w.before(next); err != nil {
			return err
		}
	}
	w.state, w.present = next, true
	if w.after != nil {
		return w.after(next)
	}
	return nil
}

func openLeaseTestStore(t *testing.T, path string, witness *testLeaseWitness, initialize bool) *Store {
	t.Helper()
	options := DefaultOptions()
	options.leaseRecoveryOwner = true
	s, err := OpenWithOptions(t.Context(), path, "workspace", 0, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.configureLeaseRecovery(t.Context(), LeaseRecoveryConfig{
		Witness: witness, RecoveryStart: time.Now(), StateID: "0123456789abcdef0123456789abcdef", Initialize: initialize,
	}); err != nil {
		s.Abort()
		t.Fatal(err)
	}
	return s
}

func abortLeaseTestStore(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseRecoveryConcurrentRaisesNeverDecrease(t *testing.T) {
	witness := &testLeaseWitness{}
	path := filepath.Join(t.TempDir(), "namespace.sqlite")
	s := openLeaseTestStore(t, path, witness, true)
	var wg sync.WaitGroup
	for i := 64; i > 0; i-- {
		wg.Add(1)
		go func(ttl time.Duration) {
			defer wg.Done()
			if err := s.RaiseMaxLease(t.Context(), ttl); err != nil {
				t.Error(err)
			}
		}(time.Duration(i) * time.Second)
	}
	wg.Wait()
	if got, err := s.MaxLease(t.Context()); err != nil || got != 64*time.Second {
		t.Fatalf("maximum = %v, %v", got, err)
	}
	abortLeaseTestStore(t, s)
	s = openLeaseTestStore(t, path, witness, false)
	defer abortLeaseTestStore(t, s)
	if err := s.RaiseMaxLease(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}
	if got, err := s.MaxLease(t.Context()); err != nil || got != 64*time.Second {
		t.Fatalf("reopened maximum = %v, %v", got, err)
	}
}

func TestLeaseRecoveryFinishesEveryDurableInterruption(t *testing.T) {
	for _, phase := range []string{"prepare", "witness before", "witness after", "finalize"} {
		t.Run(phase, func(t *testing.T) {
			witness := &testLeaseWitness{}
			path := filepath.Join(t.TempDir(), "namespace.sqlite")
			s := openLeaseTestStore(t, path, witness, true)
			fault := errors.New("injected durable lease failure")
			switch phase {
			case "prepare":
				_, err := s.write.Exec(`CREATE TRIGGER reject_lease BEFORE UPDATE OF prepared_nanos
					ON lease_recovery BEGIN SELECT RAISE(ABORT, 'injected prepare failure'); END`)
				if err != nil {
					t.Fatal(err)
				}
			case "witness before":
				witness.before = func(LeaseEvidence) error { return fault }
			case "witness after":
				witness.after = func(LeaseEvidence) error { return fault }
			case "finalize":
				_, err := s.write.Exec(`CREATE TRIGGER reject_lease BEFORE UPDATE OF accepted_nanos
					ON lease_recovery BEGIN SELECT RAISE(ABORT, 'injected finalize failure'); END`)
				if err != nil {
					t.Fatal(err)
				}
			}
			err := s.RaiseMaxLease(t.Context(), time.Minute)
			if err == nil {
				t.Fatal("interrupted raise succeeded")
			}
			if phase == "witness before" || phase == "witness after" {
				if !errors.Is(err, fault) || !errors.Is(err, syscall.EIO) {
					t.Fatalf("lost failure chain: %v", err)
				}
			}
			if _, err := s.write.Exec(`DROP TRIGGER IF EXISTS reject_lease`); err != nil {
				t.Fatal(err)
			}
			witness.before, witness.after = nil, nil
			abortLeaseTestStore(t, s)
			s = openLeaseTestStore(t, path, witness, false)
			defer abortLeaseTestStore(t, s)
			want := time.Minute
			if phase == "prepare" {
				want = 0
			}
			if got, err := s.MaxLease(t.Context()); err != nil || got != want {
				t.Fatalf("maximum = %v, %v; want %v", got, err, want)
			}
			var prepared sql.NullInt64
			if err := s.read.QueryRow(`SELECT prepared_generation FROM lease_recovery`).Scan(&prepared); err != nil {
				t.Fatal(err)
			}
			if prepared.Valid {
				t.Fatal("recovery left a prepared generation")
			}
		})
	}
}

func TestLeaseRecoveryRefusesLostReplayedOrMalformedEvidence(t *testing.T) {
	tests := []struct {
		name, damage string
		witness      func(*testLeaseWitness)
	}{
		{"missing state", `DELETE FROM lease_recovery`, nil},
		{"missing witness", "", func(w *testLeaseWitness) { w.present = false }},
		{"rolled back state", `UPDATE lease_recovery SET accepted_generation = 0, accepted_nanos = 0`, nil},
		{"rolled back witness", "", func(w *testLeaseWitness) { w.state.Generation = 0; w.state.MaxLease = 0 }},
		{"wrong database", `UPDATE lease_recovery SET database_id = 'abcdef0123456789abcdef0123456789'`, nil},
		{"wrong state", `UPDATE lease_recovery SET state_id = 'abcdef0123456789abcdef0123456789'`, nil},
		{"text counter", `UPDATE lease_recovery SET accepted_nanos = 'broken'`, nil},
		{"negative counter", `UPDATE lease_recovery SET accepted_nanos = -1`, nil},
		{"partial prepared", `UPDATE lease_recovery SET prepared_generation = 2`, nil},
		{"skipped generation", `UPDATE lease_recovery SET prepared_generation = 3, prepared_nanos = 60000000000`, nil},
		{"lower prepared", `UPDATE lease_recovery SET prepared_generation = 2, prepared_nanos = 1`, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			witness := &testLeaseWitness{}
			path := filepath.Join(t.TempDir(), "namespace.sqlite")
			s := openLeaseTestStore(t, path, witness, true)
			if err := s.RaiseMaxLease(t.Context(), time.Minute); err != nil {
				t.Fatal(err)
			}
			if test.damage != "" {
				if _, err := s.write.Exec(test.damage); err != nil {
					t.Fatal(err)
				}
			}
			if test.witness != nil {
				test.witness(witness)
			}
			abortLeaseTestStore(t, s)
			options := DefaultOptions()
			options.leaseRecoveryOwner = true
			s, err := OpenWithOptions(t.Context(), path, "workspace", 0, options)
			if err != nil {
				t.Fatal(err)
			}
			defer abortLeaseTestStore(t, s)
			err = s.configureLeaseRecovery(t.Context(), LeaseRecoveryConfig{
				Witness: witness, RecoveryStart: time.Now(), StateID: "0123456789abcdef0123456789abcdef",
			})
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("damaged evidence returned %v", err)
			}
		})
	}
}

func TestLeaseRecoveryCancellationDoesNotAcknowledgeAnIncrease(t *testing.T) {
	witness := &testLeaseWitness{}
	s := openLeaseTestStore(t, filepath.Join(t.TempDir(), "namespace.sqlite"), witness, true)
	defer abortLeaseTestStore(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.RaiseMaxLease(ctx, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if got, err := s.MaxLease(t.Context()); err != nil || got != 0 {
		t.Fatalf("maximum = %v, %v", got, err)
	}
	witness.after = func(LeaseEvidence) error { cancel(); return nil }
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	if err := s.RaiseMaxLease(ctx, time.Minute); !errors.Is(err, syscall.EIO) || !errors.Is(err, context.Canceled) {
		t.Fatalf("post-witness cancellation = %v", err)
	}
	if _, err := s.MaxLease(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("uncertain store stayed usable: %v", err)
	}
}

func TestConfigureLeaseRecoveryRequiresNativeAnchor(t *testing.T) {
	t.Run("shared owner", func(t *testing.T) {
		config := lockingTestConfig(t)
		store, err := OpenWithOptions(t.Context(), config.Database, config.Namespace, 0, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}()
		err = store.ConfigureLeaseRecovery(t.Context(), LeaseRecoveryConfig{
			Witness: &testLeaseWitness{}, StateID: "0123456789abcdef0123456789abcdef",
			RecoveryStart: time.Now(), Initialize: true,
		})
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("shared owner configured lease recovery: %v", err)
		}
		assertNoLeaseAuthority(t, store)
	})
	for _, witness := range []struct {
		name  string
		value LeaseWitness
	}{
		{"generic witness", &testLeaseWitness{}},
		{"nil witness", nil},
		{"typed nil anchor", (*LeaseAnchor)(nil)},
	} {
		t.Run(witness.name, func(t *testing.T) {
			store, _ := openNativeExclusiveTestStore(t)
			err := store.ConfigureLeaseRecovery(t.Context(), LeaseRecoveryConfig{
				Witness: witness.value, StateID: "0123456789abcdef0123456789abcdef",
				RecoveryStart: time.Now(), Initialize: true,
			})
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("unanchored witness configured lease recovery: %v", err)
			}
			assertNoLeaseAuthority(t, store)
		})
	}
	t.Run("different binding inode", func(t *testing.T) {
		store, owner := openNativeExclusiveTestStore(t)
		foreign, err := nativelease.AcquireDatabase(filepath.Join(filepath.Dir(owner.Path()), "foreign.sqlite"), true, true)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := foreign.Close(); err != nil {
				t.Error(err)
			}
		}()
		anchor := openNativeTestAnchor(t, store, foreign)
		err = store.ConfigureLeaseRecovery(t.Context(), LeaseRecoveryConfig{
			Witness: anchor, StateID: anchor.StateID(), RecoveryStart: owner.Acquired(), Initialize: true,
		})
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("different binding inode configured lease recovery: %v", err)
		}
		assertNoLeaseAuthority(t, store)
	})
	for _, invalid := range []string{"state identity", "initialization intent"} {
		t.Run(invalid, func(t *testing.T) {
			store, owner := openNativeExclusiveTestStore(t)
			anchor := openNativeTestAnchor(t, store, owner)
			config := LeaseRecoveryConfig{
				Witness: anchor, StateID: anchor.StateID(), RecoveryStart: owner.Acquired(), Initialize: true,
			}
			if invalid == "state identity" {
				config.StateID = "invalid"
			} else {
				config.Initialize = false
			}
			if err := store.ConfigureLeaseRecovery(t.Context(), config); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid %s configured lease recovery: %v", invalid, err)
			}
			assertNoLeaseAuthority(t, store)
			config.StateID, config.Initialize = anchor.StateID(), true
			if err := store.ConfigureLeaseRecovery(t.Context(), config); err != nil {
				t.Fatalf("valid native recovery after rejected configuration: %v", err)
			}
			if err := anchor.Complete(); err != nil {
				t.Fatal(err)
			}
			if err := store.EnableLocks(t.Context(), locking.DefaultOptions()); err != nil {
				t.Fatalf("valid native authority: %v", err)
			}
			if store.LockService() == nil {
				t.Fatal("valid native recovery did not expose its authority")
			}
		})
	}
}

func openNativeExclusiveTestStore(t *testing.T) (*Store, *nativelease.Database) {
	t.Helper()
	config := lockingTestConfig(t)
	owner, err := nativelease.AcquireDatabase(config.Database, true, true)
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultOptions()
	options.leaseRecoveryOwner, options.leaseOwner = true, owner
	store, err := OpenWithOptions(t.Context(), config.Database, config.Namespace, 0, options)
	if err != nil {
		owner.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Abort(); err != nil {
			t.Error(err)
		}
	})
	return store, owner
}

func openNativeTestAnchor(t *testing.T, store *Store, owner *nativelease.Database) *LeaseAnchor {
	t.Helper()
	anchor, err := OpenLeaseAnchor(LeaseAnchorConfig{
		Directory: filepath.Dir(owner.Path()), Name: "." + filepath.Base(owner.Path()) + ".leases",
		Identity: "sqlite-database-lease-recovery", BindingFD: owner.FD(),
		RecoveryStart: owner.Acquired(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Abort(); err != nil {
			t.Error(err)
		}
		if err := anchor.Close(); err != nil {
			t.Error(err)
		}
	})
	return anchor
}

func assertNoLeaseAuthority(t *testing.T, store *Store) {
	t.Helper()
	if store.LockService() != nil || store.leaseRecovery != nil {
		t.Fatal("rejected configuration attached lease authority or recovery")
	}
	var rows int
	if err := store.read.QueryRow(`SELECT count(*) FROM lease_recovery`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("rejected configuration persisted %d lease recovery records", rows)
	}
}

func TestBoundDurableLeaseOpenPreservesExclusiveOwnershipAndEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable-leases.db")
	owner, err := nativelease.AcquireDatabase(path, true, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	options := DefaultOptions()
	options.leaseOwner = owner
	witness := &checkpointStateWitness{}
	s, err := OpenBoundDurableLeaseWithOptions(t.Context(), path, "workspace", "objects", 0, options, CreateNamespaceIfMissing, DurableStartup{}, witness)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Abort(); err != nil {
			t.Error(err)
		}
	})
	anchor := openNativeTestAnchor(t, s, owner)
	if err := s.ConfigureLeaseRecovery(t.Context(), LeaseRecoveryConfig{Witness: anchor, RecoveryStart: owner.Acquired(), StateID: anchor.StateID(), Initialize: true}); err != nil {
		t.Fatal(err)
	}
	if err := anchor.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := s.EnableLocks(t.Context(), locking.DefaultOptions()); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "acknowledged"); err != nil {
		t.Fatal(err)
	}
	if err := s.RaiseMaxLease(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}
	if got, err := s.MaxLease(t.Context()); err != nil || got != time.Second {
		t.Fatalf("durable lease = %v, %v", got, err)
	}
	if state, present, err := anchor.Load(); err != nil || !present || state.MaxLease != time.Second {
		t.Fatalf("durable lease witness = %+v, %v, %v", state, present, err)
	}
	if raw, err := OpenBound(t.Context(), path, "workspace", "objects", 0, DefaultWindow()); raw != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("shared opener beside durable lease owner = %v, %v", raw, err)
	}
	if got, err := InspectDurableState(t.Context(), path); err != nil || got != witness.accepted {
		t.Fatalf("witnessed state = %+v, %v; accepted %+v", got, err, witness.accepted)
	}
}
