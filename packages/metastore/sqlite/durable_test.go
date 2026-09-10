package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
	_ "modernc.org/sqlite"
)

type commitCancellationWitness struct {
	cancel context.CancelFunc
	fault  error
	calls  int
}

func (w *commitCancellationWitness) Accept(DurableState) error {
	w.calls++
	w.cancel()
	return w.fault
}

func (*commitCancellationWitness) Checkpoint(DurableState) error { return nil }

func TestCancellationAfterCommitPreservesTheDurabilityOutcome(t *testing.T) {
	for _, failAcceptance := range []bool{false, true} {
		t.Run(fmt.Sprintf("acceptance failed %t", failAcceptance), func(t *testing.T) {
			f := newPublicationFixture(t)
			f.put(t, t.Context(), "file", 3)
			before := f.node(t, "file")
			object := f.stage(t, t.Context(), "file", 7)
			owner := f.owner(t)
			grant := f.grant(t, owner, "file", locking.Exclusive)
			request, cancel := context.WithCancel(t.Context())
			defer cancel()
			witness := &commitCancellationWitness{cancel: cancel}
			if failAcceptance {
				witness.fault = errors.New("accepted state could not be persisted")
				*f.authorityFence = witness.fault
			}
			originalWitness := f.store.witness
			f.store.witness = witness
			defer func() { f.store.witness = originalWitness }()
			err := f.store.Commit(publicationScope(request, owner, grant), "file", object)
			if witness.calls != 1 || request.Err() != context.Canceled {
				t.Fatalf("commit reached acceptance %d times with request error %v", witness.calls, request.Err())
			}
			if failAcceptance {
				if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, witness.fault) {
					t.Errorf("failed acceptance after request cancellation = %v, want original durability failure", err)
				}
			} else if err != nil {
				t.Errorf("acknowledged commit became cancellation: %v", err)
			}
			status, statusErr := f.store.locks.Status(t.Context())
			if statusErr != nil || status.Unavailable != failAcceptance {
				t.Errorf("acceptance left authority status %+v, error %v", status, statusErr)
			}
			var size int64
			var content string
			if err := f.store.read.QueryRowContext(t.Context(),
				`SELECT size, content FROM nodes WHERE id = ?`, before.ID).Scan(&size, &content); err != nil ||
				size != object.Size || content != string(object.Key) {
				t.Errorf("physical COMMIT did not precede acceptance: size=%d content=%q error=%v", size, content, err)
			}
			_, readErr := f.store.Stat(t.Context(), "file")
			if failAcceptance {
				if storage.ErrnoOf(readErr) != syscall.EIO || !errors.Is(readErr, witness.fault) {
					t.Errorf("unconfirmed committed state was not fenced: %v", readErr)
				}
			} else if readErr != nil {
				t.Errorf("confirmed state became unavailable after cancellation: %v", readErr)
			}
		})
	}
}

func TestTerminalPoolCloseErrorKeepsDurableCoordinatorReserved(t *testing.T) {
	for _, target := range []string{"reader", "snapshot", "writer"} {
		t.Run(target, func(t *testing.T) {
			writer, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			reader, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				writer.Close()
				t.Fatal(err)
			}
			snapshot, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				writer.Close()
				reader.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				writer.Close()
				reader.Close()
				snapshot.Close()
			})
			path := t.TempDir() + "/reserved.db"
			coordinator, err := acquireCoordinator(path, true)
			if err != nil {
				t.Fatal(err)
			}
			failure := errors.New(target + " driver handle remained open")
			store := &Store{
				write: writer, read: reader, snapshotRead: snapshot,
				coordinator: coordinator,
				closePool: func(db *sql.DB) error {
					if target == "writer" && db == writer ||
						target == "reader" && db == reader ||
						target == "snapshot" && db == snapshot {
						return failure
					}
					return db.Close()
				},
			}
			if err := store.Close(); !errors.Is(err, failure) {
				t.Fatalf("terminal %s close returned %v", target, err)
			}
			if !store.Terminal() {
				t.Fatal("Store did not enter terminal state after pool close failure")
			}
			if err := store.Close(); !errors.Is(err, failure) {
				t.Fatalf("repeated terminal close returned %v", err)
			}
			if _, err := acquireCoordinator(path, true); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("durable reopen after %s close failure returned %v, want EBUSY", target, err)
			}
		})
	}
}

func TestDurableCloseRetriesPersistentWALReleaseFailure(t *testing.T) {
	path := t.TempDir() + "/metastore.db"
	store, err := OpenBoundDurableWithOptions(
		t.Context(), path, "workspace", "store", 0, DefaultOptions(),
		CreateVolumeIfMissing, DurableStartup{}, acceptingCommitWitness{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	releaseCalls := 0
	store.releasePersistentWAL = func(ctx context.Context, db *sql.DB) error {
		releaseCalls++
		if releaseCalls == 1 {
			return fmt.Errorf("injected persistent WAL release failure: %w", syscall.EIO)
		}
		return disablePersistentWAL(ctx, db)
	}
	if err := store.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("closing with failed persistent WAL release returned %v, want EIO", err)
	}
	if store.Terminal() {
		t.Fatal("persistent WAL release failure made Close terminal")
	}
	if err := store.write.PingContext(t.Context()); err != nil {
		t.Fatalf("persistent WAL release failure closed the writer: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("retrying persistent WAL release: %v", err)
	}
	if releaseCalls != 2 {
		t.Fatalf("persistent WAL release ran %d times, want 2", releaseCalls)
	}
}

func TestCanceledCommitAndPoisonRemainDurabilityFailures(t *testing.T) {
	uncertain := sqlerr.NewUncertainCommit(context.Canceled)
	coordinator := new(databaseCoordinator)
	coordinator.poisonWith(uncertain)
	for _, err := range []error{uncertain, coordinator.healthy()} {
		if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, context.Canceled) {
			t.Fatalf("uncertain durability = %v (%v), want EIO retaining cancellation", err, storage.ErrnoOf(err))
		}
		for _, joined := range []error{
			errors.Join(err, context.Canceled),
			errors.Join(context.Canceled, err),
		} {
			if storage.ErrnoOf(joined) != syscall.EIO {
				t.Fatalf("joined cancellation displaced durability failure: %v", joined)
			}
		}
	}
}

func TestCoordinatorPoisonPreservesTheFirstDurabilityFailureAsEIO(t *testing.T) {
	coordinator := new(databaseCoordinator)
	coordinator.poisonWith(syscall.EEXIST)
	first := coordinator.healthy()
	if !errors.Is(first, syscall.EIO) || !errors.Is(first, syscall.EEXIST) {
		t.Fatalf("poisoned coordinator returned %v, want EIO retaining EEXIST", first)
	}
	coordinator.poisonWith(syscall.ENOSPC)
	second := coordinator.healthy()
	if !errors.Is(second, syscall.EEXIST) || errors.Is(second, syscall.ENOSPC) {
		t.Fatalf("second poison replaced the first durability failure: %v", second)
	}
}

type checkpointStateWitness struct {
	accepted, checkpointed DurableState
	checkpointErr          error
}

func (w *checkpointStateWitness) Accept(state DurableState) error { w.accepted = state; return nil }
func (w *checkpointStateWitness) Checkpoint(state DurableState) error {
	if w.checkpointErr != nil {
		return w.checkpointErr
	}
	w.checkpointed = state
	return nil
}

func TestDurableInspectionAndCheckpointPublishOnlyAcceptedState(t *testing.T) {
	path := t.TempDir() + "/durable.db"
	witness := &checkpointStateWitness{}
	s, err := OpenBoundDurableWithOptions(t.Context(), path, "workspace", "objects", 0, DefaultOptions(), CreateVolumeIfMissing, DurableStartup{}, witness)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		witness.checkpointErr = nil
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Create(t.Context(), "persisted"); err != nil {
		t.Fatal(err)
	}
	state, err := InspectDurableState(t.Context(), path)
	if err != nil || state != witness.accepted {
		t.Fatalf("inspected state = %+v, %v; accepted %+v", state, err, witness.accepted)
	}
	if _, err := s.Checkpoint(t.Context(), CheckpointMode(99)); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("unknown checkpoint mode = %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Checkpoint(canceled, FullCheckpoint); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled checkpoint = %v", err)
	}
	witness.checkpointErr = errors.New("checkpoint witness offline")
	if _, err := s.Checkpoint(t.Context(), FullCheckpoint); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unconfirmed checkpoint = %v", err)
	}
	if _, err := s.Stat(t.Context(), "persisted"); err != nil {
		t.Fatalf("checkpoint failure poisoned accepted data: %v", err)
	}
	witness.checkpointErr = nil
	for _, mode := range []CheckpointMode{PassiveCheckpoint, FullCheckpoint} {
		result, err := s.Checkpoint(t.Context(), mode)
		if err != nil || !result.Complete || result.State != state || witness.checkpointed != state || result.LogFrames != result.CheckpointedFrames {
			t.Fatalf("checkpoint %d = %+v, %v; witnessed %+v", mode, result, err, witness.checkpointed)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Checkpoint(t.Context(), FullCheckpoint); !errors.Is(err, syscall.EIO) {
		t.Fatalf("checkpoint closed store = %v", err)
	}
	if after, err := InspectDurableState(t.Context(), path); err != nil || after != state {
		t.Fatalf("closed durable state = %+v, %v", after, err)
	}
	if _, err := InspectDurableState(t.Context(), path+".missing"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("missing database inspection = %v", err)
	}
	if _, err := InspectDurableState(canceled, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled inspection = %v", err)
	}
}
