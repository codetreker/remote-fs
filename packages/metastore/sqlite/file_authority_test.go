package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func newFileAuthority(t *testing.T) (*Store, *fileSession) {
	t.Helper()
	s, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	native, _, err := s.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	session := native.(*fileSession)
	t.Cleanup(func() {
		if err := session.Dispose(context.Background()); err != nil {
			t.Error(err)
		}
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s.Store, session
}
func fileActionID(t *testing.T, s *fileSession) storage.FileActionID {
	t.Helper()
	status, err := s.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestFileActionReplayAndFingerprintPreserveSingleEffect(t *testing.T) {
	_, session := newFileAuthority(t)
	id := fileActionID(t, session)
	first, err := session.RetireRangeOwner(t.Context(), 0, id)
	if err != nil {
		t.Fatal(err)
	}
	second, err := session.RetireRangeOwner(t.Context(), 0, id)
	if err != nil {
		t.Fatal(err)
	}
	queried, err := session.QueryAction(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(session.actions) != 1 || first.State != storage.FileActionCompleted || first.RangeRevision != second.RangeRevision || first.RangeRevision != queried.RangeRevision {
		t.Fatalf("replay changed result: %+v %+v %+v", first, second, queried)
	}
	if _, err := session.RetireRangeOwner(t.Context(), 1, id); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed fingerprint=%v", err)
	}
	cancelled, err := session.CancelAction(t.Context(), id)
	if err != nil || cancelled.State != first.State {
		t.Fatalf("terminal cancellation=%+v %v", cancelled, err)
	}
}

func TestFileActionCurrentEpochCannotEvictReceipts(t *testing.T) {
	s, session := newFileAuthority(t)
	session.options.MaxActions = 1
	id := fileActionID(t, session)
	if _, err := session.RetireRangeOwner(t.Context(), 0, id); err != nil {
		t.Fatal(err)
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	session.actions[id].expires = time.Now().Add(-time.Second)
	session.rotates = time.Now().Add(time.Hour)
	s.coordinator.commit.release()
	if _, err := session.QueryAction(t.Context(), id); err != nil {
		t.Fatalf("current epoch lost receipt: %v", err)
	}
	next := fileActionID(t, session)
	if _, err := session.RetireRangeOwner(t.Context(), 1, next); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("history saturation=%v", err)
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	session.rotates = time.Now().Add(-time.Second)
	s.coordinator.commit.release()
	if _, err := session.QueryAction(t.Context(), id); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired history=%v", err)
	}
	if _, err := session.RetireRangeOwner(t.Context(), 0, id); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired epoch readmitted=%v", err)
	}
}

func TestFileSessionStaleTimerCannotUndoRenewal(t *testing.T) {
	s, session := newFileAuthority(t)
	before, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	after, err := session.Renew(t.Context())
	if err != nil || after.Revision <= before.Revision {
		t.Fatalf("renew=%+v %v", after, err)
	}
	session.expire()
	state, err := session.Status(t.Context())
	if err != nil || state.Retired {
		t.Fatalf("stale timer=%+v %v", state, err)
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	session.expires = time.Now().Add(-time.Second)
	s.coordinator.commit.release()
	session.expire()
	state, err = session.Status(t.Context())
	if err != nil || !state.Retired || !session.closed {
		t.Fatalf("expiry=%+v %v", state, err)
	}
	if len(s.fileDomain.sessions) != 0 {
		t.Fatal("empty expired session retained admission")
	}
	if _, err := session.Renew(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("renewed retired session=%v", err)
	}
}

func TestFileSessionCloseRetainsAccountedReceiptsUntilStoreShutdown(t *testing.T) {
	s, session := newFileAuthority(t)
	id := fileActionID(t, session)
	if _, err := session.Close(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := session.QueryAction(t.Context(), id); err != nil {
		t.Fatalf("closed receipt inaccessible: %v", err)
	}
	if len(s.fileDomain.sessions) != 1 {
		t.Fatal("closed history escaped authority accounting")
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.shutdownFileHistoryLocked(); err != nil {
		t.Fatal(err)
	}
	s.coordinator.commit.release()
	session.expire()
	if len(s.fileDomain.sessions) != 0 || !session.shutdown.Load() || len(session.actions) != 0 {
		t.Fatal("store shutdown retained history")
	}
	if _, err := session.QueryAction(t.Context(), id); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("shutdown history=%v", err)
	}
}

func TestFileSessionRestartFencesPersistedLease(t *testing.T) {
	config := lockingTestConfig(t)
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.Lease = 200 * time.Millisecond
	if _, _, err := s.NewFileSession(t.Context(), options); err != nil {
		t.Fatal(err)
	}
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	reopened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	persisted := reopened.fileLeaseRecovery.state.MaxLease
	original := reopened.fileDomain.recoveryUntil
	if persisted != options.Lease || reopened.fileLeaseRecovery.state.Quiescent || !original.Equal(reopened.fileLeaseRecovery.start.Add(persisted)) {
		t.Fatalf("lease watermark=%v deadline=%v start=%v quiescent=%v", persisted, original, reopened.fileLeaseRecovery.start, reopened.fileLeaseRecovery.state.Quiescent)
	}
	for _, phase := range []struct {
		delta time.Duration
		want  error
	}{{time.Hour, syscall.EAGAIN}, {-time.Hour, nil}} {
		if err := reopened.coordinator.commit.acquire(t.Context()); err != nil {
			t.Fatal(err)
		}
		reopened.fileDomain.recoveryUntil = time.Now().Add(phase.delta)
		err := reopened.checkFileAuthority(t.Context())
		reopened.coordinator.commit.release()
		if !errors.Is(err, phase.want) {
			t.Fatalf("recovery admission=%v want=%v", err, phase.want)
		}
	}
	if err := reopened.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened.fileDomain.recoveryUntil = original
	reopened.coordinator.commit.release()
}

func TestFileSessionUnknownActionCannotBeForgotten(t *testing.T) {
	s, session := newFileAuthority(t)
	id := fileActionID(t, session)
	if _, err := session.RetireRangeOwner(t.Context(), 0, id); err != nil {
		t.Fatal(err)
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	session.actions[id].result.State = storage.FileActionUnknown
	t.Cleanup(func() {
		if err := s.coordinator.commit.acquire(context.Background()); err != nil {
			t.Error(err)
			return
		}
		session.actions[id].result.State = storage.FileActionCompleted
		s.coordinator.commit.release()
	})
	session.actions[id].expires = time.Now().Add(-time.Hour)
	session.rotates = time.Now().Add(-time.Hour)
	s.coordinator.commit.release()
	result, err := session.QueryAction(t.Context(), id)
	if err != nil || result.State != storage.FileActionUnknown {
		t.Fatalf("unknown history lost=%+v %v", result, err)
	}
}

func TestFileFailedCloseRetainsDeadlineCleanupOwnership(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		name := "request-only"
		if persistent {
			name = "session-persistent"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			s, err := OpenLocking(ctx, lockingTestConfig(t))
			if err != nil {
				t.Fatal(err)
			}
			refused := errors.New("retirement accounting unavailable")
			refuse := func(int64, int64) (storage.PublicationSettlement, error) { return nil, refused }
			creation := ctx
			if persistent {
				creation = storage.WithPublicationAccounting(ctx, refuse)
			}
			native, _, err := s.NewFileSession(creation, storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			session := native.(*fileSession)
			t.Cleanup(func() {
				if err := session.Dispose(context.Background()); err != nil {
					if !persistent || !errors.Is(err, refused) {
						t.Error(err)
					}
					if err := s.Abort(); err != nil {
						t.Error(err)
					}
					return
				}
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := s.Create(ctx, "file"); err != nil {
				t.Fatal(err)
			}
			node, err := s.Stat(ctx, "file")
			if err != nil {
				t.Fatal(err)
			}
			retained, err := session.Retain(ctx, storage.RetainRequest{NodeID: uint64(node.ID), Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, fileActionID(t, session))
			if err != nil {
				t.Fatal(err)
			}
			nativeFile, _, err := session.Reference(ctx, retained.Reference)
			if err != nil {
				t.Fatal(err)
			}
			f := nativeFile.(*fileReference)
			observation, err := f.Stat(ctx, storage.ObservationOptions{IncludeLocation: true})
			if err != nil {
				t.Fatal(err)
			}
			location := *observation.Location
			entry := location.Ancestors[len(location.Ancestors)-1]
			_, err = f.PrepareRemoval(ctx, storage.PrepareRemovalRequest{ExpectedMetadataRevision: observation.Attr.MetadataRevision, Entry: entry, Witness: location, Condition: storage.RemovalFile}, fileActionID(t, session))
			if err != nil {
				t.Fatal(err)
			}
			closeID := fileActionID(t, session)
			closeAttempted, expired := make(chan struct{}), make(chan struct{})
			if err := s.coordinator.commit.acquire(ctx); err != nil {
				t.Fatal(err)
			}
			session.timer.Stop()
			session.expires = time.Now().Add(100 * time.Millisecond)
			session.timer = time.AfterFunc(time.Until(session.expires), func() { <-closeAttempted; session.expire(); close(expired) })
			s.coordinator.commit.release()
			receipt, err := session.Close(storage.WithPublicationAccounting(ctx, refuse), closeID)
			if !errors.Is(err, refused) || receipt.Effects&storage.EffectReferenceRetired == 0 {
				close(closeAttempted)
				t.Fatalf("early close=%+v %v", receipt, err)
			}
			if session.active || session.closed || s.coordinator.healthy() != nil {
				close(closeAttempted)
				t.Fatal("known refusal changed cleanup ownership")
			}
			close(closeAttempted)
			select {
			case <-expired:
			case <-time.After(3 * time.Second):
				t.Fatal("failed close lost its retirement deadline")
			}
			if persistent {
				if !errors.Is(s.coordinator.healthy(), refused) || session.closed || len(session.files) != 1 {
					t.Fatal("unresolved deadline fabricated cleanup or failed to fence")
				}
			} else {
				if !session.closed || len(session.files) != 0 {
					t.Fatal("deadline did not release references")
				}
				if _, err := s.Stat(ctx, "file"); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("prepared removal=%v", err)
				}
			}
		})
	}
}

func TestFilePendingContentCancellationSettlesBeforeLatePublication(t *testing.T) {
	s, session := newFileAuthority(t)
	id := fileActionID(t, session)
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := fileFingerprint("staged-content")
	if err != nil {
		t.Fatal(err)
	}
	action, fresh, err := session.admit(id, fingerprint, nil)
	if err != nil || !fresh {
		t.Fatalf("admit=%v %v", fresh, err)
	}
	action.result.Operation = storage.OpFileWrite
	s.coordinator.commit.release()
	result, err := session.CancelAction(t.Context(), id)
	if !errors.Is(err, syscall.EINTR) || result.State != storage.FileActionNotApplied || result.Effects != 0 {
		t.Fatalf("cancel pending content=%+v %v", result, err)
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	replay, fresh, err := session.admit(id, fingerprint, nil)
	if err != nil || fresh || replay.result.State != storage.FileActionNotApplied {
		t.Fatalf("late admission changed cancellation: %v %v", fresh, err)
	}
	s.coordinator.commit.release()
}

func TestFileCloseUsesReservedSlotAfterRetirementAndHistorySaturation(t *testing.T) {
	_, session := newFileAuthority(t)
	if _, err := session.RetireRangeOwner(t.Context(), 0, fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	session.options.MaxActions = 1
	closeID := fileActionID(t, session)
	if err := session.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	receipt, err := session.Close(t.Context(), closeID)
	if err != nil || !session.closed || receipt.State != storage.FileActionCompleted {
		t.Fatalf("terminal cleanup=%+v %v", receipt, err)
	}
	if len(session.actions) != 1 || len(session.cleanupActions) != 1 {
		t.Fatal("cleanup allocated ordinary history")
	}
	next, err := storage.NewFileActionID(session.actionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	retired, err := session.Close(t.Context(), next)
	if err != nil || retired.State != storage.FileActionRetired || retired.Action != "" || retired.Operation != storage.OpFileSessionClose || retired.Effects != 0 || retired.HistoryRemaining != 0 {
		t.Fatalf("terminal fact=%+v %v", retired, err)
	}
	if _, err := session.QueryAction(t.Context(), next); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("fabricated new close history=%v", err)
	}
}

func TestFileBeginCloseRejectsInvalidActionBeforeRetirement(t *testing.T) {
	_, session := newFileAuthority(t)
	used := fileActionID(t, session)
	if _, err := session.RetireRangeOwner(t.Context(), 0, used); err != nil {
		t.Fatal(err)
	}
	future, err := storage.NewFileActionID(session.actionEpoch + 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []storage.FileActionID{"bad", used, future} {
		_, admitted, err := session.BeginClose(t.Context(), id)
		if err == nil || admitted || !session.active || session.closeAction != nil {
			t.Fatalf("invalid close retired session: id=%s admitted=%v error=%v", id, admitted, err)
		}
	}
}

func TestFileCloseCancellationPreservesAdmittedCleanup(t *testing.T) {
	s, session := newFileAuthority(t)
	ctx := t.Context()
	if err := s.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	f := retainRangeFile(t, session, uint64(node.ID))
	id := fileActionID(t, session)
	began, proceed, err := session.BeginClose(ctx, id)
	if err != nil || !proceed || began.State != storage.FileActionPending || began.Effects&storage.EffectReferenceRetired == 0 || f.active {
		t.Fatalf("begin cleanup=%+v %v %v", began, proceed, err)
	}
	cancelled, err := session.CancelAction(ctx, id)
	if err != nil || cancelled.State != storage.FileActionPending || cancelled.Effects&storage.EffectReferenceRetired == 0 {
		t.Fatalf("cancel changed admitted cleanup=%+v %v", cancelled, err)
	}
	completed, err := session.Close(ctx, id)
	if err != nil || completed.State != storage.FileActionCompleted || !f.closed || !session.closed {
		t.Fatalf("original close could not finish=%+v %v", completed, err)
	}
}

func TestFileClosedReferenceRemainsCleanupOnly(t *testing.T) {
	s, session := newFileAuthority(t)
	ctx := t.Context()
	if err := s.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	retainID := fileActionID(t, session)
	request := storage.RetainRequest{NodeID: uint64(node.ID), Claim: storage.AccessClaim{Uses: storage.ReadContent}}
	retained, err := session.Retain(ctx, request, retainID)
	if err != nil {
		t.Fatal(err)
	}
	native, _, err := session.Reference(ctx, retained.Reference)
	if err != nil {
		t.Fatal(err)
	}
	f := native.(*fileReference)
	if _, err := f.Close(ctx, fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	resolved, live, err := session.Reference(ctx, retained.Reference)
	if err != nil || resolved != f || live {
		t.Fatalf("closed identity lookup=%v %v", resolved, err)
	}
	if _, err := resolved.Stat(ctx, storage.ObservationOptions{}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed reference admitted metadata=%v", err)
	}
	cleanupID := fileActionID(t, session)
	fact, err := resolved.Close(ctx, cleanupID)
	if err != nil || fact.State != storage.FileActionRetired || fact.Action != "" || fact.Operation != storage.OpFileClose || fact.Reference != retained.Reference || fact.Effects != 0 || fact.HistoryRemaining != 0 {
		t.Fatalf("closed identity fact=%+v %v", fact, err)
	}
	if _, err := session.QueryAction(ctx, cleanupID); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("unadmitted close acquired history=%v", err)
	}
	replay, err := session.Retain(ctx, request, retainID)
	if err != nil || replay.Reference != retained.Reference || s.fileDomain.files != 0 {
		t.Fatalf("retained replay reopened object=%+v %v", replay, err)
	}
}

func TestFileUnknownHistoryDoesNotPollForever(t *testing.T) {
	s, session := newFileAuthority(t)
	id := fileActionID(t, session)
	if _, err := session.RetireRangeOwner(t.Context(), 0, id); err != nil {
		t.Fatal(err)
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	session.actions[id].result.State = storage.FileActionUnknown
	t.Cleanup(func() {
		if err := s.coordinator.commit.acquire(context.Background()); err != nil {
			t.Error(err)
			return
		}
		session.actions[id].result.State = storage.FileActionCompleted
		s.coordinator.commit.release()
	})
	s.coordinator.commit.release()
	if err := session.Dispose(t.Context()); err != nil {
		t.Fatal(err)
	}
	if session.timer.Stop() {
		t.Fatal("unknown-only history left an expiry polling timer")
	}
	if len(session.actions) != 1 {
		t.Fatal("unknown action was forgotten")
	}
}

func TestFileRetiredHistoryCallbackCannotPoisonSharedAuthority(t *testing.T) {
	s, session := newFileAuthority(t)
	if err := session.Dispose(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !session.historyDone.Load() {
		t.Fatal("empty closed history remained scheduled")
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	session.cleanupTimeout = 10 * time.Millisecond
	done := make(chan struct{})
	go func() { session.expire(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		s.coordinator.commit.release()
		t.Fatal("queued history callback did not honor its wait bound")
	}
	s.coordinator.commit.release()
	if err := s.coordinator.healthy(); err != nil {
		t.Fatalf("retired history callback fenced a healthy authority: %v", err)
	}
}

func TestFileCloseRetiredEpochDoesNotReadmitUnknownAction(t *testing.T) {
	s, session := newFileAuthority(t)
	unused := fileActionID(t, session)
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	session.rotates = time.Now().Add(-time.Second)
	session.rotate()
	s.coordinator.commit.release()
	if err := session.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	result, err := session.Close(t.Context(), unused)
	if err != nil || result.State != storage.FileActionRetired || result.Action != "" || len(session.cleanupActions) != 0 || !session.closed {
		t.Fatalf("old unknown cleanup=%+v %v", result, err)
	}
	if _, err := session.QueryAction(t.Context(), unused); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired ID gained history=%v", err)
	}
}

func TestFileHistoryShutdownRequiresDeterminedActions(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		for _, state := range []storage.FileActionState{storage.FileActionPending, storage.FileActionUnknown} {
			s, session := newFileAuthority(t)
			id := fileActionID(t, session)
			if _, err := session.Close(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			if err := s.coordinator.commit.acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			records := session.actions
			if cleanup {
				records = session.cleanupActions
			}
			original := records[id]
			records[id] = &fileAction{result: storage.FileActionReceipt{State: state}}
			err := s.shutdownFileHistoryLocked()
			retained := records[id] != nil && !session.shutdown.Load() && len(s.fileDomain.sessions) == 1
			if original == nil {
				delete(records, id)
			} else {
				records[id] = original
			}
			s.coordinator.commit.release()
			if !errors.Is(err, syscall.EBUSY) || !retained {
				t.Fatalf("cleanup=%v state=%v shutdown=%v retained=%v", cleanup, state, err, retained)
			}
		}
	}
}
