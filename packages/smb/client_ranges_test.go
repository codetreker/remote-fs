package smb

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestWindowsRangePlanPreservesSequentialEffects(t *testing.T) {
	shared := windowsLockRange{Offset: 10, Length: 5, Type: lockShared, FailImmediately: true}
	exclusive := windowsLockRange{Offset: 10, Length: 5, Type: lockExclusive, FailImmediately: true}
	unlock := windowsLockRange{Offset: 10, Length: 5, Type: lockUnlock}
	held := storage.RangeAcquisition{ID: 1, Start: 10, End: 14}
	for _, tt := range []struct {
		name     string
		own      []storage.RangeAcquisition
		other    []storage.HeldRange
		batch    []windowsLockRange
		capacity int
		want     int
		applied  int
		err      error
		failure  windowsFailure
	}{
		{name: "lock then unlock flag", batch: []windowsLockRange{shared, unlock}, capacity: 4, want: 1, applied: 1, err: syscall.EINVAL},
		{name: "lock then invalid flags", batch: []windowsLockRange{shared, {Type: lockInvalid}}, capacity: 4, want: 1, applied: 1, err: syscall.EINVAL},
		{name: "unlock then lock flag", own: []storage.RangeAcquisition{held}, batch: []windowsLockRange{unlock, shared}, capacity: 4, want: 0, applied: 1, err: syscall.EINVAL},
		{name: "unlock missing stops", own: []storage.RangeAcquisition{held}, batch: []windowsLockRange{unlock, unlock}, capacity: 4, want: 0, applied: 1, err: syscall.ENOLCK, failure: windowsRangeNotLocked},
		{name: "conflict rolls back", batch: []windowsLockRange{{Offset: 1, Length: 1, Type: lockShared, FailImmediately: true}, exclusive}, other: []storage.HeldRange{{Range: held}}, capacity: 4, want: 0, err: syscall.EAGAIN, failure: windowsLockConflict},
		{name: "capacity preserves prefix", batch: []windowsLockRange{shared, shared}, capacity: 1, want: 1, applied: 1, err: syscall.ENOLCK},
		{name: "duplicates retain acquisitions", batch: []windowsLockRange{shared, shared}, capacity: 4, want: 2, applied: 2},
		{name: "exclusive own overlap", own: []storage.RangeAcquisition{held}, batch: []windowsLockRange{exclusive}, capacity: 4, want: 1, err: syscall.EAGAIN, failure: windowsLockConflict},
		{name: "unlock immediate ignored", own: []storage.RangeAcquisition{held}, batch: []windowsLockRange{{Offset: 10, Length: 5, Type: lockUnlock, FailImmediately: true}}, capacity: 4, want: 0, applied: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			next := storage.RangeAcquisitionID(10)
			p := planWindowsRanges(storage.RangeSnapshot{Revision: 1, Own: tt.own, Other: tt.other, Available: tt.capacity, OwnerAvailable: tt.capacity}, windowsLockBatch{Ranges: tt.batch}, &next)
			if len(p.ranges) != tt.want || p.applied != tt.applied || !errors.Is(p.err, tt.err) || windowsFailureOf(p.err) != tt.failure {
				t.Fatalf("plan=%+v error=%v", p, p.err)
			}
			if len(p.ranges) > 1 && p.ranges[0].ID == p.ranges[1].ID {
				t.Fatal("acquisitions deduplicated")
			}
		})
	}
}

func TestWindowsRangeExactUnlockPrefersExclusive(t *testing.T) {
	own := []storage.RangeAcquisition{{ID: 1, Start: 4, End: 6}, {ID: 2, Start: 4, End: 6, Exclusive: true}, {ID: 3, Start: 4, End: 6}}
	next := storage.RangeAcquisitionID(3)
	p := planWindowsRanges(storage.RangeSnapshot{Revision: 1, Own: own, Available: 4, OwnerAvailable: 4}, windowsLockBatch{Ranges: []windowsLockRange{{Offset: 4, Length: 3, Type: lockUnlock}}}, &next)
	if p.err != nil || len(p.ranges) != 2 || p.ranges[0].ID != 1 || p.ranges[1].ID != 3 || len(own) != 3 || own[1].ID != 2 {
		t.Fatalf("plan=%+v original=%+v", p, own)
	}
}

func TestWindowsRangeBoundaryAndOverflow(t *testing.T) {
	for _, tt := range []struct {
		a, b    storage.RangeAcquisition
		overlap bool
	}{
		{storage.RangeAcquisition{Boundary: true, Start: 0, End: 0}, storage.RangeAcquisition{Start: 0, End: 9}, false},
		{storage.RangeAcquisition{Boundary: true, Start: 4, End: 4}, storage.RangeAcquisition{Start: 4, End: 9}, false},
		{storage.RangeAcquisition{Boundary: true, Start: 4, End: 4}, storage.RangeAcquisition{Start: 1, End: 4}, true},
		{storage.RangeAcquisition{Boundary: true, Start: 4, End: 4}, storage.RangeAcquisition{Boundary: true, Start: 4, End: 4}, false},
		{storage.RangeAcquisition{Start: 3, End: 4}, storage.RangeAcquisition{Start: 4, End: 5}, true},
	} {
		if rangesOverlap(tt.a, tt.b) != tt.overlap || rangesOverlap(tt.b, tt.a) != tt.overlap {
			t.Fatalf("overlap %+v %+v", tt.a, tt.b)
		}
	}
	for _, r := range []windowsLockRange{{Offset: ^uint64(0), Length: 1, Type: lockShared}, {Offset: ^uint64(0), Length: 0, Type: lockExclusive}, {Offset: 0, Length: ^uint64(0), Type: lockUnlock}} {
		if err := r.Check(); err != nil {
			t.Fatal(r, err)
		}
	}
	if err := (windowsLockRange{Offset: ^uint64(0), Length: 2, Type: lockShared}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	b := rangeRegion(windowsLockRange{Offset: 9, Type: lockShared})
	if !b.Boundary || b.Start != 9 || b.End != 9 {
		t.Fatal(b)
	}
}

type rangeTestFile struct {
	storage.File
	snapshot func(context.Context) (storage.RangeSnapshot, error)
	replace  func(context.Context, storage.RangeReplaceRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	wait     func(context.Context, storage.RangeWaitRequest, storage.FileActionID) (storage.FileActionReceipt, error)
}

func (f *rangeTestFile) Reference() storage.FileReferenceID { return 1 }
func (f *rangeTestFile) NodeID() uint64                     { return 2 }
func (f *rangeTestFile) RangeSnapshot(ctx context.Context, _ storage.RangeOwnerID, _ storage.RangeScope) (storage.RangeSnapshot, error) {
	return f.snapshot(ctx)
}
func (f *rangeTestFile) ReplaceRanges(ctx context.Context, r storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.replace(ctx, r, id)
}
func (f *rangeTestFile) WaitRanges(ctx context.Context, r storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.wait(ctx, r, id)
}

type rangeTestSession struct {
	storage.FileSession
	query  func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
	cancel func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
}

func (s *rangeTestSession) Status(context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{ActionEpoch: 1}, nil
}
func (s *rangeTestSession) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.query(ctx, id)
}
func (s *rangeTestSession) CancelAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.cancel(ctx, id)
}
func rangeClient(t *testing.T, raw *rangeTestFile, session *rangeTestSession) (*clientFile, storage.FileActionID) {
	t.Helper()
	b := &clientBackend{limits: testConfig().Limits, authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })}
	s := &clientSession{backend: b, raw: session, options: storage.DefaultFileSessionOptions(), actions: make(map[windowsActionID]*clientAction)}
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	return &clientFile{raw: raw, session: s, access: windowsReadData | windowsWriteData, owner: 1}, id
}
func rangeReceipt(id storage.FileActionID) storage.FileActionReceipt {
	return storage.FileActionReceipt{Action: id, Operation: storage.OpFileReplaceRanges, State: storage.FileActionCompleted, Effects: storage.EffectRangesChanged, HistoryRemaining: time.Minute}
}

func TestClientRangeCASConfirmsPartialErrorAndUnknownEffect(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "reply", true: "lost reply"}[lost], func(t *testing.T) {
			calls, snapshots := 0, 0
			var actual storage.FileActionID
			raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
				snapshots++
				return storage.RangeSnapshot{Revision: 3, Available: 10, OwnerAvailable: 10}, nil
			}}
			raw.replace = func(_ context.Context, r storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				calls++
				actual = id
				if len(r.Ranges) != 1 || r.ExpectedRevision != 3 {
					t.Fatal(r)
				}
				if lost {
					return storage.FileActionReceipt{}, syscall.EIO
				}
				return rangeReceipt(id), nil
			}
			session := &rangeTestSession{query: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
				if id != actual {
					t.Fatal("queried replacement action")
				}
				return rangeReceipt(id), nil
			}}
			file, id := rangeClient(t, raw, session)
			batch := windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 3, Type: lockShared, FailImmediately: true}, {Type: lockUnlock}}}
			result, err := file.LockBatch(t.Context(), batch, id)
			if !errors.Is(err, syscall.EINVAL) || result.Applied != 1 || result.Action != id || result.State != windowsActionCompleted || calls != 1 || snapshots != 1 {
				t.Fatalf("result=%+v err=%v calls=%d snapshots=%d", result, err, calls, snapshots)
			}
			for _, reconcile := range []func(context.Context, windowsActionID) (windowsActionResult, error){file.session.QueryAction, file.session.CancelAction} {
				repeated, repeatErr := reconcile(t.Context(), id)
				if !errors.Is(repeatErr, syscall.EINVAL) || repeated.Applied != 1 || calls != 1 || snapshots != 1 {
					t.Fatalf("terminal sidecar replay: %+v %v", repeated, repeatErr)
				}
			}
		})
	}
}

func TestClientRangeRevisionConflictReplansUnderFreshAction(t *testing.T) {
	calls, snapshots := 0, 0
	var ids []storage.FileActionID
	raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
		snapshots++
		return storage.RangeSnapshot{Revision: uint64(snapshots), Available: 10, OwnerAvailable: 10}, nil
	}}
	raw.replace = func(_ context.Context, r storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		ids = append(ids, id)
		if calls == 1 {
			return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision}}, syscall.EAGAIN
		}
		return rangeReceipt(id), nil
	}
	file, id := rangeClient(t, raw, &rangeTestSession{})
	result, err := file.LockBatch(t.Context(), windowsLockBatch{Ranges: []windowsLockRange{{Offset: 0, Length: 1, Type: lockShared, FailImmediately: true}}}, id)
	if err != nil || result.Action != id || calls != 2 || snapshots != 2 || ids[0] == ids[1] {
		t.Fatalf("result=%+v err=%v ids=%v", result, err, ids)
	}
}

func TestClientRangeWaitNeverReportsGrant(t *testing.T) {
	snapshots, waits, replaces, pending := 0, 0, 0, 0
	raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
		snapshots++
		s := storage.RangeSnapshot{Revision: uint64(snapshots), Available: 10, OwnerAvailable: 10}
		if snapshots <= 12 {
			s.Other = []storage.HeldRange{{Range: storage.RangeAcquisition{ID: 9, Start: 0, End: 9, Exclusive: true}}}
		}
		return s, nil
	}}
	raw.wait = func(_ context.Context, r storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		waits++
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileWaitRanges, State: storage.FileActionCompleted}, nil
	}
	raw.replace = func(_ context.Context, r storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		replaces++
		if snapshots != 13 || len(r.Ranges) != 1 {
			t.Fatal("wait treated as grant", snapshots, r)
		}
		return rangeReceipt(id), nil
	}
	file, id := rangeClient(t, raw, &rangeTestSession{})
	ctx := context.WithValue(t.Context(), rangePendingKey{}, func() error { pending++; return nil })
	result, err := file.LockBatch(ctx, windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive}}}, id)
	if err != nil || result.Applied != 1 || waits != 12 || replaces != 1 || pending != 1 {
		t.Fatalf("result=%+v err=%v waits=%d replaces=%d pending=%d", result, err, waits, replaces, pending)
	}
}

func TestClientRangePendingWaitCancelledBeforeNewSnapshot(t *testing.T) {
	var events []string
	raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
		events = append(events, "snapshot")
		s := storage.RangeSnapshot{Revision: 1, Available: 10, OwnerAvailable: 10}
		if len(events) == 1 {
			s.Other = []storage.HeldRange{{Range: storage.RangeAcquisition{ID: 1, Start: 0, End: 9, Exclusive: true}}}
		}
		return s, nil
	}}
	raw.wait = func(_ context.Context, _ storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		events = append(events, "wait")
		return storage.FileActionReceipt{}, syscall.EIO
	}
	raw.replace = func(_ context.Context, _ storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		events = append(events, "replace")
		return rangeReceipt(id), nil
	}
	session := &rangeTestSession{query: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		events = append(events, "query")
		return storage.FileActionReceipt{Action: id, State: storage.FileActionPending}, nil
	}, cancel: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		events = append(events, "cancel")
		return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EINTR}, syscall.EINTR
	}}
	file, id := rangeClient(t, raw, session)
	result, err := file.LockBatch(t.Context(), windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive}}}, id)
	if err != nil || result.Applied != 1 || !reflect.DeepEqual(events, []string{"snapshot", "wait", "query", "cancel", "snapshot", "replace"}) {
		t.Fatalf("result=%+v err=%v events=%v", result, err, events)
	}
}

func TestClientRangeUnknownCASDoesNotReplan(t *testing.T) {
	snapshots, replaces, queries := 0, 0, 0
	raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
		snapshots++
		return storage.RangeSnapshot{Revision: 1, Available: 10, OwnerAvailable: 10}, nil
	}, replace: func(_ context.Context, _ storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		replaces++
		return storage.FileActionReceipt{}, syscall.EIO
	}}
	session := &rangeTestSession{query: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		queries++
		return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown}, syscall.EIO
	}}
	file, id := rangeClient(t, raw, session)
	_, err := file.LockBatch(t.Context(), windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive, FailImmediately: true}}}, id)
	if !errors.Is(err, syscall.EIO) || snapshots != 1 || replaces != 1 || queries != 1 {
		t.Fatalf("err=%v snapshots=%d replaces=%d queries=%d", err, snapshots, replaces, queries)
	}
	_, err = file.session.QueryAction(t.Context(), id)
	if !errors.Is(err, syscall.EIO) || snapshots != 1 || replaces != 1 {
		t.Fatal("unknown outcome allowed new plan")
	}
}

func TestClientRangeCancellationWaitAndCommittedGrant(t *testing.T) {
	for _, wait := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed grant", true: "revision wait"}[wait], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
				s := storage.RangeSnapshot{Revision: 1, Available: 2, OwnerAvailable: 2}
				if wait {
					s.Other = []storage.HeldRange{{Range: storage.RangeAcquisition{ID: 1, Start: 0, End: 9, Exclusive: true}}}
				}
				return s, nil
			}}
			raw.replace = func(_ context.Context, _ storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				cancel()
				return storage.FileActionReceipt{}, syscall.EINTR
			}
			raw.wait = func(_ context.Context, _ storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				cancel()
				return storage.FileActionReceipt{}, syscall.EINTR
			}
			session := &rangeTestSession{query: func(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
				if ctx.Err() != nil {
					t.Fatal("cleanup context already cancelled")
				}
				if wait {
					return storage.FileActionReceipt{Action: id, State: storage.FileActionPending}, nil
				}
				return rangeReceipt(id), nil
			}, cancel: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EINTR}, syscall.EINTR
			}}
			file, id := rangeClient(t, raw, session)
			result, err := file.LockBatch(ctx, windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive}}}, id)
			if wait {
				if result.State != windowsActionCancelled || !errors.Is(err, syscall.EINTR) || result.Applied != 0 {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			} else if err != nil || result.State != windowsActionCompleted || result.Applied != 1 {
				t.Fatalf("grant lost to local cancellation: %+v %v", result, err)
			}
		})
	}
}

func TestClientRangeConflictStillCommitsUnchangedSet(t *testing.T) {
	calls := 0
	own := []storage.RangeAcquisition{{ID: 2, Start: 10, End: 19}}
	raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
		return storage.RangeSnapshot{Revision: 7, Available: 2, OwnerAvailable: 2, Own: own, Other: []storage.HeldRange{{Range: storage.RangeAcquisition{ID: 3, Start: 0, End: 9, Exclusive: true}}}}, nil
	}, replace: func(_ context.Context, r storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		if r.ExpectedRevision != 7 || !reflect.DeepEqual(r.Ranges, own) {
			t.Fatal(r)
		}
		return rangeReceipt(id), nil
	}}
	file, id := rangeClient(t, raw, &rangeTestSession{})
	result, err := file.LockBatch(t.Context(), windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive, FailImmediately: true}}}, id)
	if calls != 1 || result.Applied != 0 || windowsFailureOf(err) != windowsLockConflict {
		t.Fatalf("calls=%d result=%+v err=%v", calls, result, err)
	}
}

func TestClientRangeCASContentionWaitsOnRejectedGuard(t *testing.T) {
	snapshots, cas, waits := 0, 0, 0
	conflict := func(id storage.FileActionID) storage.FileActionReceipt {
		return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision}}
	}
	raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
		snapshots++
		return storage.RangeSnapshot{Revision: uint64(snapshots), Available: 2, OwnerAvailable: 2}, nil
	}, replace: func(_ context.Context, _ storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		cas++
		if cas <= 8 {
			return conflict(id), syscall.EAGAIN
		}
		return rangeReceipt(id), nil
	}, wait: func(_ context.Context, r storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		waits++
		if r.ExpectedRevision != 8 || snapshots != 8 {
			t.Fatalf("waiting on new guard: snapshot=%d request=%+v", snapshots, r)
		}
		return conflict(id), syscall.EAGAIN
	}}
	file, id := rangeClient(t, raw, &rangeTestSession{})
	result, err := file.LockBatch(t.Context(), windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive}}}, id)
	if err != nil || result.Applied != 1 || waits != 1 || cas != 9 || snapshots != 9 {
		t.Fatalf("result=%+v err=%v waits=%d cas=%d snapshots=%d", result, err, waits, cas, snapshots)
	}
}

func TestClientRangeRetiredReceiptCannotGrant(t *testing.T) {
	raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
		return storage.RangeSnapshot{Revision: 1, Available: 2, OwnerAvailable: 2}, nil
	}, replace: func(_ context.Context, _ storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, State: storage.FileActionRetired}, nil
	}}
	file, id := rangeClient(t, raw, &rangeTestSession{})
	result, err := file.LockBatch(t.Context(), windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive, FailImmediately: true}}}, id)
	if !errors.Is(err, syscall.ESTALE) || result.State != windowsActionRejected || result.Applied != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestClientRangeExplicitCancellationReconcilesUnknownCAS(t *testing.T) {
	queries, cancels := 0, 0
	raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
		return storage.RangeSnapshot{Revision: 1, Available: 2, OwnerAvailable: 2}, nil
	}, replace: func(context.Context, storage.RangeReplaceRequest, storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{}, syscall.EIO
	}}
	session := &rangeTestSession{query: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		queries++
		if queries == 1 {
			return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown}, syscall.EIO
		}
		return rangeReceipt(id), nil
	}, cancel: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		cancels++
		return storage.FileActionReceipt{Action: id, State: storage.FileActionPending}, nil
	}}
	file, id := rangeClient(t, raw, session)
	_, err := file.LockBatch(t.Context(), windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive, FailImmediately: true}}}, id)
	if !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	result, err := file.session.CancelAction(t.Context(), id)
	if err != nil || result.State != windowsActionCompleted || result.Applied != 1 || queries != 2 || cancels != 1 {
		t.Fatalf("result=%+v err=%v queries=%d cancels=%d", result, err, queries, cancels)
	}
}

func TestClientRangeHistoryReleasesGraphAndExpiresCachedReceipt(t *testing.T) {
	queries := 0
	raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
		return storage.RangeSnapshot{Revision: 1, Available: 2, OwnerAvailable: 2}, nil
	}, replace: func(_ context.Context, _ storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		r := rangeReceipt(id)
		r.Observation = storage.FileObservation{Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Metadata: storage.Metadata{{Key: "business", Version: 1, Data: make([]byte, 4096)}}}}
		return r, nil
	}}
	session := &rangeTestSession{query: func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		queries++
		return storage.FileActionReceipt{Action: id, State: storage.FileActionRetired}, syscall.ESTALE
	}}
	file, id := rangeClient(t, raw, session)
	if _, err := file.LockBatch(t.Context(), windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive, FailImmediately: true}}}, id); err != nil {
		t.Fatal(err)
	}
	a := file.session.action(id).rangeAction
	state := a.retention()
	if !state.terminal || state.current != id || state.expires.IsZero() || a.file != nil || a.charge != 0 || a.result.Receipt.Observation.Attr.Metadata != nil || a.batch.Ranges != nil || a.plan.ranges != nil {
		t.Fatalf("terminal retains plan or file: %+v history=%+v", a, state)
	}
	a.expires = time.Now().Add(time.Second)
	result, err := file.session.QueryAction(t.Context(), id)
	if err != nil || result.HistoryRemaining <= 0 || result.HistoryRemaining > time.Second || queries != 0 {
		t.Fatal(result, err, queries)
	}
	a.expires = time.Now().Add(-time.Second)
	result, err = file.session.QueryAction(t.Context(), id)
	if !errors.Is(err, syscall.ESTALE) || result.State != windowsActionRejected || queries != 1 {
		t.Fatalf("expired cached success: %+v %v queries=%d", result, err, queries)
	}
}

func TestClientRangeLocalAdmissionFailureReleasesPlanAndSidecar(t *testing.T) {
	for _, phase := range []string{"batch bytes", "snapshot plan bytes", "action count"} {
		t.Run(phase, func(t *testing.T) {
			snapshots := 0
			raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
				snapshots++
				return storage.RangeSnapshot{Revision: 1, Available: 2, OwnerAvailable: 2}, nil
			}}
			file, id := rangeClient(t, raw, &rangeTestSession{})
			switch phase {
			case "batch bytes":
				file.session.backend.limits.MaxDirectoryBytes = 1
			case "snapshot plan bytes":
				file.session.backend.limits.MaxDirectoryBytes = 64
			case "action count":
				file.session.options.MaxActions = 0
			}
			_, err := file.LockBatch(t.Context(), windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive, FailImmediately: true}}}, id)
			if !errors.Is(err, syscall.ENOMEM) || file.session.planBytes != 0 || len(file.session.actions) != 0 {
				t.Fatalf("err=%v bytes=%d actions=%d", err, file.session.planBytes, len(file.session.actions))
			}
			if phase == "snapshot plan bytes" && snapshots != 1 {
				t.Fatal(snapshots)
			}
		})
	}
}

func TestClientRangeAdmissionProofCannotResolveEarlierUnknown(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "current rejection", true: "unknown then denied query"}[unknown], func(t *testing.T) {
			queries := 0
			raw := &rangeTestFile{snapshot: func(context.Context) (storage.RangeSnapshot, error) {
				return storage.RangeSnapshot{Revision: 1, Available: 2, OwnerAvailable: 2}, nil
			}, replace: func(_ context.Context, r storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				if unknown {
					return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown}, syscall.EIO
				}
				return storage.FileActionReceipt{}, &storage.FileError{Code: syscall.EIO, NotAdmitted: true}
			}}
			session := &rangeTestSession{query: func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
				queries++
				return storage.FileActionReceipt{}, &storage.FileError{Code: syscall.EACCES, NotAdmitted: true}
			}}
			file, id := rangeClient(t, raw, session)
			result, err := file.LockBatch(t.Context(), windowsLockBatch{Ranges: []windowsLockRange{{Offset: 1, Length: 2, Type: lockExclusive, FailImmediately: true}}}, id)
			if !errors.Is(err, syscall.EIO) {
				t.Fatal(result, err)
			}
			if unknown {
				if queries != 1 || result.notAdmitted || file.session.planBytes == 0 || len(file.session.actions) != 1 {
					t.Fatal(result, queries, file.session.planBytes, len(file.session.actions))
				}
			} else if queries != 0 || !result.notAdmitted || file.session.planBytes != 0 || len(file.session.actions) != 0 {
				t.Fatal(result, queries, file.session.planBytes, len(file.session.actions))
			}
		})
	}
}

func TestClientRangeCapacityIncludesAlreadyOwnedSet(t *testing.T) {
	own := storage.RangeAcquisition{ID: 1, Start: 0, End: 0}
	for _, ownerLimit := range []bool{false, true} {
		t.Run(map[bool]string{false: "resource", true: "owner"}[ownerLimit], func(t *testing.T) {
			snapshot := storage.RangeSnapshot{Revision: 1, Own: []storage.RangeAcquisition{own}, Available: 2, OwnerAvailable: 3}
			if ownerLimit {
				snapshot.Available, snapshot.OwnerAvailable = 3, 2
			}
			next := storage.RangeAcquisitionID(1)
			plan := planWindowsRanges(snapshot, windowsLockBatch{Ranges: []windowsLockRange{{Offset: 2, Length: 1, Type: lockShared, FailImmediately: true}, {Offset: 4, Length: 1, Type: lockShared, FailImmediately: true}}}, &next)
			if !errors.Is(plan.err, syscall.ENOLCK) || len(plan.ranges) != 2 || plan.ranges[0] != own || plan.applied != 1 {
				t.Fatal(plan)
			}
			snapshot.Available, snapshot.OwnerAvailable = 1, 1
			next = 1
			plan = planWindowsRanges(snapshot, windowsLockBatch{Ranges: []windowsLockRange{{Offset: 2, Length: 1, Type: lockShared, FailImmediately: true}}}, &next)
			if !errors.Is(plan.err, syscall.ENOLCK) || len(plan.ranges) != 1 || plan.applied != 0 {
				t.Fatal(plan)
			}
		})
	}
}
