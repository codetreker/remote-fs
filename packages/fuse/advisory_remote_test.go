package fuse

import (
	"context"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestAdvisorySnapshotRequiresRepresentableOwnedRanges(t *testing.T) {
	valid := storage.RangeSnapshot{Revision: 1, Own: []storage.RangeAcquisition{{ID: 1, Start: 0, End: math.MaxInt64}}}
	if err := checkRangeSnapshot(valid, flockFamily); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []storage.RangeSnapshot{
		{},
		{Revision: 1, Available: -1},
		{Revision: 1, Own: []storage.RangeAcquisition{{Start: 1, End: 2}}},
		{Revision: 1, Own: []storage.RangeAcquisition{{ID: 1, Start: 1, End: 1, Boundary: true}}},
		{Revision: 1, Own: []storage.RangeAcquisition{{ID: 1, End: math.MaxUint64}}},
		{Revision: 1, Own: []storage.RangeAcquisition{{ID: 1, End: 1}, {ID: 1, Start: 2, End: 3}}},
		{Revision: 1, Other: []storage.HeldRange{{Range: storage.RangeAcquisition{ID: 1}}}},
		{Revision: 1, Other: []storage.HeldRange{{Owner: storage.RangeOwner{Session: "remote"}, Range: storage.RangeAcquisition{ID: 1, End: math.MaxUint64}}}},
	} {
		if errnoOf(checkRangeSnapshot(invalid, posixFamily)) != syscall.EIO {
			t.Fatalf("accepted %#v", invalid)
		}
	}
	if errnoOf(checkRangeSnapshot(storage.RangeSnapshot{Revision: 1, Own: []storage.RangeAcquisition{{ID: 1, Start: 1, End: 2}}}, flockFamily)) != syscall.EIO {
		t.Fatal("accepted partial flock")
	}
	if advisoryScope(flockFamily) == advisoryScope(posixFamily) || advisoryScope(flockFamily).Enforced || advisoryScope(posixFamily).Enforced {
		t.Fatal("kernel lock domains overlap or enforce I/O")
	}
}

func TestAdvisoryConflictProjectionUsesOnlyActualIntersections(t *testing.T) {
	held := func(session string, owner uint64, start, end uint64, exclusive bool) storage.HeldRange {
		return storage.HeldRange{Owner: storage.RangeOwner{Session: session, ID: storage.RangeOwnerID(owner)}, Range: storage.RangeAcquisition{ID: 1, Start: start, End: end, Exclusive: exclusive}}
	}
	snapshot := storage.RangeSnapshot{Revision: 1, Other: []storage.HeldRange{
		held("z", 1, 10, 20, true), held("a", 4, 10, 30, true), held("a", 3, 10, 40, true), held("a", 1, 50, 60, true), held("a", 0, 0, 1, false),
	}}
	empty := held("a", 0, 0, 0, true)
	empty.Range.Boundary = true
	snapshot.Other = append(snapshot.Other, empty)
	got, found := conflictingRange(snapshot, fileLock{Start: 0, End: 20, Type: sharedType})
	if !found || got.Owner.Session != "a" || got.Owner.ID != 3 {
		t.Fatalf("conflict=%+v/%v", got, found)
	}
	got, found = conflictingRange(snapshot, fileLock{Start: 0, End: 20, Type: exclusiveType})
	if !found || got.Owner.ID != 0 || got.Range.Boundary {
		t.Fatalf("exclusive conflict=%+v/%v", got, found)
	}
	if _, found := conflictingRange(snapshot, fileLock{Start: 31, End: 49, Type: sharedType}); !found {
		t.Fatal("missed overlapping 10..40 lock")
	}
	if _, found := conflictingRange(snapshot, fileLock{Start: 41, End: 49, Type: exclusiveType}); found {
		t.Fatal("invented conflict in unlocked gap")
	}
}

func TestAdvisoryAcquisitionIDsRemainDistinctAcrossReplacement(t *testing.T) {
	v := &volume{sessionOptions: storage.DefaultFileSessionOptions()}
	old := storage.RangeSnapshot{Own: []storage.RangeAcquisition{{ID: 8, Start: 2, End: 4}, {ID: 9, Start: 5, End: 6, Exclusive: true}}}
	local := ownLocks(old, posixFamily)
	if local[0].Type != sharedType || local[1].Type != exclusiveType {
		t.Fatal("changed range types")
	}
	first, err := v.rangeAcquisitions(local)
	if err != nil {
		t.Fatal(err)
	}
	second, err := v.rangeAcquisitions(local)
	if err != nil {
		t.Fatal(err)
	}
	if first[0].ID == 0 || first[0].ID == first[1].ID || first[1].ID >= second[0].ID || !second[1].Exclusive {
		t.Fatalf("reused acquisition identities: %+v/%+v", first, second)
	}
	v.sessionOptions.MaxRanges = 1
	if _, err := v.rangeAcquisitions(local); errnoOf(err) != syscall.ENOLCK {
		t.Fatalf("oversized plan=%v", err)
	}
	v.sessionOptions.MaxRanges = 2
	v.nextRange = math.MaxUint64
	if _, err := v.rangeAcquisitions(local); errnoOf(err) != syscall.ENOLCK {
		t.Fatalf("identity exhaustion=%v", err)
	}
}

type rangeWaitSessionProbe struct {
	storage.FileSession
	status storage.FileSessionStatus
}

func (s rangeWaitSessionProbe) Status(context.Context) (storage.FileSessionStatus, error) {
	return s.status, nil
}

type rangeWaitFileProbe struct {
	storage.File
	failure syscall.Errno
}

func (f rangeWaitFileProbe) WaitRanges(ctx context.Context, _ storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	<-ctx.Done()
	err := ctx.Err()
	errno := syscall.EINTR
	if f.failure != 0 {
		err = f.failure
		errno = f.failure
	}
	return storage.FileActionReceipt{Action: id, Operation: storage.OpFileWaitRanges, State: storage.FileActionNotApplied, Errno: errno}, err
}

func TestAdvisoryBlockingWaitDistinguishesInternalBudgetFromFailure(t *testing.T) {
	for _, test := range []struct {
		name          string
		outerDeadline bool
		failure       syscall.Errno
		want          syscall.Errno
	}{
		{"internal wait budget", false, 0, 0},
		{"backend failure at deadline", false, syscall.EIO, syscall.EIO},
		{"caller deadline", true, 0, syscall.EIO},
		{"known denial wins caller deadline", true, syscall.EACCES, syscall.EACCES},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := storage.FileSessionStatus{Epoch: "session", Revision: 1, ActionEpoch: 1, Remaining: time.Minute, HistoryRemaining: time.Minute}
			v := &volume{status: status, flushTimeout: 25 * time.Millisecond, deadline: time.Now().Add(time.Minute), stop: make(chan struct{})}
			v.files = rangeWaitSessionProbe{status: status}
			h := newHandle(&node{volume: v, id: &identity{node: 1}}, rangeWaitFileProbe{failure: test.failure}, true, true)
			ctx := t.Context()
			if test.outerDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 25*time.Millisecond)
				defer cancel()
				v.flushTimeout = time.Second
			}
			if got := h.waitAdvisory(ctx, storage.RangeReplaceRequest{Owner: 1, Scope: advisoryScope(posixFamily), ExpectedRevision: 1}, true); got != test.want {
				t.Fatalf("wait=%v,want %v", got, test.want)
			}
			if v.fault != nil {
				t.Fatalf("known no-effect result fenced session: %v", v.fault)
			}
		})
	}
}
