package windowsaccess

import (
	"errors"
	"testing"
)

func engine(t *testing.T) *Engine {
	t.Helper()
	e, err := New(Limits{MaxOpens: 8, MaxRanges: 16, MaxBatchEntries: 8})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func open(t *testing.T, e *Engine, handle, node uint64, access, share Access) {
	t.Helper()
	if err := e.Open(handle, node, access, share); err != nil {
		t.Fatal(err)
	}
}

func wantError(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v, want %v", got, want)
	}
}

func TestDeletePendingBelongsToEachOpenAndSurvivesMarkedClose(t *testing.T) {
	e := engine(t)
	open(t, e, 1, 9, ShareAll, ShareAll)
	open(t, e, 2, 9, Delete, ShareAll)
	if _, err := e.LockBatch(1, []LockRequest{{Range: Range{Offset: 0, Length: 2}, Flags: Shared | FailImmediately}}); err != nil {
		t.Fatal(err)
	}
	if e.RangeCount(1) != 1 {
		t.Fatal("owned range missing")
	}
	wantError(t, e.SetDeletePending(1, true), nil)
	wantError(t, e.SetDeletePending(2, true), nil)
	wantError(t, e.SetDeletePending(1, false), nil)
	if !e.DeletePending(9) {
		t.Fatal("one open cleared another disposition")
	}
	if _, err := e.Close(2); err != nil {
		t.Fatal(err)
	}
	wantError(t, e.SetDeletePending(1, false), nil)
	if !e.DeletePending(9) {
		t.Fatal("marked close lost pending deletion")
	}
	if result, err := e.Close(1); err != nil || !result.Last || !result.DeletePending {
		t.Fatalf("close=%+v %v", result, err)
	}
	if e.RangeCount(1) != 0 {
		t.Fatal("closed handle retained ranges")
	}
}

func TestBatchComparisonBudgetRejectsWorkBeforeEffects(t *testing.T) {
	e := engine(t)
	wantError(t, e.CheckBatchWork(1, 1, 100), ErrHandle)
	open(t, e, 1, 9, ShareAll, ShareAll)
	wantError(t, e.CheckBatchWork(1, 0, 100), ErrInvalid)
	wantError(t, e.CheckBatchWork(1, 1, 0), ErrInvalid)
	wantError(t, e.CheckBatchWork(1, 11, 10), ErrCapacity)
	wantError(t, e.CheckBatchWork(1, 4, 10), ErrCapacity)
	wantError(t, e.CheckBatchWork(1, 1, 1), nil)
	if _, err := e.LockBatch(1, []LockRequest{{Range: Range{Offset: 1, Length: 1}, Flags: Exclusive | FailImmediately}}); err != nil {
		t.Fatal(err)
	}
	wantError(t, e.CheckBatchWork(1, 1, 1), ErrCapacity)
	wantError(t, e.CheckBatchWork(1, 1, 2), nil)
	if e.RangeCount(1) != 1 {
		t.Fatal("work validation changed grants")
	}
}

func TestPendingDeletionRejectsNamedDataButKeepsExistingReferences(t *testing.T) {
	e := engine(t)
	open(t, e, 1, 9, ShareAll, ShareAll)
	open(t, e, 2, 9, Read|Write, ShareAll)
	wantError(t, e.SetDeletePending(1, true), nil)
	wantError(t, e.CheckIO(9, 0, Range{Offset: 0, Length: 1}, false), ErrDeletePending)
	wantError(t, e.CheckIO(9, 0, Range{Offset: 0, Length: 1}, true), ErrDeletePending)
	wantError(t, e.CheckIO(9, 2, Range{Offset: 0, Length: 1}, false), nil)
	wantError(t, e.CheckIO(9, 2, Range{Offset: 0, Length: 1}, true), nil)
	wantError(t, e.CheckSharing(9, 1, Delete), nil)
	wantError(t, e.CheckAccess(9, 0, 0), nil)
}

func TestOpenSharingBothDirections(t *testing.T) {
	for _, access := range []Access{Read, Write, Delete} {
		t.Run(string(rune('0'+access)), func(t *testing.T) {
			e := engine(t)
			open(t, e, 1, 9, access, ShareAll)
			wantError(t, e.CheckOpen(2, 9, access, ShareAll&^access), ErrSharing)
			wantError(t, e.Open(2, 9, access, ShareAll&^access), ErrSharing)
			if e.OpenCount(9) != 1 {
				t.Fatal("failed admission changed opens")
			}
			_, err := e.Close(1)
			wantError(t, err, nil)
			open(t, e, 1, 9, ShareAll&^access, ShareAll&^access)
			wantError(t, e.Open(2, 9, access, ShareAll), ErrSharing)
			wantError(t, e.CheckSharing(9, 0, access), ErrSharing)
			wantError(t, e.CheckSharing(9, 1, access), nil)
			wantError(t, e.CheckAccess(9, 1, access), ErrAccess)
			open(t, e, 2, 10, access, 0)
			wantError(t, e.CheckAccess(10, 2, access), nil)
		})
	}
}

func TestMetadataOnlyOpensDoNotRestrictSharing(t *testing.T) {
	for _, metadataFirst := range []bool{false, true} {
		e := engine(t)
		if metadataFirst {
			open(t, e, 1, 1, 0, 0)
			open(t, e, 2, 1, ShareAll, 0)
		} else {
			open(t, e, 2, 1, ShareAll, 0)
			open(t, e, 1, 1, 0, 0)
		}
		wantError(t, e.CheckAccess(1, 1, 0), nil)
		wantError(t, e.CheckAccess(1, 2, ShareAll), nil)
		wantError(t, e.CheckSharing(1, 0, Read), ErrSharing)
		_, err := e.Close(2)
		wantError(t, err, nil)
		wantError(t, e.CheckSharing(1, 0, ShareAll), nil)
	}
}

func TestOpenAdmissionAndBounds(t *testing.T) {
	for _, limits := range []Limits{{}, {1, 0, 1}, {1, 1, 0}, {-1, 1, 1}} {
		_, err := New(limits)
		wantError(t, err, ErrInvalid)
	}
	e, err := New(Limits{1, 1, 1})
	wantError(t, err, nil)
	for _, req := range []struct {
		handle, node  uint64
		access, share Access
	}{{0, 1, Read, ShareAll}, {1, 0, Read, ShareAll}, {1, 1, 8, ShareAll}, {1, 1, Read, 8}} {
		wantError(t, e.Open(req.handle, req.node, req.access, req.share), ErrInvalid)
	}
	wantError(t, e.CheckOpen(1, 1, Read, ShareAll), nil)
	if len(e.files) != 0 || len(e.opens) != 0 {
		t.Fatal("admission check registered state")
	}
	open(t, e, 1, 1, Read, ShareAll)
	wantError(t, e.Open(1, 2, Read, ShareAll), ErrInvalid)
	wantError(t, e.Open(2, 2, Read, ShareAll), ErrCapacity)
	node, err := e.HandleNode(1)
	wantError(t, err, nil)
	if node != 1 {
		t.Fatal(node)
	}
	_, err = e.HandleNode(2)
	wantError(t, err, ErrHandle)
	for _, check := range []func(uint64, uint64, Access) error{e.CheckAccess, e.CheckSharing} {
		wantError(t, check(0, 0, Read), ErrInvalid)
		wantError(t, check(1, 0, 8), ErrInvalid)
		wantError(t, check(1, 2, Read), ErrHandle)
		wantError(t, check(2, 1, Read), ErrHandle)
		wantError(t, check(2, 0, Read), nil)
	}
	_, err = e.Close(1)
	wantError(t, err, nil)
	open(t, e, 2, 2, Write, ShareAll)
}

func TestDeletePendingAndLastClose(t *testing.T) {
	e := engine(t)
	wantError(t, e.SetDeletePending(1, true), ErrHandle)
	open(t, e, 1, 1, Read, ShareAll)
	wantError(t, e.SetDeletePending(1, true), ErrAccess)
	open(t, e, 2, 1, Delete, ShareAll)
	wantError(t, e.SetDeletePending(2, true), nil)
	if !e.DeletePending(1) {
		t.Fatal("missing deletion state")
	}
	wantError(t, e.Open(3, 1, 0, ShareAll), ErrDeletePending)
	wantError(t, e.CheckAccess(1, 1, Read), nil)
	wantError(t, e.SetDeletePending(2, false), nil)
	open(t, e, 3, 1, 0, ShareAll)
	wantError(t, e.SetDeletePending(2, true), nil)
	for i, handle := range []uint64{2, 3, 1} {
		result, err := e.Close(handle)
		wantError(t, err, nil)
		if result.Node != 1 || !result.DeletePending || result.Last != (i == 2) {
			t.Fatalf("close %d: %+v", handle, result)
		}
	}
	if e.OpenCount(1) != 0 || e.DeletePending(1) || len(e.files) != 0 {
		t.Fatal("last close retained state")
	}
	_, err := e.Close(1)
	wantError(t, err, ErrHandle)
}
