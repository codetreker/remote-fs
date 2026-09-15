package windowsaccess

import (
	"math"
	"testing"
)

func TestBatchConflictRollback(t *testing.T) {
	e := engine(t)
	open(t, e, 1, 1, Read|Write, ShareAll)
	open(t, e, 2, 1, Read|Write, ShareAll)
	grant(t, e, 1, Range{0, 1}, Exclusive)
	grant(t, e, 2, Range{10, 10}, Exclusive)
	result, err := e.LockBatch(1, []LockRequest{
		{Range{2, 2}, Exclusive | FailImmediately},
		{Range{5, 2}, Shared | FailImmediately},
		{Range{15, 2}, Exclusive | FailImmediately},
		{Range{30, 2}, Exclusive | FailImmediately},
	})
	wantError(t, err, ErrConflict)
	if result != (BatchResult{RolledBack: 2}) || e.ranges != 2 {
		t.Fatalf("rollback: %+v, ranges=%d", result, e.ranges)
	}
	for _, region := range []Range{{2, 2}, {5, 2}, {30, 2}} {
		wantError(t, e.CheckIO(1, 2, region, true), nil)
	}
	wantError(t, e.CheckIO(1, 2, Range{0, 1}, false), ErrConflict)
	for _, cleared := range e.files[1].locks[len(e.files[1].locks):cap(e.files[1].locks)] {
		if cleared != (heldLock{}) {
			t.Fatal("rollback retained discarded tail")
		}
	}
	result, err = e.LockBatch(1, []LockRequest{{Range{12, 1}, Shared}})
	wantError(t, err, ErrConflict)
	if result != (BatchResult{}) || e.ranges != 2 {
		t.Fatal("pending singleton changed state")
	}
}

func TestInvalidLaterLockRetainsEarlierGrant(t *testing.T) {
	for _, bad := range []LockRequest{
		{Range{10, 1}, Unlock | FailImmediately},
		{Range{10, 1}, Shared | Exclusive | FailImmediately},
		{Range{10, 1}, 0x80 | FailImmediately},
		{Range{math.MaxUint64, 2}, Shared | FailImmediately},
	} {
		e := engine(t)
		open(t, e, 1, 1, Read|Write, ShareAll)
		result, err := e.LockBatch(1, []LockRequest{{Range{1, 1}, Exclusive | FailImmediately}, bad})
		want := ErrInvalid
		if !bad.Range.Valid() {
			want = ErrRange
		}
		wantError(t, err, want)
		if result != (BatchResult{Applied: 1}) || e.ranges != 1 {
			t.Fatalf("earlier grant lost: %+v, %v", result, err)
		}
		wantError(t, e.CheckIO(1, 0, Range{1, 1}, false), ErrConflict)
	}
}

func TestUnlockPartialSuccess(t *testing.T) {
	for _, bad := range []LockRequest{
		{Range{20, 1}, Unlock},
		{Range{5, 5}, Shared},
		{Range{5, 5}, Exclusive | Unlock},
		{Range{math.MaxUint64, 2}, Unlock},
	} {
		e := engine(t)
		open(t, e, 1, 1, Read|Write, ShareAll)
		grant(t, e, 1, Range{0, 5}, Exclusive)
		grant(t, e, 1, Range{5, 5}, Exclusive)
		_, err := e.LockBatch(1, []LockRequest{{Range{0, 10}, Unlock}})
		wantError(t, err, ErrNotLocked)
		result, err := e.LockBatch(1, []LockRequest{{Range{0, 5}, Unlock}, bad, {Range{5, 5}, Unlock}})
		want := ErrInvalid
		if bad.Range.Offset == 20 {
			want = ErrNotLocked
		} else if !bad.Range.Valid() {
			want = ErrRange
		}
		wantError(t, err, want)
		if result != (BatchResult{Applied: 1}) || e.ranges != 1 {
			t.Fatalf("partial unlock: %+v, %v", result, err)
		}
		wantError(t, e.CheckIO(1, 0, Range{0, 5}, true), nil)
		wantError(t, e.CheckIO(1, 0, Range{5, 5}, true), ErrConflict)
	}
}

func TestBatchAdmissionAndCapacity(t *testing.T) {
	e, err := New(Limits{MaxOpens: 2, MaxRanges: 1, MaxBatchEntries: 2})
	wantError(t, err, nil)
	_, err = e.LockBatch(1, nil)
	wantError(t, err, ErrHandle)
	open(t, e, 1, 1, 0, ShareAll)
	_, err = e.LockBatch(1, []LockRequest{{Range{1, 1}, Shared}})
	wantError(t, err, ErrAccess)
	open(t, e, 2, 1, Read, ShareAll)
	for _, tc := range []struct {
		requests []LockRequest
		want     error
	}{
		{nil, ErrInvalid},
		{make([]LockRequest, 3), ErrCapacity},
		{[]LockRequest{{Range{1, 1}, Shared}, {Range{2, 1}, Shared | FailImmediately}}, ErrInvalid},
		{[]LockRequest{{Range{1, 1}, Shared | FailImmediately}, {Range{2, 1}, Shared}}, ErrInvalid},
	} {
		result, err := e.LockBatch(2, tc.requests)
		wantError(t, err, tc.want)
		if result != (BatchResult{}) || e.ranges != 0 {
			t.Fatal("admission mutated state")
		}
	}
	result, err := e.LockBatch(2, []LockRequest{
		{Range{1, 1}, Shared | FailImmediately},
		{Range{2, 1}, Shared | FailImmediately},
	})
	wantError(t, err, ErrCapacity)
	if result != (BatchResult{Applied: 1}) {
		t.Fatalf("capacity erased prior grant: %+v", result)
	}
	_, err = e.Close(2)
	wantError(t, err, nil)
	if e.ranges != 0 || len(e.files[1].locks) != 0 {
		t.Fatal("close retained locks")
	}
	open(t, e, 3, 1, Read, ShareAll)
	grant(t, e, 3, Range{1, 1}, Exclusive)
}

func TestZeroLengthLockLifecycle(t *testing.T) {
	e := engine(t)
	open(t, e, 1, 1, Read, ShareAll)
	open(t, e, 2, 1, Read, ShareAll)
	grant(t, e, 1, Range{}, Exclusive)
	grant(t, e, 2, Range{}, Exclusive)
	grant(t, e, 1, Range{math.MaxUint64, 1}, Exclusive)
	grant(t, e, 1, Range{math.MaxUint64, 1}, Unlock)
	grant(t, e, 1, Range{}, Unlock)
	grant(t, e, 2, Range{}, Unlock)
	if e.ranges != 0 {
		t.Fatal("zero-length locks were not released")
	}
}
