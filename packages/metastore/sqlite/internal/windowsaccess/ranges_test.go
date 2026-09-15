package windowsaccess

import (
	"errors"
	"math"
	"testing"
)

func grant(t *testing.T, e *Engine, handle uint64, region Range, flags LockFlags) {
	t.Helper()
	result, err := e.LockBatch(handle, []LockRequest{{region, flags}})
	if err != nil || result != (BatchResult{Applied: 1}) {
		t.Fatalf("grant: %+v, %v", result, err)
	}
}

func TestRangeLimitsAndZeroLength(t *testing.T) {
	for _, r := range []Range{{}, {7, 0}, {math.MaxUint64, 0}, {math.MaxUint64, 1}, {1, math.MaxUint64}} {
		if !r.Valid() {
			t.Fatalf("valid range rejected: %+v", r)
		}
	}
	for _, r := range []Range{{2, math.MaxUint64}, {math.MaxUint64, 2}} {
		if r.Valid() {
			t.Fatalf("overflow accepted: %+v", r)
		}
	}
	for _, tc := range []struct {
		a, b Range
		want bool
	}{
		{Range{}, Range{0, math.MaxUint64}, false},
		{Range{0, math.MaxUint64}, Range{}, false},
		{Range{5, 0}, Range{4, 2}, true},
		{Range{5, 0}, Range{5, 1}, false},
		{Range{5, 0}, Range{4, 1}, false},
		{Range{0, 5}, Range{5, 5}, false},
		{Range{0, 6}, Range{5, 5}, true},
		{Range{math.MaxUint64, 1}, Range{math.MaxUint64, 1}, true},
	} {
		if got := tc.a.overlaps(tc.b); got != tc.want {
			t.Fatalf("%+v overlaps %+v = %v", tc.a, tc.b, got)
		}
	}
}

func TestMandatoryIOAndOwnerRules(t *testing.T) {
	for _, mode := range []LockFlags{Shared, Exclusive} {
		t.Run(map[LockFlags]string{Shared: "shared", Exclusive: "exclusive"}[mode], func(t *testing.T) {
			e := engine(t)
			open(t, e, 1, 1, Read|Write, ShareAll)
			open(t, e, 2, 1, Read|Write, ShareAll)
			region := Range{5, 5}
			grant(t, e, 1, region, mode)
			for _, actor := range []uint64{0, 1, 2} {
				for _, write := range []bool{false, true} {
					var want error
					if mode == Shared && write || mode == Exclusive && actor != 1 {
						want = ErrConflict
					}
					err := e.CheckIO(1, actor, Range{4, 2}, write)
					wantError(t, err, want)
					if want != nil {
						var conflict *Conflict
						if !errors.As(err, &conflict) || conflict.Owner != 1 || conflict.Range != region || conflict.Exclusive != (mode == Exclusive) || conflict.Error() == "" {
							t.Fatalf("missing conflict detail: %v", err)
						}
					}
					wantError(t, e.CheckIO(1, actor, Range{10, 1}, write), nil)
					wantError(t, e.CheckIO(1, actor, Range{}, write), nil)
				}
			}
			wantError(t, e.CheckIO(2, 0, region, true), nil)
			wantError(t, e.CheckIO(1, 0, Range{math.MaxUint64, 2}, true), ErrRange)
			wantError(t, e.CheckIO(1, 3, region, false), ErrHandle)
		})
	}
}

func TestOverlappingOwnLocksAndExactUnlock(t *testing.T) {
	e := engine(t)
	open(t, e, 1, 1, Read|Write, ShareAll)
	open(t, e, 2, 1, Read|Write, ShareAll)
	region := Range{10, 10}
	grant(t, e, 1, region, Exclusive)
	_, err := e.LockBatch(1, []LockRequest{{Range{12, 1}, Exclusive | FailImmediately}})
	wantError(t, err, ErrConflict)
	grant(t, e, 1, region, Shared)
	grant(t, e, 1, region, Shared)
	wantError(t, e.CheckIO(1, 1, region, true), ErrConflict)
	wantError(t, e.CheckIO(1, 2, region, false), ErrConflict)
	grant(t, e, 1, region, Unlock)
	wantError(t, e.CheckIO(1, 2, region, false), nil)
	grant(t, e, 2, region, Shared)
	grant(t, e, 1, region, Unlock)
	grant(t, e, 1, region, Unlock)
	_, err = e.LockBatch(1, []LockRequest{{region, Unlock}})
	wantError(t, err, ErrNotLocked)
	_, err = e.LockBatch(2, []LockRequest{{Range{10, 9}, Unlock}})
	wantError(t, err, ErrNotLocked)
	wantError(t, e.CheckIO(1, 2, region, true), ErrConflict)
	grant(t, e, 2, region, Unlock|FailImmediately)
	wantError(t, e.CheckIO(1, 0, region, true), nil)
}
