package advisory

import (
	"math"
	"testing"
)

func TestUnsignedByteExtents(t *testing.T) {
	for _, test := range []struct {
		start, length, last uint64
		valid               bool
	}{
		{0, 0, 0, false},
		{0, 1, 0, true},
		{0, math.MaxUint64, math.MaxUint64 - 1, true},
		{1, math.MaxUint64, math.MaxUint64, true},
		{2, math.MaxUint64, 0, false},
		{math.MaxUint64, 1, math.MaxUint64, true},
		{math.MaxUint64, 2, 0, false},
		{1 << 63, 1 << 63, math.MaxUint64, true},
	} {
		last, valid := byteLast(test.start, test.length)
		if last != test.last || valid != test.valid {
			t.Fatalf("extent (%d,%d): last=%d valid=%t; want %d,%t", test.start, test.length, last, valid, test.last, test.valid)
		}
	}
}

func TestByteAndBoundaryOverlap(t *testing.T) {
	for _, test := range []struct {
		aStart, aLength, bStart, bLength uint64
		overlap                          bool
	}{
		{0, 10, 10, 1, false},
		{0, 10, 9, 1, true},
		{10, 10, 5, 6, true},
		{10, 10, 5, 5, false},
		{math.MaxUint64, 1, math.MaxUint64, 1, true},
		{math.MaxUint64 - 1, 1, math.MaxUint64, 1, false},
		{math.MaxUint64, 2, 0, 1, false},
		{0, 0, 0, 1, false},
	} {
		forward := bytesOverlap(test.aStart, test.aLength, test.bStart, test.bLength)
		backward := bytesOverlap(test.bStart, test.bLength, test.aStart, test.aLength)
		if forward != test.overlap || backward != test.overlap {
			t.Fatalf("overlap %+v: forward=%t backward=%t", test, forward, backward)
		}
	}
	for _, test := range []struct {
		start, length, cut uint64
		overlap            bool
	}{
		{0, 10, 0, false},
		{0, 10, 1, true},
		{0, 10, 9, true},
		{0, 10, 10, false},
		{10, 10, 10, false},
		{10, 10, 11, true},
		{math.MaxUint64 - 1, 2, math.MaxUint64, true},
		{math.MaxUint64, 1, math.MaxUint64, false},
		{math.MaxUint64, 2, 1, false},
	} {
		if got := boundaryOverlaps(test.start, test.length, test.cut); got != test.overlap {
			t.Fatalf("boundary overlap %+v: got %t", test, got)
		}
	}
}
