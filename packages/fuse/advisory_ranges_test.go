package fuse

import (
	"errors"
	"math"
	"reflect"
	"syscall"
	"testing"
)

func TestLocalPOSIXRangePlanning(t *testing.T) {
	rangeOf := func(start, end uint64, kind lockType) fileLock {
		return fileLock{Family: posixFamily, Type: kind, Start: start, End: end, PID: 42}
	}
	for _, test := range []struct {
		name   string
		old    []fileLock
		change fileLock
		want   []fileLock
	}{
		{"split", []fileLock{rangeOf(0, 99, sharedType)}, rangeOf(20, 29, exclusiveType), []fileLock{rangeOf(0, 19, sharedType), rangeOf(20, 29, exclusiveType), rangeOf(30, 99, sharedType)}},
		{"unlock middle", []fileLock{rangeOf(0, 99, exclusiveType)}, rangeOf(20, 29, unlockType), []fileLock{rangeOf(0, 19, exclusiveType), rangeOf(30, 99, exclusiveType)}},
		{"merge three", []fileLock{rangeOf(0, 19, sharedType), rangeOf(30, 99, sharedType)}, rangeOf(20, 29, sharedType), []fileLock{rangeOf(0, 99, sharedType)}},
		{"future eof", []fileLock{rangeOf(5, math.MaxInt64, sharedType)}, rangeOf(10, math.MaxInt64, unlockType), []fileLock{rangeOf(5, 9, sharedType)}},
		{"disjoint", []fileLock{rangeOf(10, 19, sharedType)}, rangeOf(30, 39, exclusiveType), []fileLock{rangeOf(10, 19, sharedType), rangeOf(30, 39, exclusiveType)}},
		{"remove all", []fileLock{rangeOf(0, math.MaxInt64, exclusiveType)}, rangeOf(0, math.MaxInt64, unlockType), []fileLock{}},
		{"replace all", []fileLock{rangeOf(10, 19, sharedType)}, rangeOf(0, math.MaxInt64, exclusiveType), []fileLock{rangeOf(0, math.MaxInt64, exclusiveType)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := append([]fileLock(nil), test.old...)
			got := replacePOSIXRanges(test.old, test.change)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ranges = %#v, want %#v", got, test.want)
			}
			if !reflect.DeepEqual(test.old, before) {
				t.Fatal("planning changed held ranges")
			}
		})
	}
	waiting := rangeOf(0, 10, sharedType)
	waiting.Wait = true
	if got := replacePOSIXRanges(nil, waiting); got[0].Wait {
		t.Fatal("held range retains waiting flag")
	}
}

func TestLocalKernelRangeValidation(t *testing.T) {
	valid := fileLock{Family: posixFamily, Type: sharedType, End: math.MaxInt64}
	for _, change := range []func(*fileLock){
		func(l *fileLock) { l.Family = 0 },
		func(l *fileLock) { l.Type = 0 },
		func(l *fileLock) { l.Start, l.End = 2, 1 },
		func(l *fileLock) { l.End = math.MaxUint64 },
		func(l *fileLock) { l.Family, l.Start = flockFamily, 1 },
		func(l *fileLock) { l.Type, l.Wait = unlockType, true },
	} {
		invalid := valid
		change(&invalid)
		if !errors.Is(invalid.check(), syscall.EINVAL) {
			t.Fatalf("accepted %#v", invalid)
		}
	}
	if err := valid.check(); err != nil {
		t.Fatal(err)
	}
	valid.Family = flockFamily
	if err := valid.check(); err != nil {
		t.Fatal(err)
	}
}

func TestLocalPOSIXPlansMatchByteOwnership(t *testing.T) {
	var held []fileLock
	var bytes [32]lockType
	for step := 0; step < 256; step++ {
		start := uint64(step * 7 % len(bytes))
		end := start + uint64(step*11%int(uint64(len(bytes))-start))
		kind := lockType(step%3 + 1)
		change := fileLock{Family: posixFamily, Type: kind, Start: start, End: end}
		held = replacePOSIXRanges(held, change)
		for i := start; i <= end; i++ {
			bytes[i] = kind
		}
		for i, want := range bytes {
			got := unlockType
			for j, r := range held {
				if j > 0 && (held[j-1].End >= r.Start || held[j-1].End+1 == r.Start && held[j-1].Type == r.Type) {
					t.Fatalf("step %d: ranges are overlapping or unmerged: %#v", step, held)
				}
				if r.Start <= uint64(i) && uint64(i) <= r.End {
					got = r.Type
				}
			}
			if want == 0 {
				want = unlockType
			}
			if got != want {
				t.Fatalf("step %d byte %d: got %d, want %d", step, i, got, want)
			}
		}
	}
}
