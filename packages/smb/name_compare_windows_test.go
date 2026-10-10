//go:build windows

package smb

import (
	"slices"
	"testing"
)

func TestWindowsNativeNameComparisonCodeUnits(t *testing.T) {
	units := make([]uint16, 1<<16)
	for value := range units {
		unit := uint16(value)
		units[value] = unit
		upper := uppercaseNameUnit(unit)
		order, err := nativeNameCompare([]uint16{unit}, []uint16{upper})
		if err != nil || order != 0 {
			t.Fatalf("native NLS differs from Microsoft mapping at %04x -> %04x: order=%d error=%v", unit, upper, order, err)
		}
	}
	slices.SortFunc(units, func(left, right uint16) int {
		order, err := portableNameCompare([]uint16{left}, []uint16{right})
		if err != nil {
			t.Fatal(err)
		}
		return order
	})
	for i := 1; i < len(units); i++ {
		left, right := []uint16{units[i-1]}, []uint16{units[i]}
		want, err := portableNameCompare(left, right)
		if err != nil {
			t.Fatal(err)
		}
		got, err := nativeNameCompare(left, right)
		if err != nil || got != want {
			t.Fatalf("native NLS differs from portable UTF-16 order at %04x/%04x: native=%d portable=%d error=%v", left[0], right[0], got, want, err)
		}
	}
}

func TestWindowsNativeNameComparisonVectors(t *testing.T) {
	for _, vector := range nameComparisonVectors {
		left, right := nameUnits(vector.left), nameUnits(vector.right)
		want, err := portableNameCompare(left, right)
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range [][2][]uint16{{left, right}, {right, left}} {
			got, err := nativeNameCompare(pair[0], pair[1])
			if err != nil || got != want {
				t.Fatalf("native NLS differs for %q/%q: native=%d portable=%d error=%v", vector.left, vector.right, got, want, err)
			}
			want = -want
		}
	}
	for _, pair := range [][2]string{{"\U00010000", "\ue000"}, {"a", "AA"}, {"é", "Éa"}} {
		got, err := nativeNameCompare(nameUnits(pair[0]), nameUnits(pair[1]))
		if err != nil || got != -1 {
			t.Fatalf("native UTF-16 or prefix order %q/%q: %d %v", pair[0], pair[1], got, err)
		}
	}
}
