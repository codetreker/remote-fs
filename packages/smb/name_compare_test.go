package smb

import (
	"errors"
	"slices"
	"strings"
	"syscall"
	"testing"
)

type nameComparisonVector struct {
	left, right string
	equal       bool
}

var nameComparisonVectors = []nameComparisonVector{
	{"file", "FILE", true},
	{"é", "É", true},
	{"σ", "Σ", true},
	{"ａ", "Ａ", true},
	{"ſ", "S", false},
	{"ı", "I", false},
	{"K", "K", false},
	{"µ", "Μ", false},
	{"ς", "Σ", false},
	{"\u0345", "Ι", false},
	{"ß", "ẞ", false},
	{"ß", "SS", false},
	{"Ω", "Ω", false},
	{"Å", "Å", false},
	{"\U00010428", "\U00010400", false},
	{"é", "e\u0301", false},
	{"ａ", "a", false},
	{"ﬀ", "FF", false},
	{"😀a", "😀A", true},
	{"é😀a", "É😀A", true},
	{"a", "AA", false},
	{strings.Repeat("é", maxNameUnits), strings.Repeat("É", maxNameUnits), true},
	{strings.Repeat("😀", 127) + "a", strings.Repeat("😀", 127) + "A", true},
}

func TestWindowsNameComparisonVectors(t *testing.T) {
	for _, vector := range nameComparisonVectors {
		t.Run(vector.left+"/"+vector.right, func(t *testing.T) {
			left, right := nameUnits(vector.left), nameUnits(vector.right)
			beforeLeft, beforeRight := slices.Clone(left), slices.Clone(right)
			order, err := portableNameCompare(left, right)
			if err != nil || (order == 0) != vector.equal {
				t.Fatalf("compare %q/%q = %d, %v; equal=%v", vector.left, vector.right, order, err, vector.equal)
			}
			reverse, err := portableNameCompare(right, left)
			if err != nil || reverse != -order {
				t.Fatalf("asymmetric comparison: %d/%d %v", order, reverse, err)
			}
			if !slices.Equal(left, beforeLeft) || !slices.Equal(right, beforeRight) {
				t.Fatal("comparison changed literal UTF-16")
			}
		})
	}
	for _, pair := range [][2]string{{"\U00010000", "\ue000"}, {"a", "AA"}, {"é", "Éa"}} {
		if order, err := portableNameCompare(nameUnits(pair[0]), nameUnits(pair[1])); err != nil || order != -1 {
			t.Fatalf("UTF-16 or prefix order %q/%q: %d %v", pair[0], pair[1], order, err)
		}
	}
}

func TestWindowsNameUppercaseTableCompleteness(t *testing.T) {
	mapped := 0
	for unit := 0; unit <= 0xffff; unit++ {
		upper := uppercaseNameUnit(uint16(unit))
		if windowsUppercase[unit] != 0 {
			mapped++
		}
		if uppercaseNameUnit(upper) != upper {
			t.Fatalf("non-idempotent code-unit mapping: %04x -> %04x", unit, upper)
		}
		if unit >= 0xd800 && unit <= 0xdfff && upper != uint16(unit) {
			t.Fatalf("surrogate mapped as a Unicode scalar: %04x -> %04x", unit, upper)
		}
	}
	if mapped != 973 {
		t.Fatalf("Microsoft table has %d mappings; want 973", mapped)
	}
	for _, pair := range [][2][]uint16{{nil, {'a'}}, {{'a'}, nil}, {make([]uint16, maxNameUnits+1), {'a'}}, {{'a'}, make([]uint16, maxNameUnits+1)}} {
		if _, err := portableNameCompare(pair[0], pair[1]); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid comparison length accepted: %v", err)
		}
	}
}
