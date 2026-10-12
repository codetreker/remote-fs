package smb

import (
	"errors"
	"slices"
	"strings"
	"syscall"
	"testing"
)

func TestDirectoryPatternDOSAndUTF16Vectors(t *testing.T) {
	tests := []struct {
		expression, name string
		match            bool
	}{
		{"", "file", true}, {"*", "file", true}, {"*.*", "file", true},
		{"*.*", "a.b", true}, {"*", "", false}, {"*.TXT", "a.txt", true},
		{"a?", "a", false}, {"a?", "ab", true}, {"a?", "abc", false},
		{"?", "😀", false}, {"??", "😀", true}, {"?A", "😀a", false}, {"??A", "😀a", true},
		{"<.txt", "foo.txt", true}, {"<.txt", "foo.bar.txt", true}, {"<.txt", "foo.bar", false},
		{"<.txt", ".txt", true}, {"<", "foo", true}, {"<", "foo.txt", false},
		{"<\"", "foo", true}, {"<\"", "foo.txt", false},
		{"a>>>.txt", "ab.txt", true}, {"a>>>.txt", "a.txt", true}, {"a>>>.txt", "abcde.txt", false},
		{"a>>>\"", "a", true}, {"a>>>\"", "abc", true}, {"a>>>\"", "a.b", false},
		{"a\"", "a", true}, {"a\"b", "a.b", true}, {"a\"b", "ab", false},
		{"*a*a*a*b", "aaaaaab", true}, {"*a*a*a*b", "aaaaaac", false},
		{".", "file", false}, {"..", "file", false},
	}
	for _, test := range tests {
		t.Run(test.expression+"/"+test.name, func(t *testing.T) {
			pattern, err := compileDirectoryPattern(nameUnits(test.expression))
			if err != nil {
				t.Fatal(err)
			}
			work := uint64(1 << 20)
			got, err := pattern.match(nameUnits(test.name), &work)
			if err != nil || got != test.match || work == 1<<20 {
				t.Fatalf("match = %v, %v; want %v, remaining work %d", got, err, test.match, work)
			}
		})
	}
}

func TestDirectoryPatternUsesExistingWindowsCaseEquivalence(t *testing.T) {
	for _, vector := range nameComparisonVectors {
		pattern, err := compileDirectoryPattern(nameUnits(vector.left))
		if err != nil {
			t.Fatalf("compile %q: %v", vector.left, err)
		}
		work := uint64(1 << 20)
		got, err := pattern.match(nameUnits(vector.right), &work)
		if err != nil || got != vector.equal {
			t.Fatalf("%q / %q = %v, %v; want %v", vector.left, vector.right, got, err, vector.equal)
		}
	}
}

func TestDirectoryPatternValidationAndOwnership(t *testing.T) {
	for _, value := range []string{"nul\x00name", "control\x1fname", "a/b", `a\b`, "a:b", "a|b", "CON", "NUL.txt", "literal.", "literal ", strings.Repeat("?", maxNameUnits+1)} {
		if _, err := compileDirectoryPattern(nameUnits(value)); !errors.Is(err, errNameInvalid) {
			t.Fatalf("invalid pattern %q: %v", value, err)
		}
	}
	for _, units := range [][]uint16{{0xd83d}, {0xde00}, {0xd83d, 'A'}} {
		if _, err := compileDirectoryPattern(units); !errors.Is(err, errNameInvalid) {
			t.Fatalf("invalid UTF-16 %x: %v", units, err)
		}
	}
	for _, value := range []string{".", "..", "*.", "*.*", "<.txt", "a>>>\"", "CON*"} {
		if _, err := compileDirectoryPattern(nameUnits(value)); err != nil {
			t.Fatalf("valid search pattern %q: %v", value, err)
		}
	}
	input := nameUnits("file")
	before := slices.Clone(input)
	pattern, err := compileDirectoryPattern(input)
	if err != nil || !slices.Equal(input, before) {
		t.Fatalf("compile modified pattern: %x, %v", input, err)
	}
	input[0] = 'X'
	name := nameUnits("FILE")
	nameBefore := slices.Clone(name)
	work := uint64(64)
	if got, err := pattern.match(name, &work); err != nil || !got || !slices.Equal(name, nameBefore) {
		t.Fatalf("pattern borrowed input or changed name: %v, %v", got, err)
	}
}

func TestDirectoryPatternCumulativeWorkAndTerminalPosition(t *testing.T) {
	pattern, err := compileDirectoryPattern(nameUnits("a"))
	if err != nil {
		t.Fatal(err)
	}
	work := uint64(3)
	if got, err := pattern.match(nameUnits("A"), &work); err != nil || !got || work != 1 {
		t.Fatalf("first entry work = %v, %v, remaining %d", got, err, work)
	}
	if got, err := pattern.match(nameUnits("A"), &work); got || !errors.Is(err, syscall.ENOMEM) || work != 0 {
		t.Fatalf("cumulative work exhaustion = %v, %v, remaining %d", got, err, work)
	}
	all, err := compileDirectoryPattern(nameUnits("*.*"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := all.match(nameUnits("A"), &work); got || !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("all-match bypassed work cap = %v, %v", got, err)
	}
	if got, err := pattern.match(nameUnits("A"), nil); got || !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("missing work cap = %v, %v", got, err)
	}
	maximum := strings.Repeat("a", maxNameUnits)
	long, err := compileDirectoryPattern(nameUnits(maximum))
	if err != nil {
		t.Fatal(err)
	}
	work = maxNameUnits + 1
	if got, err := long.match(nameUnits(maximum), &work); err != nil || !got || work != 0 {
		t.Fatalf("terminal position %d = %v, %v, remaining %d", maxNameUnits, got, err, work)
	}
	branching, err := compileDirectoryPattern(nameUnits(strings.Repeat("*?", 127) + "?"))
	if err != nil {
		t.Fatal(err)
	}
	work = uint64((maxNameUnits + 1) * (maxNameUnits + 1))
	if got, err := branching.match(nameUnits(maximum), &work); err != nil || !got {
		t.Fatalf("bounded branching = %v, %v", got, err)
	}
	work = 64
	if got, err := (directoryPattern{}).match(nameUnits("A"), &work); got || !errors.Is(err, errNameInvalid) {
		t.Fatalf("uncompiled pattern = %v, %v", got, err)
	}
	if got, err := pattern.match(nameUnits(maximum+"a"), &work); got || !errors.Is(err, errNameInvalid) {
		t.Fatalf("oversized name = %v, %v", got, err)
	}
}
