package storage_test

import (
	"errors"
	"io/fs"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestCleanPathNamespaceBoundary(t *testing.T) {
	for _, tc := range []struct {
		input, want string
		invalid     bool
	}{
		{"", "", false}, {".", "", false}, {"a/..", "", false}, {"a//b/./", "a/b", false},
		{"a/../b", "b", false}, {"../a", "", true}, {"a/../..", "", true}, {"/a", "", true},
		{"..hidden", "..hidden", false}, {"a\\b", "a\\b", false}, {"a/\xff", "a/\xff", false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := storage.CleanPath(tc.input)
			if tc.invalid {
				if !errors.Is(err, syscall.EINVAL) || got != "" {
					t.Fatalf("CleanPath(%q) = %q, %v", tc.input, got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("CleanPath(%q) = %q, %v; want %q", tc.input, got, err, tc.want)
			}
		})
	}
}

func TestSpaceRejectsImpossibleCapacity(t *testing.T) {
	for _, tc := range []struct {
		space storage.Space
		want  bool
	}{
		{storage.Space{}, true}, {storage.Space{Total: 100, Used: 40, Avail: 60}, true},
		{storage.Space{Total: 100, Used: 40, Avail: 20}, true}, {storage.Space{Total: 10, Used: 20}, true},
		{storage.Space{Total: 10, Used: 20, Avail: 1}, false}, {storage.Space{Total: 10, Used: 2, Avail: 9}, false},
		{storage.Space{Total: -1}, false}, {storage.Space{Used: -1}, false}, {storage.Space{Avail: -1}, false},
		{storage.Space{Total: math.MaxInt64, Used: math.MaxInt64}, true},
		{storage.Space{Total: 0, Used: math.MaxInt64, Avail: 1}, false},
	} {
		if got := tc.space.Coherent(); got != tc.want {
			t.Errorf("%+v.Coherent() = %v; want %v", tc.space, got, tc.want)
		}
	}
}

func TestAttrKindAndChangeValidation(t *testing.T) {
	for _, mode := range []fs.FileMode{0o600, fs.ModeDir | 0o755, fs.ModeSymlink | 0o777} {
		if got := (storage.Attr{Mode: mode}).IsDir(); got != (mode&fs.ModeDir != 0) {
			t.Fatalf("IsDir(%v) = %v", mode, got)
		}
	}
	zeroTime := time.Time{}
	zeroMode := fs.FileMode(0)
	for _, change := range []storage.AttrChange{{Mode: &zeroMode}, {AccessTime: &zeroTime}, {ModTime: &zeroTime}} {
		if change.Empty() || change.Check() != nil {
			t.Errorf("explicit zero change rejected: %+v", change)
		}
	}
	if !(storage.AttrChange{}).Empty() || (storage.AttrChange{}).Check() != nil {
		t.Fatal("empty change is invalid")
	}
	for _, mode := range []fs.FileMode{storage.SettableMode, 0o640, fs.ModeDir, fs.ModeSymlink, fs.ModeAppend, fs.ModeExclusive} {
		err := (storage.AttrChange{Mode: &mode}).Check()
		if mode&^storage.SettableMode == 0 {
			if err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("mode %v returned %v", mode, err)
		}
	}
}

func TestListResultReportsConfiguredBound(t *testing.T) {
	var absent *storage.ListResult
	if absent.MaxBytes() != 0 {
		t.Fatal("nil result has a nonzero bound")
	}
	for _, limit := range []int64{0, 1, math.MaxInt64} {
		result, err := storage.NewListResult(limit, 0, func(int, int64, storage.Attr) (int64, error) { return 0, nil })
		if err != nil {
			t.Fatal(err)
		}
		if result.MaxBytes() != limit {
			t.Fatalf("MaxBytes = %d; want %d", result.MaxBytes(), limit)
		}
	}
}
