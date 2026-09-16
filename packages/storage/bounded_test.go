package storage_test

import (
	"errors"
	"math"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestListResultRefusesBeforeRetainingAnOversizedEntry(t *testing.T) {
	result, err := storage.NewListResult(5, 1, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Add(storage.Entry{Name: "four"}); err != nil {
		t.Fatal(err)
	}
	if err := result.Add(storage.Entry{Name: "x"}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("adding an entry above the result bound returned %v, want EIO", err)
	}
	if entries, resultErr := result.Entries(); resultErr == nil || entries != nil {
		t.Fatalf("the refused entry left a usable partial result: %+v, %v", entries, resultErr)
	}
}

func TestListResultSortsBytewise(t *testing.T) {
	result, err := storage.NewListResult(3, 0, func(_ int, _ int64, _ int64, _ storage.Attr) (int64, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"z", "A", "a"} {
		if err := result.Add(storage.Entry{Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"A", "a", "z"} {
		if entries[i].Name != want {
			t.Fatalf("entry %d is %q, want %q", i, entries[i].Name, want)
		}
	}
}

func TestListResultFailInvalidatesRetainedEntries(t *testing.T) {
	result, err := storage.NewListResult(16, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Add(storage.Entry{Name: "retained"}); err != nil {
		t.Fatal(err)
	}
	result.Fail(syscall.EIO)
	if entries, err := result.Entries(); !errors.Is(err, syscall.EIO) || entries != nil {
		t.Fatalf("failed result exposed %+v, %v", entries, err)
	}
}

func TestListResultOwnsOnlyTheChargedNameAndTimeInstants(t *testing.T) {
	result, err := storage.NewListResult(1024, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	backing := strings.Repeat("unretained", 1<<17) + "z"
	name := backing[len(backing)-1:]
	location := time.FixedZone(strings.Repeat("location", 1<<17), 3600)
	access := time.Date(2026, time.September, 4, 12, 34, 56, 789, location)
	modified := access.Add(time.Hour)
	if err := result.Add(storage.Entry{Name: name, Attr: storage.Attr{AccessTime: access, ModTime: modified}}); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if unsafe.StringData(entries[0].Name) == unsafe.StringData(name) {
		t.Fatal("the retained name aliases the large caller-owned backing string")
	}
	if entries[0].Attr.AccessTime.Location() != time.UTC || entries[0].Attr.ModTime.Location() != time.UTC {
		t.Fatalf("retained times keep caller-owned locations: %v, %v",
			entries[0].Attr.AccessTime.Location(), entries[0].Attr.ModTime.Location())
	}
	if !entries[0].Attr.AccessTime.Equal(access) || !entries[0].Attr.ModTime.Equal(modified) {
		t.Fatalf("UTC normalization changed the instants: %+v", entries[0].Attr)
	}
}

func TestListPrefixChargesActualOutputWithoutCreatingEntry(t *testing.T) {
	result, err := storage.NewListResult(20, 2, func(index int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		if index != 0 {
			t.Fatalf("prefix consumed an entry index: %d", index)
		}
		return nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.ReservePrefix(9); err != nil {
		t.Fatal(err)
	}
	if entries, err := result.Entries(); err != nil || len(entries) != 0 {
		t.Fatalf("prefix invented directory entry: %+v %v", entries, err)
	}
	if err := result.Add(storage.Entry{Name: "one", Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular}}); err != nil {
		t.Fatalf("entry did not fit exact combined bound: %v", err)
	}
	entries, err := result.Entries()
	if err != nil || len(entries) != 1 || entries[0].Name != "one" {
		t.Fatalf("prefix changed entries: %+v %v", entries, err)
	}
}

func TestListPrefixRejectsLateRepeatedAndOverflowCharges(t *testing.T) {
	makeResult := func(max, fixed int64) *storage.ListResult {
		t.Helper()
		result, err := storage.NewListResult(max, fixed, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
			return nameBytes + metadataBytes, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	var missing *storage.ListResult
	if err := missing.ReservePrefix(0); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("nil prefix receiver=%v", err)
	}
	zero := makeResult(0, 0)
	if err := zero.ReservePrefix(0); err != nil {
		t.Fatal(err)
	}
	if entries, err := zero.Entries(); err != nil || len(entries) != 0 {
		t.Fatalf("zero prefix result=%+v %v", entries, err)
	}
	first := zero.ReservePrefix(0)
	if !errors.Is(first, syscall.EINVAL) {
		t.Fatalf("repeated zero prefix=%v", first)
	}
	if again := zero.ReservePrefix(-1); again != first {
		t.Fatal("later prefix replaced first failure")
	}
	for _, test := range []struct {
		max, fixed, charge int64
		want               syscall.Errno
	}{
		{10, 0, -1, syscall.EINVAL}, {10, 3, 8, syscall.EFBIG}, {math.MaxInt64, 1, math.MaxInt64, syscall.EFBIG},
	} {
		result := makeResult(test.max, test.fixed)
		failure := result.ReservePrefix(test.charge)
		if !errors.Is(failure, test.want) {
			t.Fatalf("prefix %d returned %v, want %v", test.charge, failure, test.want)
		}
		if entries, err := result.Entries(); entries != nil || err != failure {
			t.Fatalf("failed prefix exposed entries=%+v %v", entries, err)
		}
	}
	for _, commit := range []bool{false, true} {
		result := makeResult(100, 0)
		reservation, err := result.Reserve(1, 6, storage.Attr{ID: 2, Kind: storage.NodeRegular})
		if err != nil {
			t.Fatal(err)
		}
		if commit {
			if err := reservation.Commit("x", nil); err != nil {
				t.Fatal(err)
			}
		}
		failure := result.ReservePrefix(1)
		if !errors.Is(failure, syscall.EINVAL) {
			t.Fatalf("prefix after entry reservation=%v", failure)
		}
		if entries, err := result.Entries(); entries != nil || err != failure {
			t.Fatalf("late prefix retained partial entries=%+v %v", entries, err)
		}
		if !commit {
			if err := reservation.Commit("x", nil); err != failure {
				t.Fatal("pending entry survived prefix failure")
			}
		}
	}
	prior := errors.New("earlier producer failure")
	failed := makeResult(100, 0)
	failed.Fail(prior)
	if err := failed.ReservePrefix(0); err != prior {
		t.Fatal("prefix replaced producer failure")
	}
}
