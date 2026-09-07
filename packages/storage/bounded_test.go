package storage_test

import (
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestListResultRefusesBeforeRetainingAnOversizedEntry(t *testing.T) {
	result, err := storage.NewListResult(5, 1, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
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
	result, err := storage.NewListResult(3, 0, func(_ int, _ int64, _ storage.Attr) (int64, error) { return 1, nil })
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
	result, err := storage.NewListResult(16, 0, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
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
	result, err := storage.NewListResult(1024, 0, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
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
