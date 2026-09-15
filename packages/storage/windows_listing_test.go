package storage

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func windowsNameCharge(_ int, nameBytes int64, _ WindowsBasicAttr) (int64, error) {
	return nameBytes, nil
}

func TestWindowsListResultValidatesBoundsAndReturnsAnEmptyList(t *testing.T) {
	for _, test := range []struct {
		name       string
		max, fixed int64
		charge     func(int, int64, WindowsBasicAttr) (int64, error)
		want       error
	}{
		{"negative bound", -1, 0, windowsNameCharge, syscall.EINVAL},
		{"negative fixed charge", 1, -1, windowsNameCharge, syscall.EFBIG},
		{"fixed charge above bound", 1, 2, windowsNameCharge, syscall.EFBIG},
		{"missing charge", 1, 0, nil, syscall.EINVAL},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := NewWindowsListResult(test.max, test.fixed, test.charge)
			if result != nil || !errors.Is(err, test.want) {
				t.Fatalf("constructor = %v, %v; want nil, %v", result, err, test.want)
			}
		})
	}
	for _, limit := range []int64{0, 1, math.MaxInt64} {
		result, err := NewWindowsListResult(limit, limit, windowsNameCharge)
		if err != nil {
			t.Fatal(err)
		}
		if result.MaxBytes() != limit {
			t.Fatalf("configured bound = %d; want %d", result.MaxBytes(), limit)
		}
		entries, err := result.Entries()
		if err != nil || entries == nil || len(entries) != 0 {
			t.Fatalf("empty listing = %+v, %v", entries, err)
		}
		if err := result.Add(WindowsEntry{}); err != nil {
			t.Fatalf("zero-byte charge at exact bound: %v", err)
		}
	}
}

func TestWindowsListResultRejectsOverflowBeforeLoadingTheName(t *testing.T) {
	for _, test := range []struct {
		name       string
		max, fixed int64
		first      string
	}{
		{"small bound", 5, 1, "four"},
		{"maximum bound", math.MaxInt64, math.MaxInt64 - 1, "x"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := NewWindowsListResult(test.max, test.fixed, windowsNameCharge)
			if err != nil {
				t.Fatal(err)
			}
			if err := result.Add(WindowsEntry{Name: test.first}); err != nil {
				t.Fatalf("exact boundary: %v", err)
			}
			reservation, err := result.Reserve(math.MaxInt64, WindowsBasicAttr{})
			if reservation != nil || !errors.Is(err, syscall.EIO) {
				t.Fatalf("oversized unloaded name = %v, %v", reservation, err)
			}
			if entries, failure := result.Entries(); entries != nil || failure != err {
				t.Fatalf("overflow exposed a listing prefix: %+v, %v", entries, failure)
			}
		})
	}
}

func TestWindowsListResultRejectsNegativeLengthsAndCharges(t *testing.T) {
	for _, test := range []struct {
		name      string
		nameBytes int64
		charge    int64
		want      error
		calls     int
	}{
		{"name length", -1, 1, syscall.EIO, 1},
		{"entry charge", 1, -1, syscall.EINVAL, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			result, err := NewWindowsListResult(10, 0, func(index int, _ int64, _ WindowsBasicAttr) (int64, error) {
				calls++
				if index == 0 {
					return 1, nil
				}
				return test.charge, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := result.Add(WindowsEntry{Name: "a"}); err != nil {
				t.Fatal(err)
			}
			if _, err := result.Reserve(test.nameBytes, WindowsBasicAttr{}); !errors.Is(err, test.want) {
				t.Fatalf("invalid reservation = %v; want %v", err, test.want)
			}
			if entries, err := result.Entries(); entries != nil || !errors.Is(err, test.want) || calls != test.calls {
				t.Fatalf("failed listing = %+v, %v, charge calls=%d; want %d", entries, err, calls, test.calls)
			}
		})
	}
}

func TestWindowsListReservationsChargeBeforeCommitAndSortBytewise(t *testing.T) {
	var indexes []int
	result, err := NewWindowsListResult(8, 0, func(index int, nameBytes int64, attr WindowsBasicAttr) (int64, error) {
		indexes = append(indexes, index)
		if attr.ID != uint64(index+1) {
			t.Fatalf("charge %d lost its entry attributes: %+v", index, attr)
		}
		return nameBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"z", "B", "a", "é"}
	reservations := make([]*WindowsListReservation, len(names))
	for i, name := range names {
		reservations[i], err = result.Reserve(int64(len(name)), WindowsBasicAttr{Attr: Attr{ID: uint64(i + 1)}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("pending listing = %+v, %v; want unavailable result", entries, err)
	}
	for i := len(names) - 1; i >= 0; i-- {
		if err := reservations[i].Commit(names[i]); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(indexes, []int{0, 1, 2, 3}) {
		t.Fatalf("charge indexes = %v", indexes)
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []struct {
		name string
		id   uint64
	}{{"B", 2}, {"a", 3}, {"z", 1}, {"é", 4}} {
		if entries[i].Name != want.name || entries[i].Attr.ID != want.id {
			t.Fatalf("sorted entry %d = %+v; want %+v", i, entries[i], want)
		}
	}
}

func TestWindowsListReservationEnforcesSingleCommitAndExactNameLength(t *testing.T) {
	result, err := NewWindowsListResult(10, 0, windowsNameCharge)
	if err != nil {
		t.Fatal(err)
	}
	first, err := result.Reserve(1, WindowsBasicAttr{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Commit("a"); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit("a"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("second commit = %v; want EINVAL", err)
	}
	if entries, err := result.Entries(); err != nil || len(entries) != 1 {
		t.Fatalf("repeated commit changed completed entries: %+v, %v", entries, err)
	}
	second, err := result.Reserve(2, WindowsBasicAttr{})
	if err != nil {
		t.Fatal(err)
	}
	failure := second.Commit("long")
	if !errors.Is(failure, syscall.EIO) {
		t.Fatalf("different loaded name length = %v; want EIO", failure)
	}
	if entries, err := result.Entries(); entries != nil || err != failure {
		t.Fatalf("wrong name length exposed prefix: %+v, %v", entries, err)
	}
	if err := second.Commit("ok"); err != failure {
		t.Fatalf("name retry discarded original failure: %v", err)
	}
}

func TestWindowsListResultPreservesFirstFailureAndInvalidatesPendingEntries(t *testing.T) {
	cause := errors.Join(context.Canceled, errors.New("listing source failed"))
	calls := 0
	result, err := NewWindowsListResult(10, 0, func(index int, nameBytes int64, _ WindowsBasicAttr) (int64, error) {
		calls++
		if index == 2 {
			return 0, cause
		}
		return nameBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Add(WindowsEntry{Name: "a"}); err != nil {
		t.Fatal(err)
	}
	pending, err := result.Reserve(1, WindowsBasicAttr{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := result.Reserve(1, WindowsBasicAttr{}); err != cause {
		t.Fatalf("charge error identity lost: %v", err)
	}
	if err := result.Add(WindowsEntry{Name: "late"}); err != cause || calls != 3 {
		t.Fatalf("failed result admitted another charge: %v, calls=%d", err, calls)
	}
	for _, name := range []string{"x", "wrong length"} {
		if err := pending.Commit(name); err != cause {
			t.Fatalf("pending commit after failure = %v; want original cause", err)
		}
	}
	if err := result.Fail(syscall.EINVAL); err != cause {
		t.Fatalf("later failure replaced the first: %v", err)
	}
	if entries, err := result.Entries(); entries != nil || err != cause || !errors.Is(err, context.Canceled) {
		t.Fatalf("failed result = %+v, %v", entries, err)
	}
}

func TestWindowsListResultOwnsNamesAndAllFourTimeInstants(t *testing.T) {
	location := time.FixedZone(strings.Repeat("location", 1<<14), 3600)
	instant := time.Date(2026, time.September, 15, 12, 34, 56, 789, location)
	attr := WindowsBasicAttr{
		Attr:         Attr{ID: 42, Mode: 0o640, Size: 7, AccessTime: instant, ModTime: instant.Add(time.Hour)},
		CreationTime: instant.Add(-time.Hour), ChangeTime: instant.Add(2 * time.Hour),
		DOSAttributes: WindowsDOSHidden | WindowsDOSArchive, DeletePending: true,
	}
	normalized := attr
	normalized.AccessTime = attr.AccessTime.UTC()
	normalized.ModTime = attr.ModTime.UTC()
	normalized.CreationTime = attr.CreationTime.UTC()
	normalized.ChangeTime = attr.ChangeTime.UTC()
	var charged WindowsBasicAttr
	result, err := NewWindowsListResult(2, 0, func(_ int, nameBytes int64, received WindowsBasicAttr) (int64, error) {
		charged = received
		received.DOSAttributes = 0
		return nameBytes + 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := result.Reserve(1, attr)
	if err != nil {
		t.Fatal(err)
	}
	if charged != normalized || reservation.attr != normalized {
		t.Fatalf("attributes were not normalized before charging and retention: charge=%+v reserved=%+v", charged, reservation.attr)
	}
	if attr.AccessTime.Location() != location || attr.ModTime.Location() != location || attr.CreationTime.Location() != location || attr.ChangeTime.Location() != location {
		t.Fatal("reserving attributes changed caller-owned values")
	}
	backing := strings.Repeat("unretained", 1<<14) + "z"
	name := backing[len(backing)-1:]
	if err := reservation.Commit(name); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil || len(entries) != 1 {
		t.Fatalf("completed listing = %+v, %v", entries, err)
	}
	if entries[0].Name != name || unsafe.StringData(entries[0].Name) == unsafe.StringData(name) {
		t.Fatal("retained name does not own exactly its charged bytes")
	}
	if entries[0].Attr != normalized {
		t.Fatalf("committed entry lost Windows metadata: %+v", entries[0].Attr)
	}
}

func TestWindowsListResultExternalFailureAndNilOwners(t *testing.T) {
	cause := errors.New("enumeration failed")
	var absent *WindowsListResult
	if absent.MaxBytes() != 0 || absent.Fail(cause) != cause || absent.Fail(nil) != nil {
		t.Fatal("nil result changed bound or failure ownership")
	}
	if err := absent.Add(WindowsEntry{Name: "a"}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("nil result add = %v; want EINVAL", err)
	}
	var noReservation *WindowsListReservation
	if err := noReservation.Commit("a"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("nil reservation commit = %v; want EINVAL", err)
	}
	if err := (&WindowsListReservation{}).Commit("a"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("unowned reservation commit = %v; want EINVAL", err)
	}
	result, err := NewWindowsListResult(2, 0, windowsNameCharge)
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Add(WindowsEntry{Name: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := result.Fail(nil); err != nil {
		t.Fatalf("nil failure = %v", err)
	}
	if entries, err := result.Entries(); err != nil || len(entries) != 1 {
		t.Fatalf("nil failure invalidated listing: %+v, %v", entries, err)
	}
	pending, err := result.Reserve(1, WindowsBasicAttr{})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Fail(cause); err != cause || result.entries != nil || result.pending != 0 {
		t.Fatalf("external failure retained entries or reservations: %v", err)
	}
	if entries, err := result.Entries(); entries != nil || err != cause {
		t.Fatalf("external failure exposed entries: %+v, %v", entries, err)
	}
	if err := pending.Commit("b"); err != cause {
		t.Fatalf("external failure let a pending name commit: %v", err)
	}
}
