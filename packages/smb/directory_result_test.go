package smb

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestDirectoryResultReservesOwnedBoundedMetadata(t *testing.T) {
	zone := time.FixedZone("caller", 3600)
	stamp := time.Date(2026, 9, 16, 12, 0, 0, 0, zone)
	attr := windowsBasicAttr{Attr: storage.Attr{ID: 1, Kind: storage.NodeRegular, AccessTime: stamp, ModTime: stamp, CreationTime: &stamp, ChangeTime: &stamp, Metadata: storage.Metadata{{Key: "foreign", Version: 1, Data: []byte("payload")}}}, CreationTime: stamp, ChangeTime: stamp}
	charges := 0
	result, err := newWindowsListResult(128, 8, func(index int, n int64, a windowsBasicAttr) (int64, error) {
		if index != charges || a.Metadata != nil || a.Attr.CreationTime != nil || a.Attr.ChangeTime != nil {
			t.Fatalf("retained variable metadata: %+v", a)
		}
		for _, v := range []time.Time{a.AccessTime, a.ModTime, a.CreationTime, a.ChangeTime} {
			if v.Location() != time.UTC || !v.Equal(stamp) {
				t.Fatal(v)
			}
		}
		charges++
		return n + 16, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := result.Reserve(1, attr)
	if err != nil {
		t.Fatal(err)
	}
	if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("incomplete reservation exposed: %v %v", entries, err)
	}
	if err := reservation.Commit("z"); err != nil {
		t.Fatal(err)
	}
	if err := reservation.Commit("z"); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	if err := result.Add(windowsEntry{Name: "a", Attr: attr}); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil || len(entries) != 2 || entries[0].Name != "a" || entries[1].Name != "z" || result.MaxBytes() != 128 {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
}

func TestDirectoryResultFailureCannotExposePrefix(t *testing.T) {
	cause := errors.New("entry accounting unavailable")
	for _, kind := range []string{"capacity", "negative charge", "accounting", "negative name", "name mismatch", "producer"} {
		t.Run(kind, func(t *testing.T) {
			result, err := newWindowsListResult(2, 0, func(i int, _ int64, _ windowsBasicAttr) (int64, error) {
				if i == 1 {
					switch kind {
					case "negative charge":
						return -1, nil
					case "accounting":
						return 0, cause
					case "capacity":
						return 3, nil
					}
				}
				return 1, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = result.Add(windowsEntry{Name: "a"}); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "negative name":
				_, err = result.Reserve(-1, windowsBasicAttr{})
			case "name mismatch":
				var r *windowsListReservation
				r, err = result.Reserve(2, windowsBasicAttr{})
				if err == nil {
					err = r.Commit("x")
				}
			case "producer":
				err = result.Fail(cause)
			default:
				err = result.Add(windowsEntry{Name: "b"})
			}
			if err == nil {
				t.Fatal("failure accepted")
			}
			first := err
			if entries, err := result.Entries(); entries != nil || err != first {
				t.Fatalf("prefix=%v error=%v expected=%v", entries, err, first)
			}
			if err := result.Fail(errors.New("later")); err != first {
				t.Fatal("first error lost")
			}
			if _, err := result.Reserve(1, windowsBasicAttr{}); err != first {
				t.Fatal(err)
			}
		})
	}
}

func TestDirectoryResultRejectsInvalidConstruction(t *testing.T) {
	charge := func(int, int64, windowsBasicAttr) (int64, error) { return 0, nil }
	for _, tt := range []struct {
		max, fixed int64
		charge     func(int, int64, windowsBasicAttr) (int64, error)
	}{{-1, 0, charge}, {1, -1, charge}, {1, 2, charge}, {1, 0, nil}} {
		if r, err := newWindowsListResult(tt.max, tt.fixed, tt.charge); r != nil || err == nil {
			t.Fatal(r, err)
		}
	}
	var result *windowsListResult
	if result.MaxBytes() != 0 {
		t.Fatal("nil byte bound")
	}
	if _, err := result.Reserve(0, windowsBasicAttr{}); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	if err := result.Fail(syscall.EIO); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	var reservation *windowsListReservation
	if err := reservation.Commit(""); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	result, err := newWindowsListResult(0, 0, charge)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil || entries == nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
	if err := result.Fail(nil); err != nil {
		t.Fatal(err)
	}
	reservation, err = result.Reserve(0, windowsBasicAttr{})
	if err != nil {
		t.Fatal(err)
	}
	result.Fail(syscall.EIO)
	if err := reservation.Commit(""); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
}
