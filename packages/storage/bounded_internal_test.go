package storage

import (
	"strings"
	"testing"
	"time"
)

func TestListReservationOwnsOnlyTimeInstantsBeforeCommit(t *testing.T) {
	result, err := NewListResult(1024, 0, func(_ int, nameBytes int64, _ Attr) (int64, error) {
		return nameBytes + 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	location := time.FixedZone(strings.Repeat("location", 1<<17), 3600)
	access := time.Date(2026, time.September, 4, 12, 34, 56, 789, location)
	modified := access.Add(time.Hour)
	reservation, err := result.Reserve(1, Attr{AccessTime: access, ModTime: modified})
	if err != nil {
		t.Fatal(err)
	}
	if reservation.attr.AccessTime.Location() != time.UTC || reservation.attr.ModTime.Location() != time.UTC {
		t.Fatalf("reservation keeps caller-owned locations: %v, %v",
			reservation.attr.AccessTime.Location(), reservation.attr.ModTime.Location())
	}
	if !reservation.attr.AccessTime.Equal(access) || !reservation.attr.ModTime.Equal(modified) {
		t.Fatalf("UTC normalization changed the reserved instants: %+v", reservation.attr)
	}
}
