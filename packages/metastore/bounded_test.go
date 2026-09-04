package metastore_test

import (
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func TestChangeResultStopsBeforeAChangeThatBelongsInTheNextPage(t *testing.T) {
	result, err := metastore.NewChangeResult(5, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
		return lengths.Name, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	first, fits, err := result.Reserve(metastore.Change{}, metastore.ChangePayloadLengths{Name: 4})
	if err != nil || !fits {
		t.Fatalf("reserve first change: fits=%v err=%v", fits, err)
	}
	if err := first.Commit([]byte("four"), nil, ""); err != nil {
		t.Fatal(err)
	}
	if reservation, fits, err := result.Reserve(metastore.Change{}, metastore.ChangePayloadLengths{Name: 2}); err != nil || fits || reservation != nil {
		t.Fatalf("reserve next-page change: reservation=%v fits=%v err=%v", reservation, fits, err)
	}
	changes, err := result.Changes()
	if err != nil || len(changes) != 1 || string(changes[0].Name) != "four" {
		t.Fatalf("completed page = %+v, %v", changes, err)
	}
}

func TestOversizedChangeInvalidatesEveryRetainedChange(t *testing.T) {
	result, err := metastore.NewChangeResult(4, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
		return lengths.Name, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	reservation, _, err := result.Reserve(metastore.Change{}, metastore.ChangePayloadLengths{Name: 1})
	if err != nil || reservation.Commit([]byte("a"), nil, "") != nil {
		t.Fatalf("retain first change: %v", err)
	}
	if _, _, err := result.Reserve(metastore.Change{}, metastore.ChangePayloadLengths{Name: 5}); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized change returned %v, want EFBIG", err)
	}
	if changes, err := result.Changes(); !errors.Is(err, syscall.EFBIG) || changes != nil {
		t.Fatalf("oversized change exposed partial page %+v, %v", changes, err)
	}
}

func TestChangeResultOwnsChargedPayloadAndTimeInstants(t *testing.T) {
	result, err := metastore.NewChangeResult(16, 0, func(_ int, _ metastore.Change, _ metastore.ChangePayloadLengths) (int64, error) {
		return 3, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	location := time.FixedZone(strings.Repeat("location", 1<<17), 3600)
	instant := time.Date(2026, time.September, 4, 12, 34, 56, 789, location)
	meta := metastore.Change{From: &metastore.Location{}, Node: &metastore.Node{AccessTime: instant, ModTime: instant}}
	reservation, fits, err := result.Reserve(meta, metastore.ChangePayloadLengths{Name: 1, FromName: 1, Content: 1})
	if err != nil || !fits {
		t.Fatalf("reserve: fits=%v err=%v", fits, err)
	}
	large := make([]byte, 1<<20)
	large[len(large)-3], large[len(large)-2], large[len(large)-1] = 'n', 'f', 'c'
	name, fromName := large[len(large)-3:len(large)-2], large[len(large)-2:len(large)-1]
	contentBacking := strings.Repeat("unretained", 1<<17) + "c"
	content := metastore.Key(contentBacking[len(contentBacking)-1:])
	if err := reservation.Commit(name, fromName, content); err != nil {
		t.Fatal(err)
	}
	changes, err := result.Changes()
	if err != nil {
		t.Fatal(err)
	}
	got := changes[0]
	if unsafe.SliceData(got.Name) == unsafe.SliceData(name) || unsafe.SliceData(got.From.Name) == unsafe.SliceData(fromName) {
		t.Fatal("retained change names alias caller-owned backing storage")
	}
	if unsafe.StringData(string(got.Node.Content)) == unsafe.StringData(string(content)) {
		t.Fatal("retained content key aliases caller-owned backing storage")
	}
	if got.Node.AccessTime.Location() != time.UTC || got.Node.ModTime.Location() != time.UTC {
		t.Fatal("retained change times keep caller-owned locations")
	}
	if !got.Node.AccessTime.Equal(instant) || !got.Node.ModTime.Equal(instant) {
		t.Fatal("UTC normalization changed the retained instants")
	}
}

func TestRowResultRequiresCommitAndOwnsChargedPayload(t *testing.T) {
	result, err := metastore.NewRowResult(8, 1, func(_ int, _ metastore.Row, lengths metastore.RowPayloadLengths) (int64, error) {
		return lengths.Name + lengths.Content, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	location := time.FixedZone(strings.Repeat("row-location", 1<<16), -3600)
	instant := time.Date(2026, time.September, 4, 1, 2, 3, 4, location)
	reservation, fits, err := result.Reserve(
		metastore.Row{Node: metastore.Node{AccessTime: instant, ModTime: instant}},
		metastore.RowPayloadLengths{Name: 1, Content: 1},
	)
	if err != nil || !fits {
		t.Fatalf("reserve: fits=%v err=%v", fits, err)
	}
	if rows, err := result.Rows(); err == nil || rows != nil {
		t.Fatalf("pending reservation exposed rows %+v, %v", rows, err)
	}
	backing := make([]byte, 1<<20)
	backing[len(backing)-2], backing[len(backing)-1] = 'n', 'c'
	name := backing[len(backing)-2 : len(backing)-1]
	contentBacking := strings.Repeat("unretained", 1<<17) + "c"
	content := metastore.Key(contentBacking[len(contentBacking)-1:])
	if err := reservation.Commit(name, content); err != nil {
		t.Fatal(err)
	}
	rows, err := result.Rows()
	if err != nil {
		t.Fatal(err)
	}
	if unsafe.SliceData(rows[0].Name) == unsafe.SliceData(name) {
		t.Fatal("retained row name aliases caller-owned backing storage")
	}
	if unsafe.StringData(string(rows[0].Node.Content)) == unsafe.StringData(string(content)) {
		t.Fatal("retained row content aliases caller-owned backing storage")
	}
	if rows[0].Node.AccessTime.Location() != time.UTC || rows[0].Node.ModTime.Location() != time.UTC {
		t.Fatal("retained row times keep caller-owned locations")
	}
	if !rows[0].Node.AccessTime.Equal(instant) || !rows[0].Node.ModTime.Equal(instant) {
		t.Fatal("UTC normalization changed the retained row instants")
	}
}

func TestReservationsDoNotRetainZeroLengthViewsOfLargeBackingStorage(t *testing.T) {
	largeBytes := make([]byte, 1<<20)
	zeroBytes := largeBytes[:0]
	largeString := strings.Repeat("backing", 1<<17)
	zeroString := metastore.Key(largeString[:0])

	changeResult, err := metastore.NewChangeResult(1, 0, func(_ int, change metastore.Change, _ metastore.ChangePayloadLengths) (int64, error) {
		if change.Name != nil && unsafe.SliceData(change.Name) == unsafe.SliceData(zeroBytes) {
			t.Fatal("change reservation retained a zero-length destination view")
		}
		if change.From.Name != nil && unsafe.SliceData(change.From.Name) == unsafe.SliceData(zeroBytes) {
			t.Fatal("change reservation retained a zero-length source view")
		}
		if unsafe.StringData(string(change.Node.Content)) == unsafe.StringData(string(zeroString)) {
			t.Fatal("change reservation retained a zero-length content view")
		}
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, fits, err := changeResult.Reserve(metastore.Change{
		Name: zeroBytes, From: &metastore.Location{Name: zeroBytes}, Node: &metastore.Node{Content: zeroString},
	}, metastore.ChangePayloadLengths{}); err != nil || !fits {
		t.Fatalf("change reserve: fits=%v err=%v", fits, err)
	}

	rowResult, err := metastore.NewRowResult(1, 0, func(_ int, row metastore.Row, _ metastore.RowPayloadLengths) (int64, error) {
		if row.Name != nil && unsafe.SliceData(row.Name) == unsafe.SliceData(zeroBytes) {
			t.Fatal("row reservation retained a zero-length name view")
		}
		if unsafe.StringData(string(row.Node.Content)) == unsafe.StringData(string(zeroString)) {
			t.Fatal("row reservation retained a zero-length content view")
		}
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, fits, err := rowResult.Reserve(metastore.Row{
		Name: zeroBytes, Node: metastore.Node{Content: zeroString},
	}, metastore.RowPayloadLengths{}); err != nil || !fits {
		t.Fatalf("row reserve: fits=%v err=%v", fits, err)
	}
}
