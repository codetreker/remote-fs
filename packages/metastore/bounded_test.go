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

func TestFailedPagesDiscardCommittedAndPendingResults(t *testing.T) {
	failure := errors.New("producer read failed")
	replacement := errors.New("cleanup failed")
	for _, pending := range []bool{false, true} {
		changes, err := metastore.NewChangeResult(10, 0, func(int, metastore.Change, metastore.ChangePayloadLengths) (int64, error) { return 1, nil })
		if err != nil {
			t.Fatal(err)
		}
		change, _, err := changes.Reserve(metastore.Change{}, metastore.ChangePayloadLengths{Name: 1})
		if err != nil {
			t.Fatal(err)
		}
		if !pending {
			if err := change.Commit([]byte("x"), nil, ""); err != nil {
				t.Fatal(err)
			}
		}
		if err := changes.Fail(nil); err != nil {
			t.Fatal(err)
		}
		if changes.Fail(failure) != failure || changes.Fail(replacement) != failure {
			t.Fatal("change page lost original failure")
		}
		if got, err := changes.Changes(); got != nil || err != failure {
			t.Fatalf("failed changes = %v, %v", got, err)
		}
		if got, fits, err := changes.Reserve(metastore.Change{}, metastore.ChangePayloadLengths{}); got != nil || fits || err != failure {
			t.Fatalf("failed reserve = %v, %v, %v", got, fits, err)
		}
		if pending && change.Commit([]byte("x"), nil, "") != failure {
			t.Fatal("pending change survived failure")
		}

		rows, err := metastore.NewRowResult(10, 0, func(int, metastore.Row, metastore.RowPayloadLengths) (int64, error) { return 1, nil })
		if err != nil {
			t.Fatal(err)
		}
		row, _, err := rows.Reserve(metastore.Row{}, metastore.RowPayloadLengths{Name: 1})
		if err != nil {
			t.Fatal(err)
		}
		if !pending {
			if err := row.Commit([]byte("x"), ""); err != nil {
				t.Fatal(err)
			}
		}
		if err := rows.Fail(nil); err != nil {
			t.Fatal(err)
		}
		if rows.Fail(failure) != failure || rows.Fail(replacement) != failure {
			t.Fatal("row page lost original failure")
		}
		if got, err := rows.Rows(); got != nil || err != failure {
			t.Fatalf("failed rows = %v, %v", got, err)
		}
		if got, fits, err := rows.Reserve(metastore.Row{}, metastore.RowPayloadLengths{}); got != nil || fits || err != failure {
			t.Fatalf("failed reserve = %v, %v, %v", got, fits, err)
		}
		if pending && row.Commit([]byte("x"), "") != failure {
			t.Fatal("pending row survived failure")
		}
	}
	var changes *metastore.ChangeResult
	var rows *metastore.RowResult
	if changes.Fail(failure) != failure || rows.Fail(failure) != failure {
		t.Fatal("nil result lost producer failure")
	}
}

func TestPageConstructorsRejectInvalidBoundsAndMissingCharges(t *testing.T) {
	for _, tc := range []struct {
		max, fixed int64
		want       error
	}{{-1, 0, syscall.EINVAL}, {1, -1, syscall.EFBIG}, {1, 2, syscall.EFBIG}} {
		if _, err := metastore.NewChangeResult(tc.max, tc.fixed, func(int, metastore.Change, metastore.ChangePayloadLengths) (int64, error) { return 0, nil }); !errors.Is(err, tc.want) {
			t.Errorf("change constructor = %v; want %v", err, tc.want)
		}
		if _, err := metastore.NewRowResult(tc.max, tc.fixed, func(int, metastore.Row, metastore.RowPayloadLengths) (int64, error) { return 0, nil }); !errors.Is(err, tc.want) {
			t.Errorf("row constructor = %v; want %v", err, tc.want)
		}
	}
	if _, err := metastore.NewChangeResult(1, 0, nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	if _, err := metastore.NewRowResult(1, 0, nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
}

func TestRowPageBoundsAndPayloadMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second int64
		want   error
	}{{"next page", 2, nil}, {"oversized", 4, syscall.EFBIG}, {"negative length", -1, syscall.EIO}} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := metastore.NewRowResult(4, 1, func(_ int, _ metastore.Row, l metastore.RowPayloadLengths) (int64, error) { return l.Name, nil })
			if err != nil {
				t.Fatal(err)
			}
			first, fits, err := result.Reserve(metastore.Row{}, metastore.RowPayloadLengths{Name: 3})
			if err != nil || !fits {
				t.Fatalf("first reserve = %v, %v", fits, err)
			}
			if err := first.Commit([]byte("one"), ""); err != nil {
				t.Fatal(err)
			}
			next, fits, err := result.Reserve(metastore.Row{}, metastore.RowPayloadLengths{Name: tc.second})
			if next != nil || fits || !errors.Is(err, tc.want) {
				t.Fatalf("next reserve = %v, %v, %v", next, fits, err)
			}
			rows, err := result.Rows()
			if tc.want == nil {
				if err != nil || len(rows) != 1 || string(rows[0].Name) != "one" {
					t.Fatalf("page = %v, %v", rows, err)
				}
			} else if rows != nil || !errors.Is(err, tc.want) {
				t.Fatalf("failed page = %v, %v", rows, err)
			}
		})
	}
	result, err := metastore.NewRowResult(4, 0, func(int, metastore.Row, metastore.RowPayloadLengths) (int64, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	reservation, _, err := result.Reserve(metastore.Row{}, metastore.RowPayloadLengths{Name: 1, Content: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.Commit([]byte("too long"), "k"); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if rows, err := result.Rows(); rows != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("mismatched payload exposed %v, %v", rows, err)
	}
}
