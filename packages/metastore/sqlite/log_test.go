package sqlite_test

import (
	"database/sql"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

// Trimming takes the oldest entries, so it must not disturb where the next one lands.
//
// The window here is the smallest one the checks allow, so that the trim fires on nearly every
// write rather than after ten thousand of them.
func TestPositionsKeepIncreasingAcrossTrims(t *testing.T) {
	store := openUnder(t, database(t), "workspace", 0, sqlite.Window{Floor: 1, Cap: 2, Age: time.Hour})

	var last metastore.Position
	for i := range 40 {
		if err := store.Create(t.Context(), fmt.Sprintf("f%d", i)); err != nil {
			t.Fatal(err)
		}
		at, err := store.CommittedPosition(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if at <= last {
			t.Fatalf("write %d was recorded at position %d, which is not after the %d before it", i, at, last)
		}
		last = at
	}

	// The trim really did happen, so the run above was not a log that simply never filled: the
	// entries are far fewer than the eighty a name and its directory make.
	changes, retention, err := readChanges(t.Context(), store, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) > 4 {
		t.Fatalf("the log still holds %d entries under a cap of 2, so nothing was trimmed", len(changes))
	}
	if retention.Tail != last {
		t.Fatalf("the log's tail is %d and the last write was at %d", retention.Tail, last)
	}
	if retention.Oldest <= 1 {
		t.Fatalf("the log's oldest entry is at %d, so it never discarded its beginning", retention.Oldest)
	}
	// Every entry that survived is still ordered, and still below the tail.
	for i, c := range changes {
		if i > 0 && c.Position <= changes[i-1].Position {
			t.Fatalf("the surviving entries are at %d then %d", changes[i-1].Position, c.Position)
		}
		if c.Position > retention.Tail {
			t.Fatalf("an entry survives at position %d, past the tail at %d", c.Position, retention.Tail)
		}
	}
}

// The trap AUTOINCREMENT is in the schema for.
//
// Deleting a prefix, which is all the trim ever does, leaves SQLite's sequence alone under
// either declaration — so a case built on trimming would pass whether or not AUTOINCREMENT
// were there, and would be a guard that cannot fail. What can fail is an emptied table:
// without AUTOINCREMENT the next insert starts again at 1, and every position from 1 up is one
// a replica has already applied and will now discard in silence.
//
// The table is emptied here from underneath a live store, which is what a floor of zero, a
// namespace being dropped, or an operator clearing the log would each leave behind. The
// promise that a position is never reused belongs to the table, not to how the trim is tuned.
func TestPositionsAreNeverReusedAfterTheLogIsEmptied(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	for i := range 5 {
		if err := store.Create(t.Context(), fmt.Sprintf("f%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	issued, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if issued == 0 {
		t.Fatal("five writes were recorded at position 0")
	}

	emptied := raw(t, path)
	if _, err := emptied.Exec(`DELETE FROM changes`); err != nil {
		t.Fatal(err)
	}
	if err := emptied.Close(); err != nil {
		t.Fatal(err)
	}

	if err := store.Create(t.Context(), "after"); err != nil {
		t.Fatal(err)
	}
	next, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if next <= issued {
		t.Fatalf("the log had issued positions up to %d, emptied, and issued %d next; every replica that had applied that position would discard the event",
			issued, next)
	}
}

// A caller that fell out of the window is told which dimension pushed it out, because the two
// call for different actions: age says that caller was away too long, volume says the
// namespace changes faster than the log was configured to hold.
func TestFallingOutOfTheWindowNamesTheDimension(t *testing.T) {
	t.Run("volume", func(t *testing.T) {
		store := openUnder(t, database(t), "workspace", 0, sqlite.Window{Floor: 1, Cap: 2, Age: time.Hour})
		for i := range 20 {
			if err := store.Create(t.Context(), fmt.Sprintf("f%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		_, retention, err := readChanges(t.Context(), store, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if retention.Oldest <= 1 {
			t.Fatalf("a caller at position 0 is still inside a window whose oldest entry is %d", retention.Oldest)
		}
		if retention.TrimmedByAge {
			t.Fatal("the log reports it discarded entries for being old, but every one of them was seconds old")
		}
	})

	t.Run("age", func(t *testing.T) {
		// A window nothing outlives, and a cap far above what this case writes, so the only
		// dimension that can cut is age.
		store := openUnder(t, database(t), "workspace", 0,
			sqlite.Window{Floor: 1, Cap: 10000, Age: time.Nanosecond})
		for i := range 5 {
			if err := store.Create(t.Context(), fmt.Sprintf("f%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		_, retention, err := readChanges(t.Context(), store, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if retention.Oldest <= 1 {
			t.Fatalf("a caller at position 0 is still inside a window whose oldest entry is %d", retention.Oldest)
		}
		if !retention.TrimmedByAge {
			t.Fatal("the log discarded entries under a cap of ten thousand it never reached, and reports it was for volume")
		}
	})
}

// The floor wins over the age rule. A namespace that has been quiet for longer than the age
// bound keeps its last entries anyway, so that a brief absence from a namespace where nothing
// happened does not cost a full rebuild.
func TestTheFloorSurvivesAnAgeBoundNothingOutlives(t *testing.T) {
	const floor = 6
	store := openUnder(t, database(t), "workspace", 0,
		sqlite.Window{Floor: floor, Cap: 10000, Age: time.Nanosecond})
	for i := range 20 {
		if err := store.Create(t.Context(), fmt.Sprintf("f%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	changes, retention, err := readChanges(t.Context(), store, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != floor {
		t.Fatalf("the log holds %d entries under a floor of %d and an age bound nothing outlives", len(changes), floor)
	}
	if changes[len(changes)-1].Position != retention.Tail {
		t.Fatalf("the newest entry is at %d and the tail is at %d; the trim took the wrong end",
			changes[len(changes)-1].Position, retention.Tail)
	}
}

// The tail is read from what the tree recorded, not from the entries the log still holds, and
// that separation is what keeps "you are caught up" and "you missed everything" apart. A caller
// arriving at position 0 against a log that holds nothing must be able to tell a namespace
// nobody has written to from one whose entries are gone.
//
// The trim here cannot reach that state — a floor of at least one always keeps the newest entry
// — so the entries are removed from underneath a live store, which is what a maintenance
// deletion or a log kept somewhere the tree's transaction does not reach would leave behind.
func TestTheTailOutlivesTheEntriesItCounted(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)

	// A namespace nobody has written to: nothing held, nothing recorded, and a caller at 0 is
	// caught up rather than behind.
	_, fresh, err := readChanges(t.Context(), store, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Oldest != 0 || fresh.Tail != 0 {
		t.Fatalf("a namespace nobody has written to holds %+v, want an oldest and a tail of 0", fresh)
	}

	for i := range 5 {
		if err := store.Create(t.Context(), fmt.Sprintf("f%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_, filled, err := readChanges(t.Context(), store, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if filled.Tail == 0 {
		t.Fatal("five writes left the tail at 0")
	}

	emptied := raw(t, path)
	if _, err := emptied.Exec(`DELETE FROM changes`); err != nil {
		t.Fatal(err)
	}
	if err := emptied.Close(); err != nil {
		t.Fatal(err)
	}

	changes, lost, err := readChanges(t.Context(), store, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Fatalf("an emptied log offered %d changes", len(changes))
	}
	if lost.Oldest != 0 {
		t.Fatalf("an emptied log reports its oldest entry at %d, want 0", lost.Oldest)
	}
	if lost.Tail != filled.Tail {
		t.Fatalf("an emptied log reports a tail of %d, want the %d the tree was changed at; a caller at 0 would read this as being caught up",
			lost.Tail, filled.Tail)
	}
}

// Startup refuses a log whose committed tail is missing. The tree and log share one
// transaction, so this state is corruption rather than a crash boundary that can be repaired.
func TestALogMissingItsTailIsRefused(t *testing.T) {
	path := database(t)
	first, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if err := first.Create(t.Context(), fmt.Sprintf("f%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	before, err := first.Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	// A restart on its own is not a discontinuity: the log is the same log, so every replica
	// resumes rather than rebuilding. Without this the case below would pass for a store that
	// simply minted a new incarnation every time it opened.
	restarted := open(t, path, "workspace", 0)
	unchanged, err := restarted.Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged != before {
		t.Fatalf("reopening the database moved the incarnation from %q to %q, so every replica would rebuild after every restart",
			before, unchanged)
	}

	tampered := raw(t, path)
	if _, err := tampered.Exec(`DELETE FROM changes`); err != nil {
		t.Fatal(err)
	}
	if err := tampered.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err == nil {
		reopened.Close()
		t.Fatal("opening a log whose committed tail is missing succeeded")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening a log whose committed tail is missing: %v, want EIO", err)
	}

	db := raw(t, path)
	defer db.Close()
	var after string
	if err := db.QueryRow(`SELECT incarnation FROM logs`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != string(before) {
		t.Fatalf("a refused open changed the incarnation from %q to %q", before, after)
	}
}

// The numbers the kinds are stored as are part of the schema, and they come from an iota in
// another package. Reordering that iota is an edit nobody would think to check this file for,
// and the damage would be a stored log whose Created rows read back as Removed against
// replicas that had already applied them.
func TestTheStoredKindsAreTheOnesTheSchemaPromises(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	if err := store.Create(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	if err := store.Rename(t.Context(), "f", "g"); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(t.Context(), "g"); err != nil {
		t.Fatal(err)
	}

	db := raw(t, path)
	rows, err := db.Query(`SELECT kind FROM changes ORDER BY position`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var stored []int
	for rows.Next() {
		var kind int
		if err := rows.Scan(&kind); err != nil {
			t.Fatal(err)
		}
		stored = append(stored, kind)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	// Created, the root modified; renamed, the root modified; removed, the root modified.
	want := []int{0, 2, 3, 2, 1, 2}
	if len(stored) != len(want) {
		t.Fatalf("the log holds kinds %v, want %v", stored, want)
	}
	for i := range want {
		if stored[i] != want[i] {
			t.Fatalf("the log holds kinds %v, want %v", stored, want)
		}
	}
}

// Renaming a directory is one row for the move plus one for the directory it moved within,
// whose modification time changed. Nothing beneath it is touched, which is the property that
// makes a branch switch cheap for every replica.
func TestRenamingADirectoryIsTwoRows(t *testing.T) {
	store := open(t, database(t), "workspace", 0)
	for _, path := range []string{"a", "a/b", "a/b/c"} {
		if err := store.Mkdir(t.Context(), path); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"a/b/c/deep", "a/shallow"} {
		if err := store.Create(t.Context(), path); err != nil {
			t.Fatal(err)
		}
	}

	before, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Rename(t.Context(), "a", "moved"); err != nil {
		t.Fatal(err)
	}
	changes, _, err := readChanges(t.Context(), store, before, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 {
		t.Fatalf("renaming a directory holding four nodes recorded %d changes, want the move and the root it moved within",
			len(changes))
	}
	if changes[0].Kind != metastore.Renamed || changes[1].Kind != metastore.Modified {
		t.Fatalf("renaming a directory recorded %v then %v, want a rename and the parent's modification",
			changes[0].Kind, changes[1].Kind)
	}
}

// A database holds several namespaces and the log rows carry which one they belong to. The
// sequence is shared, so a namespace's positions have gaps in them — nothing above compares
// positions for adjacency, only for order.
func TestEachNamespaceHasItsOwnLog(t *testing.T) {
	path := database(t)
	first := open(t, path, "first", 0)
	second := open(t, path, "second", 0)

	if err := first.Create(t.Context(), "mine"); err != nil {
		t.Fatal(err)
	}
	if err := second.Create(t.Context(), "theirs"); err != nil {
		t.Fatal(err)
	}
	if err := first.Create(t.Context(), "mine-too"); err != nil {
		t.Fatal(err)
	}

	mine, _, err := readChanges(t.Context(), first, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	theirs, _, err := readChanges(t.Context(), second, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(mine) != 4 {
		t.Fatalf("the first namespace recorded %d changes for two files, want four", len(mine))
	}
	if len(theirs) != 2 {
		t.Fatalf("the second namespace recorded %d changes for one file, want two", len(theirs))
	}
	// The second namespace's writes are in between the first's, so the first's positions are
	// not consecutive. A caller may only compare them.
	if mine[len(mine)-1].Position-mine[0].Position < 4 {
		t.Fatalf("the first namespace's positions run %d..%d with no room for the other namespace's",
			mine[0].Position, mine[len(mine)-1].Position)
	}

	// Two logs, two incarnations. A replica that mistook one for the other would be told it
	// could resume against history belonging to a namespace it has never read.
	one, err := first.Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	other, err := second.Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if one == other {
		t.Fatalf("both namespaces call their history %q", one)
	}
}

func TestOpenRefusesAWindowNothingCouldBeHeldTo(t *testing.T) {
	for _, window := range []sqlite.Window{
		{Floor: 0, Cap: 10, Age: time.Minute},
		{Floor: -1, Cap: 10, Age: time.Minute},
		{Floor: 10, Cap: 9, Age: time.Minute},
		{Floor: 1, Cap: 10, Age: 0},
		{Floor: 1, Cap: 10, Age: -time.Minute},
	} {
		store, err := sqlite.Open(t.Context(), database(t), "workspace", 0, window)
		if !errors.Is(err, syscall.EINVAL) {
			if err == nil {
				store.Close()
			}
			t.Fatalf("opening under %+v: %v, want EINVAL", window, err)
		}
	}
}

// raw opens the database file directly, for the cases that have to look at what is stored or
// damage it in a way nothing above this package could.
func raw(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening %s directly: %v", path, err)
	}
	return db
}

// Every question the log answers goes to the database, so every one of them has a way to fail
// that is not a missing file. A closed store is the cheapest way to reach it, and the answer
// has to be EIO: reporting a database we cannot read as "that is not there" is the answer
// R-ERR-2 forbids above every other, and a replica would act on it.
func TestTheLogReportsADatabaseItCannotReach(t *testing.T) {
	store, err := sqlite.Open(t.Context(), database(t), "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	for name, ask := range map[string]func() error{
		"since": func() error {
			_, _, err := readChanges(t.Context(), store, 0, 10)
			return err
		},
		"incarnation": func() error {
			_, err := store.Incarnation(t.Context(), 1024)
			return err
		},
		"committed position": func() error {
			_, err := store.CommittedPosition(t.Context())
			return err
		},
		"snapshot": func() error {
			snap, _, err := store.Snapshot(t.Context())
			if err == nil {
				snap.Close()
			}
			return err
		},
	} {
		err := ask()
		if err == nil {
			t.Fatalf("%s answered from a closed database, want a failure", name)
		}
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("%s from a closed database: %v, want EIO", name, err)
		}
		if errors.Is(err, syscall.ENOENT) {
			t.Fatalf("%s from a closed database reports something as missing: %v", name, err)
		}
	}
}
