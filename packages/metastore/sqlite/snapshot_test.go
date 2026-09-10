package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	_ "modernc.org/sqlite"
)

func TestSnapshotAfterAutomaticRollbackReturnsCancellation(t *testing.T) {
	store, err := Open(t.Context(), t.TempDir()+"/metastore.db", "workspace", 4096, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing store: %v", err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	snap, _, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitForReadRollback(t, store.snapshotRead)
	result, err := metastore.NewRowResult(1024, 0,
		func(_ int, _ metastore.Row, lengths metastore.RowPayloadLengths) (int64, error) {
			return 192 + lengths.Name + lengths.Content, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snap.Next(t.Context(), 1, result); storage.ErrnoOf(err) != syscall.EINTR ||
		!errors.Is(err, context.Canceled) || !errors.Is(err, sql.ErrTxDone) || errors.Is(err, syscall.EIO) {
		t.Fatalf("reading automatically rolled back snapshot with a new context = %v, want interruption", err)
	}
	if rows, err := result.Rows(); rows != nil || storage.ErrnoOf(err) != syscall.EINTR {
		t.Fatalf("canceled snapshot exposed rows %v with error %v", rows, err)
	}
	if err := snap.Close(); storage.ErrnoOf(err) != syscall.EINTR || errors.Is(err, syscall.EIO) {
		t.Fatalf("closing automatically rolled back snapshot = %v, want interruption", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("closing snapshot twice: %v", err)
	}
}

func TestSnapshotCancellationDoesNotHideIndependentFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	picture := &snapshot{ctx: ctx}
	fault := errors.New("query failed")
	for _, cause := range []error{
		errors.Join(sql.ErrTxDone, fault),
		errors.Join(fault, sql.ErrTxDone),
		fmt.Errorf("query: %w", sql.ErrTxDone),
	} {
		err := picture.readFailure(t.Context(), cause)
		if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, cause) {
			t.Fatalf("snapshot failure %v = %v, want EIO retaining original failure", cause, err)
		}
	}
	activePicture := &snapshot{ctx: t.Context()}
	err := activePicture.readFailure(ctx, sql.ErrTxDone)
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
		t.Fatalf("canceled page hid an unexpectedly completed live transaction: %v", err)
	}
}

func TestSnapshotPoolDoesNotBlockGeneralReaderPool(t *testing.T) {
	store, err := OpenWithOptions(
		t.Context(), t.TempDir()+"/metastore.db", "workspace", 0, Options{
			Window:                       DefaultWindow(),
			MaxReaderConnections:         1,
			MaxSnapshotReaderConnections: 1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	picture, _, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer picture.Close()

	waitContext, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		blocked, _, err := store.Snapshot(waitContext)
		if blocked != nil {
			err = errors.Join(err, blocked.Close())
		}
		result <- err
	}()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for store.snapshotRead.Stats().WaitCount == 0 {
		select {
		case <-deadline.C:
			t.Fatal("a second snapshot did not wait for the dedicated snapshot pool")
		default:
			runtime.Gosched()
		}
	}
	if _, err := store.Incarnation(t.Context(), 1024); err != nil {
		t.Fatalf("a saturated snapshot pool blocked a general log read: %v", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceling a snapshot pool waiter returned %v, want context.Canceled", err)
	}
}

// A picture pages the tree, and what a page costs is decided entirely by the plan SQLite
// chooses for one statement. This asserts that plan, both against a database that has been
// analysed and against one that has not.
//
// A plan rather than a clock, because the defect this guards against is not one a correct
// answer distinguishes: the query returned exactly the right rows when it took ten minutes
// and when it takes two seconds, so nothing about the rows can tell the two apart. A wall
// clock could, but a bound loose enough not to fail on a loaded machine is loose enough to
// pass a plan that is quadratic at the sizes a test can afford to build — and the quadratic
// term is invisible until the picture is large. The plan is the property itself, and it reads
// the same on every machine.
//
// Both worlds, because statistics decide plans and nothing in this package ever runs ANALYZE:
// a served database has no sqlite_stat1 at all, so the unanalysed plan is the one that ships,
// while a database somebody has analysed by hand is one this store must still serve well. The
// two are not the same planner. Measured against modernc.org/sqlite v1.57.0, filtering the
// volume on the joined node instead of on the entry gives `SCAN n` with a sort without
// statistics and a skip-scan of the entry key with a sort with them — one query, one tree, two
// plans. Asserting only the analysed one would leave the shipped planner unguarded, and it was
// the statistics-dependence of the alternatives that decided this key in the first place.
func TestAPictureIsPagedByRangeRatherThanByScanningAndSorting(t *testing.T) {
	path := snapshotPlanDatabase(t)
	store := snapshotPlanOpen(t, path, "workspace", 0)

	// A second volume in the same database, so the plan is chosen against a table that holds
	// more than one volume's entries. That is the arrangement the entry table's key exists
	// for, and a database holding one volume would not put the question.
	other := snapshotPlanOpen(t, path, "elsewhere", 0)
	for i := range 50 {
		if err := other.Create(t.Context(), fmt.Sprintf("theirs%d", i)); err != nil {
			t.Fatal(err)
		}
		if err := store.Create(t.Context(), fmt.Sprintf("mine%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	db := snapshotPlanRaw(t, path)
	// Unanalysed first, because ANALYZE cannot be undone within one database and that is the
	// order the two worlds exist in: every database starts without statistics, and the shipped
	// store never gives it any. Each is its own verdict, so neither hides the other's answer.
	t.Run("without statistics", func(t *testing.T) {
		assertPagePlan(t, db)
	})
	if _, err := db.Exec(`ANALYZE`); err != nil {
		t.Fatal(err)
	}
	t.Run("with statistics", func(t *testing.T) {
		assertPagePlan(t, db)
	})
}

// assertPagePlan holds the plan to the shape a range gives, at several points along the cursor.
//
// Two phrases are what the assertions turn on. SCAN says a table is being read end to end
// rather than sought into, so a page is paying for rows it will discard. USE TEMP B-TREE FOR
// ORDER BY says the rows are being sorted after they are found, and since a page sorts
// everything it might return before taking its slice, that sort is repeated for every page of
// the picture — which is the quadratic term.
//
// Several cursor positions rather than one, because the plan for a statement is not always the
// plan for a whole picture. modernc.org/sqlite v1.57.0 is built with SQLITE_ENABLE_STAT4, so
// on an analysed database SQLite plans from the bound values and re-plans when they move far
// enough. The statement this one replaced did exactly that: over twenty volumes of fifty
// thousand entries it planned the per-page sort near the start of the cursor and switched to
// driving from the entry key partway along, so a plan read at one position was not the plan
// that picture ran under. This statement gave one plan at every position tried. That is what
// makes a single reading of it trustworthy, and it is a property of this statement rather than
// a general one, so it is checked here instead of assumed.
func assertPagePlan(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, cursor := range []int64{0, 25, 60, 1000} {
		assertPagePlanAt(t, db, cursor)
	}
}

func assertPagePlanAt(t *testing.T, db *sql.DB, cursor int64) {
	t.Helper()
	rows, err := db.Query(`EXPLAIN QUERY PLAN `+pageQuery, 1, cursor, []byte{}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var plan []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(plan) == 0 {
		t.Fatal("the paging statement produced no plan at all, so nothing below was checked")
	}
	whole := strings.Join(plan, "; ")

	// The entry table is sought into by its primary key, using both the volume and the
	// cursor. Either one missing from the range is a page that reads rows it will throw away.
	if !strings.Contains(whole, "SEARCH e USING PRIMARY KEY (volume=? AND (parent,name)>(?,?))") {
		t.Fatalf("at cursor %d a page does not seek the entry table by volume and cursor together; the plan is: %s",
			cursor, whole)
	}
	for _, refused := range []string{"SCAN", "TEMP B-TREE"} {
		if strings.Contains(whole, refused) {
			t.Fatalf("at cursor %d a page plans a %s, so its cost grows with the whole table rather than with the page; the plan is: %s",
				cursor, refused, whole)
		}
	}
}

func snapshotPlanDatabase(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "metastore.db")
}

func snapshotPlanOpen(t *testing.T, path, volume string, allowance int64) *Store {
	t.Helper()
	store, err := Open(t.Context(), path, volume, allowance, DefaultWindow())
	if err != nil {
		t.Fatalf("opening %q in %s: %v", volume, path, err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	return store
}

func snapshotPlanRaw(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening %s directly: %v", path, err)
	}
	return db
}
