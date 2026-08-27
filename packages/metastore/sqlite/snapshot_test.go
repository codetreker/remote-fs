package sqlite_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

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
// namespace on the joined node instead of on the entry gives `SCAN n` with a sort without
// statistics and a skip-scan of the entry key with a sort with them — one query, one tree, two
// plans. Asserting only the analysed one would leave the shipped planner unguarded, and it was
// the statistics-dependence of the alternatives that decided this key in the first place.
func TestAPictureIsPagedByRangeRatherThanByScanningAndSorting(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)

	// A second namespace in the same database, so the plan is chosen against a table that holds
	// more than one namespace's entries. That is the arrangement the entry table's key exists
	// for, and a database holding one namespace would not put the question.
	other := open(t, path, "elsewhere", 0)
	for i := range 50 {
		if err := other.Create(t.Context(), fmt.Sprintf("theirs%d", i)); err != nil {
			t.Fatal(err)
		}
		if err := store.Create(t.Context(), fmt.Sprintf("mine%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	db := raw(t, path)
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

// assertPagePlan reads the plan for one page and holds it to the shape a range gives.
//
// Two phrases are what the assertions turn on. SCAN says a table is being read end to end
// rather than sought into, so a page is paying for rows it will discard. USE TEMP B-TREE FOR
// ORDER BY says the rows are being sorted after they are found, and since a page sorts
// everything it might return before taking its slice, that sort is repeated for every page of
// the picture — which is the quadratic term.
func assertPagePlan(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`EXPLAIN QUERY PLAN `+sqlite.PageQuery, 1, int64(0), []byte{}, 1024)
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

	// The entry table is sought into by its primary key, using both the namespace and the
	// cursor. Either one missing from the range is a page that reads rows it will throw away.
	if !strings.Contains(whole, "SEARCH e USING PRIMARY KEY (namespace=? AND (parent,name)>(?,?))") {
		t.Fatalf("a page does not seek the entry table by namespace and cursor together; the plan is: %s", whole)
	}
	for _, refused := range []string{"SCAN", "TEMP B-TREE"} {
		if strings.Contains(whole, refused) {
			t.Fatalf("a page plans a %s, so its cost grows with the whole table rather than with the page; the plan is: %s",
				refused, whole)
		}
	}
}

// The plan above is only worth asserting if the rows it produces are still right, and the
// entry table's key now carries a namespace that has to agree with the node's. A picture must
// hold this namespace's tree and nothing of the one beside it.
func TestAPictureHoldsItsOwnNamespaceOnly(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	other := open(t, path, "elsewhere", 0)

	for i := range 20 {
		if err := other.Create(t.Context(), fmt.Sprintf("theirs%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Mkdir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if err := store.Create(t.Context(), fmt.Sprintf("d/mine%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	snap, _, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	held := map[string]bool{}
	for {
		page, done, err := snap.Next(t.Context(), 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range page {
			held[string(row.Name)] = true
		}
		if done {
			break
		}
	}
	// The root, d, and twenty names under it.
	if len(held) != 22 {
		t.Fatalf("the picture holds %d names, want the root, d and the twenty under it", len(held))
	}
	for name := range held {
		if strings.HasPrefix(name, "theirs") {
			t.Fatalf("the picture holds %q, which belongs to the namespace beside it", name)
		}
	}
	if !held["d"] || !held["mine19"] {
		t.Fatalf("the picture is missing part of its own tree: %v", held)
	}
}

// Paging must not depend on the page size: a picture read one row at a time reaches exactly
// the picture read in one page. The cursor is what decides this, and a cursor SQLite declines
// to use as a range still returns the right rows, so only a comparison catches a mistake here.
func TestAPictureIsTheSameHoweverItIsPaged(t *testing.T) {
	store := open(t, database(t), "workspace", 0)
	if err := store.Mkdir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	for i := range 30 {
		if err := store.Create(t.Context(), fmt.Sprintf("d/f%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	read := func(limit int) []metastore.Row {
		snap, _, err := store.Snapshot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer snap.Close()
		var all []metastore.Row
		for {
			page, done, err := snap.Next(t.Context(), limit)
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, page...)
			if done {
				return all
			}
		}
	}

	whole := read(1000)
	if len(whole) != 32 {
		t.Fatalf("the picture holds %d rows, want the root, d and the thirty under it", len(whole))
	}
	for _, limit := range []int{1, 2, 7, 31} {
		paged := read(limit)
		if len(paged) != len(whole) {
			t.Fatalf("pages of %d reach %d rows, want the %d one page holds", limit, len(paged), len(whole))
		}
		for i := range whole {
			if paged[i].Parent != whole[i].Parent || string(paged[i].Name) != string(whole[i].Name) ||
				paged[i].Node.ID != whole[i].Node.ID {
				t.Fatalf("pages of %d differ at row %d: %d/%q/%d against %d/%q/%d",
					limit, i, paged[i].Parent, paged[i].Name, paged[i].Node.ID,
					whole[i].Parent, whole[i].Name, whole[i].Node.ID)
			}
		}
	}
}
