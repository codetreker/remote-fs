package integration_test

import (
	"fmt"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// The entry table's namespace must agree with the node's. A picture must hold this
// namespace's tree and nothing of the one beside it.
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
		page, done, err := readRows(t.Context(), snap, 3)
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
			page, done, err := readRows(t.Context(), snap, limit)
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
