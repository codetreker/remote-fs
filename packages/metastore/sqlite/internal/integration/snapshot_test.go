package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	_ "modernc.org/sqlite"
)

func readRows(ctx context.Context, snap metastore.Snap, limit int) ([]metastore.Row, bool, error) {
	result, err := metastore.NewRowResult(64<<20, 0, func(_ int, _ metastore.Row, lengths metastore.RowPayloadLengths) (int64, error) {
		return 192 + lengths.Name + lengths.Content, nil
	})
	if err != nil {
		return nil, false, err
	}
	done, err := snap.Next(ctx, limit, result)
	if err != nil {
		return nil, false, err
	}
	rows, err := result.Rows()
	return rows, done, err
}

func TestSnapshotRefusesAnOversizedStoredNameBeforeExposingAPicture(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	damageDatabase(t, path, `UPDATE entries SET name=? WHERE name=CAST('file' AS BLOB)`, []byte(strings.Repeat("x", 1<<20)))
	snap, _, err := store.Snapshot(t.Context())
	if snap != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("oversized snapshot = %v, %v; want no picture and EIO", snap, err)
	}
}

func TestSnapshotRefusesLiveStorageClassCorruptionBeforeReturningAPicture(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	defer store.Close()
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	damageDatabase(t, path,
		`UPDATE entries SET name = 'text-name' WHERE volume = (SELECT id FROM volumes WHERE name = 'workspace')`)

	snap, _, err := store.Snapshot(t.Context())
	if err == nil {
		if snap != nil {
			snap.Close()
		}
		t.Fatal("Snapshot returned a picture of storage-class-corrupt entries")
	}
	if !errors.Is(err, syscall.EIO) || snap != nil {
		t.Fatalf("Snapshot returned (%v, %v), want nil and EIO", snap, err)
	}
}

// The entry table's volume must agree with the node's. A picture must hold this
// volume's tree and nothing of the one beside it.
func TestAPictureHoldsItsOwnVolumeOnly(t *testing.T) {
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
			t.Fatalf("the picture holds %q, which belongs to the volume beside it", name)
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
