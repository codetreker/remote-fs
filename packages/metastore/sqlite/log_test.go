package sqlite

import (
	"math"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	_ "modernc.org/sqlite"
)

func TestSinceDoesNotAllocateFromAnUnboundedRequestedLimit(t *testing.T) {
	store, err := open(t.Context(), t.TempDir()+"/metastore.db", "workspace", "", 0, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	store.maxIntegrityRecords = math.MaxInt64 - 1
	t.Cleanup(func() { store.Close() })

	read := func(maxBytes int64) []metastore.Change {
		result, err := metastore.NewChangeResult(maxBytes, 0,
			func(_ int, _ metastore.Change, _ metastore.ChangePayloadLengths) (int64, error) {
				return 1, nil
			})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Since(t.Context(), 0, math.MaxInt, result); err != nil {
			t.Fatal(err)
		}
		changes, err := result.Changes()
		if err != nil {
			t.Fatal(err)
		}
		return changes
	}

	if changes := read(1); len(changes) != 0 {
		t.Fatalf("empty log returned %d changes", len(changes))
	}
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if changes := read(1); len(changes) != 1 {
		t.Fatalf("one-change result returned %d changes", len(changes))
	}
}
