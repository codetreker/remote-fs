// Package memoryfixture constructs real SQLite volumes with volatile object contents
// and persistent lease evidence for integration tests.
package memoryfixture

import (
	"path/filepath"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

// New returns a borrowed metadata view and its owning object/metadata pair. Cleanup
// closes that pair before the native ownership and persistent lease evidence release.
func New(t *testing.T, volume string, allowance int64, options locking.Options) (*sqlite.Store, *objectstore.Storage) {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "volume.db"), Volume: volume,
		Allowance: allowance, SQLite: sqlite.DefaultOptions(), Locks: options, Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	backing := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := backing.Close(); err != nil {
			t.Errorf("closing volume: %v", err)
		}
	})
	return meta.Store, backing
}
