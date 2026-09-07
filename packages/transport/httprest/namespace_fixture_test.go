package httprest_test

import (
	"os"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
)

func pairedDirectory(t *testing.T, root string) (*localdir.Storage, error) {
	t.Helper()
	return pairedDirectoryWithLocks(t, root, locking.DefaultOptions())
}

func pairedDirectoryWithLocks(t *testing.T, root string, options locking.Options) (*localdir.Storage, error) {
	t.Helper()
	config := localdir.Config{
		Root:      root,
		StateRoot: t.TempDir(),
		Locks:     options,
		Limits:    localdir.DefaultLimits(),
	}
	if err := os.Chmod(config.StateRoot, 0o700); err != nil {
		return nil, err
	}
	if err := localdir.Init(t.Context(), config); err != nil {
		return nil, err
	}
	backend, err := localdir.Open(t.Context(), config)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close directory: %v", err)
		}
	})
	return backend, nil
}

func failingStorage(t *testing.T, err error) failing {
	t.Helper()
	backend, openErr := pairedDirectory(t, t.TempDir())
	if openErr != nil {
		t.Fatal(openErr)
	}
	return failing{Backend: backend, err: err}
}
