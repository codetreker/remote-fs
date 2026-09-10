package httprest_test

import (
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func volumeFixture(t *testing.T) *objectstore.Storage {
	t.Helper()
	return volumeFixtureWithLocks(t, locking.DefaultOptions())
}

func volumeFixtureWithLocks(t *testing.T, options locking.Options) *objectstore.Storage {
	t.Helper()
	_, backend := memoryfixture.New(t, "transport", 1<<30, options)
	return backend
}

func failingStorage(t *testing.T, err error) failing {
	t.Helper()
	return failing{Backend: volumeFixture(t), err: err}
}
