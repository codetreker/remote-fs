package httprest_test

import (
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func namespaceFixture(t *testing.T) *objectstore.Storage {
	t.Helper()
	return namespaceFixtureWithLocks(t, locking.DefaultOptions())
}

func namespaceFixtureWithLocks(t *testing.T, options locking.Options) *objectstore.Storage {
	t.Helper()
	_, backend := memoryfixture.New(t, "transport", 1<<30, options)
	return backend
}

func failingStorage(t *testing.T, err error) failing {
	t.Helper()
	return failing{Backend: namespaceFixture(t), err: err}
}
