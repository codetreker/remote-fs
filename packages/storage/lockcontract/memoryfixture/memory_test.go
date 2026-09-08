package memoryfixture_test

import (
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

func TestSQLiteObjectNamespaceLockContract(t *testing.T) {
	lockcontract.Run(t, func(t *testing.T, options locking.Options) lockcontract.Fixture {
		_, backing := memoryfixture.New(t, "contract", 0, options)
		facade, err := locked.New(backing)
		if err != nil {
			t.Fatal(err)
		}
		return lockcontract.Fixture{Storage: facade, Locks: facade.LockService(), Scope: facade.Scope}
	})
}
