package lockcontract_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

func TestContractWithSQLiteAuthority(t *testing.T) {
	builds, cleanups := 0, 0
	t.Run("suite", func(t *testing.T) {
		lockcontract.Run(t, func(t *testing.T, options locking.Options) lockcontract.Fixture {
			builds++
			t.Cleanup(func() { cleanups++ })
			return sqliteFixture(t, options)
		})
	})
	if builds == 0 || cleanups != builds {
		t.Fatalf("contract lifecycle: %d builds and %d cleanups; want nonzero builds and one cleanup each", builds, cleanups)
	}
}

func sqliteFixture(t *testing.T, options locking.Options) lockcontract.Fixture {
	t.Helper()
	_, backing := memoryfixture.New(t, "contract", 0, options)
	facade, err := locked.New(backing)
	if err != nil {
		t.Fatal(err)
	}
	return lockcontract.Fixture{Storage: facade, Locks: facade.LockService(), Scope: facade.Scope}
}

func TestContractRejectsAnonymousMutation(t *testing.T) {
	const marker = "RFS_LOCK_CONTRACT_MUTATION_PROBE"
	if os.Getenv(marker) == "1" {
		lockcontract.Run(t, func(t *testing.T, options locking.Options) lockcontract.Fixture {
			fixture := sqliteFixture(t, options)
			fixture.Storage = &unprotectedObjects{BoundedStorage: fixture.Storage}
			return fixture
		})
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), executable, "-test.timeout=20s", "-test.run=^TestContractRejectsAnonymousMutation$/^shared_protection$")
	cmd.Env = append(os.Environ(), marker+"=1")
	output, err := cmd.CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
		t.Fatalf("broken contract probe exit = %v; want test failure: %s", err, output)
	}
	if !strings.Contains(string(output), "error = <nil>, want lock code conflict") {
		t.Fatalf("probe failed outside the anonymous-write assertion: %s", output)
	}
}

type unprotectedObjects struct{ storage.BoundedStorage }

func (s *unprotectedObjects) Write(ctx context.Context, path string, content []byte) error {
	if string(content) == "anonymous" {
		return nil
	}
	return s.BoundedStorage.Write(ctx, path, content)
}
