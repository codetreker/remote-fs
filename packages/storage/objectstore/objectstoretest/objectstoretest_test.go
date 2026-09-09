package objectstoretest_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/objectstoretest"
)

func TestContractWithMemoryObjects(t *testing.T) {
	builds, closes := 0, 0
	t.Run("suite", func(t *testing.T) {
		objectstoretest.Run(t, func(*testing.T) objectstore.Objects {
			builds++
			return &countedObjects{Objects: memory.New(), onClose: func() { closes++ }}
		})
	})
	if builds == 0 || closes != builds {
		t.Fatalf("contract lifecycle: %d builds and %d closes; want nonzero builds and one close each", builds, closes)
	}
}

type countedObjects struct {
	objectstore.Objects
	onClose func()
}

func (o *countedObjects) Close() error {
	o.onClose()
	return o.Objects.Close()
}

func TestContractRejectsOverwrite(t *testing.T) {
	const marker = "RFS_OBJECT_CONTRACT_OVERWRITE_PROBE"
	if os.Getenv(marker) == "1" {
		objectstoretest.Run(t, func(*testing.T) objectstore.Objects {
			return &overwriteObjects{Objects: memory.New()}
		})
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), executable, "-test.timeout=20s", "-test.run=^TestContractRejectsOverwrite$/^put_is_create_only$")
	cmd.Env = append(os.Environ(), marker+"=1")
	output, err := cmd.CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
		t.Fatalf("broken contract probe exit = %v; want test failure: %s", err, output)
	}
	if !strings.Contains(string(output), "the second Put succeeded, want EEXIST") {
		t.Fatalf("probe failed outside the create-only assertion: %s", output)
	}
}

type overwriteObjects struct{ objectstore.Objects }

func (o *overwriteObjects) Put(ctx context.Context, key string, content []byte) ([]byte, error) {
	if err := o.Objects.Delete(ctx, key); err != nil {
		return nil, err
	}
	return o.Objects.Put(ctx, key, content)
}
