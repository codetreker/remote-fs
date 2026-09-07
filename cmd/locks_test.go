package cmd_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestServerBinariesEnforceFileLocks(t *testing.T) {
	for _, mode := range []string{"directory", "quota-directory", "local-store", "azure-blob"} {
		t.Run(mode, func(t *testing.T) {
			args := binaryLockNamespace(t, mode)
			server := startServerBinary(t, append(args, "-initialize-lock-state")...)
			remote := dialLockServer(t, server)
			for name, content := range map[string]string{"artifact": "original", "other": "unrelated"} {
				if err := remote.Write(t.Context(), name, []byte(content)); err != nil {
					t.Fatalf("seed %s: %v", name, err)
				}
			}

			first := binaryLockOwner(t, remote, "first")
			second := binaryLockOwner(t, remote, "second")
			shared := binaryAcquire(t, remote, first, "stable", "artifact", locking.Shared, 10*time.Second)
			otherShared := binaryAcquire(t, remote, second, "also-stable", "artifact", locking.Shared, 10*time.Second)
			assertBinaryLockRead(t, remote, "artifact", "original")
			assertBinaryMutationsConflict(t, remote)
			stable := binaryScope(t, remote, first, shared)
			if err := stable.Write(t.Context(), "artifact", []byte("forbidden")); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("shared owner's write returned %v, want EBUSY", err)
			}
			if _, err := remote.Release(t.Context(), first, shared); err != nil {
				t.Fatalf("release first shared grant: %v", err)
			}
			if _, err := remote.Release(t.Context(), second, otherShared); err != nil {
				t.Fatalf("release second shared grant: %v", err)
			}

			exclusive := binaryAcquire(t, remote, first, "exclusive", "artifact", locking.Exclusive, 10*time.Second)
			writer := binaryScope(t, remote, first, exclusive)
			assertBinaryMutationsConflict(t, remote)
			assertBinaryLockRead(t, remote, "artifact", "original")
			if err := writer.Write(t.Context(), "artifact", []byte("authorized")); err != nil {
				t.Fatalf("exclusive owner's write: %v", err)
			}
			if err := writer.Rename(t.Context(), "artifact", "renamed"); err != nil {
				t.Fatalf("rename with exclusive grant: %v", err)
			}
			resolved, err := remote.Resolve(t.Context(), first, "renamed")
			if err != nil {
				t.Fatalf("resolve renamed file: %v", err)
			}
			if resolved.ID != exclusive.Resource {
				t.Fatal("rename changed the protected logical resource")
			}
			if err := writer.Write(t.Context(), "renamed", []byte("after rename")); err != nil {
				t.Fatalf("write renamed resource with original grant: %v", err)
			}
			if err := remote.Write(t.Context(), "renamed", []byte("forbidden")); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("anonymous write after rename returned %v, want EBUSY", err)
			}
			if err := writer.Write(t.Context(), "other", []byte("forbidden")); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("unrelated proof returned %v, want ESTALE", err)
			}
			if err := writer.Remove(t.Context(), "renamed"); err != nil {
				t.Fatalf("remove with exclusive grant: %v", err)
			}
			if err := remote.Write(t.Context(), "renamed", []byte("replacement")); err != nil {
				t.Fatalf("recreate retired name: %v", err)
			}
			if err := writer.Write(t.Context(), "renamed", []byte("forbidden")); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("proof for deleted resource returned %v, want ESTALE", err)
			}
			assertBinaryLockRead(t, remote, "renamed", "replacement")
			assertBinaryLockRead(t, remote, "other", "unrelated")
		})
	}
}

func TestServerBinaryPreservesLeaseProtectionAcrossRestart(t *testing.T) {
	for _, mode := range []string{"directory", "quota-directory", "local-store", "azure-blob"} {
		t.Run(mode, func(t *testing.T) {
			args := binaryLockNamespace(t, mode)
			first := startServerBinary(t, append(args, "-initialize-lock-state", "-lock-max-lease", "3s")...)
			remote := dialLockServer(t, first)
			if err := remote.Write(t.Context(), "artifact", []byte("protected")); err != nil {
				t.Fatalf("seed protected file: %v", err)
			}
			owner := binaryLockOwner(t, remote, "owner")
			grant := binaryAcquire(t, remote, owner, "survives-restart", "artifact", locking.Exclusive, 3*time.Second)
			if err := first.cmd.Process.Kill(); err != nil {
				t.Fatalf("kill first server: %v", err)
			}
			if err := first.wait(t); err == nil {
				t.Fatal("killed server exited successfully")
			}

			restarted := startServerBinary(t, append(args, "-lock-max-lease", "500ms")...)
			reopened := dialLockServer(t, restarted)
			status, err := reopened.Status(t.Context())
			if err != nil {
				t.Fatalf("query recovery status: %v", err)
			}
			if !status.Recovering || status.RecoveryRemainingMillis <= 500 {
				t.Fatalf("restarted with recovery=%t and %d ms remaining; the issued 3 s lease must survive a 500 ms configuration", status.Recovering, status.RecoveryRemainingMillis)
			}
			assertBinaryLockRead(t, reopened, "artifact", "protected")
			if err := reopened.Write(t.Context(), "artifact", []byte("too early")); !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("write during recovery returned %v, want EAGAIN", err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for status.Recovering {
				if time.Now().After(deadline) {
					t.Fatalf("recovery did not finish: %d ms remaining", status.RecoveryRemainingMillis)
				}
				time.Sleep(20 * time.Millisecond)
				status, err = reopened.Status(t.Context())
				if err != nil {
					t.Fatalf("query recovery progress: %v", err)
				}
			}
			if err := reopened.Write(t.Context(), "artifact", []byte("recovered")); err != nil {
				t.Fatalf("write after recovery: %v", err)
			}
			stale := binaryScope(t, reopened, owner, grant)
			if err := stale.Write(t.Context(), "artifact", []byte("stale")); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("old authority proof returned %v, want ESTALE", err)
			}
			if _, err := reopened.QueryAction(t.Context(), owner, "survives-restart"); locking.CodeOf(err) != locking.Retired {
				t.Fatalf("query old action returned %v, want retired", err)
			}
			assertBinaryLockRead(t, reopened, "artifact", "recovered")
		})
	}
}

func TestDirectoryServerBinaryRefusesAlternateLockState(t *testing.T) {
	directory, stateRoot := t.TempDir(), privateDirectory(t)
	first := startServerBinary(t,
		"-listen", "127.0.0.1:0", "-dir", directory,
		"-lock-state-root", stateRoot, "-initialize-lock-state")
	first.interrupt(t)
	if err := first.wait(t); err != nil {
		t.Fatalf("stop initialized directory server: %v", err)
	}
	copied := privateDirectory(t)
	if err := os.CopyFS(copied, os.DirFS(stateRoot)); err != nil {
		t.Fatalf("copy lock state fixture: %v", err)
	}
	// CopyFS creates writable files; preserve native modes so this reaches binding validation.
	if err := filepath.WalkDir(stateRoot, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(stateRoot, name)
		if err != nil {
			return err
		}
		return os.Chmod(filepath.Join(copied, relative), info.Mode().Perm())
	}); err != nil {
		t.Fatalf("preserve copied state permissions: %v", err)
	}
	for _, attempt := range []struct {
		name string
		args []string
	}{
		{"missing state root", nil},
		{"reinitialize ready state", []string{"-lock-state-root", stateRoot, "-initialize-lock-state"}},
		{"fresh state root", []string{"-lock-state-root", privateDirectory(t)}},
		{"fresh initialization", []string{"-lock-state-root", privateDirectory(t), "-initialize-lock-state"}},
		{"copied state root", []string{"-lock-state-root", copied}},
		{"copied initialization", []string{"-lock-state-root", copied, "-initialize-lock-state"}},
	} {
		t.Run(attempt.name, func(t *testing.T) {
			args := append([]string{"-listen", "127.0.0.1:0", "-dir", directory}, attempt.args...)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, serverBinary(t), args...).CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("alternate lock state did not fail promptly: %v", ctx.Err())
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatalf("alternate lock state returned %v, want unsuccessful process exit", err)
			}
			if strings.Contains(string(output), "serving") || strings.Contains(string(output), "panic:") {
				t.Fatalf("alternate lock state reached serving or crashed: %s", output)
			}
			if !strings.Contains(string(output), "lock") {
				t.Fatalf("alternate lock state failed without identifying lock state: %s", output)
			}
		})
	}
	reopened := startServerBinary(t,
		"-listen", "127.0.0.1:0", "-dir", directory, "-lock-state-root", stateRoot)
	if err := dialLockServer(t, reopened).Write(t.Context(), "artifact", []byte("original state")); err != nil {
		t.Fatalf("reopen original lock state after rejected alternatives: %v", err)
	}
}

func binaryLockNamespace(t *testing.T, mode string) []string {
	t.Helper()
	args := []string{"-listen", "127.0.0.1:0"}
	switch mode {
	case "directory", "quota-directory":
		args = append(args, "-dir", t.TempDir(), "-lock-state-root", privateDirectory(t))
		if mode == "quota-directory" {
			args = append(args, "-quota", "64K")
		}
	case "local-store":
		args = append(args, "-local-store", privateDirectory(t), "-workspace", "locks", "-quota", "8M")
	case "azure-blob":
		args = append(args, "-blob-container", binaryLockContainer(t),
			"-metastore", filepath.Join(privateDirectory(t), "namespace.db"), "-workspace", "locks")
	default:
		t.Fatalf("unknown server mode %q", mode)
	}
	return args
}

func dialLockServer(t *testing.T, server *runningServer) *httprest.Storage {
	t.Helper()
	remote, err := httprest.Dial(server.url, &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("dial lock server: %v", err)
	}
	return remote
}

func binaryLockOwner(t *testing.T, remote *httprest.Storage, request locking.RequestID) locking.OwnerRef {
	t.Helper()
	ticket, err := remote.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatalf("begin enrollment: %v", err)
	}
	session, err := remote.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatalf("open lock session: %v", err)
	}
	owner, err := remote.CreateOwner(t.Context(), session.ID, request)
	if err != nil {
		t.Fatalf("create lock owner: %v", err)
	}
	return owner.Ref
}

func binaryAcquire(t *testing.T, remote *httprest.Storage, owner locking.OwnerRef, request locking.RequestID, name string, mode locking.Mode, ttl time.Duration) locking.GrantRef {
	t.Helper()
	resource, err := remote.Resolve(t.Context(), owner, name)
	if err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	result, err := remote.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner, Request: request, Resource: resource, Mode: mode, TTL: ttl,
	})
	if err != nil {
		t.Fatalf("acquire %s on %s: %v", mode, name, err)
	}
	if !result.Recorded || result.Receipt.Outcome != locking.Granted || result.Grant == nil || result.Grant.State != locking.Active {
		t.Fatalf("acquire %s on %s: recorded=%t, outcome=%s", mode, name, result.Recorded, result.Receipt.Outcome)
	}
	return result.Grant.Ref
}

func binaryScope(t *testing.T, remote *httprest.Storage, owner locking.OwnerRef, grants ...locking.GrantRef) storage.BoundedStorage {
	t.Helper()
	scoped, err := remote.Scope(locking.MutationScope{Owner: owner, Grants: grants})
	if err != nil {
		t.Fatalf("construct mutation scope: %v", err)
	}
	return scoped
}

func assertBinaryLockRead(t *testing.T, remote storage.Storage, name, want string) {
	t.Helper()
	content, err := remote.Read(t.Context(), name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if string(content) != want {
		t.Fatalf("read %s = %q, want %q", name, content, want)
	}
}

func assertBinaryMutationsConflict(t *testing.T, remote storage.Storage) {
	t.Helper()
	mode := fs.FileMode(0o600)
	for _, mutation := range []struct {
		name string
		run  func() error
	}{
		{"write", func() error { return remote.Write(t.Context(), "artifact", []byte("forbidden")) }},
		{"setattr", func() error { return remote.SetAttr(t.Context(), "artifact", storage.AttrChange{Mode: &mode}) }},
		{"remove", func() error { return remote.Remove(t.Context(), "artifact") }},
		{"rename source", func() error { return remote.Rename(t.Context(), "artifact", "moved") }},
		{"rename replacement", func() error { return remote.Rename(t.Context(), "other", "artifact") }},
	} {
		if err := mutation.run(); !errors.Is(err, syscall.EBUSY) {
			t.Fatalf("anonymous %s returned %v, want EBUSY", mutation.name, err)
		}
	}
}

func binaryLockContainer(t *testing.T) string {
	t.Helper()
	endpoint := os.Getenv("RFS_AZURITE_URL")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:10000/devstoreaccount1"
	}
	// Azurite's published development account matches the object-store contract fixture.
	const key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	connection := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + key + ";BlobEndpoint=" + endpoint + ";"
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", connection)
	name := fmt.Sprintf("rfs-cmd-locks-%d", time.Now().UnixNano())
	client, err := container.NewClientFromConnectionString(connection, name, nil)
	if err != nil {
		t.Fatalf("configure Blob container: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := client.Create(ctx, nil); err != nil {
		t.Fatalf("create Blob container at %s: %v", endpoint, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := client.Delete(ctx, nil); err != nil {
			t.Errorf("delete Blob container: %v", err)
		}
	})
	return name
}
