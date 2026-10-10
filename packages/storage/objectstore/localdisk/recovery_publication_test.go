package localdisk

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const markerCrashEnvironment = "REMOTE_FS_MARKER_CRASH"

func TestRecoveryMarkerPublicationSurvivesSIGKILL(t *testing.T) {
	if child := os.Getenv(markerCrashEnvironment); child != "" {
		runRecoveryMarkerCrashChild(t, strings.SplitN(child, ":", 3))
		return
	}
	for _, operation := range []string{"put", "delete"} {
		for _, boundary := range []string{"empty", "file-sync", "publication-sync", "cleanup-sync", "effect"} {
			t.Run(operation+"/"+boundary, func(t *testing.T) {
				root := privateRoot(t)
				objects, err := Open(t.Context(), root, Options{})
				if err != nil {
					t.Fatal(err)
				}
				key := "crash-target"
				if operation == "delete" {
					if _, err := objects.Put(t.Context(), key, []byte("must survive preparation")); err != nil {
						t.Fatal(err)
					}
				} else {
					location, _ := locate(key)
					fd, _, err := objects.openShard(t.Context(), location, true)
					if err != nil {
						t.Fatal(err)
					}
					if err := unix.Close(fd); err != nil {
						t.Fatal(err)
					}
				}
				if err := objects.Close(); err != nil {
					t.Fatal(err)
				}
				killRecoveryMarkerChild(t, root, operation, boundary)
				reopened, err := Open(t.Context(), root, Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := reopened.Close(); err != nil {
						t.Error(err)
					}
				}()
				got, err := reopened.Get(t.Context(), key)
				if operation == "delete" && (boundary == "empty" || boundary == "file-sync") {
					if err != nil || string(got) != "must survive preparation" {
						t.Fatalf("unaccepted delete removed object: %q, %v", got, err)
					}
				} else if !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("recovered %s at %s = %q, %v; want absent", operation, boundary, got, err)
				}
				marker, _ := markerName(key, operation == "delete")
				for _, name := range []string{marker, markerPreparationPrefix + marker} {
					if _, err := os.Lstat(filepath.Join(root, objectsDirectory, name)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("recovery left %s: %v", name, err)
					}
				}
			})
		}
	}
}

func runRecoveryMarkerCrashChild(t *testing.T, child []string) {
	if len(child) != 3 {
		t.Fatal("invalid marker crash child arguments")
	}
	operation, boundary, root := child[0], child[1], child[2]
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := "crash-target"
	marker, _ := markerName(key, operation == "delete")
	preparation := markerPreparationPrefix + marker
	location, _ := locate(key)
	stop := func() {
		if _, err := io.WriteString(os.Stdout, "marker-boundary\n"); err != nil {
			t.Fatal(err)
		}
		select {}
	}
	originalStat := objects.ops.fstat
	objects.ops.fstat = func(fd int, st *unix.Stat_t) error {
		if err := originalStat(fd, st); err != nil {
			return err
		}
		if boundary == "empty" && st.Mode&unix.S_IFMT == unix.S_IFREG && st.Size == 0 {
			stop()
		}
		return nil
	}
	originalSync := objects.ops.fsync
	objects.ops.fsync = func(fd int) error {
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			return err
		}
		if boundary == "file-sync" && st.Mode&unix.S_IFMT == unix.S_IFREG && st.Size == recoveryMarkerBytes {
			stop()
		}
		if fd == objects.objectsFD {
			_, finalErr := os.Stat(filepath.Join(root, objectsDirectory, marker))
			_, preparationErr := os.Stat(filepath.Join(root, objectsDirectory, preparation))
			if finalErr == nil && ((boundary == "publication-sync" && preparationErr == nil) ||
				(boundary == "cleanup-sync" && errors.Is(preparationErr, os.ErrNotExist))) {
				stop()
			}
		}
		return originalSync(fd)
	}
	originalUnlink := objects.ops.unlinkat
	objects.ops.unlinkat = func(fd int, name string, flags int) error {
		if operation == "delete" && boundary == "effect" && name == location.final {
			stop()
		}
		return originalUnlink(fd, name, flags)
	}
	originalLink := objects.ops.linkat
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if operation == "put" && boundary == "effect" && old == location.staging {
			stop()
		}
		return originalLink(oldFD, old, newFD, new, flags)
	}
	if operation == "delete" {
		err = objects.Delete(t.Context(), key)
	} else {
		_, err = objects.Put(t.Context(), key, []byte("not acknowledged"))
	}
	t.Fatalf("mutation missed crash boundary: %v", err)
}

func killRecoveryMarkerChild(t *testing.T, root, operation, boundary string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryMarkerPublicationSurvivesSIGKILL$", "-test.timeout=15s")
	command.Env = append(os.Environ(), markerCrashEnvironment+"="+operation+":"+boundary+":"+root)
	command.Stderr = os.Stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "marker-boundary\n" {
		t.Fatalf("child did not reach %s/%s: %q, %v", operation, boundary, line, err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("child did not stop at SIGKILL boundary: %v", err)
	}
}

func TestRecoveryMarkerPublicationErrorsRetainRecoveryAuthority(t *testing.T) {
	for _, fault := range []string{"link-refused", "ambiguous-link", "publication-sync", "unlink-preparation", "cleanup-sync"} {
		t.Run(fault, func(t *testing.T) {
			root := privateRoot(t)
			objects, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			key := "publication-error"
			if _, err := objects.Put(t.Context(), key, []byte("original")); err != nil {
				t.Fatal(err)
			}
			marker, _ := markerName(key, true)
			preparation := markerPreparationPrefix + marker
			originalLink := objects.ops.linkat
			objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
				if new == marker {
					if fault == "link-refused" {
						return syscall.ENOSPC
					}
					if fault == "ambiguous-link" {
						if err := originalLink(oldFD, old, newFD, new, flags); err != nil {
							return err
						}
						return syscall.EIO
					}
				}
				return originalLink(oldFD, old, newFD, new, flags)
			}
			originalUnlink := objects.ops.unlinkat
			objects.ops.unlinkat = func(fd int, name string, flags int) error {
				if fault == "unlink-preparation" && name == preparation {
					return syscall.EIO
				}
				return originalUnlink(fd, name, flags)
			}
			originalSync := objects.ops.fsync
			directorySyncs := 0
			objects.ops.fsync = func(fd int) error {
				if fd == objects.objectsFD {
					directorySyncs++
					if (fault == "publication-sync" && directorySyncs == 1) ||
						(fault == "cleanup-sync" && directorySyncs == 2) {
						return syscall.EIO
					}
				}
				return originalSync(fd)
			}
			if err := objects.Delete(t.Context(), key); err == nil {
				t.Fatal("Delete acknowledged failed publication")
			}
			if _, err := objects.Get(t.Context(), key); !errors.Is(err, syscall.EIO) {
				t.Fatalf("publication uncertainty did not poison store: %v", err)
			}
			if _, err := os.Stat(objectPath(t, root, key)); err != nil {
				t.Fatalf("Delete changed object before marker barriers: %v", err)
			}
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got, err := reopened.Get(t.Context(), key)
			if fault == "link-refused" {
				if err != nil || string(got) != "original" {
					t.Fatalf("unpublished Delete lost object: %q, %v", got, err)
				}
			} else if !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("published Delete failed recovery: %q, %v", got, err)
			}
		})
	}
}

func TestRecoveryPreparationCannotAuthorizeOrHideCorruption(t *testing.T) {
	for _, state := range []string{"partial", "torn", "foreign", "wrong-key", "extra-link", "oversized", "symlink", "object-stage", "mismatched-final", "corrupt-final"} {
		t.Run(state, func(t *testing.T) {
			root := privateRoot(t)
			objects, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			key := "protected-target"
			if _, err := objects.Put(t.Context(), key, []byte("protected")); err != nil {
				t.Fatal(err)
			}
			id := objects.ID()
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
			marker, _ := markerName(key, true)
			preparation := filepath.Join(root, objectsDirectory, markerPreparationPrefix+marker)
			encoded := encodeRecoveryMarker(id, key, true)
			body := encoded[:]
			switch state {
			case "partial":
				body = body[:7]
			case "torn":
				body[50] ^= 1
			case "foreign":
				id[0] ^= 1
				encoded = encodeRecoveryMarker(id, key, true)
				body = encoded[:]
			case "wrong-key":
				encoded = encodeRecoveryMarker(id, "another-key", true)
				body = encoded[:]
			case "oversized":
				body = append(body, 1)
			}
			if state == "symlink" {
				if err := os.Symlink(objectPath(t, root, key), preparation); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(preparation, body, privateFileMode); err != nil {
				t.Fatal(err)
			}
			switch state {
			case "extra-link":
				if err := os.Link(preparation, filepath.Join(root, "external-alias")); err != nil {
					t.Fatal(err)
				}
			case "object-stage":
				if err := os.WriteFile(stagingPath(t, root, key), []byte("partial"), privateFileMode); err != nil {
					t.Fatal(err)
				}
			case "mismatched-final":
				writeRecoveryMarkerForTest(t, root, id, key, true)
			case "corrupt-final":
				if err := os.Link(preparation, filepath.Join(root, objectsDirectory, marker)); err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(preparation, 0); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := Open(t.Context(), root, Options{})
			if state == "partial" || state == "torn" {
				if err != nil {
					t.Fatal(err)
				}
				got, err := reopened.Get(t.Context(), key)
				if err != nil || string(got) != "protected" {
					t.Fatalf("preparation authorized delete: %q, %v", got, err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Lstat(preparation); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("safe preparation was not removed: %v", err)
				}
			} else {
				if !errors.Is(err, syscall.EIO) {
					if reopened != nil {
						_ = reopened.Close()
					}
					t.Fatalf("unsafe preparation accepted: %v", err)
				}
				if _, err := os.Lstat(preparation); err != nil {
					t.Fatalf("unsafe preparation removed: %v", err)
				}
				if _, err := os.Stat(objectPath(t, root, key)); err != nil {
					t.Fatalf("unsafe recovery removed target: %v", err)
				}
			}
		})
	}
}

func TestRecoveryPreparationAndFinalConsumeOneBoundedAction(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{MaxRecoveryEntries: 1, MaxInFlightOperations: 1})
	if err != nil {
		t.Fatal(err)
	}
	key := "bounded-action"
	if _, err := objects.Put(t.Context(), key, []byte("published")); err != nil {
		t.Fatal(err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	final := writeRecoveryMarkerForTest(t, root, objects.ID(), key, true)
	if err := os.Link(final, filepath.Join(filepath.Dir(final), markerPreparationPrefix+filepath.Base(final))); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), root, Options{MaxRecoveryEntries: 1, MaxInFlightOperations: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Get(t.Context(), key); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("bounded pair failed deletion: %v", err)
	}
}
