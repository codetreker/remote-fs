package fuse_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func comparisonUnlink(path string) error { return comparisonRemovalCall(path, syscall.Unlink) }
func comparisonRmdir(path string) error  { return comparisonRemovalCall(path, syscall.Rmdir) }

// The comparison's backend classifies uncertain publication as EIO. Match
// os.Remove's individual EINTR handling while retaining unlink/rmdir distinctions.
// https://github.com/golang/go/blob/c293dd49cbe25e1fe8d97d94a5cb618e7b6d831e/src/os/file_unix.go#L357-L370
func comparisonRemovalCall(path string, call func(string) error) error {
	var err error
	for range 8 {
		err = call(path)
		if !errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EIO) {
			return err
		}
	}
	return err
}

func TestComparisonRemovalRetriesOnlyKnownInterruptedCalls(t *testing.T) {
	for _, test := range []struct {
		name                     string
		fail                     error
		interruptions, wantCalls int
	}{
		{"interrupted before success", nil, 2, 3},
		{"interruption bound", syscall.EINTR, 0, 8},
		{"unknown outcome", syscall.EIO, 0, 1},
		{"unknown interrupted outcome", errors.Join(syscall.EINTR, syscall.EIO), 0, 1},
		{"missing target", syscall.ENOENT, 0, 1},
		{"wrong target kind", syscall.EISDIR, 0, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls, effects := 0, 0
			err := comparisonRemovalCall("one-name", func(path string) error {
				if path != "one-name" {
					t.Fatalf("changed retry target: %q", path)
				}
				calls++
				if calls <= test.interruptions {
					return syscall.EINTR
				}
				if test.fail == nil {
					effects++
				}
				return test.fail
			})
			if !errors.Is(err, test.fail) || calls != test.wantCalls {
				t.Fatalf("error=%v calls=%d want=%v/%d", err, calls, test.fail, test.wantCalls)
			}
			wantEffects := 0
			if test.fail == nil {
				wantEffects = 1
			}
			if effects != wantEffects {
				t.Fatalf("effects=%d want=%d", effects, wantEffects)
			}
		})
	}
}

func removalComparisonStep(t *testing.T, name string) step {
	t.Helper()
	for _, candidate := range differentialSteps {
		if candidate.name == name {
			return candidate
		}
	}
	t.Fatalf("missing comparison step %q", name)
	return step{}
}

func TestComparisonRemovalPreservesEffectsAcrossMountedInterruptions(t *testing.T) {
	for _, test := range []struct {
		name, operation, path, step string
		raw                         func(string) error
		prepare                     func(*testing.T, storage.Storage)
	}{
		{"unlink", "Remove", "sub/deep.txt", "remove a file", syscall.Unlink, func(t *testing.T, s storage.Storage) {
			if err := s.Mkdir(t.Context(), "sub"); err != nil {
				t.Fatal(err)
			}
			if err := s.Write(t.Context(), "sub/deep.txt", []byte("payload")); err != nil {
				t.Fatal(err)
			}
		}},
		{"rmdir", "RemoveDir", "sub", "remove the now empty directory", syscall.Rmdir, func(t *testing.T, s storage.Storage) {
			if err := s.Mkdir(t.Context(), "sub"); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var backing storage.Storage
			var calls atomic.Int32
			mountpoint := mountFaulty(t, func(operation, path string) error {
				if operation == test.operation && path == test.path && calls.Add(1) <= 2 {
					return context.Canceled
				}
				return nil
			}, func(s storage.Storage) { backing = s; test.prepare(t, s) })
			before, err := backing.Stat(t.Context(), test.path)
			if err != nil {
				t.Fatal(err)
			}
			if err := test.raw(filepath.Join(mountpoint, filepath.FromSlash(test.path))); !errors.Is(err, syscall.EINTR) {
				t.Fatalf("single interrupted syscall=%v", err)
			}
			after, err := backing.Stat(t.Context(), test.path)
			if err != nil || after.ID != before.ID {
				t.Fatalf("pre-effect interruption changed target: %+v %v", after, err)
			}
			if _, err := removalComparisonStep(t, test.step).run(mountpoint); err != nil {
				t.Fatalf("individual syscall retry=%v", err)
			}
			if calls.Load() != 3 {
				t.Fatalf("removal attempts=%d want3", calls.Load())
			}
			if _, err := backing.Stat(t.Context(), test.path); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("successful removal missing effect: %v", err)
			}
		})
	}
}

func TestComparisonRemovalDoesNotRetryAnAppliedFailure(t *testing.T) {
	for _, directory := range []bool{false, true} {
		name := "unlink"
		if directory {
			name = "rmdir"
		}
		t.Run(name, func(t *testing.T) {
			operation, path, stepName := "Remove", "sub/deep.txt", "remove a file"
			if directory {
				operation, path, stepName = "RemoveDir", "sub", "remove the now empty directory"
			}
			var backing storage.Storage
			var calls atomic.Int32
			mountpoint := mountFaulty(t, func(gotOperation, gotPath string) error {
				if gotOperation != operation || gotPath != path {
					return nil
				}
				if calls.Add(1) != 1 {
					return syscall.ENOENT
				}
				var err error
				if directory {
					err = backing.RemoveDir(t.Context(), path)
				} else {
					err = backing.Remove(t.Context(), path)
				}
				if err != nil {
					return err
				}
				return syscall.EIO
			}, func(s storage.Storage) {
				backing = s
				if err := s.Mkdir(t.Context(), "sub"); err != nil {
					t.Fatal(err)
				}
				if !directory {
					if err := s.Write(t.Context(), path, []byte("payload")); err != nil {
						t.Fatal(err)
					}
				}
			})
			if _, err := removalComparisonStep(t, stepName).run(mountpoint); !errors.Is(err, syscall.EIO) {
				t.Fatalf("applied removal error=%v wantEIO", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("uncertain removal replayed %d times", calls.Load())
			}
			if _, err := backing.Stat(t.Context(), path); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("failure fixture did not apply removal: %v", err)
			}
		})
	}
}
