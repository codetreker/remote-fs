package service

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
)

func runtimeFixture(t *testing.T) (*runtimeManager, management.Volume) {
	t.Helper()
	root := t.TempDir()
	volumes := filepath.Join(root, "volumes")
	if err := os.Mkdir(volumes, 0700); err != nil {
		t.Fatal(err)
	}
	id, err := management.NewID()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := management.NewID()
	if err != nil {
		t.Fatal(err)
	}
	volume := management.Volume{ID: id, OwnerUserID: owner, InitialQuotaBytes: 1 << 20, State: management.VolumeCreating}
	runtime := newRuntimeManager(Config{}, &stateRoot{VolumesPath: volumes}, nil, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := runtime.Initialize(t.Context(), volume); err != nil {
		t.Fatal(err)
	}
	volume.State = management.VolumeActive
	return runtime, volume
}

func TestRuntimeSummaryCountsTreeAndPersistedSpace(t *testing.T) {
	runtime, volume := runtimeFixture(t)
	lease, err := runtime.acquire(t.Context(), volume)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if err := lease.Store.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	if err := lease.Store.Mkdir(t.Context(), "directory/nested"); err != nil {
		t.Fatal(err)
	}
	if err := lease.Store.Create(t.Context(), "root-file"); err != nil {
		t.Fatal(err)
	}
	if err := lease.Store.Create(t.Context(), "directory/nested/file"); err != nil {
		t.Fatal(err)
	}
	if err := lease.Store.Write(t.Context(), "directory/nested/file", []byte("content")); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	summary, err := runtime.Summary(t.Context(), volume)
	if err != nil {
		t.Fatal(err)
	}
	if summary.FileCount != 2 || summary.DirectoryCount != 2 || summary.UsedBytes != 4096 || summary.QuotaBytes != volume.InitialQuotaBytes {
		t.Fatalf("summary: %+v", summary)
	}
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened := newRuntimeManager(Config{}, runtime.root, nil, nil)
	defer func() {
		if err := reopened.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	again, err := reopened.Summary(t.Context(), volume)
	if err != nil || again != summary {
		t.Fatalf("reopen summary: %+v %v", again, err)
	}
}

func TestRuntimeSharedOpenAndDeleteWaitsForReferences(t *testing.T) {
	runtime, volume := runtimeFixture(t)
	leases := make([]*runtimeLease, 8)
	var workers sync.WaitGroup
	for i := range leases {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			lease, err := runtime.acquire(t.Context(), volume)
			if err != nil {
				t.Error(err)
				return
			}
			leases[index] = lease
		}(i)
	}
	workers.Wait()
	for _, lease := range leases {
		if lease == nil {
			t.Fatal("missing lease")
		}
		if lease.Store != leases[0].Store {
			t.Fatal("duplicate runtime store")
		}
		defer lease.Release()
	}
	deleting := volume
	deleting.State = management.VolumeDeleting
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := runtime.Delete(ctx, deleting); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delete with active references: %v", err)
	}
	if _, err := os.Stat(runtime.volumeRoot(volume.ID)); err != nil {
		t.Fatalf("delete removed active store: %v", err)
	}
	if _, err := runtime.acquire(t.Context(), volume); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("deleting runtime admitted read: %v", err)
	}
	for _, lease := range leases {
		if err := lease.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.Delete(t.Context(), deleting); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(runtime.volumeRoot(volume.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted root retained: %v", err)
	}
}

func TestRuntimeActiveEmptyRootIsNotInitialized(t *testing.T) {
	runtime, volume := runtimeFixture(t)
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	root := runtime.volumeRoot(volume.ID)
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	reopened := newRuntimeManager(Config{}, runtime.root, nil, nil)
	defer func() {
		if err := reopened.Close(t.Context()); err != nil {
			t.Error(err)
		}
	}()
	summary, err := reopened.Summary(t.Context(), volume)
	if !errors.Is(err, syscall.EIO) || summary != (VolumeSummary{}) {
		t.Fatalf("active empty root summary: %+v, %v; want storage failure", summary, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("active empty root was initialized: %v", entries)
	}
}

func TestRuntimeActiveMissingStorageFailsForCachedAndReopenedVolumes(t *testing.T) {
	for _, cached := range []bool{true, false} {
		name := "reopened"
		if cached {
			name = "cached"
		}
		t.Run(name, func(t *testing.T) {
			runtime, volume := runtimeFixture(t)
			lease, err := runtime.acquire(t.Context(), volume)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := lease.Release(); err != nil {
					t.Error(err)
				}
			})
			const survivor = "survivor"
			content := []byte("original native volume")
			if err := lease.Store.Write(t.Context(), survivor, content); err != nil {
				t.Fatal(err)
			}
			original, err := lease.Store.Stat(t.Context(), survivor)
			if err != nil {
				t.Fatal(err)
			}
			if err := lease.Release(); err != nil {
				t.Fatal(err)
			}
			expected, err := runtime.Summary(t.Context(), volume)
			if err != nil {
				t.Fatal(err)
			}
			if !cached {
				if err := runtime.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			root := runtime.volumeRoot(volume.ID)
			evidence := make(map[string]os.FileInfo)
			markers := make(map[string][]byte)
			for _, component := range []string{"root", "LOCALSTORE", "FORMAT", "metastore.sqlite"} {
				path := root
				if component != "root" {
					path = filepath.Join(root, component)
				}
				info, err := os.Lstat(path)
				if err != nil {
					t.Fatal(err)
				}
				evidence[path] = info
				if component == "LOCALSTORE" || component == "FORMAT" {
					markers[path], err = os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			healthy := true
			for _, component := range []string{"root", "LOCALSTORE", "FORMAT", "metastore.sqlite"} {
				t.Run(component, func(t *testing.T) {
					reader := runtime
					if !cached {
						reader = newRuntimeManager(Config{}, runtime.root, nil, nil)
					}
					path := root
					if component != "root" {
						path = filepath.Join(path, component)
					}
					moved := path + ".missing"
					if err := os.Rename(path, moved); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := os.Rename(moved, path); err != nil {
							healthy = false
							t.Error(err)
							return
						}
						for path, original := range evidence {
							info, err := os.Lstat(path)
							if err != nil || !os.SameFile(original, info) || info.Mode() != original.Mode() {
								healthy = false
								t.Errorf("restored component identity changed at %s: %v", path, err)
							}
						}
						for path, original := range markers {
							current, err := os.ReadFile(path)
							if err != nil || !bytes.Equal(current, original) {
								healthy = false
								t.Errorf("restored marker changed at %s: %v", path, err)
							}
						}
					})
					if !cached {
						t.Cleanup(func() {
							ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
							defer cancel()
							if err := reader.Close(ctx); err != nil {
								healthy = false
								t.Error(err)
							}
						})
					}
					summary, err := reader.Summary(t.Context(), volume)
					if !errors.Is(err, syscall.EIO) || summary != (VolumeSummary{}) {
						t.Fatalf("missing active %s summary: %+v, %v; want storage failure", component, summary, err)
					}
					if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("missing active %s was recreated: %v", component, err)
					}
					reader.mu.Lock()
					defer reader.mu.Unlock()
					if !cached && len(reader.entries) != 0 {
						t.Fatal("missing storage populated the reopened runtime cache")
					}
					if cached && (reader.entries[volume.ID] == nil || reader.entries[volume.ID].store != lease.Store || reader.entries[volume.ID].references != 0) {
						t.Fatal("missing storage changed the cached store or its references")
					}
				})
				if !healthy {
					t.Fatal("missing-storage fixture could not be restored")
				}
			}
			t.Run("restored-volume", func(t *testing.T) {
				reader := runtime
				if !cached {
					reader = newRuntimeManager(Config{}, runtime.root, nil, nil)
					t.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
						defer cancel()
						if err := reader.Close(ctx); err != nil {
							t.Error(err)
						}
					})
				}
				summary, err := reader.Summary(t.Context(), volume)
				if err != nil || summary != expected {
					t.Fatalf("restored volume summary: %+v, %v; want %+v", summary, err, expected)
				}
				lease, err := reader.acquire(t.Context(), volume)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := lease.Release(); err != nil {
						t.Error(err)
					}
				}()
				current, err := lease.Store.Stat(t.Context(), survivor)
				if err != nil || !reflect.DeepEqual(current, original) {
					t.Fatalf("restored node: %+v, %v; want %+v", current, err, original)
				}
				got, err := lease.Store.Read(t.Context(), survivor)
				if err != nil || !bytes.Equal(got, content) {
					t.Fatalf("restored content: %q, %v", got, err)
				}
			})
		})
	}
}
