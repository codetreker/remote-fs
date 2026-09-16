package objectstore_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type partialFileOpenStore struct {
	*sqlite.LockingStore
	cause   error
	opened  *partialFileOpenReference
	entered chan struct{}
	release chan struct{}
}

func (s *partialFileOpenStore) OpenFile(ctx context.Context, name string, options storage.FileOpenOptions) (metastore.File, error) {
	return s.failAfterOpen(s.LockingStore.OpenFile(ctx, name, options))
}

func (s *partialFileOpenStore) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (metastore.File, error) {
	return s.failAfterOpen(s.LockingStore.OpenNode(ctx, id, options))
}

func (s *partialFileOpenStore) failAfterOpen(file metastore.File, err error) (metastore.File, error) {
	if err != nil {
		return file, err
	}
	s.opened = &partialFileOpenReference{File: file, entered: s.entered, release: s.release}
	return s.opened, s.cause
}

type partialFileOpenReference struct {
	metastore.File
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
	closes  atomic.Int64
}

func (f *partialFileOpenReference) Close(ctx context.Context) error {
	f.closes.Add(1)
	f.once.Do(func() { close(f.entered) })
	select {
	case <-f.release:
		return f.File.Close(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRetainedFailedOpenOwnsNativeReferenceCleanup(t *testing.T) {
	for _, target := range []string{"path", "node"} {
		t.Run(target, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				objects := memory.New()
				base, meta := fileVolume(t, objects, 4096, func(options *sqlite.Options) {
					options.MaxRetainedFiles = 1
					options.Advisory.MaxOwners = 1
				})
				if err := base.Write(t.Context(), "f", []byte("preserved")); err != nil {
					t.Fatal(err)
				}
				original, err := base.Stat(t.Context(), "f")
				if err != nil {
					t.Fatal(err)
				}
				failure := errors.New("native open result lost")
				partial := &partialFileOpenStore{
					LockingStore: meta, cause: failure,
					entered: make(chan struct{}), release: make(chan struct{}),
				}
				volume := objectstore.New(objects, partial)
				t.Cleanup(func() {
					if err := volume.Close(); err != nil {
						t.Error(err)
					}
				})
				session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
				release := sync.OnceFunc(func() { close(partial.release) })
				t.Cleanup(release)
				options := storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}}
				var file storage.File
				if target == "path" {
					file, err = session.OpenFile(t.Context(), "f", options)
				} else {
					file, err = session.OpenNode(t.Context(), original.ID, options)
				}
				if file != nil || !errors.Is(err, failure) {
					t.Fatalf("failed open returned file=%v, error=%v", file, err)
				}
				select {
				case <-partial.entered:
				case <-time.After(3 * time.Second):
					t.Fatal("failed open left its native reference unowned")
				}
				if _, err := partial.opened.Node(t.Context()); !errors.Is(err, syscall.ESTALE) {
					t.Fatalf("failed open retained live publication authority: %v", err)
				}
				closed := make(chan error, 1)
				go func() { closed <- session.Close(t.Context()) }()
				synctest.Wait()
				select {
				case err := <-closed:
					t.Fatalf("session close passed unfinished native cleanup: %v", err)
				default:
				}
				release()
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
				if calls := partial.opened.closes.Load(); calls != 1 {
					t.Fatalf("native close calls=%d, want 1", calls)
				}
				observer := fileSessionFor(t, base, storage.DefaultFileSessionOptions())
				retained := openFileFor(t, observer, "f", options)
				if attr := readFileFor(t, retained, "preserved"); attr.ID != original.ID {
					t.Fatalf("failed open changed linked identity: %+v", attr)
				}
				if used, err := base.Usage(t.Context()); err != nil || used != 9 {
					t.Fatalf("linked usage after cleanup=%d, %v", used, err)
				}
				if err := retained.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := base.Remove(t.Context(), "f"); err != nil {
					t.Fatal(err)
				}
				if used, err := base.Usage(t.Context()); err != nil || used != 0 {
					t.Fatalf("failed-open pin retained detached charge=%d, %v", used, err)
				}
				if _, err := meta.StatNode(t.Context(), original.ID); !errors.Is(err, syscall.ESTALE) {
					t.Fatalf("failed-open pin retained removed identity: %v", err)
				}
			})
		})
	}
}
