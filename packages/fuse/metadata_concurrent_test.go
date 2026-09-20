package fuse

import (
	"context"
	"io/fs"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

type concurrentMetadataFile struct {
	storage.File
	metadata storage.ReferenceMetadataAccess
	barrier  *metadataCASBarrier
}

func (f *concurrentMetadataFile) CheckMetadataAccess() error {
	return f.metadata.CheckMetadataAccess()
}

func (f *concurrentMetadataFile) SetMetadata(ctx context.Context, namespace string, expected, data []byte) (storage.OpaquePayload, error) {
	if err := f.barrier.wait(ctx); err != nil {
		return storage.OpaquePayload{}, err
	}
	return f.metadata.SetMetadata(ctx, namespace, expected, data)
}

type metadataCASBarrier struct {
	mu      sync.Mutex
	entered int
	calls   int
	release chan struct{}
}

func (b *metadataCASBarrier) wait(ctx context.Context) error {
	b.mu.Lock()
	b.calls++
	if b.entered >= 2 {
		b.mu.Unlock()
		return nil
	}
	b.entered++
	if b.entered == 2 {
		close(b.release)
	}
	release := b.release
	b.mu.Unlock()
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *metadataCASBarrier) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func TestConcurrentChmodRefreshesTheLosingCASOnTheLiveFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, backing := memoryfixture.New(t, "fuse-chmod-cas", 0, locking.DefaultOptions())
	if err := backing.Write(ctx, "file", []byte("contents")); err != nil {
		t.Fatal(err)
	}
	session, err := backing.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	attr, err := backing.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	barrier := &metadataCASBarrier{release: make(chan struct{})}
	v := &volume{files: session, deadline: time.Now().Add(time.Minute)}
	n := &node{volume: v, id: &identity{node: attr.ID, kind: syscall.S_IFREG}}
	results := make(chan error, 2)
	for _, mode := range []fs.FileMode{0600, 0640} {
		file, err := session.OpenNode(ctx, attr.ID, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := file.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		reference := file.(storage.ReferenceMetadataAccess)
		h := newHandle(n, &concurrentMetadataFile{File: file, metadata: reference, barrier: barrier}, true, false)
		go func() { results <- n.setPermissions(ctx, h, mode) }()
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if calls := barrier.callCount(); calls != 3 {
		t.Fatalf("concurrent chmod issued %d metadata CAS calls, want one retry", calls)
	}
	final, err := session.StatNode(ctx, attr.ID)
	if err != nil {
		t.Fatal(err)
	}
	mode, err := permissions(final)
	if err != nil || mode != 0600 && mode != 0640 {
		t.Fatalf("final permissions = %v, %v", mode, err)
	}
}
