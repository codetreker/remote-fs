package limited_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func newReadonlyBacking(t *testing.T) (storage.BoundedStorage, func(*testing.T)) {
	t.Helper()
	meta, backing := memoryfixture.New(t, "limited", 0, locking.DefaultOptions())
	ctx := t.Context()
	root, err := backing.Stat(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	barrier, err := meta.Barrier(ctx, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return backing, func(child *testing.T) {
		child.Helper()
		if got, err := backing.Stat(ctx, ""); err != nil || !reflect.DeepEqual(got, root) {
			child.Fatalf("readonly rejection changed root: %+v, %v; want %+v", got, err, root)
		}
		if entries, err := backing.List(ctx, ""); err != nil || len(entries) != 0 {
			child.Fatalf("readonly rejection changed tree: %+v, %v", entries, err)
		}
		if got, err := meta.Barrier(ctx, 1024); err != nil || got != barrier {
			child.Fatalf("readonly rejection changed history: %+v, %v; want %+v", got, err, barrier)
		}
		if used, err := meta.Usage(ctx); err != nil || used != 0 {
			child.Fatalf("readonly rejection changed usage: %d, %v", used, err)
		}
		if status, err := meta.ObjectStatus(ctx); err != nil || status != (sqlite.ObjectStatus{}) {
			child.Fatalf("readonly rejection changed object maintenance: %+v, %v", status, err)
		}
		probe := &publicationProbe{BoundedStorage: backing, applyCtx: ctx, outcome: storage.PublicationApplied}
		successor, err := limited.New(ctx, probe, limited.MinLimit)
		if err != nil {
			child.Fatalf("healthy successor rejected shared backing: %v", err)
		}
		if space, err := successor.Space(ctx); err != nil || space != (storage.Space{Total: limited.MinLimit, Avail: limited.MinLimit}) {
			child.Fatalf("healthy successor inherited rejection state: %+v, %v", space, err)
		}
		if err := successor.Close(); err != nil || probe.closes != 1 {
			child.Fatalf("closing healthy successor = %v after %d calls", err, probe.closes)
		}
	}
}

func nativePublicationVolumeFactory(t *testing.T) func(*testing.T, context.Context, int64, objectstore.Objects) (*sqlite.Store, *objectstore.Storage) {
	t.Helper()
	database := filepath.Join(t.TempDir(), "volume.db")
	sequence := 0
	return func(child *testing.T, ctx context.Context, allowance int64, objects objectstore.Objects) (*sqlite.Store, *objectstore.Storage) {
		child.Helper()
		sequence++
		meta, err := sqlite.Open(ctx, database, fmt.Sprintf("quota-%d", sequence), allowance, sqlite.DefaultWindow())
		if err != nil {
			child.Fatal(err)
		}
		backing := objectstore.New(objects, meta)
		child.Cleanup(func() {
			if err := backing.Close(); err != nil {
				child.Errorf("closing native volume: %v", err)
			}
		})
		return meta, backing
	}
}

func TestNativePublicationVolumesKeepNamesAndAccountingIsolated(t *testing.T) {
	newVolume := nativePublicationVolumeFactory(t)
	var first *sqlite.Store
	var firstRoot storage.Attr
	var firstIncarnation string
	if !t.Run("populated volume", func(t *testing.T) {
		meta, backing := newVolume(t, t.Context(), 8192, memory.New())
		first = meta
		if err := backing.Write(t.Context(), "same", []byte("first")); err != nil {
			t.Fatal(err)
		}
		var err error
		firstRoot, err = backing.Stat(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		barrier, err := meta.Barrier(t.Context(), 1024)
		if err != nil || barrier.Position == 0 {
			t.Fatalf("populated history = %+v, %v", barrier, err)
		}
		firstIncarnation = string(barrier.Incarnation)
	}) {
		t.FailNow()
	}
	if !first.Terminal() {
		t.Fatal("completed child retained its metadata store")
	}
	t.Run("fresh volume", func(t *testing.T) {
		meta, backing := newVolume(t, t.Context(), 4096, memory.New())
		if entries, err := backing.List(t.Context(), ""); err != nil || len(entries) != 0 {
			t.Fatalf("fresh tree = %+v, %v", entries, err)
		}
		if body, err := backing.Read(t.Context(), "same"); !errors.Is(err, syscall.ENOENT) || body != nil {
			t.Fatalf("fresh volume inherited contents = %q, %v", body, err)
		}
		root, err := backing.Stat(t.Context(), "")
		if err != nil || root.ID == firstRoot.ID {
			t.Fatalf("fresh root = %+v, %v; previous root = %+v", root, err, firstRoot)
		}
		barrier, err := meta.Barrier(t.Context(), 1024)
		if err != nil || barrier.Position != 0 || string(barrier.Incarnation) == firstIncarnation {
			t.Fatalf("fresh history = %+v, %v", barrier, err)
		}
		if space, err := backing.Space(t.Context()); err != nil || space != (storage.Space{Total: 4096, Avail: 4096}) {
			t.Fatalf("fresh allowance = %+v, %v", space, err)
		}
		if status, err := meta.ObjectStatus(t.Context()); err != nil || status != (sqlite.ObjectStatus{}) {
			t.Fatalf("fresh object maintenance = %+v, %v", status, err)
		}
		if err := backing.Write(t.Context(), "same", []byte("second")); err != nil {
			t.Fatal(err)
		}
		if body, err := backing.Read(t.Context(), "same"); err != nil || string(body) != "second" {
			t.Fatalf("healthy successor = %q, %v", body, err)
		}
	})
}
