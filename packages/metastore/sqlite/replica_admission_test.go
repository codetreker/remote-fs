package sqlite

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestReplicaExclusiveWaitCancellationDoesNotOccupyCommitGate(t *testing.T) {
	replica, err := OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { replica.Close() })
	seeding, err := replica.Reseed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	rows := []metastore.Row{
		{Node: metastore.Node{ID: 10, Mode: fs.ModeDir | 0o755}},
		{Parent: 10, Name: []byte("file"), Node: metastore.Node{ID: 11, Mode: 0o644}},
	}
	if err := seeding.Add(t.Context(), rows); err != nil {
		seeding.Close()
		t.Fatal(err)
	}
	if err := seeding.Complete(t.Context(), 1); err != nil {
		seeding.Close()
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	result, err := storage.NewListResult(1024, 0,
		func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
			close(entered)
			<-release
			return nameBytes + 64, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	listed := make(chan error, 1)
	go func() { listed <- replica.ListBounded(t.Context(), "", result) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("bounded replica listing did not reach result accounting")
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if applied, err := replica.Apply(canceled, metastore.Change{Position: 2}); applied || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Apply returned applied=%v, err=%v", applied, err)
	}
	if next, err := replica.Reseed(canceled); next != nil || !errors.Is(err, context.Canceled) {
		if next != nil {
			next.Close()
		}
		t.Fatalf("canceled Reseed returned transaction=%v, err=%v", next != nil, err)
	}

	gateContext, cancelGate := context.WithTimeout(t.Context(), time.Second)
	defer cancelGate()
	if err := replica.store.coordinator.commit.acquire(gateContext); err != nil {
		t.Fatalf("canceled replica writers occupied the database commit gate: %v", err)
	}
	replica.store.coordinator.commit.release()
	close(release)
	if err := <-listed; err != nil {
		t.Fatalf("bounded replica listing after release: %v", err)
	}
}
