//go:build rfs_acceptance

package replicated_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestRemoteChangeReachesReplicaDuringContinuousListings(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	for i := range 4096 {
		if err := s.storage.Create(t.Context(), fmt.Sprintf("file-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	mounted, _ := mount(t, s)
	var stopped atomic.Bool
	stopReaders := func() { stopped.Store(true) }
	var group sync.WaitGroup
	var completed atomic.Int64
	failures := make(chan error, 128)
	startedReaders := make(chan struct{}, 128)
	for range 128 {
		group.Go(func() {
			first := true
			for !stopped.Load() {
				entries, err := mounted.List(t.Context(), "")
				if err != nil {
					failures <- err
					return
				}
				if len(entries) != 4096 && len(entries) != 4097 {
					failures <- fmt.Errorf("listing returned %d entries, want 4096 or 4097", len(entries))
					return
				}
				completed.Add(1)
				if first {
					startedReaders <- struct{}{}
					first = false
				}
			}
		})
	}
	defer func() { stopReaders(); group.Wait() }()
	for range 128 {
		select {
		case <-startedReaders:
		case err := <-failures:
			t.Fatalf("initial directory listing: %v", err)
		}
	}
	write(t, s, "arrived", "new content")
	written := time.Now()
	before := completed.Load()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		attr, err := mounted.Stat(ctx, "arrived")
		if err == nil {
			if attr.Size != int64(len("new content")) {
				t.Fatalf("new file has size %d", attr.Size)
			}
			break
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("remote change did not become visible: %v", err)
		}
	}
	visible := time.Since(written)
	during := completed.Load() - before
	if during == 0 {
		t.Fatal("no directory listing completed while the remote change became visible")
	}
	if visible >= time.Second {
		t.Fatalf("remote change became visible after %v, want under one second", visible)
	}
	stopReaders()
	group.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("concurrent listing: %v", err)
	}
	t.Logf("remote change visible in %v; 128 readers completed %d listings of 4096 files during that interval", visible, during)
}
