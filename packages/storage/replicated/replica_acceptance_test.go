//go:build rfs_acceptance

package replicated_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestRemoteChangeReachesReplicaDuringContinuousListings(t *testing.T) {
	const (
		readers             = 128
		responseConcurrency = readers + 1
		responseWaiters     = 64
		maxBodyBytes        = int64(2 << 20)
		responsePeak        = 4 * responseConcurrency * maxBodyBytes
		clientTimeout       = 90 * time.Second
	)
	handlerOptions := httprest.DefaultHandlerOptions()
	handlerOptions.MaxBodyBytes = maxBodyBytes
	handlerOptions.MaxConcurrentResponses = responseConcurrency
	handlerOptions.MaxInFlightResponseBytes = responsePeak
	handlerOptions.MaxWaitingResponses = responseWaiters
	s := serveWithTransportOptions(t, &http.Client{Timeout: clientTimeout}, handlerOptions)
	for i := range 4096 {
		if err := s.storage.Create(t.Context(), fmt.Sprintf("file-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	dialOptions := httprest.DefaultDialOptions()
	dialOptions.Silence = s.silence
	dialOptions.MaxBodyBytes = maxBodyBytes
	dialOptions.MaxConcurrentResponses = responseConcurrency
	dialOptions.MaxInFlightResponseBytes = responsePeak
	dialOptions.MaxWaitingResponses = responseWaiters
	if handlerOptions.MaxConcurrentResponses < readers+1 || dialOptions.MaxConcurrentResponses < readers+1 {
		t.Fatal("acceptance transport cannot admit every reader and the authority operation together")
	}
	requiredResponseBytes := int64(4 * (readers + 1) * maxBodyBytes)
	if handlerOptions.MaxInFlightResponseBytes < requiredResponseBytes || dialOptions.MaxInFlightResponseBytes < requiredResponseBytes {
		t.Fatal("acceptance transport cannot retain every admitted response")
	}
	mounted, _ := mountWithHTTPOptions(t, s, &http.Client{Timeout: clientTimeout}, dialOptions)
	var stopped atomic.Bool
	stopReaders := func() { stopped.Store(true) }
	var group sync.WaitGroup
	var completed atomic.Int64
	failures := make(chan error, readers)
	startedReaders := make(chan struct{}, readers)
	listingProgress := make(chan struct{}, 1)
	startLoad := make(chan struct{})
	var releaseLoadOnce sync.Once
	releaseLoad := func() { releaseLoadOnce.Do(func() { close(startLoad) }) }
	for range readers {
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
					select {
					case <-startLoad:
					case <-t.Context().Done():
						return
					}
				}
				select {
				case listingProgress <- struct{}{}:
				default:
				}
			}
		})
	}
	defer func() { releaseLoad(); stopReaders(); group.Wait() }()
	for range readers {
		select {
		case <-startedReaders:
		case err := <-failures:
			t.Fatalf("initial directory listing: %v", err)
		}
	}
	rendezvous := s.authorityGate.rendezvousAtBackend(readers)
	defer rendezvous.Release()
	before := completed.Load()
	releaseLoad()
	select {
	case <-rendezvous.listsArrived:
	case err := <-failures:
		t.Fatalf("arming continuous directory load: %v", err)
	case <-t.Context().Done():
		t.Fatal(context.Cause(t.Context()))
	}
	writeDone := make(chan error, 1)
	group.Go(func() { writeDone <- s.elsewhere.Write(t.Context(), "arrived", []byte("new content")) })
	select {
	case <-rendezvous.writeArrived:
	case err := <-writeDone:
		t.Fatalf("authority write ended before reaching the backend rendezvous: %v", err)
	case <-t.Context().Done():
		t.Fatal(context.Cause(t.Context()))
	}
	rendezvous.Release()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("writing %q into the volume: %v", "arrived", err)
		}
	case err := <-failures:
		t.Fatalf("concurrent listing during the remote write: %v", err)
	case <-t.Context().Done():
		t.Fatal(context.Cause(t.Context()))
	}
	written := time.Now()
	postWrite := completed.Load()
	ctx, cancel := context.WithDeadline(t.Context(), written.Add(time.Second))
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
		select {
		case err := <-failures:
			t.Fatalf("concurrent listing before visibility: %v", err)
		default:
		}
	}
	visible := time.Since(written)
	for completed.Load() == postWrite {
		select {
		case <-listingProgress:
		case err := <-failures:
			t.Fatalf("concurrent listing after visibility: %v", err)
		case <-ctx.Done():
			t.Fatal("no directory listing completed during the one-second visibility window")
		}
	}
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
