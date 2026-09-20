package replicated_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestAuthorityRendezvousHoldsListsAndWritesAtTheBackendBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s := serveWithTransportOptions(t, &http.Client{Timeout: 5 * time.Second}, httprest.DefaultHandlerOptions())
	if err := s.storage.Create(ctx, "existing"); err != nil {
		t.Fatal(err)
	}

	rendezvous := s.authorityGate.rendezvousAtBackend(1)
	defer rendezvous.Release()
	listDone := make(chan error, 1)
	go func() {
		entries, err := s.elsewhere.List(ctx, "")
		if err == nil && len(entries) != 1 {
			err = fmt.Errorf("listing returned %d entries, want 1", len(entries))
		}
		listDone <- err
	}()
	select {
	case <-rendezvous.listsArrived:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}

	writeDone := make(chan error, 1)
	go func() { writeDone <- s.elsewhere.Write(ctx, "arrived", []byte("content")) }()
	select {
	case <-rendezvous.writeArrived:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	select {
	case err := <-listDone:
		t.Fatalf("list passed the rendezvous before release: %v", err)
	default:
	}
	select {
	case err := <-writeDone:
		t.Fatalf("write passed the rendezvous before release: %v", err)
	default:
	}

	rendezvous.Release()
	for name, done := range map[string]<-chan error{"list": listDone, "write": writeDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s after release: %v", name, err)
			}
		case <-ctx.Done():
			t.Fatalf("%s after release: %v", name, context.Cause(ctx))
		}
	}
}
