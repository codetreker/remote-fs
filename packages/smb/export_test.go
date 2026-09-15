package smb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

type delayedCloseStream struct {
	*notifyTestStream
	release chan struct{}
}

func (s *delayedCloseStream) Close() error {
	err := s.notifyTestStream.Close()
	<-s.release
	return err
}

func TestUnpublishTimeoutRetainsNameUntilCleanupCompletes(t *testing.T) {
	s, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	stream := &delayedCloseStream{notifyTestStream: testNotifyStream(), release: make(chan struct{})}
	source := testNotifySource(stream.notifyTestStream)
	source.Subscribe = func(context.Context) (ChangeStream, error) { return stream, nil }
	source.Resume = func(context.Context, metastore.Incarnation, metastore.Position) (ChangeStream, error) {
		return stream, nil
	}
	e, err := s.Publish(Share{Name: "work", Volume: "trusted", Backend: &sessionBackend{}, Changes: source})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := e.Unpublish(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout %v", err)
	}
	if s.Status().StoppingExports != 1 {
		t.Fatal("cleanup state lost")
	}
	if _, err := s.Publish(Share{Name: "WORK", Volume: "trusted", Backend: &sessionBackend{}, Changes: testNotifySource(testNotifyStream())}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	close(stream.release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Unpublish(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Status().Exports != 0 {
		t.Fatal("export retained after confirmed cleanup")
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestUnpublishClosesIdleTreeWithoutClosingAuthentication(t *testing.T) {
	c, s, tr, _, _, _ := testConnection(t)
	r := fileRequest(wire.Close, make([]byte, 24))
	smbLE.PutUint16(r.Body, 24)
	r.Body[8] = 1
	if _, status := tr.files.handle(context.Background(), r); status != 0 {
		t.Fatalf("close %x", status)
	}
	if err := tr.export.Unpublish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.sessions[s.id] != s || s.signer == nil {
		t.Fatal("unpublish closed authentication")
	}
	if _, ok := s.trees[tr.id]; ok {
		t.Fatal("idle tree survived unpublish")
	}
	r = signedRequest(t, s, fileRequest(wire.Echo, wire.EmptyResponseBody()))
	h := r.Header
	if _, status, _ := c.dispatch(context.Background(), r, r, &h); status != 0 {
		t.Fatalf("echo %x", status)
	}
}
