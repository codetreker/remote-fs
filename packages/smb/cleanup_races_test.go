package smb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type gatedDoneContext struct {
	context.Context
	entered, release chan struct{}
	once             sync.Once
}

func (c *gatedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Context.Done()
}

type retryCloseStream struct {
	*notifyTestStream
	calls           atomic.Int32
	second, release chan struct{}
}

func (s *retryCloseStream) Close() error {
	if n := s.calls.Add(1); n == 1 {
		_ = s.notifyTestStream.Close()
		return syscall.EIO
	} else if n == 2 {
		close(s.second)
		<-s.release
	}
	return s.notifyTestStream.Close()
}

func TestUnpublishWaiterKeepsItsFailedAttemptDuringRetry(t *testing.T) {
	s, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	stream := &retryCloseStream{notifyTestStream: testNotifyStream(), second: make(chan struct{}), release: make(chan struct{})}
	source := testNotifySource(stream.notifyTestStream)
	source.Subscribe = func(context.Context) (ChangeStream, error) { return stream, nil }
	e, err := s.Publish(Share{Name: "work", Volume: "trusted", Backend: &sessionBackend{}, Changes: source})
	if err != nil {
		t.Fatal(err)
	}
	gate := &gatedDoneContext{Context: context.Background(), entered: make(chan struct{}), release: make(chan struct{})}
	var releaseGate, releaseStream sync.Once
	t.Cleanup(func() {
		releaseGate.Do(func() { close(gate.release) })
		releaseStream.Do(func() { close(stream.release) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	first := make(chan error, 1)
	go func() { first <- e.Unpublish(gate) }()
	<-gate.entered
	e.cleanupMu.Lock()
	attempt := e.cleanup
	e.cleanupMu.Unlock()
	<-attempt.done
	if !errors.Is(attempt.err, syscall.EIO) {
		t.Fatal(attempt.err)
	}
	second := make(chan error, 1)
	go func() { second <- e.Unpublish(context.Background()) }()
	<-stream.second
	releaseGate.Do(func() { close(gate.release) })
	if err := <-first; !errors.Is(err, syscall.EIO) {
		t.Fatalf("old waiter observed replacement attempt: %v", err)
	}
	if s.Status().Exports != 1 || s.Status().StoppingExports != 1 {
		t.Fatal("pending retry falsely retired export")
	}
	releaseStream.Do(func() { close(stream.release) })
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}

func TestLateUnpublishCannotRemoveAReplacementExport(t *testing.T) {
	s, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.Publish(Share{Name: "work", Volume: "trusted", Backend: &sessionBackend{}, Changes: testNotifySource(testNotifyStream())})
	if err != nil {
		t.Fatal(err)
	}
	gate := &gatedDoneContext{Context: context.Background(), entered: make(chan struct{}), release: make(chan struct{})}
	var released sync.Once
	t.Cleanup(func() {
		released.Do(func() { close(gate.release) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	late := make(chan error, 1)
	go func() { late <- e.Unpublish(gate) }()
	<-gate.entered
	if err := e.Unpublish(context.Background()); err != nil {
		t.Fatal(err)
	}
	replacement, err := s.Publish(Share{Name: "work", Volume: "trusted", Backend: &sessionBackend{}, Changes: testNotifySource(testNotifyStream())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replacement.changes.Close() })
	released.Do(func() { close(gate.release) })
	if err := <-late; err != nil {
		t.Fatal(err)
	}
	if s.Status().Exports != 1 {
		t.Fatal("late retirement removed a new export")
	}
	if _, err := s.Publish(Share{Name: "work", Volume: "trusted", Backend: &sessionBackend{}, Changes: testNotifySource(testNotifyStream())}); !errors.Is(err, ErrBusy) {
		t.Fatalf("replacement name is no longer registered: %v", err)
	}
}
