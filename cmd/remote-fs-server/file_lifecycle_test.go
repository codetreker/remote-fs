package main

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestRegistryCleanupFailureKeepsOwnedStorageOpen(t *testing.T) {
	operationErr, cleanupErr := errors.New("serve failed"), errors.New("file cleanup uncertain")
	closed := false
	err := withOpened(opened{close: func() error { closed = true; return nil }}, func() error {
		return errors.Join(operationErr, &fileRegistryCloseError{cause: cleanupErr})
	})
	if closed || !errors.Is(err, operationErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("uncertain registry close returned closed=%t err=%v", closed, err)
	}
}

func openFileLifecycleNamespace(t *testing.T) (opened, commandConfig) {
	t.Helper()
	config, _, err := parseConfig([]string{
		"-listen", "127.0.0.1:0", "-local-store", emptyPrivateDirectory(t),
		"-workspace", "workspace", "-quota", "1M", "-initialize-lock-state",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ns, err := open(config)
	if err != nil {
		t.Fatal(err)
	}
	return ns, config
}

func TestHandlerRetirementReclaimsDetachedFilesBeforeStorageClose(t *testing.T) {
	ns, _ := openFileLifecycleNamespace(t)
	t.Cleanup(func() {
		if err := ns.close(); err != nil {
			t.Errorf("close native storage: %v", err)
		}
	})
	handler, err := httprest.NewHandler(ns.namespace, ns.log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close(context.Background()) })
	server := newServer(handler)
	httpServer := httptest.NewServer(server.drain)
	t.Cleanup(httpServer.Close)
	client := httpServer.Client()
	client.Timeout = 5 * time.Second
	remote, err := httprest.Dial(httpServer.URL, client)
	if err != nil {
		t.Fatal(err)
	}
	session, err := remote.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenFile(t.Context(), "held", storage.FileOpenOptions{Read: true, Write: true, Create: true, Mode: 0o600})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(t.Context(), 0, []byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err := ns.namespace.Remove(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	before, err := ns.namespace.Space(t.Context())
	if err != nil || before.Used != int64(len("retained")) {
		t.Fatalf("detached file quota=%d err=%v", before.Used, err)
	}
	httpServer.Close()
	err = withOpened(ns, func() error {
		if err := closeFileRegistry(server, time.Second); err != nil {
			return err
		}
		after, err := ns.namespace.Space(t.Context())
		if err != nil || after.Used != 0 {
			t.Fatalf("handler retirement left quota=%d err=%v", after.Used, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type delayedFileSessionStorage struct {
	*localstore.Store
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *delayedFileSessionStorage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := s.Store.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &delayedFileSession{FileSession: session, storage: s}, nil
}

type delayedFileSession struct {
	storage.FileSession
	storage *delayedFileSessionStorage
}

func (s *delayedFileSession) Close(ctx context.Context) error {
	s.storage.once.Do(func() { close(s.storage.entered) })
	<-s.storage.release
	return s.FileSession.Close(ctx)
}

func TestRegistryTimeoutRetainsNativeOwnershipUntilCleanupIsKnown(t *testing.T) {
	ns, config := openFileLifecycleNamespace(t)
	backing := &delayedFileSessionStorage{
		Store: ns.namespace.(*localstore.Store), entered: make(chan struct{}), release: make(chan struct{}),
	}
	handler, err := httprest.NewHandler(backing, ns.log)
	if err != nil {
		_ = ns.close()
		t.Fatal(err)
	}
	var release sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(backing.release) })
		if err := handler.Close(context.Background()); err != nil {
			t.Errorf("finish handler cleanup: %v", err)
			return
		}
		if err := backing.Store.Close(); err != nil {
			t.Errorf("close native storage: %v", err)
		}
	})
	server := newServer(handler)
	httpServer := httptest.NewServer(server.drain)
	t.Cleanup(httpServer.Close)
	client := httpServer.Client()
	client.Timeout = 5 * time.Second
	remote, err := httprest.Dial(httpServer.URL, client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remote.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); err != nil {
		t.Fatal(err)
	}
	httpServer.Close()
	closed := false
	nativeClose := ns.close
	ns.close = func() error { closed = true; return nativeClose() }
	err = withOpened(ns, func() error { return closeFileRegistry(server, 20*time.Millisecond) })
	if !errors.Is(err, context.DeadlineExceeded) || closed {
		t.Fatalf("registry timeout returned closed=%t err=%v", closed, err)
	}
	if diagnostic := err.Error(); !strings.Contains(diagnostic, "closing HTTP file sessions") || !strings.Contains(diagnostic, context.DeadlineExceeded.Error()) {
		t.Fatalf("registry timeout omitted its cleanup stage or cause: %q", diagnostic)
	}
	select {
	case <-backing.entered:
	default:
		t.Fatal("timeout occurred before native session cleanup entered")
	}
	config.initializeLockState = false
	contender, err := open(config)
	if err == nil {
		_ = contender.close()
		t.Fatal("native ownership was released during uncertain registry cleanup")
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("retained ownership returned %v, want EBUSY", err)
	}
	release.Do(func() { close(backing.release) })
	if err := handler.Close(t.Context()); err != nil {
		t.Fatalf("reconcile handler cleanup: %v", err)
	}
}
