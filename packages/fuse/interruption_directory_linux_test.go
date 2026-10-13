//go:build linux

package fuse_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func TestSignalBeforeDirectoryReferenceAdmissionPreservesRetryableCancellation(t *testing.T) {
	if mode := os.Getenv(signalChildMode); mode != "" {
		runDirectorySignalChild(t, mode)
		return
	}
	requireFUSE(t)
	for _, mode := range []string{"raw", "go"} {
		t.Run(mode, func(t *testing.T) {
			meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
				Database: filepath.Join(t.TempDir(), "volume.db"), Volume: "directory-interrupt",
				SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			gate := &directoryAdmissionGate{LockingStore: meta, entered: make(chan context.Context, 1)}
			backing := objectstore.New(memory.New(), gate)
			t.Cleanup(func() {
				if err := backing.Close(); err != nil {
					t.Errorf("closing interrupted volume: %v", err)
				}
			})
			if err := backing.Write(t.Context(), "artifact", []byte("original")); err != nil {
				t.Fatal(err)
			}
			observed := &directorySignalStorage{FileStorage: backing, t: t, entered: make(chan directoryOpenStart, 4), results: make(chan directoryOpenResult, 4), closed: make(chan struct{}, 4)}
			trace := &openInterruptTrace{operation: "OPENDIR", interrupted: make(chan struct{})}
			point := mountStorage(t, observed, fuse.Options{Logger: log.New(trace, "", 0), Debug: true})
			gate.armed.Store(true)
			t.Cleanup(func() {
				if t.Failed() {
					_, _, output := trace.snapshot()
					t.Logf("directory interruption trace:\n%s", output)
				}
			})
			runSignalProcess(t, "TestSignalBeforeDirectoryReferenceAdmissionPreservesRetryableCancellation", mode, point, func(ctx context.Context, pid, tid int) {
				start := receiveDirectoryStart(t, ctx, observed.entered)
				original := receiveOpenContext(t, ctx, gate.entered, "native directory admission")
				receipt, err := start.session.QueryFileAction(ctx, start.action)
				if err != nil || receipt.Outcome != storage.FileActionPending || original.Err() != nil {
					t.Fatalf("held directory admission: receipt=%+v error=%v context=%v", receipt, err, original.Err())
				}
				if err := unix.Tgkill(pid, tid, syscall.SIGURG); err != nil {
					t.Fatal(err)
				}
				select {
				case <-trace.interrupted:
				case <-ctx.Done():
					t.Fatalf("kernel did not interrupt OPENDIR: %v", ctx.Err())
				}
				select {
				case <-original.Done():
				case <-ctx.Done():
					t.Fatal("OPENDIR interrupt did not cancel native context")
				}
			})
			first := receiveDirectoryResult(t, observed.results)
			if !errors.Is(first.err, context.Canceled) || storage.ErrnoOf(first.err) != syscall.EINTR || first.reference || first.receipt.Outcome != storage.FileActionNotExecuted || first.queryErr != nil {
				t.Fatalf("interrupted directory open: %+v", first)
			}
			if mode == "go" {
				if retry := receiveDirectoryResult(t, observed.results); retry.err != nil || !retry.reference {
					t.Fatalf("Go directory retry: %+v", retry)
				}
			}
			entries, err := os.ReadDir(point)
			if err != nil || len(entries) != 1 || entries[0].Name() != "artifact" {
				t.Fatalf("directory after interruption: entries=%v error=%v", entries, err)
			}
			wantClosed := 1
			if mode == "go" {
				wantClosed = 2
			}
			for range wantClosed {
				select {
				case <-observed.closed:
				case <-time.After(5 * time.Second):
					t.Fatal("directory reference was not closed")
				}
			}
			if int(observed.closes.Load()) != wantClosed {
				t.Fatalf("directory close calls=%d want=%d", observed.closes.Load(), wantClosed)
			}
			status, err := observed.session.Status(t.Context())
			if err != nil || status.Fenced || status.Retired || status.Remaining <= 0 {
				t.Fatalf("mount session after interruption: status=%+v error=%v", status, err)
			}
		})
	}
}

func runDirectorySignalChild(t *testing.T, mode string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	report := os.NewFile(3, "thread ID")
	defer report.Close()
	if _, err := fmt.Fprintln(report, unix.Gettid()); err != nil {
		t.Fatal(err)
	}
	name := os.Getenv("REMOTE_FS_SIGNAL_PATH")
	switch mode {
	case "raw":
		fd, err := unix.Open(name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if fd >= 0 {
			_ = unix.Close(fd)
		}
		if err != syscall.EINTR {
			t.Fatalf("single interrupted directory open: %v, want EINTR", err)
		}
	case "go":
		entries, err := os.ReadDir(name)
		if err != nil || len(entries) != 1 || entries[0].Name() != "artifact" {
			t.Fatalf("retried directory listing: entries=%v error=%v", entries, err)
		}
	default:
		t.Fatalf("unknown directory signal mode %q", mode)
	}
}

type directoryAdmissionGate struct {
	*sqlite.LockingStore
	armed   atomic.Bool
	entered chan context.Context
}

func (g *directoryAdmissionGate) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (metastore.NodeOpenResult, error) {
	if options.Kind == storage.NodeDirectory && g.armed.CompareAndSwap(true, false) {
		g.entered <- ctx
		<-ctx.Done()
	}
	return g.LockingStore.OpenNodeRef(ctx, id, options)
}

type directoryOpenStart struct {
	session storage.FileActions
	action  storage.FileActionID
}

type directoryOpenResult struct {
	err       error
	reference bool
	receipt   storage.FileActionReceipt
	queryErr  error
}

type directorySignalStorage struct {
	storage.FileStorage
	t       *testing.T
	entered chan directoryOpenStart
	results chan directoryOpenResult
	session storage.FileSession
	closed  chan struct{}
	closes  atomic.Int32
}

func (s *directorySignalStorage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := s.FileStorage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	s.session = session
	return &directorySignalSession{capableTestSession: testSessionCapabilities(session), observe: s}, nil
}

type directorySignalSession struct {
	capableTestSession
	observe *directorySignalStorage
}

func (s *directorySignalSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.observe.entered <- directoryOpenStart{session: s.FileActions, action: options.Action}
	result, err := s.NodeReferences.OpenNodeRef(ctx, id, options)
	if result.Reference != nil {
		result.Reference = &countedDirectoryReference{NodeReference: result.Reference, observe: s.observe}
	}
	query, cancel := context.WithTimeout(s.observe.t.Context(), 5*time.Second)
	receipt, queryErr := s.FileActions.QueryFileAction(query, options.Action)
	cancel()
	observation := directoryOpenResult{err: err, reference: result.Reference != nil, receipt: receipt, queryErr: queryErr}
	if err != nil {
		s.observe.t.Logf("native directory rejection: cause=%T: %v reference=%v outcome=%v receipt=%v query=%v", err, err, result.Reference != nil, result.Outcome, receipt.Outcome, queryErr)
	}
	s.observe.results <- observation
	return result, err
}

type countedDirectoryReference struct {
	storage.NodeReference
	observe *directorySignalStorage
}

func (r *countedDirectoryReference) Close(ctx context.Context) error {
	r.observe.closes.Add(1)
	err := r.NodeReference.Close(ctx)
	if err == nil {
		r.observe.closed <- struct{}{}
	}
	return err
}

func receiveDirectoryStart(t *testing.T, ctx context.Context, source <-chan directoryOpenStart) directoryOpenStart {
	t.Helper()
	select {
	case start := <-source:
		return start
	case <-ctx.Done():
		t.Fatalf("waiting for directory open: %v", ctx.Err())
		return directoryOpenStart{}
	}
}

func receiveDirectoryResult(t *testing.T, source <-chan directoryOpenResult) directoryOpenResult {
	t.Helper()
	select {
	case result := <-source:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("directory result was not observed")
		return directoryOpenResult{}
	}
}
