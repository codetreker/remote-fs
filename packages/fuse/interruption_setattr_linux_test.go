//go:build linux

package fuse_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/storage"
	"golang.org/x/sys/unix"
)

type confirmedChmodResult struct {
	ctx  context.Context
	attr storage.Attr
}
type confirmedChmodGate struct {
	entered   chan confirmedChmodResult
	release   chan struct{}
	once      sync.Once
	armed     atomic.Bool
	postReads atomic.Int32
}

func (g *confirmedChmodGate) unblock() { g.once.Do(func() { close(g.release) }) }
func (g *confirmedChmodGate) result(ctx context.Context, c storage.AttrChange, a storage.Attr, err error) (storage.Attr, error) {
	if err == nil && c.Mode != nil && c.Mode.Perm() == 0640 && g.armed.CompareAndSwap(false, true) {
		g.entered <- confirmedChmodResult{ctx, a}
		<-g.release
	}
	return a, err
}

type confirmedChmodStorage struct {
	storage.FileStorage
	gate *confirmedChmodGate
}

func (s *confirmedChmodStorage) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, error) {
	f, e := s.FileStorage.NewFileSession(ctx, o)
	if e != nil {
		return nil, e
	}
	return &confirmedChmodSession{f, s.gate}, nil
}

type confirmedChmodSession struct {
	storage.FileSession
	gate *confirmedChmodGate
}

func (s *confirmedChmodSession) SetNodeAttr(ctx context.Context, id uint64, c storage.AttrChange) (storage.Attr, error) {
	a, e := s.FileSession.SetNodeAttr(ctx, id, c)
	return s.gate.result(ctx, c, a, e)
}
func (s *confirmedChmodSession) StatNode(ctx context.Context, id uint64) (storage.Attr, error) {
	if s.gate.armed.Load() && ctx.Err() != nil {
		s.gate.postReads.Add(1)
	}
	return s.FileSession.StatNode(ctx, id)
}
func (s *confirmedChmodSession) OpenFile(ctx context.Context, p string, o storage.FileOpenOptions) (storage.File, error) {
	f, e := s.FileSession.OpenFile(ctx, p, o)
	if e != nil {
		return nil, e
	}
	return &confirmedChmodFile{f, s.gate}, nil
}
func (s *confirmedChmodSession) OpenNode(ctx context.Context, id uint64, o storage.FileOpenOptions) (storage.File, error) {
	f, e := s.FileSession.OpenNode(ctx, id, o)
	if e != nil {
		return nil, e
	}
	return &confirmedChmodFile{f, s.gate}, nil
}

type confirmedChmodFile struct {
	storage.File
	gate *confirmedChmodGate
}

func (f *confirmedChmodFile) SetAttr(ctx context.Context, c storage.AttrChange) (storage.Attr, error) {
	a, e := f.File.SetAttr(ctx, c)
	return f.gate.result(ctx, c, a, e)
}
func (f *confirmedChmodFile) Stat(ctx context.Context) (storage.Attr, error) {
	if f.gate.armed.Load() && ctx.Err() != nil {
		f.gate.postReads.Add(1)
	}
	return f.File.Stat(ctx)
}

func TestSignalAfterConfirmedChmodUsesReturnedAttributes(t *testing.T) {
	if os.Getenv(signalChildMode) != "" {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		report := os.NewFile(3, "thread ID")
		defer report.Close()
		if _, err := fmt.Fprintln(report, unix.Gettid()); err != nil {
			t.Fatal(err)
		}
		name := os.Getenv("REMOTE_FS_SIGNAL_PATH")
		held, err := os.OpenFile(name, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer held.Close()
		readonly, err := os.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		defer readonly.Close()
		if err := os.Rename(filepath.Join(filepath.Dir(name), "incoming"), name); err != nil {
			t.Fatal(err)
		}
		if _, err := held.WriteAt([]byte("OLD"), 0); err != nil {
			t.Fatal(err)
		}
		if err := held.Truncate(4); err != nil {
			t.Fatal(err)
		}
		if err := readonly.Chmod(0640); err != nil {
			t.Fatalf("confirmed chmod interrupted before its reply: %v", err)
		}
		info, err := readonly.Stat()
		if err != nil || info.Mode().Perm() != 0640 {
			t.Fatalf("confirmed mode=%v %v", info, err)
		}
		body, err := io.ReadAll(readonly)
		if err != nil || string(body) != "OLDg" {
			t.Fatalf("retained descriptor returned %q, %v", body, err)
		}
		body, err = os.ReadFile(name)
		if err != nil || string(body) != "replacement" {
			t.Fatalf("replacement returned %q, %v", body, err)
		}
		return
	}
	requireFUSE(t)
	volume := newSignalVolume(t, 0, map[string][]byte{"file": []byte("original"), "incoming": []byte("replacement")})
	gate := &confirmedChmodGate{entered: make(chan confirmedChmodResult, 1), release: make(chan struct{})}
	defer gate.unblock()
	point := mountStorage(t, &confirmedChmodStorage{volume.served, gate}, fuse.Options{Logger: testLogger(t), Debug: true})
	runSignalProcess(t, "TestSignalAfterConfirmedChmodUsesReturnedAttributes", "chmod", filepath.Join(point, "file"), func(ctx context.Context, pid, tid int) {
		var result confirmedChmodResult
		select {
		case result = <-gate.entered:
		case <-ctx.Done():
			t.Fatal("chmod did not return confirmed backend result")
		}
		if result.attr.Mode.Perm() != 0640 || result.ctx.Err() != nil {
			t.Fatal("mutation did not confirm before signal")
		}
		if err := unix.Tgkill(pid, tid, syscall.SIGURG); err != nil {
			t.Fatal(err)
		}
		select {
		case <-result.ctx.Done():
		case <-ctx.Done():
			t.Fatal("SIGURG did not cancel FUSE request")
		}
		gate.unblock()
	})
	if gate.postReads.Load() != 0 {
		t.Fatalf("post-confirm canceled reads=%d; want 0", gate.postReads.Load())
	}
}
