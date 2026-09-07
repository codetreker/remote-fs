package fuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
)

type mutationStorage struct {
	storage.Storage
	before func(context.Context, string) error
	after  func(string)
}

func (s *mutationStorage) enter(ctx context.Context, op string) error {
	if s.before != nil {
		return s.before(ctx, op)
	}
	return nil
}

func (s *mutationStorage) finish(op string, err error) error {
	if err == nil && s.after != nil {
		s.after(op)
	}
	return err
}

func (s *mutationStorage) Create(ctx context.Context, path string) error {
	if err := s.enter(ctx, "create"); err != nil {
		return err
	}
	return s.finish("create", s.Storage.Create(ctx, path))
}

func (s *mutationStorage) Mkdir(ctx context.Context, path string) error {
	if err := s.enter(ctx, "mkdir"); err != nil {
		return err
	}
	return s.finish("mkdir", s.Storage.Mkdir(ctx, path))
}

func (s *mutationStorage) Write(ctx context.Context, path string, body []byte) error {
	if err := s.enter(ctx, "write"); err != nil {
		return err
	}
	return s.finish("write", s.Storage.Write(ctx, path, body))
}

func (s *mutationStorage) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	if err := s.enter(ctx, "setattr"); err != nil {
		return err
	}
	return s.finish("setattr", s.Storage.SetAttr(ctx, path, change))
}

func (s *mutationStorage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	if err := s.enter(ctx, "stat"); err != nil {
		return storage.Attr{}, err
	}
	attr, err := s.Storage.Stat(ctx, path)
	return attr, s.finish("stat", err)
}

func mutationTree(t *testing.T) (*node, *node, *mutationStorage) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	local, err := localdir.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := local.Close(); err != nil {
			t.Error(err)
		}
	})
	downstream := &mutationStorage{Storage: local}
	root := &node{ns: &namespace{storage: downstream, maxFileSize: 1024, flushTimeout: DefaultFlushTimeout}, id: rootIdentity()}
	fs.NewNodeFS(root, &fs.Options{})
	child, errno := root.Lookup(t.Context(), "f", &gofuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	if !root.AddChild("f", child, false) {
		t.Fatal("attaching the file inode")
	}
	return root, child.Operations().(*node), downstream
}

func TestCreationCancellationAccountsForCompletedStages(t *testing.T) {
	for _, directory := range []bool{false, true} {
		for _, stage := range []string{"creation", "mode", "attributes"} {
			t.Run(stage+map[bool]string{false: " file", true: " directory"}[directory], func(t *testing.T) {
				root, _, downstream := mutationTree(t)
				creation := "create"
				if directory {
					creation = "mkdir"
				}
				failedOp := map[string]string{"creation": creation, "mode": "setattr", "attributes": "stat"}[stage]
				downstream.before = func(_ context.Context, op string) error {
					if op == failedOp {
						return context.Canceled
					}
					return nil
				}
				var errno syscall.Errno
				if directory {
					_, errno = root.Mkdir(t.Context(), "new", 0o700, &gofuse.EntryOut{})
				} else {
					_, _, _, errno = root.Create(t.Context(), "new", 0, 0o600, &gofuse.EntryOut{})
				}
				want := syscall.EIO
				if stage == "creation" {
					want = syscall.EINTR
				}
				if errno != want {
					t.Fatalf("creation interrupted at %s returned %v, want %v", stage, errno, want)
				}
				_, err := downstream.Storage.Stat(t.Context(), "new")
				if stage == "creation" && !errors.Is(err, syscall.ENOENT) || stage != "creation" && err != nil {
					t.Fatalf("namespace after interruption at %s: %v", stage, err)
				}
			})
		}
	}
}

func TestSetattrCancellationAccountsForCompletedStages(t *testing.T) {
	for _, test := range []struct {
		name       string
		valid      uint32
		buffer     bool
		dirty      int
		failOp     string
		failCall   int
		wantErrno  syscall.Errno
		wantWrites int
	}{
		{"resize read before write", gofuse.FATTR_SIZE, false, 0, "stat", 1, syscall.EINTR, 0},
		{"resize write refused", gofuse.FATTR_SIZE, false, 0, "write", 1, syscall.EINTR, 0},
		{"stored resize then attributes read", gofuse.FATTR_SIZE, false, 0, "stat", 2, syscall.EIO, 1},
		{"buffer resize then attributes read", gofuse.FATTR_SIZE, true, 0, "stat", 1, syscall.EIO, 0},
		{"resize then mode", gofuse.FATTR_SIZE | gofuse.FATTR_MODE, false, 0, "setattr", 1, syscall.EIO, 1},
		{"buffer resize then mode", gofuse.FATTR_SIZE | gofuse.FATTR_MODE, true, 0, "setattr", 1, syscall.EIO, 0},
		{"mode mutation interrupted", gofuse.FATTR_MODE, false, 0, "setattr", 1, syscall.EIO, 0},
		{"mode then attributes read", gofuse.FATTR_MODE, false, 0, "stat", 1, syscall.EIO, 0},
		{"first pending commit refused", gofuse.FATTR_MTIME, false, 2, "write", 1, syscall.EINTR, 0},
		{"partial pending commits", gofuse.FATTR_MTIME, false, 2, "write", 2, syscall.EIO, 1},
		{"committed handles then time", gofuse.FATTR_MTIME, false, 2, "setattr", 1, syscall.EIO, 2},
		{"buffer resize then refused commit", gofuse.FATTR_SIZE | gofuse.FATTR_MTIME, true, 0, "write", 1, syscall.EIO, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, n, downstream := mutationTree(t)
			var file fs.FileHandle
			if test.buffer {
				file = newHandle(n, []byte("contents"), committed, 0)
			}
			var pending []*handle
			for range test.dirty {
				pending = append(pending, newHandle(n, []byte("pending"), uncommitted, 0))
			}
			calls, writes := 0, 0
			downstream.before = func(_ context.Context, op string) error {
				if op == test.failOp {
					calls++
					if calls == test.failCall {
						return context.Canceled
					}
				}
				return nil
			}
			downstream.after = func(op string) {
				if op == "write" {
					writes++
				}
			}
			in := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid: test.valid, Size: 3, Mode: 0o640, Mtime: 123456789,
			}}
			err := n.setattr(t.Context(), file, in, &gofuse.AttrOut{})
			if got := errnoOf(err); got != test.wantErrno || !errors.Is(err, context.Canceled) {
				t.Fatalf("setattr = %v (%v), want %v retaining cancellation", err, got, test.wantErrno)
			}
			if writes != test.wantWrites {
				t.Fatalf("setattr completed %d writes, want %d", writes, test.wantWrites)
			}
			if test.buffer && test.valid&gofuse.FATTR_SIZE != 0 && len(file.(*handle).contents) != 3 {
				t.Fatalf("resized buffer holds %d bytes, want 3", len(file.(*handle).contents))
			}
			if test.dirty > 0 {
				clean := 0
				for _, h := range pending {
					if !h.dirty {
						clean++
					}
				}
				if clean != test.wantWrites {
					t.Fatalf("%d handles committed, want %d", clean, test.wantWrites)
				}
			}
		})
	}
}

func TestSetattrCancellationBetweenSuccessfulMutationStages(t *testing.T) {
	for _, stage := range []string{"write", "setattr"} {
		t.Run(stage, func(t *testing.T) {
			_, n, downstream := mutationTree(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			setters := 0
			downstream.before = func(ctx context.Context, op string) error {
				if op == "setattr" {
					setters++
				}
				return ctx.Err()
			}
			downstream.after = func(op string) {
				if op == stage {
					cancel()
				}
			}
			in := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid: gofuse.FATTR_SIZE | gofuse.FATTR_MODE, Size: 3, Mode: 0o640,
			}}
			if errno := n.Setattr(ctx, nil, in, &gofuse.AttrOut{}); errno != syscall.EIO {
				t.Fatalf("setattr canceled after %s = %v, want EIO", stage, errno)
			}
			if stage == "write" && setters != 0 || stage == "setattr" && setters != 1 {
				t.Fatalf("setattr canceled after %s attempted %d attribute mutations", stage, setters)
			}
		})
	}
}

func TestMutationClassificationPreservesIndependentFailures(t *testing.T) {
	fault := errors.New("namespace unavailable")
	for _, cause := range []error{
		syscall.EACCES,
		fault,
		errors.Join(context.Canceled, fault),
		errors.Join(fault, context.Canceled),
		context.DeadlineExceeded,
	} {
		if got := afterMutation(true, cause); got != cause {
			t.Errorf("mutation changed an independent failure %v to %v", cause, got)
		}
	}
	cause := fmt.Errorf("reading attributes after resize: %w", context.Canceled)
	err := afterMutation(true, cause)
	if errnoOf(err) != syscall.EIO || !errors.Is(err, cause) || !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("partial mutation lost its cause or classification: %v", err)
	}
	diagnostic := err.Error()
	for _, detail := range []string{"after a change", cause.Error(), syscall.EIO.Error()} {
		if !strings.Contains(diagnostic, detail) {
			t.Errorf("partial mutation diagnostic omits %q: %q", detail, diagnostic)
		}
	}
}

func TestMutationSuccessIgnoresLateCancellation(t *testing.T) {
	for _, operation := range []string{"create", "mkdir", "setattr"} {
		t.Run(operation, func(t *testing.T) {
			root, n, downstream := mutationTree(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			downstream.after = func(op string) {
				if op == "stat" {
					cancel()
				}
			}
			var errno syscall.Errno
			switch operation {
			case "create":
				_, _, _, errno = root.Create(ctx, "new", 0, 0o600, &gofuse.EntryOut{})
			case "mkdir":
				_, errno = root.Mkdir(ctx, "new", 0o700, &gofuse.EntryOut{})
			case "setattr":
				errno = n.Setattr(ctx, nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
					Valid: gofuse.FATTR_MODE, Mode: 0o600,
				}}, &gofuse.AttrOut{})
			}
			if errno != 0 || ctx.Err() != context.Canceled {
				t.Fatalf("completed %s returned %v, context %v", operation, errno, ctx.Err())
			}
		})
	}
}

func TestSetattrCanceledBeforeStartingHasNoEffects(t *testing.T) {
	_, n, downstream := mutationTree(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	downstream.before = func(context.Context, string) error {
		t.Fatal("a canceled setattr reached storage")
		return nil
	}
	h := newHandle(n, []byte("contents"), committed, 0)
	errno := n.Setattr(ctx, h, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		Valid: gofuse.FATTR_SIZE | gofuse.FATTR_MODE, Size: 3, Mode: 0o600,
	}}, &gofuse.AttrOut{})
	if errno != syscall.EINTR || h.dirty || string(h.contents) != "contents" {
		t.Fatalf("pre-canceled setattr returned %v and left buffer %q dirty %t", errno, h.contents, h.dirty)
	}
}
