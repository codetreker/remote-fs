package replicated_test

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestEndedMutationContextDoesNotReachHTTP(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mounted, _ := mount(t, s)
	cancelCause := errors.New("the owner withdrew this request")
	deadlineCause := errors.New("the caller's deadline expired")
	for _, test := range []struct {
		name     string
		context  func() (context.Context, context.CancelFunc)
		identity error
		cause    error
		errno    syscall.Errno
	}{
		{
			name: "cancelled", identity: context.Canceled, cause: context.Canceled, errno: syscall.EINTR,
			context: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx, cancel
			},
		},
		{
			name: "custom cancellation cause", identity: context.Canceled, cause: cancelCause, errno: syscall.EINTR,
			context: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancelCause(t.Context())
				cancel(cancelCause)
				return ctx, func() { cancel(nil) }
			},
		},
		{
			name: "deadline", identity: context.DeadlineExceeded, cause: deadlineCause, errno: syscall.EIO,
			context: func() (context.Context, context.CancelFunc) {
				return context.WithDeadlineCause(t.Context(), time.Now().Add(-time.Second), deadlineCause)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := test.context()
			defer cancel()
			mode := fs.FileMode(0o600)
			for _, operation := range []struct {
				name string
				op   string
				path string
				run  func() error
			}{
				{"create", "create", "f", func() error { return mounted.Create(ctx, "f") }},
				{"write", "write", "f", func() error { return mounted.Write(ctx, "f", []byte("content")) }},
				{"setattr", "setattr", "f", func() error { return mounted.SetAttr(ctx, "f", storage.AttrChange{Mode: &mode}) }},
				{"mkdir", "mkdir", "d", func() error { return mounted.Mkdir(ctx, "d") }},
				{"remove", "unlink", "f", func() error { return mounted.Remove(ctx, "f") }},
				{"removedir", "rmdir", "d", func() error { return mounted.RemoveDir(ctx, "d") }},
				{"rename", "rename", "g", func() error { return mounted.Rename(ctx, "f", "g") }},
			} {
				t.Run(operation.name, func(t *testing.T) {
					before := s.calls.snapshot()
					err := operation.run()
					if storage.ErrnoOf(err) != test.errno || !errors.Is(err, test.errno) || !errors.Is(err, test.identity) || !errors.Is(err, test.cause) {
						t.Fatalf("ended mutation context returned %v, want errno %v with identity %v and cause %v", err, test.errno, test.identity, test.cause)
					}
					diagnostic := err.Error()
					if !strings.HasPrefix(diagnostic, operation.op+" "+operation.path+": ") {
						t.Fatalf("refusal diagnostic does not identify %s on %q: %q", operation.op, operation.path, diagnostic)
					}
					for _, detail := range []string{"before the mutation was sent", test.identity.Error(), test.cause.Error(), test.errno.Error()} {
						if !strings.Contains(diagnostic, detail) {
							t.Fatalf("refusal diagnostic omits %q: %q", detail, diagnostic)
						}
					}
					if arrived := s.calls.since(before); arrived != "" {
						t.Fatalf("a mutation with an ended context reached HTTP: %s", arrived)
					}
				})
			}
		})
	}
}
