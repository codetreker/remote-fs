package localdir

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestLocalContextErrorPreservesCancellationAndDeadline(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, finish := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer finish()
	for _, test := range []struct {
		ctx  context.Context
		want syscall.Errno
	}{
		{t.Context(), 0},
		{canceled, syscall.EINTR},
		{expired, syscall.EIO},
	} {
		err := localContextError(test.ctx, "read", "f")
		if storage.ErrnoOf(err) != test.want || !errors.Is(err, test.ctx.Err()) {
			t.Fatalf("local context %v returned %v, want %v retaining cause", test.ctx.Err(), err, test.want)
		}
	}
}
