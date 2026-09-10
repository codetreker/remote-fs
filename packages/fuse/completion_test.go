package fuse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type completionFile struct {
	storage.File
	finish func(context.Context) error
}

func (f completionFile) Close(ctx context.Context) error { return f.finish(ctx) }
func (f completionFile) Sync(ctx context.Context) error  { return f.finish(ctx) }

func completionHandle(t *testing.T, opts Options, finish func(context.Context) error) *handle {
	t.Helper()
	timeout, err := opts.flushTimeout()
	if err != nil {
		t.Fatal(err)
	}
	v := activeTestVolume(nil, 1024)
	v.flushTimeout = timeout
	return newHandle(&node{volume: v}, completionFile{finish: finish}, true, true)
}

func completeHandleClose(ctx context.Context, h *handle) error {
	completion, cancel := h.node.volume.cleanupContext(ctx)
	defer cancel()
	return h.closeFile(completion)
}

func TestNegativeFlushTimeoutRefusesBeforeMounting(t *testing.T) {
	mount, err := New(t.TempDir(), nil, Options{FlushTimeout: -time.Nanosecond})
	cleanupReturnedTestMount(t, mount)
	if err == nil {
		t.Fatal("mount accepted a negative FlushTimeout")
	}
	if mount != nil || !strings.Contains(err.Error(), "FlushTimeout") {
		t.Fatalf("negative FlushTimeout returned mount %v, error %v", mount, err)
	}
}

type completionValueKey struct{}

func TestCloseCleanupUsesTheConfiguredOrEarlierDeadline(t *testing.T) {
	for _, test := range []struct {
		name        string
		configured  time.Duration
		request     time.Duration
		withRequest bool
		want        time.Duration
	}{
		{name: "default", want: 30 * time.Second},
		{name: "configured", configured: 3 * time.Second, want: 3 * time.Second},
		{name: "earlier request", configured: 5 * time.Second, request: time.Second, withRequest: true, want: time.Second},
		{name: "later request", configured: time.Second, request: 5 * time.Second, withRequest: true, want: time.Second},
		{name: "expired request", configured: time.Second, request: -time.Second, withRequest: true, want: -time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				var request context.Context = context.WithValue(t.Context(), completionValueKey{}, "request value")
				if test.withRequest {
					bounded, cancel := context.WithDeadline(request, start.Add(test.request))
					defer cancel()
					request = bounded
				}
				var attempted context.Context
				calls := 0
				h := completionHandle(t, Options{FlushTimeout: test.configured}, func(ctx context.Context) error {
					attempted = ctx
					calls++
					<-ctx.Done()
					return ctx.Err()
				})
				err := completeHandleClose(request, h)
				if !errors.Is(err, context.DeadlineExceeded) || errnoOf(err) != syscall.EIO || calls != 1 {
					t.Fatalf("expired cleanup returned %v, calls=%d", err, calls)
				}
				deadline, bounded := attempted.Deadline()
				if !bounded || !deadline.Equal(start.Add(test.want)) || attempted.Value(completionValueKey{}) != "request value" {
					t.Fatalf("cleanup deadline=%v bounded=%v value=%v", deadline, bounded, attempted.Value(completionValueKey{}))
				}
				if elapsed := time.Since(start); elapsed != max(test.want, 0) {
					t.Fatalf("cleanup used %s, want %s", elapsed, max(test.want, 0))
				}
			})
		})
	}
}

func TestCloseCleanupCompletesDespiteRequestCancellation(t *testing.T) {
	for _, canceledBefore := range []bool{false, true} {
		t.Run(fmt.Sprintf("already canceled %t", canceledBefore), func(t *testing.T) {
			request, cancel := context.WithCancel(t.Context())
			defer cancel()
			if canceledBefore {
				cancel()
			}
			var attempted context.Context
			calls := 0
			h := completionHandle(t, Options{}, func(ctx context.Context) error {
				attempted = ctx
				calls++
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("cleanup started canceled: %w", err)
				}
				cancel()
				return ctx.Err()
			})
			if err := completeHandleClose(request, h); err != nil || calls != 1 {
				t.Fatalf("close cleanup returned %v, calls=%d", err, calls)
			}
			if attempted.Err() != context.Canceled || h.closeDone == nil {
				t.Fatalf("cleanup left a live completion context or reference: %v", attempted.Err())
			}
			if err := completeHandleClose(t.Context(), h); err != nil || calls != 1 {
				t.Fatalf("repeated close executed cleanup again: %v, calls=%d", err, calls)
			}
		})
	}
}

func TestCloseCleanupKeepsSuccessAcknowledgedAfterItsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		h := completionHandle(t, Options{FlushTimeout: time.Second}, func(ctx context.Context) error {
			calls++
			<-ctx.Done()
			return nil
		})
		if err := completeHandleClose(t.Context(), h); err != nil || calls != 1 {
			t.Fatalf("acknowledged cleanup returned %v, calls=%d", err, calls)
		}
	})
}

func TestCloseCleanupDoesNotRetryIndependentFailures(t *testing.T) {
	fault := errors.New("cleanup response unavailable")
	for _, cause := range []error{fault, errors.Join(context.Canceled, fault), errors.Join(fault, context.Canceled)} {
		request, cancel := context.WithCancel(t.Context())
		calls := 0
		h := completionHandle(t, Options{}, func(ctx context.Context) error {
			calls++
			cancel()
			return cause
		})
		err := completeHandleClose(request, h)
		cancel()
		if err != cause || errnoOf(err) != syscall.EIO || calls != 1 {
			t.Fatalf("failed close returned %v, want original %v; calls=%d", err, cause, calls)
		}
		if again := completeHandleClose(t.Context(), h); again != err || calls != 1 {
			t.Fatalf("repeated failed cleanup returned %v, calls=%d", again, calls)
		}
		if errnoOf(h.node.volume.check()) != syscall.EIO {
			t.Fatal("unknown reference cleanup did not fence the volume")
		}
	}
}

type observedCompletionContext struct {
	context.Context
	started chan struct{}
}

func (c observedCompletionContext) Deadline() (time.Time, bool) {
	close(c.started)
	return c.Context.Deadline()
}

func TestCloseCleanupLockWaitConsumesTheOriginalBudget(t *testing.T) {
	const budget = 20 * time.Millisecond
	request := observedCompletionContext{Context: t.Context(), started: make(chan struct{})}
	calls := 0
	h := completionHandle(t, Options{FlushTimeout: budget}, func(ctx context.Context) error {
		calls++
		deadline, bounded := ctx.Deadline()
		if !bounded || deadline.After(time.Now()) {
			return fmt.Errorf("lock wait received a fresh deadline %v, bounded=%v", deadline, bounded)
		}
		<-ctx.Done()
		return ctx.Err()
	})
	h.closeMu.Lock()
	done := make(chan error, 1)
	go func() { done <- completeHandleClose(request, h) }()
	select {
	case <-request.started:
	case <-time.After(time.Second):
		h.closeMu.Unlock()
		<-done
		t.Fatal("close cleanup did not establish its deadline before locking")
	}
	time.Sleep(2 * budget)
	h.closeMu.Unlock()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("lock-delayed cleanup returned %v, calls=%d", err, calls)
	}
}

func TestFsyncRetainsRequestCancellation(t *testing.T) {
	for _, canceledBefore := range []bool{false, true} {
		t.Run(fmt.Sprintf("already canceled %t", canceledBefore), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				request, cancel := context.WithCancel(t.Context())
				defer cancel()
				if canceledBefore {
					cancel()
				}
				entered := make(chan struct{})
				calls := 0
				h := completionHandle(t, Options{}, func(ctx context.Context) error {
					calls++
					close(entered)
					<-ctx.Done()
					return ctx.Err()
				})
				start := time.Now()
				done := make(chan syscall.Errno, 1)
				go func() { done <- h.Fsync(request, 0) }()
				<-entered
				cancel()
				if errno := <-done; errno != syscall.EINTR || calls != 1 || h.closeDone != nil {
					t.Fatalf("canceled Fsync returned %v, calls=%d, retired=%v", errno, calls, h.closeDone != nil)
				}
				if elapsed := time.Since(start); elapsed != 0 {
					t.Fatalf("Fsync waited %s after request cancellation", elapsed)
				}
			})
		})
	}
}
