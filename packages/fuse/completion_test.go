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

	"github.com/hanwen/go-fuse/v2/fs"

	"github.com/codetreker/remote-fs/packages/storage"
)

type completionStorage struct {
	storage.Storage
	write func(context.Context) error
}

func (s completionStorage) Write(ctx context.Context, _ string, _ []byte) error {
	return s.write(ctx)
}

func completionHandle(t *testing.T, opts Options, write func(context.Context) error) *handle {
	t.Helper()
	timeout, err := opts.flushTimeout()
	if err != nil {
		t.Fatal(err)
	}
	n := &node{
		ns: &namespace{storage: completionStorage{write: write}, flushTimeout: timeout},
		id: rootIdentity(),
	}
	fs.NewNodeFS(n, &fs.Options{})
	return newHandle(n, []byte("pending"), uncommitted, 0)
}

func TestNegativeFlushTimeoutRefusesBeforeMounting(t *testing.T) {
	mount, err := New(t.TempDir(), nil, Options{FlushTimeout: -time.Nanosecond})
	if err == nil {
		if mount != nil {
			mount.Unmount()
		}
		t.Fatal("mount accepted a negative FlushTimeout")
	}
	if mount != nil || !strings.Contains(err.Error(), "FlushTimeout") {
		t.Fatalf("negative FlushTimeout returned mount %v, error %v", mount, err)
	}
}

type completionValueKey struct{}

func TestCloseCommitUsesTheConfiguredOrEarlierDeadline(t *testing.T) {
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
				err := h.flushForClose(request)
				if !errors.Is(err, context.DeadlineExceeded) || errnoOf(err) != syscall.EIO || calls != 1 || !h.dirty {
					t.Fatalf("expired commit returned %v, calls=%d, dirty=%v", err, calls, h.dirty)
				}
				deadline, bounded := attempted.Deadline()
				if !bounded || !deadline.Equal(start.Add(test.want)) || attempted.Value(completionValueKey{}) != "request value" {
					t.Fatalf("attempt deadline=%v bounded=%v value=%v", deadline, bounded, attempted.Value(completionValueKey{}))
				}
				if elapsed := time.Since(start); elapsed != max(test.want, 0) {
					t.Fatalf("attempt used %s, want %s", elapsed, max(test.want, 0))
				}
			})
		})
	}
}

func TestCloseCommitCompletesDespiteRequestCancellation(t *testing.T) {
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
					return fmt.Errorf("commit started canceled: %w", err)
				}
				cancel()
				return ctx.Err()
			})
			if err := h.flushForClose(request); err != nil || calls != 1 || h.dirty || h.stored != int64(len("pending")) {
				t.Fatalf("close commit returned %v, calls=%d, dirty=%v, stored=%d", err, calls, h.dirty, h.stored)
			}
			if attempted.Err() != context.Canceled {
				t.Fatalf("completed attempt retained a live context: %v", attempted.Err())
			}
		})
	}
}

func TestCloseCommitKeepsSuccessAcknowledgedAfterItsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		h := completionHandle(t, Options{FlushTimeout: time.Second}, func(ctx context.Context) error {
			calls++
			<-ctx.Done()
			return nil
		})
		if err := h.flushForClose(t.Context()); err != nil || calls != 1 || h.dirty {
			t.Fatalf("acknowledged commit returned %v, calls=%d, dirty=%v", err, calls, h.dirty)
		}
	})
}

func TestCloseCommitDoesNotRetryIndependentFailures(t *testing.T) {
	fault := errors.New("commit response unavailable")
	for _, cause := range []error{fault, errors.Join(context.Canceled, fault), errors.Join(fault, context.Canceled)} {
		request, cancel := context.WithCancel(t.Context())
		calls := 0
		h := completionHandle(t, Options{}, func(ctx context.Context) error {
			calls++
			cancel()
			return cause
		})
		err := h.flushForClose(request)
		cancel()
		if err != cause || errnoOf(err) != syscall.EIO || calls != 1 || !h.dirty {
			t.Fatalf("failed close returned %v, want original %v; calls=%d, dirty=%v", err, cause, calls, h.dirty)
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

func TestCloseCommitLockWaitConsumesTheOriginalBudget(t *testing.T) {
	// Mutex admission is not durably blocked for synctest, so this case uses a real
	// timer. Observing Deadline proves the attempt has sampled its start time while
	// the handle lock is still held.
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
	h.mu.Lock()
	done := make(chan error, 1)
	go func() { done <- h.flushForClose(request) }()
	select {
	case <-request.started:
	case <-time.After(time.Second):
		h.mu.Unlock()
		<-done
		t.Fatal("close commit did not establish its deadline before locking")
	}
	time.Sleep(2 * budget)
	h.mu.Unlock()
	err := <-done
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 || !h.dirty {
		t.Fatalf("lock-delayed commit returned %v, calls=%d, dirty=%v", err, calls, h.dirty)
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
				if errno := <-done; errno != syscall.EINTR || calls != 1 || !h.dirty ||
					h.stored != 0 || string(h.contents) != "pending" {
					t.Fatalf("canceled Fsync returned %v, calls=%d, dirty=%v, stored=%d, contents=%q",
						errno, calls, h.dirty, h.stored, h.contents)
				}
				if elapsed := time.Since(start); elapsed != 0 {
					t.Fatalf("Fsync waited %s after request cancellation", elapsed)
				}
			})
		})
	}
}
