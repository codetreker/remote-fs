package fuse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/storage/locked"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type contextSpace struct {
	storage.Storage
	query func(context.Context) (storage.Space, error)
}

func (s contextSpace) Space(ctx context.Context) (storage.Space, error) { return s.query(ctx) }

func TestInterruptedQuotaQueryLeavesBufferUntouchedAndRetryMeasuresAgain(t *testing.T) {
	for _, interruption := range []error{context.Canceled, syscall.EINTR, fmt.Errorf("quota: %w", syscall.EINTR)} {
		for _, resize := range []bool{false, true} {
			for _, hadMeasurement := range []bool{false, true} {
				name := interruption.Error() + " " + map[bool]string{false: "write", true: "resize"}[resize]
				if hadMeasurement {
					name += " with prior measurement"
				}
				t.Run(name, func(t *testing.T) {
					calls := 0
					s := contextSpace{query: func(ctx context.Context) (storage.Space, error) {
						calls++
						if err := ctx.Err(); err != nil {
							return storage.Space{}, interruption
						}
						return storage.Space{Total: 65536, Used: 32768, Avail: 32768}, nil
					}}
					n := &node{ns: &namespace{storage: s, maxFileSize: 1 << 20, flushTimeout: DefaultFlushTimeout}}
					h := newHandle(n, nil, committed, 0)
					g := &n.ns.room
					if hadMeasurement {
						g.asked = time.Now().Add(-2 * roomWindow)
						g.avail, g.measured = 65536, true
					}
					asked, avail, measured := g.asked, g.avail, g.measured
					ctx, cancel := context.WithCancel(t.Context())
					cancel()
					change := func(ctx context.Context) syscall.Errno {
						if resize {
							return errnoOf(h.resize(ctx, 36864))
						}
						written, errno := h.Write(ctx, bytes.Repeat([]byte("x"), 36864), 0)
						if written != 0 {
							t.Fatalf("refused write accepted %d bytes", written)
						}
						return errno
					}
					if errno := change(ctx); errno != syscall.EINTR {
						t.Fatalf("interrupted change returned %v, want EINTR", errno)
					}
					if len(h.contents) != 0 || h.dirty || calls != 1 || g.asking {
						t.Fatalf("interrupted change: bytes=%d dirty=%t calls=%d asking=%t", len(h.contents), h.dirty, calls, g.asking)
					}
					if g.asked != asked || g.avail != avail || g.measured != measured {
						t.Fatal("interrupted query changed the prior measurement or its timestamp")
					}
					if errno := change(t.Context()); errno != syscall.EDQUOT || calls != 2 {
						t.Fatalf("immediate retry returned %v after %d queries, want EDQUOT after a second query", errno, calls)
					}
					if len(h.contents) != 0 || h.dirty {
						t.Fatal("refused retry changed the buffer")
					}
				})
			}
		}
	}
}

func TestQuotaQueryCancellationKeepsItsCauseAndDoesNotHideOtherFailures(t *testing.T) {
	fault := errors.New("measurement service unavailable")
	for _, test := range []struct {
		name        string
		cause       error
		interrupted bool
	}{
		{"cancellation", context.Canceled, true},
		{"wrapped cancellation", fmt.Errorf("measurement: %w", context.Canceled), true},
		{"deadline", context.DeadlineExceeded, false},
		{"independent fault", fault, false},
		{"cancellation before fault", errors.Join(context.Canceled, fault), false},
		{"fault before cancellation", errors.Join(fault, context.Canceled), false},
		{"named interruption", syscall.EINTR, true},
		{"wrapped named interruption", fmt.Errorf("measurement: %w", syscall.EINTR), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := contextSpace{query: func(context.Context) (storage.Space, error) {
				return storage.Space{}, test.cause
			}}
			old := time.Now().Add(-2 * roomWindow)
			g := roomGauge{asked: old, avail: 123, measured: true}
			avail, measured, err := g.remaining(t.Context(), s)
			if avail != 123 || !measured || g.asking {
				t.Fatalf("failed query changed standing space: %d %t asking=%t", avail, measured, g.asking)
			}
			if test.interrupted {
				if !errors.Is(err, test.cause) || errnoOf(err) != syscall.EINTR || g.asked != old {
					t.Fatalf("pure interruption returned %v, timestamp %v, want original cause and timestamp", err, g.asked)
				}
			} else if err != nil || g.asked == old {
				t.Fatalf("independent failure changed the advisory measurement policy: %v, timestamp %v", err, g.asked)
			}
		})
	}
}

func TestSuccessfulQuotaQueryIgnoresLateCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := contextSpace{query: func(context.Context) (storage.Space, error) {
		cancel()
		return storage.Space{Total: 100, Used: 20, Avail: 80}, nil
	}}
	var g roomGauge
	avail, measured, err := g.remaining(ctx, s)
	if err != nil || !measured || avail != 80 || g.asked.IsZero() {
		t.Fatalf("successful query with late cancellation = %d %t %v", avail, measured, err)
	}
}

type namedInterruptionSpace struct {
	locked.Backend
	calls atomic.Int32
}

func (s *namedInterruptionSpace) Space(context.Context) (storage.Space, error) {
	if s.calls.Add(1) == 1 {
		return storage.Space{}, syscall.EINTR
	}
	return storage.Space{Total: 65536, Used: 32768, Avail: 32768}, nil
}

func TestQuotaQueryHonorsWireInterruptionAndImmediatelyMeasuresAgain(t *testing.T) {
	config := localdir.Config{Root: t.TempDir(), StateRoot: t.TempDir(), Locks: locking.DefaultOptions(), Limits: localdir.DefaultLimits()}
	if err := os.Chmod(config.StateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := localdir.Init(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	local, err := localdir.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := local.Close(); err != nil {
			t.Error(err)
		}
	})
	backend := &namedInterruptionSpace{Backend: local}
	handler, err := httprest.NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	remote, err := httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var observed error
	recording := contextSpace{query: func(ctx context.Context) (storage.Space, error) {
		space, err := remote.Space(ctx)
		observed = err
		return space, err
	}}
	n := &node{ns: &namespace{storage: recording, maxFileSize: 1 << 20, flushTimeout: DefaultFlushTimeout}}
	h := newHandle(n, nil, committed, 0)
	payload := bytes.Repeat([]byte("x"), 36864)
	if written, errno := h.Write(t.Context(), payload, 0); written != 0 || errno != syscall.EINTR {
		t.Fatalf("wire interruption returned %d bytes, %v", written, errno)
	}
	if errors.Is(observed, context.Canceled) || !errors.Is(observed, syscall.EINTR) {
		t.Fatalf("wire interruption must carry the named errno without a local cancellation cause: %v", observed)
	}
	if !n.ns.room.asked.IsZero() || n.ns.room.measured || h.dirty || len(h.contents) != 0 {
		t.Fatal("wire interruption published a measurement cooldown or changed the buffer")
	}
	if written, errno := h.Write(t.Context(), payload, 0); written != 0 || errno != syscall.EDQUOT || backend.calls.Load() != 2 {
		t.Fatalf("wire interruption retry returned %d bytes, %v, after %d probes", written, errno, backend.calls.Load())
	}
	if h.dirty || len(h.contents) != 0 {
		t.Fatal("quota refusal changed the buffer")
	}
}
