package httprest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"syscall"
)

var errBodyTooLarge = errors.New("the HTTP body exceeds its configured limit")

// requireEmptyBody reads at most one byte. Declared bodies are refused from their
// framing. Chunked and other unknown-length bodies first enter the operation admission,
// because proving their emptiness can wait for the peer to finish the body.
func (h *Handler) requireEmptyBody(r *http.Request) error {
	if r.ContentLength > 0 {
		return fmt.Errorf("the operation takes no request body, but %d bytes were announced", r.ContentLength)
	}
	release, err := h.bodies.acquireOperation(r.Context())
	if err != nil {
		return fmt.Errorf("the request ended before its empty body could be verified: %w", err)
	}
	defer release()

	var unexpected [1]byte
	n, err := io.ReadFull(r.Body, unexpected[:])
	if n != 0 {
		return errors.New("the operation takes no request body, but at least one byte arrived")
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return fmt.Errorf("the request body could not be verified as empty: %w", err)
}

// readAtMost assembles no more than limit bytes. When exactly limit arrive, the first
// excess byte is read into a fixed buffer so detecting an oversized body retains no
// additional payload.
func readAtMost(from io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(from, limit))
	if err != nil || int64(len(body)) < limit {
		return body, err
	}
	var excess [1]byte
	n, err := io.ReadFull(from, excess[:])
	if n != 0 {
		return nil, fmt.Errorf("%w of %d bytes", errBodyTooLarge, limit)
	}
	if errors.Is(err, io.EOF) {
		return body, nil
	}
	return nil, err
}

// bodyAdmission bounds the number of request bodies and the aggregate bytes reserved for
// them. Each body reserves its operation's maximum before it is read: Content-Length may
// be absent or false, so a smaller reservation would let the peer choose the excess.
type bodyAdmission struct {
	mu            sync.Mutex
	maxOperations int
	operations    int
	maxBytes      int64
	bytes         int64
	maxWaiters    int
	waiters       int
	changed       chan struct{}
}

func newBodyAdmission(operations int, bytes int64, waiters int) *bodyAdmission {
	return &bodyAdmission{
		maxOperations: operations,
		maxBytes:      bytes,
		maxWaiters:    waiters,
		changed:       make(chan struct{}),
	}
}

func (a *bodyAdmission) acquire(ctx context.Context, reservation int64) (func(), error) {
	if reservation < 0 || reservation > a.maxBytes {
		return nil, fmt.Errorf("an admission reservation of %d bytes cannot fit under the %d-byte bound: %w",
			reservation, a.maxBytes, syscall.EFBIG)
	}
	waiting := false
	for {
		if err := ctx.Err(); err != nil {
			if waiting {
				a.mu.Lock()
				a.waiters--
				a.mu.Unlock()
			}
			return nil, err
		}
		a.mu.Lock()
		if a.operations < a.maxOperations && reservation <= a.maxBytes-a.bytes {
			if waiting {
				a.waiters--
			}
			a.operations++
			a.bytes += reservation
			a.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					a.mu.Lock()
					a.operations--
					a.bytes -= reservation
					a.notifyLocked()
					a.mu.Unlock()
				})
			}, nil
		}
		if !waiting {
			if a.waiters >= a.maxWaiters {
				a.mu.Unlock()
				return nil, fmt.Errorf("the bounded admission queue already holds %d waiters: %w", a.maxWaiters, syscall.EAGAIN)
			}
			a.waiters++
			waiting = true
		}
		changed := a.changed
		a.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			a.mu.Lock()
			a.waiters--
			a.mu.Unlock()
			return nil, ctx.Err()
		}
	}
}

func (a *bodyAdmission) acquireOperation(ctx context.Context) (func(), error) {
	return a.acquire(ctx, 0)
}

func (a *bodyAdmission) notifyLocked() {
	close(a.changed)
	a.changed = make(chan struct{})
}
