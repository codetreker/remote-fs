package localdisk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
)

type healthState struct {
	mu  sync.Mutex
	err error
}

func (h *healthState) failure() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err == nil {
		return nil
	}
	return h.failureLocked()
}

// finish orders every result that asserts storage state against poison. A poisoned store
// replaces even fact-bearing errors such as ENOENT, EEXIST, EFBIG, and ENOSPC with EIO;
// otherwise the supplied result becomes observable before a later poison.
func (h *healthState) finish(ctx context.Context, result error) error {
	if result != nil && ctx.Err() != nil && errors.Is(result, syscall.EINTR) {
		return result
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err == nil {
		return result
	}
	return h.failureLocked()
}

func (h *healthState) status(result error) (error, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err == nil {
		return result, ""
	}
	failure := h.failureLocked()
	return failure, failure.Error()
}

func (h *healthState) poison(err error) error {
	if err == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err == nil {
		h.err = err
	}
	return h.failureLocked()
}

func (h *healthState) failureLocked() error {
	return fmt.Errorf("local object-store durability is no longer trustworthy: %v: %w", h.err, syscall.EIO)
}
