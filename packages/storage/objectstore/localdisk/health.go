package localdisk

import (
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
	return fmt.Errorf("local object-store durability is no longer trustworthy: %v: %w", h.err, syscall.EIO)
}

func (h *healthState) poison(err error) error {
	if err == nil {
		return nil
	}
	h.mu.Lock()
	if h.err == nil {
		h.err = err
	}
	h.mu.Unlock()
	return h.failure()
}
