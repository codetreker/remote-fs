package httprest

import (
	"context"
	"errors"
	"sync"
	"syscall"
)

func (h *Handler) admitRequest() (func(), error) {
	h.requestMu.Lock()
	defer h.requestMu.Unlock()
	if h.retiring {
		if h.stopped() {
			return nil, syscall.EIO
		}
		return nil, syscall.EBUSY
	}
	if h.stopped() && (h.accessBinding != nil || h.transfers != nil) {
		return nil, syscall.EIO
	}
	h.activeRequests++
	var once sync.Once
	return func() {
		once.Do(func() {
			h.requestMu.Lock()
			h.activeRequests--
			h.requestMu.Unlock()
		})
	}, nil
}

// ReserveRetirement atomically fences new HTTP admission before reserving both
// capability authorities. Busy leaves the original handler usable. The caller
// must reserve its native backend next, then Stop, or CancelRetirement on failure.
func (h *Handler) ReserveRetirement(ctx context.Context) error {
	h.requestMu.Lock()
	defer h.requestMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if h.retiring || h.stopped() || h.activeRequests != 0 {
		return syscall.EBUSY
	}
	if h.transfers != nil && !h.transfers.idle() {
		return syscall.EBUSY
	}
	releaseFiles, err := h.files.reserveRetirement()
	if err != nil {
		return err
	}
	locks, ok := h.locks.(interface {
		ReserveRetirement(context.Context) error
		CancelRetirement()
	})
	if !ok {
		releaseFiles()
		return errors.New("httprest: lock authority cannot reserve retirement")
	}
	if err := locks.ReserveRetirement(ctx); err != nil {
		releaseFiles()
		return err
	}
	h.retiring = true
	h.retirementRollback = func() { locks.CancelRetirement(); releaseFiles() }
	return nil
}

// CancelRetirement releases an uncommitted reservation. Stop is irreversible.
func (h *Handler) CancelRetirement() {
	h.requestMu.Lock()
	defer h.requestMu.Unlock()
	if h.stopped() || !h.retiring {
		return
	}
	h.retirementRollback()
	h.retirementRollback = nil
	h.retiring = false
}
