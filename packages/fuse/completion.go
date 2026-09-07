package fuse

import (
	"context"
	"time"
)

// Close consumes the descriptor even when it reports an error, so a canceled request
// cannot defer the commit to another close. The one attempt keeps its deadline while
// waiting for h.mu; neither lock admission nor storage completion gets a fresh budget.
// https://github.com/golang/go/blob/e3336a22ad3f0a90bd252c95d8b5544e02674205/src/os/file_unix.go#L307-L324
func (h *handle) flushForClose(ctx context.Context) error {
	deadline := time.Now().Add(h.node.ns.flushTimeout)
	if requestDeadline, bounded := ctx.Deadline(); bounded && requestDeadline.Before(deadline) {
		deadline = requestDeadline
	}
	completion, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	_, err := h.flush(completion)
	return err
}
