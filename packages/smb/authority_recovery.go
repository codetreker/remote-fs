package smb

import (
	"context"
	"errors"
	"syscall"
)

func (t *tree) retainedHandles() int {
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	return len(t.handles)
}

// The authority retirement gate serializes parent certificates with per-tree
// cleanup. Membership is frozen before any wait or backend operation.
func (a *authoritySession) recoverRetainedOwners(ctx context.Context) (bool, error) {
	a.installMu.Lock()
	s := a.smbSession
	if s == nil {
		a.installMu.Unlock()
		return false, syscall.EIO
	}
	s.mu.Lock()
	trees := make([]*tree, 0)
	for _, t := range s.trees {
		if t.authority == a {
			trees = append(trees, t)
		}
	}
	a.mu.Lock()
	complete := len(trees) == a.refs
	a.mu.Unlock()
	if s.openingTrees != 0 || !complete {
		s.mu.Unlock()
		a.installMu.Unlock()
		return false, nil
	}
	s.mu.Unlock()
	for _, t := range trees {
		t.fileMu.Lock()
		stopping := t.fileStopping
		t.fileMu.Unlock()
		if !stopping {
			a.installMu.Unlock()
			return false, nil
		}
	}
	a.stopping = true
	a.installMu.Unlock()
	for _, t := range trees {
		if err := waitFileWork(ctx, t.fenceFileWork()); err != nil {
			return false, err
		}
	}
	var handles []struct {
		tree   *tree
		handle *fileHandle
	}
	for _, t := range trees {
		t.fileMu.Lock()
		current := make([]*fileHandle, 0, len(t.handles))
		for _, h := range t.handles {
			current = append(current, h)
		}
		t.fileMu.Unlock()
		for _, h := range current {
			handles = append(handles, struct {
				tree   *tree
				handle *fileHandle
			}{t, h})
		}
	}
	// Normal retirement first uses each exact retained reference. Full parent
	// settlement is the terminal certificate for any remaining local owner.
	var errs []error
	for _, item := range handles {
		if err := item.tree.closeFileHandle(ctx, item.handle); err != nil {
			errs = append(errs, err)
		}
	}
	acquired := 0
	defer func() {
		for i := acquired - 1; i >= 0; i-- {
			handles[i].handle.closeLifetime.unlock()
		}
	}()
	for _, item := range handles {
		if err := item.handle.closeLifetime.lock(ctx); err != nil {
			return false, errors.Join(errors.Join(errs...), err)
		}
		acquired++
	}
	parentErr := a.close(ctx)
	if !a.isClosed() {
		return false, errors.Join(errors.Join(errs...), parentErr)
	}
	for _, item := range handles {
		h, t := item.handle, item.tree
		if finalErr := errors.Join(h.terminalErr, h.semanticErr); finalErr != nil {
			h.terminalErr = finalErr
			errs = append(errs, finalErr)
		}
		err := h.cleanup.run(ctx, func() error {
			h.pendingOpen = nil
			h.released = true
			h.terminal = true
			t.releaseFileHandle(h)
			return nil
		})
		if err != nil {
			errs = append(errs, err)
		}
	}
	return true, errors.Join(parentErr, errors.Join(errs...))
}
