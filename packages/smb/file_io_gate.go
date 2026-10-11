package smb

import (
	"context"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

type fileIOTicket struct {
	tree     *tree
	handle   *fileHandle
	sequence uint64
	ready    chan struct{}
	started  bool
	complete bool
	err      error
}

func (t *tree) admitFileIO(ctx context.Context, s *session, id wire.FileID, limit int) (*fileIOTicket, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 {
		return nil, syscall.EINVAL
	}
	a := t.authority
	if a == nil {
		return nil, syscall.EIO
	}
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.fileStopping || s.retired || a.stopping || a.closed || !time.Now().Before(a.deadline) {
		return nil, syscall.ESTALE
	}
	h := t.handles[id]
	if h == nil || h.ownerSession != s || h.treeID != t.id || h.sessionID != s.id || t.sessionID != s.id ||
		h.identity != a.identity || h.nodeID == 0 || handleState(h.state.Load()) != handleLive {
		return nil, syscall.EBADF
	}
	h.ioMu.Lock()
	defer h.ioMu.Unlock()
	if h.ioFenced {
		return nil, syscall.EBADF
	}
	active := len(h.ioQueue)
	if h.ioRunning != nil {
		active++
	}
	if active >= limit {
		return nil, syscall.EMFILE
	}
	if h.ioSequence == math.MaxUint64 {
		return nil, syscall.EOVERFLOW
	}
	h.ioSequence++
	ticket := &fileIOTicket{tree: t, handle: h, sequence: h.ioSequence, ready: make(chan struct{})}
	if t.fileActive == 0 {
		t.fileIdle = make(chan struct{})
	}
	t.fileActive++
	if h.ioRunning == nil {
		h.startFileIOLocked(ticket)
	} else {
		h.ioQueue = append(h.ioQueue, ticket)
	}
	return ticket, nil
}

func (h *fileHandle) startFileIOLocked(ticket *fileIOTicket) {
	h.ioRunning = ticket
	h.ioIdle = make(chan struct{})
	close(ticket.ready)
}

// The turn is released separately from a retained write owner. Cancellation
// after dispatch cannot prove that its authority action was not executed.
func (ticket *fileIOTicket) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		ticket.finish()
		return ctx.Err()
	case <-ticket.ready:
	}
	if err := ctx.Err(); err != nil {
		ticket.finish()
		return err
	}
	ticket.handle.ioMu.Lock()
	err := ticket.err
	if err == nil {
		if ticket.complete {
			err = syscall.EBADF
		} else {
			ticket.started = true
		}
	}
	ticket.handle.ioMu.Unlock()
	return err
}

func (ticket *fileIOTicket) finish() {
	t, h := ticket.tree, ticket.handle
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	h.ioMu.Lock()
	defer h.ioMu.Unlock()
	if ticket.complete {
		return
	}
	ticket.complete = true
	if h.ioRunning == ticket {
		h.ioRunning = nil
		close(h.ioIdle)
		h.ioIdle = nil
		if len(h.ioQueue) != 0 {
			next := h.ioQueue[0]
			h.ioQueue[0] = nil
			h.ioQueue = h.ioQueue[1:]
			h.startFileIOLocked(next)
		}
	} else {
		for index, queued := range h.ioQueue {
			if queued == ticket {
				copy(h.ioQueue[index:], h.ioQueue[index+1:])
				h.ioQueue[len(h.ioQueue)-1] = nil
				h.ioQueue = h.ioQueue[:len(h.ioQueue)-1]
				break
			}
		}
		ticket.err = context.Canceled
		close(ticket.ready)
	}
	t.endFileWorkLocked()
}

func (t *tree) fenceHandleFileIO(h *fileHandle) <-chan struct{} {
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	return t.fenceHandleFileIOLocked(h)
}

// tree.fileMu precedes ioMu even during retirement, so discarded queue items
// release their tree admission before a parent starts waiting for that tree.
func (t *tree) fenceHandleFileIOLocked(h *fileHandle) <-chan struct{} {
	h.ioMu.Lock()
	defer h.ioMu.Unlock()
	h.ioFenced = true
	if running := h.ioRunning; running != nil && !running.started {
		running.err = syscall.EBADF
		running.complete = true
		h.ioRunning = nil
		close(h.ioIdle)
		h.ioIdle = nil
		t.endFileWorkLocked()
	}
	for _, ticket := range h.ioQueue {
		ticket.err = syscall.EBADF
		ticket.complete = true
		close(ticket.ready)
		t.endFileWorkLocked()
	}
	h.ioQueue = nil
	return h.ioIdle
}
