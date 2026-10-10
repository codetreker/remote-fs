package smb

import (
	"context"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type flushOwner struct {
	mu            sync.Mutex
	server        *Server
	id            WriteOwnerID
	file          storage.File
	sequence      uint64
	authorize     func(context.Context) error
	firstErr      error
	lastErr       error
	confirmed     bool
	retired       bool
	responseFinal bool
}

func (s *Server) reserveFlushOwner(t *tree, h *fileHandle, sequence uint64) (*flushOwner, error) {
	id, err := s.reserveIORecord(t, h, 0, true)
	if err != nil {
		return nil, err
	}
	o := &flushOwner{server: s, id: id, file: h.file, sequence: sequence}
	s.writes.update(id, func(r *writeRecord) { r.fact.Operation = storage.OpFileSync; r.fact.PayloadReleased = true })
	return o, nil
}

func (o *flushOwner) confirm(ctx context.Context, authorize func(context.Context) error) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.authorize = authorize
	return o.confirmLocked(ctx)
}

func (o *flushOwner) confirmLocked(ctx context.Context) error {
	if o.confirmed {
		return nil
	}
	if o.retired || o.authorize == nil {
		return syscall.EIO
	}
	err := o.authorize(ctx)
	if err == nil {
		err = o.file.Sync(ctx)
	}
	o.lastErr = err
	if err != nil {
		if o.firstErr == nil {
			o.firstErr = err
		}
		o.server.writes.update(o.id, func(r *writeRecord) {
			r.failed = true
			if r.fact.ErrorCategory == WriteErrorNone {
				r.fact.ErrorCategory = writeErrorCategory(err)
			}
		})
		return err
	}
	o.confirmed = true
	o.server.writes.update(o.id, func(r *writeRecord) {
		r.terminal = true
		r.fact.Durability = WriteDurabilityConfirmed
		r.fact.ChainSettled = true
	})
	return nil
}

func (o *flushOwner) reconcile(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.confirmLocked(ctx)
}

func (o *flushOwner) pending() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return !o.confirmed && !o.retired
}

func (o *flushOwner) retire(released, settled bool) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !released || !settled {
		return o.firstErr
	}
	o.retired = true
	if !o.confirmed && o.firstErr == nil {
		o.firstErr = syscall.EIO
	}
	o.server.writes.update(o.id, func(r *writeRecord) {
		r.terminal = true
		r.fact.ReferenceReleased = true
		r.fact.ChainSettled = true
		if o.firstErr != nil {
			r.failed = true
			if r.fact.ErrorCategory == WriteErrorNone {
				r.fact.ErrorCategory = writeErrorCategory(o.firstErr)
			}
		}
	})
	o.server.writes.removeSuccess(o.id)
	return o.firstErr
}

func (o *flushOwner) recordResponse(disposition ResponseDisposition) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if disposition != ResponseSent && disposition != ResponseSendFailed && disposition != ResponseNotSent {
		return
	}
	if o.responseFinal {
		return
	}
	o.responseFinal = true
	if disposition != ResponseSent && o.firstErr == nil {
		o.firstErr = syscall.EIO
	}
	o.server.writes.update(o.id, func(r *writeRecord) {
		if r.fact.ResponseFinal {
			return
		}
		r.fact.Response = disposition
		r.fact.ResponseFinal = true
		if disposition != ResponseSent {
			r.failed = true
			if r.fact.ErrorCategory == WriteErrorNone {
				r.fact.ErrorCategory = WriteErrorIO
			}
		}
	})
	o.server.writes.removeSuccess(o.id)
}
