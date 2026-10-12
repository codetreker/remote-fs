package httprest

import (
	"context"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// Cleanup retains the capability's original access context after the admitting
// request ends. Authentication revocation prevents external use, while the
// registry remains responsible for releasing already-admitted native resources.
func (s *servedFileSession) cleanupContext() context.Context {
	ctx := context.Background()
	if s.binding == (storage.AccessBinding{}) {
		return ctx
	}
	bound, err := storage.WithAccessBinding(ctx, s.binding)
	if err != nil {
		panic(err)
	}
	return bound
}

// Retirement reserves the empty registry before Handler.Stop becomes
// irreversible. Volatile terminal receipts retain their original authority
// until expiry, even when all native references have already been released.
func (r *fileRegistry) reserveRetirement() (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.retiring {
		return nil, syscall.EBUSY
	}
	if r.enrolling != 0 || len(r.sessions) != 0 {
		return nil, syscall.EBUSY
	}
	now := time.Now()
	for _, terminal := range r.terminalCloses {
		terminal.mu.Lock()
		live := now.Before(terminal.expires)
		terminal.mu.Unlock()
		if live {
			return nil, syscall.EBUSY
		}
	}
	r.retiring = true
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			r.retiring = false
			r.mu.Unlock()
		})
	}, nil
}
