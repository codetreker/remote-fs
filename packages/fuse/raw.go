package fuse

import (
	"fmt"
	"sync"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/storage"
)

type rawRequestKind uint8

const (
	rawFlush rawRequestKind = iota + 1
	rawRelease
)

type rawRequest struct {
	kind        rawRequestKind
	owner       storage.LockOwner
	flockUnlock bool
}

// The high-level bridge keeps only the caller and cancel channel. A unique channel
// carries the association without depending on go-fuse's private file-handle table.
// https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/bridge.go#L982-L990
type rawMetadata struct {
	mu       sync.Mutex
	limit    int
	requests map[<-chan struct{}]rawRequest
}

func (m *rawMetadata) lookup(cancel <-chan struct{}) (rawRequest, bool) {
	if m == nil {
		return rawRequest{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	request, ok := m.requests[cancel]
	return request, ok
}

func (m *rawMetadata) begin(cancel <-chan struct{}, request rawRequest) (<-chan struct{}, func(), bool) {
	m.mu.Lock()
	if len(m.requests) >= m.limit {
		m.mu.Unlock()
		return nil, nil, false
	}
	forwarded := make(chan struct{})
	m.requests[forwarded] = request
	m.mu.Unlock()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-cancel:
			close(forwarded)
		case <-stop:
		}
	}()
	return forwarded, func() {
		close(stop)
		<-done
		m.mu.Lock()
		delete(m.requests, forwarded)
		m.mu.Unlock()
	}, true
}

type rawFilesystem struct {
	gofuse.RawFileSystem
	ns *namespace
}

func newRawFilesystem(raw gofuse.RawFileSystem, ns *namespace) gofuse.RawFileSystem {
	ns.raw = &rawMetadata{limit: ns.sessionOptions.MaxOperations, requests: make(map[<-chan struct{}]rawRequest)}
	return &rawFilesystem{RawFileSystem: raw, ns: ns}
}

func (r *rawFilesystem) Flush(cancel <-chan struct{}, input *gofuse.FlushIn) gofuse.Status {
	forwarded, done, ok := r.ns.raw.begin(cancel, rawRequest{kind: rawFlush, owner: storage.LockOwner(input.LockOwner)})
	if !ok {
		r.ns.fence(fmt.Errorf("flush owner metadata capacity exhausted: %w", syscall.EIO))
		return gofuse.EIO
	}
	defer done()
	return r.RawFileSystem.Flush(forwarded, input)
}

func (r *rawFilesystem) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	forwarded, done, ok := r.ns.raw.begin(cancel, rawRequest{
		kind: rawRelease, owner: storage.LockOwner(input.LockOwner),
		flockUnlock: input.ReleaseFlags&gofuse.FUSE_RELEASE_FLOCK_UNLOCK != 0,
	})
	if !ok {
		r.ns.fence(fmt.Errorf("release owner metadata capacity exhausted: %w", syscall.EIO))
		// Release must still retire the retained file if owner cleanup cannot be
		// associated with this call. The session fence retires all its owners.
		r.RawFileSystem.Release(nil, input)
		return
	}
	defer done()
	r.RawFileSystem.Release(forwarded, input)
}
