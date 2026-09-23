package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileHandle struct {
	id             wire.FileID
	file           storage.File
	node           storage.NodeReference
	pendingOpen    func(context.Context) (bool, error)
	access         uint32
	action         storage.FileActionID
	closing        bool
	requestCloseMu contextLock
	cleanup        cleanupGate
}

func (t *tree) beginFileWork(s *session) bool {
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	if t.fileStopping {
		return false
	}
	s.mu.Lock()
	retired := s.retired
	s.mu.Unlock()
	if retired {
		return false
	}
	if t.fileActive == 0 {
		t.fileIdle = make(chan struct{})
	}
	t.fileActive++
	return true
}

func (t *tree) endFileWork() {
	t.fileMu.Lock()
	t.fileActive--
	if t.fileActive == 0 {
		close(t.fileIdle)
		t.fileIdle = nil
	}
	t.fileMu.Unlock()
}

func (t *tree) fenceFileWork() <-chan struct{} {
	t.fileMu.Lock()
	t.fileStopping = true
	idle := t.fileIdle
	t.fileMu.Unlock()
	return idle
}

func waitFileWork(ctx context.Context, idle <-chan struct{}) error {
	if idle == nil {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// A reserved slot exists before a name-changing authority action starts. The
// FileId never transfers to another open, including when the action fails.
func (t *tree) reserveFileHandle(s *session, limit int) (*fileHandle, error) {
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	if t.fileStopping || t.fileActive == 0 || len(t.handles) >= limit {
		return nil, syscall.EMFILE
	}
	s.mu.Lock()
	if s.nextFileID == math.MaxUint64 {
		s.mu.Unlock()
		return nil, syscall.EMFILE
	}
	s.nextFileID++
	sequence := s.nextFileID
	s.mu.Unlock()
	id := wire.FileID{}
	binary.LittleEndian.PutUint64(id[:8], s.id)
	binary.LittleEndian.PutUint64(id[8:], sequence)
	handle := &fileHandle{id: id}
	if t.handles == nil {
		t.handles = make(map[wire.FileID]*fileHandle)
	}
	t.handles[id] = handle
	return handle, nil
}

func (t *tree) findFileHandle(id wire.FileID) *fileHandle {
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	return t.handles[id]
}

func (t *tree) releaseFileHandle(handle *fileHandle) {
	t.fileMu.Lock()
	if t.handles[handle.id] == handle {
		delete(t.handles, handle.id)
	}
	t.fileMu.Unlock()
}

func (t *tree) releaseAllFileHandles() {
	t.fileMu.Lock()
	clear(t.handles)
	t.fileMu.Unlock()
}

func (t *tree) closeFileHandle(ctx context.Context, handle *fileHandle) error {
	return handle.cleanup.run(ctx, func() error {
		handle.closing = true
		var pendingErr error
		if handle.pendingOpen != nil {
			noReference, recoverErr := handle.pendingOpen(ctx)
			if noReference {
				t.releaseFileHandle(handle)
				return recoverErr
			}
			if handle.file == nil && handle.node == nil {
				return recoverErr
			}
			pendingErr = recoverErr
			handle.pendingOpen = nil
		}
		var result storage.ReferenceCloseResult
		var err error
		switch {
		case handle.file != nil:
			result, err = handle.file.CloseWithResult(ctx)
		case handle.node != nil:
			result, err = handle.node.CloseWithResult(ctx)
		default:
			return syscall.EIO
		}
		err = errors.Join(err, result.Check(err))
		if result.Released && result.Check(err) == nil {
			t.releaseFileHandle(handle)
		}
		return errors.Join(pendingErr, err)
	})
}

func (t *tree) closeFileHandles(ctx context.Context) error {
	t.fileMu.Lock()
	handles := make([]*fileHandle, 0, len(t.handles))
	for _, handle := range t.handles {
		handles = append(handles, handle)
	}
	t.fileMu.Unlock()
	var errs []error
	for _, handle := range handles {
		if err := t.closeFileHandle(ctx, handle); err != nil {
			errs = append(errs, err)
		}
	}
	t.fileMu.Lock()
	retained := len(t.handles)
	t.fileMu.Unlock()
	if retained != 0 {
		errs = append(errs, syscall.EIO)
	}
	return errors.Join(errs...)
}
