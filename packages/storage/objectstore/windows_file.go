package objectstore

import (
	"context"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type windowsFile struct {
	session         *windowsSession
	native          metastore.WindowsFile
	operations      int
	idle            chan struct{}
	closing, closed bool
	forgetAfter     time.Time
	closePermit     chan struct{}
}

var _ storage.WindowsFile = (*windowsFile)(nil)

func (f *windowsFile) Reference() string { return f.native.Reference() }

func (f *windowsFile) begin(ctx context.Context, control bool) (context.Context, func(), error) {
	ctx, done, err := f.session.begin(ctx, control)
	if err != nil {
		return nil, nil, err
	}
	f.session.mu.Lock()
	if f.closing {
		f.session.mu.Unlock()
		done()
		return nil, nil, syscall.EBADF
	}
	if f.operations == 0 {
		f.idle = make(chan struct{})
	}
	f.operations++
	f.session.mu.Unlock()
	return ctx, func() {
		f.session.mu.Lock()
		f.operations--
		if f.operations == 0 {
			close(f.idle)
		}
		f.session.mu.Unlock()
		done()
	}, nil
}

func (f *windowsFile) Stat(ctx context.Context) (storage.WindowsAttr, error) {
	ctx, done, err := f.begin(ctx, false)
	if err != nil {
		return storage.WindowsAttr{}, err
	}
	defer done()
	return f.native.Stat(ctx)
}

func (f *windowsFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	if err := storage.CheckWindowsRange(offset, int64(length)); err != nil {
		return storage.FileRead{}, err
	}
	ctx, done, err := f.begin(ctx, false)
	if err != nil {
		return storage.FileRead{}, err
	}
	defer done()
	_, attempts, _ := f.session.domain.FileOperationLimits()
	operation := metastore.WindowsIO{Offset: offset, Length: int64(length)}
	var missing metastore.Key
	for range attempts {
		node, err := f.native.Capture(ctx, operation)
		if err != nil {
			return storage.FileRead{}, err
		}
		if node.Size < 0 {
			return storage.FileRead{}, syscall.EIO
		}
		if node.Size > f.session.options.MaxFileSize {
			return storage.FileRead{}, syscall.EFBIG
		}
		count := min(int64(length), max(node.Size-offset, 0))
		if count == 0 {
			return storage.FileRead{Attr: node.Attr()}, nil
		}
		if node.Size > math.MaxInt64-count {
			return storage.FileRead{}, syscall.EFBIG
		}
		release, err := f.session.domain.AcquireMaterialization(ctx, node.Size+count)
		if err != nil {
			return storage.FileRead{}, err
		}
		body, err := f.session.storage.fileBody(ctx, node, missing)
		if isOnly(err, syscall.ENOENT) {
			release()
			missing = node.Content
			continue
		}
		if err != nil {
			release()
			return storage.FileRead{}, err
		}
		data := make([]byte, int(count))
		copy(data, body[offset:offset+count])
		release()
		return storage.FileRead{Attr: node.Attr(), Data: data}, nil
	}
	return storage.FileRead{}, syscall.EAGAIN
}

func (f *windowsFile) SetAttr(ctx context.Context, change storage.WindowsAttrChange, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	ctx, done, err := f.begin(ctx, false)
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	defer done()
	return f.session.result(f.native.SetAttr(ctx, change, id))
}

func (f *windowsFile) ListBounded(ctx context.Context, result *storage.WindowsListResult) (err error) {
	if result == nil {
		return syscall.EINVAL
	}
	defer func() {
		if err != nil {
			result.Fail(err)
		}
	}()
	ctx, done, err := f.begin(ctx, false)
	if err != nil {
		return err
	}
	defer done()
	return f.native.ListBounded(ctx, result)
}

func (f *windowsFile) Rename(ctx context.Context, request storage.WindowsRenameRequest, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	ctx, done, err := f.begin(ctx, false)
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	defer done()
	return f.session.result(f.native.Rename(ctx, request, id))
}

func (f *windowsFile) SetDeletePending(ctx context.Context, pending bool, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	ctx, done, err := f.begin(ctx, false)
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	defer done()
	return f.session.result(f.native.SetDeletePending(ctx, pending, id))
}

func (f *windowsFile) LockBatch(ctx context.Context, batch storage.WindowsLockBatch, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	ctx, done, err := f.begin(ctx, true)
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	defer done()
	return f.session.result(f.native.LockBatch(ctx, batch, id))
}

func (f *windowsFile) Sync(ctx context.Context) error {
	ctx, done, err := f.begin(ctx, false)
	if err != nil {
		return err
	}
	defer done()
	return f.native.Sync(ctx)
}

func (f *windowsFile) ReadLink(ctx context.Context) (storage.WindowsSymlinkInfo, error) {
	ctx, done, err := f.begin(ctx, false)
	if err != nil {
		return storage.WindowsSymlinkInfo{}, err
	}
	defer done()
	return f.native.ReadLink(ctx)
}

func (f *windowsFile) SetLink(ctx context.Context, target string, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	ctx, done, err := f.begin(ctx, false)
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	defer done()
	return f.session.result(f.native.SetLink(ctx, target, id))
}

func (f *windowsFile) Close(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	if _, err := id.Epoch(); err != nil {
		return storage.WindowsActionResult{}, err
	}
	ctx, done, err := f.session.begin(ctx, true)
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	defer done()
	select {
	case <-f.closePermit:
		defer func() { f.closePermit <- struct{}{} }()
	case <-ctx.Done():
		return storage.WindowsActionResult{}, ctx.Err()
	}
	f.session.mu.Lock()
	f.closing = true
	idle := f.idle
	f.session.mu.Unlock()
	select {
	case <-idle:
	case <-ctx.Done():
		return storage.WindowsActionResult{}, ctx.Err()
	}
	// Final cleanup uses the creation-time hook exactly once, even when a
	// different wrapper or request context performs the close.
	cleanup, cancel := f.session.cleanupOperation(ctx)
	stop := context.AfterFunc(ctx, cancel)
	r, err := f.session.result(f.native.Close(cleanup, id))
	stop()
	cancel()
	if r.State == storage.WindowsActionCompleted && r.Errno == 0 {
		f.session.mu.Lock()
		f.closed = true
		f.forgetAfter = time.Now().Add(f.session.options.History)
		f.session.mu.Unlock()
		f.session.storage.sweepAfterMutation()
	}
	return r, err
}
