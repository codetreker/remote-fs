package objectstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type openFile struct {
	session    *fileSession
	native     metastore.File
	options    storage.FileOpenOptions
	active     bool
	operations sync.WaitGroup
	flock      map[storage.LockOwner]uint64
	retireMu   sync.Mutex
	retired    bool
	closeMu    sync.Mutex
	closeDone  chan struct{}
	closeErr   error
}

var _ storage.File = (*openFile)(nil)

func (f *openFile) begin(ctx context.Context) (context.Context, func(), error) {
	return f.admit(ctx, fileDataOperation)
}

func (f *openFile) admit(ctx context.Context, class fileOperationClass) (context.Context, func(), error) {
	done, err := f.session.admit(ctx, false, class)
	if err != nil {
		return nil, nil, err
	}
	f.session.mu.Lock()
	if !f.active {
		f.session.mu.Unlock()
		done()
		return nil, nil, syscall.EBADF
	}
	f.operations.Add(1)
	f.session.mu.Unlock()
	_, _, timeout := f.session.domain.FileOperationLimits()
	operation, cancel := context.WithTimeout(ctx, timeout)
	return metastore.WithFilePublicationGuard(operation, f.session.publicationAllowed), func() { cancel(); f.operations.Done(); done() }, nil
}

func (f *openFile) state(ctx context.Context) (metastore.FileState, error) {
	node, err := f.native.Node(ctx)
	if err != nil {
		return metastore.FileState{}, err
	}
	if err := f.session.locks.IOHealth(ctx, uint64(node.ID)); err != nil {
		return metastore.FileState{}, err
	}
	return node, nil
}

func (f *openFile) Stat(ctx context.Context) (storage.Attr, error) {
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	node, err := f.state(ctx)
	return node.Attr(), err
}

func (f *openFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	if offset < 0 || length < 0 {
		return storage.FileRead{}, syscall.EINVAL
	}
	if !f.options.Read {
		return storage.FileRead{}, syscall.EBADF
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.FileRead{}, err
	}
	defer done()
	_, attempts, _ := f.session.domain.FileOperationLimits()
	maxBytes := f.session.options.MaxFileSize
	var missing metastore.Key
	for range attempts {
		node, err := f.state(ctx)
		if err != nil {
			return storage.FileRead{}, err
		}
		if node.Size < 0 || node.Size > maxBytes {
			return storage.FileRead{}, syscall.EFBIG
		}
		count := min(int64(length), max(node.Size-offset, 0))
		if node.Size > math.MaxInt64-count {
			return storage.FileRead{}, syscall.EFBIG
		}
		release, err := f.session.domain.AcquireMaterialization(ctx, node.Size+count)
		if err != nil {
			return storage.FileRead{}, err
		}
		body, err := f.body(ctx, node, missing)
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
		if count != 0 {
			copy(data, body[offset:offset+count])
		}
		release()
		return storage.FileRead{Attr: node.Attr(), Data: data}, nil
	}
	return storage.FileRead{}, fmt.Errorf("retained file changed during every content read: %w", syscall.EAGAIN)
}

func (f *openFile) body(ctx context.Context, node metastore.FileState, missing metastore.Key) ([]byte, error) {
	if node.Content == "" {
		if node.Size != 0 {
			return nil, fmt.Errorf("retained file has bytes without an object: %w", syscall.EIO)
		}
		return nil, nil
	}
	if node.Content == missing {
		return nil, fmt.Errorf("retained file names a missing object: %w", syscall.EIO)
	}
	body, err := f.session.storage.objects.(BoundedObjects).GetBounded(ctx, string(node.Content), max(node.Size, 1))
	if err != nil {
		if isOnly(err, syscall.ENOENT) {
			return nil, err
		}
		if errors.Is(err, syscall.EFBIG) {
			return nil, sanitizeFailure("retained file object exceeds its recorded size", err, func(e error) bool { return errors.Is(e, syscall.EFBIG) })
		}
		return nil, objectFailure("reading", fmt.Sprintf("node %d", node.ID), err)
	}
	if int64(len(body)) != node.Size {
		return nil, fmt.Errorf("retained file object size differs from metadata: %w", syscall.EIO)
	}
	return body, nil
}

func (f *openFile) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	if offset < 0 {
		return storage.Attr{}, syscall.EINVAL
	}
	if int64(len(data)) > math.MaxInt64-offset {
		return storage.Attr{}, syscall.EFBIG
	}
	if !f.options.Write {
		return storage.Attr{}, syscall.EBADF
	}
	if len(data) == 0 {
		return f.Stat(ctx)
	}
	return f.mutate(ctx, func(previous int64) int64 {
		if len(data) == 0 {
			return previous
		}
		return max(previous, offset+int64(len(data)))
	}, func(body []byte) {
		if len(data) != 0 {
			copy(body[offset:], data)
		}
	})
}

func (f *openFile) Truncate(ctx context.Context, size int64) (storage.Attr, error) {
	if size < 0 {
		return storage.Attr{}, syscall.EINVAL
	}
	return f.mutate(ctx, func(int64) int64 { return size }, func([]byte) {})
}

func (f *openFile) mutate(ctx context.Context, size func(int64) int64, patch func([]byte)) (storage.Attr, error) {
	if !f.options.Write {
		return storage.Attr{}, syscall.EBADF
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	_, attempts, _ := f.session.domain.FileOperationLimits()
	maxBytes := f.session.options.MaxFileSize
	var missing metastore.Key
	for range attempts {
		node, err := f.state(ctx)
		if err != nil {
			return storage.Attr{}, err
		}
		next := size(node.Size)
		if node.Size < 0 {
			return storage.Attr{}, syscall.EIO
		}
		if next < 0 || next > maxBytes {
			return storage.Attr{}, syscall.EFBIG
		}
		// Emptying a retained object preserves no old bytes. The revision-CAS
		// publication still validates strong permissions and the reference lifetime.
		if next == 0 {
			result, retry, err := f.publish(ctx, node, nil)
			if retry {
				continue
			}
			return result, err
		}
		if node.Size > maxBytes {
			return storage.Attr{}, syscall.EFBIG
		}
		if node.Size > math.MaxInt64-next {
			return storage.Attr{}, syscall.EFBIG
		}
		release, err := f.session.domain.AcquireMaterialization(ctx, node.Size+next)
		if err != nil {
			return storage.Attr{}, err
		}
		body, err := f.body(ctx, node, missing)
		if isOnly(err, syscall.ENOENT) {
			release()
			missing = node.Content
			continue
		}
		if err != nil {
			release()
			return storage.Attr{}, err
		}
		content := make([]byte, int(next))
		copy(content, body)
		patch(content)
		result, retry, err := f.publish(ctx, node, content)
		release()
		if retry {
			continue
		}
		return result, err
	}
	return storage.Attr{}, fmt.Errorf("retained file changed during every publication attempt: %w", syscall.EAGAIN)
}

func (f *openFile) publish(ctx context.Context, previous metastore.FileState, content []byte) (storage.Attr, bool, error) {
	s := f.session.storage
	name := fmt.Sprintf("node %d", previous.ID)
	object := metastore.Object{Size: int64(len(content)), ModTime: s.now()}
	if len(content) > 0 {
		key, err := f.native.Reserve(ctx, object.Size)
		if err != nil {
			return storage.Attr{}, false, err
		}
		digest, err := s.objects.Put(ctx, string(key), content)
		if err != nil {
			operationErr := objectFailure("storing", name, err)
			if cleanupErr := f.cleanupObject(key, true); cleanupErr != nil {
				operationErr = errors.Join(operationErr, internalFailure("quarantining", name, cleanupErr))
			}
			return storage.Attr{}, false, operationErr
		}
		object.Key, object.Digest = key, digest
	}
	node, err := f.native.Commit(ctx, previous.Revision, object)
	if err != nil {
		if object.Key != "" {
			cleanupErr := f.cleanupObject(object.Key, false)
			s.sweepAfterMutation()
			if cleanupErr != nil {
				if isNamespaceFact(cleanupErr) {
					err = ambiguousCommitFailure(name, err)
				}
				return storage.Attr{}, false, errors.Join(err, internalFailure("abandoning", name, cleanupErr))
			}
		}
		return storage.Attr{}, isOnly(err, syscall.EAGAIN), err
	}
	s.sweepAfterMutation()
	return node.Attr(), false, nil
}

func (f *openFile) cleanupObject(key metastore.Key, quarantine bool) error {
	_, _, timeout := f.session.domain.FileOperationLimits()
	ctx, cancel := context.WithTimeout(f.session.storage.cleanupContext, timeout)
	defer cancel()
	if quarantine {
		return f.session.storage.meta.Quarantine(ctx, key)
	}
	return f.session.storage.meta.Abandon(ctx, key)
}

func (f *openFile) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	if err := change.Check(); err != nil {
		return storage.Attr{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	if _, err := f.state(ctx); err != nil {
		return storage.Attr{}, err
	}
	node, err := f.native.SetAttr(ctx, change)
	return node.Attr(), err
}

func (f *openFile) Sync(ctx context.Context) error {
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	_, attempts, _ := f.session.domain.FileOperationLimits()
	maxBytes := f.session.options.MaxFileSize
	var missing metastore.Key
	for range attempts {
		node, err := f.state(ctx)
		if err != nil {
			return err
		}
		if node.Size < 0 || node.Size > maxBytes {
			return syscall.EFBIG
		}
		release, err := f.session.domain.AcquireMaterialization(ctx, node.Size)
		if err != nil {
			return err
		}
		_, err = f.body(ctx, node, missing)
		release()
		if !isOnly(err, syscall.ENOENT) {
			return err
		}
		missing = node.Content
	}
	return syscall.EAGAIN
}

func (f *openFile) retire() error {
	f.session.mu.Lock()
	f.active = false
	f.session.mu.Unlock()
	f.retireMu.Lock()
	defer f.retireMu.Unlock()
	if f.retired {
		return nil
	}
	ctx, cancel := f.session.operationContext(f.session.cleanup)
	defer cancel()
	if err := f.native.Retire(ctx); err != nil {
		return err
	}
	f.retired = true
	return nil
}

func (f *openFile) startClose() <-chan struct{} {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	if f.closeDone != nil {
		select {
		case <-f.closeDone:
			if f.closeErr == nil {
				return f.closeDone
			}
		default:
			return f.closeDone
		}
	}
	f.closeDone = make(chan struct{})
	go f.finishClose()
	return f.closeDone
}

func (f *openFile) finishClose() {
	err := f.retire()
	if err == nil {
		f.operations.Wait()
		f.session.mu.Lock()
		owners := make(map[storage.LockOwner]uint64, len(f.flock))
		for owner, node := range f.flock {
			owners[owner] = node
		}
		f.session.mu.Unlock()
		for owner, node := range owners {
			err = errors.Join(err, f.dropClosedFlock(node, owner))
		}
		// Cleanup keeps the creation-time accounting hooks. Attaching Close's
		// context again would reserve and settle the same outer quota twice.
		ctx, cancel := f.session.operationContext(f.session.cleanup)
		err = errors.Join(err, f.native.Close(ctx))
		cancel()
	}
	if err == nil {
		f.session.mu.Lock()
		delete(f.session.files, f)
		f.session.mu.Unlock()
		f.session.storage.sweepAfterMutation()
	}
	f.closeMu.Lock()
	f.closeErr = err
	close(f.closeDone)
	f.closeMu.Unlock()
}

func (f *openFile) Close(ctx context.Context) error {
	done := f.startClose()
	select {
	case <-done:
		f.closeMu.Lock()
		defer f.closeMu.Unlock()
		return f.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
