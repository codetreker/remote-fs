package objectstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *windowsFile) WriteAt(ctx context.Context, offset int64, data []byte, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	if err := storage.CheckWindowsRange(offset, int64(len(data))); err != nil {
		return storage.WindowsActionResult{}, err
	}
	if offset+int64(len(data)) > f.session.options.MaxFileSize {
		return storage.WindowsActionResult{}, syscall.EFBIG
	}
	operation := metastore.WindowsIO{Offset: offset, Length: int64(len(data)), Write: true}
	return f.mutate(ctx, id, sha256.Sum256(data), operation, func(previous int64) int64 {
		if len(data) == 0 {
			return previous
		}
		return max(previous, offset+int64(len(data)))
	}, func(body []byte) { copy(body[offset:], data) })
}

func (f *windowsFile) Truncate(ctx context.Context, size int64, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	if size < 0 {
		return storage.WindowsActionResult{}, syscall.EINVAL
	}
	if size > f.session.options.MaxFileSize {
		return storage.WindowsActionResult{}, syscall.EFBIG
	}
	operation := metastore.WindowsIO{Write: true, Truncate: true, Size: size}
	return f.mutate(ctx, id, sha256.Sum256(nil), operation, func(int64) int64 { return size }, func([]byte) {})
}

func (f *windowsFile) mutate(ctx context.Context, id storage.WindowsActionID, fingerprint [32]byte, operation metastore.WindowsIO, size func(int64) int64, patch func([]byte)) (storage.WindowsActionResult, error) {
	ctx, done, err := f.begin(ctx, false)
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	defer done()
	result, fresh, err := f.native.BeginContent(ctx, id, fingerprint, operation)
	if err != nil || !fresh {
		return f.session.result(result, err)
	}
	_, attempts, _ := f.session.domain.FileOperationLimits()
	var missing metastore.Key
	for range attempts {
		node, err := f.native.Capture(ctx, operation)
		if err != nil {
			return f.reject(id, err)
		}
		if node.Size < 0 {
			return f.reject(id, syscall.EIO)
		}
		next := size(node.Size)
		if next < 0 || next > f.session.options.MaxFileSize {
			return f.reject(id, syscall.EFBIG)
		}
		if next == 0 {
			result, retry, err := f.publish(ctx, id, node, nil)
			if retry {
				continue
			}
			return result, err
		}
		if node.Size > f.session.options.MaxFileSize || node.Size > math.MaxInt64-next {
			return f.reject(id, syscall.EFBIG)
		}
		release, err := f.session.domain.AcquireMaterialization(ctx, node.Size+next)
		if err != nil {
			return f.reject(id, err)
		}
		body, err := f.session.storage.fileBody(ctx, node, missing)
		if isOnly(err, syscall.ENOENT) {
			release()
			missing = node.Content
			continue
		}
		if err != nil {
			release()
			return f.reject(id, err)
		}
		content := make([]byte, int(next))
		copy(content, body)
		patch(content)
		result, retry, err := f.publish(ctx, id, node, content)
		release()
		if retry {
			continue
		}
		return result, err
	}
	return f.reject(id, syscall.EAGAIN)
}

func (f *windowsFile) reject(id storage.WindowsActionID, cause error) (storage.WindowsActionResult, error) {
	ctx, cancel := f.session.operationContext(f.session.cleanup)
	defer cancel()
	result, err := f.native.RejectContent(ctx, id, cause)
	return f.session.result(result, errors.Join(cause, err))
}

func (f *windowsFile) cleanupObject(key metastore.Key, quarantine bool) error {
	ctx, cancel := f.session.operationContext(f.session.storage.cleanupContext)
	defer cancel()
	if quarantine {
		return f.session.storage.meta.Quarantine(ctx, key)
	}
	return f.session.storage.meta.Abandon(ctx, key)
}

func (f *windowsFile) publish(ctx context.Context, id storage.WindowsActionID, previous metastore.FileState, content []byte) (storage.WindowsActionResult, bool, error) {
	s := f.session.storage
	name := fmt.Sprintf("node %d", previous.ID)
	object := metastore.Object{Size: int64(len(content)), ModTime: s.now()}
	if len(content) > 0 {
		key, err := f.native.Reserve(ctx, object.Size)
		if err != nil {
			result, failure := f.reject(id, err)
			return result, false, failure
		}
		digest, err := s.objects.Put(ctx, string(key), content)
		if err != nil {
			failure := objectFailure("storing", name, err)
			if cleanup := f.cleanupObject(key, true); cleanup != nil {
				failure = errors.Join(failure, internalFailure("quarantining", name, cleanup))
			}
			result, failure := f.reject(id, failure)
			return result, false, failure
		}
		object.Key, object.Digest = key, digest
	}
	result, err := f.native.CommitContent(ctx, id, previous.Revision, object)
	if result.State == storage.WindowsActionCompleted && result.Errno == 0 {
		s.sweepAfterMutation()
		r, failure := f.session.result(result, err)
		return r, false, failure
	}
	if object.Key != "" {
		cleanup := f.cleanupObject(object.Key, false)
		s.sweepAfterMutation()
		if cleanup != nil {
			if isVolumeFact(cleanup) {
				err = ambiguousCommitFailure(name, err)
			}
			err = errors.Join(err, internalFailure("abandoning", name, cleanup))
			r, failure := f.session.result(result, err)
			return r, false, failure
		}
	}
	// A revision conflict retains the admitted action while its staged object
	// is discarded. Every retry must keep that action and its original bytes.
	if isOnly(err, syscall.EAGAIN) && result.State != storage.WindowsActionRejected {
		return storage.WindowsActionResult{}, true, err
	}
	r, failure := f.session.result(result, err)
	return r, false, failure
}
