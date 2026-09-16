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

func (f *openFile) WriteAt(ctx context.Context, request storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	offset, data := request.Offset, request.Data
	if err := checkFileRange(offset, int64(len(data))); err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	if offset+int64(len(data)) > f.session.options.MaxFileSize {
		return storage.FileActionReceipt{}, beforeFileAdmission(syscall.EFBIG)
	}
	operation := storage.FileIO{Offset: offset, Length: int64(len(data)), Write: true, Owner: request.Owner, ExpectedSize: request.ExpectedSize}
	return f.mutate(ctx, id, sha256.Sum256(data), operation, func(previous int64) int64 {
		if len(data) == 0 {
			return previous
		}
		return max(previous, offset+int64(len(data)))
	}, func(body []byte) {
		if len(data) > 0 {
			copy(body[offset:], data)
		}
	})
}

func (f *openFile) Truncate(ctx context.Context, request storage.FileTruncateRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if request.Size < 0 {
		return storage.FileActionReceipt{}, beforeFileAdmission(syscall.EINVAL)
	}
	if request.Size > f.session.options.MaxFileSize {
		return storage.FileActionReceipt{}, beforeFileAdmission(syscall.EFBIG)
	}
	operation := storage.FileIO{Write: true, Truncate: true, Size: request.Size, Owner: request.Owner}
	return f.mutate(ctx, id, sha256.Sum256(nil), operation, func(int64) int64 { return request.Size }, func([]byte) {})
}

func (f *openFile) mutate(ctx context.Context, id storage.FileActionID, fingerprint [32]byte, operation storage.FileIO, size func(int64) int64, patch func([]byte)) (receipt storage.FileActionReceipt, err error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	releaseIO, err := f.acquireContentIO(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	pastAdmission := false
	defer func() {
		err = errors.Join(err, releaseIO())
		if pastAdmission {
			err = afterFileAdmission(err)
		}
	}()
	result, fresh, err := f.native.BeginContent(ctx, id, fingerprint, operation)
	pastAdmission = !storage.IsFileCallNotAdmitted(err)
	if err != nil || !fresh {
		return result, err
	}
	_, attempts, _ := f.session.authority.FileOperationLimits()
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
		release, err := f.session.authority.AcquireMaterialization(ctx, node.Size+next)
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

func (f *openFile) reject(id storage.FileActionID, cause error) (storage.FileActionReceipt, error) {
	ctx, cancel := f.session.operationContext(f.session.cleanup)
	defer cancel()
	result, err := f.native.RejectContent(ctx, id, cause)
	return result, errors.Join(cause, err)
}

func (f *openFile) cleanupObject(key metastore.Key, quarantine bool) error {
	ctx, cancel := f.session.operationContext(f.session.storage.cleanupContext)
	defer cancel()
	if quarantine {
		return f.session.storage.meta.Quarantine(ctx, key)
	}
	return f.session.storage.meta.Abandon(ctx, key)
}

func (f *openFile) publish(ctx context.Context, id storage.FileActionID, previous metastore.FileState, content []byte) (storage.FileActionReceipt, bool, error) {
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
	if result.Effects&storage.EffectContentChanged != 0 || result.State == storage.FileActionCompleted && result.Errno == 0 {
		s.sweepAfterMutation()
		return result, false, err
	}
	if result.State == storage.FileActionUnknown || result.State == 0 {
		return result, false, err
	}
	if object.Key != "" {
		cleanup := f.cleanupObject(object.Key, false)
		s.sweepAfterMutation()
		if cleanup != nil {
			if isVolumeFact(cleanup) {
				err = ambiguousCommitFailure(name, err)
			}
			err = errors.Join(err, internalFailure("abandoning", name, cleanup))
			if result.State == storage.FileActionPending {
				rejected, rejectionErr := f.reject(id, err)
				return rejected, false, rejectionErr
			}
			return result, false, err
		}
	}
	// A revision conflict retains the admitted action while its staged object
	// is discarded. Every retry must keep that action and its original bytes.
	if isOnly(err, syscall.EAGAIN) && result.State == storage.FileActionPending {
		return storage.FileActionReceipt{}, true, err
	}
	return result, false, err
}

func checkFileRange(offset, length int64) error {
	if offset < 0 || length < 0 {
		return syscall.EINVAL
	}
	if length > math.MaxInt64-offset {
		return syscall.EFBIG
	}
	return nil
}

func (f *openFile) ReadAt(ctx context.Context, request storage.FileReadRequest) (read storage.FileRead, err error) {
	if err := checkFileRange(request.Offset, int64(request.Length)); err != nil {
		return storage.FileRead{}, err
	}
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileRead{}, err
	}
	defer done()
	releaseIO, err := f.acquireContentIO(ctx)
	if err != nil {
		return storage.FileRead{}, err
	}
	defer func() { err = errors.Join(err, releaseIO()) }()
	_, attempts, _ := f.session.authority.FileOperationLimits()
	operation := storage.FileIO{Offset: request.Offset, Length: int64(request.Length), Owner: request.Owner}
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
		count := min(int64(request.Length), max(node.Size-request.Offset, 0))
		if count == 0 {
			return storage.FileRead{Attr: node.Attr()}, nil
		}
		if node.Size > math.MaxInt64-count {
			return storage.FileRead{}, syscall.EFBIG
		}
		release, err := f.session.authority.AcquireMaterialization(ctx, node.Size+count)
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
		copy(data, body[request.Offset:request.Offset+count])
		release()
		return storage.FileRead{Attr: node.Attr(), Data: data}, nil
	}
	return storage.FileRead{}, syscall.EAGAIN
}

func (f *openFile) Sync(ctx context.Context) (err error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return err
	}
	defer done()
	releaseIO, err := f.acquireContentIO(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, releaseIO()) }()
	_, attempts, _ := f.session.authority.FileOperationLimits()
	var missing metastore.Key
	for range attempts {
		node, err := f.native.Sync(ctx)
		if err != nil {
			return err
		}
		switch node.Kind {
		case storage.NodeDirectory, storage.NodeSymlink:
			return nil
		case storage.NodeRegular:
		default:
			return syscall.EIO
		}
		if node.Size < 0 {
			return syscall.EIO
		}
		if node.Size > f.session.options.MaxFileSize {
			return syscall.EFBIG
		}
		release, err := f.session.authority.AcquireMaterialization(ctx, node.Size)
		if err != nil {
			return err
		}
		_, err = f.session.storage.fileBody(ctx, node, missing)
		release()
		if !isOnly(err, syscall.ENOENT) {
			return err
		}
		missing = node.Content
	}
	return syscall.EAGAIN
}

func (f *openFile) acquireContentIO(ctx context.Context) (func() error, error) {
	membership, err := f.native.AcquireIO(ctx)
	if err != nil {
		return nil, err
	}
	return func() error {
		cleanup, cancel := f.session.cleanupOperation(ctx)
		defer cancel()
		return membership.Close(cleanup)
	}, nil
}

func afterFileAdmission(err error) error {
	if !storage.IsFileCallNotAdmitted(err) {
		return err
	}
	result := &storage.FileError{Code: storage.ErrnoOf(err), Cause: err}
	var fact *storage.FileError
	if errors.As(err, &fact) {
		result.Conflict = fact.Conflict
	}
	return result
}

func beforeFileAdmission(err error) error {
	if err == nil || storage.IsFileCallNotAdmitted(err) {
		return err
	}
	result := &storage.FileError{NotAdmitted: true, Code: storage.ErrnoOf(err), Cause: err}
	var fact *storage.FileError
	if errors.As(err, &fact) {
		result.Conflict = fact.Conflict
	}
	return result
}
