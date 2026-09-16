package sqlite

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileRangeKey struct {
	node  int64
	owner storage.RangeOwnerID
	scope storage.RangeScope
}

func fileAccessError(err error) error {
	if err == nil {
		return nil
	}
	var existing *storage.FileError
	if errors.As(err, &existing) {
		return err
	}
	code := storage.ErrnoOf(err)
	var conflict *storage.FileConflict
	switch {
	case errors.Is(err, fileaccess.ErrInvalid):
		code = syscall.EINVAL
	case errors.Is(err, fileaccess.ErrRevision):
		code = syscall.EAGAIN
		conflict = &storage.FileConflict{Kind: storage.ConflictRevision}
	case errors.Is(err, fileaccess.ErrConflict):
		code = syscall.EAGAIN
		conflict = &storage.FileConflict{Kind: storage.ConflictRange}
	case errors.Is(err, fileaccess.ErrAccess):
		code = syscall.EACCES
	case errors.Is(err, fileaccess.ErrCapacity):
		code = syscall.EAGAIN
		conflict = &storage.FileConflict{Kind: storage.ConflictCapacity}
	case errors.Is(err, fileaccess.ErrDeadlock):
		code = syscall.EDEADLK
		conflict = &storage.FileConflict{Kind: storage.ConflictDeadlock}
	case errors.Is(err, fileaccess.ErrUnknownClaim), errors.Is(err, fileaccess.ErrRetired), errors.Is(err, fileaccess.ErrClosed):
		code = syscall.ESTALE
		conflict = &storage.FileConflict{Kind: storage.ConflictRetired}
	case errors.Is(err, fileaccess.ErrExhausted):
		code = syscall.EOVERFLOW
	case errors.Is(err, fileaccess.ErrCanceled), errors.Is(err, context.Canceled):
		code = syscall.EINTR
	}
	return &storage.FileError{Code: code, Conflict: conflict, Cause: err}
}
func (s *fileSession) rangeOwner(id storage.RangeOwnerID) (fileaccess.Owner, error) {
	owner := fileaccess.Owner{Session: s.id, ID: uint64(id)}
	if _, ok := s.owners[id]; ok {
		return owner, nil
	}
	if len(s.owners) >= s.options.MaxRangeOwners {
		return owner, fileaccess.ErrCapacity
	}
	s.owners[id] = struct{}{}
	return owner, nil
}
func (s *fileSession) releaseIdleRangeOwner(id storage.RangeOwnerID) error {
	if _, known := s.owners[id]; !known {
		return nil
	}
	count, err := s.store.fileDomain.access.OwnerRangeCount(fileaccess.Owner{Session: s.id, ID: uint64(id)})
	if err != nil {
		return err
	}
	if count != 0 {
		return nil
	}
	delete(s.owners, id)
	for key := range s.rangeCounts {
		if key.owner == id {
			delete(s.rangeCounts, key)
		}
	}
	return nil
}

func nativeRanges(ranges []storage.RangeAcquisition) ([]fileaccess.Acquisition, error) {
	result := make([]fileaccess.Acquisition, len(ranges))
	for i, r := range ranges {
		if err := r.Check(); err != nil {
			return nil, err
		}
		result[i] = fileaccess.Acquisition{ID: uint64(r.ID), Start: r.Start, End: r.End, Boundary: r.Boundary, Exclusive: r.Exclusive}
	}
	return result, nil
}
func publicRange(r fileaccess.Acquisition) storage.RangeAcquisition {
	return storage.RangeAcquisition{ID: storage.RangeAcquisitionID(r.ID), Start: r.Start, End: r.End, Boundary: r.Boundary, Exclusive: r.Exclusive}
}
func (f *fileReference) rangeScope(scope storage.RangeScope) (fileaccess.Scope, error) {
	if err := scope.Check(); err != nil {
		return fileaccess.Scope{}, err
	}
	if err := f.check(0); err != nil {
		return fileaccess.Scope{}, err
	}
	return fileaccess.Scope{Resource: uint64(f.id), Domain: uint64(scope.Domain), Enforced: scope.Enforced}, nil
}
func (s *fileSession) remainingRanges(key fileRangeKey) int {
	total := 0
	for _, n := range s.rangeCounts {
		total += n
	}
	return s.options.MaxRanges - total + s.rangeCounts[key]
}
func (f *fileReference) RangeSnapshot(ctx context.Context, id storage.RangeOwnerID, scope storage.RangeScope) (storage.RangeSnapshot, error) {
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.RangeSnapshot{}, err
	}
	defer done()
	nativeScope, err := f.rangeScope(scope)
	if err != nil {
		return storage.RangeSnapshot{}, err
	}
	owner := fileaccess.Owner{Session: f.session.id, ID: uint64(id)}
	snapshot, err := f.session.store.fileDomain.access.Snapshot(owner, nativeScope)
	if err != nil {
		return storage.RangeSnapshot{}, fileAccessError(err)
	}
	key := fileRangeKey{f.id, id, scope}
	result := storage.RangeSnapshot{Revision: snapshot.Revision, Available: min(snapshot.Available, f.session.remainingRanges(key)), OwnerAvailable: min(snapshot.OwnerAvailable, f.session.remainingRanges(key))}
	if _, known := f.session.owners[id]; !known && len(f.session.owners) >= f.session.options.MaxRangeOwners {
		result.OwnerAvailable = 0
	}
	for _, r := range snapshot.Own {
		result.Own = append(result.Own, publicRange(r))
	}
	for _, held := range snapshot.Other {
		epoch := ""
		for session := range f.session.store.fileDomain.sessions {
			if session.id == held.Owner.Session {
				epoch = session.epoch
				break
			}
		}
		if epoch == "" {
			return storage.RangeSnapshot{}, syscall.EIO
		}
		result.Other = append(result.Other, storage.HeldRange{Owner: storage.RangeOwner{Session: epoch, ID: storage.RangeOwnerID(held.Owner.ID)}, Range: publicRange(held.Acquisition)})
	}
	return result, nil
}
func (f *fileReference) ReplaceRanges(ctx context.Context, request storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if len(request.Ranges) > min(f.session.options.MaxRanges, f.session.maxRangeSet) {
		return storage.FileActionReceipt{}, syscall.EFBIG
	}
	if err := request.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	ranges, err := nativeRanges(request.Ranges)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	fp, err := fileFingerprint(struct {
		Op      storage.Operation
		Request storage.RangeReplaceRequest
	}{storage.OpFileReplaceRanges, request})
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer done()
	a, fresh, err := f.session.admit(id, fp, f)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if !fresh {
		return f.session.result(a)
	}
	a.result.Operation = storage.OpFileReplaceRanges
	scope, err := f.rangeScope(request.Scope)
	if err != nil {
		return f.session.finishRange(a, request.Owner, err)
	}
	owner := fileaccess.Owner{Session: f.session.id, ID: uint64(request.Owner)}
	if len(ranges) > 0 {
		owner, err = f.session.rangeOwner(request.Owner)
		if err != nil {
			return f.session.finishRange(a, request.Owner, err)
		}
	}
	key := fileRangeKey{f.id, request.Owner, request.Scope}
	if len(ranges) > f.session.remainingRanges(key) {
		return f.session.finishRange(a, request.Owner, fileaccess.ErrCapacity)
	}
	revision, err := f.session.store.fileDomain.access.ReplaceOwned(owner, scope, request.ExpectedRevision, ranges, f.lifetimeGuardLocked(ctx))
	a.result.RangeRevision = revision
	if err == nil {
		f.session.rangeCounts[key] = len(ranges)
		if len(ranges) == 0 {
			delete(f.session.rangeCounts, key)
		}
		if revision != request.ExpectedRevision {
			a.result.Effects |= storage.EffectRangesChanged
		}
	}
	return f.session.finishRange(a, request.Owner, err)
}

func (f *fileReference) WaitRanges(ctx context.Context, request storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s := f.session
	if len(request.Ranges) > min(s.options.MaxRanges, s.maxRangeSet) {
		return storage.FileActionReceipt{}, syscall.EFBIG
	}
	if err := request.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	ranges, err := nativeRanges(request.Ranges)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	fp, err := fileFingerprint(struct {
		Op      storage.Operation
		Request storage.RangeWaitRequest
	}{storage.OpFileWaitRanges, request})
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	done, err := s.acquire(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	a, fresh, err := s.admit(id, fp, f)
	if err != nil {
		done()
		return storage.FileActionReceipt{}, err
	}
	if !fresh {
		r, e := s.result(a)
		done()
		return r, e
	}
	a.result.Operation = storage.OpFileWaitRanges
	fail := func(err error) (storage.FileActionReceipt, error) {
		r, e := s.finishRange(a, request.Owner, err)
		done()
		return r, e
	}
	scope, err := f.rangeScope(request.Scope)
	if err != nil {
		return fail(err)
	}
	owner := fileaccess.Owner{Session: s.id, ID: uint64(request.Owner)}
	if s.waiters >= s.options.MaxWaiters {
		return fail(fileaccess.ErrCapacity)
	}
	waitID, err := s.store.fileDomain.allocateIntent()
	if err != nil {
		return fail(err)
	}
	wait, err := s.store.fileDomain.access.RegisterWait(waitID, owner, scope, request.ExpectedRevision, ranges, request.DetectDeadlock, f.lifetimeGuardLocked(ctx))
	if err != nil {
		return fail(err)
	}
	a.wait = wait
	a.waitOwner = request.Owner
	a.waitScope = request.Scope
	s.waiters++
	done()
	revision, waitErr := wait.Await(ctx)
	if errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded) {
		waitErr = &storage.FileError{Code: syscall.EINTR, Cause: waitErr}
	}
	cleanup, cancel := context.WithTimeout(s.cleanup, s.cleanupTimeout)
	defer cancel()
	if err := s.store.coordinator.commit.acquire(cleanup); err != nil {
		s.store.coordinator.poisonWith(err)
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileWaitRanges, State: storage.FileActionUnknown}, errors.Join(syscall.EIO, err)
	}
	defer s.store.coordinator.commit.release()
	if a.result.State != storage.FileActionPending {
		return s.result(a)
	}
	a.result.RangeRevision = revision
	if waitErr == nil {
		waitErr = s.check(cleanup)
	}
	return s.finish(a, waitErr)
}

func (s *fileSession) RetireRangeOwner(ctx context.Context, ownerID storage.RangeOwnerID, id storage.FileActionID) (storage.FileActionReceipt, error) {
	fp, err := fileFingerprint(struct {
		Op    storage.Operation
		Owner storage.RangeOwnerID
	}{storage.OpFileRetireRangeOwner, ownerID})
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	done, err := s.acquire(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer done()
	a, fresh, err := s.admit(id, fp, nil)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if !fresh {
		return s.result(a)
	}
	a.result.Operation = storage.OpFileRetireRangeOwner
	if err := s.lifetimeGuardLocked(ctx)(); err != nil {
		return s.finish(a, err)
	}
	err = s.store.fileDomain.access.RetireOwner(fileaccess.Owner{Session: s.id, ID: uint64(ownerID)})
	if err == nil {
		delete(s.owners, ownerID)
		for key, n := range s.rangeCounts {
			if key.owner == ownerID {
				if n != 0 {
					a.result.Effects |= storage.EffectRangesChanged
				}
				delete(s.rangeCounts, key)
			}
		}
	}
	revision, revisionErr := s.store.fileDomain.access.Revision()
	a.result.RangeRevision = revision
	err = errors.Join(err, revisionErr)
	return s.finish(a, err)
}
func (f *fileReference) RetireRangeOwner(ctx context.Context, ownerID storage.RangeOwnerID, scope storage.RangeScope, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s := f.session
	fp, err := fileFingerprint(struct {
		Op    storage.Operation
		Owner storage.RangeOwnerID
		Scope storage.RangeScope
	}{storage.OpFileRetireRanges, ownerID, scope})
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	done, err := s.acquire(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer done()
	a, fresh, err := s.admit(id, fp, f)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if !fresh {
		return s.result(a)
	}
	a.result.Operation = storage.OpFileRetireRanges
	nativeScope, err := f.rangeScope(scope)
	if err != nil {
		return s.finishRange(a, ownerID, err)
	}
	owner := fileaccess.Owner{Session: s.id, ID: uint64(ownerID)}
	snapshot, err := s.store.fileDomain.access.Snapshot(owner, nativeScope)
	if err != nil {
		return s.finishRange(a, ownerID, err)
	}
	revision, err := s.store.fileDomain.access.ReplaceOwned(owner, nativeScope, snapshot.Revision, nil, f.lifetimeGuardLocked(ctx))
	a.result.RangeRevision = revision
	if err == nil {
		delete(s.rangeCounts, fileRangeKey{f.id, ownerID, scope})
		if len(snapshot.Own) > 0 {
			a.result.Effects |= storage.EffectRangesChanged
		}
	}
	if err == nil {
		err = s.store.fileDomain.access.CancelOwnerWaits(owner, nativeScope)
		if err == nil {
			s.retireScopeWaitsLocked(ownerID, scope, f.id)
		}
	}
	return s.finishRange(a, ownerID, err)
}

func (s *fileSession) finishRange(a *fileAction, owner storage.RangeOwnerID, err error) (storage.FileActionReceipt, error) {
	if cleanupErr := s.releaseIdleRangeOwner(owner); cleanupErr != nil {
		err = &storage.FileError{Code: syscall.EIO, Cause: errors.Join(err, cleanupErr)}
	}
	return s.finish(a, err)
}

func (s *fileSession) retireScopeWaitsLocked(owner storage.RangeOwnerID, scope storage.RangeScope, node int64) {
	for _, action := range s.actions {
		if action.wait == nil || action.waitOwner != owner || action.waitScope != scope || action.file == nil || action.file.id != node {
			continue
		}
		action.wait.Cancel()
		s.finish(action, fileaccess.ErrRetired)
	}
}
