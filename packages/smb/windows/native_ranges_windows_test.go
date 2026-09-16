package windows

import (
	"context"
	"errors"
	"time"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/storage"
)

type nativeRangeKey struct {
	owner storage.RangeOwnerID
	node  uint64
	scope storage.RangeScope
}

func (s *nativeSession) rangeBudget(key nativeRangeKey) int {
	used := 0
	for k, n := range s.rangeCounts {
		if k != key {
			used += n
		}
	}
	return s.options.MaxRanges - used
}

func (f *nativeFile) rangeOwner(owner storage.RangeOwnerID, scope storage.RangeScope) (fileaccess.Owner, fileaccess.Scope, error) {
	o := fileaccess.Owner{Session: f.session.id, ID: uint64(owner)}
	s := fileaccess.Scope{Resource: f.node.attr.ID, Domain: uint64(scope.Domain), Enforced: scope.Enforced}
	if err := scope.Check(); err != nil {
		return o, s, err
	}
	return o, s, nil
}
func nativeRanges(ranges []storage.RangeAcquisition) ([]fileaccess.Acquisition, error) {
	out := make([]fileaccess.Acquisition, len(ranges))
	for i, r := range ranges {
		if err := r.Check(); err != nil {
			return nil, err
		}
		out[i] = fileaccess.Acquisition{ID: uint64(r.ID), Start: r.Start, End: r.End, Exclusive: r.Exclusive, Boundary: r.Boundary}
	}
	return out, nil
}
func nativeRangeError(err error) error {
	if errors.Is(err, fileaccess.ErrConflict) {
		return nativeConflict(storage.ConflictRange)
	}
	if errors.Is(err, fileaccess.ErrDeadlock) {
		return nativeConflict(storage.ConflictDeadlock)
	}
	return nativeAccessError(err)
}
func (f *nativeFile) RangeSnapshot(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope) (storage.RangeSnapshot, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := f.health(ctx); err != nil {
		return storage.RangeSnapshot{}, err
	}
	o, s, err := f.rangeOwner(owner, scope)
	if err != nil {
		return storage.RangeSnapshot{}, err
	}
	snap, err := a.access.Snapshot(o, s)
	if err != nil {
		return storage.RangeSnapshot{}, nativeRangeError(err)
	}
	result := storage.RangeSnapshot{Revision: snap.Revision, Available: snap.Available, OwnerAvailable: snap.OwnerAvailable}
	budget := f.session.rangeBudget(nativeRangeKey{owner, f.node.attr.ID, scope})
	result.Available = min(result.Available, budget)
	result.OwnerAvailable = min(result.OwnerAvailable, budget)
	if !f.session.owners[owner] && len(f.session.owners) >= f.session.options.MaxRangeOwners {
		result.OwnerAvailable = 0
	}
	for _, r := range snap.Own {
		result.Own = append(result.Own, storage.RangeAcquisition{ID: storage.RangeAcquisitionID(r.ID), Start: r.Start, End: r.End, Exclusive: r.Exclusive, Boundary: r.Boundary})
	}
	for _, r := range snap.Other {
		var epoch string
		for _, session := range a.sessions {
			if session.id == r.Owner.Session {
				epoch = session.epoch
				break
			}
		}
		result.Other = append(result.Other, storage.HeldRange{Owner: storage.RangeOwner{Session: epoch, ID: storage.RangeOwnerID(r.Owner.ID)}, Range: storage.RangeAcquisition{ID: storage.RangeAcquisitionID(r.Acquisition.ID), Start: r.Acquisition.Start, End: r.Acquisition.End, Exclusive: r.Acquisition.Exclusive, Boundary: r.Acquisition.Boundary}})
	}
	return result, nil
}
func (f *nativeFile) ReplaceRanges(ctx context.Context, r storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileReplaceRanges, r, func() (storage.FileActionReceipt, error) {
		key := nativeRangeKey{r.Owner, f.node.attr.ID, r.Scope}
		if len(r.Ranges) > f.session.rangeBudget(key) {
			return storage.FileActionReceipt{}, nativeConflict(storage.ConflictCapacity)
		}
		if len(r.Ranges) > 0 && !f.session.owners[r.Owner] && len(f.session.owners) >= f.session.options.MaxRangeOwners {
			return storage.FileActionReceipt{}, nativeConflict(storage.ConflictCapacity)
		}
		o, s, err := f.rangeOwner(r.Owner, r.Scope)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		ranges, err := nativeRanges(r.Ranges)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		revision, err := f.session.authority.access.ReplaceOwned(o, s, r.ExpectedRevision, ranges, f.publicationGuard(ctx))
		if err != nil {
			return storage.FileActionReceipt{}, nativeRangeError(err)
		}
		f.session.rangeCounts[key] = len(r.Ranges)
		f.session.syncRangeOwners()
		return storage.FileActionReceipt{Effects: storage.EffectRangesChanged, RangeRevision: revision}, nil
	})
}
func (s *nativeSession) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.run(ctx, id, storage.OpFileRetireRangeOwner, owner, func() (storage.FileActionReceipt, error) {
		if err := s.authority.access.RetireOwner(fileaccess.Owner{Session: s.id, ID: uint64(owner)}); err != nil {
			return storage.FileActionReceipt{}, nativeRangeError(err)
		}
		delete(s.owners, owner)
		for key := range s.rangeCounts {
			if key.owner == owner {
				delete(s.rangeCounts, key)
			}
		}
		return storage.FileActionReceipt{Effects: storage.EffectRangesChanged}, nil
	})
}
func (f *nativeFile) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileRetireRanges, struct {
		Owner storage.RangeOwnerID
		Scope storage.RangeScope
	}{owner, scope}, func() (storage.FileActionReceipt, error) {
		o, s, err := f.rangeOwner(owner, scope)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		a := f.session.authority.access
		if err := a.CancelOwnerWaits(o, s); err != nil {
			return storage.FileActionReceipt{}, nativeRangeError(err)
		}
		snap, err := a.Snapshot(o, s)
		if err != nil {
			return storage.FileActionReceipt{}, nativeRangeError(err)
		}
		revision, err := a.ReplaceOwned(o, s, snap.Revision, nil, f.publicationGuard(ctx))
		if err != nil {
			return storage.FileActionReceipt{}, nativeRangeError(err)
		}
		delete(f.session.rangeCounts, nativeRangeKey{owner, f.node.attr.ID, scope})
		f.session.syncRangeOwners()
		return storage.FileActionReceipt{Effects: storage.EffectRangesChanged, RangeRevision: revision}, nil
	})
}
func (f *nativeFile) WaitRanges(ctx context.Context, r storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	a := f.session.authority
	result, err := f.mutate(ctx, id, storage.OpFileWaitRanges, r, func() (storage.FileActionReceipt, error) {
		if len(f.session.waits) >= min(f.session.options.MaxWaiters, f.session.options.MaxPendingActions) {
			return storage.FileActionReceipt{}, nativeConflict(storage.ConflictCapacity)
		}
		o, s, err := f.rangeOwner(r.Owner, r.Scope)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		ranges, err := nativeRanges(r.Ranges)
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
		a.nextWait++
		wait, err := a.access.RegisterWait(a.nextWait, o, s, r.ExpectedRevision, ranges, r.DetectDeadlock, f.publicationGuard(ctx))
		if err != nil {
			return storage.FileActionReceipt{}, nativeRangeError(err)
		}
		f.session.waits[id] = wait
		return storage.FileActionReceipt{State: storage.FileActionPending}, nil
	})
	if err != nil || result.State != storage.FileActionPending {
		return result, err
	}
	a.mu.Lock()
	wait := f.session.waits[id]
	a.mu.Unlock()
	if wait == nil {
		return f.session.QueryAction(ctx, id)
	}
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type completion struct {
		revision uint64
		err      error
	}
	done := make(chan completion, 1)
	go func() { revision, err := wait.Await(waitCtx); done <- completion{revision, err} }()
	var completed completion
waiting:
	for {
		a.mu.Lock()
		health := f.health(ctx)
		expires := f.session.expires
		changed := f.session.changed
		a.mu.Unlock()
		if health != nil {
			cancel()
			completed = <-done
			completed.err = health
			break
		}
		timer := time.NewTimer(time.Until(expires))
		select {
		case completed = <-done:
			timer.Stop()
			break waiting
		case <-changed:
			timer.Stop()
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			cancel()
			completed = <-done
			break waiting
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	old := f.session.actions[id]
	if old.result.State != storage.FileActionPending {
		return nativeReceiptCopy(old.result), old.err
	}
	delete(f.session.waits, id)
	old.result.State = storage.FileActionCompleted
	old.result.RangeRevision = completed.revision
	if completed.err == nil {
		completed.err = f.health(ctx)
	}
	old.err = nativeRangeError(completed.err)
	if old.err != nil {
		old.result.State = storage.FileActionNotApplied
		old.result.Errno = storage.ErrnoOf(old.err)
	}
	f.session.actions[id] = old
	return nativeReceiptCopy(old.result), old.err
}

func (s *nativeSession) syncRangeOwners() {
	clear(s.owners)
	for key, n := range s.rangeCounts {
		if n > 0 {
			s.owners[key.owner] = true
		} else {
			delete(s.rangeCounts, key)
		}
	}
}
