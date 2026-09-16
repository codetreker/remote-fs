package windows

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (a *nativeAuthority) empty(n *nativeNode) bool {
	if !n.attr.IsDir() {
		return true
	}
	for _, child := range a.nodes {
		if !child.detached && child.parent == n.attr.ID {
			return false
		}
	}
	return true
}
func (f *nativeFile) removal() storage.RemovalStatus {
	r := f.session.authority.removal(f.node)
	if f.prepared != nil && !f.closed {
		r.Prepared = true
		r.PreparedCondition = f.prepared.PreparedCondition
		r.IntentID = f.prepared.IntentID
	}
	return r
}
func (f *nativeFile) prepareCondition(condition storage.RemovalCondition) error {
	n := f.node
	if n.attr.ID == 1 || n.detached {
		return syscall.EBUSY
	}
	if condition != storage.RemovalFile && condition != storage.RemovalIfEmpty {
		return syscall.EINVAL
	}
	if n.attr.IsDir() != (condition == storage.RemovalIfEmpty) {
		return syscall.EINVAL
	}
	if err := f.session.authority.use(n, f.reference, storage.RemoveEntry); err != nil {
		return err
	}
	f.prepared = &storage.RemovalStatus{IntentID: storage.RemovalIntentID(f.reference), EntryID: n.entry, Prepared: true, PreparedCondition: condition, State: n.state, Generation: n.generation}
	return nil
}
func (f *nativeFile) prepare(r storage.PrepareRemovalRequest) error {
	if err := r.Check(); err != nil {
		return err
	}
	a := f.session.authority
	if r.ExpectedMetadataRevision != 0 && r.ExpectedMetadataRevision != f.node.attr.MetadataRevision {
		return nativeConflict(storage.ConflictRevision)
	}
	if err := f.session.checkWitness(r.Witness); err != nil {
		return err
	}
	location := a.location(f.node)
	if len(location.Ancestors) == 0 {
		return syscall.EBUSY
	}
	current := location.Ancestors[len(location.Ancestors)-1]
	if current.ParentID != r.Entry.ParentID || current.NodeID != r.Entry.NodeID || current.EntryID != r.Entry.EntryID || current.DirectoryRevision != r.Entry.DirectoryRevision || string(current.Name) != string(r.Entry.Name) {
		return nativeConflict(storage.ConflictRevision)
	}
	return f.prepareCondition(r.Condition)
}
func (f *nativeFile) PrepareRemoval(ctx context.Context, r storage.PrepareRemovalRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFilePrepareRemoval, r, func() (storage.FileActionReceipt, error) {
		if err := f.prepare(r); err != nil {
			return storage.FileActionReceipt{}, err
		}
		return storage.FileActionReceipt{Removal: f.removal(), Effects: storage.EffectPreparedChanged, Observation: f.observation()}, nil
	})
}
func (f *nativeFile) CancelPrepared(ctx context.Context, intent storage.RemovalIntentID, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileCancelPrepared, intent, func() (storage.FileActionReceipt, error) {
		if f.prepared == nil || f.prepared.IntentID != intent {
			return storage.FileActionReceipt{}, nativeConflict(storage.ConflictIdentity)
		}
		f.prepared = nil
		return storage.FileActionReceipt{Removal: f.removal(), Effects: storage.EffectPreparedChanged}, nil
	})
}
func (f *nativeFile) DrainEntry(ctx context.Context, r storage.DrainEntryRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileDrainEntry, r, func() (storage.FileActionReceipt, error) {
		p := storage.PrepareRemovalRequest{Entry: r.Entry, Witness: r.Witness, Condition: r.Condition, ExpectedMetadataRevision: r.ExpectedMetadataRevision}
		old := f.prepared
		if err := f.prepare(p); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if !f.session.authority.empty(f.node) {
			f.prepared = old
			return storage.FileActionReceipt{}, syscall.ENOTEMPTY
		}
		f.prepared = old
		f.node.state = storage.EntryDraining
		f.node.generation++
		f.node.drainCondition = r.Condition
		return storage.FileActionReceipt{Removal: f.removal(), Effects: storage.EffectDrainChanged}, nil
	})
}
func (f *nativeFile) CancelDrain(ctx context.Context, r storage.CancelDrainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileCancelDrain, r, func() (storage.FileActionReceipt, error) {
		n := f.node
		if n.entry != r.EntryID || n.state != storage.EntryDraining || n.generation != r.Generation {
			return storage.FileActionReceipt{}, nativeConflict(storage.ConflictRevision)
		}
		n.state = storage.EntryActive
		n.drainCondition = 0
		n.generation++
		return storage.FileActionReceipt{Removal: storage.RemovalStatus{EntryID: n.entry, State: n.state, Generation: n.generation}, Effects: storage.EffectDrainChanged}, nil
	})
}
func (f *nativeFile) ReplaceClaim(ctx context.Context, claim storage.AccessClaim, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.mutate(ctx, id, storage.OpFileReplaceClaim, claim, func() (storage.FileActionReceipt, error) {
		if err := claim.Check(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		if err := f.session.authority.access.ReplaceClaim(uint64(f.reference), fileaccess.Claim{Uses: uint64(claim.Uses), Excludes: uint64(claim.Excludes)}, f.publicationGuard(ctx)); err != nil {
			return storage.FileActionReceipt{}, nativeAccessError(err)
		}
		f.claim = claim
		return storage.FileActionReceipt{Effects: storage.EffectClaimChanged}, nil
	})
}
func (f *nativeFile) release() error {
	if f.closed {
		return nil
	}
	a := f.session.authority
	n := f.node
	f.closed = true
	if err := a.access.CloseClaim(uint64(f.reference)); err != nil {
		return err
	}
	if f.prepared != nil && !n.detached && n.state == storage.EntryActive && f.prepared.Generation == n.generation {
		n.state = storage.EntryDraining
		n.drainCondition = f.prepared.PreparedCondition
		n.generation++
	}
	count, err := a.access.ClaimCount(n.attr.ID)
	if err != nil {
		return err
	}
	if count == 0 && n.detached {
		n.data = nil
		delete(a.nodes, n.attr.ID)
		return nil
	}
	if count != 0 || n.state != storage.EntryDraining || n.detached {
		return nil
	}
	if n.drainCondition == storage.RemovalFile && n.attr.IsDir() {
		n.state = storage.EntryActive
		n.drainCondition = 0
		return syscall.EISDIR
	}
	if !a.empty(n) {
		n.state = storage.EntryActive
		n.drainCondition = 0
		return syscall.ENOTEMPTY
	}
	a.record(n, metastore.Removed, metastore.ChangeName, a.image(n))
	a.nodes[n.parent].attr.DirectoryRevision++
	n.detached = true
	n.state = storage.EntryDetached
	n.drainCondition = 0
	n.data = nil
	delete(a.nodes, n.attr.ID)
	return nil
}
func (f *nativeFile) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := id.Epoch(); err != nil {
		return storage.FileActionReceipt{}, nativeNotAdmitted(err)
	}
	if f.closed {
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileClose, State: storage.FileActionRetired, Reference: f.reference, HistoryRemaining: f.session.options.History}, nil
	}
	return f.session.runLocked(ctx, id, storage.OpFileClose, f.reference, func() (storage.FileActionReceipt, error) {
		err := f.release()
		return storage.FileActionReceipt{Reference: f.reference, Effects: storage.EffectReferenceRetired, Observation: f.observation()}, err
	})
}

func (s *nativeSession) retire() error {
	var failures []error
	for _, f := range s.files {
		if err := f.release(); err != nil {
			failures = append(failures, err)
		}
	}
	if err := s.authority.access.RetireSession(s.id); err != nil {
		failures = append(failures, err)
	}
	s.closed = true
	close(s.changed)
	s.changed = make(chan struct{})
	return errors.Join(failures...)
}
func (s *nativeSession) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if _, err := id.Epoch(); err != nil {
		return storage.FileActionReceipt{}, nativeNotAdmitted(err)
	}
	if s.closed {
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileSessionClose, State: storage.FileActionRetired, HistoryRemaining: s.options.History}, nil
	}
	return s.runLocked(ctx, id, storage.OpFileSessionClose, nil, func() (storage.FileActionReceipt, error) {
		err := s.retire()
		return storage.FileActionReceipt{Effects: storage.EffectReferenceRetired}, err
	})
}
