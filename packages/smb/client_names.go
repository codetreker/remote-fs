package smb

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type directoryView struct {
	revision storage.DirectoryRevision
	entries  []storage.DirectoryEntry
	index    map[string]int
}

func (s *clientSession) directory(ctx context.Context, f storage.File, revision storage.DirectoryRevision) (directoryView, error) {
	if err := s.authorize(ctx, storage.OpFileListAt, 0, storage.AccessClaim{}, f.Reference(), f.NodeID(), 0, 0); err != nil {
		return directoryView{}, err
	}
	view := directoryView{revision: revision}
	var cursor storage.DirectoryCursor
	var charged int64
	for {
		remaining := s.backend.limits.MaxDirectoryBytes - charged
		if remaining < 256 {
			return directoryView{}, syscall.ENOMEM
		}
		maximum := remaining
		if maximum > storage.MaxDirectoryPageBytes {
			maximum = storage.MaxDirectoryPageBytes
		}
		page, err := f.ListAt(ctx, storage.DirectoryPageRequest{Revision: revision, Cursor: cursor, MaxEntries: storage.MaxDirectoryPageEntries, MaxBytes: int(maximum)})
		if err != nil {
			return directoryView{}, err
		}
		if page.ParentID != f.NodeID() || page.Revision == 0 || revision != 0 && page.Revision != revision {
			return directoryView{}, syscall.EIO
		}
		if revision == 0 {
			revision = page.Revision
			view.revision = revision
		}
		for _, entry := range page.Entries {
			cost := int64(512 + len(entry.Name)*5)
			for _, m := range entry.Attr.Metadata {
				cost += int64(len(m.Key) + len(m.Data) + 64)
			}
			if cost > s.backend.limits.MaxDirectoryBytes-charged {
				return directoryView{}, syscall.ENOMEM
			}
			charged += cost
			view.entries = append(view.entries, entry)
		}
		if page.Done {
			if page.Next.ParentID != 0 {
				return directoryView{}, syscall.EIO
			}
			break
		}
		if page.Next.ParentID == 0 || len(page.Entries) == 0 {
			return directoryView{}, syscall.EIO
		}
		cursor = page.Next
	}
	names := make([]localNameEntry, len(view.entries))
	for i, e := range view.entries {
		names[i] = localNameEntry{Name: e.Name, ID: e.Attr.ID}
	}
	index, err := directoryNameIndex(names, s.backend.limits.MaxDirectoryBytes)
	if err != nil {
		return directoryView{}, err
	}
	view.index = index
	return view, nil
}
func (s *clientSession) closeTemporary(ctx context.Context, f storage.File) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.backend.limits.CleanupTimeout)
	defer cancel()
	id, err := s.next(cleanup)
	if err == nil {
		var r storage.FileActionReceipt
		r, err = f.Close(cleanup, id)
		if r.State == storage.FileActionUnknown || r.State == storage.FileActionPending || r.State == 0 {
			r, err = s.raw.QueryAction(cleanup, id)
		}
		if err == nil && r.Errno == 0 && (r.State == storage.FileActionCompleted || r.State == storage.FileActionRetired) {
			return nil
		}
	}
	return cleanupFailed(errors.Join(err, s.requestRetirement(cleanup)))
}
func (s *clientSession) validateLocation(ctx context.Context, location storage.EntryLocation) error {
	if err := location.Check(); err != nil {
		return err
	}
	if location.RootNodeID != s.state.RootID {
		return syscall.EIO
	}
	if location.State == storage.LocationDetached {
		return syscall.ENOENT
	}
	for i, edge := range location.Ancestors {
		if _, err := nameKey(edge.Name); err != nil {
			return err
		}
		prefix := storage.EntryLocation{State: storage.LocationRoot, RootNodeID: location.RootNodeID, NodeID: edge.ParentID}
		if i > 0 {
			prefix.State = storage.LocationLinked
			prefix.Ancestors = location.Ancestors[:i]
		}
		id, err := s.next(ctx)
		if err != nil {
			return err
		}
		if err = s.authorize(ctx, storage.OpFileRetain, fileEffects(storage.OpFileRetain), storage.AccessClaim{}, 0, edge.ParentID, 0, 0); err != nil {
			return err
		}
		receipt, err := s.raw.Retain(ctx, storage.RetainRequest{NodeID: edge.ParentID, Witness: &prefix}, id)
		if err != nil {
			return s.retentionFailure(ctx, receipt, err)
		}
		parent, err := s.raw.Reference(ctx, receipt.Reference)
		if err != nil {
			return s.cleanupOwnership(ctx, err)
		}
		if parent == nil || parent.Reference() != receipt.Reference || parent.NodeID() != edge.ParentID {
			return s.cleanupOwnership(ctx, syscall.EIO)
		}
		view, viewErr := s.directory(ctx, parent, edge.DirectoryRevision)
		if viewErr == nil {
			key, _ := nameKey(edge.Name)
			at, ok := view.index[key]
			if !ok || view.entries[at].EntryID != edge.EntryID || view.entries[at].Attr.ID != edge.NodeID {
				viewErr = syscall.ESTALE
			}
		}
		if viewErr == nil && i == len(location.Ancestors)-1 {
			_, viewErr = parent.CheckObservation(ctx, storage.ObservationCondition{MetadataRevision: receipt.Observation.Attr.MetadataRevision, DirectoryRevision: edge.DirectoryRevision, Location: prefix})
		}
		if closeErr := s.closeTemporary(ctx, parent); closeErr != nil {
			return errors.Join(viewErr, closeErr)
		}
		if viewErr != nil {
			return viewErr
		}
	}
	return nil
}
func (s *clientSession) retentionFailure(ctx context.Context, r storage.FileActionReceipt, cause error) error {
	if r.Reference != 0 || r.State == 0 || r.State == storage.FileActionUnknown || r.State == storage.FileActionPending {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.backend.limits.CleanupTimeout)
		defer cancel()
		return cleanupFailed(errors.Join(cause, s.requestRetirement(cleanup)))
	}
	return cause
}
func (s *clientSession) lookup(ctx context.Context, l windowsLookup) (storage.EntryTarget, storage.FileObservation, bool, error) {
	if err := l.Check(); err != nil {
		return storage.EntryTarget{}, storage.FileObservation{}, false, err
	}
	if err := s.authorize(ctx, storage.OpFileStat, 0, storage.AccessClaim{}, l.ParentReference, l.ParentID, 0, 0); err != nil {
		return storage.EntryTarget{}, storage.FileObservation{}, false, err
	}
	parent, err := s.raw.Reference(ctx, l.ParentReference)
	if err != nil {
		return storage.EntryTarget{}, storage.FileObservation{}, false, err
	}
	if parent == nil {
		return storage.EntryTarget{}, storage.FileObservation{}, false, syscall.EIO
	}
	if parent.NodeID() != l.ParentID {
		return storage.EntryTarget{}, storage.FileObservation{}, false, syscall.ESTALE
	}
	observation, err := parent.Stat(ctx, storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		return storage.EntryTarget{}, storage.FileObservation{}, false, err
	}
	if l.proof != nil && (observation.Location == nil || !sameEntryChain(l.proof.Location, *observation.Location)) {
		return storage.EntryTarget{}, observation, false, syscall.ESTALE
	}
	if observation.Location == nil {
		return storage.EntryTarget{}, storage.FileObservation{}, false, syscall.EIO
	}
	if err = s.validateLocation(ctx, *observation.Location); err != nil {
		return storage.EntryTarget{}, storage.FileObservation{}, false, err
	}
	view, err := s.directory(ctx, parent, observation.Attr.DirectoryRevision)
	if err != nil {
		return storage.EntryTarget{}, storage.FileObservation{}, false, err
	}
	witness := observation.Location.Clone()
	target := storage.EntryTarget{Parent: l.ParentReference, ParentID: l.ParentID, Name: []byte(l.Name), DirectoryRevision: view.revision, Witness: &witness}
	key, _ := nameKey([]byte(l.Name))
	at, exists := view.index[key]
	var found storage.FileObservation
	if exists {
		entry := view.entries[at]
		target.Name = entry.Name
		target.ExpectedEntryID = entry.EntryID
		target.ExpectedNodeID = entry.Attr.ID
		target.ExpectedMetadataRevision = entry.Attr.MetadataRevision
		found.Attr = entry.Attr
	}
	if l.ExpectedID != 0 && (!exists || target.ExpectedNodeID != l.ExpectedID) {
		return target, found, exists, syscall.ESTALE
	}
	if _, err = parent.CheckObservation(ctx, storage.ObservationCondition{MetadataRevision: observation.Attr.MetadataRevision, DirectoryRevision: view.revision, Location: *observation.Location}); err != nil {
		return target, found, exists, err
	}
	return target, found, exists, nil
}
func (f *clientFile) ObserveName(ctx context.Context) (windowsAttr, error) {
	if err := f.authorize(ctx, storage.OpFileStat, 0); err != nil {
		return windowsAttr{}, err
	}
	observation, err := f.raw.Stat(ctx, storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		return windowsAttr{}, err
	}
	if observation.Location == nil {
		return windowsAttr{}, syscall.EIO
	}
	if observation.Location.State != storage.LocationDetached {
		if err = f.session.validateLocation(ctx, *observation.Location); err != nil {
			return windowsAttr{}, err
		}
	}
	checked, err := f.raw.CheckObservation(ctx, storage.ObservationCondition{MetadataRevision: observation.Attr.MetadataRevision, DirectoryRevision: observation.Attr.DirectoryRevision, Location: *observation.Location})
	if err != nil {
		return windowsAttr{}, err
	}
	return projectWindowsAttr(checked, f.session.backend.defaults)
}
func (f *clientFile) ListBounded(ctx context.Context, result *windowsListResult) (err error) {
	defer func() {
		if err != nil {
			err = result.Fail(err)
		}
	}()
	if result == nil {
		return syscall.EINVAL
	}
	if err := f.require(windowsReadData); err != nil {
		return err
	}
	observation, err := f.raw.Stat(ctx, storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		return err
	}
	if observation.Location == nil {
		return syscall.EIO
	}
	if err = f.session.validateLocation(ctx, *observation.Location); err != nil {
		return err
	}
	view, err := f.session.directory(ctx, f.raw, observation.Attr.DirectoryRevision)
	if err != nil {
		return err
	}
	if _, err = f.raw.CheckObservation(ctx, storage.ObservationCondition{MetadataRevision: observation.Attr.MetadataRevision, DirectoryRevision: view.revision, Location: *observation.Location}); err != nil {
		return err
	}
	for _, entry := range view.entries {
		attr, err := projectWindowsAttr(storage.FileObservation{Attr: entry.Attr}, f.session.backend.defaults)
		if err != nil {
			return err
		}
		if err = result.Add(windowsEntry{Name: string(entry.Name), Attr: attr.windowsBasicAttr}); err != nil {
			return err
		}
	}
	return nil
}

func sameEntryChain(a, b storage.EntryLocation) bool {
	if a.State != b.State || a.RootNodeID != b.RootNodeID || a.NodeID != b.NodeID || len(a.Ancestors) != len(b.Ancestors) {
		return false
	}
	for i, x := range a.Ancestors {
		y := b.Ancestors[i]
		if x.ParentID != y.ParentID || x.EntryID != y.EntryID || x.NodeID != y.NodeID || string(x.Name) != string(y.Name) {
			return false
		}
	}
	return true
}

func (s *clientSession) cleanupOwnership(ctx context.Context, cause error) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.backend.limits.CleanupTimeout)
	defer cancel()
	return cleanupFailed(errors.Join(cause, s.requestRetirement(cleanup)))
}
