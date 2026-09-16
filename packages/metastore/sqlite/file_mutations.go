package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *fileReference) Rename(ctx context.Context, request storage.RenameRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := request.Check(); err != nil {
		return storage.FileActionReceipt{}, fileNotAdmitted(err)
	}
	if request.Source.ExpectedNodeID != uint64(f.id) || request.Source.ExpectedEntryID != f.entryID {
		return storage.FileActionReceipt{}, fileNotAdmitted(syscall.ESTALE)
	}
	output := request.NewName
	if output == nil {
		output = request.Destination.Name
	}
	moved := request.Source.ParentID != request.Destination.ParentID || !bytes.Equal(request.Source.Name, output)
	displacedOther := request.Destination.ExpectedNodeID != 0 && request.Destination.ExpectedNodeID != uint64(f.id)
	nodes := []int64{f.id}
	var effect storage.FileEffects
	if moved {
		effect |= storage.EffectEntryMoved
	}
	if displacedOther {
		nodes = append(nodes, int64(request.Destination.ExpectedNodeID))
		effect |= storage.EffectEntryDetached
	}
	return f.mutate(ctx, storage.OpFileRename, request, id, &volumeIntent{kind: locking.RenameMutation, nodes: nodes, totalUsage: true}, effect, func(tx *sql.Tx) error {
		if err := f.check(storage.RemoveEntry); err != nil {
			return err
		}
		s := f.session.store
		from, moving, _, err := f.session.resolveTarget(ctx, tx, request.Source)
		if err != nil {
			return err
		}
		to, displaced, _, err := f.session.resolveTarget(ctx, tx, request.Destination)
		if err != nil {
			return err
		}
		if moving.ID != f.id {
			return syscall.ESTALE
		}
		if !bytes.Equal(output, request.Destination.Name) {
			if err := f.checkRenameOutput(ctx, tx, to.ID, output); err != nil {
				return err
			}
		}
		if effect == 0 {
			return nil
		}
		if err := s.checkParentEntriesActive(ctx, tx, to.ID); err != nil {
			return err
		}
		if moving.IsDir() {
			if to.ID == moving.ID {
				return syscall.EINVAL
			}
			ancestry, err := s.captureLocation(ctx, tx, s.root, to.ID)
			if err != nil {
				return err
			}
			for _, at := range ancestry.Ancestors {
				if at.NodeID == uint64(moving.ID) {
					return syscall.EINVAL
				}
			}
		}
		before, err := s.captureEventImage(ctx, tx, moving)
		if err != nil {
			return err
		}
		if displacedOther {
			if displaced.IsDir() != moving.IsDir() {
				if displaced.IsDir() {
					return syscall.EISDIR
				}
				return syscall.ENOTDIR
			}
			if displaced.IsDir() {
				empty, err := s.isEmpty(ctx, tx, displaced.ID)
				if err != nil {
					return err
				}
				if !empty {
					return syscall.ENOTEMPTY
				}
			}
			if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: to.ID, Name: request.Destination.Name}, displaced); err != nil {
				return err
			}
			if err := s.unlink(ctx, tx, to.ID, request.Destination.Name); err != nil {
				return err
			}
			if err := s.discard(ctx, tx, displaced); err != nil {
				return err
			}
		}
		if moved {
			result, err := tx.ExecContext(ctx, `UPDATE entries SET parent=?,name=? WHERE volume=? AND id=? AND node=?`, to.ID, output, s.volume, int64(f.entryID), f.id)
			if err != nil {
				return err
			}
			if err := requireRetirementRow(result, syscall.ESTALE); err != nil {
				return err
			}
			if err := s.bumpDirectoryRevision(ctx, tx, from.ID); err != nil {
				return err
			}
			if to.ID != from.ID {
				if err := s.bumpDirectoryRevision(ctx, tx, to.ID); err != nil {
					return err
				}
			}
			if err := s.recordRenamed(ctx, tx, before, moving.ID); err != nil {
				return err
			}
		}
		now := time.Now()
		if err := s.touch(ctx, tx, from.ID, now, from); err != nil {
			return err
		}
		if to.ID != from.ID {
			return s.touch(ctx, tx, to.ID, now, to)
		}
		return nil
	})
}

func (f *fileReference) checkRenameOutput(ctx context.Context, tx *sql.Tx, parent int64, name []byte) error {
	var entry, node int64
	err := tx.QueryRowContext(ctx, `SELECT id,node FROM entries WHERE volume=? AND parent=? AND name=?`, f.session.store.volume, parent, name).Scan(&entry, &node)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if entry != int64(f.entryID) || node != f.id {
		return syscall.EEXIST
	}
	return nil
}

func (s *fileSession) SetNodeAttr(ctx context.Context, nodeID uint64, change storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if nodeID == 0 || nodeID > math.MaxInt64 {
		return storage.FileActionReceipt{}, syscall.ESTALE
	}
	if err := change.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	fingerprint, err := fileFingerprint(struct {
		Operation storage.Operation
		Node      uint64
		Change    storage.AttrChange
	}{storage.OpFileSetNodeAttr, nodeID, change})
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	done, err := s.acquire(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer done()
	a, fresh, err := s.admit(id, fingerprint, nil)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if !fresh {
		return s.result(a)
	}
	a.result.Operation = storage.OpFileSetNodeAttr
	ctx = s.nativeContext(ctx)
	var observed storage.FileObservation
	err = s.store.mutateTransactionLocked(ctx, ctx, &volumeIntent{kind: locking.SetAttrMutation, node: int64(nodeID)}, func(tx *sql.Tx) error {
		if err := s.check(ctx); err != nil {
			return err
		}
		if err := s.store.setNodeAttr(ctx, tx, int64(nodeID), change); err != nil {
			return err
		}
		probe := &fileReference{session: s, id: int64(nodeID)}
		var err error
		observed, err = probe.observation(ctx, tx, storage.ObservationOptions{})
		return err
	})
	if err == nil {
		a.result.Observation = observed
		if !change.Empty() {
			a.result.Effects = storage.EffectMetadataChanged
		}
	}
	return s.finish(a, err)
}

func (f *fileReference) SetKind(ctx context.Context, request storage.SetKindRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := request.Check(); err != nil {
		return storage.FileActionReceipt{}, fileNotAdmitted(err)
	}
	return f.mutate(ctx, storage.OpFileSetKind, request, id, &volumeIntent{kind: locking.SetAttrMutation, nodes: []int64{f.id}, totalUsage: true}, storage.EffectMetadataChanged|storage.EffectContentChanged, func(tx *sql.Tx) error {
		s := f.session.store
		if err := f.check(storage.WriteContent); err != nil {
			return err
		}
		before, err := s.nodeByID(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if before.MetadataRevision != request.ExpectedRevision {
			return syscall.EAGAIN
		}
		if request.Witness != nil {
			if request.Witness.NodeID != uint64(f.id) {
				return syscall.ESTALE
			}
			if err := s.validateLocation(ctx, tx, *request.Witness); err != nil {
				return err
			}
		}
		if s.coordinator.pins[retainedNode{s.volume, f.id}] != 1 {
			return syscall.EBUSY
		}
		if before.Size != 0 {
			return syscall.EINVAL
		}
		if before.IsDir() {
			empty, err := s.isEmpty(ctx, tx, f.id)
			if err != nil {
				return err
			}
			if !empty {
				return syscall.ENOTEMPTY
			}
		}
		revisionFloor := max(uint64(before.MetadataRevision), uint64(before.DirectoryRevision))
		if revisionFloor >= math.MaxInt64 {
			return syscall.EOVERFLOW
		}
		nextRevision := int64(revisionFloor + 1)
		directoryRevision := int64(0)
		if request.Kind == storage.NodeDirectory {
			directoryRevision = nextRevision
		}
		operation := storage.FileIO{Write: true, Length: max(before.Size, int64(len(request.LinkTarget))), Owner: request.Owner}
		if err := s.checkFileIOLocked(f.nativeContext(ctx), tx, f.id, before.Size, operation); err != nil {
			return err
		}
		metadata, err := storage.EncodeMetadata(request.Metadata)
		if err != nil {
			return err
		}
		if err := s.account(ctx, tx, int64(len(request.LinkTarget))-before.Size); err != nil {
			return err
		}
		if before.Content != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE objects SET state=? WHERE volume=? AND key=?`, stateGarbage, s.volume, string(before.Content)); err != nil {
				return err
			}
		}
		sec, nsec := sqlvalue.StoredTime(time.Now())
		_, err = tx.ExecContext(ctx, `UPDATE nodes SET kind=?,size=?,link_target=?,content=NULL,metadata=?,metadata_revision=?,directory_revision=?,change_sec=?,change_nsec=?,mtime_sec=?,mtime_nsec=? WHERE volume=? AND id=?`,
			request.Kind, len(request.LinkTarget), append([]byte{}, request.LinkTarget...), metadata, nextRevision, directoryRevision, sec, nsec, sec, nsec, s.volume, f.id)
		if err != nil {
			return err
		}
		if err := s.advanceContentRevision(ctx, tx, f.id); err != nil {
			return err
		}
		return s.recordNamedChangedMask(ctx, tx, before, metastore.ChangeContent)
	})
}

func (f *fileReference) validateRemoval(ctx context.Context, tx *sql.Tx, entry storage.EntryCondition, witness storage.EntryLocation, expected storage.NodeMetadataRevision, condition storage.RemovalCondition) error {
	if err := f.check(storage.RemoveEntry); err != nil {
		return err
	}
	if entry.NodeID != uint64(f.id) || entry.EntryID != f.entryID || witness.NodeID != uint64(f.id) {
		return syscall.ESTALE
	}
	if err := f.session.store.validateLocation(ctx, tx, witness); err != nil {
		return err
	}
	if err := f.session.store.checkEntryCondition(ctx, tx, entry); err != nil {
		return err
	}
	node, err := f.session.store.nodeByID(ctx, tx, f.id)
	if err != nil {
		return err
	}
	if expected != 0 && node.MetadataRevision != expected {
		return syscall.EAGAIN
	}
	if node.IsDir() && condition != storage.RemovalIfEmpty {
		return syscall.EISDIR
	}
	return nil
}

func (f *fileReference) PrepareRemoval(ctx context.Context, request storage.PrepareRemovalRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := request.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	result, err := f.mutate(ctx, storage.OpFilePrepareRemoval, request, id, &volumeIntent{kind: locking.RemoveMutation, node: f.id}, storage.EffectPreparedChanged, func(tx *sql.Tx) error {
		if err := f.validateRemoval(ctx, tx, request.Entry, request.Witness, request.ExpectedMetadataRevision, request.Condition); err != nil {
			return err
		}
		return f.installPrepared(ctx, tx, request.Condition)
	})
	if err == nil {
		result.Removal = result.Observation.Removal
	}
	return result, err
}

func (f *fileReference) CancelPrepared(ctx context.Context, intent storage.RemovalIntentID, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if intent == 0 {
		return storage.FileActionReceipt{}, syscall.EINVAL
	}
	result, err := f.mutate(ctx, storage.OpFileCancelPrepared, intent, id, nil, storage.EffectPreparedChanged, func(tx *sql.Tx) error {
		if f.intent != intent {
			return syscall.ESTALE
		}
		if err := f.session.store.cancelPreparedRemoval(ctx, tx, f.durableReference(), f.intentToken(intent)); err != nil {
			return err
		}
		f.intent = 0
		return nil
	})
	return result, err
}

func (f *fileReference) DrainEntry(ctx context.Context, request storage.DrainEntryRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := request.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	result, err := f.mutate(ctx, storage.OpFileDrainEntry, request, id, &volumeIntent{kind: locking.RemoveMutation, node: f.id}, storage.EffectDrainChanged, func(tx *sql.Tx) error {
		if err := f.validateRemoval(ctx, tx, request.Entry, request.Witness, request.ExpectedMetadataRevision, request.Condition); err != nil {
			return err
		}
		if err := f.session.store.checkDrainCapacity(ctx, tx, int64(f.entryID)); err != nil {
			return err
		}
		_, err := f.session.store.drainEntry(ctx, tx, int64(f.entryID), request.Condition == storage.RemovalIfEmpty, f.session.store.fileDomain.authority)
		return err
	})
	if err == nil {
		result.Removal = result.Observation.Removal
	}
	return result, err
}

func (f *fileReference) CancelDrain(ctx context.Context, request storage.CancelDrainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := request.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	return f.mutate(ctx, storage.OpFileCancelDrain, request, id, &volumeIntent{kind: locking.SetAttrMutation, node: f.id}, storage.EffectDrainChanged, func(tx *sql.Tx) error {
		if request.EntryID != f.entryID {
			return syscall.ESTALE
		}
		return f.session.store.cancelEntryDrain(ctx, tx, int64(request.EntryID), uint64(request.Generation))
	})
}

func (s *Store) checkDrainCapacity(ctx context.Context, tx *sql.Tx, entry int64) error {
	state, err := s.entryRetirementState(ctx, tx, entry)
	if err != nil {
		return err
	}
	if state.Draining {
		return nil
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM entries WHERE volume=? AND draining=1`, s.volume).Scan(&count); err != nil {
		return err
	}
	if count >= s.fileDomain.config.MaxDrains {
		return syscall.EAGAIN
	}
	return nil
}
