package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *fileReference) Retire(ctx context.Context) error {
	if err := f.session.store.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer f.session.store.coordinator.commit.release()
	f.active = false
	f.session.retireWaitsLocked(f)
	return nil
}

func (f *fileReference) BeginClose(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, bool, error) {
	fingerprint, err := fileFingerprint(storage.OpFileClose)
	if err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	if err := f.session.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	defer f.session.store.coordinator.commit.release()
	a, proceed, err := f.session.admitClose(id, fingerprint, f)
	if err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	if !proceed {
		if a == nil {
			return f.session.retiredClose(f), false, nil
		}
		result, err := f.session.result(a)
		return result, false, err
	}
	f.active = false
	f.session.retireWaitsLocked(f)
	if a == nil {
		return storage.FileActionReceipt{}, true, nil
	}
	a.result.Operation = storage.OpFileClose
	a.result.Effects |= storage.EffectReferenceRetired
	result, err := f.session.result(a)
	return result, true, err
}

func (f *fileReference) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	fingerprint, err := fileFingerprint(storage.OpFileClose)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if err := f.session.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer f.session.store.coordinator.commit.release()
	a, proceed, err := f.session.admitClose(id, fingerprint, f)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if !proceed {
		if a == nil {
			return f.session.retiredClose(f), nil
		}
		return f.session.result(a)
	}
	f.active = false
	effects, err := f.closeLocked(ctx)
	return f.session.finishClose(a, f, effects, err)
}

func (f *fileReference) closeLocked(ctx context.Context) (storage.FileEffects, error) {
	if f.closed {
		return 0, nil
	}
	f.active = false
	effects := storage.EffectReferenceRetired
	if err := f.session.store.reapFinishedIOLocked(); err != nil {
		return effects, err
	}
	ioPending := f.ioUsers != 0
	s := f.session.store
	if err := s.coordinator.healthy(); err != nil {
		return effects, err
	}
	key := retainedNode{s.volume, f.id}
	count := s.coordinator.pins[key]
	if count < 1 {
		return effects, syscall.EIO
	}
	var state metastore.FileState
	var entry entryRetirement
	prepared, activates := f.intent != 0, false
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = s.fileState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if f.entryID != 0 {
			entry, err = s.entryRetirementState(ctx, tx, int64(f.entryID))
			if err != nil && !errors.Is(err, syscall.ESTALE) {
				return err
			}
		}
		if prepared && entry.EntryID != 0 {
			var ifEmpty bool
			if err := tx.QueryRowContext(ctx, `SELECT if_empty FROM removal_intents WHERE volume=? AND reference=? AND token=?`, s.volume, f.durableReference(), f.intentToken(f.intent)).Scan(&ifEmpty); err != nil {
				return err
			}
			activates = true
			if ifEmpty {
				empty, err := s.isEmpty(ctx, tx, f.id)
				if err != nil {
					return err
				}
				activates = empty
			}
		}
		return nil
	})
	if err != nil {
		return effects, err
	}
	finalDetach := !ioPending && count == 1 && (entry.Draining || activates)
	if prepared || finalDetach || !ioPending && count == 1 && state.Detached {
		var intent *volumeIntent
		if activates || finalDetach {
			intent = &volumeIntent{kind: locking.RemoveMutation, nodes: []int64{f.id}, totalUsage: true}
		}
		if !ioPending && count == 1 && state.Detached {
			intent = &volumeIntent{kind: locking.RemoveMutation, node: f.id, cleanup: true}
		}
		handle := uint64(f.reference)
		if f.claimReleased {
			handle = 0
		}
		ctx = context.WithValue(ctx, fileActorKey{}, fileActor{session: f.session.id, reference: handle, node: f.id, cleanup: true})
		err = s.mutateTransactionLocked(ctx, ctx, intent, func(tx *sql.Tx) error {
			if prepared {
				if activates {
					if err := s.checkDrainCapacity(ctx, tx, int64(f.entryID)); err != nil {
						return err
					}
				}
				current, err := s.activatePreparedRemoval(ctx, tx, f.durableReference())
				if err != nil {
					return err
				}
				if current.EntryID != 0 {
					entry = current
				}
			}
			if !ioPending && count == 1 && entry.Draining {
				if err := s.detachDrainedEntry(ctx, tx, entry); err != nil {
					return err
				}
			} else if !ioPending && count == 1 && state.Detached {
				if err := s.discardNode(ctx, tx, state.Node); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return effects, err
		}
		if prepared {
			effects |= storage.EffectPreparedChanged
			f.intent = 0
		}
		if activates {
			effects |= storage.EffectDrainChanged
		}
		if finalDetach {
			effects |= storage.EffectEntryDetached
		}
	}
	if !f.claimReleased {
		if err := s.fileDomain.access.CloseClaim(uint64(f.reference)); err != nil {
			s.coordinator.poisonWith(err)
			return effects, fileAccessError(err)
		}
		f.claimReleased = true
	}
	if ioPending {
		return effects, errFileIOActive
	}
	if count == 1 {
		delete(s.coordinator.pins, key)
	} else {
		s.coordinator.pins[key] = count - 1
	}
	delete(s.files, f)
	delete(f.session.files, f.reference)
	s.fileDomain.files--
	f.closed = true
	return effects, nil
}

func (s *Store) detachDrainedEntry(ctx context.Context, tx *sql.Tx, entry entryRetirement) error {
	ready, err := s.entryReadyToDetach(ctx, tx, entry.EntryID, entry.Generation)
	if err != nil {
		return err
	}
	if ready.NodeID != entry.NodeID {
		return syscall.ESTALE
	}
	var parentID int64
	var name []byte
	if err := tx.QueryRowContext(ctx, `SELECT parent,name FROM entries WHERE volume=? AND id=? AND node=?`, s.volume, entry.EntryID, entry.NodeID).Scan(&parentID, &name); err != nil {
		return err
	}
	parent, err := s.nodeByID(ctx, tx, parentID)
	if err != nil {
		return err
	}
	node, err := s.nodeByID(ctx, tx, entry.NodeID)
	if err != nil {
		return err
	}
	if node.IsDir() {
		empty, err := s.isEmpty(ctx, tx, node.ID)
		if err != nil {
			return err
		}
		if !empty {
			return syscall.ENOTEMPTY
		}
	}
	if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: parentID, Name: name}, node); err != nil {
		return err
	}
	if err := s.unlink(ctx, tx, parentID, name); err != nil {
		return err
	}
	if err := s.discardNode(ctx, tx, node); err != nil {
		return err
	}
	return s.touch(ctx, tx, parentID, time.Now(), parent)
}
