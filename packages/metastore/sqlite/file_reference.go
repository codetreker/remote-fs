package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strconv"
	"syscall"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *fileReference) NodeID() uint64 { return uint64(f.id) }

func (f *fileReference) check(uses storage.AccessUse) error {
	if !f.active || f.closed {
		return syscall.EBADF
	}
	if uses & ^f.claim.Uses != 0 {
		return syscall.EBADF
	}
	return nil
}

func (f *fileReference) durableReference() string {
	return f.session.store.fileDomain.authority + ":" + strconv.FormatUint(uint64(f.reference), 10)
}

func (f *fileReference) intentToken(id storage.RemovalIntentID) string {
	return f.session.store.fileDomain.authority + ":intent:" + strconv.FormatUint(uint64(id), 10)
}

func (f *fileReference) observation(ctx context.Context, tx *sql.Tx, options storage.ObservationOptions) (storage.FileObservation, error) {
	s := f.session.store
	node, err := s.nodeByID(ctx, tx, f.id)
	if err != nil {
		return storage.FileObservation{}, err
	}
	result := storage.FileObservation{Attr: node.Attr()}
	if options.IncludeLocation {
		location, err := s.captureLocation(ctx, tx, s.root, f.id)
		if err != nil {
			return storage.FileObservation{}, err
		}
		result.Location = &location
	}
	if options.IncludeLinkTarget {
		result.LinkTarget = bytes.Clone(node.LinkTarget)
	}
	entry := int64(f.entryID)
	if entry == 0 && f.id != s.root {
		err := tx.QueryRowContext(ctx, `SELECT id FROM entries WHERE volume=? AND node=?`, s.volume, f.id).Scan(&entry)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return storage.FileObservation{}, err
		}
	}
	result.Removal = storage.RemovalStatus{State: storage.EntryDetached}
	if f.id == s.root {
		result.Removal.State = storage.EntryActive
	}
	if entry != 0 {
		state, err := s.entryRetirementState(ctx, tx, entry)
		if err != nil && !errors.Is(err, syscall.ESTALE) {
			return storage.FileObservation{}, err
		}
		if err == nil {
			if state.NodeID != f.id {
				return storage.FileObservation{}, syscall.EIO
			}
			result.Removal = storage.RemovalStatus{EntryID: storage.EntryID(entry), State: storage.EntryActive, Generation: storage.DrainGeneration(state.Generation)}
			if state.Draining {
				result.Removal.State = storage.EntryDraining
				result.Removal.DrainCondition = storage.RemovalFile
				if state.IfEmpty {
					result.Removal.DrainCondition = storage.RemovalIfEmpty
				}
			}
		}
	}
	if f.intent != 0 {
		var empty bool
		err := tx.QueryRowContext(ctx, `SELECT if_empty FROM removal_intents WHERE volume=? AND reference=? AND token=?`, s.volume, f.durableReference(), f.intentToken(f.intent)).Scan(&empty)
		if err != nil {
			return storage.FileObservation{}, err
		}
		result.Removal.IntentID, result.Removal.Prepared = f.intent, true
		result.Removal.EntryID = f.entryID
		result.Removal.PreparedCondition = storage.RemovalFile
		if empty {
			result.Removal.PreparedCondition = storage.RemovalIfEmpty
		}
	}
	return result, nil
}

func (f *fileReference) Stat(ctx context.Context, options storage.ObservationOptions) (storage.FileObservation, error) {
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.FileObservation{}, err
	}
	defer done()
	if err := f.check(0); err != nil {
		return storage.FileObservation{}, err
	}
	var value storage.FileObservation
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error { var err error; value, err = f.observation(ctx, tx, options); return err })
	return value, err
}

func (f *fileReference) CheckObservation(ctx context.Context, condition storage.ObservationCondition) (storage.FileObservation, error) {
	if err := condition.Check(); err != nil {
		return storage.FileObservation{}, err
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.FileObservation{}, err
	}
	defer done()
	if err := f.check(0); err != nil {
		return storage.FileObservation{}, err
	}
	var value storage.FileObservation
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error {
		if condition.Location.NodeID != uint64(f.id) {
			return syscall.ESTALE
		}
		if err := f.session.store.validateLocation(ctx, tx, condition.Location); err != nil {
			return err
		}
		var err error
		value, err = f.observation(ctx, tx, storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
		if err != nil {
			return err
		}
		if value.Attr.MetadataRevision != condition.MetadataRevision || value.Attr.DirectoryRevision != condition.DirectoryRevision {
			return syscall.EAGAIN
		}
		return nil
	})
	return value, err
}

func (f *fileReference) ListAt(ctx context.Context, request storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	if err := request.Check(); err != nil {
		return storage.DirectoryPage{}, err
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.DirectoryPage{}, err
	}
	defer done()
	if err := f.check(0); err != nil {
		return storage.DirectoryPage{}, err
	}
	var page storage.DirectoryPage
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		page, err = f.session.store.listDirectoryPage(ctx, tx, f.id, request)
		return err
	})
	return page, err
}

func (f *fileReference) mutate(ctx context.Context, operation storage.Operation, request any, id storage.FileActionID, intent *volumeIntent, effect storage.FileEffects, mutation func(*sql.Tx) error) (storage.FileActionReceipt, error) {
	fingerprint, err := fileFingerprint(struct {
		Operation storage.Operation
		Request   any
	}{operation, request})
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer done()
	a, fresh, err := f.session.admit(id, fingerprint, f)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if !fresh {
		return f.session.result(a)
	}
	a.result.Operation = operation
	if err := f.check(0); err != nil {
		return f.session.finish(a, err)
	}
	ctx = f.nativeContext(ctx)
	previousIntent := f.intent
	var observed storage.FileObservation
	err = f.session.store.mutateTransactionLocked(ctx, ctx, intent, func(tx *sql.Tx) error {
		if err := mutation(tx); err != nil {
			return err
		}
		var err error
		observed, err = f.observation(ctx, tx, storage.ObservationOptions{})
		return err
	})
	if err == nil {
		a.result.Observation, a.result.Removal, a.result.Effects = observed, observed.Removal, effect
	} else if f.session.store.coordinator.healthy() == nil {
		f.intent = previousIntent
	}
	return f.session.finish(a, err)
}

func (f *fileReference) SetAttr(ctx context.Context, change storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := change.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	effect := storage.EffectMetadataChanged
	if change.Empty() {
		effect = 0
	}
	return f.mutate(ctx, storage.OpFileSetAttr, change, id, &volumeIntent{kind: locking.SetAttrMutation, node: f.id}, effect,
		func(tx *sql.Tx) error { return f.session.store.setNodeAttr(ctx, tx, f.id, change) })
}

func (f *fileReference) ReplaceClaim(ctx context.Context, claim storage.AccessClaim, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := claim.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	fingerprint, err := fileFingerprint(struct {
		Operation storage.Operation
		Claim     storage.AccessClaim
	}{storage.OpFileReplaceClaim, claim})
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer done()
	a, fresh, err := f.session.admit(id, fingerprint, f)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if !fresh {
		return f.session.result(a)
	}
	a.result.Operation = storage.OpFileReplaceClaim
	if err := f.check(0); err != nil {
		return f.session.finish(a, err)
	}
	err = f.session.store.fileDomain.access.ReplaceClaim(uint64(f.reference), fileaccess.Claim{Uses: uint64(claim.Uses), Excludes: uint64(claim.Excludes)}, f.lifetimeGuardLocked(ctx))
	if err == nil {
		f.claim = claim
		a.result.Effects = storage.EffectClaimChanged
	}
	return f.session.finish(a, fileClaimError(err))
}

func (f *fileReference) Sync(ctx context.Context) (metastore.FileState, error) {
	done, err := f.session.acquire(ctx)
	if err != nil {
		return metastore.FileState{}, err
	}
	defer done()
	if err := f.check(0); err != nil {
		return metastore.FileState{}, err
	}
	var state metastore.FileState
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = f.session.store.fileState(ctx, tx, f.id)
		return err
	})
	return state, err
}
