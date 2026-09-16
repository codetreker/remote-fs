package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

type retainPlan struct {
	node     uint64
	expected storage.NodeMetadataRevision
	target   *storage.EntryTarget
	witness  *storage.EntryLocation
	initial  *storage.NodeInitial
	reset    *storage.ResetAndRetainRequest
	replace  bool
	claim    storage.AccessClaim
	prepared *storage.RemovalCondition
}

func (s *fileSession) Retain(ctx context.Context, request storage.RetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := request.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	return s.retain(ctx, storage.OpFileRetain, request, id, retainPlan{node: request.NodeID, expected: request.ExpectedMetadataRevision, claim: request.Claim, witness: request.Witness, prepared: request.Prepared})
}

func (s *fileSession) RetainAt(ctx context.Context, request storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := request.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	return s.retain(ctx, storage.OpFileRetainAt, request, id, retainPlan{target: &request.Target, claim: request.Claim, prepared: request.Prepared})
}

func (s *fileSession) CreateAndRetainAt(ctx context.Context, request storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := request.CheckCreate(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	return s.retain(ctx, storage.OpFileCreateAndRetainAt, request, id, retainPlan{target: &request.Target, initial: &request.Initial, claim: request.Claim, prepared: request.Prepared})
}

func (s *fileSession) ReplaceAndRetainAt(ctx context.Context, request storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := request.CheckReplace(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	return s.retain(ctx, storage.OpFileReplaceAndRetainAt, request, id, retainPlan{target: &request.Target, initial: &request.Initial, replace: true, claim: request.Claim, prepared: request.Prepared})
}

func (s *fileSession) ResetAndRetainAt(ctx context.Context, request storage.ResetAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := request.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	return s.retain(ctx, storage.OpFileResetAndRetainAt, request, id, retainPlan{target: &request.Target, reset: &request, claim: request.Claim, prepared: request.Prepared})
}

func (s *fileSession) resolveTarget(ctx context.Context, tx *sql.Tx, target storage.EntryTarget) (metastore.Node, metastore.Node, storage.EntryID, error) {
	parent := s.files[target.Parent]
	if parent == nil || parent.id != int64(target.ParentID) || parent.check(0) != nil {
		return metastore.Node{}, metastore.Node{}, 0, syscall.ESTALE
	}
	if target.Witness != nil {
		if target.Witness.NodeID != target.ParentID || target.Witness.State == storage.LocationDetached {
			return metastore.Node{}, metastore.Node{}, 0, syscall.ESTALE
		}
		if err := s.store.validateLocation(ctx, tx, *target.Witness); err != nil {
			return metastore.Node{}, metastore.Node{}, 0, err
		}
	}
	directory, err := s.store.nodeByID(ctx, tx, parent.id)
	if err != nil {
		return metastore.Node{}, metastore.Node{}, 0, err
	}
	if !directory.IsDir() {
		return directory, metastore.Node{}, 0, syscall.ENOTDIR
	}
	if directory.DirectoryRevision != target.DirectoryRevision {
		return directory, metastore.Node{}, 0, syscall.EAGAIN
	}
	var entry, nodeID int64
	err = tx.QueryRowContext(ctx, `SELECT id,node FROM entries WHERE volume=? AND parent=? AND name=?`, s.store.volume, parent.id, target.Name).Scan(&entry, &nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		if target.ExpectedEntryID != 0 || target.ExpectedNodeID != 0 {
			return directory, metastore.Node{}, 0, syscall.ESTALE
		}
		return directory, metastore.Node{}, 0, nil
	}
	if err != nil {
		return directory, metastore.Node{}, 0, err
	}
	if uint64(entry) != uint64(target.ExpectedEntryID) || uint64(nodeID) != target.ExpectedNodeID {
		return directory, metastore.Node{}, 0, syscall.ESTALE
	}
	node, err := s.store.nodeByID(ctx, tx, nodeID)
	if err == nil && target.ExpectedMetadataRevision != 0 && node.MetadataRevision != target.ExpectedMetadataRevision {
		err = syscall.EAGAIN
	}
	return directory, node, storage.EntryID(entry), err
}

func (s *fileSession) retain(ctx context.Context, operation storage.Operation, request any, id storage.FileActionID, plan retainPlan) (storage.FileActionReceipt, error) {
	fingerprint, err := fileFingerprint(struct {
		Operation storage.Operation
		Request   any
	}{operation, request})
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
	a.result.Operation = operation
	ctx = s.nativeContext(ctx)
	d := s.store.fileDomain
	if len(s.files) >= s.options.MaxFiles || d.files >= d.maxFiles {
		return s.finish(a, syscall.EMFILE)
	}
	var parent, before metastore.Node
	var entry storage.EntryID
	err = s.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		if plan.target != nil {
			parent, before, entry, err = s.resolveTarget(ctx, tx, *plan.target)
		} else {
			if plan.node == 0 || plan.node > math.MaxInt64 {
				return syscall.ESTALE
			}
			before, err = s.store.nodeByID(ctx, tx, int64(plan.node))
			if err == nil && plan.witness != nil {
				err = s.store.validateLocation(ctx, tx, *plan.witness)
			}
			if err == nil && before.ID != s.store.root {
				var raw int64
				e := tx.QueryRowContext(ctx, `SELECT id FROM entries WHERE volume=? AND node=?`, s.store.volume, before.ID).Scan(&raw)
				if e != nil && !errors.Is(e, sql.ErrNoRows) {
					return e
				}
				entry = storage.EntryID(raw)
			}
		}
		if err != nil {
			return err
		}
		if plan.expected != 0 && before.MetadataRevision != plan.expected {
			return syscall.EAGAIN
		}
		if before.ID != 0 {
			return s.store.checkNodeAdmission(ctx, tx, before.ID)
		}
		return nil
	})
	if err != nil {
		return s.finish(a, err)
	}
	if plan.initial == nil && before.ID == 0 {
		return s.finish(a, syscall.ENOENT)
	}
	if plan.initial != nil && !plan.replace && before.ID != 0 {
		return s.finish(a, syscall.EEXIST)
	}
	if plan.reset != nil && (before.Kind != storage.NodeRegular || before.MetadataRevision != plan.reset.ExpectedRevision) {
		if before.Kind != storage.NodeRegular {
			return s.finish(a, syscall.EINVAL)
		}
		return s.finish(a, syscall.EAGAIN)
	}
	ref, err := d.allocateReference()
	if err != nil {
		return s.finish(a, err)
	}
	f := &fileReference{session: s, id: before.ID, reference: storage.FileReferenceID(ref), claim: plan.claim, active: true, entryID: entry}
	registered := false
	register := func() error {
		err := d.access.RegisterClaim(ref, s.id, uint64(f.id), fileaccess.Claim{Uses: uint64(plan.claim.Uses), Excludes: uint64(plan.claim.Excludes)}, f.lifetimeGuardLocked(ctx))
		if err == nil {
			registered = true
		}
		return fileClaimError(err)
	}
	if plan.initial == nil {
		if err := register(); err != nil {
			return s.finish(a, err)
		}
		ctx = f.nativeContext(ctx)
	}
	var observed storage.FileObservation
	effects := storage.EffectRetained
	var intent *volumeIntent
	if plan.initial != nil {
		intent = &volumeIntent{kind: locking.CreateMutation, nodes: []int64{parent.ID}, totalUsage: true}
	}
	if plan.replace {
		intent = &volumeIntent{kind: locking.RemoveMutation, nodes: []int64{before.ID}, totalUsage: true}
	}
	if plan.reset != nil {
		intent = &volumeIntent{kind: locking.WriteMutation, node: before.ID}
		ctx = metastore.WithFileIO(ctx, storage.FileIO{Write: true, Truncate: true})
	}
	if plan.prepared != nil && intent == nil && before.ID != 0 {
		intent = &volumeIntent{kind: locking.RemoveMutation, node: before.ID}
	}
	change := func(tx *sql.Tx) error {
		if plan.initial != nil {
			if plan.replace {
				if before.IsDir() {
					empty, err := s.store.isEmpty(ctx, tx, before.ID)
					if err != nil {
						return err
					}
					if !empty {
						return syscall.ENOTEMPTY
					}
				}
				at := metastore.Location{Parent: parent.ID, Name: plan.target.Name}
				if err := s.store.recordRemoved(ctx, tx, at, before); err != nil {
					return err
				}
				if err := s.store.unlink(ctx, tx, parent.ID, plan.target.Name); err != nil {
					return err
				}
				if err := s.store.discard(ctx, tx, before); err != nil {
					return err
				}
			}
			node, err := s.store.insertInitialNode(ctx, tx, *plan.initial)
			if err != nil {
				return err
			}
			f.id = node.ID
			if err := s.store.link(ctx, tx, parent.ID, plan.target.Name, node.ID); err != nil {
				return err
			}
			var raw int64
			if err := tx.QueryRowContext(ctx, `SELECT id FROM entries WHERE volume=? AND node=?`, s.store.volume, f.id).Scan(&raw); err != nil {
				return err
			}
			f.entryID = storage.EntryID(raw)
			if err := register(); err != nil {
				return err
			}
			if err := s.store.touch(ctx, tx, parent.ID, time.Now(), parent); err != nil {
				return err
			}
			if err := s.store.recordCreated(ctx, tx, metastore.Location{Parent: parent.ID, Name: plan.target.Name}, node.ID); err != nil {
				return err
			}
			effects |= storage.EffectCreated
			if plan.replace {
				effects |= storage.EffectEntryDetached
			}
		}
		if plan.reset != nil {
			if err := s.store.replaceNodeContent(ctx, tx, before, metastore.Object{ModTime: time.Now()}, &plan.reset.Change); err != nil {
				return err
			}
			effects |= storage.EffectContentChanged
			if !plan.reset.Change.Empty() {
				effects |= storage.EffectMetadataChanged
			}
		}
		if plan.prepared != nil {
			if err := f.installPrepared(ctx, tx, *plan.prepared); err != nil {
				return err
			}
			effects |= storage.EffectPreparedChanged
		}
		var err error
		observed, err = f.observation(ctx, tx, storage.ObservationOptions{IncludeLocation: plan.target != nil && plan.target.Witness != nil || plan.witness != nil, IncludeLinkTarget: true})
		return err
	}
	if plan.initial != nil || plan.reset != nil || plan.prepared != nil {
		err = s.store.mutateTransactionLocked(ctx, ctx, intent, change)
	} else {
		err = s.store.inspect(ctx, change)
	}
	if err == nil && plan.initial == nil && plan.reset == nil && plan.prepared == nil {
		err = f.lifetimeGuardLocked(ctx)()
	}
	if err != nil && s.store.coordinator.healthy() == nil {
		if registered {
			if closeErr := d.access.CloseClaim(ref); closeErr != nil {
				s.store.coordinator.poisonWith(closeErr)
				err = errors.Join(err, closeErr)
			}
		}
		return s.finish(a, err)
	}
	if registered {
		s.files[f.reference] = f
		s.store.files[f] = struct{}{}
		d.files++
		s.store.coordinator.pins[retainedNode{s.store.volume, f.id}]++
		a.file = f
		a.result.Reference = f.reference
	}
	if err == nil {
		a.result.Observation, a.result.Removal, a.result.Effects = observed, observed.Removal, effects
	}
	return s.finish(a, err)
}

func (s *Store) insertInitialNode(ctx context.Context, tx *sql.Tx, initial storage.NodeInitial) (metastore.Node, error) {
	if err := initial.Check(); err != nil {
		return metastore.Node{}, err
	}
	id, err := dbstate.AllocateNodeID(ctx, tx)
	if err != nil {
		return metastore.Node{}, err
	}
	metadata, err := storage.EncodeMetadata(initial.Metadata)
	if err != nil {
		return metastore.Node{}, err
	}
	now := time.Now()
	selectTime := func(requested *time.Time) time.Time {
		if requested != nil {
			return *requested
		}
		return now
	}
	atimeSec, atimeNsec := sqlvalue.StoredTime(selectTime(initial.AccessTime))
	mtimeSec, mtimeNsec := sqlvalue.StoredTime(selectTime(initial.ModTime))
	creationSec, creationNsec := sqlvalue.StoredTime(selectTime(initial.CreationTime))
	changeSec, changeNsec := sqlvalue.StoredTime(selectTime(initial.ChangeTime))
	size := int64(len(initial.LinkTarget))
	if err := s.account(ctx, tx, size); err != nil {
		return metastore.Node{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,creation_sec,creation_nsec,change_sec,change_nsec,metadata_revision,directory_revision,metadata,link_target,content) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,NULL)`,
		id, s.volume, initial.Kind, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec, creationSec, creationNsec, changeSec, changeNsec,
		directoryInitialRevision(initial.Kind), metadata, append([]byte{}, initial.LinkTarget...))
	if err != nil {
		return metastore.Node{}, err
	}
	return s.nodeByID(ctx, tx, id)
}

func (f *fileReference) installPrepared(ctx context.Context, tx *sql.Tx, condition storage.RemovalCondition) error {
	if f.entryID == 0 {
		return syscall.ESTALE
	}
	if err := f.check(storage.RemoveEntry); err != nil {
		return err
	}
	node, err := f.session.store.nodeByID(ctx, tx, f.id)
	if err != nil {
		return err
	}
	if node.IsDir() && condition != storage.RemovalIfEmpty {
		return syscall.EISDIR
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM removal_intents WHERE volume=?`, f.session.store.volume).Scan(&count); err != nil {
		return err
	}
	if count >= f.session.store.fileDomain.config.MaxPrepared {
		return syscall.EAGAIN
	}
	id, err := f.session.store.fileDomain.allocateIntent()
	if err != nil {
		return err
	}
	intent := storage.RemovalIntentID(id)
	if err := f.session.store.prepareEntryRemoval(ctx, tx, f.durableReference(), f.intentToken(intent), int64(f.entryID), condition == storage.RemovalIfEmpty, f.session.store.fileDomain.authority); err != nil {
		return err
	}
	f.intent = intent
	return nil
}
