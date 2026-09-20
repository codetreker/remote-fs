package sqlite

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ metastore.AtomicFileOpener = (*Store)(nil)

func (s *Store) CheckAtomicFileOpen() error { return s.CheckFileStore() }

func (s *Store) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (metastore.OpenResult, error) {
	if err := name.Check(); err != nil {
		return metastore.OpenResult{}, err
	}
	if err := options.Check(); err != nil {
		return metastore.OpenResult{}, err
	}
	file, state, outcome, err := s.openAtomicChild(ctx, name, storage.NodeRegular, options.Read, options.Write,
		storage.ReadMetadata|storage.WriteMetadata, options.Create, options.Exclusive, options.Target,
		options.Use, options.Existing, options.Initial, options.CloseIntent)
	if file == nil {
		return metastore.OpenResult{}, err
	}
	return metastore.OpenResult{File: file, State: state, Outcome: outcome}, err
}

func (s *Store) openAtomicChild(ctx context.Context, name storage.ChildName, kind storage.NodeKind, read, write bool,
	metadata storage.MetadataPermissions, create, exclusive bool, target storage.ChildCondition,
	use storage.UseClaim, existing storage.ExistingEffect,
	initial storage.InitialState, closeIntent *storage.CloseIntent,
) (*retainedFile, metastore.FileState, storage.OpenOutcome, error) {
	scope, err := newReferenceScope()
	if err != nil {
		return nil, metastore.FileState{}, 0, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return nil, metastore.FileState{}, 0, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return nil, metastore.FileState{}, 0, err
	}
	if s.fileDomain.files >= s.fileDomain.maxFiles {
		return nil, metastore.FileState{}, 0, syscall.EAGAIN
	}
	file := &retainedFile{store: s, scope: scope, session: metastore.ReferenceSession(ctx), use: use,
		read: read, write: write, metadata: metadata, active: true}
	var before metastore.Node
	var found bool
	inspect := func(tx *sql.Tx) error {
		if _, _, err := s.directoryTarget(ctx, tx, name.Parent, 0); err != nil {
			return err
		}
		id, exists, err := s.lookupNodeID(ctx, tx, int64(name.Parent.NodeID), name.RawLeaf)
		if err != nil {
			return err
		}
		found = exists
		if found && exclusive {
			return syscall.EEXIST
		}
		if found {
			if existing == storage.Keep {
				before, err = s.returnedNode(ctx, tx, id)
			} else {
				before, err = s.nodeByID(ctx, tx, id)
			}
			if err != nil {
				return err
			}
		}
		if err := checkChildCondition(target, before, found); err != nil {
			return err
		}
		if !found {
			if !create {
				return syscall.ENOENT
			}
			pending, err := s.nodePendingUnlink(ctx, tx, int64(name.Parent.NodeID))
			if err != nil {
				return err
			}
			if pending {
				return storage.ErrPendingDelete
			}
			return nil
		}
		if before.Kind != kind {
			return nodeKindMismatch(before.Kind, kind)
		}
		pending, err := s.nodePendingUnlink(ctx, tx, before.ID)
		if err != nil {
			return err
		}
		if pending {
			return storage.ErrPendingDelete
		}
		return nil
	}
	if err := s.inspect(ctx, inspect); err != nil {
		return nil, metastore.FileState{}, 0, sqlerr.Failure(err)
	}
	mutation := !found || existing != storage.Keep || closeIntent != nil
	intent := &volumeIntent{kind: locking.CreateMutation, nodes: []int64{}}
	if found {
		intent.nodes = nil
		intent.node = before.ID
		if existing == storage.ResetContent {
			intent.kind = locking.WriteMutation
			intent.scope = scope
		} else if existing == storage.ReplaceNode {
			intent.kind = locking.RemoveMutation
		} else {
			intent.kind = locking.SetAttrMutation
		}
	}
	if found && existing == storage.Keep {
		intent = nil
	}
	var state metastore.FileState
	var outcome storage.OpenOutcome
	claimed := false
	apply := func(tx *sql.Tx) error {
		if err := inspect(tx); err != nil {
			return err
		}
		at := time.Now()
		node, selected, err := s.applyAtomicOpen(ctx, tx, name, before, found, kind, existing, initial, at)
		if err != nil {
			return err
		}
		file.id, outcome = node.ID, selected
		if err := s.fileDomain.coordinator.AddUse(ctx, uint64(node.ID), scope, use); err != nil {
			return err
		}
		claimed = true
		if err := s.armCloseIntent(ctx, tx, file, closeIntent); err != nil {
			return err
		}
		state, err = s.returnedFileState(ctx, tx, node.ID)
		if err == nil {
			err = metastore.CheckFilePublication(ctx)
		}
		return err
	}
	if mutation {
		err = s.mutateTransactionLocked(ctx, ctx, intent, apply)
	} else {
		err = s.inspect(ctx, apply)
		if err == nil {
			err = metastore.CheckFilePublication(ctx)
		}
	}
	var deleteIntent storage.DeleteIntentID
	if closeIntent != nil {
		deleteIntent = closeIntent.ID
	}
	return s.finishReferenceOpen(ctx, file, claimed, deleteIntent, state, outcome, err)
}

func (s *Store) finishReferenceOpen(ctx context.Context, file *retainedFile, claimed bool, closeIntent storage.DeleteIntentID, state metastore.FileState, outcome storage.OpenOutcome, err error) (*retainedFile, metastore.FileState, storage.OpenOutcome, error) {
	if err != nil {
		if claimed && (s.coordinator.healthy() != nil || storage.IsPublicationAccountingUncertain(err)) {
			file.active, file.closeErr = false, sqlerr.Failure(err)
			file.closeIntent = closeIntent
			s.retainFileLocked(file)
			return file, state, outcome, file.closeErr
		}
		if claimed {
			err = errors.Join(err, s.fileDomain.coordinator.DropUse(context.WithoutCancel(ctx), uint64(file.id), file.scope))
		}
		return nil, metastore.FileState{}, 0, sqlerr.Failure(err)
	}
	file.closeIntent = closeIntent
	s.retainFileLocked(file)
	return file, state, outcome, nil
}

func (s *Store) retainFileLocked(file *retainedFile) {
	s.files[file] = struct{}{}
	s.fileDomain.files++
	s.coordinator.pins[retainedNode{s.volume, file.id}]++
}

func nodeKindMismatch(actual, requested storage.NodeKind) error {
	if actual == storage.NodeSymlink {
		return syscall.ELOOP
	}
	if actual == storage.NodeDirectory {
		return syscall.EISDIR
	}
	if requested == storage.NodeDirectory {
		return syscall.ENOTDIR
	}
	return syscall.EINVAL
}

func (s *Store) applyAtomicOpen(ctx context.Context, tx *sql.Tx, name storage.ChildName, before metastore.Node,
	found bool, kind storage.NodeKind, existing storage.ExistingEffect, initial storage.InitialState, at time.Time,
) (metastore.Node, storage.OpenOutcome, error) {
	if found && existing == storage.Keep {
		return before, storage.Opened, nil
	}
	if found && existing == storage.ResetContent {
		if err := s.checkByteExtent(ctx, uint64(before.ID), storage.UseScope{}, 0, before.Size, storage.WriteData); err != nil {
			return metastore.Node{}, 0, err
		}
		if err := s.replaceNodeContentFields(ctx, tx, before, metastore.Object{ModTime: at}, at); err != nil {
			return metastore.Node{}, 0, err
		}
		if err := s.applyOpenFields(ctx, tx, before, initial.OnReset, at); err != nil {
			return metastore.Node{}, 0, err
		}
		if err := s.recordNamedChanged(ctx, tx, before.ID); err != nil {
			return metastore.Node{}, 0, err
		}
		node, err := s.nodeByID(ctx, tx, before.ID)
		return node, storage.Reset, err
	}
	selected, outcome := initial.OnCreate, storage.Created
	if found {
		if err := s.fileDomain.coordinator.CheckUse(ctx, uint64(before.ID), storage.UseScope{}, storage.DeleteName); err != nil {
			return metastore.Node{}, 0, err
		}
		if err := s.unlink(ctx, tx, int64(name.Parent.NodeID), name.RawLeaf); err != nil {
			return metastore.Node{}, 0, err
		}
		if err := s.discard(ctx, tx, before, at); err != nil {
			return metastore.Node{}, 0, err
		}
		if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: int64(name.Parent.NodeID), Name: name.RawLeaf}); err != nil {
			return metastore.Node{}, 0, err
		}
		selected, outcome = initial.OnReplace, storage.Replaced
	}
	node, err := s.insertNode(ctx, tx, kind, selected, at)
	if err != nil {
		return metastore.Node{}, 0, err
	}
	if node.Size != 0 {
		if err := s.account(ctx, tx, node.Size); err != nil {
			return metastore.Node{}, 0, err
		}
	}
	if err := s.link(ctx, tx, int64(name.Parent.NodeID), name.RawLeaf, node.ID); err != nil {
		return metastore.Node{}, 0, err
	}
	if err := s.recordCreated(ctx, tx, metastore.Location{Parent: int64(name.Parent.NodeID), Name: name.RawLeaf}, node.ID); err != nil {
		return metastore.Node{}, 0, err
	}
	if err := s.touch(ctx, tx, int64(name.Parent.NodeID), at); err != nil {
		return metastore.Node{}, 0, err
	}
	return node, outcome, nil
}

func (s *Store) applyOpenFields(ctx context.Context, tx *sql.Tx, node metastore.Node, initial storage.InitialFields, at time.Time) error {
	if len(initial.Metadata) != 0 {
		metadata := storage.CloneMetadata(node.Metadata)
		if metadata == nil {
			metadata = make(map[string]storage.OpaquePayload, len(initial.Metadata))
		}
		for namespace, data := range initial.Metadata {
			version := uint64(1)
			if current, exists := metadata[namespace]; exists {
				if len(current.Version) != 8 || binary.BigEndian.Uint64(current.Version) == 0 {
					return syscall.EIO
				}
				version = binary.BigEndian.Uint64(current.Version)
				if version == math.MaxUint64 {
					return syscall.EOVERFLOW
				}
				version++
			}
			metadata[namespace] = storage.OpaquePayload{Version: binary.BigEndian.AppendUint64(nil, version), Data: data}
		}
		encoded, err := storage.EncodeMetadata(metadata)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`, encoded, s.volume, node.ID); err != nil {
			return err
		}
	}
	return applyChange(ctx, tx, node, initial.Attr, at)
}
