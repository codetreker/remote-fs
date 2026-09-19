package sqlite

import (
	"context"
	"database/sql"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ metastore.NamespaceAccess = (*Store)(nil)

type namespaceMutation struct {
	parent, destination metastore.FileState
	source, displaced   metastore.Node
	outputIsSource      bool
}

func (s *Store) checkNameMutationUses(ctx context.Context, uses []storage.TargetUse, nodes ...int64) error {
	for _, provided := range uses {
		used := false
		for _, node := range nodes {
			if provided.NodeID == uint64(node) {
				used = true
				break
			}
		}
		if !used {
			return syscall.EINVAL
		}
	}
	for i, node := range nodes {
		if node == 0 || i > 0 && node == nodes[0] {
			continue
		}
		scope, err := s.targetScope(ctx, uint64(node), storage.DeleteName, uses)
		if err != nil {
			return err
		}
		if err := s.fileDomain.coordinator.CheckUse(ctx, uint64(node), scope, storage.DeleteName); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) checkNameAddition(ctx context.Context, tx *sql.Tx, parent int64) error {
	pending, err := s.nodePendingUnlink(ctx, tx, parent)
	if err != nil {
		return err
	}
	if pending {
		return storage.ErrPendingDelete
	}
	return nil
}

func (s *Store) prepareNamespaceMutation(ctx context.Context, tx *sql.Tx, command storage.NameCommand) (namespaceMutation, error) {
	var mutation namespaceMutation
	if err := s.checkNamespaceGuards(ctx, tx, command.Guards); err != nil {
		return mutation, err
	}
	var err error
	mutation.parent, _, err = s.directoryTarget(ctx, tx, command.Name.Parent, 0)
	if err != nil {
		return mutation, err
	}
	var found bool
	if command.Kind == storage.NameRename && storage.HasAttrResultBudget(ctx) {
		var id int64
		id, found, err = s.lookupNodeID(ctx, tx, mutation.parent.ID, command.Name.RawLeaf)
		if err == nil && found {
			mutation.source, err = s.returnedNode(ctx, tx, id)
		}
	} else {
		mutation.source, found, err = s.lookup(ctx, tx, mutation.parent.ID, command.Name.RawLeaf)
	}
	if err != nil {
		return mutation, err
	}
	if err := checkChildCondition(command.Target, mutation.source, found); err != nil {
		return mutation, err
	}
	switch command.Kind {
	case storage.NameCreate, storage.NameMkdir, storage.NameSymlink:
		if found {
			return mutation, syscall.EEXIST
		}
		if err := s.checkNameMutationUses(ctx, command.Uses); err != nil {
			return mutation, err
		}
		return mutation, s.checkNameAddition(ctx, tx, mutation.parent.ID)
	case storage.NameRemove, storage.NameRemoveDir:
		if !found {
			return mutation, syscall.ENOENT
		}
		if mutation.source.ID == s.root {
			return mutation, syscall.EBUSY
		}
		if err := s.checkNameMutationUses(ctx, command.Uses, mutation.source.ID); err != nil {
			return mutation, err
		}
		if command.Kind == storage.NameRemove && mutation.source.IsDir() {
			return mutation, syscall.EISDIR
		}
		if command.Kind == storage.NameRemoveDir {
			if !mutation.source.IsDir() {
				return mutation, syscall.ENOTDIR
			}
			empty, err := s.isEmpty(ctx, tx, mutation.source.ID)
			if err != nil {
				return mutation, err
			}
			if !empty {
				return mutation, syscall.ENOTEMPTY
			}
		}
		return mutation, nil
	case storage.NameRename:
		if !found {
			return mutation, syscall.ENOENT
		}
		return s.prepareNamespaceRename(ctx, tx, command, mutation)
	default:
		return mutation, syscall.EINVAL
	}
}

func (s *Store) prepareNamespaceRename(ctx context.Context, tx *sql.Tx, command storage.NameCommand, mutation namespaceMutation) (namespaceMutation, error) {
	destination := command.Destination
	var err error
	mutation.destination, _, err = s.directoryTarget(ctx, tx, destination.Parent, 0)
	if err != nil {
		return mutation, err
	}
	var occupied bool
	mutation.displaced, occupied, err = s.lookup(ctx, tx, mutation.destination.ID, destination.ObservedLeaf)
	if err != nil {
		return mutation, err
	}
	if err := checkChildCondition(destination.Expected, mutation.displaced, occupied); err != nil {
		return mutation, err
	}
	output, outputExists, err := s.lookup(ctx, tx, mutation.destination.ID, destination.OutputLeaf)
	if err != nil {
		return mutation, err
	}
	if outputExists && output.ID != mutation.source.ID && (!occupied || output.ID != mutation.displaced.ID) {
		return mutation, storage.ErrConditionConflict
	}
	mutation.outputIsSource = outputExists && output.ID == mutation.source.ID
	if mutation.source.ID == s.root || occupied && mutation.displaced.ID == s.root {
		return mutation, syscall.EBUSY
	}
	if err := s.checkNameMutationUses(ctx, command.Uses, mutation.source.ID, mutation.displaced.ID); err != nil {
		return mutation, err
	}
	if mutation.outputIsSource && (!occupied || mutation.displaced.ID == mutation.source.ID) {
		return mutation, nil
	}
	if err := s.checkNameAddition(ctx, tx, mutation.destination.ID); err != nil {
		return mutation, err
	}
	if mutation.source.IsDir() {
		inside, err := s.directoryContains(ctx, tx, mutation.source.ID, mutation.destination.ID)
		if err != nil {
			return mutation, err
		}
		if inside {
			return mutation, syscall.EINVAL
		}
	}
	if occupied && mutation.displaced.ID != mutation.source.ID {
		switch {
		case mutation.displaced.IsDir() && !mutation.source.IsDir():
			return mutation, syscall.EISDIR
		case !mutation.displaced.IsDir() && mutation.source.IsDir():
			return mutation, syscall.ENOTDIR
		case mutation.displaced.IsDir():
			empty, err := s.isEmpty(ctx, tx, mutation.displaced.ID)
			if err != nil {
				return mutation, err
			}
			if !empty {
				return mutation, syscall.ENOTEMPTY
			}
		}
	}
	return mutation, nil
}

func (s *Store) directoryContains(ctx context.Context, tx *sql.Tx, ancestor, directory int64) (bool, error) {
	var contains bool
	err := tx.QueryRowContext(ctx, `WITH RECURSIVE ancestors(id) AS (
		VALUES(?) UNION SELECT e.parent FROM entries e JOIN ancestors a ON e.node=a.id WHERE e.volume=?
	) SELECT EXISTS(SELECT 1 FROM ancestors WHERE id=?)`, directory, s.volume, ancestor).Scan(&contains)
	return contains, err
}

func (s *Store) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	if err := command.Check(); err != nil {
		return storage.NameResult{}, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return storage.NameResult{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return storage.NameResult{}, err
	}
	if err := metastore.CheckFilePublication(ctx); err != nil {
		return storage.NameResult{}, err
	}
	var prepared namespaceMutation
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		prepared, err = s.prepareNamespaceMutation(ctx, tx, command)
		return err
	})
	if err != nil {
		return storage.NameResult{}, sqlerr.Failure(err)
	}
	intent := &volumeIntent{kind: locking.CreateMutation, nodes: []int64{}}
	switch command.Kind {
	case storage.NameRemove, storage.NameRemoveDir:
		intent.kind = locking.RemoveMutation
		intent.nodes = []int64{prepared.source.ID}
	case storage.NameRename:
		intent.kind = locking.RenameMutation
		displaced := prepared.displaced.ID
		if displaced == prepared.source.ID {
			displaced = 0
		}
		intent.nodes = []int64{prepared.source.ID, displaced}
	case storage.NameSymlink:
		intent.added = int64(len(command.Initial.LinkTarget))
	}
	var result storage.NameResult
	err = s.mutateTransactionLocked(ctx, ctx, intent, func(tx *sql.Tx) error {
		mutation, err := s.prepareNamespaceMutation(ctx, tx, command)
		if err != nil {
			return err
		}
		result, err = s.applyNamespaceMutation(ctx, tx, command, mutation, time.Now())
		return err
	})
	if err != nil {
		return storage.NameResult{}, sqlerr.Failure(err)
	}
	return result, nil
}

func (s *Store) applyNamespaceMutation(ctx context.Context, tx *sql.Tx, command storage.NameCommand, mutation namespaceMutation, at time.Time) (storage.NameResult, error) {
	switch command.Kind {
	case storage.NameCreate, storage.NameMkdir, storage.NameSymlink:
		kind := storage.NodeRegular
		if command.Kind == storage.NameMkdir {
			kind = storage.NodeDirectory
		} else if command.Kind == storage.NameSymlink {
			kind = storage.NodeSymlink
		}
		node, err := s.insertNode(ctx, tx, kind, command.Initial, at)
		if err != nil {
			return storage.NameResult{}, err
		}
		if err := s.account(ctx, tx, node.Size); err != nil {
			return storage.NameResult{}, err
		}
		if err := s.link(ctx, tx, mutation.parent.ID, command.Name.RawLeaf, node.ID); err != nil {
			return storage.NameResult{}, err
		}
		if err := s.recordCreated(ctx, tx, metastore.Location{Parent: mutation.parent.ID, Name: command.Name.RawLeaf}, node.ID); err != nil {
			return storage.NameResult{}, err
		}
		if err := s.touch(ctx, tx, mutation.parent.ID, at); err != nil {
			return storage.NameResult{}, err
		}
		attr := node.Attr()
		return storage.NameResult{Attr: &attr}, nil
	case storage.NameRemove, storage.NameRemoveDir:
		if err := s.unlink(ctx, tx, mutation.parent.ID, command.Name.RawLeaf); err != nil {
			return storage.NameResult{}, err
		}
		if err := s.discard(ctx, tx, mutation.source, at); err != nil {
			return storage.NameResult{}, err
		}
		if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: mutation.parent.ID, Name: command.Name.RawLeaf}); err != nil {
			return storage.NameResult{}, err
		}
		return storage.NameResult{}, s.touch(ctx, tx, mutation.parent.ID, at)
	case storage.NameRename:
		return s.applyNamespaceRename(ctx, tx, command, mutation, at)
	default:
		return storage.NameResult{}, syscall.EINVAL
	}
}

func (s *Store) applyNamespaceRename(ctx context.Context, tx *sql.Tx, command storage.NameCommand, mutation namespaceMutation, at time.Time) (storage.NameResult, error) {
	if mutation.outputIsSource && (mutation.displaced.ID == 0 || mutation.displaced.ID == mutation.source.ID) {
		node, err := s.returnedNode(ctx, tx, mutation.source.ID)
		if err != nil {
			return storage.NameResult{}, err
		}
		attr := node.Attr()
		return storage.NameResult{Attr: &attr}, nil
	}
	destination := command.Destination
	if mutation.displaced.ID != 0 && mutation.displaced.ID != mutation.source.ID {
		if err := s.unlink(ctx, tx, mutation.destination.ID, destination.ObservedLeaf); err != nil {
			return storage.NameResult{}, err
		}
		if err := s.discard(ctx, tx, mutation.displaced, at); err != nil {
			return storage.NameResult{}, err
		}
		if err := s.recordRemoved(ctx, tx, metastore.Location{Parent: mutation.destination.ID, Name: destination.ObservedLeaf}); err != nil {
			return storage.NameResult{}, err
		}
	}
	if !mutation.outputIsSource {
		if err := s.unlink(ctx, tx, mutation.parent.ID, command.Name.RawLeaf); err != nil {
			return storage.NameResult{}, err
		}
		if err := s.link(ctx, tx, mutation.destination.ID, destination.OutputLeaf, mutation.source.ID); err != nil {
			return storage.NameResult{}, err
		}
	}
	if err := s.setNodeChangeTime(ctx, tx, mutation.source.ID, at); err != nil {
		return storage.NameResult{}, err
	}
	moved, err := s.returnedNode(ctx, tx, mutation.source.ID)
	if err != nil {
		return storage.NameResult{}, err
	}
	if mutation.outputIsSource {
		if err := s.recordChanged(ctx, tx, moved.ID); err != nil {
			return storage.NameResult{}, err
		}
	} else {
		if err := s.recordRenamed(ctx, tx,
			metastore.Location{Parent: mutation.destination.ID, Name: destination.OutputLeaf},
			metastore.Location{Parent: mutation.parent.ID, Name: command.Name.RawLeaf}, moved); err != nil {
			return storage.NameResult{}, err
		}
	}
	if err := s.touch(ctx, tx, mutation.parent.ID, at); err != nil {
		return storage.NameResult{}, err
	}
	if mutation.destination.ID != mutation.parent.ID {
		if err := s.touch(ctx, tx, mutation.destination.ID, at); err != nil {
			return storage.NameResult{}, err
		}
	}
	attr := moved.Attr()
	return storage.NameResult{Attr: &attr}, nil
}
