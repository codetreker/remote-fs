package sqlite

import (
	"context"
	"database/sql"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type retainedNodeReference struct{ file *retainedFile }

var _ metastore.NodeReferences = (*Store)(nil)
var _ metastore.NodeReference = (*retainedNodeReference)(nil)

func (s *Store) CheckNodeReferences() error { return s.CheckFileStore() }

func (s *Store) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (metastore.NodeOpenResult, error) {
	if err := name.Check(); err != nil {
		return metastore.NodeOpenResult{}, err
	}
	if err := options.Check(); err != nil {
		return metastore.NodeOpenResult{}, err
	}
	file, state, outcome, err := s.openAtomicChild(ctx, name, options.Kind, false, false, options.MetadataAccess,
		options.Create, options.Exclusive, options.Target, options.Guards, options.ExpectedMetadata, options.Use, storage.Keep,
		options.InitialState, options.CloseIntent)
	if file == nil {
		return metastore.NodeOpenResult{}, err
	}
	return metastore.NodeOpenResult{Reference: &retainedNodeReference{file: file}, State: state, Outcome: outcome}, err
}

func (s *Store) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (metastore.NodeOpenResult, error) {
	if err := options.Check(); err != nil {
		return metastore.NodeOpenResult{}, err
	}
	if id == 0 || options.Create || options.Exclusive || options.Target.State == storage.Absent || options.Target.State == storage.SameNode && options.Target.NodeID != id {
		return metastore.NodeOpenResult{}, syscall.EINVAL
	}
	if id > math.MaxInt64 {
		return metastore.NodeOpenResult{}, syscall.ESTALE
	}
	scope, err := newReferenceScope()
	if err != nil {
		return metastore.NodeOpenResult{}, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return metastore.NodeOpenResult{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return metastore.NodeOpenResult{}, err
	}
	if int64(s.fileDomain.files)+s.fileDomain.recoveredReferences >= int64(s.fileDomain.maxFiles) {
		return metastore.NodeOpenResult{}, syscall.EAGAIN
	}
	file := &retainedFile{store: s, id: int64(id), scope: scope, session: metastore.ReferenceSession(ctx),
		use: options.Use, metadata: options.MetadataAccess, active: true}
	var state metastore.FileState
	claimed := false
	open := func(tx *sql.Tx) error {
		if err := s.checkNamespaceGuards(ctx, tx, options.Guards); err != nil {
			return err
		}
		if options.CloseIntent != nil {
			if err := s.checkNamespaceGuards(ctx, tx, options.CloseIntent.Guards); err != nil {
				return err
			}
		}
		var err error
		state, err = s.returnedFileState(ctx, tx, int64(id))
		if err != nil {
			return err
		}
		if state.Kind != options.Kind {
			return nodeKindMismatch(state.Kind, options.Kind)
		}
		pending, err := s.nodePendingUnlink(ctx, tx, int64(id))
		if err != nil {
			return err
		}
		if pending {
			return storage.ErrPendingDelete
		}
		if err := checkExpectedMetadata(state.Metadata, options.ExpectedMetadata); err != nil {
			return err
		}
		if err := s.fileDomain.coordinator.AddUse(ctx, id, scope, options.Use); err != nil {
			return err
		}
		claimed = true
		if err := s.armCloseIntent(ctx, tx, file, options.CloseIntent); err != nil {
			return err
		}
		return metastore.CheckFilePublication(ctx)
	}
	if options.CloseIntent != nil {
		err = s.mutateTransactionLocked(ctx, ctx, nil, open)
	} else {
		err = s.inspect(ctx, open)
	}
	file, state, outcome, err := s.finishReferenceOpen(ctx, file, claimed, options.CloseIntent != nil, state, storage.Opened, err)
	if file == nil {
		return metastore.NodeOpenResult{}, err
	}
	return metastore.NodeOpenResult{Reference: &retainedNodeReference{file: file}, State: state, Outcome: outcome}, err
}

func (r *retainedNodeReference) Node(ctx context.Context) (metastore.FileState, error) {
	if r.file.metadata&storage.ReadMetadata == 0 {
		return metastore.FileState{}, syscall.EBADF
	}
	return r.file.Node(ctx)
}

func (r *retainedNodeReference) SetAttr(ctx context.Context, change storage.AttrChange) (metastore.FileState, error) {
	if r.file.metadata&storage.WriteMetadata == 0 {
		return metastore.FileState{}, syscall.EBADF
	}
	return r.file.SetAttr(ctx, change)
}

func (r *retainedNodeReference) Retire(ctx context.Context) error { return r.file.Retire(ctx) }
func (r *retainedNodeReference) Close(ctx context.Context) error  { return r.file.Close(ctx) }
func (r *retainedNodeReference) Order(ctx context.Context, transition func() error) error {
	return r.file.Order(ctx, transition)
}
func (r *retainedNodeReference) CheckScopedReference() error { return r.file.CheckScopedReference() }
func (r *retainedNodeReference) Scope(ctx context.Context) (storage.UseScope, error) {
	return r.file.Scope(ctx)
}
func (r *retainedNodeReference) CheckReferenceState() error { return r.file.CheckReferenceState() }
func (r *retainedNodeReference) State(ctx context.Context) (metastore.ReferenceState, error) {
	return r.file.State(ctx)
}
func (r *retainedNodeReference) CheckDeleteIntent() error { return r.file.CheckDeleteIntent() }
func (r *retainedNodeReference) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (metastore.ReferenceState, error) {
	return r.file.SetPendingUnlink(ctx, command)
}
func (r *retainedNodeReference) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (metastore.ReferenceState, error) {
	return r.file.ClearPendingUnlink(ctx, command)
}

func (r *retainedNodeReference) CheckConditionalFileMutation() error {
	return r.file.CheckConditionalFileMutation()
}
func (r *retainedNodeReference) MutateFile(ctx context.Context, command storage.FileMutation) (metastore.FileState, error) {
	return r.file.MutateFile(ctx, command)
}
func (r *retainedNodeReference) CommitMutation(ctx context.Context, command storage.FileMutation, revision uint64, object metastore.Object) (metastore.FileState, error) {
	return r.file.CommitMutation(ctx, command, revision, object)
}
