package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ metastore.ConditionalFileMutation = (*retainedFile)(nil)

func (f *retainedFile) CheckConditionalFileMutation() error { return f.store.CheckFileStore() }

func (f *retainedFile) checkMutationConditions(ctx context.Context, tx *sql.Tx, state metastore.FileState, command storage.FileMutation) error {
	if err := f.store.checkNamespaceGuards(ctx, tx, command.Guards); err != nil {
		return err
	}
	for _, use := range command.Uses {
		if use.NodeID != uint64(f.id) {
			return syscall.EINVAL
		}
		if _, err := f.store.resolveUseScope(ctx, use.Scope, use.NodeID, 0); err != nil {
			return err
		}
	}
	if command.ExpectedSize != nil && *command.ExpectedSize != state.Size {
		return storage.ErrConditionConflict
	}
	check := func(namespace string, expected []byte) error {
		actual, present := state.Metadata[namespace]
		if present != (len(expected) != 0) || present && !bytes.Equal(actual.Version, expected) {
			return storage.ErrConditionConflict
		}
		return nil
	}
	for namespace, expected := range command.ExpectedMetadata {
		if err := check(namespace, expected); err != nil {
			return err
		}
	}
	for namespace, update := range command.Metadata {
		if err := check(namespace, update.Version); err != nil {
			return err
		}
	}
	return nil
}

func mutationAccess(command storage.FileMutation) metastore.FileAccess {
	access := metastore.FileAccess{Uses: storage.WriteData, Offset: command.Offset, Length: int64(len(command.Data))}
	if command.Kind == storage.MutateTruncate {
		access.Truncate, access.Size = true, command.Size
	}
	if command.Kind == storage.MutateAppend {
		access.Append = true
	}
	return access
}

func (f *retainedFile) MutateFile(ctx context.Context, command storage.FileMutation) (metastore.FileState, error) {
	if err := command.Check(); err != nil {
		return metastore.FileState{}, err
	}
	metadata := command.Kind == storage.MutateAttributes
	if !metadata && (command.Kind != storage.MutateWriteAt && command.Kind != storage.MutateAppend || len(command.Data) != 0) {
		return metastore.FileState{}, syscall.EINVAL
	}
	if metadata && f.metadata&storage.WriteMetadata == 0 || !metadata && !f.write {
		return metastore.FileState{}, syscall.EBADF
	}
	var state metastore.FileState
	err := f.store.mutatePublication(ctx, &volumeIntent{kind: locking.SetAttrMutation, node: f.id}, func(tx *sql.Tx) error {
		if err := f.check(); err != nil {
			return err
		}
		var before metastore.FileState
		var err error
		if !metadata || command.Attr.Empty() && len(command.Metadata) == 0 {
			before, err = f.store.returnedFileState(ctx, tx, f.id)
		} else {
			before, err = f.store.fileState(ctx, tx, f.id)
		}
		if err != nil {
			return err
		}
		if err := f.checkMutationConditions(ctx, tx, before, command); err != nil {
			return err
		}
		if !metadata {
			if err := f.store.checkAccessIntent(ctx, before, f.scope, mutationAccess(command)); err != nil {
				return err
			}
			state = before
			return nil
		}
		if command.Attr.Empty() && len(command.Metadata) == 0 {
			state = before
			return nil
		}
		initial := storage.InitialFields{Attr: command.Attr, Metadata: make(map[string][]byte, len(command.Metadata))}
		for namespace, update := range command.Metadata {
			initial.Metadata[namespace] = update.Data
		}
		if err := f.store.applyOpenFields(ctx, tx, before.Node, initial, time.Now()); err != nil {
			return err
		}
		if err := f.store.recordNamedChanged(ctx, tx, f.id); err != nil {
			return err
		}
		state, err = f.store.returnedFileState(ctx, tx, f.id)
		return err
	})
	return state, sqlerr.Failure(err)
}

func (f *retainedFile) CommitMutation(ctx context.Context, command storage.FileMutation, expected uint64, object metastore.Object) (metastore.FileState, error) {
	if err := command.Check(); err != nil {
		return metastore.FileState{}, err
	}
	if command.Kind == storage.MutateAttributes || expected == 0 || object.Size < 0 {
		return metastore.FileState{}, syscall.EINVAL
	}
	if !f.write {
		return metastore.FileState{}, syscall.EBADF
	}
	ctx = metastore.WithFileAccess(ctx, mutationAccess(command))
	var state metastore.FileState
	err := f.store.mutatePublication(ctx, &volumeIntent{kind: locking.WriteMutation, node: f.id, scope: f.scope}, func(tx *sql.Tx) error {
		if err := f.check(); err != nil {
			return err
		}
		before, err := f.store.fileState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if err := f.checkMutationConditions(ctx, tx, before, command); err != nil {
			return err
		}
		if before.Revision != expected {
			return syscall.EAGAIN
		}
		size := command.Size
		switch command.Kind {
		case storage.MutateWriteAt:
			if int64(len(command.Data)) > math.MaxInt64-command.Offset {
				return syscall.EFBIG
			}
			size = max(before.Size, command.Offset+int64(len(command.Data)))
		case storage.MutateAppend:
			if int64(len(command.Data)) > math.MaxInt64-before.Size {
				return syscall.EFBIG
			}
			size = before.Size + int64(len(command.Data))
		}
		if object.Size != size {
			return syscall.EINVAL
		}
		if err := f.store.replaceNodeContent(ctx, tx, before.Node, object); err != nil {
			return err
		}
		state, err = f.store.returnedFileState(ctx, tx, f.id)
		return err
	})
	return state, sqlerr.Failure(err)
}
