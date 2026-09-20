package objectstore

import (
	"context"
	"errors"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *openFile) CheckConditionalFileMutation() error {
	native, ok := any(f.native).(metastore.ConditionalFileMutation)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckConditionalFileMutation()
}

func (f *openFile) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	if err := f.CheckConditionalFileMutation(); err != nil {
		return storage.Attr{}, err
	}
	if err := command.CheckDataLimit(f.session.options.MaxFileSize); err != nil {
		return storage.Attr{}, err
	}
	return runFileAction(ctx, f.session, command.Action, storage.OpFileMutate, command,
		func(attr storage.Attr) storage.Attr { return attr.Clone() },
		func(attr storage.Attr) bool { return attr.ID != 0 },
		func() (storage.Attr, error) { return f.mutateFile(ctx, command) })
}

func (f *openFile) mutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	switch command.Kind {
	case storage.MutateTruncate:
		ctx = metastore.WithFileAccess(ctx, metastore.FileAccess{Uses: storage.WriteData, Truncate: true, Size: command.Size})
		return f.mutate(ctx, func(int64) int64 { return command.Size }, func([]byte) {}, &command)
	case storage.MutateWriteAt:
		ctx = metastore.WithFileAccess(ctx, metastore.FileAccess{Uses: storage.WriteData, Offset: command.Offset, Length: int64(len(command.Data))})
		if len(command.Data) != 0 {
			return f.mutate(ctx, func(previous int64) int64 {
				return max(previous, command.Offset+int64(len(command.Data)))
			}, func(body []byte) { copy(body[command.Offset:], command.Data) }, &command)
		}
	case storage.MutateAppend:
		ctx = metastore.WithFileAccess(ctx, metastore.FileAccess{Uses: storage.WriteData, Append: true, Length: int64(len(command.Data))})
		if len(command.Data) != 0 {
			return f.mutate(ctx, func(previous int64) int64 {
				if previous > math.MaxInt64-int64(len(command.Data)) {
					return -1
				}
				return previous + int64(len(command.Data))
			}, func(body []byte) { copy(body[len(body)-len(command.Data):], command.Data) }, &command)
		}
	}
	if command.Kind != storage.MutateAttributes && !f.options.Write {
		return storage.Attr{}, syscall.EBADF
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	state, err := any(f.native).(metastore.ConditionalFileMutation).MutateFile(ctx, command)
	return state.Attr(), err
}

func isRevisionRace(err error) bool {
	return isOnly(err, syscall.EAGAIN) &&
		!errors.Is(err, storage.ErrUseConflict) &&
		!errors.Is(err, storage.ErrRangeConflict) &&
		!errors.Is(err, storage.ErrConditionConflict)
}

var _ storage.ConditionalFileMutation = (*openFile)(nil)
