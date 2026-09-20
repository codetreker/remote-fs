package sqlite

import (
	"context"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Store) checkByteExtent(ctx context.Context, node uint64, scope storage.UseScope, offset, length int64, uses storage.Uses) error {
	if offset < 0 || length < 0 || length > math.MaxInt64-offset {
		return syscall.EINVAL
	}
	if length == 0 {
		return s.fileDomain.coordinator.CheckUse(ctx, node, scope, uses)
	}
	return s.fileDomain.coordinator.CheckIO(ctx, node, scope,
		storage.Range{Kind: storage.Bytes, Start: uint64(offset), Length: uint64(length)}, uses)
}

func (s *Store) checkAccessIntent(ctx context.Context, state metastore.FileState, scope storage.UseScope, access metastore.FileAccess) error {
	if access.Uses == storage.ReadData && access.Offset >= 0 && access.Length >= 0 {
		if access.Offset >= state.Size {
			access.Length = 0
		} else {
			access.Length = min(access.Length, state.Size-access.Offset)
		}
	}
	if err := access.Check(); err != nil {
		return err
	}
	if state.Kind != storage.NodeRegular {
		return syscall.EISDIR
	}
	offset, length := access.Offset, access.Length
	if access.Append {
		offset = state.Size
	}
	if access.Truncate {
		offset = min(state.Size, access.Size)
		length = max(state.Size, access.Size) - offset
	}
	return s.checkByteExtent(ctx, uint64(state.ID), scope, offset, length, access.Uses)
}

func (f *retainedFile) checkCaptureAccess(ctx context.Context, state metastore.FileState) error {
	access, present := metastore.FileAccessFrom(ctx)
	if !present {
		return nil
	}
	if access.Uses == storage.ReadData && !f.read || access.Uses == storage.WriteData && !f.write {
		return syscall.EBADF
	}
	return f.store.checkAccessIntent(ctx, state, f.scope, access)
}

func (s *Store) checkContentPublication(ctx context.Context, before metastore.FileState, nextSize int64, scope storage.UseScope) error {
	access, present := metastore.FileAccessFrom(ctx)
	if present {
		if access.Uses != storage.WriteData {
			return syscall.EINVAL
		}
		return s.checkAccessIntent(ctx, before, scope, access)
	}
	return s.checkByteExtent(ctx, uint64(before.ID), scope, 0, max(before.Size, nextSize), storage.WriteData)
}

func (s *Store) CheckUseOwners() error    { return s.CheckFileStore() }
func (s *Store) CheckRangeControl() error { return s.CheckFileStore() }
