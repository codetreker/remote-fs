package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/windowsaccess"
	"github.com/codetreker/remote-fs/packages/storage"
)

type windowsActorKey struct{}

func withWindowsActor(ctx context.Context, handle uint64) context.Context {
	return context.WithValue(ctx, windowsActorKey{}, handle)
}
func windowsActor(ctx context.Context) uint64 {
	actor, _ := ctx.Value(windowsActorKey{}).(uint64)
	return actor
}

func (s *Store) registerWindowsOpenLocked(ctx context.Context, handle uint64, id int64, access storage.WindowsAccess, share storage.WindowsShare) error {
	if time.Now().Before(s.fileDomain.windows.recoveryUntil) {
		return syscall.EAGAIN
	}
	return windowsError(s.fileDomain.windows.access.Open(handle, uint64(id), windowsAccess(access), windowsaccess.Access(share)))
}

func (s *Store) checkWindowsIOLocked(id int64, actor uint64, size int64, operation metastore.WindowsIO) error {
	if s.fileDomain == nil {
		return nil
	}
	if time.Now().Before(s.fileDomain.windows.recoveryUntil) {
		return syscall.EAGAIN
	}
	if operation.Truncate {
		if operation.Size < 0 {
			return syscall.EINVAL
		}
		operation.Offset = min(size, operation.Size)
		operation.Length = max(size, operation.Size) - operation.Offset
		operation.Write = true
	}
	if err := storage.CheckWindowsRange(operation.Offset, operation.Length); err != nil {
		return err
	}
	if !operation.Write {
		operation.Length = min(operation.Length, max(size-operation.Offset, 0))
	}
	if operation.Length == 0 {
		access := windowsaccess.Read
		if operation.Write {
			access = windowsaccess.Write
		}
		return windowsError(s.fileDomain.windows.access.CheckAccess(uint64(id), actor, access))
	}
	return windowsError(s.fileDomain.windows.access.CheckIO(uint64(id), actor, windowsaccess.Range{Offset: uint64(operation.Offset), Length: uint64(operation.Length)}, operation.Write))
}

func (s *Store) checkWindowsMutationLocked(ctx context.Context, tx *sql.Tx, intent volumeIntent) error {
	if s.fileDomain == nil || intent.cleanup {
		return nil
	}
	if time.Now().Before(s.fileDomain.windows.recoveryUntil) {
		return syscall.EAGAIN
	}
	var nodes []metastore.Node
	if len(intent.nodes) != 0 {
		for _, id := range intent.nodes {
			node, err := s.nodeByID(ctx, tx, id)
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
		}
	} else if intent.node != 0 {
		node, err := s.nodeByID(ctx, tx, intent.node)
		if err != nil {
			return err
		}
		nodes = append(nodes, node)
	} else {
		for _, path := range intent.paths {
			node, err := s.resolve(ctx, tx, path)
			if errors.Is(err, syscall.ENOENT) {
				continue
			}
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
		}
	}
	actor := windowsActor(ctx)
	for index, node := range nodes {
		switch intent.kind {
		case locking.WriteMutation:
			if actor == 0 {
				if err := s.checkWindowsReadonly(ctx, tx, node.ID); err != nil {
					return err
				}
			}
			operation, ok := metastore.FileIOFromContext(ctx)
			if !ok {
				continue
			}
			if err := s.checkWindowsIOLocked(node.ID, actor, node.Size, operation); err != nil {
				return err
			}
		case locking.RemoveMutation, locking.RenameMutation:
			if !node.IsDir() && (intent.kind == locking.RemoveMutation && actor == 0 || intent.kind == locking.RenameMutation && index > 0) {
				if err := s.checkWindowsReadonly(ctx, tx, node.ID); err != nil {
					return err
				}
			}
			own := actor
			if own != 0 {
				id, err := s.fileDomain.windows.access.HandleNode(own)
				if err != nil {
					return windowsError(err)
				}
				if id != uint64(node.ID) {
					own = 0
				}
			}
			if own == 0 {
				if err := s.fileDomain.windows.access.CheckAccess(uint64(node.ID), 0, windowsaccess.Delete); err != nil {
					return windowsError(err)
				}
			} else if err := s.fileDomain.windows.access.CheckSharing(uint64(node.ID), own, windowsaccess.Delete); err != nil {
				return windowsError(err)
			}
		}
	}
	return nil
}

func (s *Store) checkWindowsReadonly(ctx context.Context, tx *sql.Tx, id int64) error {
	var attributes uint32
	var mode fs.FileMode
	if err := tx.QueryRowContext(ctx, `SELECT mode,windows_attributes FROM nodes WHERE volume=? AND id=?`, s.volume, id).Scan(&mode, &attributes); err != nil {
		return err
	}
	if err := checkWindowsKindAttributes(mode, attributes); err != nil {
		return err
	}
	if attributes&storage.WindowsDOSReadOnly != 0 && attributes&storage.WindowsDOSDirectory == 0 {
		return syscall.EACCES
	}
	return nil
}

func (s *Store) configureWindowsRecovery(ctx context.Context, r *LeaseRecovery) error {
	if s.fileDomain == nil {
		return nil
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.coordinator.commit.release()
	var version uint32
	if err := s.inspect(ctx, func(tx *sql.Tx) error { var err error; version, err = s.windowsNameVersion(ctx, tx); return err }); err != nil {
		return err
	}
	if version != 0 {
		s.fileDomain.windows.recoveryUntil = r.start.Add(r.state.MaxLease)
	}
	return nil
}

func checkWindowsKindAttributes(mode fs.FileMode, attributes uint32) error {
	if attributes&^(storage.WindowsSettableDOSAttributes|storage.WindowsDOSDirectory) != 0 {
		return syscall.EIO
	}
	if attributes&storage.WindowsDOSDirectory != 0 && !mode.IsDir() && mode&fs.ModeSymlink == 0 {
		return syscall.EIO
	}
	return nil
}
