package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileActorKey struct{}
type fileActor struct {
	session, reference uint64
	node               int64
	cleanup            bool
}

func fileClaimError(err error) error {
	if errors.Is(err, fileaccess.ErrConflict) {
		return &storage.FileError{Code: syscall.EACCES, Conflict: &storage.FileConflict{Kind: storage.ConflictClaim}, Cause: err}
	}
	if errors.Is(err, fileaccess.ErrAccess) {
		return &storage.FileError{Code: syscall.EBADF, Cause: err}
	}
	return fileAccessError(err)
}

func (f *fileReference) lifetimeGuardLocked(ctx context.Context) fileaccess.Guard {
	return func() error {
		if err := f.session.store.coordinator.healthy(); err != nil {
			return err
		}
		if err := metastore.CheckFilePublication(ctx); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !f.session.active || f.session.closed || !time.Now().Before(f.session.expires) {
			return syscall.ESTALE
		}
		if !f.active || f.closed {
			return syscall.EBADF
		}
		return nil
	}
}

func (s *fileSession) lifetimeGuardLocked(ctx context.Context) fileaccess.Guard {
	return func() error {
		if err := s.store.coordinator.healthy(); err != nil {
			return err
		}
		if err := metastore.CheckFilePublication(ctx); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !s.active || s.closed || !time.Now().Before(s.expires) {
			return syscall.ESTALE
		}
		return nil
	}
}

func (s *fileSession) nativeContext(ctx context.Context) context.Context {
	return metastore.WithFilePublicationGuard(ctx, func() error {
		if !s.active || s.closed || !time.Now().Before(s.expires) {
			return syscall.ESTALE
		}
		return nil
	})
}

func (f *fileReference) nativeContext(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, fileActorKey{}, fileActor{session: f.session.id, reference: uint64(f.reference), node: f.id})
	return metastore.WithFilePublicationGuard(ctx, func() error {
		if !f.active || !f.session.active || !time.Now().Before(f.session.expires) {
			return syscall.ESTALE
		}
		return nil
	})
}

func (s *Store) checkFileIOLocked(ctx context.Context, tx *sql.Tx, id int64, size int64, operation storage.FileIO) error {
	if s.fileDomain == nil {
		return nil
	}
	if bypass, _ := ctx.Value(fileRecoveryKey{}).(bool); !bypass && (time.Now().Before(s.fileDomain.recoveryUntil) || s.fileDomain.recoveryPending) {
		return syscall.EAGAIN
	}
	actor, _ := ctx.Value(fileActorKey{}).(fileActor)
	if actor.node != id {
		actor = fileActor{}
	}
	if actor.reference == 0 {
		if err := s.checkNodeAdmission(ctx, tx, id); err != nil {
			return err
		}
	}
	uses := storage.ReadContent
	if operation.Write || operation.Truncate {
		uses = storage.WriteContent
	}
	if err := s.fileDomain.access.CheckUse(uint64(id), actor.reference, uint64(uses)); err != nil {
		return fileClaimError(err)
	}
	if operation.Truncate {
		if operation.Size < 0 {
			return syscall.EINVAL
		}
		operation.Offset, operation.Length = min(size, operation.Size), max(size, operation.Size)-min(size, operation.Size)
		operation.Write = true
	}
	if operation.Offset < 0 || operation.Length < 0 || operation.Offset > int64(^uint64(0)>>1)-operation.Length {
		return syscall.EINVAL
	}
	if !operation.Write {
		operation.Length = min(operation.Length, max(size-operation.Offset, 0))
	}
	if operation.Length == 0 {
		return nil
	}
	var owner *fileaccess.Owner
	if operation.Owner != nil {
		if actor.session == 0 {
			return syscall.EINVAL
		}
		value := fileaccess.Owner{Session: actor.session, ID: uint64(*operation.Owner)}
		owner = &value
	}
	return fileAccessError(s.fileDomain.access.CheckIO(uint64(id), owner,
		fileaccess.Span{Start: uint64(operation.Offset), End: uint64(operation.Offset + operation.Length - 1)}, operation.Write))
}

func (s *Store) checkFileMutationLocked(ctx context.Context, tx *sql.Tx, intent volumeIntent) error {
	if s.fileDomain == nil || intent.cleanup {
		return nil
	}
	if bypass, _ := ctx.Value(fileRecoveryKey{}).(bool); !bypass && (time.Now().Before(s.fileDomain.recoveryUntil) || s.fileDomain.recoveryPending) {
		return syscall.EAGAIN
	}
	var nodes []metastore.Node
	ids := intent.nodes
	if intent.node != 0 {
		ids = []int64{intent.node}
	}
	for _, id := range ids {
		node, err := s.nodeByID(ctx, tx, id)
		if err != nil {
			return err
		}
		nodes = append(nodes, node)
	}
	if len(ids) == 0 {
		for _, name := range intent.paths {
			node, err := s.resolve(ctx, tx, name)
			if errors.Is(err, syscall.ENOENT) {
				continue
			}
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
		}
	}
	actor, _ := ctx.Value(fileActorKey{}).(fileActor)
	for _, node := range nodes {
		switch intent.kind {
		case locking.WriteMutation:
			if operation, ok := metastore.FileIOFromContext(ctx); ok {
				if err := s.checkFileIOLocked(ctx, tx, node.ID, node.Size, operation); err != nil {
					return err
				}
			}
		case locking.RemoveMutation, locking.RenameMutation:
			handle := actor.reference
			if node.ID != actor.node {
				handle = 0
			}
			if handle == 0 && !actor.cleanup {
				if err := s.checkNodeAdmission(ctx, tx, node.ID); err != nil {
					return err
				}
			}
			var err error
			if actor.cleanup {
				err = s.fileDomain.access.CheckExclusions(uint64(node.ID), handle, uint64(storage.RemoveEntry))
			} else {
				err = s.fileDomain.access.CheckUse(uint64(node.ID), handle, uint64(storage.RemoveEntry))
			}
			if err != nil {
				return fileClaimError(err)
			}
		}
	}
	return nil
}

func (s *Store) checkNodeAdmission(ctx context.Context, tx *sql.Tx, id int64) error {
	if id == s.root {
		return nil
	}
	var draining bool
	err := tx.QueryRowContext(ctx, `SELECT draining FROM entries WHERE volume=? AND node=?`, s.volume, id).Scan(&draining)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if draining {
		return &storage.FileError{Code: syscall.EBUSY, Conflict: &storage.FileConflict{Kind: storage.ConflictDraining, NodeID: uint64(id)}}
	}
	return nil
}
