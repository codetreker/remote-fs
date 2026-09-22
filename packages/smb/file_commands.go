package smb

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileAuthorizationError struct {
	cause          error
	classification syscall.Errno
}

func (e *fileAuthorizationError) Error() string         { return e.cause.Error() }
func (e *fileAuthorizationError) Unwrap() error         { return e.cause }
func (e *fileAuthorizationError) Classification() error { return e.classification }

func (c *connection) authorizeFileAccess(ctx context.Context, tree *tree, request authz.AccessRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	request.Volume = tree.export.share.Volume
	err := c.server.config.Authorize.Authorize(ctx, request)
	if callErr := ctx.Err(); callErr != nil {
		return callErr
	}
	if err == nil {
		return nil
	}
	errno := syscall.EIO
	if errors.Is(err, authz.ErrDenied) {
		errno = syscall.EACCES
	}
	return &fileAuthorizationError{cause: err, classification: errno}
}

func (c *connection) authorizeFileOperation(ctx context.Context, tree *tree, operation storage.Operation) error {
	return c.authorizeFileAccess(ctx, tree, authz.AccessRequest{Operation: operation})
}

func (c *connection) closeHandle(ctx context.Context, tree *tree, request wire.Request) ([]byte, uint32) {
	decoded, err := request.Close()
	if err != nil || decoded.Flags&^uint16(1) != 0 {
		return nil, statusInvalid
	}
	if err := c.authorizeFileOperation(ctx, tree, storage.OpFileClose); err != nil {
		return nil, fileCommandStatus(err)
	}
	handle := tree.files.get(decoded.FileID)
	if handle == nil {
		return nil, statusFileClosed
	}
	capture := decoded.Flags&1 != 0
	if capture {
		if err := c.authorizeFileOperation(ctx, tree, storage.OpFileStat); err != nil {
			if storage.ErrnoOf(err) == syscall.EINTR {
				return nil, fileCommandStatus(err)
			}
			capture = false
		}
	}
	var releaseResult func()
	if capture {
		releaseResult, err = reserveFileCommandResult(ctx, tree, false)
		if err != nil {
			if storage.ErrnoOf(err) == syscall.EINTR {
				return nil, fileCommandStatus(err)
			}
			capture = false
		} else {
			defer releaseResult()
			ctx = storage.WithBoundedAttrResult(ctx, tree.files.limits.MaxOpenResultBytes, func(_ storage.Attr, metadataBytes int64) error {
				charge, err := storage.MetadataRetentionBytes(metadataBytes)
				if err != nil {
					return err
				}
				if charge+512 > tree.files.limits.MaxOpenResultBytes {
					return syscall.EFBIG
				}
				return nil
			})
		}
	}
	attr, err := tree.files.closeIDWithAttr(ctx, decoded.FileID, handle, capture)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	if !capture {
		return wire.CloseResponseBody(0, wire.FileInformation{}), statusOK
	}
	if attr.ID != handle.nodeID {
		return nil, statusIO
	}
	size, err := virtualFileSize(attr)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	information, err := captureCreateInformation(attr, size)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	return wire.CloseResponseBody(1, information), statusOK
}
