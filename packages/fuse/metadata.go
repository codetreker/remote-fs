package fuse

import (
	"context"
	"errors"
	iofs "io/fs"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"

	"github.com/codetreker/remote-fs/packages/fuse/posix"
	"github.com/codetreker/remote-fs/packages/storage"
)

const maxPermissionCASAttempts = 8

type attributeHandle interface {
	stat(context.Context) (storage.Attr, error)
	setAttr(context.Context, storage.AttrChange) (storage.Attr, error)
}

type attributeChange struct {
	storage.AttrChange
	Mode *iofs.FileMode
}

// Absent permissions have mount-local presentation defaults. Reading them does
// not persist a payload or claim an authority-issued metadata version.
func permissions(attr storage.Attr) (iofs.FileMode, error) {
	var mode iofs.FileMode
	switch attr.Kind {
	case storage.NodeRegular:
		mode = 0644
	case storage.NodeDirectory:
		mode = iofs.ModeDir | 0755
	case storage.NodeSymlink:
		mode = iofs.ModeSymlink | 0777
	default:
		return 0, syscall.EIO
	}
	if payload, ok := attr.Metadata[posix.Namespace]; ok {
		if len(payload.Version) == 0 {
			return 0, syscall.EIO
		}
		value, err := posix.Decode(payload.Data)
		if err != nil {
			return 0, err
		}
		mode = mode.Type() | value
	}
	return mode, nil
}

func attributeMode(attr storage.Attr) (uint32, syscall.Errno) {
	mode, err := permissions(attr)
	if err != nil {
		return 0, errnoOf(err)
	}
	return systemMode(mode)
}

func initialPermissions(mode iofs.FileMode) (map[string][]byte, error) {
	data, err := posix.Encode(mode)
	if err != nil {
		return nil, err
	}
	return map[string][]byte{posix.Namespace: data}, nil
}

func (n *node) setPermissions(ctx context.Context, f fs.FileHandle, mode iofs.FileMode) error {
	data, err := posix.Encode(mode)
	if err != nil {
		return err
	}
	var read func(context.Context) (storage.Attr, error)
	var update func(context.Context, string, []byte, []byte) (storage.OpaquePayload, error)
	if h, ok := f.(*handle); ok {
		reference, ok := h.file.(storage.ReferenceMetadataAccess)
		if !ok {
			return syscall.EOPNOTSUPP
		}
		if err := reference.CheckMetadataAccess(); err != nil {
			return err
		}
		read = h.stat
		update = reference.SetMetadata
	} else {
		access, ok := n.volume.files.(storage.MetadataAccess)
		if !ok {
			return syscall.EOPNOTSUPP
		}
		if err := access.CheckMetadataAccess(); err != nil {
			return err
		}
		read = func(ctx context.Context) (storage.Attr, error) {
			return n.volume.files.StatNode(ctx, n.id.node)
		}
		update = func(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
			return access.SetMetadata(ctx, n.id.node, namespace, version, data)
		}
	}
	for range maxPermissionCASAttempts {
		attr, err := read(ctx)
		if err != nil {
			return err
		}
		if err := n.checkAttr(attr); err != nil {
			return err
		}
		if attr.Kind == storage.NodeSymlink {
			return syscall.EOPNOTSUPP
		}
		payload, err := update(ctx, posix.Namespace, attr.Metadata[posix.Namespace].Version, data)
		if errors.Is(err, storage.ErrConditionConflict) && errnoOf(err) == syscall.EAGAIN {
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return afterMutation(true, err)
		}
		if len(payload.Version) == 0 {
			return syscall.EIO
		}
		got, err := posix.Decode(payload.Data)
		if err != nil {
			return err
		}
		if got != mode {
			return syscall.EIO
		}
		return nil
	}
	return storage.ErrConditionConflict
}
