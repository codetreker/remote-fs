package smb

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func encodeSID(text string) ([]byte, error) {
	parts := strings.Split(text, "-")
	if len(parts) < 4 || len(parts) > 18 || parts[0] != "S" || parts[1] != "1" {
		return nil, ErrConfig
	}
	authority, err := strconv.ParseUint(parts[2], 10, 48)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 8+4*(len(parts)-3))
	b[0] = 1
	b[1] = byte(len(parts) - 3)
	for i := 7; i >= 2; i-- {
		b[i] = byte(authority)
		authority >>= 8
	}
	for i, text := range parts[3:] {
		value, err := strconv.ParseUint(text, 10, 32)
		if err != nil {
			return nil, err
		}
		smbLE.PutUint32(b[8+i*4:], uint32(value))
	}
	return b, nil
}

func (c *connection) securityInfo(ctx context.Context, t *tree, r wire.Request) ([]byte, uint32) {
	q, err := r.QueryInfo()
	if err != nil || q.Type != 3 || q.Flags != 0 || len(q.Input) != 0 {
		return nil, statusInvalid
	}
	if q.Additional == 0 || q.Additional&^uint32(7) != 0 {
		return nil, statusUnsupported
	}
	h := t.files.get(q.FileID)
	if h == nil {
		return nil, fileClosed
	}
	if h.access&0x20000 == 0 {
		return nil, statusDenied
	}
	if err := h.file.Sync(ctx); err != nil {
		return nil, statusError(err)
	}
	principal, ok := PrincipalFromContext(ctx)
	if !ok {
		return nil, statusDenied
	}
	sid, err := encodeSID(principal.SID)
	if err != nil {
		return nil, statusIO
	}
	descriptor := make([]byte, 20)
	descriptor[0] = 1
	control := uint16(0x8000)
	if q.Additional&1 != 0 {
		smbLE.PutUint32(descriptor[4:], uint32(len(descriptor)))
		descriptor = append(descriptor, sid...)
	}
	if q.Additional&2 != 0 {
		smbLE.PutUint32(descriptor[8:], uint32(len(descriptor)))
		descriptor = append(descriptor, sid...)
	}
	if q.Additional&4 != 0 {
		control |= 4
		smbLE.PutUint32(descriptor[16:], uint32(len(descriptor)))
		acl := make([]byte, 16+len(sid))
		acl[0] = 2
		smbLE.PutUint16(acl[2:], uint16(len(acl)))
		smbLE.PutUint16(acl[4:], 1)
		smbLE.PutUint16(acl[10:], uint16(8+len(sid)))
		smbLE.PutUint32(acl[12:], h.access)
		copy(acl[16:], sid)
		descriptor = append(descriptor, acl...)
	}
	smbLE.PutUint16(descriptor[2:], control)
	if len(descriptor) > int(q.OutputLength) {
		b := make([]byte, 20)
		smbLE.PutUint16(b, 9)
		b[2] = 1
		smbLE.PutUint32(b[4:], 12)
		smbLE.PutUint32(b[8:], 4)
		smbLE.PutUint32(b[16:], uint32(len(descriptor)))
		return b, fileBufferTooSmall
	}
	return wire.BufferResponseBody(descriptor), 0
}

func (c *connection) authorizeMaximumAccess(ctx context.Context, t *tree, intent windowsOpenIntent) (windowsAccess, error) {
	kind := storage.NodeRegular
	if intent.Kind == windowsDirectory {
		kind = storage.NodeDirectory
	}
	return maximumWindowsAccess(ctx, intent, kind, func(candidate windowsOpenIntent) error {
		return c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: storage.OpFileRetainAt, Effects: fileEffects(storage.OpFileRetainAt), Claim: windowsClaim(candidate, kind)})
	}, func(op storage.Operation) error {
		return c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: op, Effects: fileEffects(op)})
	})
}
func maximumWindowsAccess(ctx context.Context, intent windowsOpenIntent, kind storage.NodeKind, retain func(windowsOpenIntent) error, operation func(storage.Operation) error) (windowsAccess, error) {
	mask := intent.Access
	required := mask
	if intent.DeleteOnClose || intent.Disposition == windowsSupersede {
		required |= windowsDelete
	}
	if intent.Disposition == windowsOverwrite || intent.Disposition == windowsOverwriteIf {
		required |= windowsWriteData
	}
	candidate := intent
	candidate.MaximumAllowed = false
	candidate.Access = required
	if err := candidate.Check(); err != nil {
		return 0, err
	}
	if err := retain(candidate); err != nil {
		return 0, err
	}
	for _, entry := range accessBits {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		op := windowsAccessOperation(entry.semantic, kind)
		err := operation(op)
		if err == nil {
			candidate.Access = mask | entry.semantic
			err = retain(candidate)
		}
		if err == nil {
			mask |= entry.semantic
		} else if !errors.Is(err, authz.ErrDenied) {
			return 0, err
		} else if required&entry.semantic != 0 {
			return 0, err
		}
	}
	if mask == 0 {
		return 0, authz.ErrDenied
	}
	return mask, nil
}
func windowsAccessOperation(access windowsAccess, kind storage.NodeKind) storage.Operation {
	switch access {
	case windowsReadData:
		if kind == storage.NodeDirectory {
			return storage.OpFileListAt
		}
		return storage.OpFileRead
	case windowsWriteData, windowsAppendData:
		if kind == storage.NodeDirectory {
			return storage.OpFileCreateAndRetainAt
		}
		return storage.OpFileWrite
	case windowsDelete:
		return storage.OpFileDrainEntry
	case windowsReadAttributes, windowsReadSecurity:
		return storage.OpFileStat
	case windowsWriteAttributes:
		return storage.OpFileSetAttr
	case windowsSynchronize:
		return storage.OpFileSync
	case windowsDeleteChild:
		return storage.OpFileRename
	default:
		panic("unknown Windows access bit")
	}
}
