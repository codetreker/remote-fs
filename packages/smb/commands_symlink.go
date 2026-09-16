package smb

import (
	"context"
	"errors"
	"path"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

const fsctlGetReparsePoint uint32 = 0x000900a8
const fsctlSetReparsePoint uint32 = 0x000900a4

func relativeLink(target string, location windowsNameInfo) (string, error) {
	if err := checkWindowsLinkTarget(target); err != nil {
		return "", err
	}
	if err := location.Check(); err != nil {
		return "", err
	}
	if location.State != windowsNameLinked {
		return "", syscall.EIO
	}
	parent := path.Dir(location.Path)
	var base []string
	if parent != "." {
		base = strings.Split(parent, "/")
	}
	parts := append([]string(nil), base...)
	absolute := strings.HasPrefix(target, "/")
	if absolute {
		parts = nil
	}
	for _, component := range strings.Split(target, "/") {
		switch component {
		case "", ".":
		case "..":
			if len(parts) == 0 {
				return "", syscall.EACCES
			}
			parts = parts[:len(parts)-1]
		default:
			if _, err := nameKey([]byte(component)); err != nil {
				return "", err
			}
			parts = append(parts, component)
		}
	}
	if !absolute {
		return strings.ReplaceAll(target, "/", "\\"), nil
	}
	common := 0
	for common < len(base) && common < len(parts) && base[common] == parts[common] {
		common++
	}
	relative := make([]string, 0, len(base)+len(parts)-2*common)
	for i := common; i < len(base); i++ {
		relative = append(relative, "..")
	}
	relative = append(relative, parts[common:]...)
	if len(relative) == 0 {
		return ".", nil
	}
	return strings.Join(relative, "\\"), nil
}

func createFailure(err error) ([]byte, uint32) {
	var link *windowsSymlinkError
	if !errors.As(err, &link) {
		return nil, statusError(err)
	}
	target, targetErr := relativeLink(link.Target, link.Location)
	if targetErr != nil {
		return nil, statusError(targetErr)
	}
	if link.Unparsed != "" && !strings.HasPrefix(link.Unparsed, "/") {
		return nil, statusIO
	}
	body, encodeErr := wire.SymlinkErrorResponseBody(target, strings.ReplaceAll(link.Unparsed, "/", "\\"))
	if encodeErr != nil {
		return nil, statusIO
	}
	return body, 0x8000002d
}

func (c *connection) reparse(ctx context.Context, t *tree, r wire.Request) ([]byte, uint32) {
	q, err := r.IOCTL()
	if err != nil || q.Flags != 1 || q.MaxOutputResponse > uint32(c.server.config.Limits.MaxIOBytes) {
		return nil, statusInvalid
	}
	h := t.files.get(q.FileID)
	if h == nil {
		return nil, fileClosed
	}
	switch q.Code {
	case fsctlGetReparsePoint:
		if len(q.Input) != 0 {
			return nil, statusInvalid
		}
		info, err := h.file.ReadLink(ctx)
		if err != nil {
			return nil, statusError(err)
		}
		if info.Unparsed != "" {
			return nil, statusIO
		}
		target, err := relativeLink(info.Target, info.Location)
		if err != nil {
			return nil, statusError(err)
		}
		data, err := wire.ReparseSymlinkData(target)
		if err != nil {
			return nil, statusIO
		}
		if len(data) > int(q.MaxOutputResponse) {
			return nil, fileBufferTooSmall
		}
		return wire.IOCTLResponseBody(q.Code, q.FileID, q.Flags, data), 0
	case fsctlSetReparsePoint:
		target, relative, err := wire.ParseSymlinkReparse(q.Input)
		if err != nil {
			return nil, 0xc0000278
		}
		if !relative {
			parts := strings.Split(target, "\\")
			if len(parts) < 6 || parts[0] != "" || parts[1] != "??" || !strings.EqualFold(parts[2], "UNC") || !strings.EqualFold(parts[4], t.export.share.Name) {
				return nil, statusUnsupported
			}
			host := strings.ToLower(parts[3])
			if host != "localhost" && host != "127.0.0.1" && host != "[::1]" {
				return nil, statusDenied
			}
			target = "/" + strings.Join(parts[5:], "/")
		} else {
			target = strings.ReplaceAll(target, "\\", "/")
		}
		if err := checkWindowsLinkTarget(target); err != nil {
			return nil, statusError(err)
		}
		action, err := t.files.actionID(ctx)
		if err != nil {
			return nil, statusError(err)
		}
		result, err := h.file.SetLink(ctx, target, action)
		if status := t.files.mutationResult(ctx, action, result, err); status != 0 {
			return nil, status
		}
		return wire.IOCTLResponseBody(q.Code, q.FileID, q.Flags, nil), 0
	}
	return nil, statusUnsupported
}
