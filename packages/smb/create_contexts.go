package smb

import (
	"encoding/binary"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

const reservedCreateContext = "\x93\xad\x25\x50\x9c\xb4\x11\xe7\xb4\x23\x83\xde\x96\x8b\xcd\x7c"

// Optional durable, lease and allocation requests do not grant those features.
// Their valid payloads are ignored; other intents are outside this endpoint's
// supported CREATE set and are rejected before the basic open.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/9adbc354-5fad-40e7-9a62-4a4b6c1ff8a0
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/24cfce29-5d80-4651-ba5f-a2661c666b41
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/3f343618-01b0-4aaf-b8a3-b270f9a6d334
func validateCreateContexts(request wire.CreateRequest) error {
	seen := make(map[string]struct{}, len(request.Contexts))
	var durableV1, durableV2, unsupported bool
	for _, context := range request.Contexts {
		name := string(context.Name)
		if _, exists := seen[name]; exists {
			return syscall.EINVAL
		}
		seen[name] = struct{}{}
		switch name {
		case "DHnQ":
			durableV1 = true
			if len(context.Data) != 16 {
				return syscall.EINVAL
			}
		case "DH2Q":
			durableV2 = true
			if len(context.Data) != 32 || binary.LittleEndian.Uint32(context.Data[4:8])&^uint32(2) != 0 {
				return syscall.EINVAL
			}
		case "RqLs":
			if len(context.Data) != 32 && len(context.Data) != 52 {
				return syscall.EINVAL
			}
			if binary.LittleEndian.Uint32(context.Data[16:20])&^uint32(7) != 0 ||
				len(context.Data) == 52 && binary.LittleEndian.Uint32(context.Data[20:24])&^uint32(4) != 0 {
				return syscall.EINVAL
			}
		case "AlSi":
			if len(context.Data) != 8 {
				return syscall.EINVAL
			}
		case "DHnC":
			if len(context.Data) != 16 {
				return syscall.EINVAL
			}
			unsupported = true
		case "DH2C":
			if len(context.Data) != 36 || binary.LittleEndian.Uint32(context.Data[32:36])&^uint32(2) != 0 {
				return syscall.EINVAL
			}
			unsupported = true
		case "MxAc":
			if len(context.Data) != 0 && len(context.Data) != 8 {
				return syscall.EINVAL
			}
			unsupported = true
		case "QFid":
			if len(context.Data) != 0 {
				return syscall.EINVAL
			}
			unsupported = true
		case reservedCreateContext:
		default:
			unsupported = true
		}
	}
	if durableV1 && durableV2 {
		return syscall.EINVAL
	}
	if unsupported {
		return syscall.EOPNOTSUPP
	}
	return nil
}
