package smb

import (
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func validateCreateContexts(request wire.CreateRequest) error {
	seen := make(map[string]struct{}, len(request.Contexts))
	var durableV1, durableV2 bool
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
			if len(context.Data) != 32 {
				return syscall.EINVAL
			}
		case "RqLs":
			if len(context.Data) != 32 && len(context.Data) != 52 {
				return syscall.EINVAL
			}
		case "AlSi":
			if len(context.Data) != 8 {
				return syscall.EINVAL
			}
		default:
			return syscall.EOPNOTSUPP
		}
	}
	if durableV1 && durableV2 {
		return syscall.EINVAL
	}
	return nil
}
