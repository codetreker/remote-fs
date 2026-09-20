package httprest

import (
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
	"unicode/utf8"
)

func validateCapabilityArguments(req fileRequest) error {
	switch req.Op {
	case storage.OpFileScope:
		return nil
	case storage.OpFileSetNodeMetadata, storage.OpFileSetMetadata:
		if req.Op == storage.OpFileSetNodeMetadata && req.Node == 0 {
			return syscall.EINVAL
		}
		return storage.CheckMetadataUpdate(req.Namespace, []byte(req.Version), []byte(req.Payload))
	case storage.OpFileNewUseOwner:
		if req.Node == 0 || req.Scope == nil {
			return syscall.EINVAL
		}
		if err := req.Scope.Check(); err != nil {
			return err
		}
		if !utf8.ValidString(req.Scope.Token) {
			return syscall.EINVAL
		}
		return req.OwnerOptions.Check()
	case storage.OpFileRetireUseOwner:
		if req.Owner == 0 {
			return syscall.EINVAL
		}
	case storage.OpFileRangeApply, storage.OpFileRangeGetConflict:
		if req.Owner == 0 || len(req.Commands) == 0 || len(req.Commands) > storage.MaxRangeCommands {
			return syscall.EINVAL
		}
		if req.Op == storage.OpFileRangeGetConflict && len(req.Commands) != 1 {
			return syscall.EINVAL
		}
		for _, command := range req.Commands {
			if err := command.Check(); err != nil {
				return err
			}
		}
		if req.Op == storage.OpFileRangeApply {
			_, err := req.LockID.Epoch()
			return err
		}
	case storage.OpFileRangeQuery, storage.OpFileRangeCancel:
		if req.Owner == 0 {
			return syscall.EINVAL
		}
		_, err := req.LockID.Epoch()
		return err
	case storage.OpFileRangeDrop:
		if req.Owner == 0 || req.Domain < storage.DomainRecord || req.Domain > storage.DomainEnforced {
			return syscall.EINVAL
		}
	}
	return nil
}
