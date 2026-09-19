package httprest

import (
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

func validateCapabilityArguments(req fileRequest) error {
	switch req.Op {
	case storage.OpFileOpenAt:
		if err := req.Child.Check(); err != nil {
			return err
		}
		return req.OpenAt.storage().Check()
	case storage.OpFileOpenNodeRef:
		if req.Node == 0 || req.NodeRef.Create || req.NodeRef.Exclusive {
			return syscall.EINVAL
		}
		return req.NodeRef.storage().Check()
	case storage.OpFileOpenChildRef:
		if err := req.Child.Check(); err != nil {
			return err
		}
		return req.NodeRef.storage().Check()
	case storage.OpFileLookupAt:
		return req.Child.Check()
	case fileObserveName:
		if req.ResultBytes <= 0 {
			return syscall.EINVAL
		}
		return req.Guards.Check()
	case fileObserveDirectoryMetadata:
		if req.ResultBytes <= 0 {
			return syscall.EINVAL
		}
		if err := req.Directory.Check(); err != nil {
			return err
		}
		return req.DirectoryMetadata.Check()
	case storage.OpFileReadDirNode:
		if req.ResultBytes <= 0 {
			return syscall.EINVAL
		}
		return req.Directory.Check()
	case storage.OpFileMutateName:
		return req.Name.storage().Check()
	case storage.OpFileSetNodeMetadata, storage.OpFileSetMetadata:
		if req.Op == storage.OpFileSetNodeMetadata && req.Node == 0 {
			return syscall.EINVAL
		}
		if err := storage.CheckMetadataNamespace(req.Namespace); err != nil {
			return err
		}
		if len(req.Version) > storage.MaxObservationTokenBytes || len(req.Payload) > storage.MaxMetadataValueBytes {
			return syscall.EFBIG
		}
	case storage.OpFileNewUseOwner:
		if req.Node == 0 || req.OwnerOptions.Lifetime < storage.OwnerReference || req.OwnerOptions.Lifetime > storage.OwnerExplicit {
			return syscall.EINVAL
		}
		return req.Scope.Check()
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
	case storage.OpFileSetPendingUnlink:
		return req.Pending.Check()
	case storage.OpFileClearPendingUnlink:
		return req.ClearPending.Check()
	case storage.OpFileMutate:
		return req.Mutation.storage().Check()
	}
	return nil
}

func checkLinkTarget(kind storage.NodeKind, size int64, target []byte) error {
	if kind == storage.NodeSymlink {
		if len(target) == 0 || len(target) > storage.MaxLinkTargetBytes || size != int64(len(target)) {
			return errors.New("symbolic-link target and captured size disagree")
		}
	} else if len(target) != 0 {
		return errors.New("non-link node carries a link target")
	}
	return nil
}

func nameResultNeedsAttr(kind storage.NameOperation) bool {
	return kind != storage.NameRemove && kind != storage.NameRemoveDir
}
