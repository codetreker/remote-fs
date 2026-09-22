package httprest

import (
	"github.com/codetreker/remote-fs/packages/storage"
	"math"
	"syscall"
	"unicode/utf8"
)

func validateCapabilityArguments(req fileRequest) error {
	switch req.Op {
	case storage.OpFileScope:
		return nil
	case storage.OpFileState:
		return nil
	case storage.OpFileQueryAction:
		return req.FileAction.Check()
	case storage.OpFileQueryDeleteIntent:
		if err := req.DeleteOwner.Check(); err != nil {
			return err
		}
		return req.DeleteIntent.Check()
	case storage.OpFileListDeleteIntents:
		if err := req.DeleteOwner.Check(); err != nil {
			return err
		}
		if uint64(req.DeleteAfter) > math.MaxInt64 || req.DeleteLimit < 1 || req.DeleteLimit > storage.MaxDeleteIntentPageEntries {
			return syscall.EINVAL
		}
		return nil
	case storage.OpFileAcknowledgeDeleteIntent:
		if req.Acknowledge == nil {
			return syscall.EINVAL
		}
		return req.Acknowledge.storage().Check()
	case storage.OpFileOpenAt:
		if req.Selection == nil || req.OpenAt == nil {
			return syscall.EINVAL
		}
		if err := req.Selection.storage().Check(); err != nil {
			return err
		}
		return req.OpenAt.storage().Check()
	case storage.OpFileOpenNodeRef:
		if req.Node == 0 || req.NodeRef == nil {
			return syscall.EINVAL
		}
		return req.NodeRef.storage().Check()
	case storage.OpFileOpenChildRef:
		if req.Selection == nil || req.NodeRef == nil {
			return syscall.EINVAL
		}
		if err := req.Selection.storage().Check(); err != nil {
			return err
		}
		return req.NodeRef.storage().Check()
	case storage.OpFileLookupAt:
		if req.Child == nil {
			return syscall.EINVAL
		}
		return req.Child.storage().Check()
	case storage.OpFileReadDirNode:
		if req.Directory == nil {
			return syscall.EINVAL
		}
		return req.Directory.Check()
	case storage.OpFileObserveDirectoryMetadata:
		if req.Directory == nil || req.DirectoryMetadata == nil {
			return syscall.EINVAL
		}
		if err := req.Directory.Check(); err != nil {
			return err
		}
		return req.DirectoryMetadata.storage().Check()
	case storage.OpFileObserveName:
		return req.Guards.storage().Check()
	case storage.OpFileMutateName:
		if req.Name == nil {
			return syscall.EINVAL
		}
		return req.Name.storage().Check()
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
	case storage.OpFileSetPendingUnlink:
		if req.Pending == nil {
			return syscall.EINVAL
		}
		return req.Pending.storage().Check()
	case storage.OpFileClearPendingUnlink:
		if req.ClearPending == nil {
			return syscall.EINVAL
		}
		return req.ClearPending.storage().Check()
	case storage.OpFileMutate:
		if req.Mutation == nil {
			return syscall.EINVAL
		}
		return req.Mutation.storage().Check()
	}
	return nil
}
