package httprest

import (
	"context"
	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (h *Handler) authorizeFile(ctx context.Context, req fileRequest) error {
	access := authz.AccessRequest{Operation: req.Op}
	var additional []storage.Operation
	switch req.Op {
	case fileObserveDirectoryMetadata, fileObserveName:
		access.Operation = storage.OpReplicationSnapshot
	case storage.OpFileOpen, storage.OpFileOpenNode:
		access.Open = req.Open.OpenAccess
	case storage.OpFileOpenAt:
		options := req.OpenAt.storage()
		access.Open = storage.OpenAccess{Read: options.Read, Write: options.Write, Create: options.Create, Exclusive: options.Exclusive, Truncate: options.Existing == storage.ResetContent}
		if options.Existing == storage.ReplaceNode {
			additional = append(additional, storage.OpVolumeRemove)
		}
		if options.CloseIntent != nil {
			additional = append(additional, storage.OpFileSetPendingUnlink)
		}
		if options.Existing == storage.ResetContent {
			if !options.Initial.OnReset.Attr.Empty() {
				additional = append(additional, storage.OpFileSetAttr)
			}
			if len(options.Initial.OnReset.Metadata) > 0 {
				additional = append(additional, storage.OpFileSetMetadata)
			}
		}
	case storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		access.Open = storage.OpenAccess{
			Read:      req.NodeRef.MetadataAccess&storage.ReadMetadata != 0 || req.NodeRef.Use.Uses&(storage.ReadData|storage.ReadEntries) != 0,
			Write:     req.NodeRef.MetadataAccess&storage.WriteMetadata != 0 || req.NodeRef.Use.Uses&storage.WriteData != 0,
			Create:    req.NodeRef.Create,
			Exclusive: req.NodeRef.Exclusive,
		}
		if req.NodeRef.CloseIntent != nil {
			additional = append(additional, storage.OpFileSetPendingUnlink)
		}
	case storage.OpFileMutateName:
		switch req.Name.Kind {
		case storage.NameCreate, storage.NameSymlink:
			access.Operation = storage.OpVolumeCreate
		case storage.NameMkdir:
			access.Operation = storage.OpVolumeMkdir
		case storage.NameRemove:
			access.Operation = storage.OpVolumeRemove
		case storage.NameRemoveDir:
			access.Operation = storage.OpVolumeRemoveDir
		case storage.NameRename:
			access.Operation = storage.OpVolumeRename
		}
	case storage.OpFileMutate:
		mutation := req.Mutation.storage()
		if mutation.Kind == storage.MutateWriteAt || mutation.Kind == storage.MutateAppend {
			additional = append(additional, storage.OpFileWrite)
		}
		if mutation.Kind == storage.MutateTruncate {
			additional = append(additional, storage.OpFileTruncate)
		}
		if !mutation.Attr.Empty() {
			additional = append(additional, storage.OpFileSetAttr)
		}
		if len(mutation.Metadata) > 0 {
			additional = append(additional, storage.OpFileSetMetadata)
		}
	}
	if err := h.authorize(ctx, access); err != nil {
		return err
	}
	for _, op := range additional {
		if err := h.authorize(ctx, authz.AccessRequest{Operation: op}); err != nil {
			return err
		}
	}
	return nil
}
