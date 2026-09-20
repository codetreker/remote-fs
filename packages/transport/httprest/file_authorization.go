package httprest

import (
	"context"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (h *Handler) authorizeFile(ctx context.Context, request fileRequest) error {
	accesses := []authz.AccessRequest{{Operation: request.Op}}
	appendOperation := func(operation storage.Operation) {
		for _, access := range accesses {
			if access.Operation == operation && access.Open == (storage.OpenAccess{}) {
				return
			}
		}
		accesses = append(accesses, authz.AccessRequest{Operation: operation})
	}

	switch request.Op {
	case storage.OpFileOpen, storage.OpFileOpenNode:
		accesses[0].Open = request.Open.OpenAccess
	case storage.OpFileOpenAt:
		options := request.OpenAt.storage()
		accesses[0].Open = storage.OpenAccess{Read: options.Read, Write: options.Write, Create: options.Create, Exclusive: options.Exclusive, Truncate: options.Existing == storage.ResetContent}
		if options.Existing == storage.ReplaceNode {
			appendOperation(storage.OpVolumeRemove)
		}
		if !options.Initial.OnCreate.Attr.Empty() || !options.Initial.OnReset.Attr.Empty() || !options.Initial.OnReplace.Attr.Empty() {
			appendOperation(storage.OpFileSetAttr)
		}
		if len(options.Initial.OnCreate.Metadata)+len(options.Initial.OnReset.Metadata)+len(options.Initial.OnReplace.Metadata) != 0 {
			appendOperation(storage.OpFileSetMetadata)
		}
		if options.CloseIntent != nil {
			appendOperation(storage.OpFileSetPendingUnlink)
		}
	case storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		options := request.NodeRef.storage()
		accesses[0].Open = storage.OpenAccess{
			Read:      options.MetadataAccess&storage.ReadMetadata != 0 || options.Use.Uses&(storage.ReadData|storage.ReadEntries) != 0,
			Write:     options.MetadataAccess&storage.WriteMetadata != 0 || options.Use.Uses&storage.WriteData != 0,
			Create:    options.Create,
			Exclusive: options.Exclusive,
		}
		if len(options.InitialState.OnCreate.Metadata) != 0 {
			appendOperation(storage.OpFileSetMetadata)
		}
		if !options.InitialState.OnCreate.Attr.Empty() {
			appendOperation(storage.OpFileSetAttr)
		}
		if options.CloseIntent != nil {
			appendOperation(storage.OpFileSetPendingUnlink)
		}
	case storage.OpFileMutateName:
		switch request.Name.Kind {
		case storage.NameCreate, storage.NameSymlink:
			appendOperation(storage.OpVolumeCreate)
		case storage.NameMkdir:
			appendOperation(storage.OpVolumeMkdir)
		case storage.NameRemove:
			appendOperation(storage.OpVolumeRemove)
		case storage.NameRemoveDir:
			appendOperation(storage.OpVolumeRemoveDir)
		case storage.NameRename:
			appendOperation(storage.OpVolumeRename)
		}
		if !request.Name.Initial.Attr.Storage().Empty() {
			appendOperation(storage.OpFileSetAttr)
		}
		if len(request.Name.Initial.Metadata) != 0 {
			appendOperation(storage.OpFileSetMetadata)
		}
	case storage.OpFileMutate:
		mutation := request.Mutation.storage()
		switch mutation.Kind {
		case storage.MutateWriteAt, storage.MutateAppend:
			appendOperation(storage.OpFileWrite)
		case storage.MutateTruncate:
			appendOperation(storage.OpFileTruncate)
		}
		if !mutation.Attr.Empty() {
			appendOperation(storage.OpFileSetAttr)
		}
		if len(mutation.Metadata) != 0 {
			appendOperation(storage.OpFileSetMetadata)
		}
	}

	for _, access := range accesses {
		if err := h.authorize(ctx, access); err != nil {
			return err
		}
	}
	return nil
}
