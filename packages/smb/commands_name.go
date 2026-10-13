package smb

import (
	"bytes"
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

var errCannotDelete = errors.New("file is read-only")

func nameCommandStatus(err error) uint32 {
	if errors.Is(err, errCannotDelete) {
		return 0xc0000121
	}
	if errors.Is(err, syscall.ENOTEMPTY) {
		return statusDirectoryNotEmpty
	}
	return createStatusError(err)
}

func windowsNameCondition(attr storage.Attr) storage.ChildCondition {
	var token []byte
	if value, present := attr.Metadata[windowsMetadataKey]; present {
		token = bytes.Clone(value.Version)
	}
	return storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID, ExpectedMetadata: map[string][]byte{windowsMetadataKey: token}}
}

func (c *connection) nameTargetUse(ctx context.Context, s *session, t *tree, h *fileHandle) ([]storage.TargetUse, error) {
	if err := c.authorizeReference(ctx, s, t, h, storage.OpFileScope, nil); err != nil {
		return nil, err
	}
	scoped, ok := h.reference().(storage.ScopedReference)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := scoped.CheckScopedReference(); err != nil {
		return nil, err
	}
	scope, err := scoped.Scope(ctx)
	if err != nil {
		return nil, err
	}
	if scope.Check() != nil {
		return nil, syscall.EIO
	}
	if err := checkFileIOIdentity(ctx, s, t, h); err != nil {
		return nil, err
	}
	return []storage.TargetUse{{NodeID: h.nodeID, Scope: scope}}, nil
}

func (c *connection) buildRenameCommand(ctx context.Context, s *session, t *tree, h *fileHandle, info wire.FileRenameInfo, action storage.FileActionID, uses []storage.TargetUse) (mutationCommand, func(), error) {
	source, err := c.captureReferenceName(ctx, s, t, h)
	if err != nil {
		return mutationCommand{}, nil, err
	}
	fail := func(err error) (mutationCommand, func(), error) { source.release(); return mutationCommand{}, nil, err }
	if source.binding.State != storage.NameLinked {
		return fail(syscall.EACCES)
	}
	destination, err := resolveCreateNameCaptured(ctx, t, info.Name, source)
	if err != nil {
		return fail(err)
	}
	if destination.Root {
		return fail(syscall.EACCES)
	}
	path, err := parseSMBPath(info.Name)
	if err != nil || len(path.components) == 0 {
		return fail(syscall.EINVAL)
	}
	if destination.Attr != nil && destination.Attr.ID != h.nodeID {
		if !info.ReplaceIfExists {
			return fail(syscall.EEXIST)
		}
		attributes, err := projectWindowsAttributes(*destination.Attr)
		if err != nil {
			return fail(err)
		}
		if destination.Attr.IsDir() || attributes&dosReadOnly != 0 {
			return fail(syscall.EACCES)
		}
	}
	unionBytes := 1024
	for _, guard := range []*storage.NamespaceGuards{&source.guards, destination.Selection.Guards} {
		for _, directory := range guard.Directories {
			unionBytes += 128 + 8*len(directory.Revision)
		}
		for _, edge := range guard.Edges {
			unionBytes += 128 + 8*len(edge.RawLeaf)
		}
	}
	if err := source.reserve(unionBytes); err != nil {
		return fail(err)
	}
	guards, err := unionNamespaceGuards(&source.guards, destination.Selection.Guards)
	if err != nil {
		return fail(err)
	}
	if err = c.validateReferenceName(ctx, s, t, h, source, guards); err != nil {
		return fail(err)
	}
	command := storage.NameCommand{Kind: storage.NameRename, Action: action,
		Name:   storage.ChildName{Parent: storage.DirectoryTarget{NodeID: source.binding.ParentID}, RawLeaf: bytes.Clone(source.binding.RawLeaf)},
		Target: windowsNameCondition(source.attr), Uses: uses, Guards: guards, RequireNoOpenDescendants: h.nodeKind == storage.NodeDirectory,
		Destination: &storage.RenameTarget{Parent: destination.Selection.Name.Parent, ObservedLeaf: bytes.Clone(destination.Selection.Name.RawLeaf), Expected: destination.Condition, OutputLeaf: []byte(path.components[len(path.components)-1])}}
	if err = command.Check(); err != nil {
		return fail(err)
	}
	return mutationCommand{name: &command}, source.release, nil
}

func (c *connection) buildDispositionCommand(ctx context.Context, s *session, t *tree, h *fileHandle, deleting bool, action storage.FileActionID, uses []storage.TargetUse) (mutationCommand, func(), error) {
	if deleting {
		source, err := c.captureReferenceName(ctx, s, t, h)
		if err != nil {
			return mutationCommand{}, nil, err
		}
		if source.binding.State != storage.NameLinked {
			source.release()
			return mutationCommand{}, nil, syscall.EACCES
		}
		attributes, err := projectWindowsAttributes(source.attr)
		if err != nil {
			source.release()
			return mutationCommand{}, nil, err
		}
		if attributes&dosReadOnly != 0 {
			source.release()
			return mutationCommand{}, nil, errCannotDelete
		}
		condition := storage.UnlinkFile
		if h.nodeKind == storage.NodeDirectory {
			condition = storage.UnlinkIfEmpty
		}
		command := storage.PendingUnlinkCommand{Action: action, Condition: condition, ExpectedMetadata: windowsNameCondition(source.attr).ExpectedMetadata, Uses: uses}
		return mutationCommand{pending: &command}, source.release, nil
	}
	if err := c.authorizeReference(ctx, s, t, h, storage.OpFileState, nil); err != nil {
		return mutationCommand{}, nil, err
	}
	stateAccess, ok := h.reference().(storage.ReferenceStateAccess)
	if !ok {
		return mutationCommand{}, nil, syscall.EOPNOTSUPP
	}
	if err := stateAccess.CheckReferenceState(); err != nil {
		return mutationCommand{}, nil, err
	}
	state, err := stateAccess.State(ctx, storage.StateScalars)
	if err != nil {
		return mutationCommand{}, nil, err
	}
	if state.CheckProjection(h.nodeID, storage.StateScalars) != nil || state.Attr.Kind != h.nodeKind {
		return mutationCommand{}, nil, syscall.EIO
	}
	command := storage.ClearPendingUnlinkCommand{Action: action, Generation: bytes.Clone(state.PendingGeneration), Uses: uses}
	return mutationCommand{clear: &command}, func() {}, nil
}

func (c *connection) setNameInformation(ctx context.Context, s *session, t *tree, request wire.Request, parsed wire.SetInfoRequest) ([]byte, uint32) {
	var rename wire.FileRenameInfo
	var deleting bool
	var err error
	operation := storage.OpFileMutateName
	if parsed.FileInfoClass == wire.FileRenameInformation {
		rename, err = wire.ParseFileRenameInformation(parsed.Buffer)
	} else {
		if len(parsed.Buffer) == 0 {
			return nil, statusInfoLengthMismatch
		}
		deleting, err = wire.ParseFileDispositionInformation(parsed.Buffer)
		operation = storage.OpFileClearPendingUnlink
		if deleting {
			operation = storage.OpFileSetPendingUnlink
		}
	}
	if err != nil {
		return nil, statusInvalid
	}
	id, err := relatedIOFileID(ctx, request, parsed.FileID)
	if err != nil {
		return nil, ioStatusError(err)
	}
	authorize := func(call context.Context, h *fileHandle) error {
		if err := c.authorizeReference(call, s, t, h, operation, nil); err != nil {
			return err
		}
		if parsed.FileInfoClass == wire.FileRenameInformation {
			return c.authorizeReference(call, s, t, h, storage.OpVolumeRename, nil)
		}
		return nil
	}
	ticket, err := c.admitInformation(ctx, s, t, id, accessDelete, authorize)
	if err != nil {
		return nil, nameCommandStatus(err)
	}
	defer ticket.finish()
	h := ticket.handle
	uses, err := c.nameTargetUse(ctx, s, t, h)
	if err != nil {
		return nil, nameCommandStatus(err)
	}
	epoch := func() uint64 {
		t.authority.installMu.RLock()
		defer t.authority.installMu.RUnlock()
		return t.authority.actionEpoch
	}
	action, err := storage.NewFileActionID(epoch())
	if err != nil {
		return nil, nameCommandStatus(err)
	}
	build := func(call context.Context) (mutationCommand, func(), error) {
		if parsed.FileInfoClass == wire.FileRenameInformation {
			return c.buildRenameCommand(call, s, t, h, rename, action, uses)
		}
		return c.buildDispositionCommand(call, s, t, h, deleting, action, uses)
	}
	command, release, err := build(ctx)
	if err != nil {
		return nil, nameCommandStatus(err)
	}
	owner, err := c.server.reserveMutationCommand(t, h, ticket.sequence, command, nil)
	release()
	if err != nil {
		return nil, nameCommandStatus(err)
	}
	h.pendingWrite = owner
	registerIOResponse(ctx, owner.recordResponse)
	var captures []func()
	defer func() {
		for _, release := range captures {
			release()
		}
	}()
	reobserve := func(call context.Context) (mutationCommand, error) {
		command, release, err := build(call)
		if err == nil {
			captures = append(captures, release)
		}
		return command, err
	}
	owner.resultByteLimit = int64(c.server.config.Limits.MaxFrameBytes)
	_, err = owner.executeCommand(ctx, func(call context.Context) error { return authorize(call, h) }, reobserve, epoch)
	if err != nil {
		return nil, nameCommandStatus(err)
	}
	return wire.SetInfoResponseBody(), statusOK
}

func (c *connection) queryNormalizedName(ctx context.Context, s *session, t *tree, request wire.Request, parsed wire.QueryInfoRequest) ([]byte, uint32) {
	id, err := relatedIOFileID(ctx, request, parsed.FileID)
	if err != nil {
		return nil, ioStatusError(err)
	}
	authorize := func(call context.Context, h *fileHandle) error {
		return c.authorizeReference(call, s, t, h, storage.OpFileObserveName, nil)
	}
	ticket, err := c.admitInformation(ctx, s, t, id, 0, authorize)
	if err != nil {
		return nil, nameCommandStatus(err)
	}
	defer ticket.finish()
	if parsed.OutputBufferLength < 8 {
		return wire.InfoLengthMismatchErrorBody(), statusInfoLengthMismatch
	}
	name, err := c.captureReferenceName(ctx, s, t, ticket.handle)
	if err != nil {
		return nil, nameCommandStatus(err)
	}
	defer name.release()
	if err = authorize(ctx, ticket.handle); err != nil {
		return nil, nameCommandStatus(err)
	}
	data, overflow, err := wire.FileNormalizedNameInformationBody(name.path, parsed.OutputBufferLength)
	if err != nil {
		return nil, statusIO
	}
	body, err := wire.QueryInfoResponseBody(data)
	if err != nil {
		return nil, statusIO
	}
	if overflow {
		return body, statusBufferOverflow
	}
	return body, statusOK
}
