package smb

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	accessReadData       uint32 = 0x00000001
	accessWriteData      uint32 = 0x00000002
	accessAppend         uint32 = 0x00000004
	accessReadEA         uint32 = 0x00000008
	accessWriteEA        uint32 = 0x00000010
	accessExecute        uint32 = 0x00000020
	accessReadAttr       uint32 = 0x00000080
	accessWriteAttr      uint32 = 0x00000100
	accessDelete         uint32 = 0x00010000
	accessReadCtrl       uint32 = 0x00020000
	accessSync           uint32 = 0x00100000
	accessGenericAll     uint32 = 0x10000000
	accessGenericExecute uint32 = 0x20000000
	accessGenericWrite   uint32 = 0x40000000
	accessGenericRead    uint32 = 0x80000000
	createDirectory      uint32 = 0x00000001
	createNonDirectory   uint32 = 0x00000040
	createDeleteOnClose  uint32 = 0x00001000
	createOpenByFileID   uint32 = 0x00002000
)

type openIntent struct {
	access                         uint32
	read, write, create, exclusive bool
	reset, directory               bool
	use                            storage.UseClaim
	metadataOnly                   bool
}

func normalizeAccess(access uint32) (uint32, error) {
	if access&accessGenericAll != 0 {
		access |= accessReadData | accessWriteData | accessAppend | accessReadEA | accessWriteEA | accessExecute | accessReadAttr | accessWriteAttr | accessDelete | accessReadCtrl | accessSync
	}
	if access&accessGenericRead != 0 {
		access |= accessReadData | accessReadEA | accessReadAttr | accessReadCtrl | accessSync
	}
	if access&accessGenericWrite != 0 {
		access |= accessWriteData | accessAppend | accessWriteEA | accessWriteAttr | accessReadCtrl | accessSync
	}
	if access&accessGenericExecute != 0 {
		access |= accessExecute | accessReadAttr | accessReadCtrl | accessSync
	}
	access &^= accessGenericAll | accessGenericRead | accessGenericWrite | accessGenericExecute
	const supported = accessReadData | accessWriteData | accessAppend | accessReadEA | accessWriteEA | accessExecute | accessReadAttr | accessWriteAttr | accessDelete | accessReadCtrl | accessSync
	if access&^supported != 0 {
		return 0, syscall.EOPNOTSUPP
	}
	return access, nil
}

func classifyCreate(request wire.CreateRequest) (openIntent, error) {
	var intent openIntent
	if request.SecurityFlags != 0 || request.ShareAccess&^uint32(7) != 0 || request.Options&createDirectory != 0 && request.Options&createNonDirectory != 0 {
		return intent, syscall.EINVAL
	}
	if request.Options&(createDeleteOnClose|createOpenByFileID) != 0 || request.Options&^(createDirectory|createNonDirectory|0x20) != 0 {
		return intent, syscall.EOPNOTSUPP
	}
	if request.OplockLevel != 0 && request.OplockLevel != 1 && request.OplockLevel != 8 && request.OplockLevel != 9 && request.OplockLevel != 0xff || request.Impersonation > 3 || !validDOSAttributes(request.Attributes) {
		return intent, syscall.EOPNOTSUPP
	}
	if err := validateCreateContexts(request); err != nil {
		return intent, err
	}
	access, err := normalizeAccess(request.DesiredAccess)
	if err != nil {
		return intent, err
	}
	intent.access = access
	intent.directory = request.Options&createDirectory != 0
	intent.read = access&(accessReadData|accessExecute) != 0
	intent.write = access&(accessWriteData|accessAppend) != 0
	intent.metadataOnly = !intent.read && !intent.write
	switch request.Disposition {
	case 1: // FILE_OPEN
	case 2: // FILE_CREATE
		intent.create, intent.exclusive = true, true
	case 3: // FILE_OPEN_IF
		intent.create = true
	case 4: // FILE_OVERWRITE
		intent.reset = true
	case 5: // FILE_OVERWRITE_IF
		intent.create, intent.reset = true, true
	default:
		return intent, syscall.EOPNOTSUPP
	}
	if intent.reset && (!intent.write || intent.directory) || intent.directory && request.Disposition >= 4 || intent.metadataOnly && intent.reset {
		return intent, syscall.EACCES
	}
	if intent.directory && intent.read {
		intent.use.Uses = storage.ReadEntries
	} else {
		if intent.read {
			intent.use.Uses |= storage.ReadData
		}
		if intent.write {
			intent.use.Uses |= storage.WriteData
		}
	}
	if access&accessDelete != 0 {
		intent.use.Uses |= storage.DeleteName
	}
	if request.ShareAccess&1 == 0 {
		intent.use.Deny |= storage.ReadData | storage.ReadEntries
	}
	if request.ShareAccess&2 == 0 {
		intent.use.Deny |= storage.WriteData
	}
	if request.ShareAccess&4 == 0 {
		intent.use.Deny |= storage.DeleteName
	}
	return intent, nil
}

func (c *connection) authorizeOpen(ctx context.Context, t *tree, operation storage.Operation, intent openIntent, initial storage.InitialState) error {
	authorize := func(operation storage.Operation, access storage.OpenAccess) error {
		return c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation, Open: access})
	}
	if err := authorize(operation, storage.OpenAccess{Read: intent.read || intent.access&accessReadAttr != 0, Write: intent.write || intent.access&accessWriteAttr != 0, Create: intent.create, Exclusive: intent.exclusive, Truncate: intent.reset}); err != nil {
		return err
	}
	if len(initial.OnCreate.Metadata)+len(initial.OnReset.Metadata) != 0 {
		if err := authorize(storage.OpFileSetMetadata, storage.OpenAccess{}); err != nil {
			return err
		}
	}
	return nil
}

func (c *connection) createFile(ctx context.Context, s *session, t *tree, request wire.Request) ([]byte, uint32, wire.FileID) {
	parsed, err := request.Create()
	if err != nil {
		return nil, statusInvalid, wire.FileID{}
	}
	intent, err := classifyCreate(parsed)
	if err != nil {
		return nil, createStatusError(err), wire.FileID{}
	}
	if !t.beginFileWork(s) {
		return nil, statusNetworkDeleted, wire.FileID{}
	}
	defer t.endFileWork()
	if err := checkCreateCapabilities(t.authority.raw); err != nil {
		return nil, createStatusError(err), wire.FileID{}
	}
	handle, err := t.reserveFileHandle(s, c.server.config.Limits.MaxHandles)
	if err != nil {
		return nil, createStatusError(err), wire.FileID{}
	}
	releaseReservation := func() { t.releaseFileHandle(handle) }
	resolved, err := resolveCreateName(ctx, t, parsed.Name)
	if err != nil {
		releaseReservation()
		return nil, namespaceStatus(err), wire.FileID{}
	}
	if resolved.Root && (intent.create || intent.reset || parsed.Options&createNonDirectory != 0) {
		releaseReservation()
		return nil, createStatusError(syscall.EISDIR), wire.FileID{}
	}
	if resolved.DirectoryRequired && parsed.Options&createNonDirectory != 0 {
		releaseReservation()
		return nil, createStatusError(syscall.ENOTDIR), wire.FileID{}
	}
	if resolved.Attr == nil && !intent.create && !resolved.Root {
		releaseReservation()
		return nil, createStatusError(syscall.ENOENT), wire.FileID{}
	}
	if resolved.Attr != nil && intent.exclusive {
		releaseReservation()
		return nil, createStatusError(syscall.EEXIST), wire.FileID{}
	}
	isDirectory := resolved.Root || resolved.Attr != nil && resolved.Attr.IsDir() || resolved.Attr == nil && (intent.directory || resolved.DirectoryRequired)
	if intent.directory && resolved.Attr != nil && !resolved.Attr.IsDir() || parsed.Options&createNonDirectory != 0 && isDirectory {
		releaseReservation()
		return nil, createStatusError(syscall.ENOTDIR), wire.FileID{}
	}
	if isDirectory && intent.reset {
		releaseReservation()
		return nil, createStatusError(syscall.EISDIR), wire.FileID{}
	}
	if isDirectory && intent.read {
		intent.use.Uses &^= storage.ReadData
		intent.use.Uses |= storage.ReadEntries
	}
	initial := storage.InitialState{}
	if intent.create {
		attributes := parsed.Attributes &^ dosNormal
		if !isDirectory {
			attributes |= dosArchive
		}
		initial.OnCreate.Metadata, err = withWindowsMetadata(nil, windowsMetadata{Attributes: attributes})
		if err != nil {
			releaseReservation()
			return nil, createStatusError(err), wire.FileID{}
		}
	}
	if resolved.Attr != nil {
		attributes, projectionErr := projectWindowsAttributes(*resolved.Attr)
		if projectionErr != nil {
			releaseReservation()
			return nil, createStatusError(projectionErr), wire.FileID{}
		}
		if attributes&dosReadOnly != 0 && (intent.write || intent.reset || intent.access&accessDelete != 0) {
			releaseReservation()
			return nil, createStatusError(syscall.EACCES), wire.FileID{}
		}
		if intent.reset {
			initial.OnReset.Metadata, err = withWindowsMetadata(nil, windowsMetadata{Attributes: (attributes | dosArchive) &^ dosNormal})
			if err != nil {
				releaseReservation()
				return nil, createStatusError(err), wire.FileID{}
			}
		}
	}
	var operation storage.Operation
	if isDirectory || intent.metadataOnly {
		operation = storage.OpFileOpenChildRef
	} else {
		operation = storage.OpFileOpenAt
	}
	if resolved.Root {
		operation = storage.OpFileOpenNodeRef
		resolved.Condition = storage.ChildCondition{State: storage.SameNode, NodeID: resolved.RootID}
	}
	if err = c.authorizeOpen(ctx, t, operation, intent, initial); err != nil {
		releaseReservation()
		return nil, createStatusError(err), wire.FileID{}
	}
	t.authority.installMu.RLock()
	valid := !t.authority.stopping && !t.authority.closed && t.authority.actionEpoch != 0
	epoch := t.authority.actionEpoch
	t.authority.installMu.RUnlock()
	if !valid {
		releaseReservation()
		return nil, statusSessionDeleted, wire.FileID{}
	}
	action, err := storage.NewFileActionID(epoch)
	if err != nil {
		releaseReservation()
		return nil, createStatusError(err), wire.FileID{}
	}
	handle.action, handle.access = action, intent.access
	var attr storage.Attr
	var outcome storage.OpenOutcome
	var open func(context.Context) (storage.Attr, storage.OpenOutcome, error)
	if resolved.Root || isDirectory || intent.metadataOnly {
		references := t.authority.raw.(storage.NodeReferences)
		options := storage.NodeRefOptions{Kind: storage.NodeRegular, Target: resolved.Condition, Action: action, Use: intent.use,
			MetadataAccess: storage.ReadMetadata, Create: intent.create, Exclusive: intent.exclusive, InitialState: initial}
		if intent.access&(accessWriteEA|accessWriteAttr) != 0 {
			options.MetadataAccess |= storage.WriteMetadata
		}
		if isDirectory {
			options.Kind = storage.NodeDirectory
		}
		selection := resolved.Selection.Clone()
		root, rootID := resolved.Root, resolved.RootID
		open = func(call context.Context) (storage.Attr, storage.OpenOutcome, error) {
			call = storage.WithBoundedAttrResult(call, int64(c.server.config.Limits.MaxFrameBytes), createAttrBudget(int64(c.server.config.Limits.MaxFrameBytes)))
			var opened storage.NodeOpenResult
			var openErr error
			if root {
				opened, openErr = references.OpenNodeRef(call, rootID, options)
			} else {
				opened, openErr = references.OpenChildRef(call, selection, options)
			}
			handle.node = opened.Reference
			return opened.Attr, opened.Outcome, openErr
		}
	} else {
		options := storage.OpenAtOptions{Read: intent.read, Write: intent.write, Create: intent.create, Exclusive: intent.exclusive,
			Target: resolved.Condition, Action: action, Use: intent.use, Existing: storage.Keep, Initial: initial}
		if intent.reset {
			options.Existing = storage.ResetContent
		}
		selection := resolved.Selection.Clone()
		open = func(call context.Context) (storage.Attr, storage.OpenOutcome, error) {
			call = storage.WithBoundedAttrResult(call, int64(c.server.config.Limits.MaxFrameBytes), createAttrBudget(int64(c.server.config.Limits.MaxFrameBytes)))
			opened, openErr := t.authority.raw.(storage.AtomicFileOpener).OpenAt(call, selection, options)
			handle.file = opened.File
			return opened.Attr, opened.Outcome, openErr
		}
	}
	attr, outcome, err = open(ctx)
	if handle.file == nil && handle.node == nil {
		handle.pendingOpen = c.pendingOpenRecovery(t, handle, operation, intent, initial, open)
	}
	if err != nil {
		return nil, c.finishFailedOpen(ctx, t, handle, err), wire.FileID{}
	}
	if handle.file == nil && handle.node == nil {
		return nil, c.finishFailedOpen(ctx, t, handle, syscall.EIO), wire.FileID{}
	}
	metadata, err := projectCreateMetadata(attr)
	if err != nil {
		return nil, c.finishFailedOpen(ctx, t, handle, err), wire.FileID{}
	}
	if outcome < storage.Opened || outcome > storage.Reset {
		return nil, c.finishFailedOpen(ctx, t, handle, syscall.EIO), wire.FileID{}
	}
	response := wire.CreateResponse{CreateAction: uint32(outcome), CreationTime: metadata.CreationTime,
		LastAccessTime: metadata.LastAccessTime, LastWriteTime: metadata.LastWriteTime, ChangeTime: metadata.ChangeTime,
		EndOfFile: metadata.EndOfFile, Attributes: metadata.Attributes, FileID: handle.id}
	return wire.CreateResponseBody(response), statusOK, handle.id
}

func checkCreateCapabilities(session storage.FileSession) error {
	if err := checkCreateNameCapability(session); err != nil {
		return err
	}
	opener, ok := session.(storage.AtomicFileOpener)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := opener.CheckAtomicFileOpen(); err != nil {
		return err
	}
	references, ok := session.(storage.NodeReferences)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := references.CheckNodeReferences(); err != nil {
		return err
	}
	actions, ok := session.(storage.FileActions)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return actions.CheckFileActions()
}

func (c *connection) pendingOpenRecovery(t *tree, handle *fileHandle, operation storage.Operation, intent openIntent, initial storage.InitialState, open func(context.Context) (storage.Attr, storage.OpenOutcome, error)) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		volume := t.export.share.Volume
		if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: volume, Operation: storage.OpFileQueryAction}); err != nil {
			return false, err
		}
		receipt, err := t.authority.raw.(storage.FileActions).QueryFileAction(ctx, handle.action)
		if err != nil {
			return false, err
		}
		if receipt.Check() != nil || receipt.Action != handle.action || receipt.Operation != "" && receipt.Operation != operation {
			return false, syscall.EIO
		}
		if receipt.Outcome == storage.FileActionNotExecuted && receipt.Operation == operation {
			return true, nil
		}
		if receipt.Outcome == storage.FileActionRetired || receipt.Operation == "" && receipt.Outcome != storage.FileActionNotExecuted {
			return false, syscall.EIO
		}
		if err := c.authorizeOpen(ctx, t, operation, intent, initial); err != nil {
			return false, err
		}
		_, _, err = open(ctx)
		if handle.file == nil && handle.node == nil {
			if err != nil {
				if authErr := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: volume, Operation: storage.OpFileQueryAction}); authErr == nil {
					if final, queryErr := t.authority.raw.(storage.FileActions).QueryFileAction(ctx, handle.action); queryErr == nil &&
						final.Check() == nil && final.Action == handle.action && final.Operation == operation && final.Outcome == storage.FileActionNotExecuted {
						return true, err
					}
				}
			}
			if err == nil {
				err = syscall.EIO
			}
			return false, err
		}
		return false, err
	}
}

func (c *connection) finishFailedOpen(ctx context.Context, t *tree, handle *fileHandle, openErr error) uint32 {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.server.config.Limits.CleanupTimeout)
	defer cancel()
	if closeErr := t.closeFileHandle(cleanup, handle); closeErr != nil {
		c.server.cleanupFailure(closeErr)
	}
	return createStatusError(openErr)
}

func (c *connection) closeFile(ctx context.Context, s *session, t *tree, request wire.Request, inherited wire.FileID) ([]byte, uint32) {
	parsed, err := request.Close()
	if err != nil || parsed.Flags&^uint16(1) != 0 {
		return nil, statusInvalid
	}
	if parsed.FileID == wire.InvalidFileID && request.Header.Flags&wire.FlagRelated != 0 {
		parsed.FileID = inherited
	}
	if parsed.FileID == (wire.FileID{}) || parsed.FileID == wire.InvalidFileID {
		return nil, closeStatusError(syscall.EBADF)
	}
	if !t.beginFileWork(s) {
		return nil, statusNetworkDeleted
	}
	defer t.endFileWork()
	handle := t.findFileHandle(parsed.FileID)
	if handle == nil {
		return nil, closeStatusError(syscall.EBADF)
	}
	if err := handle.requestCloseMu.lock(ctx); err != nil {
		return nil, closeStatusError(err)
	}
	defer handle.requestCloseMu.unlock()
	if t.findFileHandle(parsed.FileID) != handle {
		return nil, closeStatusError(syscall.EBADF)
	}
	if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: storage.OpFileClose}); err != nil {
		return nil, closeStatusError(err)
	}
	response := wire.CloseResponse{}
	if parsed.Flags&1 != 0 {
		if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: storage.OpFileStat}); err == nil {
			var attr storage.Attr
			if handle.file != nil {
				attr, err = handle.file.Stat(ctx)
			} else if handle.node != nil {
				attr, err = handle.node.Stat(ctx)
			}
			if err == nil {
				var metadata createMetadata
				metadata, err = projectCreateMetadata(attr)
				if err == nil {
					response.Flags = 1
					response.CreationTime, response.LastAccessTime, response.LastWriteTime, response.ChangeTime = metadata.CreationTime, metadata.LastAccessTime, metadata.LastWriteTime, metadata.ChangeTime
					response.EndOfFile, response.Attributes = metadata.EndOfFile, metadata.Attributes
				}
			}
		}
	}
	if err := t.closeFileHandle(ctx, handle); err != nil {
		return nil, closeStatusError(err)
	}
	return wire.CloseResponseBody(response), statusOK
}
