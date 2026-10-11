package smb

import (
	"bytes"
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
	createSyncAlert      uint32 = 0x00000010
	createSyncNonAlert   uint32 = 0x00000020
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
	metadata                       storage.MetadataPermissions
	attributes                     uint32
	contentEffects                 []storage.ContentMetadataEffect
}

func normalizeAccess(access uint32) (uint32, error) {
	if access&accessGenericAll != 0 {
		// FILE_ALL_ACCESS includes rights outside the supported access set. Keep
		// them in the expansion so unsupported requests are refused as a whole.
		// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/77b36d0f-6016-458a-a7a0-0f4a72ae1534
		access |= 0x001f01ff
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
	if request.ShareAccess&^uint32(7) != 0 || request.Options&createDirectory != 0 && request.Options&createNonDirectory != 0 {
		return intent, syscall.EINVAL
	}
	if request.Options&(createDeleteOnClose|createOpenByFileID) != 0 || request.Options&^(createDirectory|createNonDirectory|createSyncAlert|createSyncNonAlert) != 0 {
		return intent, syscall.EOPNOTSUPP
	}
	if request.OplockLevel != 0 && request.OplockLevel != 1 && request.OplockLevel != 8 && request.OplockLevel != 9 && request.OplockLevel != 0xff || request.Impersonation > 3 {
		return intent, syscall.EOPNOTSUPP
	}
	attributes, err := normalizeCreateAttributes(request.Attributes)
	if err != nil {
		return intent, err
	}
	intent.attributes = attributes
	if err := validateCreateContexts(request); err != nil {
		return intent, err
	}
	access, err := normalizeAccess(request.DesiredAccess)
	if err != nil {
		return intent, err
	}
	if request.Options&(createSyncAlert|createSyncNonAlert) == createSyncAlert|createSyncNonAlert {
		return intent, syscall.EINVAL
	}
	if request.Options&(createSyncAlert|createSyncNonAlert) != 0 && access&accessSync == 0 {
		return intent, syscall.EACCES
	}
	intent.access = access
	intent.directory = request.Options&createDirectory != 0
	intent.read = access&accessReadData != 0
	intent.write = access&(accessWriteData|accessAppend) != 0
	intent.metadataOnly = !intent.read && !intent.write
	if access&(accessReadEA|accessReadAttr) != 0 {
		intent.metadata |= storage.ReadMetadata
	}
	if access&(accessWriteEA|accessWriteAttr) != 0 {
		intent.metadata |= storage.WriteMetadata
	}
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
	if access&(accessReadData|accessExecute) != 0 {
		if intent.directory {
			intent.use.Uses |= storage.ReadEntries
		} else {
			intent.use.Uses |= storage.ReadData
		}
	}
	if intent.write {
		intent.use.Uses |= storage.WriteData
	}
	if access&accessDelete != 0 {
		intent.use.Uses |= storage.DeleteName
	}
	if intent.use.Uses != 0 && request.ShareAccess&1 == 0 {
		intent.use.Deny |= storage.ReadData | storage.ReadEntries
	}
	if intent.use.Uses != 0 && request.ShareAccess&2 == 0 {
		intent.use.Deny |= storage.WriteData
	}
	if intent.use.Uses != 0 && request.ShareAccess&4 == 0 {
		intent.use.Deny |= storage.DeleteName
	}
	return intent, nil
}

func (c *connection) authorizeOpen(ctx context.Context, t *tree, operation storage.Operation, intent openIntent, initial storage.InitialState) error {
	authorize := func(operation storage.Operation, access storage.OpenAccess, effects []storage.ContentMetadataEffect) error {
		request := authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation, Open: access,
			ContentMetadataEffects: storage.CloneContentMetadataEffects(effects)}
		return c.server.config.Authorize.Authorize(ctx, request.Clone())
	}
	if err := authorize(operation, storage.OpenAccess{Read: intent.read || intent.access&accessExecute != 0 || intent.metadata&storage.ReadMetadata != 0, Write: intent.write || intent.metadata&storage.WriteMetadata != 0, Create: intent.create, Exclusive: intent.exclusive, Truncate: intent.reset, ContentMetadataEffects: storage.CloneContentMetadataEffects(intent.contentEffects)}, nil); err != nil {
		return err
	}
	if len(initial.OnCreate.Metadata)+len(initial.OnReset.Metadata) != 0 {
		if err := authorize(storage.OpFileSetMetadata, storage.OpenAccess{}, nil); err != nil {
			return err
		}
	}
	if len(intent.contentEffects) != 0 {
		if err := authorize(storage.OpFileObserveContentMetadata, storage.OpenAccess{}, intent.contentEffects); err != nil {
			return err
		}
		if err := authorize(storage.OpFileSetMetadata, storage.OpenAccess{}, intent.contentEffects); err != nil {
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
	if resolved.Root {
		if intent.exclusive {
			releaseReservation()
			return nil, createStatusError(syscall.EEXIST), wire.FileID{}
		}
		if intent.reset || parsed.Options&createNonDirectory != 0 {
			releaseReservation()
			return nil, createStatusError(syscall.EISDIR), wire.FileID{}
		}
		intent.create = false
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
	if isDirectory && intent.access&(accessReadData|accessExecute) != 0 {
		intent.use.Uses &^= storage.ReadData
		intent.use.Uses |= storage.ReadEntries
	}
	initial := storage.InitialState{}
	if intent.create {
		attributes := intent.attributes
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
		resolved.Condition.ExpectedMetadata = map[string][]byte{windowsMetadataKey: bytes.Clone(resolved.Attr.Metadata[windowsMetadataKey].Version)}
		attributes, projectionErr := projectWindowsAttributes(*resolved.Attr)
		if projectionErr != nil {
			releaseReservation()
			return nil, createStatusError(projectionErr), wire.FileID{}
		}
		if !isDirectory && attributes&dosReadOnly != 0 && intent.write {
			releaseReservation()
			return nil, createStatusError(syscall.EACCES), wire.FileID{}
		}
		if intent.reset {
			if attributes&(dosHidden|dosSystem)&^intent.attributes != 0 {
				releaseReservation()
				return nil, createStatusError(syscall.EACCES), wire.FileID{}
			}
			initial.OnReset.Metadata, err = withWindowsMetadata(nil, windowsMetadata{Attributes: intent.attributes | dosArchive})
			if err != nil {
				releaseReservation()
				return nil, createStatusError(err), wire.FileID{}
			}
		}
	}
	if !isDirectory && !intent.metadataOnly && intent.write {
		intent.contentEffects = windowsContentEffects()
	}
	var operation storage.Operation
	if isDirectory || intent.metadataOnly {
		operation = storage.OpFileOpenChildRef
	} else {
		operation = storage.OpFileOpenAt
	}
	if resolved.Root {
		operation = storage.OpFileOpenNodeRef
		resolved.Condition.State, resolved.Condition.NodeID = storage.SameNode, resolved.RootID
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
	handle.contentEffects = storage.CloneContentMetadataEffects(intent.contentEffects)
	handle.recordOpenDiagnostic()
	var attr storage.Attr
	var outcome storage.OpenOutcome
	var open func(context.Context) (storage.Attr, storage.OpenOutcome, error)
	if resolved.Root || isDirectory || intent.metadataOnly {
		references := t.authority.raw.(storage.NodeReferences)
		options := storage.NodeRefOptions{Kind: storage.NodeRegular, Target: resolved.Condition, Action: action, Use: intent.use,
			MetadataAccess: intent.metadata, Create: intent.create, Exclusive: intent.exclusive, InitialState: initial}
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
		options := storage.OpenAtOptions{Read: intent.read, Write: intent.write, MetadataAccess: intent.metadata, Create: intent.create, Exclusive: intent.exclusive,
			Target: resolved.Condition, Action: action, Use: intent.use, Existing: storage.Keep, Initial: initial, ContentMetadataEffects: storage.CloneContentMetadataEffects(intent.contentEffects)}
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
		handle.pendingOpen = c.pendingOpenRecovery(t, handle, operation, open)
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
	if !validCreateOutcome(parsed.Disposition, outcome) || (attr.IsDir() != isDirectory) || attr.Kind == storage.NodeSymlink || outcome == storage.Created && (attr.BirthTime == nil || attr.ChangeTime == nil) || outcome == storage.Reset && attr.ChangeTime == nil {
		return nil, c.finishFailedOpen(ctx, t, handle, syscall.EIO), wire.FileID{}
	}
	state, stateErr := t.authority.raw.Status(ctx)
	if stateErr != nil {
		return nil, c.finishFailedOpen(ctx, t, handle, stateErr), wire.FileID{}
	}
	if state.Epoch != t.authority.identity.SessionEpoch || state.Remaining <= 0 || state.Retired || state.Fenced {
		return nil, c.finishFailedOpen(ctx, t, handle, syscall.ESTALE), wire.FileID{}
	}
	if handle.file != nil {
		mutation, ok := handle.file.(storage.ConditionalFileMutation)
		if !ok {
			return nil, c.finishFailedOpen(ctx, t, handle, syscall.EOPNOTSUPP), wire.FileID{}
		}
		if err := mutation.CheckConditionalFileMutation(); err != nil {
			return nil, c.finishFailedOpen(ctx, t, handle, err), wire.FileID{}
		}
		if len(intent.contentEffects) != 0 {
			content, ok := handle.file.(storage.ReferenceContentMetadata)
			if !ok {
				return nil, c.finishFailedOpen(ctx, t, handle, syscall.EOPNOTSUPP), wire.FileID{}
			}
			if err := content.CheckContentMetadata(); err != nil {
				return nil, c.finishFailedOpen(ctx, t, handle, err), wire.FileID{}
			}
		}
	}
	if err := handle.bindReference(ctx, t, attr, intent.use); err != nil {
		return nil, c.finishFailedOpen(ctx, t, handle, err), wire.FileID{}
	}
	response := wire.CreateResponse{CreateAction: uint32(outcome), CreationTime: metadata.CreationTime,
		LastAccessTime: metadata.LastAccessTime, LastWriteTime: metadata.LastWriteTime, ChangeTime: metadata.ChangeTime,
		AllocationSize: metadata.AllocationSize, EndOfFile: metadata.EndOfFile, Attributes: metadata.Attributes, FileID: handle.id}
	return wire.CreateResponseBody(response), statusOK, handle.id
}

func validCreateOutcome(disposition uint32, outcome storage.OpenOutcome) bool {
	switch disposition {
	case 1:
		return outcome == storage.Opened
	case 2:
		return outcome == storage.Created
	case 3:
		return outcome == storage.Opened || outcome == storage.Created
	case 4:
		return outcome == storage.Reset
	case 5:
		return outcome == storage.Created || outcome == storage.Reset
	default:
		return false
	}
}

func checkCreateCapabilities(session storage.FileSession) error {
	content, ok := session.(storage.OpenContentMetadata)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := content.CheckOpenContentMetadata(); err != nil {
		return err
	}
	identity, ok := session.(storage.FileSessionIdentity)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := identity.CheckFileSessionIdentity(); err != nil {
		return err
	}
	stable, ok := session.(storage.StableReferenceIdentity)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := stable.CheckStableReferenceIdentity(); err != nil {
		return err
	}
	metadata, ok := session.(storage.OpenMetadataAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := metadata.CheckOpenMetadataAccess(); err != nil {
		return err
	}
	recovery, ok := session.(storage.RecoverableReferenceClose)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := recovery.CheckRecoverableReferenceClose(); err != nil {
		return err
	}
	reporter, ok := session.(storage.AllocationReporting)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := reporter.CheckAllocationReporting(); err != nil {
		return err
	}
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

func (c *connection) pendingOpenRecovery(t *tree, handle *fileHandle, operation storage.Operation, open func(context.Context) (storage.Attr, storage.OpenOutcome, error)) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
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
		_, _, err = open(ctx)
		if handle.file == nil && handle.node == nil {
			if err != nil {
				if final, queryErr := t.authority.raw.(storage.FileActions).QueryFileAction(ctx, handle.action); queryErr == nil &&
					final.Check() == nil && final.Action == handle.action && final.Operation == operation && final.Outcome == storage.FileActionNotExecuted {
					return true, err
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
