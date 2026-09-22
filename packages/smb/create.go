package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	fileReadData        uint32 = 0x1
	fileWriteData       uint32 = 0x2
	fileAppendData      uint32 = 0x4
	fileWriteEA         uint32 = 0x10
	fileExecute         uint32 = 0x20
	fileWriteAttributes uint32 = 0x100
	fileDelete          uint32 = 0x10000
	fileSynchronize     uint32 = 0x100000

	createDirectory     uint32 = 0x1
	createWriteThrough  uint32 = 0x2
	createNonDirectory  uint32 = 0x40
	createDeleteOnClose uint32 = 0x1000
)

type createPlan struct {
	access       uint32
	share        uint32
	name         resolvedName
	file         *storage.OpenAtOptions
	node         *storage.NodeRefOptions
	outcome      storage.OpenOutcome
	kind         storage.NodeKind
	create       bool
	writeThrough bool
	contexts     []wire.CreateContext
}

type createNameResolver func(
	context.Context,
	*tree,
	string,
	namespaceReferenceRetainer,
	namespaceAuthorizer,
	namespaceActionFactory,
) (resolvedName, error)

type createStatusError struct {
	status uint32
	cause  error
}

func (e *createStatusError) Error() string { return e.cause.Error() }
func (e *createStatusError) Unwrap() error { return e.cause }
func createFailure(status uint32, err error) error {
	return &createStatusError{status: status, cause: err}
}

func createStatus(err error) uint32 {
	if err == nil {
		return statusOK
	}
	var failure *createStatusError
	if errors.As(err, &failure) {
		return failure.status
	}
	switch {
	case errors.Is(err, errNameInvalid), errors.Is(err, errPathMissing):
		return namespaceStatus(err)
	case storage.ErrnoOf(err) == syscall.EIO:
		return statusIO
	case errors.Is(err, storage.ErrUseConflict):
		return statusSharingViolation
	case errors.Is(err, storage.ErrPendingDelete):
		return statusDeletePending
	case errors.Is(err, storage.ErrConditionConflict):
		return statusRetry
	case errors.Is(err, syscall.EEXIST):
		return statusObjectNameCollision
	case errors.Is(err, syscall.EISDIR):
		return statusFileIsADirectory
	case storage.ErrnoOf(err) == syscall.EDQUOT:
		return statusQuotaExceeded
	case storage.ErrnoOf(err) == syscall.ENOSPC:
		return statusDiskFull
	default:
		return namespaceStatus(err)
	}
}

func expandCreateAccess(access uint32) (uint32, error) {
	for _, generic := range []struct{ bit, rights uint32 }{
		{0x80000000, 0x120089},
		{0x40000000, 0x120116},
		{0x20000000, 0x1200a0},
		{0x10000000, 0x1f01ff},
	} {
		if access&generic.bit != 0 {
			access = access&^generic.bit | generic.rights
		}
	}
	if access&^uint32(0x1301ff) != 0 {
		return 0, syscall.EOPNOTSUPP
	}
	return access, nil
}

func validateCreateRequest(request wire.CreateRequest) (uint32, error) {
	access, err := expandCreateAccess(request.DesiredAccess)
	if err != nil {
		return 0, err
	}
	if request.Disposition > 5 || request.ShareAccess&^uint32(7) != 0 || request.Impersonation > 3 {
		return 0, syscall.EINVAL
	}
	if request.Disposition == 0 {
		return 0, syscall.EOPNOTSUPP
	}
	switch request.OplockLevel {
	case 0, 1, 8, 9, 0xff:
	default:
		return 0, syscall.EINVAL
	}
	const ignoredOptions = 0x10 | 0x20 | 0x100 | 0x400 | 0x20000 | 0x800000
	const supportedOptions = createDirectory | createWriteThrough | 0x4 | ignoredOptions | createNonDirectory | 0x800 | createDeleteOnClose | 0x4000 | 0x200000
	if request.Options&^uint32(supportedOptions) != 0 {
		return 0, syscall.EOPNOTSUPP
	}
	if request.Options&(createDirectory|createNonDirectory) == createDirectory|createNonDirectory {
		return 0, syscall.EINVAL
	}
	if request.Options&createDeleteOnClose != 0 && access&fileDelete == 0 {
		return 0, syscall.EACCES
	}
	const supportedAttributes = dosSettableAttributes | dosNormal | dosDirectory
	if request.Attributes&^uint32(supportedAttributes) != 0 {
		return 0, syscall.EOPNOTSUPP
	}
	if request.Attributes&dosNormal != 0 && request.Attributes&^uint32(dosNormal) != 0 {
		return 0, syscall.EINVAL
	}
	if err := validateCreateContexts(request.Contexts); err != nil {
		return 0, err
	}
	return access, nil
}

func createUses(kind storage.NodeKind, access, share uint32) storage.UseClaim {
	var claim storage.UseClaim
	if access&fileReadData != 0 {
		if kind == storage.NodeDirectory {
			claim.Uses |= storage.ReadEntries
		} else {
			claim.Uses |= storage.ReadData
		}
	}
	if access&fileExecute != 0 {
		if kind == storage.NodeDirectory {
			claim.Uses |= storage.ReadEntries
		} else {
			claim.Uses |= storage.ReadData
		}
	}
	if access&(fileWriteData|fileAppendData) != 0 {
		claim.Uses |= storage.WriteData
	}
	if access&fileDelete != 0 {
		claim.Uses |= storage.DeleteName
	}
	if share&1 == 0 {
		claim.Deny |= storage.ReadData | storage.ReadEntries
	}
	if share&2 == 0 {
		claim.Deny |= storage.WriteData
	}
	if share&4 == 0 {
		claim.Deny |= storage.DeleteName
	}
	return claim
}

func buildCreatePlan(request wire.CreateRequest, name resolvedName, owner storage.DeleteIntentOwner) (createPlan, error) {
	access, err := validateCreateRequest(request)
	if err != nil {
		return createPlan{}, err
	}
	plan := createPlan{
		access: access, share: request.ShareAccess, name: name,
		writeThrough: request.Options&createWriteThrough != 0, contexts: request.Contexts,
	}
	present := name.condition.State == storage.SameNode
	if present != (name.attr != nil) || !present && name.condition.State != storage.Absent {
		return createPlan{}, syscall.EIO
	}
	kind := storage.NodeRegular
	if request.Options&createDirectory != 0 || name.directoryRequired {
		kind = storage.NodeDirectory
	}
	if present {
		if name.attr.ID != name.condition.NodeID || name.attr.ID == 0 {
			return createPlan{}, syscall.EIO
		}
		kind = name.attr.Kind
	}
	if kind != storage.NodeRegular && kind != storage.NodeDirectory {
		return createPlan{}, syscall.EOPNOTSUPP
	}
	if (request.Options&createDirectory != 0 || name.directoryRequired) && kind != storage.NodeDirectory {
		return createPlan{}, syscall.ENOTDIR
	}
	if request.Options&createNonDirectory != 0 && kind == storage.NodeDirectory {
		return createPlan{}, syscall.EISDIR
	}
	if request.Attributes&dosDirectory != 0 && kind != storage.NodeDirectory {
		return createPlan{}, syscall.EINVAL
	}
	if name.root && (!present || request.Disposition != 1 && request.Disposition != 3 || request.Options&createDeleteOnClose != 0) {
		return createPlan{}, syscall.EACCES
	}
	if kind == storage.NodeDirectory && request.Disposition != 1 && request.Disposition != 2 && request.Disposition != 3 {
		return createPlan{}, syscall.EINVAL
	}
	if present && request.Disposition == 2 {
		return createPlan{}, syscall.EEXIST
	}
	if !present && (request.Disposition == 1 || request.Disposition == 4) {
		return createPlan{}, createFailure(0xc000000f, syscall.ENOENT)
	}

	if !present {
		for _, value := range request.Contexts {
			switch string(value.Name) {
			case "ExtA":
				return createPlan{}, createFailure(0xc000004f, syscall.EOPNOTSUPP)
			case "SecD":
				return createPlan{}, syscall.EOPNOTSUPP
			}
		}
	}
	reset := present && (request.Disposition == 4 || request.Disposition == 5)
	if reset && access&fileWriteData == 0 {
		return createPlan{}, syscall.EACCES
	}

	attributes := request.Attributes & (dosSettableAttributes &^ dosNormal)
	target := name.condition
	if present {
		old, err := decodeWindowsMetadata(name.attr.Metadata)
		if err != nil {
			return createPlan{}, err
		}
		if old.Attributes&dosReadOnly != 0 && request.Options&createDeleteOnClose != 0 {
			return createPlan{}, createFailure(0xc0000121, syscall.EACCES)
		}
		if kind == storage.NodeRegular && old.Attributes&dosReadOnly != 0 && access&(fileWriteData|fileAppendData) != 0 {
			return createPlan{}, syscall.EACCES
		}
		if reset && old.Attributes&(dosHidden|dosSystem)&^attributes != 0 {
			return createPlan{}, syscall.EACCES
		}
		if stored, exists := name.attr.Metadata[windowsMetadataKey]; exists {
			target.ExpectedMetadata = map[string][]byte{windowsMetadataKey: bytes.Clone(stored.Version)}
		} else {
			target.ExpectedMetadata = map[string][]byte{windowsMetadataKey: nil}
		}
		size, err := virtualFileSize(*name.attr)
		if err != nil {
			return createPlan{}, err
		}
		if _, err := captureCreateInformation(*name.attr, size); err != nil {
			return createPlan{}, err
		}
	}
	if !present && attributes&dosReadOnly != 0 && request.Options&createDeleteOnClose != 0 {
		return createPlan{}, createFailure(0xc0000121, syscall.EACCES)
	}

	create := !present
	createCapable := request.Disposition == 2 || request.Disposition == 3 || request.Disposition == 5
	plan.create = createCapable
	var initial storage.InitialState
	if create || reset {
		if kind == storage.NodeRegular {
			attributes |= dosArchive
		}
		payload, err := encodeWindowsMetadata(windowsMetadata{Attributes: attributes})
		if err != nil {
			return createPlan{}, err
		}
		fields := storage.InitialFields{Metadata: map[string][]byte{windowsMetadataKey: payload}}
		if reset {
			initial.OnReset = fields
		} else {
			initial.OnCreate = fields
		}
	}
	claim := createUses(kind, access, request.ShareAccess)
	var intent *storage.CloseIntent
	if request.Options&createDeleteOnClose != 0 {
		intentID, err := storage.NewDeleteIntentID()
		if err != nil {
			return createPlan{}, err
		}
		condition := storage.UnlinkFile
		if kind == storage.NodeDirectory {
			condition = storage.UnlinkIfEmpty
		}
		intent = &storage.CloseIntent{ID: intentID, Owner: owner, Trigger: storage.OnReferenceClose, Condition: condition}
	}

	plan.kind = kind
	plan.outcome = storage.Opened
	if create {
		plan.outcome = storage.Created
	} else if reset {
		plan.outcome = storage.Reset
	}
	if kind == storage.NodeRegular && claim.Uses&(storage.ReadData|storage.WriteData) != 0 {
		existing := storage.Keep
		if reset {
			existing = storage.ResetContent
		}
		plan.file = &storage.OpenAtOptions{
			Read: claim.Uses&storage.ReadData != 0, Write: claim.Uses&storage.WriteData != 0,
			Create: createCapable, Exclusive: request.Disposition == 2, Target: target,
			Use: claim, Existing: existing, Initial: initial, CloseIntent: intent,
		}
	} else {
		metadata := storage.ReadMetadata
		if access&(fileWriteEA|fileWriteAttributes) != 0 {
			metadata |= storage.WriteMetadata
		}
		plan.node = &storage.NodeRefOptions{
			Kind: kind, Target: target, Use: claim, MetadataAccess: metadata,
			Create: createCapable, Exclusive: request.Disposition == 2, InitialState: initial, CloseIntent: intent,
		}
		if name.root {
			plan.node.Create = false
			plan.node.Exclusive = false
		}
	}
	if plan.name.attr != nil {
		scalar := plan.name.attr.Clone()
		scalar.Metadata = nil
		plan.name.attr = &scalar
	}
	return plan, nil
}

func (a *authoritySession) newFileAction() (storage.FileActionID, error) {
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	if a.stopping || a.closed || a.actionEpoch == 0 {
		return "", syscall.EIO
	}
	return storage.NewFileActionID(a.actionEpoch)
}

func authorizeCreate(ctx context.Context, tree *tree, plan createPlan, authorize namespaceAuthorizer) error {
	request := authz.AccessRequest{Volume: tree.export.share.Volume}
	var additional []storage.Operation
	if options := plan.file; options != nil {
		request.Operation = storage.OpFileOpenAt
		request.Open = storage.OpenAccess{
			Read: options.Read, Write: options.Write, Create: plan.create,
			Exclusive: options.Exclusive, Truncate: options.Existing == storage.ResetContent,
		}
		for _, initial := range []storage.InitialFields{options.Initial.OnCreate, options.Initial.OnReset, options.Initial.OnReplace} {
			if !initial.Attr.Empty() {
				additional = append(additional, storage.OpFileSetAttr)
			}
			if len(initial.Metadata) != 0 {
				additional = append(additional, storage.OpFileSetMetadata)
			}
		}
		if options.CloseIntent != nil {
			additional = append(additional, storage.OpFileSetPendingUnlink)
		}
	} else {
		options := plan.node
		request.Operation = storage.OpFileOpenChildRef
		if plan.name.root {
			request.Operation = storage.OpFileOpenNodeRef
		}
		request.Open = storage.OpenAccess{
			Read:   options.MetadataAccess&storage.ReadMetadata != 0 || options.Use.Uses&(storage.ReadData|storage.ReadEntries) != 0,
			Write:  options.MetadataAccess&storage.WriteMetadata != 0 || options.Use.Uses&storage.WriteData != 0,
			Create: plan.create, Exclusive: options.Exclusive,
		}
		if !options.InitialState.OnCreate.Attr.Empty() {
			additional = append(additional, storage.OpFileSetAttr)
		}
		if len(options.InitialState.OnCreate.Metadata) != 0 {
			additional = append(additional, storage.OpFileSetMetadata)
		}
		if options.CloseIntent != nil {
			additional = append(additional, storage.OpFileSetPendingUnlink)
		}
	}
	if err := authorize(ctx, request); err != nil {
		return err
	}
	for _, operation := range additional {
		if err := authorize(ctx, authz.AccessRequest{Volume: tree.export.share.Volume, Operation: operation}); err != nil {
			return err
		}
	}
	return nil
}

func zeroCreateAttr(attr storage.Attr) bool {
	return attr.ID == 0 && attr.Kind == 0 && attr.BirthTime == nil && attr.ChangeTime == nil && attr.Metadata == nil &&
		attr.Size == 0 && attr.AccessTime.IsZero() && attr.ModTime.IsZero()
}

func finalizeOpenReservation(ctx context.Context, server *Server, reservation *openReservation, cause error) error {
	reservation.releaseResponse()
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), server.config.Limits.CleanupTimeout)
	defer cancel()
	if cleanupErr := reservation.finish(cleanup); cleanupErr != nil {
		server.cleanupFailure(cleanupErr)
		return createFailure(statusIO, errors.Join(cause, cleanupErr))
	}
	return cause
}

func executeCreatePlan(ctx context.Context, tree *tree, reservation *openReservation, plan createPlan, authorize namespaceAuthorizer) ([]byte, bool, error) {
	if err := authorizeCreate(ctx, tree, plan, authorize); err != nil {
		return nil, false, err
	}
	if err := authorize(ctx, authz.AccessRequest{Volume: tree.export.share.Volume, Operation: storage.OpFileScope}); err != nil {
		return nil, false, err
	}
	action, err := tree.authority.newFileAction()
	if err != nil {
		return nil, false, err
	}
	if plan.file != nil {
		plan.file.Action = action
		if err := plan.file.Check(); err != nil {
			return nil, false, err
		}
	} else {
		plan.node.Action = action
		if err := plan.node.Check(); err != nil {
			return nil, false, err
		}
	}
	var opener storage.AtomicFileOpener
	var references storage.NodeReferences
	if plan.file != nil {
		var ok bool
		opener, ok = tree.authority.raw.(storage.AtomicFileOpener)
		if !ok {
			return nil, false, syscall.EOPNOTSUPP
		}
		if err := opener.CheckAtomicFileOpen(); err != nil {
			return nil, false, err
		}
	} else {
		var ok bool
		references, ok = tree.authority.raw.(storage.NodeReferences)
		if !ok {
			return nil, false, syscall.EOPNOTSUPP
		}
		if err := references.CheckNodeReferences(); err != nil {
			return nil, false, err
		}
	}
	if intent := createPlanDeleteIntent(plan); intent != "" {
		reservation.setDeleteIntent(intent)
	}
	reservation.setWriteThrough(plan.writeThrough)

	call := storage.WithBoundedAttrResult(ctx, tree.files.limits.MaxOpenResultBytes, reservation.resultBudget)
	var reference handleReference
	var attr storage.Attr
	var outcome storage.OpenOutcome
	if plan.file != nil {
		result, failure := opener.OpenAt(call, plan.name.selection, *plan.file)
		reservation.attachFile(result)
		reference, attr, outcome, err = result.File, result.Attr, result.Outcome, failure
	} else {
		var result storage.NodeOpenResult
		if plan.name.root {
			result, err = references.OpenNodeRef(call, plan.name.rootID, *plan.node)
		} else {
			result, err = references.OpenChildRef(call, plan.name.selection, *plan.node)
		}
		reservation.attachNode(result)
		reference, attr, outcome = result.Reference, result.Attr, result.Outcome
	}
	if err != nil {
		retry := errors.Is(err, storage.ErrConditionConflict) && storage.ErrnoOf(err) == syscall.EAGAIN &&
			reference == nil && zeroCreateAttr(attr) && outcome == 0
		if retry {
			reservation.discardDeleteIntent()
		}
		if !retry && (reference != nil || !zeroCreateAttr(attr) || outcome != 0) {
			err = errors.Join(err, syscall.EIO)
		}
		return nil, retry, namespaceFailure(err)
	}
	if reference == nil || attr.ID == 0 || attr.Kind != plan.kind || outcome != plan.outcome ||
		plan.name.condition.State == storage.SameNode && attr.ID != plan.name.condition.NodeID {
		return nil, false, fmt.Errorf("atomic open returned inconsistent identity or outcome: %w", syscall.EIO)
	}
	identity, err := storage.ReferenceNodeID(reference)
	if err != nil || identity != attr.ID {
		return nil, false, errors.Join(err, syscall.EIO)
	}
	if err := checkSMBReference(reference); err != nil {
		return nil, false, err
	}
	scoped, ok := reference.(storage.ScopedReference)
	if !ok {
		return nil, false, syscall.EOPNOTSUPP
	}
	if err := scoped.CheckScopedReference(); err != nil {
		return nil, false, err
	}
	scope, err := scoped.Scope(ctx)
	if err != nil {
		return nil, false, err
	}
	if err := scope.Check(); err != nil {
		return nil, false, err
	}
	reservation.setScope(scope)

	size, err := virtualFileSize(attr)
	if err != nil {
		return nil, false, err
	}
	information, err := captureCreateInformation(attr, size)
	if err != nil {
		return nil, false, err
	}
	actionResult, err := createAction(outcome)
	if err != nil {
		return nil, false, err
	}
	body := createResponseContexts(wire.CreateResponseBody(wire.CreateResult{
		Action: actionResult, FileInformation: information,
	}), plan.contexts, attr.ID, virtualVolumeSerial(tree.export.share.Volume), information.ChangeTime)
	if err := ctx.Err(); err != nil {
		return nil, false, createFailure(statusIO, err)
	}
	id, err := reservation.install(plan.access, plan.share)
	if err != nil {
		return nil, false, err
	}
	copy(body[64:80], id[:])
	return body, false, nil
}

func createPlanDeleteIntent(plan createPlan) storage.DeleteIntentID {
	if plan.file != nil && plan.file.CloseIntent != nil {
		return plan.file.CloseIntent.ID
	}
	if plan.node != nil && plan.node.CloseIntent != nil {
		return plan.node.CloseIntent.ID
	}
	return ""
}

func (c *connection) createOpen(ctx context.Context, tree *tree, request wire.CreateRequest) ([]byte, error) {
	return c.createOpenWithResolver(ctx, tree, request, resolveName)
}

func (c *connection) createOpenWithResolver(ctx context.Context, tree *tree, request wire.CreateRequest, resolve createNameResolver) ([]byte, error) {
	if _, err := validateCreateRequest(request); err != nil {
		return nil, err
	}
	authorize := func(ctx context.Context, request authz.AccessRequest) error {
		return c.authorizeFileAccess(ctx, tree, request)
	}
	for range 4 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reservation, err := tree.files.reserveResponse()
		if err != nil {
			return nil, err
		}
		name, err := resolve(ctx, tree, request.Name, reservation.adoptAux, authorize, tree.authority.newFileAction)
		if err != nil {
			retry := errors.Is(err, storage.ErrConditionConflict) && storage.ErrnoOf(err) == syscall.EAGAIN
			err = finalizeOpenReservation(ctx, c.server, reservation, err)
			var forced *createStatusError
			if errors.As(err, &forced) {
				retry = false
			}
			if retry {
				continue
			}
			return nil, err
		}
		plan, err := buildCreatePlan(request, name, tree.export.share.DeleteIntentOwner)
		name.releaseCapture()
		if err != nil {
			return nil, finalizeOpenReservation(ctx, c.server, reservation, err)
		}
		body, retry, err := executeCreatePlan(ctx, tree, reservation, plan, authorize)
		err = finalizeOpenReservation(ctx, c.server, reservation, err)
		var forced *createStatusError
		if errors.As(err, &forced) {
			retry = false
		}
		if err != nil && body != nil {
			var id wire.FileID
			copy(id[:], body[64:80])
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.server.config.Limits.CleanupTimeout)
			if handle := tree.files.get(id); handle != nil {
				err = errors.Join(err, tree.files.closeID(cleanup, id, handle))
			}
			cancel()
			body = nil
		}
		if retry && err != nil {
			continue
		}
		return body, err
	}
	return nil, storage.ErrConditionConflict
}

func (c *connection) create(ctx context.Context, tree *tree, request wire.Request) ([]byte, uint32) {
	decoded, err := request.Create()
	if err != nil {
		return nil, statusInvalid
	}
	body, err := c.createOpen(ctx, tree, decoded)
	return body, createStatus(err)
}

// Optional durable requests do not grant reconnectable handles. Unsupported
// contexts fail before namespace or file authority is touched.
func validateCreateContexts(contexts []wire.CreateContext) error {
	var durableV2, incompatibleDurable, reconnect bool
	responseBytes := 152
	seen := make(map[string]struct{}, len(contexts))
	for _, value := range contexts {
		name := string(value.Name)
		if _, duplicate := seen[name]; duplicate {
			return syscall.EINVAL
		}
		seen[name] = struct{}{}
		switch name {
		case "RqLs", "AlSi", "SecD", "ExtA":
		case "DHnQ":
			incompatibleDurable = true
		case "DH2Q":
			durableV2 = true
		case "DHnC", "DH2C":
			incompatibleDurable = true
			reconnect = true
		case "QFid":
			if len(value.Data) != 0 {
				return syscall.EINVAL
			}
			responseBytes += 56
		case "MxAc":
			if len(value.Data) != 0 && len(value.Data) != 8 {
				return syscall.EINVAL
			}
			responseBytes += 32
		case "\x93\xad\x25\x50\x9c\xb4\x11\xe7\xb4\x23\x83\xde\x96\x8b\xcd\x7c":
		default:
			return syscall.EOPNOTSUPP
		}
		if responseBytes > responseBudget(wire.Request{Header: wire.Header{Command: wire.Create}}) {
			return syscall.EFBIG
		}
	}
	if durableV2 && incompatibleDurable {
		return syscall.EINVAL
	}
	if reconnect {
		return syscall.EOPNOTSUPP
	}
	return nil
}

func createResponseContexts(body []byte, contexts []wire.CreateContext, nodeID, volumeID, changeTime uint64) []byte {
	previous := -1
	for _, value := range contexts {
		var data []byte
		switch string(value.Name) {
		case "QFid":
			data = make([]byte, 32)
			binary.LittleEndian.PutUint64(data, nodeID)
			binary.LittleEndian.PutUint64(data[8:], volumeID)
		case "MxAc":
			data = make([]byte, 8)
			queryStatus := uint32(statusUnsupported)
			if len(value.Data) == 8 && binary.LittleEndian.Uint64(value.Data) == changeTime {
				queryStatus = 0xc0000073
			}
			binary.LittleEndian.PutUint32(data, queryStatus)
		default:
			continue
		}
		start := len(body)
		body = append(body, make([]byte, 24+len(data))...)
		if previous < 0 {
			binary.LittleEndian.PutUint32(body[80:], uint32(wire.HeaderSize+start))
		} else {
			binary.LittleEndian.PutUint32(body[previous:], uint32(start-previous))
		}
		context := body[start:]
		binary.LittleEndian.PutUint16(context[4:], 16)
		binary.LittleEndian.PutUint16(context[6:], 4)
		binary.LittleEndian.PutUint16(context[10:], 24)
		binary.LittleEndian.PutUint32(context[12:], uint32(len(data)))
		copy(context[16:20], value.Name)
		copy(context[24:], data)
		previous = start
	}
	if previous >= 0 {
		binary.LittleEndian.PutUint32(body[84:], uint32(len(body)-88))
	}
	return body
}
