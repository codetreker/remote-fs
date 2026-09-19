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
	createNonDirectory  uint32 = 0x40
	createDeleteOnClose uint32 = 0x1000
)

type createNameResolver func(context.Context, storage.FileStorage, storage.FileSession, string, Limits, func(context.Context, storage.Operation) error) (resolvedName, error)

type createPlan struct {
	access   uint32
	share    uint32
	name     resolvedName
	file     *storage.OpenAtOptions
	node     *storage.NodeRefOptions
	outcome  storage.OpenOutcome
	kind     storage.NodeKind
	contexts []wire.CreateContext
}

type createStatusError struct {
	status uint32
	cause  error
}

func (e *createStatusError) Error() string         { return e.cause.Error() }
func (e *createStatusError) Unwrap() error         { return e.cause }
func createFailure(status uint32, err error) error { return &createStatusError{status, err} }

func createStatus(err error) uint32 {
	if err == nil {
		return statusOK
	}
	var failure *createStatusError
	if errors.As(err, &failure) {
		return failure.status
	}
	if errors.Is(err, errNameInvalid) || errors.Is(err, errPathMissing) {
		return namespaceStatus(err)
	}
	if storage.ErrnoOf(err) == syscall.EIO {
		return statusError(err)
	}
	switch {
	case errors.Is(err, storage.ErrUseConflict):
		return 0xc0000043
	case errors.Is(err, storage.ErrPendingDelete):
		return 0xc0000056
	case errors.Is(err, syscall.EEXIST):
		return 0xc0000035
	case errors.Is(err, syscall.EISDIR):
		return 0xc00000ba
	default:
		return namespaceStatus(err)
	}
}

func expandCreateAccess(access uint32) (uint32, error) {
	for _, generic := range []struct{ bit, rights uint32 }{
		{0x80000000, 0x120089}, {0x40000000, 0x120116},
		{0x20000000, 0x1200a0}, {0x10000000, 0x1f01ff},
	} {
		if access&generic.bit != 0 {
			access = access&^generic.bit | generic.rights
		}
	}
	if access & ^uint32(0x1301ff) != 0 {
		return 0, syscall.EOPNOTSUPP
	}
	return access, nil
}

func validateCreateRequest(request wire.CreateRequest) (uint32, error) {
	access, err := expandCreateAccess(request.DesiredAccess)
	if err != nil {
		return 0, err
	}
	if request.Disposition > 5 || request.ShareAccess & ^uint32(7) != 0 || request.Impersonation > 3 {
		return 0, syscall.EINVAL
	}
	switch request.OplockLevel {
	case 0, 1, 8, 9, 0xff:
	default:
		return 0, syscall.EINVAL
	}
	const ignoredOptions = 0x10 | 0x20 | 0x100 | 0x400 | 0x10000 | 0x20000 | 0x800000
	const supportedOptions = createDirectory | 0x2 | 0x4 | ignoredOptions | createNonDirectory | 0x800 | createDeleteOnClose | 0x4000 | 0x200000
	if request.Options & ^uint32(supportedOptions) != 0 {
		return 0, syscall.EOPNOTSUPP
	}
	if request.Options&(createDirectory|createNonDirectory) == createDirectory|createNonDirectory {
		return 0, syscall.EINVAL
	}
	if request.Options&createDeleteOnClose != 0 && access&fileDelete == 0 {
		return 0, syscall.EACCES
	}
	if request.Attributes & ^uint32(0x005effb7) != 0 {
		return 0, syscall.EINVAL
	}
	if err := validateCreateContexts(request.Contexts); err != nil {
		return 0, err
	}
	return access, nil
}

func createUses(kind storage.NodeKind, access, share uint32) storage.UseClaim {
	var use storage.UseClaim
	if access&fileReadData != 0 {
		if kind == storage.NodeDirectory {
			use.Uses |= storage.ReadEntries
		} else {
			use.Uses |= storage.ReadData
		}
	}
	if access&fileExecute != 0 {
		use.Uses |= storage.ReadData
	}
	if access&(fileWriteData|fileAppendData) != 0 {
		use.Uses |= storage.WriteData
	}
	if access&fileDelete != 0 {
		use.Uses |= storage.DeleteName
	}
	if share&1 == 0 {
		use.Deny |= storage.ReadData | storage.ReadEntries
	}
	if share&2 == 0 {
		use.Deny |= storage.WriteData
	}
	if share&4 == 0 {
		use.Deny |= storage.DeleteName
	}
	return use
}

func buildCreatePlan(request wire.CreateRequest, name resolvedName) (createPlan, error) {
	access, err := validateCreateRequest(request)
	if err != nil {
		return createPlan{}, err
	}
	plan := createPlan{access: access, share: request.ShareAccess, name: name, contexts: request.Contexts}
	present := name.Condition.State == storage.SameNode
	if present != (name.Attr != nil) || !present && name.Condition.State != storage.Absent {
		return createPlan{}, syscall.EIO
	}
	kind := storage.NodeRegular
	if request.Options&createDirectory != 0 || name.DirectoryRequired {
		kind = storage.NodeDirectory
	}
	if present {
		if name.Attr.ID != name.Condition.NodeID || name.Attr.ID == 0 {
			return createPlan{}, syscall.EIO
		}
		kind = name.Attr.Kind
	}
	if kind != storage.NodeRegular && kind != storage.NodeDirectory {
		return createPlan{}, syscall.EOPNOTSUPP
	}
	if (request.Options&createDirectory != 0 || name.DirectoryRequired) && kind != storage.NodeDirectory {
		return createPlan{}, syscall.ENOTDIR
	}
	if request.Options&createNonDirectory != 0 && kind == storage.NodeDirectory {
		return createPlan{}, syscall.EISDIR
	}
	if name.Root && (!present || request.Disposition != 1 && request.Disposition != 3 || request.Options&createDeleteOnClose != 0) {
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
	if present && request.Disposition == 0 {
		return createPlan{}, syscall.EOPNOTSUPP
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
		return createPlan{}, syscall.EOPNOTSUPP
	}

	attributes := request.Attributes & (dosSettableAttributes &^ dosNormal)
	var expected map[string][]byte
	if present {
		old, err := decodeWindowsMetadata(name.Attr.Metadata)
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
		expected = map[string][]byte{windowsMetadataKey: bytes.Clone(name.Attr.Metadata[windowsMetadataKey].Version)}
		// Existing unknown history cannot become a failed response after a
		// destructive open that we already knew was unrepresentable.
		size, err := virtualFileSize(*name.Attr)
		if err != nil {
			return createPlan{}, err
		}
		if _, err := captureCreateInformation(*name.Attr, size); err != nil {
			return createPlan{}, err
		}
	}
	if !present && attributes&dosReadOnly != 0 && request.Options&createDeleteOnClose != 0 {
		return createPlan{}, createFailure(0xc0000121, syscall.EACCES)
	}
	create := !present
	var initial storage.InitialState
	if !present || reset {
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
	use := createUses(kind, access, request.ShareAccess)
	var intent *storage.CloseIntent
	if request.Options&createDeleteOnClose != 0 {
		condition := storage.UnlinkFile
		if kind == storage.NodeDirectory {
			condition = storage.UnlinkIfEmpty
		}
		intent = &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: condition, Guards: &name.Guards}
	}
	plan.kind = kind
	plan.outcome = storage.Opened
	if !present {
		plan.outcome = storage.Created
	} else if reset {
		plan.outcome = storage.Reset
	}
	if kind == storage.NodeRegular && use.Uses&(storage.ReadData|storage.WriteData) != 0 {
		existing := storage.Keep
		if reset {
			existing = storage.ResetContent
		}
		plan.file = &storage.OpenAtOptions{Read: use.Uses&storage.ReadData != 0, Write: use.Uses&storage.WriteData != 0,
			Create: create, Exclusive: request.Disposition == 2, Target: name.Condition, ExpectedMetadata: expected,
			Guards: &name.Guards, Use: use, Existing: existing, Initial: initial, CloseIntent: intent}
		if err := plan.file.Check(); err != nil {
			return createPlan{}, err
		}
	} else {
		metadata := storage.ReadMetadata
		if access&fileWriteAttributes != 0 {
			metadata |= storage.WriteMetadata
		}
		plan.node = &storage.NodeRefOptions{Kind: kind, Target: name.Condition, ExpectedMetadata: expected,
			Guards: &name.Guards, Use: use, MetadataAccess: metadata, Create: create, Exclusive: request.Disposition == 2,
			InitialState: initial, CloseIntent: intent}
		if name.Root {
			plan.node.Create = false
		}
		if err := plan.node.Check(); err != nil {
			return createPlan{}, err
		}
	}
	return plan, nil
}

func (c *connection) authorizeCreate(ctx context.Context, t *tree, plan createPlan) error {
	request := authz.AccessRequest{Volume: t.export.share.Volume}
	var additional []storage.Operation
	if options := plan.file; options != nil {
		request.Operation = storage.OpFileOpenAt
		request.Open = storage.OpenAccess{Read: options.Read, Write: options.Write, Create: options.Create, Exclusive: options.Exclusive, Truncate: options.Existing == storage.ResetContent}
		if options.Existing == storage.ResetContent {
			additional = append(additional, storage.OpFileSetAttr, storage.OpFileSetMetadata)
		}
		if options.CloseIntent != nil {
			additional = append(additional, storage.OpFileSetPendingUnlink)
		}
	} else {
		options := plan.node
		request.Operation = storage.OpFileOpenChildRef
		if plan.name.Root {
			request.Operation = storage.OpFileOpenNodeRef
		}
		request.Open = storage.OpenAccess{Read: options.MetadataAccess&storage.ReadMetadata != 0 || options.Use.Uses&(storage.ReadData|storage.ReadEntries) != 0,
			Write:  options.MetadataAccess&storage.WriteMetadata != 0 || options.Use.Uses&storage.WriteData != 0,
			Create: options.Create, Exclusive: options.Exclusive}
		if options.CloseIntent != nil {
			additional = append(additional, storage.OpFileSetPendingUnlink)
		}
	}
	if err := c.server.config.Authorize.Authorize(ctx, request); err != nil {
		return err
	}
	for _, operation := range additional {
		if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation}); err != nil {
			return err
		}
	}
	return nil
}

func zeroCreateAttr(attr storage.Attr) bool {
	return attr.ID == 0 && attr.Kind == 0 && attr.BirthTime == nil && attr.ChangeTime == nil && attr.Metadata == nil && attr.Size == 0 && attr.AccessTime.IsZero() && attr.ModTime.IsZero()
}

func (c *connection) createAttempt(ctx context.Context, t *tree, plan createPlan) ([]byte, bool, error) {
	if err := c.authorizeCreate(ctx, t, plan); err != nil {
		return nil, false, err
	}
	var fileOpener storage.AtomicFileOpener
	var references storage.NodeReferences
	if plan.file != nil {
		var ok bool
		fileOpener, ok = t.authority.raw.(storage.AtomicFileOpener)
		if !ok {
			return nil, false, syscall.EOPNOTSUPP
		}
		if err := fileOpener.CheckAtomicFileOpen(); err != nil {
			return nil, false, err
		}
	} else {
		var ok bool
		references, ok = t.authority.raw.(storage.NodeReferences)
		if !ok {
			return nil, false, syscall.EOPNOTSUPP
		}
		if err := references.CheckNodeReferences(); err != nil {
			return nil, false, err
		}
	}
	reservation, err := t.files.reserveResponse()
	if err != nil {
		return nil, false, err
	}
	call := storage.WithAttrResultBudget(ctx, func(_ storage.Attr, metadataBytes int64) error {
		charge, err := storage.MetadataRetentionBytes(metadataBytes)
		if err != nil {
			return err
		}
		if charge > reservation.charge-512 {
			return syscall.EFBIG
		}
		return nil
	})
	var reference storage.NodeReference
	var attr storage.Attr
	var outcome storage.OpenOutcome
	if plan.file != nil {
		result, failure := fileOpener.OpenAt(call, plan.name.Target, *plan.file)
		reservation.attachFile(result)
		reference, attr, outcome, err = result.File, result.Attr, result.Outcome, failure
	} else {
		var result storage.NodeOpenResult
		if plan.name.Root {
			result, err = references.OpenNodeRef(call, plan.name.RootID, *plan.node)
		} else {
			result, err = references.OpenChildRef(call, plan.name.Target, *plan.node)
		}
		reservation.attachNode(result)
		reference, attr, outcome = result.Reference, result.Attr, result.Outcome
	}
	settle := func(failure error) error {
		reservation.releaseResponse()
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.server.config.Limits.CleanupTimeout)
		defer cancel()
		if cleanupErr := reservation.finish(cleanup); cleanupErr != nil {
			c.server.cleanupFailure(cleanupErr)
			return createFailure(statusIO, errors.Join(failure, cleanupErr))
		}
		return failure
	}
	if err != nil {
		retry := errors.Is(err, storage.ErrConditionConflict) && storage.ErrnoOf(err) == syscall.EAGAIN && reference == nil && zeroCreateAttr(attr) && outcome == 0
		failure := settle(namespaceFailure(err))
		// A cleanup failure is never permission to issue a second mutation.
		var forced *createStatusError
		if errors.As(failure, &forced) {
			retry = false
		}
		return nil, retry, failure
	}
	if reference == nil || attr.ID == 0 || attr.Kind != plan.kind || outcome != plan.outcome || plan.name.Condition.State == storage.SameNode && attr.ID != plan.name.Condition.NodeID {
		return nil, false, settle(fmt.Errorf("atomic open returned inconsistent identity or outcome: %w", syscall.EIO))
	}
	identity, err := storage.ReferenceNodeID(reference)
	if err != nil {
		return nil, false, settle(err)
	}
	if identity != attr.ID {
		return nil, false, settle(fmt.Errorf("reference differs from captured identity: %w", syscall.EIO))
	}
	size, err := virtualFileSize(attr)
	if err != nil {
		return nil, false, settle(err)
	}
	info, err := captureCreateInformation(attr, size)
	if err != nil {
		return nil, false, settle(err)
	}
	action, err := createAction(outcome)
	if err != nil {
		return nil, false, settle(err)
	}
	body := createResponseContexts(wire.CreateResponseBody(wire.CreateResult{Action: action, FileInformation: info}), plan.contexts, attr.ID, virtualVolumeSerial(t.export.share.Volume), info.ChangeTime)
	if err := ctx.Err(); err != nil {
		return nil, false, settle(createFailure(statusIO, err))
	}
	id, err := reservation.install(plan.access, plan.share)
	if err != nil {
		return nil, false, settle(err)
	}
	copy(body[64:80], id[:])
	if err := settle(nil); err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.server.config.Limits.CleanupTimeout)
		defer cancel()
		if handle := t.files.get(id); handle != nil {
			err = errors.Join(err, t.files.closeID(cleanup, id, handle))
		}
		return nil, false, err
	}
	return body, false, nil
}

func (c *connection) createOpen(ctx context.Context, t *tree, request wire.CreateRequest, resolve createNameResolver) ([]byte, error) {
	if _, err := validateCreateRequest(request); err != nil {
		return nil, err
	}
	authorize := func(ctx context.Context, operation storage.Operation) error {
		return c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation})
	}
	for attempt := 0; attempt < 4; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name, err := resolve(ctx, t.export.share.Backend, t.authority.raw, request.Name, c.server.config.Limits, authorize)
		if err != nil {
			if errors.Is(err, storage.ErrConditionConflict) && storage.ErrnoOf(err) == syscall.EAGAIN {
				continue
			}
			return nil, err
		}
		plan, err := buildCreatePlan(request, name)
		if err != nil {
			return nil, err
		}
		body, retry, err := c.createAttempt(ctx, t, plan)
		if retry {
			continue
		}
		return body, err
	}
	return nil, storage.ErrConditionConflict
}

func (c *connection) create(ctx context.Context, t *tree, request wire.Request) ([]byte, uint32) {
	decoded, err := request.Create()
	if err != nil {
		return nil, statusInvalid
	}
	body, err := c.createOpen(ctx, t, decoded, resolveName)
	return body, createStatus(err)
}

// Context-specific ignore rules do not suppress durable combination checks or
// the mandatory identity/maximal-access response contexts.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/8c61e928-9242-44ed-96a0-98d1032d0d39
func validateCreateContexts(contexts []wire.CreateContext) error {
	var durableV2, incompatibleDurable, reconnect bool
	responseBytes := 152
	for _, value := range contexts {
		switch string(value.Name) {
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
			binary.LittleEndian.PutUint32(body[80:], uint32(64+start))
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
