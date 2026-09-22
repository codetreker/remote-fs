package httprest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func capabilitiesOf(value any) (*fileCapabilities, error) {
	caps := &fileCapabilities{}
	var failure error
	check := func(target *bool, call func() error) {
		err := call()
		if err == nil {
			*target = true
		} else if storage.ErrnoOf(err) != syscall.EOPNOTSUPP {
			failure = errors.Join(failure, err)
		}
	}
	if capability, ok := value.(storage.ReferenceNameObserver); ok {
		check(&caps.ReferenceName, func() error {
			if err := capability.CheckReferenceNameObservation(); err != nil {
				return err
			}
			_, err := storage.ReferenceNodeID(value)
			return err
		})
	}
	if capability, ok := value.(storage.AtomicFileOpener); ok {
		check(&caps.AtomicOpen, capability.CheckAtomicFileOpen)
	}
	if capability, ok := value.(storage.NamespaceAccess); ok {
		check(&caps.Namespace, capability.CheckNamespaceAccess)
	}
	if capability, ok := value.(storage.NodeReferences); ok {
		check(&caps.References, capability.CheckNodeReferences)
	}
	if capability, ok := value.(storage.FileActions); ok {
		check(&caps.Actions, capability.CheckFileActions)
	}
	if capability, ok := value.(storage.UseOwners); ok {
		check(&caps.Owners, capability.CheckUseOwners)
	}
	if capability, ok := value.(storage.RangeControl); ok {
		check(&caps.Ranges, capability.CheckRangeControl)
	}
	if capability, ok := value.(storage.ReferenceStateAccess); ok {
		check(&caps.State, capability.CheckReferenceState)
	}
	if capability, ok := value.(storage.ScopedReference); ok {
		check(&caps.Scope, capability.CheckScopedReference)
	}
	if capability, ok := value.(storage.DeleteIntent); ok {
		check(&caps.Delete, capability.CheckDeleteIntent)
	}
	if capability, ok := value.(storage.ConditionalFileMutation); ok {
		check(&caps.Conditional, capability.CheckConditionalFileMutation)
	}
	return caps, failure
}

func sessionCapabilitiesOf(value storage.FileSession) (*fileCapabilities, error) {
	caps, err := capabilitiesOf(value)
	reader, hasReader := value.(storage.DirectoryReader)
	observer, hasObserver := value.(storage.DirectoryMetadataObserver)
	if hasReader && hasObserver {
		readErr := reader.CheckDirectoryRead()
		observeErr := observer.CheckDirectoryMetadataObservation()
		if readErr == nil && observeErr == nil {
			caps.DirectoryMetadata = true
		}
		for _, checkErr := range []error{readErr, observeErr} {
			if checkErr != nil && storage.ErrnoOf(checkErr) != syscall.EOPNOTSUPP {
				err = errors.Join(err, checkErr)
			}
		}
	}
	if capability, ok := value.(storage.MetadataAccess); ok {
		checkErr := capability.CheckMetadataAccess()
		if checkErr == nil {
			caps.Metadata = true
		} else if storage.ErrnoOf(checkErr) != syscall.EOPNOTSUPP {
			err = errors.Join(err, checkErr)
		}
	}
	return caps, err
}

func referenceCapabilitiesOf(value retainedReference) (*fileCapabilities, error) {
	caps, err := capabilitiesOf(value)
	if capability, ok := value.(storage.ReferenceMetadataAccess); ok {
		checkErr := capability.CheckMetadataAccess()
		if checkErr == nil {
			caps.Metadata = true
		} else if storage.ErrnoOf(checkErr) != syscall.EOPNOTSUPP {
			err = errors.Join(err, checkErr)
		}
	}
	return caps, err
}

func retainedReferencePresent(value retainedReference) bool {
	if value == nil {
		return false
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !reflected.IsNil()
	default:
		return true
	}
}

func (h *Handler) openReference(ctx context.Context, session *servedFileSession, request fileRequest) (fileResponse, error) {
	response := fileResponse{}
	session.mu.Lock()
	if len(session.files) >= session.options.MaxFiles {
		session.mu.Unlock()
		return response, syscall.EAGAIN
	}
	capability := fileCapability()
	entry := &servedFile{closing: true}
	session.files[capability] = entry
	session.mu.Unlock()

	var reference retainedReference
	var err error
	switch request.Op {
	case storage.OpFileOpen:
		reference, err = session.native.OpenFile(ctx, string(request.Path), request.Open)
	case storage.OpFileOpenNode:
		reference, err = session.native.OpenNode(ctx, request.Node, request.Open)
	case storage.OpFileOpenAt:
		provider, ok := session.native.(storage.AtomicFileOpener)
		if !ok {
			err = syscall.EOPNOTSUPP
			break
		}
		if err = provider.CheckAtomicFileOpen(); err == nil {
			var result storage.OpenResult
			result, err = provider.OpenAt(ctx, request.Selection.storage(), request.OpenAt.storage())
			reference = result.File
			if result.Attr.ID != 0 {
				response.Attr = AttrOf(result.Attr)
				response.Outcome = result.Outcome
			}
		}
	case storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		provider, ok := session.native.(storage.NodeReferences)
		if !ok {
			err = syscall.EOPNOTSUPP
			break
		}
		if err = provider.CheckNodeReferences(); err == nil {
			var result storage.NodeOpenResult
			if request.Op == storage.OpFileOpenNodeRef {
				result, err = provider.OpenNodeRef(ctx, request.Node, request.NodeRef.storage())
			} else {
				result, err = provider.OpenChildRef(ctx, request.Selection.storage(), request.NodeRef.storage())
			}
			reference = result.Reference
			if result.Attr.ID != 0 {
				response.Attr = AttrOf(result.Attr)
				response.Outcome = result.Outcome
			}
		}
	}

	session.mu.Lock()
	if !retainedReferencePresent(reference) {
		reference = nil
		delete(session.files, capability)
	} else {
		entry.native = reference
		entry.closing = false
		entry.pending = time.Now().Add(min(h.files.limits.PendingAck, session.options.Lease))
	}
	session.mu.Unlock()

	if reference == nil {
		if err == nil {
			err = syscall.EIO
		}
		return response, err
	}
	openErr := err
	response.File = capability
	var capabilityErr error
	response.Capabilities, capabilityErr = referenceCapabilitiesOf(reference)
	if capabilityErr != nil {
		openErr = errors.Join(openErr, capabilityErr, syscall.EIO)
	}
	if (request.Op == storage.OpFileOpenNodeRef || request.Op == storage.OpFileOpenChildRef) && (!response.Capabilities.Scope || !response.Capabilities.State) {
		openErr = errors.Join(openErr, errors.New("node reference lacks mandatory scope or state capability"), syscall.EIO)
	}
	identityInvalid := false
	if response.Capabilities.ReferenceName {
		node, identityErr := storage.ReferenceNodeID(reference)
		if identityErr != nil {
			identityInvalid = true
			openErr = errors.Join(openErr, identityErr, syscall.EIO)
		} else {
			switch request.Op {
			case storage.OpFileOpen:
				response.Node = node
				if request.Open.ExpectedID != 0 && node != request.Open.ExpectedID {
					identityInvalid = true
					openErr = errors.Join(openErr, errors.New("opened reference substituted its expected node identity"), syscall.EIO)
				}
			case storage.OpFileOpenNode:
				response.Node = node
				if node != request.Node {
					identityInvalid = true
					openErr = errors.Join(openErr, errors.New("opened reference substituted its requested node identity"), syscall.EIO)
				}
			case storage.OpFileOpenNodeRef:
				if response.Attr == nil || response.Attr.ID != node || node != request.Node {
					identityInvalid = true
					openErr = errors.Join(openErr, errors.New("opened node reference substituted its requested identity"), syscall.EIO)
				}
			case storage.OpFileOpenChildRef:
				if response.Attr == nil || response.Attr.ID != node || request.NodeRef.Target.State == storage.SameNode && node != request.NodeRef.Target.NodeID {
					identityInvalid = true
					openErr = errors.Join(openErr, errors.New("opened child reference substituted its requested identity"), syscall.EIO)
				}
			case storage.OpFileOpenAt:
				invalidTarget := response.Attr == nil || response.Attr.ID != node
				if request.OpenAt.Target.State == storage.SameNode {
					if request.OpenAt.Existing == storage.ReplaceNode {
						invalidTarget = invalidTarget || node == request.OpenAt.Target.NodeID || response.Outcome != storage.Replaced
					} else {
						invalidTarget = invalidTarget || node != request.OpenAt.Target.NodeID
					}
				}
				if invalidTarget {
					identityInvalid = true
					openErr = errors.Join(openErr, errors.New("atomic open returned an invalid target identity or outcome"), syscall.EIO)
				}
			default:
				if response.Attr == nil || response.Attr.ID != node {
					identityInvalid = true
					openErr = errors.Join(openErr, errors.New("opened reference disagrees with its captured attributes"), syscall.EIO)
				}
			}
		}
	}
	if openErr != nil {
		response, barrierErr := h.finishFileMutation(ctx, response)
		if identityInvalid {
			response.File = ""
			response.Node = 0
			response.Capabilities = nil
		}
		return response, errors.Join(openErr, barrierErr)
	}
	return h.finishFileMutation(ctx, response)
}

func (h *Handler) performSessionCapability(ctx context.Context, session storage.FileSession, req fileRequest) (fileResponse, error) {
	response := fileResponse{}
	switch req.Op {
	case storage.OpFileObserveDirectoryMetadata:
		return h.observeDirectoryMetadata(ctx, session, req)
	case storage.OpFileQueryAction, storage.OpFileQueryDeleteIntent, storage.OpFileListDeleteIntents, storage.OpFileAcknowledgeDeleteIntent:
		capability, ok := session.(storage.FileActions)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckFileActions(); err != nil {
			return response, err
		}
		if req.Op == storage.OpFileQueryAction {
			value, err := capability.QueryFileAction(ctx, req.FileAction)
			if value.Action != "" {
				response.ActionReceipt = &value
			}
			return response, err
		}
		if req.Op == storage.OpFileAcknowledgeDeleteIntent {
			return response, capability.AcknowledgeDeleteIntent(ctx, req.Acknowledge.storage())
		}
		if req.Op == storage.OpFileListDeleteIntents {
			value, err := capability.ListDeleteIntents(ctx, req.DeleteOwner, req.DeleteAfter, req.DeleteLimit)
			if err == nil {
				response.DeletePage, err = deleteIntentPageOf(value)
			}
			return response, err
		}
		value, err := capability.QueryDeleteIntent(ctx, req.DeleteOwner, req.DeleteIntent)
		if value.ID != "" {
			wire, wireErr := deleteIntentStatusOf(value)
			response.DeleteStatus = wire
			err = errors.Join(err, wireErr)
		}
		return response, err
	case storage.OpFileReadDirNode:
		capability, ok := session.(storage.DirectoryReader)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckDirectoryRead(); err != nil {
			return response, err
		}
		result, err := newObservedDirectoryResult(min(h.maxBodyBytes, req.ResultBytes))
		if err != nil {
			return response, err
		}
		observation, err := capability.ReadDirNodeBounded(ctx, *req.Directory, result)
		if err != nil {
			result.Fail(err)
			return response, err
		}
		if observation.ParentID != req.Directory.NodeID {
			err := errors.Join(errors.New("directory observation substituted its target identity"), syscall.EIO)
			result.Fail(err)
			return response, err
		}
		entries, err := result.Entries()
		if err != nil {
			return response, err
		}
		directory := storage.ObservedDirectory{Observation: observation, Entries: make([]storage.ObservedEntry, 0, len(entries))}
		for _, entry := range entries {
			directory.Entries = append(directory.Entries, storage.ObservedEntry{RawLeaf: []byte(entry.Name), Attr: entry.Attr})
		}
		if err := directory.Check(); err != nil {
			result.Fail(err)
			return response, err
		}
		response.Directory = observedDirectoryOf(directory)
		return response, nil
	case storage.OpFileLookupAt, storage.OpFileMutateName:
		capability, ok := session.(storage.NamespaceAccess)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckNamespaceAccess(); err != nil {
			return response, err
		}
		if req.Op == storage.OpFileLookupAt {
			value, err := capability.LookupAt(ctx, req.Child.storage())
			if value.ID != 0 {
				response.Attr = AttrOf(value)
			}
			return response, err
		}
		result, err := capability.MutateName(ctx, req.Name.storage())
		if result.Attr != nil {
			response.Attr = AttrOf(*result.Attr)
		}
		return response, err
	case storage.OpFileSetNodeMetadata:
		capability, ok := session.(storage.MetadataAccess)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckMetadataAccess(); err != nil {
			return response, err
		}
		value, err := capability.SetMetadata(ctx, req.Node, req.Namespace, []byte(req.Version), []byte(req.Payload))
		if err == nil {
			response.Metadata = &OpaquePayload{Version: bytes.Clone(value.Version), Data: append([]byte{}, value.Data...)}
		}
		return response, err
	case storage.OpFileNewUseOwner, storage.OpFileRetireUseOwner:
		capability, ok := session.(storage.UseOwners)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckUseOwners(); err != nil {
			return response, err
		}
		if req.Op == storage.OpFileNewUseOwner {
			owner, err := capability.NewUseOwner(ctx, req.Node, *req.Scope, req.OwnerOptions)
			response.Owner = owner
			return response, err
		}
		return response, capability.RetireUseOwner(ctx, req.Owner)
	case storage.OpFileRangeGetConflict, storage.OpFileRangeApply, storage.OpFileRangeQuery, storage.OpFileRangeCancel, storage.OpFileRangeDrop:
		capability, ok := session.(storage.RangeControl)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckRangeControl(); err != nil {
			return response, err
		}
		switch req.Op {
		case storage.OpFileRangeGetConflict:
			conflict, err := capability.GetConflict(ctx, req.Owner, req.Commands[0])
			response.Conflict = &conflict
			return response, err
		case storage.OpFileRangeDrop:
			return response, capability.Drop(ctx, req.Owner, req.Domain)
		}
		var attempt storage.RangeAttempt
		var err error
		switch req.Op {
		case storage.OpFileRangeApply:
			attempt, err = capability.Apply(ctx, req.Owner, req.Commands, req.LockID)
		case storage.OpFileRangeQuery:
			attempt, err = capability.Query(ctx, req.Owner, req.LockID)
		case storage.OpFileRangeCancel:
			attempt, err = capability.Cancel(ctx, req.Owner, req.LockID)
		}
		if attempt.Request != "" {
			copy := attempt.Clone()
			response.Attempt = &copy
		}
		return response, err
	}
	return response, syscall.EINVAL
}

func performReferenceCapability(ctx context.Context, file retainedReference, req fileRequest) (fileResponse, error) {
	response := fileResponse{}
	switch req.Op {
	case storage.OpFileObserveName:
		return observeReferenceName(ctx, file, req)
	case storage.OpFileState:
		capability, ok := file.(storage.ReferenceStateAccess)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckReferenceState(); err != nil {
			return response, err
		}
		value, err := capability.State(ctx)
		if value.Attr.ID != 0 {
			response.State = referenceStateOf(value)
		}
		return response, err
	case storage.OpFileScope:
		capability, ok := file.(storage.ScopedReference)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckScopedReference(); err != nil {
			return response, err
		}
		scope, err := capability.Scope(ctx)
		if err == nil {
			if err := scope.Check(); err != nil {
				return response, fmt.Errorf("file reference returned an invalid use scope: %w", syscall.EIO)
			}
			response.Scope = &scope
		}
		return response, err
	case storage.OpFileSetMetadata:
		capability, ok := file.(storage.ReferenceMetadataAccess)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckMetadataAccess(); err != nil {
			return response, err
		}
		value, err := capability.SetMetadata(ctx, req.Namespace, []byte(req.Version), []byte(req.Payload))
		if err == nil {
			response.Metadata = &OpaquePayload{Version: bytes.Clone(value.Version), Data: append([]byte{}, value.Data...)}
		}
		return response, err
	case storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink:
		capability, ok := file.(storage.DeleteIntent)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckDeleteIntent(); err != nil {
			return response, err
		}
		var value storage.ReferenceState
		var err error
		if req.Op == storage.OpFileSetPendingUnlink {
			value, err = capability.SetPendingUnlink(ctx, req.Pending.storage())
		} else {
			value, err = capability.ClearPendingUnlink(ctx, req.ClearPending.storage())
		}
		if value.Attr.ID != 0 {
			response.State = referenceStateOf(value)
		}
		return response, err
	case storage.OpFileMutate:
		capability, ok := file.(storage.ConditionalFileMutation)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckConditionalFileMutation(); err != nil {
			return response, err
		}
		value, err := capability.MutateFile(ctx, req.Mutation.storage())
		if value.ID != 0 {
			response.Attr = AttrOf(value)
		}
		return response, err
	}
	return response, syscall.EINVAL
}
