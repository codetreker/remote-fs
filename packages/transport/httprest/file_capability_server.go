package httprest

import (
	"context"
	"encoding/json"
	"errors"
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
	if v, ok := value.(storage.DirectoryMetadataObserver); ok {
		check(&caps.DirectoryMetadata, v.CheckDirectoryMetadataObservation)
	}
	if v, ok := value.(storage.ReferenceNameObserver); ok {
		check(&caps.ReferenceName, func() error {
			if err := v.CheckReferenceNameObservation(); err != nil {
				return err
			}
			_, err := storage.ReferenceNodeID(value)
			return err
		})
	}
	if v, ok := value.(storage.AtomicFileOpener); ok {
		check(&caps.AtomicOpen, v.CheckAtomicFileOpen)
	}
	if v, ok := value.(storage.NamespaceAccess); ok {
		check(&caps.Namespace, v.CheckNamespaceAccess)
	}
	if v, ok := value.(storage.NodeReferences); ok {
		check(&caps.References, v.CheckNodeReferences)
	}
	if v, ok := value.(interface{ CheckMetadataAccess() error }); ok {
		check(&caps.Metadata, v.CheckMetadataAccess)
	}
	if v, ok := value.(storage.UseOwners); ok {
		check(&caps.Owners, v.CheckUseOwners)
	}
	if v, ok := value.(storage.RangeControl); ok {
		check(&caps.Ranges, v.CheckRangeControl)
	}
	if v, ok := value.(storage.ReferenceStateAccess); ok {
		check(&caps.State, v.CheckReferenceState)
	}
	if v, ok := value.(storage.ScopedReference); ok {
		check(&caps.Scope, v.CheckScopedReference)
	}
	if v, ok := value.(storage.DeleteIntent); ok {
		check(&caps.Delete, v.CheckDeleteIntent)
	}
	if v, ok := value.(storage.ConditionalFileMutation); ok {
		check(&caps.Conditional, v.CheckConditionalFileMutation)
	}
	return caps, failure
}

func (h *Handler) openReference(ctx context.Context, s *servedFileSession, req fileRequest) (fileResponse, error) {
	response := fileResponse{}
	s.mu.Lock()
	if len(s.files) >= s.options.MaxFiles {
		s.mu.Unlock()
		return response, syscall.EAGAIN
	}
	cap := fileCapability()
	entry := &servedFile{closing: true}
	s.files[cap] = entry
	s.mu.Unlock()
	var native storage.NodeReference
	var err error
	partialEffect := false
	switch req.Op {
	case storage.OpFileOpen:
		native, err = s.native.OpenFile(ctx, string(req.Path), req.Open)
	case storage.OpFileOpenNode:
		native, err = s.native.OpenNode(ctx, req.Node, req.Open)
	case storage.OpFileOpenAt:
		if v, ok := s.native.(storage.AtomicFileOpener); ok {
			if err = v.CheckAtomicFileOpen(); err == nil {
				var result storage.OpenResult
				result, err = v.OpenAt(ctx, *req.Child, req.OpenAt.storage())
				native = result.File
				partialEffect = result.File != nil || result.Attr.ID != 0
				if err == nil {
					response.Attr = AttrOf(result.Attr)
					response.Outcome = result.Outcome
				}
			}
		} else {
			err = syscall.EOPNOTSUPP
		}
	case storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		if v, ok := s.native.(storage.NodeReferences); ok {
			if err = v.CheckNodeReferences(); err == nil {
				var result storage.NodeOpenResult
				if req.Op == storage.OpFileOpenNodeRef {
					result, err = v.OpenNodeRef(ctx, req.Node, req.NodeRef.storage())
				} else {
					result, err = v.OpenChildRef(ctx, *req.Child, req.NodeRef.storage())
				}
				native = result.Reference
				partialEffect = result.Reference != nil || result.Attr.ID != 0
				if err == nil {
					response.Attr = AttrOf(result.Attr)
					response.Outcome = result.Outcome
				}
			}
		} else {
			err = syscall.EOPNOTSUPP
		}
	}
	if err == nil && native == nil {
		err = syscall.EIO
	}
	s.mu.Lock()
	if native == nil {
		delete(s.files, cap)
	} else {
		entry.native = native
		entry.closing = err != nil
		entry.pending = time.Now().Add(min(h.files.limits.PendingAck, s.options.Lease))
	}
	s.mu.Unlock()
	if err != nil {
		if native != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), min(h.files.limits.PendingAck, s.options.Lease))
			cleanupErr := native.Close(cleanup)
			cancel()
			s.mu.Lock()
			if cleanupErr == nil {
				delete(s.files, cap)
			} else {
				entry.closing = false
				entry.pending = time.Now()
			}
			s.mu.Unlock()
			err = errors.Join(err, cleanupErr)
		}
		if partialEffect {
			err = uncertainFileEffect(err)
		}
		return response, err
	}
	response.File = cap
	response.Capabilities, err = capabilitiesOf(native)
	if err != nil {
		return response, uncertainFileEffect(err)
	}
	if response.Capabilities.ReferenceName {
		node, identityErr := storage.ReferenceNodeID(native)
		if identityErr != nil {
			return response, uncertainFileEffect(identityErr)
		}
		switch req.Op {
		case storage.OpFileOpen:
			response.Node = node
			if req.Open.ExpectedID != 0 && node != req.Open.ExpectedID {
				err = errors.New("opened reference substituted its expected node identity")
			}
		case storage.OpFileOpenNode:
			response.Node = node
			if node != req.Node {
				err = errors.New("opened reference substituted its requested node identity")
			}
		default:
			if response.Attr == nil || response.Attr.ID != node {
				err = errors.New("opened reference disagrees with its captured attributes")
			}
		}
		if err != nil {
			return response, uncertainFileEffect(err)
		}
	}
	return h.finishFileMutation(ctx, response, nil)
}

func (h *Handler) performSessionCapability(ctx context.Context, s storage.FileSession, req fileRequest) (fileResponse, error) {
	out := fileResponse{}
	switch req.Op {
	case fileObserveDirectoryMetadata:
		return h.observeDirectoryMetadata(ctx, s, req)
	case storage.OpFileLookupAt, storage.OpFileReadDirNode, storage.OpFileMutateName:
		v, ok := s.(storage.NamespaceAccess)
		if !ok {
			return out, syscall.EOPNOTSUPP
		}
		if err := v.CheckNamespaceAccess(); err != nil {
			return out, err
		}
		if req.Op == storage.OpFileLookupAt {
			value, err := v.LookupAt(ctx, *req.Child)
			if err == nil {
				out.Attr = AttrOf(value)
			}
			return out, err
		}
		if req.Op == storage.OpFileReadDirNode {
			result, err := newObservedDirectoryResult(min(h.maxBodyBytes, req.ResultBytes))
			if err != nil {
				return out, err
			}
			observation, err := v.ReadDirNodeBounded(ctx, *req.Directory, result)
			if err != nil {
				return out, err
			}
			entries, err := result.Entries()
			if err != nil {
				return out, err
			}
			directory := storage.ObservedDirectory{Observation: observation, Entries: make([]storage.ObservedEntry, 0, len(entries))}
			for _, entry := range entries {
				directory.Entries = append(directory.Entries, storage.ObservedEntry{RawLeaf: []byte(entry.Name), Attr: entry.Attr})
			}
			out.Directory = observedDirectoryOf(directory)
			return out, nil
		}
		result, err := v.MutateName(ctx, req.Name.storage())
		if err != nil && result.Attr != nil {
			return out, uncertainFileEffect(err)
		}
		if err == nil && result.Attr == nil && nameResultNeedsAttr(req.Name.Kind) {
			return out, syscall.EIO
		}
		if err == nil && result.Attr != nil {
			out.Attr = AttrOf(*result.Attr)
		}
		return out, err
	case storage.OpFileSetNodeMetadata:
		v, ok := s.(storage.MetadataAccess)
		if !ok {
			return out, syscall.EOPNOTSUPP
		}
		if err := v.CheckMetadataAccess(); err != nil {
			return out, err
		}
		value, err := v.SetMetadata(ctx, req.Node, req.Namespace, req.Version, req.Payload)
		if err == nil {
			out.Metadata = &OpaquePayload{Version: value.Version, Data: append([]byte{}, value.Data...)}
		}
		return out, err
	case storage.OpFileNewUseOwner, storage.OpFileRetireUseOwner:
		v, ok := s.(storage.UseOwners)
		if !ok {
			return out, syscall.EOPNOTSUPP
		}
		if err := v.CheckUseOwners(); err != nil {
			return out, err
		}
		if req.Op == storage.OpFileNewUseOwner {
			owner, err := v.NewUseOwner(ctx, req.Node, *req.Scope, req.OwnerOptions)
			out.Owner = owner
			return out, err
		}
		return out, v.RetireUseOwner(ctx, req.Owner)
	case storage.OpFileRangeGetConflict, storage.OpFileRangeApply, storage.OpFileRangeQuery, storage.OpFileRangeCancel, storage.OpFileRangeDrop:
		v, ok := s.(storage.RangeControl)
		if !ok {
			return out, syscall.EOPNOTSUPP
		}
		if err := v.CheckRangeControl(); err != nil {
			return out, err
		}
		if req.Op == storage.OpFileRangeGetConflict {
			result, err := v.GetConflict(ctx, req.Owner, req.Commands[0])
			out.Conflict = &result
			return out, err
		}
		if req.Op == storage.OpFileRangeDrop {
			return out, v.Drop(ctx, req.Owner, req.Domain)
		}
		var result storage.RangeAttempt
		var err error
		switch req.Op {
		case storage.OpFileRangeApply:
			result, err = v.Apply(ctx, req.Owner, req.Commands, req.LockID)
		case storage.OpFileRangeQuery:
			result, err = v.Query(ctx, req.Owner, req.LockID)
		case storage.OpFileRangeCancel:
			result, err = v.Cancel(ctx, req.Owner, req.LockID)
		}
		if err == nil {
			copy := result.Clone()
			out.Attempt = &copy
		}
		return out, err
	}
	return out, syscall.EINVAL
}

func performReferenceCapability(ctx context.Context, ref storage.NodeReference, req fileRequest) (fileResponse, error) {
	out := fileResponse{}
	switch req.Op {
	case fileObserveName:
		return observeReferenceName(ctx, ref, req)
	case storage.OpFileState:
		v, ok := ref.(storage.ReferenceStateAccess)
		if !ok {
			return out, syscall.EOPNOTSUPP
		}
		if err := v.CheckReferenceState(); err != nil {
			return out, err
		}
		result, err := v.State(ctx)
		if err == nil {
			out.State = referenceStateOf(result)
		}
		return out, err
	case storage.OpFileScope:
		v, ok := ref.(storage.ScopedReference)
		if !ok {
			return out, syscall.EOPNOTSUPP
		}
		if err := v.CheckScopedReference(); err != nil {
			return out, err
		}
		result, err := v.Scope(ctx)
		out.Scope = &result
		return out, err
	case storage.OpFileSetMetadata:
		v, ok := ref.(storage.ReferenceMetadataAccess)
		if !ok {
			return out, syscall.EOPNOTSUPP
		}
		if err := v.CheckMetadataAccess(); err != nil {
			return out, err
		}
		result, err := v.SetMetadata(ctx, req.Namespace, req.Version, req.Payload)
		if err == nil {
			out.Metadata = &OpaquePayload{Version: result.Version, Data: append([]byte{}, result.Data...)}
		}
		return out, err
	case storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink:
		v, ok := ref.(storage.DeleteIntent)
		if !ok {
			return out, syscall.EOPNOTSUPP
		}
		if err := v.CheckDeleteIntent(); err != nil {
			return out, err
		}
		var result storage.ReferenceState
		var err error
		if req.Op == storage.OpFileSetPendingUnlink {
			result, err = v.SetPendingUnlink(ctx, *req.Pending)
		} else {
			result, err = v.ClearPendingUnlink(ctx, *req.ClearPending)
		}
		if err == nil {
			out.State = referenceStateOf(result)
		}
		return out, err
	case storage.OpFileMutate:
		v, ok := ref.(storage.ConditionalFileMutation)
		if !ok {
			return out, syscall.EOPNOTSUPP
		}
		if err := v.CheckConditionalFileMutation(); err != nil {
			return out, err
		}
		result, err := v.MutateFile(ctx, req.Mutation.storage())
		if err == nil {
			out.Attr = AttrOf(result)
		}
		return out, err
	}
	return out, syscall.EINVAL
}

func newObservedDirectoryResult(limit int64) (*storage.ListResult, error) {
	maximum := fileResponse{Epoch: ^uint64(0), Data: []byte{}, Directory: &observedDirectory{Observation: storage.DirectoryObservation{ParentID: ^uint64(0), Revision: make([]byte, storage.MaxObservationTokenBytes)}, Entries: []observedEntry{}}}
	encoded, err := json.Marshal(maximum)
	if err != nil {
		return nil, err
	}
	return storage.NewListResult(limit, int64(len(encoded)), func(index int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
		return listEntryWireBytes(index, nameBytes, metadataBytes, attr, limit, "rawLeaf")
	})
}
