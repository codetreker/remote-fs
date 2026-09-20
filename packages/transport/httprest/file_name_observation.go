package httprest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type directoryObservation struct {
	ParentID uint64          `json:"parentId"`
	Revision metadataVersion `json:"revision"`
}

func directoryObservationOf(value storage.DirectoryObservation) directoryObservation {
	return directoryObservation{ParentID: value.ParentID, Revision: metadataVersion(bytes.Clone(value.Revision))}
}

func (value directoryObservation) storage() storage.DirectoryObservation {
	return storage.DirectoryObservation{ParentID: value.ParentID, Revision: bytes.Clone(value.Revision)}
}

type observedEdge struct {
	ParentID uint64         `json:"parentId"`
	RawLeaf  canonicalBytes `json:"rawLeaf"`
	ChildID  uint64         `json:"childId"`
}

func observedEdgeOf(value storage.ObservedEdge) observedEdge {
	return observedEdge{ParentID: value.ParentID, RawLeaf: canonicalBytes(bytes.Clone(value.RawLeaf)), ChildID: value.ChildID}
}

func (value observedEdge) storage() storage.ObservedEdge {
	return storage.ObservedEdge{ParentID: value.ParentID, RawLeaf: bytes.Clone(value.RawLeaf), ChildID: value.ChildID}
}

type namespaceGuards struct {
	Directories []directoryObservation `json:"directories,omitempty"`
	Edges       []observedEdge         `json:"edges,omitempty"`
	RootID      uint64                 `json:"rootId"`
}

func namespaceGuardsOf(value *storage.NamespaceGuards) *namespaceGuards {
	if value == nil {
		return nil
	}
	result := &namespaceGuards{RootID: value.RootID}
	if value.Directories != nil {
		result.Directories = make([]directoryObservation, len(value.Directories))
		for index, observation := range value.Directories {
			result.Directories[index] = directoryObservationOf(observation)
		}
	}
	if value.Edges != nil {
		result.Edges = make([]observedEdge, len(value.Edges))
		for index, edge := range value.Edges {
			result.Edges[index] = observedEdgeOf(edge)
		}
	}
	return result
}

func (value *namespaceGuards) storage() *storage.NamespaceGuards {
	if value == nil {
		return nil
	}
	result := &storage.NamespaceGuards{RootID: value.RootID}
	if value.Directories != nil {
		result.Directories = make([]storage.DirectoryObservation, len(value.Directories))
		for index, observation := range value.Directories {
			result.Directories[index] = observation.storage()
		}
	}
	if value.Edges != nil {
		result.Edges = make([]storage.ObservedEdge, len(value.Edges))
		for index, edge := range value.Edges {
			result.Edges[index] = edge.storage()
		}
	}
	return result
}

type nameObservation struct {
	NodeID   uint64                   `json:"nodeId"`
	State    storage.NameBindingState `json:"state"`
	ParentID uint64                   `json:"parentId"`
	RawLeaf  *canonicalBytes          `json:"rawLeaf,omitempty"`
}

func nameObservationOf(value storage.NameObservation) *nameObservation {
	result := &nameObservation{NodeID: value.NodeID, State: value.State, ParentID: value.ParentID}
	if value.RawLeaf != nil {
		leaf := canonicalBytes(bytes.Clone(value.RawLeaf))
		result.RawLeaf = &leaf
	}
	return result
}

func (value nameObservation) storage() storage.NameObservation {
	result := storage.NameObservation{NodeID: value.NodeID, State: value.State, ParentID: value.ParentID}
	if value.RawLeaf != nil {
		result.RawLeaf = bytes.Clone(*value.RawLeaf)
	}
	return result
}

type directoryMetadataOptions struct {
	Guards      *namespaceGuards `json:"guards,omitempty"`
	IncludeName bool             `json:"includeName"`
}

func directoryMetadataOptionsOf(value storage.DirectoryMetadataOptions) *directoryMetadataOptions {
	return &directoryMetadataOptions{Guards: namespaceGuardsOf(value.Guards), IncludeName: value.IncludeName}
}

func (value directoryMetadataOptions) storage() storage.DirectoryMetadataOptions {
	return storage.DirectoryMetadataOptions{Guards: value.Guards.storage(), IncludeName: value.IncludeName}
}

type observedDirectory struct {
	Observation directoryObservation `json:"observation"`
	Entries     []observedEntry      `json:"entries"`
	Name        *nameObservation     `json:"name,omitempty"`
}

type observedEntry struct {
	RawLeaf canonicalBytes `json:"rawLeaf"`
	Attr    *Attr          `json:"attr"`
}

func observedDirectoryOf(value storage.ObservedDirectory) *observedDirectory {
	result := &observedDirectory{Observation: directoryObservationOf(value.Observation), Entries: make([]observedEntry, 0, len(value.Entries))}
	for _, entry := range value.Entries {
		result.Entries = append(result.Entries, observedEntry{RawLeaf: canonicalBytes(bytes.Clone(entry.RawLeaf)), Attr: AttrOf(entry.Attr)})
	}
	return result
}

func (value observedDirectory) check() error {
	if value.Entries == nil {
		return errors.New("directory response carries no listing")
	}
	for _, entry := range value.Entries {
		if entry.Attr == nil {
			return errors.New("directory entry carries no attributes")
		}
	}
	return value.storage().Check()
}

func (value observedDirectory) storage() storage.ObservedDirectory {
	result := storage.ObservedDirectory{Observation: value.Observation.storage(), Entries: make([]storage.ObservedEntry, 0, len(value.Entries))}
	for _, entry := range value.Entries {
		result.Entries = append(result.Entries, storage.ObservedEntry{RawLeaf: bytes.Clone(entry.RawLeaf), Attr: entry.Attr.Storage()})
	}
	return result
}

func nameObservationWireBudget(limit int64, directory bool) storage.NameObservationBudget {
	return func(scalar storage.NameObservation, leafBytes int64) (int64, error) {
		retained, err := storage.NameObservationRetentionBytes(leafBytes)
		if err != nil {
			return 0, err
		}
		wire := nameObservationOf(scalar)
		wire.RawLeaf = nil
		encoded, err := json.Marshal(wire)
		if err != nil {
			return 0, err
		}
		wireBytes := int64(len(encoded))
		if directory {
			wireBytes += int64(len(`,"name":`))
		} else {
			response, marshalErr := json.Marshal(fileResponse{Epoch: math.MaxUint64, Data: []byte{}, NameObservation: wire})
			if marshalErr != nil {
				return 0, marshalErr
			}
			wireBytes = int64(len(response))
		}
		if leafBytes != 0 {
			if leafBytes > int64(math.MaxInt) {
				return 0, syscall.EFBIG
			}
			wireBytes += int64(len(`,"rawLeaf":""`)) + int64(base64.StdEncoding.EncodedLen(int(leafBytes)))
		}
		if wireBytes > limit-retained {
			return 0, syscall.EFBIG
		}
		return retained + wireBytes, nil
	}
}

func newObservedDirectoryResult(limit int64) (*storage.ListResult, error) {
	maximum := fileResponse{
		Epoch: math.MaxUint64,
		Data:  []byte{},
		Directory: &observedDirectory{
			Observation: directoryObservation{ParentID: math.MaxUint64, Revision: make([]byte, storage.MaxObservationTokenBytes)},
			Entries:     []observedEntry{},
		},
	}
	encoded, err := json.Marshal(maximum)
	if err != nil {
		return nil, err
	}
	return storage.NewListResult(limit, int64(len(encoded)), func(index int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
		if index >= storage.MaxDirectoryEntries {
			return 0, syscall.EFBIG
		}
		if nameBytes > storage.MaxLeafBytes {
			return 0, syscall.ENAMETOOLONG
		}
		encodedAttr, err := json.Marshal(AttrOf(attr))
		if err != nil {
			return 0, err
		}
		if nameBytes > limit || nameBytes > int64(math.MaxInt) {
			return 0, syscall.EFBIG
		}
		entryBytes := int64(len(`{"rawLeaf":"","attr":}`)) + int64(base64.StdEncoding.EncodedLen(int(nameBytes))) + int64(len(encodedAttr))
		metadataBytes, err = metadataResultBytes(metadataBytes)
		if err != nil {
			return 0, err
		}
		entryBytes += metadataBytes
		if index != 0 {
			entryBytes++
		}
		return entryBytes, nil
	})
}

func (h *Handler) observeDirectoryMetadata(ctx context.Context, session storage.FileSession, request fileRequest) (fileResponse, error) {
	observer, ok := session.(storage.DirectoryMetadataObserver)
	if !ok {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if err := observer.CheckDirectoryMetadataObservation(); err != nil {
		return fileResponse{}, err
	}
	result, err := newObservedDirectoryResult(min(h.maxBodyBytes, request.ResultBytes))
	if err != nil {
		return fileResponse{}, err
	}
	options := request.DirectoryMetadata.storage()
	observation, err := observer.ObserveDirectoryMetadata(ctx, *request.Directory, options, result)
	if err != nil {
		result.Fail(err)
		return fileResponse{}, err
	}
	if err := observation.Check(*request.Directory, options); err != nil {
		result.Fail(err)
		return fileResponse{}, err
	}
	entries, err := result.Entries()
	if err != nil {
		return fileResponse{}, err
	}
	observed := storage.ObservedDirectory{Observation: observation.Observation, Entries: make([]storage.ObservedEntry, 0, len(entries))}
	for _, entry := range entries {
		observed.Entries = append(observed.Entries, storage.ObservedEntry{RawLeaf: []byte(entry.Name), Attr: entry.Attr})
	}
	if err := observed.Check(); err != nil {
		result.Fail(err)
		return fileResponse{}, err
	}
	directory := &observedDirectory{Observation: directoryObservationOf(observation.Observation), Entries: make([]observedEntry, 0, len(entries))}
	if observation.Name != nil {
		directory.Name = nameObservationOf(*observation.Name)
	}
	for _, entry := range observed.Entries {
		directory.Entries = append(directory.Entries, observedEntry{RawLeaf: canonicalBytes(bytes.Clone(entry.RawLeaf)), Attr: AttrOf(entry.Attr)})
	}
	return fileResponse{Directory: directory}, nil
}

func observeReferenceName(ctx context.Context, reference retainedReference, request fileRequest) (fileResponse, error) {
	observer, ok := reference.(storage.ReferenceNameObserver)
	if !ok {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if err := observer.CheckReferenceNameObservation(); err != nil {
		return fileResponse{}, err
	}
	node, err := storage.ReferenceNodeID(reference)
	if err != nil {
		return fileResponse{}, err
	}
	observation, err := observer.ObserveName(ctx, request.Guards.storage())
	if err != nil {
		return fileResponse{}, err
	}
	if err := observation.Check(); err != nil {
		return fileResponse{}, err
	}
	if observation.NodeID != node {
		return fileResponse{}, errors.Join(errors.New("name observation substituted reference identity"), syscall.EIO)
	}
	return fileResponse{NameObservation: nameObservationOf(observation)}, nil
}

func (session *remoteFileSession) CheckDirectoryMetadataObservation() error {
	return checkFileCapability(session.capabilities.DirectoryMetadata)
}

func (session *remoteFileSession) ReadDirNode(ctx context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	if err := session.CheckNamespaceAccess(); err != nil {
		return storage.ObservedDirectory{}, err
	}
	if err := target.Check(); err != nil {
		return storage.ObservedDirectory{}, err
	}
	response, err := session.call(ctx, fileRequest{Op: storage.OpFileReadDirNode, Directory: &target, ResultBytes: session.storage.maxBodyBytes})
	if err != nil {
		return storage.ObservedDirectory{}, err
	}
	return response.Directory.storage(), nil
}

func (session *remoteFileSession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (observation storage.DirectoryObservation, returned error) {
	if result == nil {
		return observation, syscall.EINVAL
	}
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	if err := session.CheckNamespaceAccess(); err != nil {
		return observation, err
	}
	if err := target.Check(); err != nil {
		return observation, err
	}
	limit := min(session.storage.maxBodyBytes, result.MaxBytes())
	if limit <= 0 {
		return observation, syscall.EFBIG
	}
	response, err := session.call(ctx, fileRequest{Op: storage.OpFileReadDirNode, Directory: &target, ResultBytes: limit})
	if err != nil {
		return observation, err
	}
	for _, entry := range response.Directory.Entries {
		if err := result.Add(storage.Entry{Name: string(entry.RawLeaf), Attr: entry.Attr.Storage()}); err != nil {
			return observation, err
		}
	}
	return response.Directory.Observation.storage(), nil
}

func (session *remoteFileSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (observation storage.DirectoryMetadataObservation, returned error) {
	if result == nil {
		return observation, syscall.EINVAL
	}
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	if err := session.CheckDirectoryMetadataObservation(); err != nil {
		return observation, err
	}
	if err := target.Check(); err != nil {
		return observation, err
	}
	if err := options.Check(); err != nil {
		return observation, err
	}
	limit := min(session.storage.maxBodyBytes, result.MaxBytes())
	if limit <= 0 {
		return observation, syscall.EFBIG
	}
	response, err := session.call(ctx, fileRequest{Op: storage.OpFileObserveDirectoryMetadata, Directory: &target, DirectoryMetadata: directoryMetadataOptionsOf(options), ResultBytes: limit})
	if err != nil {
		return observation, err
	}
	if response.Directory.Name != nil {
		name := response.Directory.Name.storage()
		scalar := name
		scalar.RawLeaf = nil
		charge, err := storage.CheckNameObservationBudget(ctx, scalar, int64(len(name.RawLeaf)))
		if err != nil {
			return observation, err
		}
		if err := result.ReservePrefix(charge); err != nil {
			return observation, err
		}
		observation.Name = &name
	}
	for _, entry := range response.Directory.Entries {
		if err := result.Add(storage.Entry{Name: string(entry.RawLeaf), Attr: entry.Attr.Storage()}); err != nil {
			return storage.DirectoryMetadataObservation{}, err
		}
	}
	observation.Observation = response.Directory.Observation.storage()
	return observation, nil
}

func (file *remoteFile) CheckReferenceNameObservation() error {
	if err := checkFileCapability(file.capabilities.ReferenceName); err != nil {
		return err
	}
	_, err := storage.ReferenceNodeID(file)
	return err
}

func (file *remoteFile) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	if err := file.CheckReferenceNameObservation(); err != nil {
		return storage.NameObservation{}, err
	}
	if err := guards.Check(); err != nil {
		return storage.NameObservation{}, err
	}
	response, err := file.call(ctx, fileRequest{Op: storage.OpFileObserveName, Guards: namespaceGuardsOf(guards), ResultBytes: file.session.storage.maxBodyBytes})
	if err != nil {
		return storage.NameObservation{}, err
	}
	observation := response.NameObservation.storage()
	if observation.NodeID != file.node {
		return storage.NameObservation{}, unreachable(Request{Op: OpFile}, errors.New("name observation substituted reference identity"))
	}
	scalar := observation
	scalar.RawLeaf = nil
	if _, err := storage.CheckNameObservationBudget(ctx, scalar, int64(len(observation.RawLeaf))); err != nil {
		return storage.NameObservation{}, err
	}
	return observation, nil
}

func (reference *remoteNodeReference) CheckReferenceNameObservation() error {
	return reference.file.CheckReferenceNameObservation()
}

func (reference *remoteNodeReference) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	return reference.file.ObserveName(ctx, guards)
}

func (file *remoteFile) ReferenceNodeID() (uint64, error) {
	if file.node == 0 {
		return 0, syscall.EOPNOTSUPP
	}
	return file.node, nil
}

func (reference *remoteNodeReference) ReferenceNodeID() (uint64, error) {
	return reference.file.ReferenceNodeID()
}

var _ storage.DirectoryMetadataObserver = (*remoteFileSession)(nil)
var _ storage.ReferenceNameObserver = (*remoteFile)(nil)
var _ storage.ReferenceNameObserver = (*remoteNodeReference)(nil)
var _ storage.ReferenceIdentity = (*remoteFile)(nil)
var _ storage.ReferenceIdentity = (*remoteNodeReference)(nil)
