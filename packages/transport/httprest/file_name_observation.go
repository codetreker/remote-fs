package httprest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func nameObservationWireBudget(limit int64, directory bool) storage.NameObservationBudget {
	return func(scalar storage.NameObservation, leafBytes int64) (int64, error) {
		retained, err := storage.NameObservationRetentionBytes(leafBytes)
		if err != nil {
			return 0, err
		}
		var encoded []byte
		if directory {
			encoded, err = json.Marshal(scalar)
		} else {
			encoded, err = json.Marshal(fileResponse{Epoch: math.MaxUint64, Data: []byte{}, NameObservation: &scalar})
		}
		if err != nil {
			return 0, err
		}
		charge := retained + int64(len(encoded))
		if directory {
			charge += int64(len(`,"name":`))
		}
		if leafBytes != 0 {
			charge += int64(len(`,"RawLeaf":""`)) + int64(base64.StdEncoding.EncodedLen(int(leafBytes)))
		}
		if charge > limit {
			return 0, syscall.EFBIG
		}
		return charge, nil
	}
}

func (h *Handler) observeDirectoryMetadata(ctx context.Context, session storage.FileSession, req fileRequest) (fileResponse, error) {
	observer, ok := session.(storage.DirectoryMetadataObserver)
	if !ok {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if err := observer.CheckDirectoryMetadataObservation(); err != nil {
		return fileResponse{}, err
	}
	result, err := newObservedDirectoryResult(min(h.maxBodyBytes, req.ResultBytes))
	if err != nil {
		return fileResponse{}, err
	}
	observation, err := observer.ObserveDirectoryMetadata(ctx, *req.Directory, *req.DirectoryMetadata, result)
	if err != nil {
		result.Fail(err)
		return fileResponse{}, err
	}
	if err := observation.Check(*req.Directory, *req.DirectoryMetadata); err != nil {
		result.Fail(err)
		return fileResponse{}, err
	}
	entries, err := result.Entries()
	if err != nil {
		return fileResponse{}, err
	}
	if len(entries) > storage.MaxDirectoryEntries {
		return fileResponse{}, syscall.EFBIG
	}
	directory := &observedDirectory{Observation: observation.Observation, Name: observation.Name, Entries: make([]observedEntry, 0, len(entries))}
	for _, entry := range entries {
		directory.Entries = append(directory.Entries, observedEntry{RawLeaf: []byte(entry.Name), Attr: AttrOf(entry.Attr)})
	}
	return fileResponse{Directory: directory}, nil
}

func observeReferenceName(ctx context.Context, reference storage.NodeReference, req fileRequest) (fileResponse, error) {
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
	observation, err := observer.ObserveName(ctx, req.Guards)
	if err != nil {
		return fileResponse{}, err
	}
	if err := observation.Check(); err != nil {
		return fileResponse{}, err
	}
	if observation.NodeID != node {
		return fileResponse{}, errors.New("name observation substituted reference identity")
	}
	return fileResponse{NameObservation: &observation}, nil
}

func (s *remoteFileSession) CheckDirectoryMetadataObservation() error {
	return checkFileCapability(s.capabilities.DirectoryMetadata)
}

func (s *remoteFileSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (observation storage.DirectoryMetadataObservation, returned error) {
	if result == nil {
		return observation, syscall.EINVAL
	}
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	if err := s.CheckDirectoryMetadataObservation(); err != nil {
		return observation, err
	}
	if err := target.Check(); err != nil {
		return observation, err
	}
	if err := options.Check(); err != nil {
		return observation, err
	}
	limit := min(s.storage.maxBodyBytes, result.MaxBytes())
	if limit <= 0 {
		return observation, syscall.EFBIG
	}
	response, err := s.call(ctx, fileRequest{Op: fileObserveDirectoryMetadata, Directory: &target, DirectoryMetadata: &options, ResultBytes: limit})
	if err != nil {
		return observation, err
	}
	if response.Directory.Name != nil {
		scalar := *response.Directory.Name
		scalar.RawLeaf = nil
		charge, err := storage.CheckNameObservationBudget(ctx, scalar, int64(len(response.Directory.Name.RawLeaf)))
		if err != nil {
			return observation, err
		}
		if err := result.ReservePrefix(charge); err != nil {
			return observation, err
		}
	}
	for _, entry := range response.Directory.Entries {
		if err := result.Add(storage.Entry{Name: string(entry.RawLeaf), Attr: entry.Attr.Storage()}); err != nil {
			return observation, err
		}
	}
	return storage.DirectoryMetadataObservation{Observation: response.Directory.Observation, Name: response.Directory.Name}, nil
}

func (f *remoteFile) CheckReferenceNameObservation() error {
	if err := checkFileCapability(f.capabilities.ReferenceName); err != nil {
		return err
	}
	_, err := storage.ReferenceNodeID(f)
	return err
}

func (f *remoteFile) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	if err := f.CheckReferenceNameObservation(); err != nil {
		return storage.NameObservation{}, err
	}
	if err := guards.Check(); err != nil {
		return storage.NameObservation{}, err
	}
	response, err := f.call(ctx, fileRequest{Op: fileObserveName, Guards: guards, ResultBytes: f.session.storage.maxBodyBytes})
	if err != nil {
		return storage.NameObservation{}, err
	}
	if response.NameObservation.NodeID != f.node {
		return storage.NameObservation{}, unreachable(Request{Op: OpFile}, errors.New("name observation substituted reference identity"))
	}
	scalar := *response.NameObservation
	scalar.RawLeaf = nil
	if _, err := storage.CheckNameObservationBudget(ctx, scalar, int64(len(response.NameObservation.RawLeaf))); err != nil {
		return storage.NameObservation{}, err
	}
	return *response.NameObservation, nil
}

func (r *remoteNodeReference) CheckReferenceNameObservation() error {
	return r.file.CheckReferenceNameObservation()
}

func (r *remoteNodeReference) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	return r.file.ObserveName(ctx, guards)
}

var _ storage.DirectoryMetadataObserver = (*remoteFileSession)(nil)
var _ storage.ReferenceNameObserver = (*remoteFile)(nil)
var _ storage.ReferenceNameObserver = (*remoteNodeReference)(nil)

func (f *remoteFile) ReferenceNodeID() (uint64, error) {
	if f.node == 0 {
		return 0, syscall.EOPNOTSUPP
	}
	return f.node, nil
}
func (r *remoteNodeReference) ReferenceNodeID() (uint64, error) { return r.file.ReferenceNodeID() }
