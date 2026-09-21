package storage

import (
	"context"
	"fmt"
	"syscall"
)

type DirectoryMetadataOptions struct {
	Guards      *NamespaceGuards `json:",omitempty"`
	IncludeName bool
}

// DirectoryMetadataObservation pairs a complete directory capture with its
// own current binding when requested. Both belong to one guarded capture.
type DirectoryMetadataObservation struct {
	Observation DirectoryObservation
	Name        *NameObservation `json:",omitempty"`
}

// DirectoryMetadataObserver is an optional FileSession capability. It exposes
// snapshot-authorized metadata independently of application ReadEntries. The
// result collector must be empty on entry. Producers reserve names, metadata
// and any requested current-name prefix before loading their variable payloads.
// Any failure invalidates result; entries become usable only after both this
// call and result.Entries succeed.
type DirectoryMetadataObserver interface {
	CheckDirectoryMetadataObservation() error
	ObserveDirectoryMetadata(context.Context, DirectoryTarget, DirectoryMetadataOptions, *ListResult) (DirectoryMetadataObservation, error)
}

func (o DirectoryMetadataOptions) Check() error { return o.Guards.Check() }

func (o DirectoryMetadataOptions) Clone() DirectoryMetadataOptions {
	o.Guards = o.Guards.Clone()
	return o
}

// Check verifies the captured directory identity and requested name output.
func (o DirectoryMetadataObservation) Check(target DirectoryTarget, options DirectoryMetadataOptions) error {
	if err := target.Check(); err != nil {
		return err
	}
	if err := options.Check(); err != nil {
		return err
	}
	if err := o.Observation.Check(); err != nil {
		return err
	}
	if o.Observation.ParentID != target.NodeID {
		return fmt.Errorf("directory observation substituted its target identity: %w", syscall.EIO)
	}
	if options.IncludeName != (o.Name != nil) {
		return fmt.Errorf("directory observation does not match requested name output: %w", syscall.EIO)
	}
	if o.Name == nil {
		return nil
	}
	if err := o.Name.Check(); err != nil {
		return err
	}
	if o.Name.NodeID != o.Observation.ParentID || o.Name.State == NameDetached {
		return fmt.Errorf("directory name observation has inconsistent identity or linkedness: %w", syscall.EIO)
	}
	return nil
}

func (o DirectoryMetadataObservation) Clone() DirectoryMetadataObservation {
	o.Observation = o.Observation.Clone()
	if o.Name != nil {
		name := o.Name.Clone()
		o.Name = &name
	}
	return o
}
