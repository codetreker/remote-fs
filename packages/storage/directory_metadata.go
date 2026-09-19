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

// DirectoryMetadataObservation pairs a complete directory capture with its own
// binding only when requested. Name and Observation belong to the same guarded
// capture. Successful directory targets are Root or Linked, never Detached.
type DirectoryMetadataObservation struct {
	Observation DirectoryObservation
	Name        *NameObservation `json:",omitempty"`
}

// DirectoryMetadataObserver is an optional FileSession capability. It discloses
// metadata under the host's existing snapshot authorization, independently of
// application ReadEntries. Supplied scopes still require the exact live reference;
// invalid scopes never fall back to bare IDs. Guards, target and output share one
// native capture. No reference, action, journal entry or lifetime is created.
//
// result must be empty on entry. Names and metadata are reserved before loading;
// IncludeName reserves its actual binding prefix before any child reservation.
// Failure calls result.Fail and returns a zero observation. Entries are usable
// only after both this operation and result.Entries succeed. Missing capability
// yields EOPNOTSUPP from check and call. This facet is not required by FUSE.
type DirectoryMetadataObserver interface {
	CheckDirectoryMetadataObservation() error
	ObserveDirectoryMetadata(context.Context, DirectoryTarget, DirectoryMetadataOptions, *ListResult) (DirectoryMetadataObservation, error)
}

func (o DirectoryMetadataOptions) Check() error { return o.Guards.Check() }

// Check verifies response identity and requested output, without claiming that
// a later namespace state must still match the historical observation.
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
