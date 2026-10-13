package httprest

import (
	"bytes"
	"errors"

	"github.com/codetreker/remote-fs/packages/storage"
)

func referenceStateOfProjection(value storage.ReferenceState, projection storage.StateProjection) *referenceState {
	if projection == storage.StateFull {
		return referenceStateOf(value)
	}
	state := &referenceState{Attr: &stateAttrJSON{ID: value.Attr.ID, Kind: value.Attr.Kind, Size: value.Attr.Size, AllocationSize: value.Attr.AllocationSize, AllocationKnown: value.Attr.AllocationKnown}, Detached: value.Detached, PendingUnlink: value.PendingUnlink}
	if value.PendingUnlink {
		generation := metadataVersion(bytes.Clone(value.PendingGeneration))
		state.PendingGeneration = &generation
	}
	return state
}

func validateReferenceStateProjection(state *referenceState, projection storage.StateProjection) error {
	if err := projection.Check(); err != nil {
		return err
	}
	if state.Attr == nil {
		return errors.New("reference state is incomplete")
	}
	if projection == storage.StateFull {
		return validateReferenceState(state)
	}
	attr := state.Attr
	if attr.AccessTime != nil || attr.ModTime != nil || attr.BirthTime != nil || attr.ChangeTime != nil || attr.Metadata != nil || state.LinkTarget != nil {
		return errors.New("scalar reference state exposes protected facts")
	}
	if (state.PendingGeneration != nil) != state.PendingUnlink {
		return errors.New("scalar reference state carries inconsistent pending generation")
	}
	return state.storage().CheckProjection(attr.ID, projection)
}

type stateAttrJSON struct {
	ID              uint64                   `json:"id"`
	Kind            storage.NodeKind         `json:"kind"`
	Size            int64                    `json:"size"`
	AllocationSize  int64                    `json:"allocation_size"`
	AllocationKnown bool                     `json:"allocation_known"`
	AccessTime      *Time                    `json:"access_time,omitempty"`
	ModTime         *Time                    `json:"mod_time,omitempty"`
	BirthTime       *Time                    `json:"birth_time,omitempty"`
	ChangeTime      *Time                    `json:"change_time,omitempty"`
	Metadata        map[string]OpaquePayload `json:"metadata,omitempty"`
}

func stateAttrOf(value storage.Attr) *stateAttrJSON {
	access, modified := TimeOf(value.AccessTime), TimeOf(value.ModTime)
	return &stateAttrJSON{ID: value.ID, Kind: value.Kind, Size: value.Size, AllocationSize: value.AllocationSize, AllocationKnown: value.AllocationKnown, AccessTime: &access, ModTime: &modified, BirthTime: optionalTimeOf(value.BirthTime), ChangeTime: optionalTimeOf(value.ChangeTime), Metadata: metadataOf(value.Metadata)}
}

func (value stateAttrJSON) Storage() storage.Attr {
	attr := storage.Attr{ID: value.ID, Kind: value.Kind, Size: value.Size, AllocationSize: value.AllocationSize, AllocationKnown: value.AllocationKnown, BirthTime: optionalTimeStorage(value.BirthTime), ChangeTime: optionalTimeStorage(value.ChangeTime), Metadata: metadataStorage(value.Metadata)}
	if value.AccessTime != nil {
		attr.AccessTime = value.AccessTime.Time()
	}
	if value.ModTime != nil {
		attr.ModTime = value.ModTime.Time()
	}
	return attr
}

func (value stateAttrJSON) check() error {
	if value.AccessTime == nil || value.ModTime == nil {
		return errors.New("full reference state omits required times")
	}
	return AttrOf(value.Storage()).check()
}

func (state referenceState) storage() storage.ReferenceState {
	value := storage.ReferenceState{Attr: state.Attr.Storage(), Detached: state.Detached, PendingUnlink: state.PendingUnlink}
	if state.LinkTarget != nil {
		value.LinkTarget = bytes.Clone(*state.LinkTarget)
	}
	if state.PendingGeneration != nil {
		value.PendingGeneration = bytes.Clone(*state.PendingGeneration)
	}
	return value
}
