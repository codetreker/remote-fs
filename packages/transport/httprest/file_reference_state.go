package httprest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func referenceStateOfProjection(value storage.ReferenceState, projection storage.StateProjection) *referenceState {
	result := referenceStateOf(value)
	result.scalar = projection == storage.StateScalars
	return result
}

func (state referenceState) MarshalJSON() ([]byte, error) {
	type stateWire referenceState
	if !state.scalar {
		return json.Marshal(stateWire(state))
	}
	if state.Attr == nil {
		return nil, errors.New("reference state is incomplete")
	}
	scalar := struct {
		ID              uint64           `json:"id"`
		Kind            storage.NodeKind `json:"kind"`
		Size            int64            `json:"size"`
		AllocationSize  int64            `json:"allocation_size"`
		AllocationKnown bool             `json:"allocation_known"`
	}{state.Attr.ID, state.Attr.Kind, state.Attr.Size, state.Attr.AllocationSize, state.Attr.AllocationKnown}
	attr, err := json.Marshal(scalar)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Attr          json.RawMessage `json:"attr"`
		Detached      bool            `json:"detached"`
		PendingUnlink bool            `json:"pendingUnlink"`
	}{attr, state.Detached, state.PendingUnlink})
}

func (state *referenceState) UnmarshalJSON(data []byte) error {
	var raw struct {
		Attr              json.RawMessage `json:"attr"`
		Detached          bool            `json:"detached"`
		PendingUnlink     bool            `json:"pendingUnlink"`
		PendingGeneration metadataVersion `json:"pendingGeneration,omitempty"`
		LinkTarget        canonicalBytes  `json:"linkTarget,omitempty"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw.Attr, &members); err != nil {
		return err
	}
	_, access := members["access_time"]
	_, modified := members["mod_time"]
	scalar := !access && !modified
	var attr Attr
	if scalar {
		for _, name := range []string{"id", "kind", "size", "allocation_size", "allocation_known"} {
			value, ok := members[name]
			if !ok || bytes.Equal(value, []byte("null")) {
				return fmt.Errorf("scalar attributes carry no %s", name)
			}
		}
		var value stateAttrJSON
		if err := decodeFileJSON(raw.Attr, &value); err != nil {
			return err
		}
		attr = Attr{ID: value.ID, Kind: value.Kind, Size: value.Size, AllocationSize: value.AllocationSize, AllocationKnown: value.AllocationKnown, BirthTime: value.BirthTime, ChangeTime: value.ChangeTime, Metadata: value.Metadata}
		attr.AccessTime, attr.ModTime = TimeOf(time.Time{}), TimeOf(time.Time{})
	} else if err := json.Unmarshal(raw.Attr, &attr); err != nil {
		return err
	}
	protected := false
	for _, name := range []string{"access_time", "mod_time", "birth_time", "change_time", "metadata"} {
		if _, ok := members[name]; ok {
			protected = true
		}
	}
	if err := json.Unmarshal(data, &members); err != nil {
		return err
	}
	for _, name := range []string{"linkTarget", "pendingGeneration"} {
		if _, ok := members[name]; ok {
			protected = true
		}
	}
	*state = referenceState{Attr: &attr, Detached: raw.Detached, PendingUnlink: raw.PendingUnlink, PendingGeneration: raw.PendingGeneration, LinkTarget: raw.LinkTarget, scalar: scalar, protectedFields: protected}
	return nil
}

func validateReferenceStateProjection(state *referenceState, projection storage.StateProjection) error {
	if err := projection.Check(); err != nil {
		return err
	}
	if state.Attr == nil {
		return errors.New("reference state is incomplete")
	}
	if projection == storage.StateFull {
		if state.scalar {
			return errors.New("full reference state omits protected facts")
		}
		return validateReferenceState(state)
	}
	if state.protectedFields {
		return errors.New("scalar reference state exposes protected facts")
	}
	return state.storage().CheckProjection(state.Attr.ID, projection)
}

// State has one wire envelope. The reduced projection permits only the scalar
// members; full queries and mutation results still require complete Attr times.
type referenceStateJSON struct {
	Attr              stateAttrJSON   `json:"attr"`
	Detached          bool            `json:"detached"`
	PendingUnlink     bool            `json:"pendingUnlink"`
	PendingGeneration metadataVersion `json:"pendingGeneration,omitempty"`
	LinkTarget        canonicalBytes  `json:"linkTarget,omitempty"`
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
