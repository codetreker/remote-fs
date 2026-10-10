package storage

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"syscall"
)

const MaxContentMetadataEffects = MaxMetadataNamespaces
const MaxContentMetadataEffectBytes = MaxMetadataBytes

// ContentMetadataEffect seals a fixed transformation into a writable reference.
// Prefix bytes are immutable; every payload byte has explicit clear and set masks.
// Absence has one enrolled initial payload rather than an inferred default.
type ContentMetadataEffect struct {
	Namespace     string
	PayloadBytes  int
	PresentPrefix []byte
	AbsentPayload []byte
	ClearMask     []byte
	SetMask       []byte
}

// OpenContentMetadata verifies enrollment support through the complete backing chain.
type OpenContentMetadata interface{ CheckOpenContentMetadata() error }

// ReferenceContentMetadata observes only an enrolled namespace through the exact
// live writable reference, without granting public metadata read or write access.
type ReferenceContentMetadata interface {
	CheckContentMetadata() error
	ObserveContentMetadata(context.Context, uint16) (ContentMetadataObservation, error)
}

type ContentMetadataObservation struct {
	NodeID uint64
	Value  *OpaquePayload
}

func (e ContentMetadataEffect) Check() error {
	if err := CheckMetadataNamespace(e.Namespace); err != nil {
		return err
	}
	if e.PayloadBytes < 1 {
		return syscall.EINVAL
	}
	if e.PayloadBytes > MaxMetadataValueBytes {
		return syscall.EFBIG
	}
	if len(e.ClearMask) != e.PayloadBytes || len(e.SetMask) != e.PayloadBytes || len(e.AbsentPayload) != e.PayloadBytes || len(e.PresentPrefix) > e.PayloadBytes {
		return syscall.EINVAL
	}
	if !bytes.HasPrefix(e.AbsentPayload, e.PresentPrefix) {
		return syscall.EINVAL
	}
	for i := range e.PresentPrefix {
		if e.ClearMask[i] != 0 || e.SetMask[i] != 0 {
			return syscall.EINVAL
		}
	}
	return nil
}

func CheckContentMetadataEffects(effects []ContentMetadataEffect) error {
	if len(effects) > MaxContentMetadataEffects {
		return syscall.EFBIG
	}
	names := make(map[string]struct{}, len(effects))
	size := 0
	for _, e := range effects {
		if err := e.Check(); err != nil {
			return err
		}
		if _, ok := names[e.Namespace]; ok {
			return syscall.EINVAL
		}
		names[e.Namespace] = struct{}{}
		size += len(e.Namespace) + len(e.PresentPrefix) + len(e.AbsentPayload) + len(e.ClearMask) + len(e.SetMask)
		if size > MaxContentMetadataEffectBytes {
			return syscall.EFBIG
		}
	}
	return nil
}

func CloneContentMetadataEffects(effects []ContentMetadataEffect) []ContentMetadataEffect {
	if len(effects) == 0 {
		return nil
	}
	cloned := slices.Clone(effects)
	for i := range cloned {
		e := &cloned[i]
		e.Namespace = strings.Clone(e.Namespace)
		e.PresentPrefix = bytes.Clone(e.PresentPrefix)
		e.AbsentPayload = bytes.Clone(e.AbsentPayload)
		e.ClearMask = bytes.Clone(e.ClearMask)
		e.SetMask = bytes.Clone(e.SetMask)
	}
	return cloned
}

func ResolveContentMetadataEffects(effects []ContentMetadataEffect, indices []uint16) ([]ContentMetadataEffect, error) {
	if err := CheckContentMetadataEffects(effects); err != nil {
		return nil, err
	}
	if len(indices) > MaxContentMetadataEffects {
		return nil, syscall.EFBIG
	}
	selected := make([]ContentMetadataEffect, 0, len(indices))
	seen := make(map[uint16]struct{}, len(indices))
	for _, i := range indices {
		if int(i) >= len(effects) {
			return nil, syscall.EINVAL
		}
		if _, ok := seen[i]; ok {
			return nil, syscall.EINVAL
		}
		seen[i] = struct{}{}
		selected = append(selected, effects[i])
	}
	return CloneContentMetadataEffects(selected), nil
}

// CheckContentMutation requires an explicit condition even for an absent namespace.
// Effects cannot be used as arbitrary metadata writes or as empty data operations.
func CheckContentMutation(command FileMutation, effects []ContentMetadataEffect) error {
	if err := command.Check(); err != nil {
		return err
	}
	selected, err := ResolveContentMetadataEffects(effects, command.ContentEffects)
	if err != nil {
		return err
	}
	for _, e := range selected {
		if _, ok := command.ExpectedMetadata[e.Namespace]; !ok {
			return syscall.EINVAL
		}
	}
	return nil
}

func (o ContentMetadataObservation) Clone() ContentMetadataObservation {
	if o.Value != nil {
		value := OpaquePayload{Version: bytes.Clone(o.Value.Version), Data: bytes.Clone(o.Value.Data)}
		o.Value = &value
	}
	return o
}

func (o ContentMetadataObservation) Check() error {
	if o.NodeID == 0 {
		return syscall.EIO
	}
	if o.Value != nil && (len(o.Value.Version) == 0 || len(o.Value.Version) > MaxObservationTokenBytes || len(o.Value.Data) > MaxMetadataValueBytes) {
		return syscall.EIO
	}
	return nil
}

func (o ContentMetadataObservation) CheckEffect(nodeID uint64, effect ContentMetadataEffect) error {
	if nodeID == 0 || o.NodeID != nodeID {
		return syscall.EIO
	}
	if err := effect.Check(); err != nil {
		return err
	}
	if o.Value == nil {
		return nil
	}
	if len(o.Value.Version) == 0 || len(o.Value.Version) > MaxObservationTokenBytes || len(o.Value.Data) != effect.PayloadBytes || !bytes.HasPrefix(o.Value.Data, effect.PresentPrefix) {
		return syscall.EIO
	}
	return nil
}

// Apply uses only the sealed payload form. A malformed existing value is an I/O
// failure; replacing it with the absence payload would conceal corruption.
func (e ContentMetadataEffect) Apply(value *OpaquePayload) ([]byte, error) {
	if err := e.Check(); err != nil {
		return nil, err
	}
	source := e.AbsentPayload
	if value != nil {
		if len(value.Version) == 0 || len(value.Version) > MaxObservationTokenBytes || len(value.Data) != e.PayloadBytes || !bytes.HasPrefix(value.Data, e.PresentPrefix) {
			return nil, syscall.EIO
		}
		source = value.Data
	}
	result := bytes.Clone(source)
	for i := range result {
		result[i] = (result[i] &^ e.ClearMask[i]) | e.SetMask[i]
	}
	return result, nil
}
