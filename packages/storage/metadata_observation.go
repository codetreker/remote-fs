package storage

import (
	"bytes"
	"strings"
	"syscall"
)

type MetadataObservationPurpose uint8

const (
	MetadataObservationContentEffect MetadataObservationPurpose = iota + 1
	MetadataObservationNamespace
)

// MetadataObservationRequest selects either an enrolled content effect or a
// bounded namespace condition. The selectors have separate permissions and
// cannot be combined. Namespace is checked against current host policy per call.
type MetadataObservationRequest struct {
	Purpose         MetadataObservationPurpose
	EffectIndex     uint16
	Namespace       string
	MaxPayloadBytes int
}

func (r MetadataObservationRequest) Check() error {
	switch r.Purpose {
	case MetadataObservationContentEffect:
		if r.Namespace != "" || r.MaxPayloadBytes != 0 {
			return syscall.EINVAL
		}
	case MetadataObservationNamespace:
		if r.EffectIndex != 0 {
			return syscall.EINVAL
		}
		if err := CheckMetadataNamespace(r.Namespace); err != nil {
			return err
		}
		if r.MaxPayloadBytes < 0 {
			return syscall.EINVAL
		}
		if r.MaxPayloadBytes > MaxMetadataValueBytes {
			return syscall.EFBIG
		}
	default:
		return syscall.EINVAL
	}
	return nil
}

func (r MetadataObservationRequest) Clone() MetadataObservationRequest {
	r.Namespace = strings.Clone(r.Namespace)
	return r
}

// MetadataObservation owns exactly one namespace condition. Nil Value is
// confirmed absence; a nonnil value with empty Data is present. Failed queries
// return zero. Backends separately bound their decoding scratch space.
type MetadataObservation struct {
	NodeID uint64
	Value  *OpaquePayload
}

func (o MetadataObservation) Clone() MetadataObservation {
	if o.Value != nil {
		value := OpaquePayload{Version: bytes.Clone(o.Value.Version), Data: bytes.Clone(o.Value.Data)}
		o.Value = &value
	}
	return o
}

func (o MetadataObservation) Check() error {
	if o.NodeID == 0 {
		return syscall.EIO
	}
	if o.Value != nil && (len(o.Value.Version) == 0 || len(o.Value.Version) > MaxObservationTokenBytes || len(o.Value.Data) > MaxMetadataValueBytes) {
		return syscall.EIO
	}
	return nil
}

func (o MetadataObservation) CheckNamespace(nodeID uint64, namespace string, maxPayloadBytes int) error {
	request := MetadataObservationRequest{Purpose: MetadataObservationNamespace, Namespace: namespace, MaxPayloadBytes: maxPayloadBytes}
	if err := request.Check(); err != nil {
		return err
	}
	if err := o.Check(); err != nil {
		return err
	}
	if nodeID == 0 || o.NodeID != nodeID {
		return syscall.EIO
	}
	if o.Value != nil && len(o.Value.Data) > maxPayloadBytes {
		return syscall.EFBIG
	}
	return nil
}
