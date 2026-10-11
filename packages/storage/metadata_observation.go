package storage

import (
	"context"
	"slices"
	"strings"
	"syscall"
)

// MetadataObservationScope enrolls a single bounded namespace for conditional
// metadata updates. A zero payload bound admits present empty values.
type MetadataObservationScope struct {
	Namespace       string
	MaxPayloadBytes int
}

// OpenMetadataObservation verifies enrollment support through the complete
// backing chain before an open can have effects.
type OpenMetadataObservation interface {
	CheckOpenMetadataObservation() error
}

// ReferenceMetadataObserver observes only an enrolled namespace through the
// exact live File or NodeReference. Enrollment and calls require WriteMetadata;
// they grant no ReadMetadata or additional metadata mutation permissions.
// The result owns its bytes, distinguishes absence from an empty present value,
// and is zero on every error. Backend scratch space remains bounded separately.
type ReferenceMetadataObserver interface {
	CheckMetadataObservation() error
	ObserveMetadata(context.Context, uint16) (ContentMetadataObservation, error)
}

func (s MetadataObservationScope) Check() error {
	if err := CheckMetadataNamespace(s.Namespace); err != nil {
		return err
	}
	if s.MaxPayloadBytes < 0 {
		return syscall.EINVAL
	}
	if s.MaxPayloadBytes > MaxMetadataValueBytes {
		return syscall.EFBIG
	}
	return nil
}

func CheckMetadataObservationScopes(scopes []MetadataObservationScope) error {
	if len(scopes) > MaxMetadataNamespaces {
		return syscall.EFBIG
	}
	seen := make(map[string]struct{}, len(scopes))
	bytes := 6
	for _, scope := range scopes {
		if err := scope.Check(); err != nil {
			return err
		}
		if _, exists := seen[scope.Namespace]; exists {
			return syscall.EINVAL
		}
		seen[scope.Namespace] = struct{}{}
		bytes += 8 + len(scope.Namespace) + MaxObservationTokenBytes + scope.MaxPayloadBytes
		if bytes > MaxMetadataBytes {
			return syscall.EFBIG
		}
	}
	return nil
}

func CloneMetadataObservationScopes(scopes []MetadataObservationScope) []MetadataObservationScope {
	if len(scopes) == 0 {
		return nil
	}
	result := slices.Clone(scopes)
	for i := range result {
		result[i].Namespace = strings.Clone(result[i].Namespace)
	}
	return result
}

func (o ContentMetadataObservation) CheckScope(nodeID uint64, scope MetadataObservationScope) error {
	if err := scope.Check(); err != nil {
		return err
	}
	if err := o.Check(); err != nil {
		return err
	}
	if nodeID == 0 || o.NodeID != nodeID {
		return syscall.EIO
	}
	if o.Value != nil && len(o.Value.Data) > scope.MaxPayloadBytes {
		return syscall.EFBIG
	}
	return nil
}
