package httprest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/codetreker/remote-fs/packages/storage"
)

// MetadataAccessWithBarrier exposes the replication point that follows a
// successful node metadata replacement.
type MetadataAccessWithBarrier interface {
	storage.MetadataAccess
	SetMetadataWithBarrier(context.Context, uint64, string, []byte, []byte) (storage.OpaquePayload, *MutationBarrier, error)
}

// ReferenceMetadataAccessWithBarrier is the retained-reference equivalent.
type ReferenceMetadataAccessWithBarrier interface {
	storage.ReferenceMetadataAccess
	SetMetadataWithBarrier(context.Context, string, []byte, []byte) (storage.OpaquePayload, *MutationBarrier, error)
}

// metadataVersion is request-side CAS state. Empty means the namespace must be
// absent; response-side OpaquePayload deliberately rejects an empty version.
type metadataVersion []byte
type metadataPayload []byte

func encodeCanonicalBytes(value []byte) ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString(value))
}

func (v metadataVersion) MarshalJSON() ([]byte, error) { return encodeCanonicalBytes(v) }
func (p metadataPayload) MarshalJSON() ([]byte, error) { return encodeCanonicalBytes(p) }

func decodeCanonicalBytes(data []byte, maximum int, field string) ([]byte, error) {
	if string(data) == "null" {
		return nil, errors.New(field + " cannot be null")
	}
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return nil, err
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, errors.New(field + " requires canonical base64")
	}
	if len(decoded) > maximum {
		return nil, errors.New(field + " exceeds its bound")
	}
	if len(decoded) == 0 {
		return nil, nil
	}
	return decoded, nil
}

func (v *metadataVersion) UnmarshalJSON(data []byte) error {
	decoded, err := decodeCanonicalBytes(data, storage.MaxObservationTokenBytes, "metadata version")
	if err != nil {
		return err
	}
	*v = metadataVersion(decoded)
	return nil
}

func (p *metadataPayload) UnmarshalJSON(data []byte) error {
	decoded, err := decodeCanonicalBytes(data, storage.MaxMetadataValueBytes, "metadata payload")
	if err != nil {
		return err
	}
	*p = metadataPayload(decoded)
	return nil
}
