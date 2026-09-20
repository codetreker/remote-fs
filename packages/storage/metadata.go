package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"syscall"
)

type NodeKind uint8

const (
	NodeRegular NodeKind = iota + 1
	NodeDirectory
	NodeSymlink
)

func (k NodeKind) Check() error {
	if k < NodeRegular || k > NodeSymlink {
		return syscall.EINVAL
	}
	return nil
}

const (
	MaxMetadataNamespaces     = 16
	MaxMetadataNamespaceBytes = 128
	MaxMetadataValueBytes     = 32 << 10
	MaxMetadataBytes          = 64 << 10
	MaxObservationTokenBytes  = 64
)

// OpaquePayload is owned by its namespace's client. Version is an authority-issued
// equality token for that namespace only; Data may be empty but remains present.
type OpaquePayload struct{ Version, Data []byte }

func CheckMetadataNamespace(name string) error {
	if len(name) == 0 || len(name) > MaxMetadataNamespaceBytes {
		return syscall.EINVAL
	}
	for i := range name {
		c := name[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return syscall.EINVAL
		}
	}
	return nil
}
func metadataSize(values map[string]OpaquePayload) (int, error) {
	if len(values) > MaxMetadataNamespaces {
		return 0, syscall.EFBIG
	}
	size := 6
	for name, value := range values {
		if err := CheckMetadataNamespace(name); err != nil {
			return 0, err
		}
		if len(value.Version) == 0 || len(value.Version) > MaxObservationTokenBytes {
			return 0, syscall.EINVAL
		}
		if len(value.Data) > MaxMetadataValueBytes {
			return 0, syscall.EFBIG
		}
		size += 8 + len(name) + len(value.Version) + len(value.Data)
		if size > MaxMetadataBytes {
			return 0, syscall.EFBIG
		}
	}
	return size, nil
}
func CheckMetadata(values map[string]OpaquePayload) error { _, err := metadataSize(values); return err }

// CheckInitialMetadata bounds raw initial values before the authority assigns
// each namespace's first version. Nil and empty maps both initialize no values.
func CheckInitialMetadata(values map[string][]byte) error {
	if len(values) > MaxMetadataNamespaces {
		return syscall.EFBIG
	}
	size := 6
	for name, data := range values {
		if err := CheckMetadataNamespace(name); err != nil {
			return err
		}
		if len(data) > MaxMetadataValueBytes {
			return syscall.EFBIG
		}
		size += 8 + len(name) + MaxObservationTokenBytes + len(data)
		if size > MaxMetadataBytes {
			return syscall.EFBIG
		}
	}
	return nil
}
func CloneMetadata(values map[string]OpaquePayload) map[string]OpaquePayload {
	if values == nil {
		return nil
	}
	copy := make(map[string]OpaquePayload, len(values))
	for name, value := range values {
		copy[strings.Clone(name)] = OpaquePayload{Version: bytes.Clone(value.Version), Data: bytes.Clone(value.Data)}
	}
	return copy
}
func CloneInitialMetadata(values map[string][]byte) map[string][]byte {
	if values == nil {
		return nil
	}
	copy := make(map[string][]byte, len(values))
	for name, data := range values {
		copy[strings.Clone(name)] = bytes.Clone(data)
	}
	return copy
}

// EncodeMetadata preserves every namespace without interpreting platform bytes.
// Sorted keys and length prefixes make one canonical bounded representation.
func EncodeMetadata(values map[string]OpaquePayload) ([]byte, error) {
	size, err := metadataSize(values)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, size)
	out = append(out, 'R', 'F', 'M', 1)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(values)))
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := values[name]
		out = binary.LittleEndian.AppendUint16(out, uint16(len(name)))
		out = binary.LittleEndian.AppendUint16(out, uint16(len(value.Version)))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(value.Data)))
		out = append(out, name...)
		out = append(out, value.Version...)
		out = append(out, value.Data...)
	}
	return out, nil
}
func DecodeMetadata(data []byte) (map[string]OpaquePayload, error) {
	invalid := func() (map[string]OpaquePayload, error) {
		return nil, fmt.Errorf("invalid metadata envelope: %w", syscall.EIO)
	}
	if len(data) < 6 || len(data) > MaxMetadataBytes || !bytes.Equal(data[:4], []byte{'R', 'F', 'M', 1}) {
		return invalid()
	}
	count := int(binary.LittleEndian.Uint16(data[4:6]))
	if count > MaxMetadataNamespaces {
		return invalid()
	}
	data = data[6:]
	if count == 0 {
		if len(data) != 0 {
			return invalid()
		}
		return nil, nil
	}
	values := make(map[string]OpaquePayload, count)
	previous := ""
	for range count {
		if len(data) < 8 {
			return invalid()
		}
		keySize, versionSize, valueSize := int(binary.LittleEndian.Uint16(data[:2])), int(binary.LittleEndian.Uint16(data[2:4])), uint64(binary.LittleEndian.Uint32(data[4:8]))
		data = data[8:]
		if keySize == 0 || keySize > MaxMetadataNamespaceBytes || versionSize == 0 || versionSize > MaxObservationTokenBytes || valueSize > MaxMetadataValueBytes {
			return invalid()
		}
		if keySize > len(data) || versionSize > len(data)-keySize || valueSize > uint64(len(data)-keySize-versionSize) {
			return invalid()
		}
		key := string(data[:keySize])
		if key <= previous || CheckMetadataNamespace(key) != nil {
			return invalid()
		}
		previous = key
		values[key] = OpaquePayload{Version: bytes.Clone(data[keySize : keySize+versionSize]), Data: bytes.Clone(data[keySize+versionSize : keySize+versionSize+int(valueSize)])}
		data = data[keySize+versionSize+int(valueSize):]
	}
	if len(data) != 0 {
		return invalid()
	}
	if CheckMetadata(values) != nil {
		return invalid()
	}
	return values, nil
}

// Clone owns mutable payloads and optional instants; all instants are normalized
// to UTC so retained results do not retain caller-supplied location objects.
func (a Attr) Clone() Attr {
	a.AccessTime = a.AccessTime.UTC()
	a.ModTime = a.ModTime.UTC()
	if a.BirthTime != nil {
		value := a.BirthTime.UTC()
		a.BirthTime = &value
	}
	if a.ChangeTime != nil {
		value := a.ChangeTime.UTC()
		a.ChangeTime = &value
	}
	a.Metadata = CloneMetadata(a.Metadata)
	return a
}

const MaxLinkTargetBytes = MaxMetadataValueBytes

func MetadataSize(values map[string]OpaquePayload) (int, error) { return metadataSize(values) }

// MetadataRetentionBytes bounds encoded bytes plus decoded map/value storage.
// The length gives an upper bound on namespace count because every canonical
// record includes eight header bytes, a nonempty key and a nonempty version.
// The fixed allowance also covers the map header and optional instant copies.
func MetadataRetentionBytes(encodedBytes int64) (int64, error) {
	if encodedBytes < 6 {
		return 0, syscall.EINVAL
	}
	if encodedBytes > MaxMetadataBytes {
		return 0, syscall.EFBIG
	}
	count := min(int64(MaxMetadataNamespaces), (encodedBytes-6)/10)
	return 512 + 512*count + 4*encodedBytes, nil
}
