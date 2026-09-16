package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
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

type NodeMetadataRevision uint64
type ContentRevision uint64

const (
	MaxMetadataBytes    = 32 << 10
	MaxMetadataEntries  = 16
	MaxMetadataKeyBytes = 64
	MaxLinkTargetBytes  = 4096
)

// OpaqueMetadata is interpreted only by the client that owns Key. A present
// value with an unknown Version is distinct from an absent value.
type OpaqueMetadata struct {
	Key     string
	Version uint32
	Data    []byte
}

// Metadata is ordered by Key. Updates preserve values owned by other clients.
type Metadata []OpaqueMetadata

func (m Metadata) Check() error {
	if len(m) > MaxMetadataEntries {
		return syscall.EFBIG
	}
	size := 6
	previous := ""
	for _, item := range m {
		if len(item.Key) == 0 || len(item.Key) > MaxMetadataKeyBytes || item.Key <= previous || item.Version == 0 {
			return syscall.EINVAL
		}
		for _, b := range []byte(item.Key) {
			if !((b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '-' || b == '_' || b == '.') {
				return syscall.EINVAL
			}
		}
		if len(item.Data) > MaxMetadataBytes-size-10-len(item.Key) {
			return syscall.EFBIG
		}
		size += 10 + len(item.Key) + len(item.Data)
		previous = item.Key
	}
	return nil
}

func (m Metadata) Clone() Metadata {
	if m == nil {
		return nil
	}
	out := make(Metadata, len(m))
	for i, item := range m {
		out[i] = OpaqueMetadata{Key: strings.Clone(item.Key), Version: item.Version, Data: bytes.Clone(item.Data)}
	}
	return out
}

func (m Metadata) Get(key string) (OpaqueMetadata, bool) {
	index, found := slices.BinarySearchFunc(m, key, func(item OpaqueMetadata, key string) int { return strings.Compare(item.Key, key) })
	if !found {
		return OpaqueMetadata{}, false
	}
	item := m[index]
	item.Key = strings.Clone(item.Key)
	item.Data = bytes.Clone(item.Data)
	return item, true
}

// With replaces one client's value and returns an independent canonical envelope.
func (m Metadata) With(value OpaqueMetadata) (Metadata, error) {
	if err := m.Check(); err != nil {
		return nil, err
	}
	out := m.Clone()
	index, found := slices.BinarySearchFunc(out, value.Key, func(item OpaqueMetadata, key string) int { return strings.Compare(item.Key, key) })
	value.Key = strings.Clone(value.Key)
	value.Data = bytes.Clone(value.Data)
	if found {
		out[index] = value
	} else {
		out = slices.Insert(out, index, value)
	}
	if err := out.Check(); err != nil {
		return nil, err
	}
	return out, nil
}

func (m Metadata) EncodedSize() (int, error) {
	if err := m.Check(); err != nil {
		return 0, err
	}
	size := 6
	for _, item := range m {
		size += 10 + len(item.Key) + len(item.Data)
	}
	return size, nil
}

// EncodeMetadata produces one canonical, versioned persistence representation.
func EncodeMetadata(m Metadata) ([]byte, error) {
	if err := m.Check(); err != nil {
		return nil, err
	}
	size := 6
	for _, item := range m {
		size += 10 + len(item.Key) + len(item.Data)
	}
	out := make([]byte, 0, size)
	out = append(out, 'R', 'F', 'M', 1)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(m)))
	for _, item := range m {
		out = binary.LittleEndian.AppendUint16(out, uint16(len(item.Key)))
		out = binary.LittleEndian.AppendUint32(out, item.Version)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(item.Data)))
		out = append(out, item.Key...)
		out = append(out, item.Data...)
	}
	return out, nil
}

func DecodeMetadata(data []byte) (Metadata, error) {
	if len(data) < 6 || len(data) > MaxMetadataBytes || !bytes.Equal(data[:4], []byte{'R', 'F', 'M', 1}) {
		return nil, fmt.Errorf("invalid metadata envelope: %w", syscall.EIO)
	}
	count := int(binary.LittleEndian.Uint16(data[4:6]))
	if count > MaxMetadataEntries {
		return nil, syscall.EIO
	}
	data = data[6:]
	var out Metadata
	for range count {
		if len(data) < 10 {
			return nil, syscall.EIO
		}
		keySize := int(binary.LittleEndian.Uint16(data[:2]))
		version := binary.LittleEndian.Uint32(data[2:6])
		size := uint64(binary.LittleEndian.Uint32(data[6:10]))
		data = data[10:]
		if keySize > len(data) || size > uint64(len(data)-keySize) {
			return nil, syscall.EIO
		}
		out = append(out, OpaqueMetadata{Key: string(data[:keySize]), Version: version, Data: bytes.Clone(data[keySize : keySize+int(size)])})
		data = data[keySize+int(size):]
	}
	if len(data) != 0 {
		return nil, syscall.EIO
	}
	if err := out.Check(); err != nil {
		return nil, fmt.Errorf("invalid metadata envelope: %w", syscall.EIO)
	}
	return out, nil
}

// Clone owns metadata bytes and timestamp values without retaining foreign time
// locations. Nil creation/change timestamps remain unknown.
func (a Attr) Clone() Attr {
	a.Metadata = a.Metadata.Clone()
	a.AccessTime = a.AccessTime.UTC()
	a.ModTime = a.ModTime.UTC()
	if a.CreationTime != nil {
		value := a.CreationTime.UTC()
		a.CreationTime = &value
	}
	if a.ChangeTime != nil {
		value := a.ChangeTime.UTC()
		a.ChangeTime = &value
	}
	return a
}

func (a Attr) Check() error {
	if a.ID == 0 || a.Size < 0 || a.MetadataRevision == 0 || (a.Kind == NodeDirectory) != (a.DirectoryRevision != 0) {
		return syscall.EINVAL
	}
	if err := a.Kind.Check(); err != nil {
		return err
	}
	return a.Metadata.Check()
}
