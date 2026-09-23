package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

const windowsMetadataKey = "smb.windows"

const (
	dosReadOnly     uint32 = 0x1
	dosHidden       uint32 = 0x2
	dosSystem       uint32 = 0x4
	dosDirectory    uint32 = 0x10
	dosArchive      uint32 = 0x20
	dosNormal       uint32 = 0x80
	dosReparsePoint uint32 = 0x400

	dosSettableAttributes = dosReadOnly | dosHidden | dosSystem | dosArchive | dosNormal
)

type windowsMetadata struct {
	Attributes uint32
}

func validDOSAttributes(attributes uint32) bool {
	return attributes&^dosSettableAttributes == 0 && (attributes&dosNormal == 0 || attributes == dosNormal)
}

// Format version belongs to Data. OpaquePayload.Version remains an uninterpreted
// authority CAS token. Structural attributes are derived from the captured kind.
func encodeWindowsMetadata(value windowsMetadata) ([]byte, error) {
	if !validDOSAttributes(value.Attributes) {
		return nil, fmt.Errorf("invalid Windows metadata attributes: %w", syscall.EINVAL)
	}
	data := make([]byte, 8)
	copy(data, "SMW\x01")
	binary.LittleEndian.PutUint32(data[4:], value.Attributes)
	return data, nil
}

// An absent namespace contributes no Windows-only attributes. An empty
// present namespace is malformed, not an absent value.
func decodeWindowsMetadata(values map[string]storage.OpaquePayload) (windowsMetadata, error) {
	if err := storage.CheckMetadata(values); err != nil {
		return windowsMetadata{}, errors.Join(syscall.EIO, err)
	}
	value, present := values[windowsMetadataKey]
	if !present {
		return windowsMetadata{}, nil
	}
	data := value.Data
	if len(data) < 4 || !bytes.Equal(data[:3], []byte("SMW")) {
		return windowsMetadata{}, fmt.Errorf("invalid Windows metadata header: %w", syscall.EIO)
	}
	if data[3] != 1 {
		return windowsMetadata{}, fmt.Errorf("unsupported Windows metadata format %d: %w", data[3], syscall.EOPNOTSUPP)
	}
	if len(data) != 8 {
		return windowsMetadata{}, fmt.Errorf("Windows metadata requires 8 bytes: %w", syscall.EIO)
	}
	attributes := binary.LittleEndian.Uint32(data[4:])
	if !validDOSAttributes(attributes) {
		return windowsMetadata{}, fmt.Errorf("invalid Windows metadata attributes: %w", syscall.EIO)
	}
	return windowsMetadata{Attributes: attributes}, nil
}

// Initial metadata is copied as a whole so adding this namespace cannot discard
// or alias another client's values. Existing authority version tokens are never
// converted into initial values by this helper.
func withWindowsMetadata(initial map[string][]byte, value windowsMetadata) (map[string][]byte, error) {
	data, err := encodeWindowsMetadata(value)
	if err != nil {
		return nil, err
	}
	if err := storage.CheckInitialMetadata(initial); err != nil {
		return nil, err
	}
	values := make(map[string][]byte, len(initial)+1)
	for name, data := range initial {
		values[name] = data
	}
	values[windowsMetadataKey] = data
	if err := storage.CheckInitialMetadata(values); err != nil {
		return nil, err
	}
	return storage.CloneInitialMetadata(values), nil
}

// NORMAL is the representation of no other attributes, never a bit combined
// with DIRECTORY or REPARSE_POINT.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/ca28ec38-f155-4768-81d6-4bfeb8586fc9
func projectWindowsAttributes(attr storage.Attr) (uint32, error) {
	if attr.ID == 0 || attr.Kind.Check() != nil {
		return 0, fmt.Errorf("invalid captured node identity or kind: %w", syscall.EIO)
	}
	metadata, err := decodeWindowsMetadata(attr.Metadata)
	if err != nil {
		return 0, err
	}
	attributes := metadata.Attributes
	if attr.Kind == storage.NodeDirectory {
		attributes = attributes&^dosNormal | dosDirectory
	}
	if attr.Kind == storage.NodeSymlink {
		attributes = attributes&^dosNormal | dosReparsePoint
	}
	if attributes == 0 {
		attributes = dosNormal
	}
	return attributes, nil
}

type createMetadata struct {
	Attributes     uint32
	CreationTime   uint64
	LastAccessTime uint64
	LastWriteTime  uint64
	ChangeTime     uint64
	EndOfFile      uint64
}

const filetimeUnixOffset = int64(11644473600)

func toFiletime(value time.Time) (uint64, error) {
	value = value.UTC()
	seconds := value.Unix()
	if seconds < -filetimeUnixOffset {
		return 0, fmt.Errorf("timestamp precedes Windows FILETIME epoch: %w", syscall.EIO)
	}
	if seconds > int64(^uint64(0)/10_000_000)-filetimeUnixOffset {
		return 0, fmt.Errorf("timestamp exceeds Windows FILETIME range: %w", syscall.EIO)
	}
	ticks := uint64(seconds + filetimeUnixOffset)
	partial := uint64(value.Nanosecond() / 100)
	if ticks == ^uint64(0)/10_000_000 && partial > ^uint64(0)%10_000_000 {
		return 0, fmt.Errorf("timestamp exceeds Windows FILETIME range: %w", syscall.EIO)
	}
	return ticks*10_000_000 + partial, nil
}

func optionalFiletime(value *time.Time) (uint64, error) {
	if value == nil {
		return 0, nil
	}
	return toFiletime(*value)
}

func projectCreateMetadata(attr storage.Attr) (createMetadata, error) {
	attributes, err := projectWindowsAttributes(attr)
	if err != nil {
		return createMetadata{}, err
	}
	if attr.Size < 0 {
		return createMetadata{}, fmt.Errorf("negative captured file length: %w", syscall.EIO)
	}
	creation, err := optionalFiletime(attr.BirthTime)
	if err != nil {
		return createMetadata{}, err
	}
	access, err := toFiletime(attr.AccessTime)
	if err != nil {
		return createMetadata{}, err
	}
	write, err := toFiletime(attr.ModTime)
	if err != nil {
		return createMetadata{}, err
	}
	change, err := optionalFiletime(attr.ChangeTime)
	if err != nil {
		return createMetadata{}, err
	}
	result := createMetadata{Attributes: attributes, CreationTime: creation, LastAccessTime: access, LastWriteTime: write, ChangeTime: change}
	if attr.Kind == storage.NodeRegular {
		result.EndOfFile = uint64(attr.Size)
	}
	return result, nil
}

// The storage producer runs this admission before an open effect and before
// loading returned opaque metadata. It bounds the retained result and rejects
// scalar facts that could not be encoded in CREATE's fixed response.
func createAttrBudget(limit int64) storage.AttrResultBudget {
	return func(attr storage.Attr, metadataBytes int64) error {
		if attr.ID == 0 || attr.Kind.Check() != nil || attr.Size < 0 {
			return syscall.EIO
		}
		for _, value := range []*time.Time{attr.BirthTime, &attr.AccessTime, &attr.ModTime, attr.ChangeTime} {
			if value != nil {
				if _, err := toFiletime(*value); err != nil {
					return err
				}
			}
		}
		retained, err := storage.MetadataRetentionBytes(metadataBytes)
		if err != nil {
			return err
		}
		if retained+512 > limit {
			return syscall.EFBIG
		}
		return nil
	}
}
