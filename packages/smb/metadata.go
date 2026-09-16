package smb

import (
	"encoding/binary"
	"fmt"
	"syscall"
)

const (
	windowsMetadataKey            = "smb.windows"
	windowsMetadataVersion uint32 = 1
)

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
	Attributes       uint32
	DirectorySymlink bool
}

func validDOSAttributes(attributes uint32) bool {
	return attributes&^dosSettableAttributes == 0 && (attributes&dosNormal == 0 || attributes == dosNormal)
}

func encodeWindowsMetadata(metadata windowsMetadata) ([]byte, error) {
	if !validDOSAttributes(metadata.Attributes) {
		return nil, fmt.Errorf("invalid Windows metadata attributes: %w", syscall.EINVAL)
	}
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint32(payload, metadata.Attributes)
	if metadata.DirectorySymlink {
		binary.LittleEndian.PutUint32(payload[4:], 1)
	}
	return payload, nil
}

// Absence is distinct from a present payload containing zero attributes. The
// caller applies its configured defaults only when present is false and checks
// DirectorySymlink against the observed node kind before presenting metadata.
func decodeWindowsMetadata(payload []byte, version uint32, present bool) (windowsMetadata, error) {
	if !present {
		if version != 0 || len(payload) != 0 {
			return windowsMetadata{}, fmt.Errorf("absent Windows metadata has a version or payload: %w", syscall.EIO)
		}
		return windowsMetadata{}, nil
	}
	if version != windowsMetadataVersion {
		return windowsMetadata{}, fmt.Errorf("unsupported Windows metadata version %d: %w", version, syscall.EOPNOTSUPP)
	}
	if len(payload) != 8 {
		return windowsMetadata{}, fmt.Errorf("Windows metadata must contain exactly 8 bytes: %w", syscall.EIO)
	}
	attributes := binary.LittleEndian.Uint32(payload)
	hints := binary.LittleEndian.Uint32(payload[4:])
	if !validDOSAttributes(attributes) || hints&^uint32(1) != 0 {
		return windowsMetadata{}, fmt.Errorf("invalid Windows metadata attributes or hints: %w", syscall.EIO)
	}
	return windowsMetadata{Attributes: attributes, DirectorySymlink: hints&1 != 0}, nil
}
