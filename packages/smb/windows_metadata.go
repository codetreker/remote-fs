package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"

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
	Attributes       uint32
	DirectorySymlink bool
}

func validDOSAttributes(attributes uint32) bool {
	return attributes&^dosSettableAttributes == 0 && (attributes&dosNormal == 0 || attributes == dosNormal)
}

// Format version belongs to Data. OpaquePayload.Version remains the authority's
// uninterpreted compare-and-swap token.
func encodeWindowsMetadata(value windowsMetadata) ([]byte, error) {
	if !validDOSAttributes(value.Attributes) {
		return nil, fmt.Errorf("invalid Windows metadata attributes: %w", syscall.EINVAL)
	}
	data := make([]byte, 12)
	copy(data, "SMW\x01")
	binary.LittleEndian.PutUint32(data[4:], value.Attributes)
	if value.DirectorySymlink {
		binary.LittleEndian.PutUint32(data[8:], 1)
	}
	return data, nil
}

// An absent namespace contributes no Windows-only attributes or directory-link
// hint. An empty present namespace is malformed.
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
	if len(data) != 12 {
		return windowsMetadata{}, fmt.Errorf("Windows metadata requires 12 bytes: %w", syscall.EIO)
	}
	attributes := binary.LittleEndian.Uint32(data[4:])
	hints := binary.LittleEndian.Uint32(data[8:])
	if !validDOSAttributes(attributes) || hints&^uint32(1) != 0 {
		return windowsMetadata{}, fmt.Errorf("invalid Windows metadata attributes or hints: %w", syscall.EIO)
	}
	return windowsMetadata{Attributes: attributes, DirectorySymlink: hints&1 != 0}, nil
}

// Initial metadata is copied as a whole so adding this namespace cannot discard
// or alias another client's values.
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

// NORMAL represents the absence of every other reported attribute; structural
// DIRECTORY and REPARSE_POINT bits come only from the captured node kind.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/ca28ec38-f155-4768-81d6-4bfeb8586fc9
func projectWindowsAttributes(attr storage.Attr) (uint32, error) {
	if attr.ID == 0 || attr.Kind.Check() != nil {
		return 0, fmt.Errorf("invalid captured node identity or kind: %w", syscall.EIO)
	}
	metadata, err := decodeWindowsMetadata(attr.Metadata)
	if err != nil {
		return 0, err
	}
	if metadata.DirectorySymlink && attr.Kind != storage.NodeSymlink {
		return 0, fmt.Errorf("directory-link hint on a non-link node: %w", syscall.EIO)
	}
	attributes := metadata.Attributes
	if attr.Kind == storage.NodeDirectory || metadata.DirectorySymlink {
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
