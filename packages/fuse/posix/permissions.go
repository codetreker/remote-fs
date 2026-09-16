// Package posix encodes the permission metadata interpreted by the Linux client.
// Node kinds and authority-issued metadata versions are carried separately.
package posix

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"syscall"
)

// Namespace identifies the canonical four-byte permission payload format.
const Namespace = "posix.permissions.v1"

// Settable contains permission and special bits, without node-type flags.
const Settable = fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky

// Encode returns raw POSIX permission bits as one little-endian uint32. Node
// kinds and unsupported Go mode flags fail with EINVAL before publication.
func Encode(mode fs.FileMode) ([]byte, error) {
	if mode&^Settable != 0 {
		return nil, fmt.Errorf("POSIX permissions contain node type or unsupported flags: %w", syscall.EINVAL)
	}
	bits := uint32(mode.Perm())
	if mode&fs.ModeSetuid != 0 {
		bits |= 04000
	}
	if mode&fs.ModeSetgid != 0 {
		bits |= 02000
	}
	if mode&fs.ModeSticky != 0 {
		bits |= 01000
	}
	return binary.LittleEndian.AppendUint32(nil, bits), nil
}

// Decode rejects a malformed present payload with EIO. Absence is a separate
// metadata-map fact; an empty payload cannot request default permissions.
func Decode(data []byte) (fs.FileMode, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("POSIX permissions require a four-byte payload: %w", syscall.EIO)
	}
	bits := binary.LittleEndian.Uint32(data)
	if bits&^uint32(07777) != 0 {
		return 0, fmt.Errorf("POSIX permissions contain unsupported bits: %w", syscall.EIO)
	}
	mode := fs.FileMode(bits & 0777)
	if bits&04000 != 0 {
		mode |= fs.ModeSetuid
	}
	if bits&02000 != 0 {
		mode |= fs.ModeSetgid
	}
	if bits&01000 != 0 {
		mode |= fs.ModeSticky
	}
	return mode, nil
}
