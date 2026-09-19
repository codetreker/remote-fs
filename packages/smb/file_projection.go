package smb

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

const virtualAllocationUnit = 512

// Allocation describes dense virtual extents, not physical storage or reserved
// quota. The same captured EOF must feed every allocation-bearing response.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/8ab3924f-c703-44a7-ac6f-3a75a7a849d8
func virtualFileSize(attr storage.Attr) (fileSizeInformation, error) {
	size := fileSizeInformation{AllocationUnit: virtualAllocationUnit}
	switch attr.Kind {
	case storage.NodeDirectory:
		return size, nil
	case storage.NodeRegular:
		if attr.Size < 0 {
			return fileSizeInformation{}, fmt.Errorf("negative captured file size: %w", syscall.EIO)
		}
		if attr.Size > math.MaxInt64-(virtualAllocationUnit-1) {
			return fileSizeInformation{}, fmt.Errorf("virtual allocation exceeds signed size: %w", syscall.EOVERFLOW)
		}
		size.EndOfFile = attr.Size
		size.AllocationSize = (attr.Size + virtualAllocationUnit - 1) / virtualAllocationUnit * virtualAllocationUnit
		return size, nil
	default:
		return fileSizeInformation{}, syscall.EOPNOTSUPP
	}
}

// The serial identifies the host-configured virtual volume in this SMB
// presentation. It is neither a physical device serial nor a file handle ID.
func virtualVolumeSerial(volume string) uint64 {
	digest := sha256.Sum256([]byte("smb.volume-id.v1\x00" + volume))
	return binary.LittleEndian.Uint64(digest[:8])
}
