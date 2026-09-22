package smb

import (
	"encoding/binary"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const filesystemAttributePrefix = 12

func filesystemInformationSize(class byte) (uint32, error) {
	switch class {
	case 3:
		return 24, nil
	case 4:
		return 8, nil
	case 5:
		return filesystemAttributePrefix, nil
	case 7:
		return 32, nil
	default:
		return 0, syscall.EOPNOTSUPP
	}
}

func encodeFilesystemSizeInformation(space storage.Space, full bool) ([]byte, error) {
	if !space.Coherent() {
		return nil, fmt.Errorf("incoherent volume space: %w", syscall.EIO)
	}
	length := 24
	if full {
		length = 32
	}
	data := make([]byte, length)
	binary.LittleEndian.PutUint64(data, uint64(space.Total/virtualAllocationUnit))
	binary.LittleEndian.PutUint64(data[8:], uint64(space.Avail/virtualAllocationUnit))
	geometry := 16
	if full {
		// Caller availability may be tighter than unused virtual volume budget.
		binary.LittleEndian.PutUint64(data[16:], uint64(max(space.Total-space.Used, 0)/virtualAllocationUnit))
		geometry = 24
	}
	binary.LittleEndian.PutUint32(data[geometry:], 1)
	binary.LittleEndian.PutUint32(data[geometry+4:], virtualAllocationUnit)
	return data, nil
}

func encodeFilesystemDeviceInformation() []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data, 7)
	binary.LittleEndian.PutUint32(data[4:], 0x10)
	return data
}

// The name length describes the complete display name on BUFFER_OVERFLOW.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/431745d0-93af-4663-a0dd-1936feaec4c5
func encodeFilesystemAttributeInformation(capacity uint32) ([]byte, bool, error) {
	if capacity < filesystemAttributePrefix {
		return nil, false, syscall.EINVAL
	}
	name := wire.EncodeUTF16("REMOTEFS")
	copied := min(capacity-filesystemAttributePrefix, uint32(len(name))) &^ 1
	data := make([]byte, filesystemAttributePrefix+int(copied))
	binary.LittleEndian.PutUint32(data, 0x6)
	binary.LittleEndian.PutUint32(data[4:], maxNameUnits)
	binary.LittleEndian.PutUint32(data[8:], uint32(len(name)))
	copy(data[filesystemAttributePrefix:], name[:copied])
	return data, copied < uint32(len(name)), nil
}
