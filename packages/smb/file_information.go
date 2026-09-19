package smb

import (
	"encoding/binary"
	"fmt"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const fileReadAttributes uint32 = 0x80

// These are supplied allocation facts, not a rounding policy for Attr.Size.
// Directory sizes and link data-stream sizes also require an explicit source.
type fileSizeInformation struct {
	AllocationSize int64
	EndOfFile      int64
	AllocationUnit uint32
}

func (size fileSizeInformation) check() error {
	if size.AllocationUnit == 0 {
		return fmt.Errorf("allocation unit is unavailable: %w", syscall.EOPNOTSUPP)
	}
	if size.AllocationSize < 0 || size.EndOfFile < 0 || size.AllocationSize%int64(size.AllocationUnit) != 0 {
		return fmt.Errorf("invalid allocation size or end of file: %w", syscall.EIO)
	}
	return nil
}

// Query timestamps are nonnegative signed 64-bit counts of 100ns intervals.
// Nanoseconds below that resolution are truncated; no overflow becomes zero.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/a69cc039-d288-4673-9598-772b6083f8bf
func checkedWindowsTime(value time.Time) (uint64, error) {
	const offset int64 = 11644473600
	const ticks int64 = 10000000
	const lastSeconds = math.MaxInt64/ticks - offset
	const lastNanos = (math.MaxInt64%ticks)*100 + 99
	if value.Before(time.Unix(-offset, 0)) || value.After(time.Unix(lastSeconds, lastNanos)) {
		return 0, fmt.Errorf("timestamp is outside the Windows query range: %w", syscall.EOVERFLOW)
	}
	return uint64(value.Unix()+offset)*uint64(ticks) + uint64(value.Nanosecond()/100), nil
}

func captureBasicInformation(attr storage.Attr) (wire.FileInformation, error) {
	attributes, err := projectWindowsAttributes(attr)
	if err != nil {
		return wire.FileInformation{}, err
	}
	if attr.BirthTime == nil || attr.ChangeTime == nil {
		return wire.FileInformation{}, fmt.Errorf("captured creation or change time is unavailable: %w", syscall.EOPNOTSUPP)
	}
	var encoded [4]uint64
	for index, instant := range []time.Time{*attr.BirthTime, attr.AccessTime, attr.ModTime, *attr.ChangeTime} {
		encoded[index], err = checkedWindowsTime(instant)
		if err != nil {
			return wire.FileInformation{}, err
		}
	}
	return wire.FileInformation{CreationTime: encoded[0], AccessTime: encoded[1], WriteTime: encoded[2], ChangeTime: encoded[3], Attributes: attributes}, nil
}

// The caller passes the Attr from the atomic open result. No later observation
// is substituted, and allocation facts must describe that same captured state.
func captureCreateInformation(attr storage.Attr, size fileSizeInformation) (wire.FileInformation, error) {
	if err := size.check(); err != nil {
		return wire.FileInformation{}, err
	}
	if attr.Kind == storage.NodeRegular && (attr.Size < 0 || attr.Size != size.EndOfFile) {
		return wire.FileInformation{}, fmt.Errorf("end of file differs from the captured regular file: %w", syscall.EIO)
	}
	info, err := captureBasicInformation(attr)
	if err != nil {
		return wire.FileInformation{}, err
	}
	info.AllocationSize, info.EndOfFile = uint64(size.AllocationSize), uint64(size.EndOfFile)
	return info, nil
}

func createAction(outcome storage.OpenOutcome) (uint32, error) {
	switch outcome {
	case storage.Opened:
		return 1, nil
	case storage.Created:
		return 2, nil
	case storage.Reset:
		return 3, nil
	case storage.Replaced:
		return 0, nil
	default:
		return 0, fmt.Errorf("unknown captured open outcome: %w", syscall.EIO)
	}
}

// This checks the SMB granted-access requirement only. Current host and native
// authorization remain separate obligations of the command handler.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d64e0451-64a2-425a-848e-52a7dddab7b9
func checkFileInformationAccess(class byte, granted uint32) error {
	switch class {
	case 4, 34, 35:
		if granted&fileReadAttributes == 0 {
			return syscall.EACCES
		}
		return nil
	case 5, 6, 8, 59:
		return nil
	default:
		return syscall.EOPNOTSUPP
	}
}

func encodeFileTimes(data []byte, info wire.FileInformation) {
	binary.LittleEndian.PutUint64(data, info.CreationTime)
	binary.LittleEndian.PutUint64(data[8:], info.AccessTime)
	binary.LittleEndian.PutUint64(data[16:], info.WriteTime)
	binary.LittleEndian.PutUint64(data[24:], info.ChangeTime)
}

func encodeBasicInformation(attr storage.Attr) ([]byte, error) {
	info, err := captureBasicInformation(attr)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 40)
	encodeFileTimes(data, info)
	binary.LittleEndian.PutUint32(data[32:], info.Attributes)
	return data, nil
}

// Links counts non-deleted links. Neither that count nor delete-pending state is
// inferred from an open handle, a pathname, or an Attr without those facts.
func encodeStandardInformation(size fileSizeInformation, links uint32, pending, directory bool) ([]byte, error) {
	if err := size.check(); err != nil {
		return nil, err
	}
	data := make([]byte, 24)
	binary.LittleEndian.PutUint64(data, uint64(size.AllocationSize))
	binary.LittleEndian.PutUint64(data[8:], uint64(size.EndOfFile))
	binary.LittleEndian.PutUint32(data[16:], links)
	if pending {
		data[20] = 1
	}
	if directory {
		data[21] = 1
	}
	return data, nil
}

func encodeNetworkOpenInformation(attr storage.Attr, size fileSizeInformation) ([]byte, error) {
	info, err := captureCreateInformation(attr, size)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 56)
	encodeFileTimes(data, info)
	binary.LittleEndian.PutUint64(data[32:], info.AllocationSize)
	binary.LittleEndian.PutUint64(data[40:], info.EndOfFile)
	binary.LittleEndian.PutUint32(data[48:], info.Attributes)
	return data, nil
}

func encodeAttributeTagInformation(attr storage.Attr) ([]byte, error) {
	attributes, err := projectWindowsAttributes(attr)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data, attributes)
	if attr.Kind == storage.NodeSymlink {
		binary.LittleEndian.PutUint32(data[4:], wire.SymlinkReparseTag)
	}
	return data, nil
}

func encodeInternalInformation(nodeID uint64) ([]byte, error) {
	if nodeID == 0 {
		return nil, fmt.Errorf("captured file identity is unavailable: %w", syscall.EIO)
	}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, nodeID)
	return data, nil
}

func encodeAccessInformation(granted uint32) []byte {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, granted)
	return data
}

// The 128-bit identifier embeds the authority's immutable 64-bit NodeID. The
// supplied serial distinguishes its volume; an SMB open's FileID is unrelated.
func encodeFileIDInformation(nodeID, volumeSerial uint64) ([]byte, error) {
	if nodeID == 0 {
		return nil, fmt.Errorf("captured file identity is unavailable: %w", syscall.EIO)
	}
	data := make([]byte, 24)
	binary.LittleEndian.PutUint64(data, volumeSerial)
	binary.LittleEndian.PutUint64(data[8:], nodeID)
	return data, nil
}
