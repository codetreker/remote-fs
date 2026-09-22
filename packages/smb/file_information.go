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
const symlinkReparseTag uint32 = 0xa000000c

// These are supplied allocation facts, not a rounding policy for Attr.Size.
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
// Nanoseconds below that resolution are truncated; overflow is an explicit error.
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

func optionalWindowsTime(value *time.Time) (uint64, error) {
	if value == nil {
		return 0, nil
	}
	return checkedWindowsTime(*value)
}

func captureBasicInformation(attr storage.Attr) (wire.FileInformation, error) {
	attributes, err := projectWindowsAttributes(attr)
	if err != nil {
		return wire.FileInformation{}, err
	}
	creation, err := optionalWindowsTime(attr.BirthTime)
	if err != nil {
		return wire.FileInformation{}, err
	}
	access, err := checkedWindowsTime(attr.AccessTime)
	if err != nil {
		return wire.FileInformation{}, err
	}
	write, err := checkedWindowsTime(attr.ModTime)
	if err != nil {
		return wire.FileInformation{}, err
	}
	change, err := optionalWindowsTime(attr.ChangeTime)
	if err != nil {
		return wire.FileInformation{}, err
	}
	return wire.FileInformation{
		CreationTime: creation,
		AccessTime:   access,
		WriteTime:    write,
		ChangeTime:   change,
		Attributes:   attributes,
	}, nil
}

// The caller supplies Attr and allocation facts from the same atomic open result.
func captureCreateInformation(attr storage.Attr, size fileSizeInformation) (wire.FileInformation, error) {
	if err := size.check(); err != nil {
		return wire.FileInformation{}, err
	}
	if attr.Kind == storage.NodeRegular && (attr.Size < 0 || attr.Size != size.EndOfFile) {
		return wire.FileInformation{}, fmt.Errorf("end of file differs from the captured regular file: %w", syscall.EIO)
	}
	information, err := captureBasicInformation(attr)
	if err != nil {
		return wire.FileInformation{}, err
	}
	information.AllocationSize = uint64(size.AllocationSize)
	information.EndOfFile = uint64(size.EndOfFile)
	return information, nil
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

func fileInformationSize(class byte) (uint32, error) {
	switch class {
	case 4:
		return 40, nil
	case 5, 59:
		return 24, nil
	case 6, 35:
		return 8, nil
	case 7, 8:
		return 4, nil
	case 34:
		return 56, nil
	default:
		return 0, syscall.EOPNOTSUPP
	}
}

// This checks SMB granted access only. Host and authority authorization remain
// separate obligations of the command handler.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d64e0451-64a2-425a-848e-52a7dddab7b9
func checkFileInformationAccess(class byte, granted uint32) error {
	if _, err := fileInformationSize(class); err != nil {
		return err
	}
	switch class {
	case 4, 34, 35:
		if granted&fileReadAttributes == 0 {
			return syscall.EACCES
		}
	}
	return nil
}

func encodeFileTimes(data []byte, information wire.FileInformation) {
	binary.LittleEndian.PutUint64(data, information.CreationTime)
	binary.LittleEndian.PutUint64(data[8:], information.AccessTime)
	binary.LittleEndian.PutUint64(data[16:], information.WriteTime)
	binary.LittleEndian.PutUint64(data[24:], information.ChangeTime)
}

func encodeBasicInformation(attr storage.Attr) ([]byte, error) {
	information, err := captureBasicInformation(attr)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 40)
	encodeFileTimes(data, information)
	binary.LittleEndian.PutUint32(data[32:], information.Attributes)
	return data, nil
}

// Link count and delete-pending state are explicit handle facts; they are not
// inferred from a pathname or from Attr.
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
	information, err := captureCreateInformation(attr, size)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 56)
	encodeFileTimes(data, information)
	binary.LittleEndian.PutUint64(data[32:], information.AllocationSize)
	binary.LittleEndian.PutUint64(data[40:], information.EndOfFile)
	binary.LittleEndian.PutUint32(data[48:], information.Attributes)
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
		binary.LittleEndian.PutUint32(data[4:], symlinkReparseTag)
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
// supplied serial distinguishes its volume; an SMB open FileID is unrelated.
func encodeFileIDInformation(nodeID, volumeSerial uint64) ([]byte, error) {
	if nodeID == 0 {
		return nil, fmt.Errorf("captured file identity is unavailable: %w", syscall.EIO)
	}
	data := make([]byte, 24)
	binary.LittleEndian.PutUint64(data, volumeSerial)
	binary.LittleEndian.PutUint64(data[8:], nodeID)
	return data, nil
}
