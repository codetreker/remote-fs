package smb

import (
	"encoding/binary"
	"io/fs"
	"math"
	"strings"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

var smbLE = binary.LittleEndian

const (
	fileSuccess               uint32 = 0
	fileInvalidParameter      uint32 = 0xc000000d
	fileInvalidInfoClass      uint32 = 0xc0000003
	fileNotSupported          uint32 = 0xc00000bb
	fileBufferTooSmall        uint32 = 0xc0000023
	fileNoMoreFiles           uint32 = 0x80000006
	fileClosed                uint32 = 0xc0000128
	fileIOError               uint32 = 0xc0000185
	fileEOF                   uint32 = 0xc0000011
	fileInsufficientResources uint32 = 0xc000009a
)

func windowsTime(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	const epoch = int64(11644473600)
	seconds := t.Unix()
	if seconds < -epoch || seconds > int64(math.MaxUint64/10000000)-epoch {
		return 0
	}
	return uint64(seconds+epoch)*10000000 + uint64(t.Nanosecond()/100)
}

func decodeWindowsTime(v uint64) *time.Time {
	if v == 0 || v == math.MaxUint64 {
		return nil
	}
	t := time.Unix(int64(v/10000000)-11644473600, int64(v%10000000)*100).UTC()
	return &t
}

func fileAttributes(a storage.WindowsAttr) uint32 {
	flags := a.DOSAttributes
	if a.IsDir() {
		flags = flags&^storage.WindowsDOSNormal | 0x10
	}
	if a.Mode&fs.ModeSymlink != 0 {
		flags = flags&^storage.WindowsDOSNormal | 0x400
	}
	if flags == 0 {
		flags = storage.WindowsDOSNormal
	}
	return flags
}

func allocationSize(a storage.Attr) uint64 {
	// Allocation is reported in bytes because the object backend has no block allocation.
	if a.IsDir() || a.Mode&fs.ModeSymlink != 0 || a.Size <= 0 {
		return 0
	}
	return uint64(a.Size)
}

func encodeBasicInfo(a storage.WindowsAttr) []byte {
	b := make([]byte, 40)
	smbLE.PutUint64(b, windowsTime(a.CreationTime))
	smbLE.PutUint64(b[8:], windowsTime(a.AccessTime))
	smbLE.PutUint64(b[16:], windowsTime(a.ModTime))
	smbLE.PutUint64(b[24:], windowsTime(a.ChangeTime))
	smbLE.PutUint32(b[32:], fileAttributes(a))
	return b
}

func encodeStandardInfo(a storage.WindowsAttr) []byte {
	b := make([]byte, 24)
	smbLE.PutUint64(b, allocationSize(a.Attr))
	if !a.IsDir() && a.Mode&fs.ModeSymlink == 0 {
		smbLE.PutUint64(b[8:], uint64(a.Size))
	}
	smbLE.PutUint32(b[16:], 1)
	if a.DeletePending {
		b[20] = 1
	}
	if a.IsDir() || a.Mode&fs.ModeSymlink != 0 && a.DOSAttributes&storage.WindowsDOSDirectory != 0 {
		b[21] = 1
	}
	return b
}

func encodeFileInfo(class byte, a storage.WindowsAttr, access uint32, position uint64) ([]byte, uint32) {
	switch class {
	case 9, 18:
		if err := a.NameInfo.Check(); err != nil {
			return nil, fileIOError
		}
		if a.NameInfo.State == storage.WindowsNameDetached {
			return nil, 0xc0000123
		}
		name := wire.EncodeUTF16("\\" + strings.ReplaceAll(a.NameInfo.Path, "/", "\\"))
		n := make([]byte, 4+len(name))
		smbLE.PutUint32(n, uint32(len(name)))
		copy(n[4:], name)
		if class == 9 {
			return n, fileSuccess
		}
		b := make([]byte, 96)
		copy(b, encodeBasicInfo(a))
		copy(b[40:], encodeStandardInfo(a))
		smbLE.PutUint64(b[64:], a.ID)
		smbLE.PutUint32(b[76:], access)
		smbLE.PutUint64(b[80:], position)
		return append(b, n...), fileSuccess
	case 4:
		return encodeBasicInfo(a), fileSuccess
	case 5:
		return encodeStandardInfo(a), fileSuccess
	case 6:
		b := make([]byte, 8)
		smbLE.PutUint64(b, a.ID)
		return b, fileSuccess
	case 7:
		return make([]byte, 4), fileSuccess // No extended attributes are exposed.
	case 8:
		b := make([]byte, 4)
		smbLE.PutUint32(b, access)
		return b, fileSuccess
	case 14:
		b := make([]byte, 8)
		smbLE.PutUint64(b, position)
		return b, fileSuccess
	case 16:
		return make([]byte, 4), fileSuccess
	case 17:
		return make([]byte, 4), fileSuccess // Byte alignment.
	case 22: // The unnamed data stream is the only exposed stream.
		if a.IsDir() || a.Mode&fs.ModeSymlink != 0 {
			return nil, fileSuccess
		}
		n := wire.EncodeUTF16("::$DATA")
		b := make([]byte, 24+len(n))
		smbLE.PutUint32(b[4:], uint32(len(n)))
		smbLE.PutUint64(b[8:], uint64(a.Size))
		smbLE.PutUint64(b[16:], allocationSize(a.Attr))
		copy(b[24:], n)
		return b, fileSuccess
	case 34:
		b := make([]byte, 56)
		copy(b, encodeBasicInfo(a)[:32])
		smbLE.PutUint64(b[32:], allocationSize(a.Attr))
		if !a.IsDir() && a.Mode&fs.ModeSymlink == 0 {
			smbLE.PutUint64(b[40:], uint64(a.Size))
		}
		smbLE.PutUint32(b[48:], fileAttributes(a))
		return b, fileSuccess
	case 35:
		b := make([]byte, 8)
		smbLE.PutUint32(b, fileAttributes(a))
		if a.Mode&fs.ModeSymlink != 0 {
			smbLE.PutUint32(b[4:], wire.SymlinkReparseTag)
		}
		return b, fileSuccess
	default:
		return nil, fileNotSupported
	}
}

func directoryEntry(class byte, name string, a storage.WindowsAttr, index uint32) ([]byte, uint32) {
	n := wire.EncodeUTF16(name)
	base := 0
	switch class {
	case 1:
		base = 64
	case 2:
		base = 68
	case 3:
		base = 94
	case 12:
		base = 12
	case 37:
		base = 104
	case 38:
		base = 80
	default:
		return nil, fileInvalidInfoClass
	}
	b := make([]byte, (base+len(n)+7)&^7)
	smbLE.PutUint32(b[4:], index)
	if class == 12 {
		smbLE.PutUint32(b[8:], uint32(len(n)))
	} else {
		smbLE.PutUint64(b[8:], windowsTime(a.CreationTime))
		smbLE.PutUint64(b[16:], windowsTime(a.AccessTime))
		smbLE.PutUint64(b[24:], windowsTime(a.ModTime))
		smbLE.PutUint64(b[32:], windowsTime(a.ChangeTime))
		if !a.IsDir() && a.Mode&fs.ModeSymlink == 0 {
			smbLE.PutUint64(b[40:], uint64(a.Size))
		}
		smbLE.PutUint64(b[48:], allocationSize(a.Attr))
		smbLE.PutUint32(b[56:], fileAttributes(a))
		smbLE.PutUint32(b[60:], uint32(len(n)))
		if class != 1 && a.Mode&fs.ModeSymlink != 0 {
			smbLE.PutUint32(b[64:], wire.SymlinkReparseTag)
		}
		if class == 37 {
			smbLE.PutUint64(b[96:], a.ID)
		}
		if class == 38 {
			smbLE.PutUint64(b[72:], a.ID)
		}
	}
	copy(b[base:], n)
	return b, fileSuccess
}

func queryResponse(data []byte) []byte {
	b := make([]byte, 8+len(data))
	smbLE.PutUint16(b, 9)
	smbLE.PutUint16(b[2:], 72)
	smbLE.PutUint32(b[4:], uint32(len(data)))
	copy(b[8:], data)
	return b
}

func expandAccess(mask uint32) uint32 {
	const genericRead uint32 = 0x80000000
	const genericWrite uint32 = 0x40000000
	const genericExecute uint32 = 0x20000000
	const genericAll uint32 = 0x10000000
	if mask&genericAll != 0 {
		mask = mask&^genericAll | 0x001f01ff
	}
	if mask&genericRead != 0 {
		mask = mask&^genericRead | 0x00120089
	}
	if mask&genericWrite != 0 {
		mask = mask&^genericWrite | 0x00120116
	}
	if mask&genericExecute != 0 {
		mask = mask&^genericExecute | 0x001200a0
	}
	return mask
}

func decodeAccess(mask uint32) (storage.WindowsAccess, uint32) {
	mask = expandAccess(mask)
	// EA and execute bits convey no additional authority for the exposed filesystem.
	if mask & ^uint32(0x001201ff|0x00010000) != 0 {
		return 0, fileNotSupported
	}
	var access storage.WindowsAccess
	for _, v := range accessBits {
		if mask&v.raw != 0 {
			access |= v.semantic
		}
	}
	return access, fileSuccess
}

var accessBits = []struct {
	raw      uint32
	semantic storage.WindowsAccess
}{{1, storage.WindowsReadData}, {2, storage.WindowsWriteData}, {4, storage.WindowsAppendData}, {0x40, storage.WindowsDeleteChild}, {0x80, storage.WindowsReadAttributes}, {0x100, storage.WindowsWriteAttributes}, {0x10000, storage.WindowsDelete}, {0x20000, storage.WindowsReadSecurity}, {0x100000, storage.WindowsSynchronize}}

func encodeAccess(access storage.WindowsAccess) uint32 {
	var mask uint32
	for _, v := range accessBits {
		if access&v.semantic != 0 {
			mask |= v.raw
		}
	}
	return mask
}
