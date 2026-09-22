package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFilesystemInformationPreservesMeasuredSpace(t *testing.T) {
	for _, test := range []struct {
		space                 storage.Space
		total, caller, unused uint64
	}{
		{storage.Space{Total: 4097, Used: 513, Avail: 1025}, 8, 2, 7},
		{storage.Space{Total: 1024, Used: 2048}, 2, 0, 0},
		{storage.Space{Total: math.MaxInt64, Avail: math.MaxInt64}, math.MaxInt64 / 512, math.MaxInt64 / 512, math.MaxInt64 / 512},
	} {
		data, err := encodeFilesystemSizeInformation(test.space, true)
		if err != nil || len(data) != 32 || binary.LittleEndian.Uint64(data) != test.total ||
			binary.LittleEndian.Uint64(data[8:]) != test.caller || binary.LittleEndian.Uint64(data[16:]) != test.unused ||
			binary.LittleEndian.Uint32(data[24:]) != 1 || binary.LittleEndian.Uint32(data[28:]) != virtualAllocationUnit {
			t.Fatalf("space %+v = %x, %v", test.space, data, err)
		}
	}
	for _, invalid := range []storage.Space{{Total: -1}, {Used: -1}, {Avail: -1}, {Total: 5, Used: 4, Avail: 2}} {
		if data, err := encodeFilesystemSizeInformation(invalid, true); data != nil || !errors.Is(err, syscall.EIO) {
			t.Fatalf("incoherent space = %x, %v", data, err)
		}
	}
}

func TestFilesystemInformationClassesAndBoundedName(t *testing.T) {
	for class, size := range map[byte]uint32{3: 24, 4: 8, 5: 12, 7: 32} {
		if got, err := filesystemInformationSize(class); err != nil || got != size {
			t.Fatalf("class %d size = %d, %v", class, got, err)
		}
	}
	for _, class := range []byte{0, 1, 2, 6, 8, 255} {
		if _, err := filesystemInformationSize(class); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("unsupported class %d = %v", class, err)
		}
	}
	device := encodeFilesystemDeviceInformation()
	if len(device) != 8 || binary.LittleEndian.Uint32(device) != 7 || binary.LittleEndian.Uint32(device[4:]) != 0x10 {
		t.Fatalf("device information = %x", device)
	}
	name := wire.EncodeUTF16("REMOTEFS")
	for _, capacity := range []uint32{12, 13, 14, 27, 28, math.MaxUint32} {
		data, overflow, err := encodeFilesystemAttributeInformation(capacity)
		copied := int(min(capacity-filesystemAttributePrefix, uint32(len(name))) &^ 1)
		if err != nil || len(data) != filesystemAttributePrefix+copied || overflow != (copied < len(name)) ||
			binary.LittleEndian.Uint32(data[8:]) != uint32(len(name)) || !bytes.Equal(data[filesystemAttributePrefix:], name[:copied]) {
			t.Fatalf("capacity %d = %x, overflow %v, %v", capacity, data, overflow, err)
		}
	}
	if data, overflow, err := encodeFilesystemAttributeInformation(filesystemAttributePrefix - 1); data != nil || overflow || !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("short capacity = %x, %v, %v", data, overflow, err)
	}
}
