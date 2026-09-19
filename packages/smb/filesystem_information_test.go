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

func TestFilesystemInformationPreservesVirtualAndCallerSpace(t *testing.T) {
	for _, test := range []struct {
		name                  string
		space                 storage.Space
		total, caller, actual uint64
	}{
		{"independent caller bound", storage.Space{Total: 4097, Used: 513, Avail: 1025}, 8, 2, 7},
		{"over allowance", storage.Space{Total: 1024, Used: 2048}, 2, 0, 0},
		{"sub-unit remainder", storage.Space{Total: 511, Used: 1, Avail: 510}, 0, 0, 0},
		{"signed maximum", storage.Space{Total: math.MaxInt64, Avail: math.MaxInt64}, math.MaxInt64 / 512, math.MaxInt64 / 512, math.MaxInt64 / 512},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, full := range []bool{false, true} {
				data, err := encodeFilesystemSizeInformation(test.space, full)
				length, geometry := 24, 16
				if full {
					length, geometry = 32, 24
				}
				if err != nil || len(data) != length || binary.LittleEndian.Uint64(data) != test.total || binary.LittleEndian.Uint64(data[8:]) != test.caller {
					t.Fatalf("space full=%v = %x, %v", full, data, err)
				}
				if full && binary.LittleEndian.Uint64(data[16:]) != test.actual {
					t.Fatalf("actual virtual availability = %d, want %d", binary.LittleEndian.Uint64(data[16:]), test.actual)
				}
				if binary.LittleEndian.Uint32(data[geometry:]) != 1 || binary.LittleEndian.Uint32(data[geometry+4:]) != 512 {
					t.Fatalf("virtual geometry = %x", data[geometry:])
				}
			}
		})
	}
	for _, invalid := range []storage.Space{{Total: -1}, {Used: -1}, {Avail: -1}, {Total: 5, Used: 4, Avail: 2}, {Total: 5, Used: 9, Avail: 1}} {
		if data, err := encodeFilesystemSizeInformation(invalid, true); data != nil || !errors.Is(err, syscall.EIO) {
			t.Fatalf("incoherent space = %x, %v", data, err)
		}
	}
}

func TestFilesystemInformationExposesOnlyKnownInterfaceFacts(t *testing.T) {
	device := encodeFilesystemDeviceInformation()
	if len(device) != 8 || binary.LittleEndian.Uint32(device) != 7 || binary.LittleEndian.Uint32(device[4:]) != 0x10 {
		t.Fatalf("virtual disk device = %x", device)
	}
	device[0] = 0
	if encodeFilesystemDeviceInformation()[0] != 7 {
		t.Fatal("device result bytes are shared")
	}
	name := wire.EncodeUTF16("REMOTEFS")
	for _, capacity := range []uint32{12, 13, 14, 27, 28, math.MaxUint32} {
		data, overflow, err := encodeFilesystemAttributeInformation(capacity)
		copied := int(min(capacity-12, 16) &^ 1)
		if err != nil || len(data) != 12+copied || overflow != (copied < 16) || binary.LittleEndian.Uint32(data) != 0x6 || binary.LittleEndian.Uint32(data[4:]) != 255 || binary.LittleEndian.Uint32(data[8:]) != 16 || !bytes.Equal(data[12:], name[:copied]) {
			t.Fatalf("attribute information capacity %d = %x, overflow %v, %v", capacity, data, overflow, err)
		}
	}
	if data, overflow, err := encodeFilesystemAttributeInformation(11); data != nil || overflow || !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid encoder capacity = %x, %v, %v", data, overflow, err)
	}
}
