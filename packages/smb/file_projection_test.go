package smb

import (
	"errors"
	"math"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestVirtualFileAllocationUsesCapturedLogicalExtents(t *testing.T) {
	for _, test := range []struct{ eof, allocation int64 }{{0, 0}, {1, 512}, {511, 512}, {512, 512}, {513, 1024}, {4097, 4608}, {math.MaxInt64 &^ 511, math.MaxInt64 &^ 511}} {
		got, err := virtualFileSize(storage.Attr{Kind: storage.NodeRegular, Size: test.eof})
		if err != nil || got.EndOfFile != test.eof || got.AllocationSize != test.allocation || got.AllocationUnit != virtualAllocationUnit {
			t.Fatalf("size %d = %+v, %v", test.eof, got, err)
		}
	}
	if got, err := virtualFileSize(storage.Attr{Kind: storage.NodeDirectory, Size: -1}); err != nil || got != (fileSizeInformation{AllocationUnit: virtualAllocationUnit}) {
		t.Fatalf("directory size = %+v, %v", got, err)
	}
}

func TestVirtualFileAllocationRejectsUnrepresentableSizes(t *testing.T) {
	for _, test := range []struct {
		attr storage.Attr
		want error
	}{
		{storage.Attr{Kind: storage.NodeRegular, Size: -1}, syscall.EIO},
		{storage.Attr{Kind: storage.NodeRegular, Size: math.MaxInt64}, syscall.EOVERFLOW},
		{storage.Attr{Kind: storage.NodeSymlink, Size: 9}, syscall.EOPNOTSUPP},
		{storage.Attr{}, syscall.EOPNOTSUPP},
	} {
		got, err := virtualFileSize(test.attr)
		if got != (fileSizeInformation{}) || !errors.Is(err, test.want) {
			t.Fatalf("size %+v = %+v, %v", test.attr, got, err)
		}
	}
}

func TestVirtualVolumeSerialIsStableAndVolumeSpecific(t *testing.T) {
	if got := virtualVolumeSerial("canonical-volume"); got != 0x3893fa5fcb470ad2 {
		t.Fatalf("serial = %016x", got)
	}
	if virtualVolumeSerial("canonical-volume") == virtualVolumeSerial("other-volume") {
		t.Fatal("different volumes share a serial")
	}
}
