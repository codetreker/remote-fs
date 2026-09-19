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
		attr := storage.Attr{Kind: storage.NodeRegular, Size: test.eof}
		got, err := virtualFileSize(attr)
		if err != nil || got.EndOfFile != test.eof || got.AllocationSize != test.allocation || got.AllocationUnit != 512 || attr.Size != test.eof {
			t.Fatalf("size %d: %+v %v", test.eof, got, err)
		}
	}
	got, err := virtualFileSize(storage.Attr{Kind: storage.NodeDirectory, Size: math.MaxInt64})
	if err != nil || got.EndOfFile != 0 || got.AllocationSize != 0 || got.AllocationUnit != 512 {
		t.Fatalf("directory: %+v %v", got, err)
	}
}

func TestVirtualFileAllocationRejectsUnknownOrUnrepresentableSizes(t *testing.T) {
	for _, test := range []struct {
		attr storage.Attr
		want error
	}{
		{storage.Attr{Kind: storage.NodeRegular, Size: -1}, syscall.EIO},
		{storage.Attr{Kind: storage.NodeRegular, Size: math.MaxInt64}, syscall.EOVERFLOW},
		{storage.Attr{Kind: storage.NodeRegular, Size: (math.MaxInt64 &^ 511) + 1}, syscall.EOVERFLOW},
		{storage.Attr{Kind: storage.NodeSymlink, Size: 9}, syscall.EOPNOTSUPP},
		{storage.Attr{}, syscall.EOPNOTSUPP},
	} {
		got, err := virtualFileSize(test.attr)
		if !errors.Is(err, test.want) || got != (fileSizeInformation{}) {
			t.Fatalf("%+v: %+v %v", test.attr, got, err)
		}
	}
}

func TestVirtualVolumeSerialUsesCanonicalVolumeNotShareAliases(t *testing.T) {
	// This fixed vector binds the assigned serial's domain and byte order.
	if got := virtualVolumeSerial("canonical-volume"); got != 0x3893fa5fcb470ad2 {
		t.Fatalf("serial=%016x", got)
	}
	if virtualVolumeSerial("canonical-volume") == virtualVolumeSerial("other-volume") {
		t.Fatal("volume serial stability or separation")
	}
}
