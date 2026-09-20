package posix_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/fuse/posix"
)

func TestPermissionEncodingPreservesEveryPermissionAndSpecialBit(t *testing.T) {
	for raw := uint32(0); raw <= 07777; raw++ {
		data := binary.LittleEndian.AppendUint32(nil, raw)
		mode, err := posix.Decode(data)
		if err != nil {
			t.Fatalf("decode %o: %v", raw, err)
		}
		if uint32(mode.Perm()) != raw&0777 || (mode&fs.ModeSetuid != 0) != (raw&04000 != 0) || (mode&fs.ModeSetgid != 0) != (raw&02000 != 0) || (mode&fs.ModeSticky != 0) != (raw&01000 != 0) {
			t.Fatalf("decode %o gave %v", raw, mode)
		}
		encoded, err := posix.Encode(mode)
		if err != nil || !bytes.Equal(encoded, data) {
			t.Fatalf("round trip %o: %x, %v", raw, encoded, err)
		}
	}
}

func TestPermissionEncodingRejectsNodeKindsAndUnrecognizedFlags(t *testing.T) {
	for _, unsupported := range []fs.FileMode{fs.ModeDir, fs.ModeSymlink, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeDevice, fs.ModeCharDevice, fs.ModeIrregular, fs.ModeAppend, fs.ModeExclusive, fs.ModeTemporary, 1 << 9} {
		if data, err := posix.Encode(0600 | unsupported); data != nil || !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("encode unsupported mode %v: %x, %v", unsupported, data, err)
		}
	}
}

func TestPermissionDecodingRejectsMalformedPresentPayloads(t *testing.T) {
	for _, data := range [][]byte{nil, {}, {0}, {0, 0, 0}, {0, 0, 0, 0, 0}, {0, 0x10, 0, 0}, {0, 0, 1, 0}, {0, 0, 0, 1}} {
		if _, err := posix.Decode(data); !errors.Is(err, syscall.EIO) {
			t.Fatalf("decode malformed payload %x: %v", data, err)
		}
	}
	if posix.Namespace != "posix.permissions.v1" {
		t.Fatalf("changed persistent namespace: %q", posix.Namespace)
	}
}
