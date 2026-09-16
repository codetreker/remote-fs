package fuse_test

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/storage"
)

func permissionMetadata(mode fs.FileMode) storage.Metadata {
	bits := uint32(mode.Perm())
	if mode&fs.ModeSetuid != 0 {
		bits |= 04000
	}
	if mode&fs.ModeSetgid != 0 {
		bits |= 02000
	}
	if mode&fs.ModeSticky != 0 {
		bits |= 01000
	}
	data := make([]byte, 16)
	binary.LittleEndian.PutUint32(data, 1)
	binary.LittleEndian.PutUint32(data[4:], bits)
	return storage.Metadata{{Key: "posix", Version: 1, Data: data}}
}

func setStoredMode(t *testing.T, s storage.Storage, path string, mode fs.FileMode) {
	t.Helper()
	attr, err := s.Stat(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := attr.Metadata.With(permissionMetadata(mode)[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetAttr(t.Context(), path, storage.AttrChange{ExpectedRevision: attr.MetadataRevision, Metadata: &metadata}); err != nil {
		t.Fatal(err)
	}
}

func storedMode(t *testing.T, attr storage.Attr) fs.FileMode {
	t.Helper()
	var kind fs.FileMode
	var bits uint32
	switch attr.Kind {
	case storage.NodeRegular:
		bits = 0644
	case storage.NodeDirectory:
		kind, bits = fs.ModeDir, 0755
	case storage.NodeSymlink:
		kind, bits = fs.ModeSymlink, 0777
	default:
		t.Fatalf("invalid node kind %v", attr.Kind)
	}
	if value, present := attr.Metadata.Get("posix"); present {
		if value.Version != 1 || len(value.Data) != 16 {
			t.Fatalf("invalid POSIX payload: version=%d length=%d", value.Version, len(value.Data))
		}
		bits = binary.LittleEndian.Uint32(value.Data[4:])
	}
	mode := kind | fs.FileMode(bits&0777)
	if bits&04000 != 0 {
		mode |= fs.ModeSetuid
	}
	if bits&02000 != 0 {
		mode |= fs.ModeSetgid
	}
	if bits&01000 != 0 {
		mode |= fs.ModeSticky
	}
	return mode
}

func storedModeFromMetadata(metadata storage.Metadata) fs.FileMode {
	value, present := metadata.Get("posix")
	if !present || value.Version != 1 || len(value.Data) != 16 {
		return fs.ModeIrregular
	}
	return fs.FileMode(binary.LittleEndian.Uint32(value.Data[4:]) & 0777)
}

func TestContentWritesPreservePOSIXMetadata(t *testing.T) {
	mountpoint, _, backing := mountedPair(t)
	path := filepath.Join(mountpoint, "mode")
	if err := os.WriteFile(path, []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []fs.FileMode{0600, 0755 | fs.ModeSetuid, 0750 | fs.ModeSetgid, 0777 | fs.ModeSticky} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		before, err := backing.Stat(t.Context(), "mode")
		if err != nil {
			t.Fatal(err)
		}
		if err := backing.Write(t.Context(), "mode", []byte("after")); err != nil {
			t.Fatal(err)
		}
		after, err := backing.Stat(t.Context(), "mode")
		if err != nil {
			t.Fatal(err)
		}
		if before.ID != after.ID || after.Kind != storage.NodeRegular || storedMode(t, after) != mode {
			t.Fatalf("write changed identity, kind or mode: before=%+v after=%+v", before, after)
		}
	}
}

func TestPermissionHelperRejectsNonSettableModesBeforePublication(t *testing.T) {
	backing := fuseVolume(t)
	if err := backing.Write(t.Context(), "mode", nil); err != nil {
		t.Fatal(err)
	}
	setStoredMode(t, backing, "mode", 0600)
	for _, bit := range []fs.FileMode{fs.ModeDir, fs.ModeSymlink, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeDevice, fs.ModeCharDevice, fs.ModeIrregular, fs.ModeAppend, fs.ModeExclusive, fs.ModeTemporary} {
		attr, err := backing.Stat(t.Context(), "mode")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fuse.WithPermissions(attr.Metadata, 0600|bit); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("mode %v error=%v", bit, err)
		}
		after, err := backing.Stat(t.Context(), "mode")
		if err != nil || after.Kind != storage.NodeRegular || storedMode(t, after) != 0600 || after.MetadataRevision != attr.MetadataRevision {
			t.Fatalf("rejected mode changed node: %+v, %v", after, err)
		}
	}
}
