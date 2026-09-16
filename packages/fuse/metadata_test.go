package fuse

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/codetreker/remote-fs/packages/storage"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"io/fs"
	"reflect"
	"syscall"
	"testing"
)

func TestPOSIXMetadataEncoding(t *testing.T) {
	for _, value := range []posixAttributes{
		{mode: 0}, {mode: 07777}, {mode: 0600, uid: 42, hasUID: true},
		{mode: 0754, gid: 84, hasGID: true}, {mode: 0754, uid: 42, gid: 84, hasUID: true, hasGID: true},
	} {
		encoded := encodePOSIX(value)
		decoded, err := decodePOSIX(1, encoded)
		if err != nil || decoded != value {
			t.Fatalf("roundtrip %+v = %+v, %v", value, decoded, err)
		}
	}
}

func TestPOSIXMetadataRejectsCorruption(t *testing.T) {
	valid := encodePOSIX(posixAttributes{mode: 0644})
	for _, test := range []struct {
		name    string
		version uint32
		data    []byte
	}{
		{"unknown version", 2, valid}, {"short", 1, valid[:15]}, {"long", 1, append(append([]byte{}, valid...), 0)},
		{"missing mode", 1, make([]byte, 16)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodePOSIX(test.version, test.data); !errors.Is(err, syscall.EIO) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	for _, test := range []struct {
		name   string
		offset int
		word   uint32
	}{
		{"unknown flags", 0, 9}, {"kind in mode", 4, syscall.S_IFREG | 0644}, {"absent uid", 8, 1}, {"absent gid", 12, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := append([]byte{}, valid...)
			binary.LittleEndian.PutUint32(data[test.offset:], test.word)
			if _, err := decodePOSIX(1, data); !errors.Is(err, syscall.EIO) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestPOSIXModeValidation(t *testing.T) {
	for _, bit := range []fs.FileMode{fs.ModeDir, fs.ModeSymlink, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeDevice, fs.ModeCharDevice, fs.ModeIrregular, fs.ModeAppend, fs.ModeExclusive, fs.ModeTemporary} {
		if _, err := modeBits(0600 | bit); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("mode %v error = %v", bit, err)
		}
	}
	for _, mode := range []fs.FileMode{0600, 0754 | fs.ModeSetuid, 0754 | fs.ModeSetgid, 0754 | fs.ModeSticky, 0754 | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky, 0644} {
		bits, err := modeBits(mode)
		if err != nil || goMode(bits) != mode {
			t.Fatalf("mode %v bits = %o, error = %v", mode, bits, err)
		}
	}
}

func TestPermissionDefaultsValidation(t *testing.T) {
	for _, p := range []PermissionDefaults{{}, {File: 0644, Directory: 0755, Symlink: 0777}} {
		if err := p.check(); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []PermissionDefaults{{File: 010000}, {Directory: 010000}, {Symlink: 010000}} {
		if !errors.Is(p.check(), syscall.EINVAL) {
			t.Fatalf("accepted %+v", p)
		}
	}
}

func TestPOSIXEncodingOmitsAbsentOwner(t *testing.T) {
	a := encodePOSIX(posixAttributes{mode: 0600, uid: 99, gid: 88})
	b := encodePOSIX(posixAttributes{mode: 0600})
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("absent owner leaked: %v", a)
	}
}

func TestPOSIXMetadataDefaultsAndOwners(t *testing.T) {
	v := &volume{permissions: PermissionDefaults{File: 0644, Directory: 0755, Symlink: 0777}, owner: gofuse.Owner{Uid: 42, Gid: 84}}
	for _, test := range []struct {
		kind storage.NodeKind
		mode uint32
	}{
		{storage.NodeRegular, syscall.S_IFREG | 0644}, {storage.NodeDirectory, syscall.S_IFDIR | 0755}, {storage.NodeSymlink, syscall.S_IFLNK | 0777},
	} {
		mode, owner, err := v.attributes(storage.Attr{Kind: test.kind})
		if err != nil || mode != test.mode || owner != v.owner {
			t.Fatalf("kind %v = %o/%+v, %v", test.kind, mode, owner, err)
		}
	}
	attr := storage.Attr{Kind: storage.NodeRegular, Metadata: storage.Metadata{{Key: "posix", Version: 1, Data: encodePOSIX(posixAttributes{mode: 0, uid: 9, gid: 10, hasUID: true, hasGID: true})}}}
	mode, owner, err := v.attributes(attr)
	if err != nil || mode != syscall.S_IFREG || owner.Uid != 9 || owner.Gid != 10 {
		t.Fatalf("explicit metadata = %o/%+v, %v", mode, owner, err)
	}
	attr.Metadata[0].Version = 2
	if _, _, err := v.attributes(attr); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown present value = %v", err)
	}
	if _, _, err := v.attributes(storage.Attr{Kind: 255}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown kind = %v", err)
	}
}

func TestPOSIXMetadataUpdatePreservesOtherClients(t *testing.T) {
	v := &volume{}
	original := storage.Attr{Kind: storage.NodeRegular, MetadataRevision: 12, Metadata: storage.Metadata{
		{Key: "other", Version: 99, Data: []byte{1, 2, 3}},
		{Key: "posix", Version: 1, Data: encodePOSIX(posixAttributes{mode: 0600, uid: 42, gid: 84, hasUID: true, hasGID: true})},
	}}
	before := original.Metadata.Clone()
	mode := fs.FileMode(0754 | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
	change, err := v.mergeChange(original, attributeChange{Mode: &mode})
	if err != nil {
		t.Fatal(err)
	}
	if change.ExpectedRevision != 12 || change.Metadata == nil {
		t.Fatalf("missing revision condition: %+v", change)
	}
	other, _ := change.Metadata.Get("other")
	if other.Version != 99 || !reflect.DeepEqual(other.Data, []byte{1, 2, 3}) {
		t.Fatalf("other metadata = %+v", other)
	}
	encoded, _ := change.Metadata.Get("posix")
	value, err := decodePOSIX(encoded.Version, encoded.Data)
	if err != nil || value.mode != 07754 || value.uid != 42 || value.gid != 84 || !value.hasUID || !value.hasGID {
		t.Fatalf("changed metadata = %+v, %v", value, err)
	}
	if !reflect.DeepEqual(original.Metadata, before) {
		t.Fatal("input metadata changed")
	}
	for _, invalid := range []fs.FileMode{fs.ModeDir, fs.ModeSymlink, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeDevice, fs.ModeCharDevice, fs.ModeIrregular, fs.ModeAppend, fs.ModeExclusive, fs.ModeTemporary} {
		mode := 0600 | invalid
		if _, err := v.mergeChange(original, attributeChange{Mode: &mode}); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("mode %v error = %v", mode, err)
		}
		if !reflect.DeepEqual(original.Metadata, before) {
			t.Fatal("invalid change modified prior mode")
		}
	}
	original.MetadataRevision = 0
	if _, err := v.mergeChange(original, attributeChange{Mode: &mode}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("missing revision = %v", err)
	}
}

func testMode(t *testing.T, attr storage.Attr) fs.FileMode {
	t.Helper()
	v := &volume{permissions: PermissionDefaults{File: 0644, Directory: 0755, Symlink: 0777}}
	value, err := v.posix(attr)
	if err != nil {
		t.Fatal(err)
	}
	return goMode(value.mode)
}

func TestPermissionUpdateRejectsUnknownOrMalformedMetadata(t *testing.T) {
	for _, metadata := range []storage.Metadata{
		{{Key: "posix", Version: 2, Data: make([]byte, 16)}},
		{{Key: "posix", Version: 1, Data: []byte{1}}},
		{{Key: "z", Version: 1}, {Key: "a", Version: 1}},
	} {
		if _, err := WithPermissions(metadata, 0600); !errors.Is(err, syscall.EIO) {
			t.Fatalf("error=%v", err)
		}
		if _, err := (&volume{}).posix(storage.Attr{Kind: storage.NodeRegular, Metadata: metadata}); !errors.Is(err, syscall.EIO) {
			t.Fatalf("render error=%v", err)
		}
	}
	metadata, err := WithPermissions(storage.Metadata{{Key: "other", Version: 7, Data: []byte{9}}}, 0600)
	if err != nil {
		t.Fatal(err)
	}
	value, _ := metadata.Get("posix")
	decoded, err := decodePOSIX(value.Version, value.Data)
	if err != nil || decoded.mode != 0600 || decoded.hasUID || decoded.hasGID {
		t.Fatalf("initial mode=%+v, %v", decoded, err)
	}
	full := make(storage.Metadata, storage.MaxMetadataEntries)
	for i := range full {
		full[i] = storage.OpaqueMetadata{Key: fmt.Sprintf("a%02d", i), Version: 1}
	}
	if _, err := WithPermissions(full, 0600); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("full envelope error=%v", err)
	}
}
