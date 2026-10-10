package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func metadataTestValue(t *testing.T, value windowsMetadata) map[string]storage.OpaquePayload {
	t.Helper()
	data, err := encodeWindowsMetadata(value)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{0xff, 0, 9}, Data: data}}
}

func TestCreateMetadataUsesCapturedFacts(t *testing.T) {
	modified := time.Date(2026, 9, 23, 1, 2, 3, 456700000, time.UTC)
	created := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	attr := storage.Attr{ID: 4, Kind: storage.NodeRegular, Size: 123, AllocationKnown: true, AllocationSize: 4096, AccessTime: modified, ModTime: modified, BirthTime: &created, Metadata: metadataTestValue(t, windowsMetadata{Attributes: dosHidden | dosArchive})}
	got, err := projectCreateMetadata(attr)
	if err != nil || got.Attributes != dosHidden|dosArchive || got.EndOfFile != 123 || got.AllocationSize != 4096 || got.ChangeTime != 0 {
		t.Fatalf("CREATE facts: %+v %v", got, err)
	}
	if got.CreationTime == 0 || got.LastAccessTime != got.LastWriteTime || got.LastWriteTime%10_000_000 != 4_567_000 {
		t.Fatalf("FILETIME conversion: %+v", got)
	}
	attr.Kind = storage.NodeDirectory
	attr.Size = 99 // Directory size is unspecified by storage.
	attr.AllocationSize = 0
	got, err = projectCreateMetadata(attr)
	if err != nil || got.Attributes != dosHidden|dosArchive|dosDirectory || got.EndOfFile != 0 || got.AllocationSize != 0 {
		t.Fatalf("directory projection used unspecified size: %+v %v", got, err)
	}
	attr.Kind = storage.NodeRegular
	attr.AccessTime = time.Time{}
	if _, err := projectCreateMetadata(attr); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unrepresentable access time accepted: %v", err)
	}
	attr.AccessTime = modified
	attr.Size = -1
	if _, err := projectCreateMetadata(attr); !errors.Is(err, syscall.EIO) {
		t.Fatalf("negative size accepted: %v", err)
	}
}

func TestCreateAttrBudgetRefusesUnencodablePreEffectResults(t *testing.T) {
	stamp := time.Date(2026, 9, 23, 1, 2, 3, 0, time.UTC)
	attr := storage.Attr{ID: 4, Kind: storage.NodeRegular, Size: 7, AllocationKnown: true, AllocationSize: 4096, AccessTime: stamp, ModTime: stamp}
	budget := createAttrBudget(2048)
	if err := budget(attr, 6); err != nil {
		t.Fatalf("valid scalar rejected: %v", err)
	}
	invalid := attr
	invalid.ModTime = time.Time{}
	if err := budget(invalid, 6); !errors.Is(err, syscall.EIO) {
		t.Fatalf("invalid time accepted: %v", err)
	}
	if err := budget(attr, storage.MaxMetadataBytes); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized retained metadata accepted: %v", err)
	}
}

func TestCreateAllocationProjectionUsesAuthoritativeCapture(t *testing.T) {
	stamp := time.Date(2026, 9, 23, 1, 2, 3, 0, time.UTC)
	base := storage.Attr{ID: 4, Kind: storage.NodeRegular, AllocationKnown: true, AccessTime: stamp, ModTime: stamp}
	budget := createAttrBudget(2048)
	for _, test := range []struct {
		size, allocated int64
	}{
		{0, 0}, {1, 512}, {1, 4096}, {4096, 4096}, {4097, 8192},
	} {
		attr := base
		attr.Size, attr.AllocationSize = test.size, test.allocated
		if err := budget(attr, 6); err != nil {
			t.Fatalf("pre-effect budget for size %d: %v", test.size, err)
		}
		got, err := projectCreateMetadata(attr)
		if err != nil || got.EndOfFile != uint64(test.size) || got.AllocationSize != uint64(test.allocated) {
			t.Fatalf("captured size %d allocation %d projected as %+v: %v", test.size, test.allocated, got, err)
		}
	}
	for _, test := range []struct {
		name      string
		known     bool
		allocated int64
	}{
		{"unknown", false, 0},
		{"unknown nonzero", false, 4096},
		{"negative", true, -4096},
	} {
		attr := base
		attr.Size = 1
		attr.AllocationKnown, attr.AllocationSize = test.known, test.allocated
		if err := budget(attr, 6); !errors.Is(err, syscall.EIO) {
			t.Fatalf("%s reached effect: %v", test.name, err)
		}
		if projected, err := projectCreateMetadata(attr); !errors.Is(err, syscall.EIO) || projected != (createMetadata{}) {
			t.Fatalf("%s projected: %+v %v", test.name, projected, err)
		}
	}
}

func TestWindowsMetadataEncodingOwnsItsFormatAndBytes(t *testing.T) {
	value := windowsMetadata{Attributes: dosHidden | dosArchive}
	encoded, err := encodeWindowsMetadata(value)
	want := []byte{'S', 'M', 'W', 1, 0x22, 0, 0, 0}
	if err != nil || !bytes.Equal(encoded, want) {
		t.Fatalf("Windows payload = %x, %v", encoded, err)
	}
	metadata := map[string]storage.OpaquePayload{
		windowsMetadataKey: {Version: []byte{0xff, 0, 9}, Data: encoded},
		"foreign.value":    {Version: []byte{7}, Data: []byte{0xff, 0, 5}},
	}
	before := storage.CloneMetadata(metadata)
	if decoded, err := decodeWindowsMetadata(metadata); err != nil || decoded != value {
		t.Fatalf("decoded Windows payload = %+v, %v", decoded, err)
	}
	if !reflect.DeepEqual(metadata, before) {
		t.Fatal("decoding changed source metadata")
	}
	metadata[windowsMetadataKey].Version[0] = 2
	if decoded, err := decodeWindowsMetadata(metadata); err != nil || decoded != value {
		t.Fatalf("authority CAS token became format version: %+v, %v", decoded, err)
	}
	encoded[0] = 0
	again, err := encodeWindowsMetadata(value)
	if err != nil || !bytes.Equal(again, want) {
		t.Fatalf("encoder retained caller-owned bytes: %x, %v", again, err)
	}
	for _, attributes := range []uint32{0, dosReadOnly, dosHidden | dosSystem | dosArchive, dosNormal} {
		input := windowsMetadata{Attributes: attributes}
		if got, err := decodeWindowsMetadata(metadataTestValue(t, input)); err != nil || got != input {
			t.Fatalf("round trip %+v = %+v, %v", input, got, err)
		}
	}
}

func TestWindowsMetadataDistinguishesAbsenceFromInvalidPresence(t *testing.T) {
	for _, metadata := range []map[string]storage.OpaquePayload{nil, {}, {"foreign": {Version: []byte{1}, Data: []byte("opaque")}}} {
		if got, err := decodeWindowsMetadata(metadata); err != nil || got != (windowsMetadata{}) {
			t.Fatalf("absent Windows namespace = %+v, %v", got, err)
		}
	}
	valid := metadataTestValue(t, windowsMetadata{})[windowsMetadataKey].Data
	for _, test := range []struct {
		name    string
		data    []byte
		version []byte
		want    error
	}{
		{name: "empty", version: []byte{1}, want: syscall.EIO},
		{name: "short header", data: []byte("SMW"), version: []byte{1}, want: syscall.EIO},
		{name: "bad magic", data: append([]byte("BAD\x01"), make([]byte, 4)...), version: []byte{1}, want: syscall.EIO},
		{name: "unsupported format", data: append([]byte("SMW\x02"), make([]byte, 4)...), version: []byte{1}, want: syscall.EOPNOTSUPP},
		{name: "short value", data: valid[:7], version: []byte{1}, want: syscall.EIO},
		{name: "trailing byte", data: append(bytes.Clone(valid), 0), version: []byte{1}, want: syscall.EIO},
		{name: "absent authority token", data: valid, want: syscall.EIO},
		{name: "oversized authority token", data: valid, version: make([]byte, storage.MaxObservationTokenBytes+1), want: syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeWindowsMetadata(map[string]storage.OpaquePayload{windowsMetadataKey: {Version: test.version, Data: test.data}})
			if !errors.Is(err, test.want) || got != (windowsMetadata{}) {
				t.Fatalf("invalid payload = %+v, %v; want %v", got, err, test.want)
			}
		})
	}
	for _, attributes := range []uint32{dosDirectory, dosReparsePoint, dosNormal | dosHidden, 0xffffffff} {
		if data, err := encodeWindowsMetadata(windowsMetadata{Attributes: attributes}); data != nil || !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid input bits %x = %x, %v", attributes, data, err)
		}
		metadata := metadataTestValue(t, windowsMetadata{})
		binary.LittleEndian.PutUint32(metadata[windowsMetadataKey].Data[4:], attributes)
		if got, err := decodeWindowsMetadata(metadata); got != (windowsMetadata{}) || !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid stored bits %x = %+v, %v", attributes, got, err)
		}
	}
}

func TestWindowsMetadataPreservesForeignInitialValuesAndBounds(t *testing.T) {
	initial := map[string][]byte{"foreign.one": {0xff, 0, 9}, "foreign.empty": {}, windowsMetadataKey: []byte("replaced")}
	before := storage.CloneInitialMetadata(initial)
	result, err := withWindowsMetadata(initial, windowsMetadata{Attributes: dosReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(initial, before) || !bytes.Equal(result["foreign.one"], initial["foreign.one"]) || result["foreign.empty"] == nil {
		t.Fatal("adding Windows metadata changed or discarded foreign values")
	}
	result["foreign.one"][0] = 1
	if !reflect.DeepEqual(initial, before) {
		t.Fatal("returned foreign bytes alias the input")
	}
	if !bytes.Equal(result[windowsMetadataKey], []byte{'S', 'M', 'W', 1, 1, 0, 0, 0}) {
		t.Fatalf("new Windows value = %x", result[windowsMetadataKey])
	}
	if result, err := withWindowsMetadata(nil, windowsMetadata{Attributes: dosDirectory}); result != nil || !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid Windows update = %v, %v", result, err)
	}
	if result, err := withWindowsMetadata(map[string][]byte{"BAD": {}}, windowsMetadata{}); result != nil || !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid foreign namespace = %v, %v", result, err)
	}
	full := make(map[string][]byte)
	for i := range storage.MaxMetadataNamespaces {
		full[fmt.Sprintf("foreign.%d", i)] = nil
	}
	if result, err := withWindowsMetadata(full, windowsMetadata{}); result != nil || !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("namespace overflow = %v, %v", result, err)
	}
	if len(full) != storage.MaxMetadataNamespaces {
		t.Fatal("failed update changed source map")
	}
	oversized := map[string][]byte{"foreign": make([]byte, storage.MaxMetadataValueBytes+1)}
	if result, err := withWindowsMetadata(oversized, windowsMetadata{}); result != nil || !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized foreign value = %v, %v", result, err)
	}
}

func TestWindowsMetadataProjectsOnlyKnownKindAndDOSFacts(t *testing.T) {
	for _, test := range []struct {
		name     string
		kind     storage.NodeKind
		metadata windowsMetadata
		want     uint32
	}{
		{"plain file", storage.NodeRegular, windowsMetadata{}, dosNormal},
		{"explicit normal", storage.NodeRegular, windowsMetadata{Attributes: dosNormal}, dosNormal},
		{"hidden file", storage.NodeRegular, windowsMetadata{Attributes: dosHidden}, dosHidden},
		{"directory", storage.NodeDirectory, windowsMetadata{}, dosDirectory},
		{"normal directory", storage.NodeDirectory, windowsMetadata{Attributes: dosNormal}, dosDirectory},
		{"link", storage.NodeSymlink, windowsMetadata{}, dosReparsePoint},
	} {
		t.Run(test.name, func(t *testing.T) {
			attr := storage.Attr{ID: 7, Kind: test.kind, Metadata: metadataTestValue(t, test.metadata)}
			before := attr.Clone()
			if got, err := projectWindowsAttributes(attr); err != nil || got != test.want {
				t.Fatalf("projection = %x, %v; want %x", got, err, test.want)
			}
			if !reflect.DeepEqual(attr.Metadata, before.Metadata) {
				t.Fatal("projection changed captured metadata")
			}
		})
	}
	for _, attr := range []storage.Attr{
		{Kind: storage.NodeRegular}, {ID: 7},
		{ID: 7, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{1}}}},
	} {
		if got, err := projectWindowsAttributes(attr); got != 0 || !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid projection = %x, %v", got, err)
		}
	}
	if got, err := projectWindowsAttributes(storage.Attr{ID: 7, Kind: storage.NodeRegular}); err != nil || got != dosNormal {
		t.Fatalf("absent namespace projection = %x, %v", got, err)
	}
}
