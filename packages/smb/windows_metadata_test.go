package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"syscall"
	"testing"

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

func TestWindowsMetadataEncodingOwnsItsFormatAndBytes(t *testing.T) {
	value := windowsMetadata{Attributes: dosHidden | dosArchive, DirectorySymlink: true}
	encoded, err := encodeWindowsMetadata(value)
	want := []byte{'S', 'M', 'W', 1, 0x22, 0, 0, 0, 1, 0, 0, 0}
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
		for _, directory := range []bool{false, true} {
			input := windowsMetadata{Attributes: attributes, DirectorySymlink: directory}
			if got, err := decodeWindowsMetadata(metadataTestValue(t, input)); err != nil || got != input {
				t.Fatalf("round trip %+v = %+v, %v", input, got, err)
			}
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
		{name: "bad magic", data: append([]byte("BAD\x01"), make([]byte, 8)...), version: []byte{1}, want: syscall.EIO},
		{name: "unsupported format", data: append([]byte("SMW\x02"), make([]byte, 8)...), version: []byte{1}, want: syscall.EOPNOTSUPP},
		{name: "short value", data: valid[:11], version: []byte{1}, want: syscall.EIO},
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
	metadata := metadataTestValue(t, windowsMetadata{})
	binary.LittleEndian.PutUint32(metadata[windowsMetadataKey].Data[8:], 2)
	if got, err := decodeWindowsMetadata(metadata); got != (windowsMetadata{}) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown hint = %+v, %v", got, err)
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
	if !bytes.Equal(result[windowsMetadataKey], []byte{'S', 'M', 'W', 1, 1, 0, 0, 0, 0, 0, 0, 0}) {
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
		{"directory link", storage.NodeSymlink, windowsMetadata{Attributes: dosNormal, DirectorySymlink: true}, dosDirectory | dosReparsePoint},
		{"hidden directory link", storage.NodeSymlink, windowsMetadata{Attributes: dosHidden, DirectorySymlink: true}, dosHidden | dosDirectory | dosReparsePoint},
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
		{ID: 7, Kind: storage.NodeRegular, Metadata: metadataTestValue(t, windowsMetadata{DirectorySymlink: true})},
		{ID: 7, Kind: storage.NodeDirectory, Metadata: metadataTestValue(t, windowsMetadata{DirectorySymlink: true})},
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
