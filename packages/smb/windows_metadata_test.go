package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	return map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{1}, Data: data}}
}

func TestWindowsMetadataRoundTripAndValidation(t *testing.T) {
	value := windowsMetadata{Attributes: dosHidden | dosArchive}
	encoded, err := encodeWindowsMetadata(value)
	want := []byte{'S', 'M', 'W', 1, 0x22, 0, 0, 0}
	if err != nil || !bytes.Equal(encoded, want) {
		t.Fatalf("encoded metadata = %x, %v", encoded, err)
	}
	metadata := metadataTestValue(t, value)
	before := storage.CloneMetadata(metadata)
	if decoded, err := decodeWindowsMetadata(metadata); err != nil || decoded != value {
		t.Fatalf("decoded metadata = %+v, %v", decoded, err)
	}
	if !reflect.DeepEqual(metadata, before) {
		t.Fatal("decoding changed source metadata")
	}
	for _, attributes := range []uint32{dosDirectory, dosReparsePoint, dosNormal, dosNormal | dosHidden, 0xffffffff} {
		if data, err := encodeWindowsMetadata(windowsMetadata{Attributes: attributes}); data != nil || !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid attributes %x = %x, %v", attributes, data, err)
		}
		data := []byte{'S', 'M', 'W', 1, 0, 0, 0, 0}
		binary.LittleEndian.PutUint32(data[4:], attributes)
		stored := map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{1}, Data: data}}
		if decoded, err := decodeWindowsMetadata(stored); decoded != (windowsMetadata{}) || !errors.Is(err, syscall.EIO) {
			t.Fatalf("stored derived attributes %x = %+v, %v", attributes, decoded, err)
		}
	}
	for _, metadata := range []map[string]storage.OpaquePayload{nil, {}, {"foreign": {Version: []byte{1}, Data: []byte("opaque")}}} {
		if decoded, err := decodeWindowsMetadata(metadata); err != nil || decoded != (windowsMetadata{}) {
			t.Fatalf("absent namespace = %+v, %v", decoded, err)
		}
	}
	invalid := metadataTestValue(t, windowsMetadata{})
	invalid[windowsMetadataKey] = storage.OpaquePayload{Version: []byte{1}, Data: append(invalid[windowsMetadataKey].Data, make([]byte, 4)...)}
	if decoded, err := decodeWindowsMetadata(invalid); decoded != (windowsMetadata{}) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("obsolete derived hint payload = %+v, %v", decoded, err)
	}
}

func TestWindowsMetadataPreservesForeignInitialValues(t *testing.T) {
	initial := map[string][]byte{"foreign.one": {0xff, 0, 9}, "foreign.empty": {}, windowsMetadataKey: []byte("replaced")}
	before := storage.CloneInitialMetadata(initial)
	result, err := withWindowsMetadata(initial, windowsMetadata{Attributes: dosReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(initial, before) || !bytes.Equal(result["foreign.one"], initial["foreign.one"]) || result["foreign.empty"] == nil {
		t.Fatal("Windows metadata update changed foreign values")
	}
	result["foreign.one"][0] = 1
	if !reflect.DeepEqual(initial, before) {
		t.Fatal("returned metadata aliases the input")
	}
	if !bytes.Equal(result[windowsMetadataKey], []byte{'S', 'M', 'W', 1, 1, 0, 0, 0}) {
		t.Fatalf("Windows metadata = %x", result[windowsMetadataKey])
	}
}

func TestWindowsMetadataProjectsOnlyKnownFacts(t *testing.T) {
	for _, test := range []struct {
		kind     storage.NodeKind
		metadata windowsMetadata
		want     uint32
	}{
		{storage.NodeRegular, windowsMetadata{}, dosNormal},
		{storage.NodeRegular, windowsMetadata{Attributes: dosHidden}, dosHidden},
		{storage.NodeDirectory, windowsMetadata{}, dosDirectory},
		{storage.NodeDirectory, windowsMetadata{Attributes: dosHidden}, dosHidden | dosDirectory},
		{storage.NodeSymlink, windowsMetadata{}, dosReparsePoint},
		{storage.NodeSymlink, windowsMetadata{Attributes: dosHidden}, dosHidden | dosReparsePoint},
	} {
		attr := storage.Attr{ID: 7, Kind: test.kind, Metadata: metadataTestValue(t, test.metadata)}
		if got, err := projectWindowsAttributes(attr); err != nil || got != test.want {
			t.Fatalf("projection for %d = %x, %v; want %x", test.kind, got, err, test.want)
		}
	}
	for _, attr := range []storage.Attr{
		{Kind: storage.NodeRegular},
		{ID: 7},
		{ID: 7, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{1}, Data: []byte{'S', 'M', 'W', 1, 0, 0, 0, 0, 1, 0, 0, 0}}}},
	} {
		if got, err := projectWindowsAttributes(attr); got != 0 || !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid projection = %x, %v", got, err)
		}
	}
}
