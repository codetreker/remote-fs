package storage

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func TestMetadataCodecOwnsCanonicalOpaqueValues(t *testing.T) {
	values := map[string]OpaquePayload{"z.client": {Version: []byte{0xff, 1}, Data: []byte{0, 0xff}}, "a.client": {Version: []byte{1}, Data: []byte("raw")}}
	encoded, err := EncodeMetadata(values)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeMetadata(encoded)
	if err != nil || !reflect.DeepEqual(decoded, values) {
		t.Fatalf("metadata roundtrip=%+v %v", decoded, err)
	}
	encodedAgain, err := EncodeMetadata(map[string]OpaquePayload{"a.client": values["a.client"], "z.client": values["z.client"]})
	if err != nil || !bytes.Equal(encoded, encodedAgain) {
		t.Fatal("map iteration changed canonical encoding")
	}
	decoded["a.client"].Version[0] = 7
	decoded["z.client"].Data[0] = 9
	if values["a.client"].Version[0] != 1 || values["z.client"].Data[0] != 0 {
		t.Fatal("decoded payload aliases original")
	}
	copy := CloneMetadata(values)
	copy["a.client"].Data[0] = 1
	if values["a.client"].Data[0] != 'r' {
		t.Fatal("metadata clone aliases original")
	}
	raw := map[string][]byte{"a": {1, 2}}
	initial := CloneInitialMetadata(raw)
	initial["a"][0] = 7
	if raw["a"][0] != 1 {
		t.Fatal("initial metadata clone aliases source")
	}
	if CloneMetadata(nil) != nil || CloneInitialMetadata(nil) != nil {
		t.Fatal("nil clone changed absence")
	}
	empty, err := EncodeMetadata(nil)
	if err != nil || len(empty) != 6 {
		t.Fatalf("empty envelope=%x %v", empty, err)
	}
	if got, err := DecodeMetadata(empty); err != nil || len(got) != 0 {
		t.Fatalf("empty envelope=%+v %v", got, err)
	}
}

func TestMetadataRejectsUnboundedAndNoncanonicalInput(t *testing.T) {
	for _, name := range []string{"", strings.Repeat("a", MaxMetadataNamespaceBytes+1), "bad space", "Upper", "x/escape", "é"} {
		if CheckMetadataNamespace(name) == nil {
			t.Fatalf("accepted namespace%q", name)
		}
	}
	for _, values := range []map[string]OpaquePayload{
		{"bad key": {Version: []byte{1}}}, {"a": {}}, {"a": {Version: make([]byte, MaxObservationTokenBytes+1)}}, {"a": {Version: []byte{1}, Data: make([]byte, MaxMetadataValueBytes+1)}},
		{"a": {Version: []byte{1}, Data: make([]byte, MaxMetadataValueBytes)}, "b": {Version: []byte{1}, Data: make([]byte, MaxMetadataValueBytes)}},
	} {
		if CheckMetadata(values) == nil {
			t.Fatalf("accepted invalid metadata keys%v", reflect.ValueOf(values).MapKeys())
		}
		if _, err := EncodeMetadata(values); err == nil {
			t.Fatal("encoded invalid metadata")
		}
	}
	tooMany := map[string]OpaquePayload{}
	for i := 0; i <= MaxMetadataNamespaces; i++ {
		tooMany[strings.Repeat("a", i+1)] = OpaquePayload{Version: []byte{1}}
	}
	if !errors.Is(CheckMetadata(tooMany), syscall.EFBIG) {
		t.Fatal("namespace count unbounded")
	}
	raw := map[string][]byte{}
	for key := range tooMany {
		raw[key] = nil
	}
	if !errors.Is(CheckInitialMetadata(raw), syscall.EFBIG) {
		t.Fatal("initial namespace count unbounded")
	}
	for _, initial := range []map[string][]byte{{"bad name": nil}, {"a": make([]byte, MaxMetadataValueBytes+1)}, {"a": make([]byte, MaxMetadataValueBytes), "b": make([]byte, MaxMetadataValueBytes)}} {
		if CheckInitialMetadata(initial) == nil {
			t.Fatal("initial metadata accepted oversize/invalid names")
		}
	}
	encoded, err := EncodeMetadata(map[string]OpaquePayload{"x": {Version: []byte{1}, Data: []byte("data")}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(encoded); i++ {
		if _, err := DecodeMetadata(encoded[:i]); err == nil {
			t.Fatalf("accepted truncated envelope at%d", i)
		}
	}
	for _, bad := range [][]byte{nil, []byte("other"), append(bytes.Clone(encoded), 0), bytes.Repeat([]byte{0}, MaxMetadataBytes+1), {'R', 'F', 'M', 1, 255, 255}} {
		if _, err := DecodeMetadata(bad); !errors.Is(err, syscall.EIO) {
			t.Fatalf("bad envelope err=%v", err)
		}
	}
	duplicate := append([]byte{'R', 'F', 'M', 1, 2, 0}, encoded[6:]...)
	duplicate = append(duplicate, encoded[6:]...)
	if _, err := DecodeMetadata(duplicate); err == nil {
		t.Fatal("duplicate keys accepted")
	}
	wrong := bytes.Clone(encoded)
	wrong[14] = '!'
	if _, err := DecodeMetadata(wrong); err == nil {
		t.Fatal("invalid encoded namespace accepted")
	}
}

func TestAttrCloneAndListingOwnNewMetadataAndInstants(t *testing.T) {
	zone := time.FixedZone(strings.Repeat("zone", 1024), 3600)
	instant := time.Date(2026, 1, 2, 3, 4, 5, 6, zone)
	attr := Attr{ID: 1, Kind: NodeRegular, BirthTime: &instant, ChangeTime: &instant, Metadata: map[string]OpaquePayload{"owner": {Version: []byte{1}, Data: []byte{2}}}}
	result, err := NewListResult(1000, 0, func(int, int64, int64, Attr) (int64, error) { return 100, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Add(Entry{Name: "f", Attr: attr}); err != nil {
		t.Fatal(err)
	}
	attr.Metadata["owner"].Data[0] = 8
	instant = time.Time{}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	got := entries[0].Attr
	if got.Metadata["owner"].Data[0] != 2 || got.BirthTime.IsZero() || got.ChangeTime.IsZero() || got.BirthTime.Location() != time.UTC {
		t.Fatal("listing retained mutable caller state")
	}
	if _, err := result.Reserve(1, 6, Attr{Metadata: map[string]OpaquePayload{"bad": {}}}); err == nil {
		t.Fatal("listing accepted invalid payload version")
	}
	for _, kind := range []NodeKind{NodeRegular, NodeDirectory, NodeSymlink} {
		if err := kind.Check(); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []NodeKind{0, 4, 255} {
		if err := kind.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("unknown kind%d=%v", kind, err)
		}
	}
}

func TestMetadataSizeMatchesCanonicalEncoding(t *testing.T) {
	for _, values := range []map[string]OpaquePayload{nil, {"client": {Version: []byte{1, 0xff}, Data: []byte{2, 3, 4}}}} {
		size, err := MetadataSize(values)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := EncodeMetadata(values)
		if err != nil || size != len(encoded) {
			t.Fatalf("size=%d actual=%d err=%v", size, len(encoded), err)
		}
	}
}

func TestMetadataRetentionBudgetIncludesMapStorage(t *testing.T) {
	values := map[string]OpaquePayload{}
	for i := 0; i < MaxMetadataNamespaces; i++ {
		values[string(rune('a'+i))] = OpaquePayload{Version: []byte{1}}
	}
	encoded, err := EncodeMetadata(values)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := MetadataRetentionBytes(int64(len(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	// Key/value slots alone exceed a multiplier of tiny encoded records, before
	// the map header, buckets, payload allocations and optional times are counted.
	slots := int64(len(values)) * int64(unsafe.Sizeof("")+unsafe.Sizeof(OpaquePayload{}))
	if retained <= slots {
		t.Fatalf("metadata charge%d fails to cover%d bytes ofslots", retained, slots)
	}
	if _, err := MetadataRetentionBytes(5); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("short envelope=%v", err)
	}
	if _, err := MetadataRetentionBytes(MaxMetadataBytes + 1); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized envelope=%v", err)
	}
}

func TestMetadataClonesDoNotRetainNamespaceBackingStorage(t *testing.T) {
	backing := strings.Repeat("namespace", 1<<17) + "a"
	namespace := backing[len(backing)-1:]
	values := map[string]OpaquePayload{namespace: {Version: []byte{1}, Data: []byte{2}}}
	assertKey := func(keys map[string]OpaquePayload) {
		t.Helper()
		for key := range keys {
			if unsafe.StringData(key) == unsafe.StringData(namespace) {
				t.Fatal("bounded payload retained caller's namespace backing allocation")
			}
		}
	}
	assertKey(CloneMetadata(values))
	for key := range CloneInitialMetadata(map[string][]byte{namespace: {2}}) {
		if unsafe.StringData(key) == unsafe.StringData(namespace) {
			t.Fatal("initial payload retained caller's namespace backing allocation")
		}
	}
	result, err := NewListResult(1024, 0, func(_ int, nameBytes, metadataBytes int64, _ Attr) (int64, error) {
		return nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Add(Entry{Name: "f", Attr: Attr{ID: 1, Kind: NodeRegular, Metadata: values}}); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	assertKey(entries[0].Attr.Metadata)
}
