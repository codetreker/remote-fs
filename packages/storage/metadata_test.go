package storage_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestMetadataCanonicalEncodingAndForeignValueOwnership(t *testing.T) {
	original := storage.Metadata{{Key: "app", Version: 1, Data: []byte{1, 255}}}
	encoded, err := storage.EncodeMetadata(original)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(encoded) != "52464d0101000300010000000200000061707001ff" {
		t.Fatalf("encoding %x", encoded)
	}
	if n, err := original.EncodedSize(); err != nil || n != len(encoded) {
		t.Fatalf("size %d %v", n, err)
	}
	decoded, err := storage.DecodeMetadata(encoded)
	if err != nil || !reflect.DeepEqual(decoded, original) {
		t.Fatalf("decoded %#v %v", decoded, err)
	}
	encoded[len(encoded)-1] = 0
	if decoded[0].Data[1] != 255 {
		t.Fatal("decoded metadata aliases encoded input")
	}
	got, ok := original.Get("app")
	if !ok {
		t.Fatal("missing owned key")
	}
	got.Data[0] = 9
	if original[0].Data[0] != 1 {
		t.Fatal("Get aliases original")
	}
	if _, ok := original.Get("absent"); ok {
		t.Fatal("absent key appeared")
	}
	next, err := original.With(storage.OpaqueMetadata{Key: "other", Version: 7, Data: []byte("opaque")})
	if err != nil || len(next) != 2 || next[0].Key != "app" {
		t.Fatalf("insert %#v %v", next, err)
	}
	next[0].Data[0] = 8
	if original[0].Data[0] != 1 {
		t.Fatal("With lost foreign ownership")
	}
	replacement, err := original.With(storage.OpaqueMetadata{Key: "app", Version: 2, Data: []byte("new")})
	if err != nil || len(replacement) != 1 || replacement[0].Version != 2 || original[0].Version != 1 {
		t.Fatalf("replace %#v %v", replacement, err)
	}
	clone := original.Clone()
	clone[0].Data[0] = 7
	if original[0].Data[0] != 1 || storage.Metadata(nil).Clone() != nil {
		t.Fatal("clone ownership/nil changed")
	}
	empty, err := storage.EncodeMetadata(nil)
	if err != nil || hex.EncodeToString(empty) != "52464d010000" {
		t.Fatalf("empty %x %v", empty, err)
	}
	if out, err := storage.DecodeMetadata(empty); err != nil || len(out) != 0 {
		t.Fatalf("empty decode %#v %v", out, err)
	}
}

func TestMetadataRejectsInvalidAndOverBudgetEnvelopes(t *testing.T) {
	atLimit := storage.Metadata{{Key: "a", Version: 1, Data: make([]byte, storage.MaxMetadataBytes-17)}}
	if n, err := atLimit.EncodedSize(); err != nil || n != storage.MaxMetadataBytes {
		t.Fatalf("at limit %d %v", n, err)
	}
	atLimit[0].Data = append(atLimit[0].Data, 0)
	invalid := []storage.Metadata{
		atLimit, {{Key: "", Version: 1}}, {{Key: "A", Version: 1}}, {{Key: "a/b", Version: 1}}, {{Key: "a\x00", Version: 1}},
		{{Key: strings.Repeat("a", storage.MaxMetadataKeyBytes+1), Version: 1}}, {{Key: "a", Version: 0}},
		{{Key: "b", Version: 1}, {Key: "a", Version: 1}}, {{Key: "a", Version: 1}, {Key: "a", Version: 2}},
	}
	var full storage.Metadata
	for i := 0; i < storage.MaxMetadataEntries; i++ {
		full = append(full, storage.OpaqueMetadata{Key: fmt.Sprintf("a%02d", i), Version: 1})
	}
	if err := full.Check(); err != nil {
		t.Fatal(err)
	}
	invalid = append(invalid, append(full, storage.OpaqueMetadata{Key: "z", Version: 1}))
	for i, m := range invalid {
		if err := m.Check(); err == nil {
			t.Fatalf("invalid %d accepted", i)
		}
		if _, err := m.EncodedSize(); err == nil {
			t.Fatalf("invalid size %d accepted", i)
		}
		if _, err := storage.EncodeMetadata(m); err == nil {
			t.Fatalf("invalid encoding %d accepted", i)
		}
		if _, err := m.With(storage.OpaqueMetadata{Key: "valid", Version: 1}); err == nil {
			t.Fatalf("invalid base %d accepted", i)
		}
	}
	if _, err := (storage.Metadata{}).With(storage.OpaqueMetadata{Key: "A", Version: 1}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid update %v", err)
	}
}

func TestMetadataDecoderFailsClosedOnCorruption(t *testing.T) {
	data, _ := storage.EncodeMetadata(storage.Metadata{{Key: "a", Version: 1, Data: []byte{1, 2}}})
	for i := 0; i < len(data); i++ {
		if _, err := storage.DecodeMetadata(data[:i]); !errors.Is(err, syscall.EIO) {
			t.Fatalf("truncated %d: %v", i, err)
		}
	}
	corrupt := [][]byte{append(bytes.Clone(data), 0), make([]byte, storage.MaxMetadataBytes+1)}
	for _, index := range []int{0, 3, 4, 6, 12, 16} {
		copy := bytes.Clone(data)
		copy[index] = 255
		corrupt = append(corrupt, copy)
	}
	// The count is finite before entry allocation; zero entries cannot hide trailing bytes.
	zeroVersion := bytes.Clone(data)
	clear(zeroVersion[8:12])
	corrupt = append(corrupt, zeroVersion)
	noEntries := bytes.Clone(data)
	noEntries[4] = 0
	corrupt = append(corrupt, noEntries)
	for i, input := range corrupt {
		if _, err := storage.DecodeMetadata(input); !errors.Is(err, syscall.EIO) {
			t.Fatalf("corrupt %d accepted: %v", i, err)
		}
	}
}

func TestGenericAttrPreservesUnknownTimesAndOwnsMetadata(t *testing.T) {
	at := time.Date(2026, 9, 16, 1, 2, 3, 4, time.FixedZone("custom", 3600))
	attr := storage.Attr{ID: 1, Kind: storage.NodeRegular, MetadataRevision: 1, CreationTime: &at, Metadata: storage.Metadata{{Key: "app", Version: 1, Data: []byte{1}}}}
	if err := attr.Check(); err != nil {
		t.Fatal(err)
	}
	clone := attr.Clone()
	clone.Metadata[0].Data[0] = 2
	if clone.CreationTime == attr.CreationTime || !clone.CreationTime.Equal(at) || clone.CreationTime.Location() != time.UTC || clone.ChangeTime != nil || attr.Metadata[0].Data[0] != 1 {
		t.Fatal("clone lost ownership or unknown time")
	}
	zero := time.Time{}
	attr.ChangeTime = &zero
	if clone = attr.Clone(); clone.ChangeTime == attr.ChangeTime || !clone.ChangeTime.IsZero() {
		t.Fatal("known zero instant lost")
	}
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory, storage.NodeSymlink} {
		if err := kind.Check(); err != nil {
			t.Fatal(err)
		}
	}
	for _, bad := range []storage.NodeKind{0, 255} {
		if err := bad.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatal(err)
		}
	}
	for _, mutate := range []func(*storage.Attr){func(a *storage.Attr) { a.ID = 0 }, func(a *storage.Attr) { a.Size = -1 }, func(a *storage.Attr) { a.Kind = 0 }, func(a *storage.Attr) { a.MetadataRevision = 0 }, func(a *storage.Attr) { a.DirectoryRevision = 1 }} {
		bad := attr.Clone()
		mutate(&bad)
		if err := bad.Check(); err == nil {
			t.Fatal("invalid attr accepted")
		}
	}
	directory := storage.Attr{ID: 1, Kind: storage.NodeDirectory, MetadataRevision: 1, DirectoryRevision: 1}
	if err := directory.Check(); err != nil {
		t.Fatal(err)
	}
}
