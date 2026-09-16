package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func TestWindowsMetadataWireFormat(t *testing.T) {
	metadata := windowsMetadata{Attributes: dosHidden | dosArchive, DirectorySymlink: true}
	payload, err := encodeWindowsMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x22, 0, 0, 0, 1, 0, 0, 0}
	if !bytes.Equal(payload, want) {
		t.Fatalf("encoded payload = %x; want %x", payload, want)
	}
	decoded, err := decodeWindowsMetadata(want, 1, true)
	if err != nil || decoded != metadata {
		t.Fatalf("decoded payload = %+v, %v; want %+v", decoded, err, metadata)
	}
	if !bytes.Equal(payload, want) {
		t.Fatal("decoding changed the payload")
	}
	payload[0] = 0
	again, err := encodeWindowsMetadata(metadata)
	if err != nil || !bytes.Equal(again, want) {
		t.Fatalf("encoding reused mutable bytes: %x, %v", again, err)
	}
}

func TestWindowsMetadataRoundTrip(t *testing.T) {
	for attributes := uint32(0); attributes <= dosSettableAttributes; attributes++ {
		if attributes&^(dosReadOnly|dosHidden|dosSystem|dosArchive) != 0 && attributes != dosNormal {
			continue
		}
		for _, directorySymlink := range []bool{false, true} {
			t.Run(fmt.Sprintf("attributes_%x_hint_%t", attributes, directorySymlink), func(t *testing.T) {
				metadata := windowsMetadata{Attributes: attributes, DirectorySymlink: directorySymlink}
				payload, err := encodeWindowsMetadata(metadata)
				if err != nil {
					t.Fatal(err)
				}
				if len(payload) != 8 {
					t.Fatalf("encoded %d bytes; want 8", len(payload))
				}
				decoded, err := decodeWindowsMetadata(payload, windowsMetadataVersion, true)
				if err != nil || decoded != metadata {
					t.Fatalf("round trip = %+v, %v; want %+v", decoded, err, metadata)
				}
			})
		}
	}
}

func TestWindowsMetadataAbsence(t *testing.T) {
	for _, payload := range [][]byte{nil, {}} {
		if metadata, err := decodeWindowsMetadata(payload, 0, false); err != nil || metadata != (windowsMetadata{}) {
			t.Fatalf("absent payload = %+v, %v", metadata, err)
		}
		if metadata, err := decodeWindowsMetadata(payload, 1, true); !errors.Is(err, syscall.EIO) || metadata != (windowsMetadata{}) {
			t.Fatalf("present empty payload = %+v, %v; want zero and EIO", metadata, err)
		}
	}
	if metadata, err := decodeWindowsMetadata([]byte{1}, 0, false); !errors.Is(err, syscall.EIO) || metadata != (windowsMetadata{}) {
		t.Fatalf("absent nonempty payload = %+v, %v; want zero and EIO", metadata, err)
	}
	for _, version := range []uint32{1, 2, ^uint32(0)} {
		if metadata, err := decodeWindowsMetadata(nil, version, false); !errors.Is(err, syscall.EIO) || metadata != (windowsMetadata{}) {
			t.Fatalf("absent version %d = %+v, %v; want zero and EIO", version, metadata, err)
		}
	}
}

func TestWindowsMetadataRejectsMalformedPayloads(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 4, 5, 6, 7, 9, 12, 1024} {
		if metadata, err := decodeWindowsMetadata(make([]byte, size), 1, true); !errors.Is(err, syscall.EIO) || metadata != (windowsMetadata{}) {
			t.Errorf("payload of %d bytes = %+v, %v; want zero and EIO", size, metadata, err)
		}
	}
	for _, version := range []uint32{0, 2, ^uint32(0)} {
		for _, payload := range [][]byte{nil, make([]byte, 8), make([]byte, 12)} {
			if metadata, err := decodeWindowsMetadata(payload, version, true); !errors.Is(err, syscall.EOPNOTSUPP) || metadata != (windowsMetadata{}) {
				t.Errorf("version %d with %d bytes = %+v, %v; want zero and EOPNOTSUPP", version, len(payload), metadata, err)
			}
		}
	}
	for bit := 1; bit < 32; bit++ {
		payload := []byte{2, 0, 0, 0, 0, 0, 0, 0}
		binary.LittleEndian.PutUint32(payload[4:], 1|uint32(1)<<bit)
		if metadata, err := decodeWindowsMetadata(payload, 1, true); !errors.Is(err, syscall.EIO) || metadata != (windowsMetadata{}) {
			t.Errorf("hint bit %d = %+v, %v; want zero and EIO", bit, metadata, err)
		}
	}
}

func TestWindowsMetadataRejectsInvalidAttributes(t *testing.T) {
	invalid := []uint32{dosNormal | dosReadOnly, dosNormal | dosHidden, dosNormal | dosSystem, dosNormal | dosArchive, dosSettableAttributes}
	for bit := 0; bit < 32; bit++ {
		attribute := uint32(1) << bit
		if attribute&dosSettableAttributes == 0 {
			invalid = append(invalid, attribute)
		}
	}
	for _, attributes := range invalid {
		t.Run(fmt.Sprintf("attributes_%x", attributes), func(t *testing.T) {
			if payload, err := encodeWindowsMetadata(windowsMetadata{Attributes: attributes, DirectorySymlink: true}); !errors.Is(err, syscall.EINVAL) || payload != nil {
				t.Fatalf("invalid encode = %x, %v; want nil and EINVAL", payload, err)
			}
			payload := []byte{0, 0, 0, 0, 1, 0, 0, 0}
			binary.LittleEndian.PutUint32(payload, attributes)
			if metadata, err := decodeWindowsMetadata(payload, 1, true); !errors.Is(err, syscall.EIO) || metadata != (windowsMetadata{}) {
				t.Fatalf("invalid decode = %+v, %v; want zero and EIO", metadata, err)
			}
		})
	}
}
