package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func informationTestAttr(t *testing.T) storage.Attr {
	t.Helper()
	birth, change := time.Unix(0, 0).UTC(), time.Unix(3, 0).UTC()
	return storage.Attr{ID: 0x0102030405060708, Kind: storage.NodeRegular, Size: 257,
		BirthTime: &birth, AccessTime: time.Unix(1, 0).UTC(), ModTime: time.Unix(2, 0).UTC(), ChangeTime: &change,
		Metadata: metadataTestValue(t, windowsMetadata{Attributes: dosHidden | dosArchive})}
}

func TestFileInformationCheckedTimeRangeAndResolution(t *testing.T) {
	for _, test := range []struct {
		name  string
		value time.Time
		want  uint64
	}{
		{"Windows epoch", time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC), 0},
		{"Unix epoch", time.Unix(0, 0), 116444736000000000},
		{"before Unix epoch", time.Unix(-1, 999999999), 116444735999999999},
		{"100ns precision", time.Unix(0, 123456789), 116444736001234567},
		{"time zone", time.Date(1970, 1, 1, 5, 30, 0, 0, time.FixedZone("offset", 19800)), 116444736000000000},
		{"beyond UnixNano range", time.Date(2400, 1, 1, 0, 0, 0, 0, time.UTC), 252139392000000000},
		{"largest tick", time.Unix(910692730085, 477580700), math.MaxInt64},
		{"largest sub-tick", time.Unix(910692730085, 477580799), math.MaxInt64},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := checkedWindowsTime(test.value); err != nil || got != test.want {
				t.Fatalf("FILETIME = %d, %v; want %d", got, err, test.want)
			}
		})
	}
	for _, invalid := range []time.Time{time.Time{}, time.Unix(-11644473600, -1), time.Unix(910692730085, 477580800), time.Date(40000, 1, 1, 0, 0, 0, 0, time.UTC)} {
		if got, err := checkedWindowsTime(invalid); got != 0 || !errors.Is(err, syscall.EOVERFLOW) {
			t.Fatalf("out-of-range FILETIME = %d, %v", got, err)
		}
	}
}

func TestFileInformationCapturedCreateAndQueryLayouts(t *testing.T) {
	attr := informationTestAttr(t)
	before := attr.Clone()
	size := fileSizeInformation{AllocationSize: 4096, EndOfFile: 257, AllocationUnit: 4096}
	info, err := captureCreateInformation(attr, size)
	want := wire.FileInformation{CreationTime: 116444736000000000, AccessTime: 116444736010000000,
		WriteTime: 116444736020000000, ChangeTime: 116444736030000000, AllocationSize: 4096, EndOfFile: 257, Attributes: 0x22}
	if err != nil || info != want {
		t.Fatalf("captured CREATE information = %+v, %v", info, err)
	}
	for _, test := range []struct {
		outcome storage.OpenOutcome
		action  uint32
	}{{storage.Opened, 1}, {storage.Created, 2}, {storage.Reset, 3}, {storage.Replaced, 0}} {
		action, err := createAction(test.outcome)
		if err != nil || action != test.action {
			t.Fatalf("create action %v = %d, %v", test.outcome, action, err)
		}
		id := wire.FileID{0xaa, 0xbb}
		body := wire.CreateResponseBody(wire.CreateResult{Action: action, FileInformation: info, FileID: id})
		if len(body) != 88 || binary.LittleEndian.Uint16(body) != 89 || binary.LittleEndian.Uint32(body[4:]) != test.action || !bytes.Equal(body[64:80], id[:]) {
			t.Fatalf("CREATE envelope = %x", body)
		}
		for index, value := range []uint64{want.CreationTime, want.AccessTime, want.WriteTime, want.ChangeTime, 4096, 257} {
			if got := binary.LittleEndian.Uint64(body[8+index*8:]); got != value {
				t.Fatalf("CREATE field %d = %d, want %d", index, got, value)
			}
		}
		if binary.LittleEndian.Uint32(body[56:]) != 0x22 {
			t.Fatalf("CREATE attributes = %x", body[56:60])
		}
	}
	if action, err := createAction(0); action != 0 || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown open outcome = %d, %v", action, err)
	}
	basic, err := encodeBasicInformation(attr)
	if err != nil || len(basic) != 40 || binary.LittleEndian.Uint32(basic[32:]) != 0x22 || !bytes.Equal(basic[36:], make([]byte, 4)) {
		t.Fatalf("BasicInformation = %x, %v", basic, err)
	}
	network, err := encodeNetworkOpenInformation(attr, size)
	if err != nil || len(network) != 56 || !bytes.Equal(network[:32], basic[:32]) || binary.LittleEndian.Uint64(network[32:]) != 4096 || binary.LittleEndian.Uint64(network[40:]) != 257 || binary.LittleEndian.Uint32(network[48:]) != 0x22 || !bytes.Equal(network[52:], make([]byte, 4)) {
		t.Fatalf("NetworkOpenInformation = %x, %v", network, err)
	}
	for index, value := range []uint64{want.CreationTime, want.AccessTime, want.WriteTime, want.ChangeTime} {
		if got := binary.LittleEndian.Uint64(basic[index*8:]); got != value {
			t.Fatalf("BasicInformation time %d = %d, want %d", index, got, value)
		}
	}
	if !reflect.DeepEqual(attr, before) {
		t.Fatal("encoding changed the captured attributes")
	}
	basic[0] ^= 1
	if fresh, err := encodeBasicInformation(attr); err != nil || fresh[0] == basic[0] {
		t.Fatal("encoded result bytes are not independently owned")
	}
}

func TestFileInformationRejectsUnavailableOrContradictoryCapture(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*storage.Attr, *fileSizeInformation)
		want   error
	}{
		{"unknown birth", func(a *storage.Attr, _ *fileSizeInformation) { a.BirthTime = nil }, syscall.EOPNOTSUPP},
		{"unknown change", func(a *storage.Attr, _ *fileSizeInformation) { a.ChangeTime = nil }, syscall.EOPNOTSUPP},
		{"unrepresentable access", func(a *storage.Attr, _ *fileSizeInformation) { a.AccessTime = time.Time{} }, syscall.EOVERFLOW},
		{"missing identity", func(a *storage.Attr, _ *fileSizeInformation) { a.ID = 0 }, syscall.EIO},
		{"invalid metadata", func(a *storage.Attr, _ *fileSizeInformation) {
			a.Metadata[windowsMetadataKey] = storage.OpaquePayload{Version: []byte{1}}
		}, syscall.EIO},
		{"unknown allocation", func(_ *storage.Attr, s *fileSizeInformation) { s.AllocationUnit = 0 }, syscall.EOPNOTSUPP},
		{"negative allocation", func(_ *storage.Attr, s *fileSizeInformation) { s.AllocationSize = -4096 }, syscall.EIO},
		{"negative EOF", func(_ *storage.Attr, s *fileSizeInformation) { s.EndOfFile = -1 }, syscall.EIO},
		{"unaligned allocation", func(_ *storage.Attr, s *fileSizeInformation) { s.AllocationSize = 4097 }, syscall.EIO},
		{"different revision size", func(_ *storage.Attr, s *fileSizeInformation) { s.EndOfFile = 258 }, syscall.EIO},
		{"negative captured size", func(a *storage.Attr, _ *fileSizeInformation) { a.Size = -1 }, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			attr, size := informationTestAttr(t), fileSizeInformation{AllocationSize: 4096, EndOfFile: 257, AllocationUnit: 4096}
			test.change(&attr, &size)
			if got, err := captureCreateInformation(attr, size); got != (wire.FileInformation{}) || !errors.Is(err, test.want) {
				t.Fatalf("invalid CREATE capture = %+v, %v; want %v", got, err, test.want)
			}
			if got, err := encodeNetworkOpenInformation(attr, size); got != nil || !errors.Is(err, test.want) {
				t.Fatalf("invalid network capture = %x, %v", got, err)
			}
		})
	}
	attr := informationTestAttr(t)
	attr.BirthTime = nil
	if got, err := encodeBasicInformation(attr); got != nil || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unknown basic timestamp = %x, %v", got, err)
	}
	epoch := time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC)
	attr.BirthTime = &epoch
	if got, err := encodeBasicInformation(attr); err != nil || binary.LittleEndian.Uint64(got) != 0 {
		t.Fatalf("known epoch was treated as unknown: %x, %v", got, err)
	}
	attr.Kind, attr.Size = storage.NodeDirectory, -42
	if got, err := captureCreateInformation(attr, fileSizeInformation{AllocationUnit: 512}); err != nil || got.EndOfFile != 0 || got.Attributes != dosHidden|dosArchive|dosDirectory {
		t.Fatalf("explicit directory sizes = %+v, %v", got, err)
	}
}

func TestFileInformationStandardRequiresExplicitAllocationAndLinkFacts(t *testing.T) {
	data, err := encodeStandardInformation(fileSizeInformation{AllocationSize: 4096, EndOfFile: 257, AllocationUnit: 4096}, 3, false, false)
	if err != nil || len(data) != 24 || binary.LittleEndian.Uint64(data) != 4096 || binary.LittleEndian.Uint64(data[8:]) != 257 || binary.LittleEndian.Uint32(data[16:]) != 3 || !bytes.Equal(data[20:], make([]byte, 4)) {
		t.Fatalf("StandardInformation = %x, %v", data, err)
	}
	data, err = encodeStandardInformation(fileSizeInformation{AllocationUnit: 512}, 0, true, true)
	if err != nil || len(data) != 24 || binary.LittleEndian.Uint32(data[16:]) != 0 || !bytes.Equal(data[20:], []byte{1, 1, 0, 0}) {
		t.Fatalf("pending directory facts = %x, %v", data, err)
	}
	data, err = encodeStandardInformation(fileSizeInformation{EndOfFile: 257, AllocationUnit: 512}, 1, false, false)
	if err != nil || binary.LittleEndian.Uint64(data) != 0 || binary.LittleEndian.Uint64(data[8:]) != 257 {
		t.Fatalf("explicit sparse allocation facts = %x, %v", data, err)
	}
	for _, size := range []fileSizeInformation{{}, {AllocationSize: -1, AllocationUnit: 512}, {EndOfFile: -1, AllocationUnit: 512}, {AllocationSize: 513, AllocationUnit: 512}} {
		if data, err := encodeStandardInformation(size, 1, false, false); data != nil || err == nil {
			t.Fatalf("invalid allocation facts = %x, %v", data, err)
		}
	}
}

func TestFileInformationAttributeTagDoesNotRequireHistoricalTimes(t *testing.T) {
	for _, test := range []struct {
		kind            storage.NodeKind
		directoryLink   bool
		attributes, tag uint32
	}{
		{storage.NodeRegular, false, dosNormal, 0},
		{storage.NodeDirectory, false, dosDirectory, 0},
		{storage.NodeSymlink, false, dosReparsePoint, 0xa000000c},
		{storage.NodeSymlink, true, dosReparsePoint | dosDirectory, 0xa000000c},
	} {
		attr := storage.Attr{ID: 5, Kind: test.kind, Metadata: metadataTestValue(t, windowsMetadata{DirectorySymlink: test.directoryLink})}
		data, err := encodeAttributeTagInformation(attr)
		if err != nil || len(data) != 8 || binary.LittleEndian.Uint32(data) != test.attributes || binary.LittleEndian.Uint32(data[4:]) != test.tag {
			t.Fatalf("attribute tag for %d = %x, %v", test.kind, data, err)
		}
	}
	if data, err := encodeAttributeTagInformation(storage.Attr{}); data != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("invalid attribute capture = %x, %v", data, err)
	}
}

func TestFileInformationIdentityAndAccessUseOnlyCapturedScalars(t *testing.T) {
	const node = uint64(0x0102030405060708)
	const serial = uint64(0x8877665544332211)
	internal, err := encodeInternalInformation(node)
	if err != nil || !bytes.Equal(internal, []byte{8, 7, 6, 5, 4, 3, 2, 1}) {
		t.Fatalf("internal identity = %x, %v", internal, err)
	}
	full, err := encodeFileIDInformation(node, serial)
	want := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0}
	if err != nil || !bytes.Equal(full, want) {
		t.Fatalf("volume/file identity = %x, %v", full, err)
	}
	for _, id := range []uint64{1, math.MaxUint64} {
		data, err := encodeFileIDInformation(id, serial)
		if err != nil || binary.LittleEndian.Uint64(data[8:]) != id {
			t.Fatalf("opaque identity %x truncated: %x, %v", id, data, err)
		}
	}
	other, err := encodeFileIDInformation(node, serial+1)
	if err != nil || bytes.Equal(full, other) || !bytes.Equal(full[8:], other[8:]) {
		t.Fatalf("volume identity was not separate: %x, %v", other, err)
	}
	full[8] = 0
	if again, err := encodeFileIDInformation(node, serial); err != nil || !bytes.Equal(again, want) {
		t.Fatal("identity results alias prior output")
	}
	if data, err := encodeInternalInformation(0); data != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("zero internal ID = %x, %v", data, err)
	}
	if data, err := encodeFileIDInformation(0, serial); data != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("zero file ID = %x, %v", data, err)
	}
	for _, granted := range []uint32{0, 1, 0x001f01ff, math.MaxUint32} {
		data := encodeAccessInformation(granted)
		if len(data) != 4 || binary.LittleEndian.Uint32(data) != granted {
			t.Fatalf("granted access changed: %x", data)
		}
	}
}

func TestFileInformationClassRightsAndMissingHandleFacts(t *testing.T) {
	for _, class := range []byte{4, 34, 35} {
		for _, insufficient := range []uint32{0, 1, 0x100, 0x80000000} {
			if err := checkFileInformationAccess(class, insufficient); !errors.Is(err, syscall.EACCES) {
				t.Fatalf("class %d granted %x = %v", class, insufficient, err)
			}
		}
		if err := checkFileInformationAccess(class, fileReadAttributes); err != nil {
			t.Fatalf("class %d read-attributes grant = %v", class, err)
		}
	}
	for _, class := range []byte{5, 6, 8, 59} {
		if err := checkFileInformationAccess(class, 0); err != nil {
			t.Fatalf("class %d incorrectly required attributes: %v", class, err)
		}
	}
	for _, class := range []byte{0, 7, 9, 14, 16, 18, 48, 255} {
		if err := checkFileInformationAccess(class, math.MaxUint32); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("unsupported class %d = %v", class, err)
		}
	}
}
