package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func informationTestAttr(t *testing.T) storage.Attr {
	t.Helper()
	birth := time.Unix(0, 0).UTC()
	change := time.Unix(3, 0).UTC()
	return storage.Attr{
		ID: 0x0102030405060708, Kind: storage.NodeRegular, Size: 257,
		BirthTime: &birth, AccessTime: time.Unix(1, 0).UTC(), ModTime: time.Unix(2, 0).UTC(), ChangeTime: &change,
		Metadata: metadataTestValue(t, windowsMetadata{Attributes: dosHidden | dosArchive}),
	}
}

func TestFileInformationTimeRangeAndUnknownHistory(t *testing.T) {
	for _, test := range []struct {
		value time.Time
		want  uint64
	}{
		{time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC), 0},
		{time.Unix(0, 0), 116444736000000000},
		{time.Unix(0, 123456789), 116444736001234567},
		{time.Unix(910692730085, 477580799), math.MaxInt64},
	} {
		if got, err := checkedWindowsTime(test.value); err != nil || got != test.want {
			t.Fatalf("FILETIME = %d, %v; want %d", got, err, test.want)
		}
	}
	for _, invalid := range []time.Time{time.Time{}, time.Unix(-11644473600, -1), time.Unix(910692730085, 477580800)} {
		if got, err := checkedWindowsTime(invalid); got != 0 || !errors.Is(err, syscall.EOVERFLOW) {
			t.Fatalf("invalid FILETIME = %d, %v", got, err)
		}
	}

	attr := informationTestAttr(t)
	attr.BirthTime, attr.ChangeTime = nil, nil
	first, err := encodeBasicInformation(attr)
	if err != nil || binary.LittleEndian.Uint64(first) != 0 || binary.LittleEndian.Uint64(first[24:]) != 0 {
		t.Fatalf("unknown historical times = %x, %v", first, err)
	}
	second, err := encodeBasicInformation(attr)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("unknown historical times were unstable = %x, %v", second, err)
	}
}

func TestFileInformationLayoutsAndClasses(t *testing.T) {
	attr := informationTestAttr(t)
	size := fileSizeInformation{AllocationSize: 4096, EndOfFile: 257, AllocationUnit: 512}
	information, err := captureCreateInformation(attr, size)
	want := wire.FileInformation{
		CreationTime: 116444736000000000, AccessTime: 116444736010000000,
		WriteTime: 116444736020000000, ChangeTime: 116444736030000000,
		AllocationSize: 4096, EndOfFile: 257, Attributes: dosHidden | dosArchive,
	}
	if err != nil || information != want {
		t.Fatalf("CREATE information = %+v, %v", information, err)
	}
	for _, test := range []struct {
		outcome storage.OpenOutcome
		action  uint32
	}{{storage.Opened, 1}, {storage.Created, 2}, {storage.Reset, 3}, {storage.Replaced, 0}} {
		if action, err := createAction(test.outcome); err != nil || action != test.action {
			t.Fatalf("action %d = %d, %v", test.outcome, action, err)
		}
	}
	basic, err := encodeBasicInformation(attr)
	if err != nil || len(basic) != 40 || binary.LittleEndian.Uint32(basic[32:]) != dosHidden|dosArchive {
		t.Fatalf("basic information = %x, %v", basic, err)
	}
	network, err := encodeNetworkOpenInformation(attr, size)
	if err != nil || len(network) != 56 || binary.LittleEndian.Uint64(network[32:]) != 4096 || binary.LittleEndian.Uint64(network[40:]) != 257 {
		t.Fatalf("network information = %x, %v", network, err)
	}
	for class, size := range map[byte]uint32{4: 40, 5: 24, 6: 8, 7: 4, 8: 4, 34: 56, 35: 8, 59: 24} {
		if got, err := fileInformationSize(class); err != nil || got != size {
			t.Fatalf("class %d size = %d, %v", class, got, err)
		}
	}
	for _, class := range []byte{0, 1, 3, 9, 255} {
		if _, err := fileInformationSize(class); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("unsupported class %d = %v", class, err)
		}
		if err := checkFileInformationAccess(class, math.MaxUint32); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("unsupported access class %d = %v", class, err)
		}
	}
}

func TestFileInformationIdentityAndStandardFacts(t *testing.T) {
	standard, err := encodeStandardInformation(fileSizeInformation{AllocationSize: 4096, EndOfFile: 257, AllocationUnit: 512}, 3, true, false)
	if err != nil || len(standard) != 24 || binary.LittleEndian.Uint32(standard[16:]) != 3 || standard[20] != 1 {
		t.Fatalf("standard information = %x, %v", standard, err)
	}
	internal, err := encodeInternalInformation(0x0102030405060708)
	if err != nil || !bytes.Equal(internal, []byte{8, 7, 6, 5, 4, 3, 2, 1}) {
		t.Fatalf("internal information = %x, %v", internal, err)
	}
	fileID, err := encodeFileIDInformation(0x0102030405060708, 0x8877665544332211)
	if err != nil || binary.LittleEndian.Uint64(fileID) != 0x8877665544332211 || binary.LittleEndian.Uint64(fileID[8:]) != 0x0102030405060708 {
		t.Fatalf("file ID information = %x, %v", fileID, err)
	}
	if data, err := encodeInternalInformation(0); data != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("zero identity = %x, %v", data, err)
	}
}
