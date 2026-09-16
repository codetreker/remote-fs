package httprest

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileRangeSnapshotBoundCoversEveryPermittedOwnerAndAcquisition(t *testing.T) {
	snapshot := storage.RangeSnapshot{Revision: math.MaxUint64, Available: math.MaxInt, OwnerAvailable: math.MaxInt}
	empty, err := marshalFileJSON(fileResponse{Ranges: &snapshot})
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Other = []storage.HeldRange{{Owner: storage.RangeOwner{Session: strings.Repeat("\x00", storage.MaxFileSessionIDBytes), ID: math.MaxUint64}, Range: storage.RangeAcquisition{ID: math.MaxUint64, Start: math.MaxUint64, End: math.MaxUint64}}}
	one, err := marshalFileJSON(fileResponse{Ranges: &snapshot})
	if err != nil {
		t.Fatal(err)
	}
	encodedEntry := int64(len(one) - len(empty))
	complete := int64(len(empty)) + storage.MaxRangeSnapshotRanges*encodedEntry + storage.MaxRangeSnapshotRanges - 1
	if complete > fileRangeSnapshotResponseLimit() {
		t.Fatalf("complete permitted snapshot needs %d bytes, bound %d", complete, fileRangeSnapshotResponseLimit())
	}
	var decoded fileResponse
	if err := decodeFileJSON(one, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(fileRequest{Op: storage.OpFileRangeSnapshot}, decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Ranges.Other[0].Owner.Session != snapshot.Other[0].Owner.Session {
		t.Fatal("owner identity was not preserved")
	}
}

func TestFileDirectoryResponseBoundCoversOpaqueMetadataNamesAndCursor(t *testing.T) {
	instant := time.Unix(math.MinInt64, 999999999)
	attr := storage.Attr{ID: 2, Kind: storage.NodeRegular, Size: math.MaxInt64, MetadataRevision: math.MaxUint64, AccessTime: instant, ModTime: instant, CreationTime: &instant, ChangeTime: &instant,
		Metadata: storage.Metadata{{Key: strings.Repeat("k", storage.MaxMetadataKeyBytes), Version: math.MaxUint32, Data: make([]byte, storage.MaxMetadataBytes-6-10-storage.MaxMetadataKeyBytes)}}}
	name := []byte(strings.Repeat("\xff", storage.MaxEntryNameBytes))
	charge, err := storage.DirectoryEntryBytes(len(name), storage.MaxMetadataBytes)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.DirectoryPageRequest{MaxEntries: 1, MaxBytes: storage.DirectoryPageBaseBytes + charge}
	page := storage.DirectoryPage{ParentID: 1, Revision: math.MaxUint64, Entries: []storage.DirectoryEntry{{EntryID: math.MaxUint64, Name: name, Attr: attr}}, Next: storage.DirectoryCursor{ParentID: 1, Revision: math.MaxUint64, After: name}}
	wire, err := fileDirectoryPageOf(page)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := marshalFileJSON(fileResponse{Page: wire})
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(encoded)) > fileDirectoryResponseLimit(request) {
		t.Fatalf("page needs %d bytes, bound %d", len(encoded), fileDirectoryResponseLimit(request))
	}
	var decoded fileResponse
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(fileRequest{Op: storage.OpFileListAt, List: &request}, decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded.Page.Next.After) != string(name) || len(decoded.Page.Entries[0].Attr.Metadata) != storage.MaxMetadataBytes {
		t.Fatal("bounded directory facts changed")
	}
}
