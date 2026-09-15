package httprest

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func windowsListingResult(t *testing.T, limit int64) *storage.WindowsListResult {
	t.Helper()
	result, err := storage.NewWindowsListResult(limit, 0, func(_ int, nameBytes int64, _ storage.WindowsBasicAttr) (int64, error) {
		return 512 + nameBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestWindowsServerListsNativeDirectoryMetadataWithoutPrefixes(t *testing.T) {
	client, _, backend := windowsServerFixture(t, DefaultFileLimits())
	remoteSession := windowsServerSession(t, client, storage.DefaultFileSessionOptions())
	remoteRoot := windowsServerOpen(t, remoteSession, windowsRootRequest()).File
	empty := windowsListingResult(t, 1<<20)
	if err := remoteRoot.ListBounded(t.Context(), empty); err != nil {
		t.Fatal(err)
	}
	if entries, err := empty.Entries(); err != nil || len(entries) != 0 {
		t.Fatalf("empty directory = %+v, %v", entries, err)
	}
	for _, name := range []string{"z", "&", "目录"} {
		if err := backend.Write(t.Context(), name, []byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	nativeSession, err := backend.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := nativeSession.Close(t.Context()); err != nil {
			t.Error(err)
		}
	}()
	nativeRoot := windowsServerOpen(t, nativeSession, windowsRootRequest()).File
	want := windowsListingResult(t, 1<<20)
	if err := nativeRoot.ListBounded(t.Context(), want); err != nil {
		t.Fatal(err)
	}
	wantEntries, err := want.Entries()
	if err != nil {
		t.Fatal(err)
	}
	got := windowsListingResult(t, 1<<20)
	if err := remoteRoot.ListBounded(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	gotEntries, err := got.Entries()
	if err != nil || !reflect.DeepEqual(gotEntries, wantEntries) {
		t.Fatalf("HTTP directory metadata = %+v, %v; native = %+v", gotEntries, err, wantEntries)
	}
	if len(gotEntries) != 4 || gotEntries[0].Name != "&" || gotEntries[1].Name != "directory" || !gotEntries[1].Attr.IsDir() || gotEntries[2].Name != "z" || gotEntries[3].Name != "目录" {
		t.Fatalf("directory names or types changed: %+v", gotEntries)
	}
	bounded := windowsListingResult(t, 513)
	if err := remoteRoot.ListBounded(t.Context(), bounded); !errors.Is(err, syscall.EIO) {
		t.Fatalf("listing beyond the caller's representation bound = %v", err)
	}
	if entries, err := bounded.Entries(); !errors.Is(err, syscall.EIO) || entries != nil {
		t.Fatalf("failed bounded listing exposed a prefix: %+v, %v", entries, err)
	}
}

func TestWindowsListingAdmissionCoversTheEncodedResponse(t *testing.T) {
	at := time.Unix(123, 456).UTC()
	attr := storage.WindowsBasicAttr{Attr: storage.Attr{ID: 3, Mode: 0o640, Size: 7, AccessTime: at, ModTime: at}, CreationTime: at, ChangeTime: at, DOSAttributes: storage.WindowsDOSArchive}
	entries := []storage.WindowsEntry{{Name: "&", Attr: attr}, {Name: "&&", Attr: attr}}
	response := emptyWindowsResponse()
	for _, entry := range entries {
		response.Entries = append(response.Entries, windowsEntry{Name: entry.Name, Attr: windowsBasicAttrOf(entry.Attr)})
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	limit := int64(len(encoded))
	complete, err := newWindowsListResult(limit)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := complete.Add(entry); err != nil {
			t.Fatalf("the encoded response fits its exact %d-byte bound: %v", limit, err)
		}
	}
	if got, err := complete.Entries(); err != nil || !reflect.DeepEqual(got, entries) {
		t.Fatalf("exact-bound listing = %+v, %v", got, err)
	}
	tight, err := newWindowsListResult(limit - 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := tight.Add(entries[0]); err != nil {
		t.Fatal(err)
	}
	if err := tight.Add(entries[1]); !errors.Is(err, syscall.EIO) {
		t.Fatalf("one byte below the complete encoded response = %v", err)
	}
	if prefix, err := tight.Entries(); !errors.Is(err, syscall.EIO) || prefix != nil {
		t.Fatalf("response-budget failure exposed a prefix: %+v, %v", prefix, err)
	}
	oversized, err := newWindowsListResult(limit)
	if err != nil {
		t.Fatal(err)
	}
	if reservation, err := oversized.Reserve(math.MaxInt64, attr); !errors.Is(err, syscall.EIO) || reservation != nil {
		t.Fatalf("oversized name was admitted before loading: %v, %v", reservation, err)
	}
	if prefix, err := oversized.Entries(); !errors.Is(err, syscall.EIO) || prefix != nil {
		t.Fatalf("oversized-name failure exposed a prefix: %+v, %v", prefix, err)
	}
}
