package limited

import (
	"errors"
	"math"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestMeasurementReservesMetadataBeforeRetainingTheEntry(t *testing.T) {
	metadata := map[string]storage.OpaquePayload{"app.payload": {Version: []byte{1}, Data: make([]byte, 1024)}}
	size, err := storage.MetadataSize(metadata)
	if err != nil {
		t.Fatal(err)
	}
	attr := storage.Attr{ID: 1, Kind: storage.NodeRegular, Size: 7}
	base, err := measurementEntryBytes(0, 1, 6, attr)
	if err != nil {
		t.Fatal(err)
	}
	full, err := measurementEntryBytes(0, 1, int64(size), attr)
	if err != nil || full <= base {
		t.Fatalf("metadata charge=%d,base=%d,%v", full, base, err)
	}
	for _, bound := range []int64{base, full - 1, full} {
		result, err := storage.NewListResult(bound, 0, measurementEntryBytes)
		if err != nil {
			t.Fatal(err)
		}
		reservation, err := result.Reserve(1, int64(size), attr)
		if bound < full {
			if reservation != nil || !errors.Is(err, syscall.EIO) {
				t.Fatalf("unfunded metadata retained: %v,%v", reservation, err)
			}
			if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EIO) {
				t.Fatalf("refused listing exposed prefix: %+v,%v", entries, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := reservation.Commit("f", metadata); err != nil {
			t.Fatal(err)
		}
		entries, err := result.Entries()
		if err != nil || len(entries) != 1 || len(entries[0].Attr.Metadata["app.payload"].Data) != 1024 {
			t.Fatalf("funded metadata=%+v,%v", entries, err)
		}
	}
	for _, invalid := range []int64{-1, 5, storage.MaxMetadataBytes + 1} {
		want := syscall.EINVAL
		if invalid > storage.MaxMetadataBytes {
			want = syscall.EFBIG
		}
		if _, err := measurementEntryBytes(0, 1, invalid, attr); !errors.Is(err, want) {
			t.Fatalf("invalid metadata size%d=%v", invalid, err)
		}
	}
	if _, err := measurementEntryBytes(0, math.MaxInt64, 6, attr); !errors.Is(err, syscall.EOVERFLOW) {
		t.Fatalf("name overflow=%v", err)
	}
}

func TestMeasurementChargesTinyNamespaceMapAllocations(t *testing.T) {
	for _, count := range []int{1, storage.MaxMetadataNamespaces} {
		metadata := make(map[string]storage.OpaquePayload, count)
		for i := range count {
			metadata[string(rune('a'+i))] = storage.OpaquePayload{Version: []byte{1}}
		}
		encoded, err := storage.MetadataSize(metadata)
		if err != nil {
			t.Fatal(err)
		}
		stamp := time.Unix(1700000000, 0)
		attr := storage.Attr{ID: 1, Kind: storage.NodeRegular, BirthTime: &stamp, ChangeTime: &stamp}
		slotBytes := int64(unsafe.Sizeof(struct {
			key   string
			value storage.OpaquePayload
		}{}))
		minimum := int64(unsafe.Sizeof(storage.Entry{})) + int64(max(count, 8))*slotBytes + int64(count*2) + 2*int64(unsafe.Sizeof(stamp)) + 1
		charge, err := measurementEntryBytes(0, 1, int64(encoded), attr)
		if err != nil || charge <= minimum {
			t.Fatalf("%d tiny namespaces charge%d cannot cover even%d retained bytes: %v", count, charge, minimum, err)
		}
		result, err := storage.NewListResult(minimum, 0, measurementEntryBytes)
		if err != nil {
			t.Fatal(err)
		}
		if reservation, err := result.Reserve(1, int64(encoded), attr); reservation != nil || !errors.Is(err, syscall.EIO) {
			t.Fatalf("%d namespaces exceeded retained bound without refusal: %v,%v", count, reservation, err)
		}
	}
}
