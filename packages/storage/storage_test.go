package storage_test

import (
	"errors"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestCleanPathVolumeBoundary(t *testing.T) {
	for _, tc := range []struct {
		input, want string
		invalid     bool
	}{
		{"", "", false}, {".", "", false}, {"a/..", "", false}, {"a//b/./", "a/b", false},
		{"a/../b", "b", false}, {"../a", "", true}, {"a/../..", "", true}, {"/a", "", true},
		{"..hidden", "..hidden", false}, {"a\\b", "a\\b", false}, {"a/\xff", "a/\xff", false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := storage.CleanPath(tc.input)
			if tc.invalid {
				if !errors.Is(err, syscall.EINVAL) || got != "" {
					t.Fatalf("CleanPath(%q) = %q, %v", tc.input, got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("CleanPath(%q) = %q, %v; want %q", tc.input, got, err, tc.want)
			}
		})
	}
}

func TestSpaceRejectsImpossibleCapacity(t *testing.T) {
	for _, tc := range []struct {
		space storage.Space
		want  bool
	}{
		{storage.Space{}, true}, {storage.Space{Total: 100, Used: 40, Avail: 60}, true},
		{storage.Space{Total: 100, Used: 40, Avail: 20}, true}, {storage.Space{Total: 10, Used: 20}, true},
		{storage.Space{Total: 10, Used: 20, Avail: 1}, false}, {storage.Space{Total: 10, Used: 2, Avail: 9}, false},
		{storage.Space{Total: -1}, false}, {storage.Space{Used: -1}, false}, {storage.Space{Avail: -1}, false},
		{storage.Space{Total: math.MaxInt64, Used: math.MaxInt64}, true},
		{storage.Space{Total: 0, Used: math.MaxInt64, Avail: 1}, false},
	} {
		if got := tc.space.Coherent(); got != tc.want {
			t.Errorf("%+v.Coherent() = %v; want %v", tc.space, got, tc.want)
		}
	}
}

func TestAttrKindAndChangeValidation(t *testing.T) {
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory, storage.NodeSymlink} {
		if got := (storage.Attr{Kind: kind}).IsDir(); got != (kind == storage.NodeDirectory) {
			t.Fatalf("IsDir(%v) = %v", kind, got)
		}
	}
	zeroTime := time.Time{}
	empty := storage.Metadata{}
	for _, change := range []storage.AttrChange{{ExpectedRevision: 1, Metadata: &empty}, {AccessTime: &zeroTime}, {ModTime: &zeroTime}, {CreationTime: &zeroTime}, {ChangeTime: &zeroTime}} {
		if change.Empty() || change.Check() != nil {
			t.Errorf("explicit zero change rejected: %+v", change)
		}
	}
	if !(storage.AttrChange{}).Empty() || (storage.AttrChange{}).Check() != nil {
		t.Fatal("empty change is invalid")
	}
	if err := (storage.AttrChange{Metadata: &empty}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("unversioned metadata replacement: %v", err)
	}
	invalid := storage.Metadata{{Key: "X", Version: 1}}
	if err := (storage.AttrChange{ExpectedRevision: 1, Metadata: &invalid}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("malformed metadata replacement: %v", err)
	}
}

func TestListResultReportsConfiguredBound(t *testing.T) {
	var absent *storage.ListResult
	if absent.MaxBytes() != 0 {
		t.Fatal("nil result has a nonzero bound")
	}
	for _, limit := range []int64{0, 1, math.MaxInt64} {
		result, err := storage.NewListResult(limit, 0, func(int, int64, int64, storage.Attr) (int64, error) { return 0, nil })
		if err != nil {
			t.Fatal(err)
		}
		if result.MaxBytes() != limit {
			t.Fatalf("MaxBytes = %d; want %d", result.MaxBytes(), limit)
		}
	}
}

func TestListResultReservesCanonicalMetadataBeforeLoadingAndOwnsPayload(t *testing.T) {
	payload := storage.Metadata{{Key: "app", Version: 2, Data: []byte("value")}}
	size, _ := payload.EncodedSize()
	result, err := storage.NewListResult(int64(size+1), 0, func(_ int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
		if len(attr.Metadata) != 0 {
			t.Fatal("metadata loaded before reservation")
		}
		return nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := result.Reserve(1, int64(size), storage.Attr{ID: 1, Kind: storage.NodeRegular, MetadataRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.Commit("a", payload); err != nil {
		t.Fatal(err)
	}
	payload[0].Data[0] = 'x'
	entries, err := result.Entries()
	if err != nil || string(entries[0].Attr.Metadata[0].Data) != "value" {
		t.Fatalf("retained payload %+v %v", entries, err)
	}
	if _, err := result.Reserve(1, 6, storage.Attr{}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unbounded next entry: %v", err)
	}
	if entries, err := result.Entries(); err == nil || entries != nil {
		t.Fatal("partial list survived refusal")
	}
}

func TestListResultInvalidatesMismatchedMetadataAdmission(t *testing.T) {
	makeResult := func() *storage.ListResult {
		r, err := storage.NewListResult(1000, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
			return nameBytes + metadataBytes, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	for _, size := range []int64{0, 5, storage.MaxMetadataBytes + 1} {
		r := makeResult()
		if _, err := r.Reserve(1, size, storage.Attr{}); !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid declared size%d: %v", size, err)
		}
	}
	r := makeResult()
	if _, err := r.Reserve(1, 6, storage.Attr{Metadata: storage.Metadata{{Key: "a", Version: 1}}}); !errors.Is(err, syscall.EIO) {
		t.Fatal("preloaded metadata accepted")
	}
	for _, metadata := range []storage.Metadata{{{Key: "a", Version: 1}}, {{Key: "a", Version: 0}}} {
		r := makeResult()
		reservation, err := r.Reserve(1, 6, storage.Attr{})
		if err != nil {
			t.Fatal(err)
		}
		if err := reservation.Commit("a", metadata); err == nil {
			t.Fatal("mismatched metadata accepted")
		}
		if entries, err := r.Entries(); err == nil || entries != nil {
			t.Fatal("bad metadata left plausible listing")
		}
	}
	r = makeResult()
	if err := r.Add(storage.Entry{Name: "a", Attr: storage.Attr{Metadata: storage.Metadata{{Key: "a", Version: 0}}}}); err == nil {
		t.Fatal("invalid metadata Add accepted")
	}
}
