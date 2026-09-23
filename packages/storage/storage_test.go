package storage_test

import (
	"bytes"
	"errors"
	"math"
	"reflect"
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

func TestCleanPathBoundsEveryNormalizedLeaf(t *testing.T) {
	maximum := string(bytes.Repeat([]byte{'x'}, storage.MaxLeafBytes))
	if got, err := storage.CleanPath("parent/" + maximum); err != nil || got != "parent/"+maximum {
		t.Fatalf("maximum component = %q, %v", got, err)
	}
	for _, input := range []string{
		string(bytes.Repeat([]byte{'x'}, storage.MaxLeafBytes+1)),
		"parent/" + string(bytes.Repeat([]byte{'x'}, storage.MaxLeafBytes+1)),
	} {
		if got, err := storage.CleanPath(input); !errors.Is(err, syscall.ENAMETOOLONG) || got != "" {
			t.Fatalf("oversized path component %q = %q, %v", input[:16], got, err)
		}
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
			t.Fatalf("kind%d directory=%v", kind, got)
		}
	}
	zero := time.Time{}
	for _, change := range []storage.AttrChange{{BirthTime: &zero}, {AccessTime: &zero}, {ModTime: &zero}} {
		if change.Empty() || change.Check() != nil {
			t.Fatalf("explicit zero instant rejected: %+v", change)
		}
	}
	if _, exposed := reflect.TypeFor[storage.AttrChange]().FieldByName("ChangeTime"); exposed {
		t.Fatal("system-maintained change time is caller-settable")
	}
	if !(storage.AttrChange{}).Empty() || (storage.AttrChange{}).Check() != nil {
		t.Fatal("empty time change rejected")
	}
}

func TestAllocationFactRejectsContradictions(t *testing.T) {
	for _, test := range []struct {
		attr  storage.Attr
		valid bool
	}{
		{storage.Attr{}, true},
		{storage.Attr{AllocationKnown: true}, true},
		{storage.Attr{AllocationKnown: true, AllocationSize: 4096}, true},
		{storage.Attr{AllocationSize: 4096}, false},
		{storage.Attr{AllocationKnown: true, AllocationSize: -1}, false},
		{storage.Attr{AllocationSize: -1}, false},
	} {
		err := test.attr.CheckAllocation()
		if test.valid && err != nil || !test.valid && !errors.Is(err, syscall.EIO) {
			t.Fatalf("allocation %+v validated as %v, valid=%v", test.attr, err, test.valid)
		}
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

func TestListResultProjectsAttributesBeforeChargingAndRetention(t *testing.T) {
	metadata := map[string]storage.OpaquePayload{"app": {Version: []byte{1}, Data: []byte("value")}}
	metadataBytes, err := storage.MetadataSize(metadata)
	if err != nil {
		t.Fatal(err)
	}
	charged := make([]storage.Attr, 0, 2)
	result, err := storage.NewListResult(1024, 0, func(_ int, _, _ int64, attr storage.Attr) (int64, error) {
		charged = append(charged, attr)
		return attr.AllocationSize, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.ProjectAttrs(func(attr storage.Attr) storage.Attr {
		attr.AllocationSize = attr.Size * 2
		attr.AllocationKnown = true
		return attr
	}); err != nil {
		t.Fatal(err)
	}
	if err := result.Add(storage.Entry{Name: "b", Attr: storage.Attr{Size: 4, Metadata: metadata}}); err != nil {
		t.Fatal(err)
	}
	reservation, err := result.Reserve(1, int64(metadataBytes), storage.Attr{Size: 5})
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.Commit("a", metadata); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(charged) != 2 || charged[0].AllocationSize != 8 || charged[1].AllocationSize != 10 || charged[0].Metadata != nil || charged[1].Metadata != nil {
		t.Fatalf("charge received unprojected attributes or metadata: %+v", charged)
	}
	if len(entries) != 2 || entries[0].Name != "a" || entries[0].Attr.AllocationSize != 10 || entries[1].Name != "b" || entries[1].Attr.AllocationSize != 8 {
		t.Fatalf("retained entries differ from projected charge: %+v", entries)
	}
	if string(entries[0].Attr.Metadata["app"].Data) != "value" || string(entries[1].Attr.Metadata["app"].Data) != "value" {
		t.Fatalf("metadata was lost during projection: %+v", entries)
	}
	metadata["app"] = storage.OpaquePayload{Version: []byte{2}, Data: []byte("changed")}
	if string(entries[0].Attr.Metadata["app"].Data) != "value" || string(entries[1].Attr.Metadata["app"].Data) != "value" {
		t.Fatal("retained metadata aliases the caller")
	}
}

func TestListResultComposesNestedAttributeProjections(t *testing.T) {
	var charged storage.Attr
	result, err := storage.NewListResult(14, 0, func(_ int, _, _ int64, attr storage.Attr) (int64, error) {
		charged = attr
		return attr.AllocationSize, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// A wrapper registers before delegating to the backing wrapper.
	if err := result.ProjectAttrs(func(attr storage.Attr) storage.Attr {
		attr.AllocationSize *= 2
		return attr
	}); err != nil {
		t.Fatal(err)
	}
	if err := result.ProjectAttrs(func(attr storage.Attr) storage.Attr {
		attr.AllocationSize += 3
		return attr
	}); err != nil {
		t.Fatal(err)
	}
	if err := result.Add(storage.Entry{Name: "a", Attr: storage.Attr{AllocationSize: 4}}); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if charged.AllocationSize != 14 || len(entries) != 1 || entries[0].Attr.AllocationSize != 14 {
		t.Fatalf("nested projection order or retained charge differs: charge=%+v, entries=%+v", charged, entries)
	}
}

func TestListResultRejectsInvalidAttributeProjectionSetup(t *testing.T) {
	newResult := func(t *testing.T) *storage.ListResult {
		t.Helper()
		result, err := storage.NewListResult(100, 0, func(int, int64, int64, storage.Attr) (int64, error) { return 1, nil })
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	identity := func(attr storage.Attr) storage.Attr { return attr }
	var absent *storage.ListResult
	if err := absent.ProjectAttrs(identity); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("nil receiver: %v", err)
	}
	for _, scenario := range []struct {
		name  string
		setup func(*testing.T, *storage.ListResult)
		apply func(*storage.ListResult) error
	}{
		{"nil projection", func(*testing.T, *storage.ListResult) {}, func(r *storage.ListResult) error { return r.ProjectAttrs(nil) }},
		{"pending entry", func(t *testing.T, r *storage.ListResult) {
			if _, err := r.Reserve(1, 6, storage.Attr{}); err != nil {
				t.Fatal(err)
			}
		}, func(r *storage.ListResult) error { return r.ProjectAttrs(identity) }},
		{"committed entry", func(t *testing.T, r *storage.ListResult) {
			if err := r.Add(storage.Entry{Name: "a"}); err != nil {
				t.Fatal(err)
			}
		}, func(r *storage.ListResult) error { return r.ProjectAttrs(identity) }},
		{"projected metadata", func(*testing.T, *storage.ListResult) {}, func(r *storage.ListResult) error {
			if err := r.ProjectAttrs(func(attr storage.Attr) storage.Attr { attr.Metadata = map[string]storage.OpaquePayload{}; return attr }); err != nil {
				return err
			}
			return r.Add(storage.Entry{Name: "a"})
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			result := newResult(t)
			scenario.setup(t, result)
			if err := scenario.apply(result); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("invalid projection setup = %v; want EINVAL", err)
			}
			if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("failed listing exposed entries: %+v, %v", entries, err)
			}
		})
	}
}
