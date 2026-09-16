package storage_test

import (
	"errors"
	"math"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestGenericRangesPreserveIndependentAcquisitionsAndBoundaryAnchors(t *testing.T) {
	for _, scope := range []storage.RangeScope{{}, {Domain: math.MaxUint64}, {Enforced: true}} {
		if err := scope.Check(); err != nil {
			t.Fatal(err)
		}
	}
	if err := (storage.RangeScope{Domain: 1, Enforced: true}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	for _, r := range []storage.RangeAcquisition{{ID: 1}, {ID: 2, Start: math.MaxUint64, End: math.MaxUint64}, {ID: 3, Start: 7, End: 7, Boundary: true}} {
		if err := r.Check(); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range []storage.RangeAcquisition{{}, {ID: 1, Start: 2, End: 1}, {ID: 2, Start: 3, End: 4, Boundary: true}} {
		if err := r.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid range %+v: %v", r, err)
		}
	}
	duplicateSpans := []storage.RangeAcquisition{{ID: 1, Start: 3, End: 9}, {ID: 2, Start: 3, End: 9}}
	request := storage.RangeReplaceRequest{Owner: 0, ExpectedRevision: 1, Ranges: duplicateSpans}
	if err := request.Check(); err != nil {
		t.Fatalf("distinct duplicate spans: %v", err)
	}
	if err := (storage.RangeWaitRequest{Owner: 0, ExpectedRevision: 1, Ranges: duplicateSpans, DetectDeadlock: true}).Check(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*storage.RangeReplaceRequest){func(r *storage.RangeReplaceRequest) { r.ExpectedRevision = 0 }, func(r *storage.RangeReplaceRequest) { r.Scope = storage.RangeScope{Domain: 1, Enforced: true} }, func(r *storage.RangeReplaceRequest) { r.Ranges = []storage.RangeAcquisition{{ID: 1}, {ID: 1}} }, func(r *storage.RangeReplaceRequest) { r.Ranges = []storage.RangeAcquisition{{}} }} {
		bad := request
		change(&bad)
		if err := bad.Check(); err == nil {
			t.Fatal("invalid replacement accepted")
		}
	}
	request.Ranges = make([]storage.RangeAcquisition, storage.MaxRangeAcquisitions+1)
	if err := request.Check(); !errors.Is(err, syscall.ENOLCK) {
		t.Fatalf("oversized set: %v", err)
	}
}
