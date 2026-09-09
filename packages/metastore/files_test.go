package metastore_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func TestFilePublicationGuardsPreserveChainAndFirstFailure(t *testing.T) {
	ctx := context.Background()
	if err := metastore.CheckFilePublication(ctx); err != nil {
		t.Fatal(err)
	}
	var calls []int
	first := metastore.WithFilePublicationGuard(ctx, func() error { calls = append(calls, 1); return nil })
	failure := errors.New("retired reference")
	reject := false
	second := metastore.WithFilePublicationGuard(first, func() error {
		calls = append(calls, 2)
		if reject {
			return failure
		}
		return nil
	})
	if err := metastore.CheckFilePublication(second); err != nil || !reflect.DeepEqual(calls, []int{2, 1}) {
		t.Fatalf("guards = %v, %v", calls, err)
	}
	calls = nil
	reject = true
	if err := metastore.CheckFilePublication(second); err != failure || !reflect.DeepEqual(calls, []int{2}) {
		t.Fatalf("failure = %v, calls = %v", err, calls)
	}
	calls = nil
	if err := metastore.CheckFilePublication(first); err != nil || !reflect.DeepEqual(calls, []int{1}) {
		t.Fatalf("parent context changed: %v, %v", err, calls)
	}
}

func TestFilePublicationGuardRejectsNilCheck(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil guard was accepted")
		}
	}()
	metastore.WithFilePublicationGuard(context.Background(), nil)
}
