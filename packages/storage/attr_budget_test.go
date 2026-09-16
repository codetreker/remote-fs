package storage_test

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestAttributeResultBudgetUsesUnloadedFactsAndCurrentRequest(t *testing.T) {
	birth := time.Unix(123, 4).UTC()
	scalar := storage.Attr{ID: 1, Kind: storage.NodeRegular, BirthTime: &birth}
	calls := 0
	refusal := errors.New("response capacity exhausted")
	ctx := storage.WithAttrResultBudget(t.Context(), func(got storage.Attr, bytes int64) error {
		calls++
		if got.ID != 1 || got.Metadata != nil || bytes != 128 {
			t.Fatalf("admission gotloaded/wrongfacts:%+v %d", got, bytes)
		}
		*got.BirthTime = time.Time{}
		return refusal
	})
	if err := storage.CheckAttrResultBudget(ctx, scalar, 128); err != refusal || calls != 1 {
		t.Fatalf("callback result=%v calls=%d", err, calls)
	}
	if scalar.BirthTime.IsZero() {
		t.Fatal("callback mutated producer's instant")
	}
	if err := storage.CheckAttrResultBudget(t.Context(), scalar, 6); err != nil {
		t.Fatalf("absentcallerbudget=%v", err)
	}
	if calls != 1 {
		t.Fatal("request budget escaped into another context")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := storage.CheckAttrResultBudget(cancelled, scalar, 128); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancelled admission=%v calls=%d", err, calls)
	}
}

func TestAttributeResultBudgetRejectsPayloadBeforeCallback(t *testing.T) {
	calls := 0
	ctx := storage.WithAttrResultBudget(t.Context(), func(storage.Attr, int64) error { calls++; return nil })
	for _, test := range []struct {
		attr  storage.Attr
		bytes int64
		want  syscall.Errno
	}{
		{storage.Attr{}, 5, syscall.EIO}, {storage.Attr{}, -1, syscall.EIO}, {storage.Attr{}, storage.MaxMetadataBytes + 1, syscall.EFBIG},
		{storage.Attr{Metadata: map[string]storage.OpaquePayload{}}, 6, syscall.EINVAL},
	} {
		if err := storage.CheckAttrResultBudget(ctx, test.attr, test.bytes); !errors.Is(err, test.want) {
			t.Fatalf("admission error=%v want%v", err, test.want)
		}
	}
	if calls != 0 {
		t.Fatal("malformed or alreadyloadedpayload reachedbudget callback")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("nilbudgetaccepted")
		}
	}()
	_ = storage.WithAttrResultBudget(t.Context(), nil)
}

func TestAttributeResultBudgetPresenceIsRequestLocal(t *testing.T) {
	base := t.Context()
	if storage.HasAttrResultBudget(base) {
		t.Fatal("unbudgeted context advertises caller admission")
	}
	calls := 0
	bounded := storage.WithAttrResultBudget(base, func(storage.Attr, int64) error { calls++; return nil })
	inherited, cancel := context.WithCancel(bounded)
	cancel()
	if !storage.HasAttrResultBudget(bounded) || !storage.HasAttrResultBudget(inherited) {
		t.Fatal("request budget lost through context inheritance")
	}
	if storage.HasAttrResultBudget(base) || storage.HasAttrResultBudget(context.Background()) || calls != 0 {
		t.Fatal("presence query escaped context or invoked accounting")
	}
}
