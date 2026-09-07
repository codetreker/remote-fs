package httprest

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestClientScopeIsFrozenAcrossEveryMutationAndBarrier(t *testing.T) {
	original := locking.MutationScope{Owner: lockTestOwner, Grants: []locking.GrantRef{lockTestGrant}}
	observed := []locking.MutationScope{}
	client := lockTestClient(t, func(request *http.Request) (*http.Response, error) {
		encoded := request.Header.Get(HeaderMutationScope)
		body, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		var scope locking.MutationScope
		if err := decodeLockJSON(body, &scope); err != nil {
			t.Fatal(err)
		}
		observed = append(observed, scope)
		return lockTestResponse(`{"barrier":{"incarnation":"log","position":1}}`), nil
	})
	scoped, err := client.WithScope(original)
	if err != nil {
		t.Fatal(err)
	}
	original.Grants[0].Generation++
	original.Owner = locking.OwnerRef{}
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"write":             func() error { return scoped.Write(ctx, "f", nil) },
		"setattr no-op":     func() error { return scoped.SetAttr(ctx, "f", storage.AttrChange{}) },
		"create":            func() error { return scoped.Create(ctx, "f") },
		"mkdir":             func() error { return scoped.Mkdir(ctx, "d") },
		"remove":            func() error { return scoped.Remove(ctx, "f") },
		"removedir":         func() error { return scoped.RemoveDir(ctx, "d") },
		"self rename":       func() error { return scoped.Rename(ctx, "f", "f") },
		"write barrier":     func() error { _, err := scoped.WriteWithBarrier(ctx, "f", nil); return err },
		"setattr barrier":   func() error { _, err := scoped.SetAttrWithBarrier(ctx, "f", storage.AttrChange{}); return err },
		"create barrier":    func() error { _, err := scoped.CreateWithBarrier(ctx, "f"); return err },
		"mkdir barrier":     func() error { _, err := scoped.MkdirWithBarrier(ctx, "d"); return err },
		"remove barrier":    func() error { _, err := scoped.RemoveWithBarrier(ctx, "f"); return err },
		"removedir barrier": func() error { _, err := scoped.RemoveDirWithBarrier(ctx, "d"); return err },
		"rename barrier":    func() error { _, err := scoped.RenameWithBarrier(ctx, "f", "f"); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); err != nil {
				t.Fatal(err)
			}
		})
	}
	expected := locking.MutationScope{Owner: lockTestOwner, Grants: []locking.GrantRef{lockTestGrant}}
	for _, scope := range observed {
		if !reflect.DeepEqual(scope, expected) {
			t.Fatal("caller mutation changed retained scope")
		}
	}
	if len(observed) != 14 {
		t.Fatalf("observed %d mutations", len(observed))
	}
}

func TestClientReadAndAnonymousViewsOmitScope(t *testing.T) {
	calls := 0
	client := lockTestClient(t, func(request *http.Request) (*http.Response, error) {
		calls++
		if request.Header.Get(HeaderMutationScope) != "" {
			t.Fatal("ordinary or anonymous request carried proofs")
		}
		return lockTestResponse(`{}`), nil
	})
	scoped, err := client.WithScope(locking.MutationScope{Owner: lockTestOwner, Grants: []locking.GrantRef{lockTestGrant}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, req := range []Request{{Op: OpRead, Path: "f"}, {Op: OpList, Path: "d"}, {Op: OpStat, Path: "f"}, {Op: OpSpace}} {
		body, err := scoped.call(locking.WithScope(ctx, locking.MutationScope{Owner: lockTestOwner, Grants: []locking.GrantRef{lockTestGrant}}), req, nil)
		if err != nil {
			t.Fatal(err)
		}
		body.release()
	}
	cleared, err := scoped.WithScope(locking.MutationScope{})
	if err != nil {
		t.Fatal(err)
	}
	if err := cleared.Write(ctx, "f", nil); err != nil {
		t.Fatal(err)
	}
	if err := scoped.Write(locking.WithScope(ctx, locking.MutationScope{}), "f", nil); err != nil {
		t.Fatal(err)
	}
	if calls != 6 {
		t.Fatalf("request count = %d", calls)
	}
}

func TestClientRejectsInvalidScopeBeforeDispatch(t *testing.T) {
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) { t.Fatal("invalid scope dispatched"); return nil, nil })
	invalid := locking.MutationScope{Owner: lockTestOwner, Grants: []locking.GrantRef{lockTestGrant, lockTestGrant}}
	if _, err := client.WithScope(invalid); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("duplicate proof = %v", err)
	}
	invalid = locking.MutationScope{Owner: locking.OwnerRef{Session: "\xff", Owner: "owner"}}
	if _, err := client.WithScope(invalid); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid UTF8 proof = %v", err)
	}
	if err := client.Write(locking.WithScope(context.Background(), invalid), "f", nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid context scope = %v", err)
	}
}
