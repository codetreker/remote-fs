package replicated_test

import (
	"errors"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestServingReplicaViewsKeepsLocksPairedWithMutations(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		name := "base"
		if scoped {
			name = "scoped"
		}
		t.Run(name, func(t *testing.T) {
			authority := serve(t, httprest.DefaultLimits())
			replica, _ := mount(t, authority)
			if err := replica.Write(t.Context(), "file", []byte("before")); err != nil {
				t.Fatal(err)
			}
			owner := replicaLockOwner(t, authority.elsewhere)
			grant := replicaLockGrant(t, authority.elsewhere, owner)
			proof := locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}}
			var view storage.BoundedStorage = replica
			if scoped {
				var err error
				view, err = replica.Scope(proof)
				if err != nil {
					t.Fatal(err)
				}
			}
			proxy, closeProxy := serveReplicaView(t, view)
			status, err := proxy.QueryGrant(t.Context(), owner, grant)
			if err != nil || status.State != locking.Active {
				t.Fatalf("replica control did not resolve the underlying authority's grant: state=%s err=%v", status.State, err)
			}
			if err := proxy.Write(t.Context(), "file", []byte("anonymous")); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("anonymous proxy mutation bypassed the underlying grant: %v", err)
			}
			authorized, err := proxy.Scope(proof)
			if err != nil {
				t.Fatal(err)
			}
			if err := authorized.Write(t.Context(), "file", []byte("authorized through replica")); err != nil {
				t.Fatalf("the proxy's authority did not authorize its namespace mutation: %v", err)
			}
			attr, err := replica.Stat(t.Context(), "file")
			if err != nil || attr.Size != int64(len("authorized through replica")) {
				t.Fatalf("proxy mutation returned before replica confirmation: size=%d err=%v", attr.Size, err)
			}
			if _, err := proxy.Release(t.Context(), owner, grant); err != nil {
				t.Fatal(err)
			}
			status, err = authority.elsewhere.QueryGrant(t.Context(), owner, grant)
			if err != nil || status.State != locking.Released {
				t.Fatalf("proxy release did not reach the mutation authority: state=%s err=%v", status.State, err)
			}
			if err := proxy.Write(t.Context(), "file", []byte("after release")); err != nil {
				t.Fatalf("anonymous proxy mutation retained a released view proof: %v", err)
			}
			body, err := view.Read(t.Context(), "file")
			if err != nil || string(body) != "after release" {
				t.Fatalf("replica snapshot read required a live grant: %q, %v", body, err)
			}
			closeProxy()
			if err := replica.Write(t.Context(), "file", []byte("after proxy close")); err != nil {
				t.Fatalf("closing a serving view closed its shared replica: %v", err)
			}
			body, err = authority.elsewhere.Read(t.Context(), "file")
			if err != nil || string(body) != "after proxy close" {
				t.Fatalf("the authority did not retain the replica's final mutation: %q, %v", body, err)
			}
		})
	}
}

func serveReplicaView(t *testing.T, view storage.Storage) (*httprest.Storage, func()) {
	t.Helper()
	handler, err := httprest.NewHandler(view, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	closeProxy := sync.OnceFunc(func() {
		handler.Stop()
		server.Close()
	})
	t.Cleanup(closeProxy)
	client, err := httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client, closeProxy
}
