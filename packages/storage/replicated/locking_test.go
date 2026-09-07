package replicated_test

import (
	"errors"
	"io"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestReplicatedLockContract(t *testing.T) {
	lockcontract.Run(t, func(t *testing.T, options locking.Options) lockcontract.Fixture {
		server := serveWithLockOptions(t, httprest.DefaultLimits(), 0, options)
		mounted, _ := mount(t, server)
		return lockcontract.Fixture{Storage: mounted, Locks: mounted, Scope: mounted.Scope}
	})
}

func replicaLockOwner(t *testing.T, s *replicated.Storage) locking.OwnerRef {
	t.Helper()
	ticket, err := s.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := s.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	o, err := s.CreateOwner(t.Context(), session.ID, "replica-owner")
	if err != nil {
		t.Fatal(err)
	}
	return o.Ref
}

func replicaLockGrant(t *testing.T, s *replicated.Storage, owner locking.OwnerRef) locking.GrantRef {
	t.Helper()
	resource, err := s.Resolve(t.Context(), owner, "file")
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner, Request: "replica-acquire", Resource: resource, Mode: locking.Exclusive, TTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Receipt.Outcome != locking.Granted || result.Grant == nil {
		t.Fatal("replica acquisition did not return its authoritative grant")
	}
	return result.Grant.Ref
}

func TestScopedReplicaConfirmsMutationAndSharesLifetime(t *testing.T) {
	server := serve(t, httprest.DefaultLimits())
	server.events.slowEvents(200 * time.Millisecond)
	mounted, _ := mount(t, server)
	if err := mounted.Write(t.Context(), "file", []byte("before")); err != nil {
		t.Fatal(err)
	}
	o := replicaLockOwner(t, mounted)
	g := replicaLockGrant(t, mounted, o)
	proofs := []locking.GrantRef{g}
	view, err := mounted.Scope(locking.MutationScope{Owner: o, Grants: proofs})
	if err != nil {
		t.Fatal(err)
	}
	if _, ownsLifetime := view.(io.Closer); ownsLifetime {
		t.Fatal("a scoped replica independently owns the shared subscription and database")
	}
	if err := view.Write(locking.WithScope(t.Context(), locking.MutationScope{}), "file", []byte("anonymous")); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("an explicit anonymous request inherited the replica view's grant: %v", err)
	}
	proofs[0].Generation++
	if err := view.Write(t.Context(), "file", []byte("confirmed scoped content")); err != nil {
		t.Fatal(err)
	}
	attr, err := mounted.Stat(t.Context(), "file")
	if err != nil || attr.Size != int64(len("confirmed scoped content")) {
		t.Fatalf("scoped write returned before its replica barrier: size=%d err=%v", attr.Size, err)
	}
	if err := mounted.Write(t.Context(), "file", []byte("anonymous")); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("scoped view changed the base client's anonymous authority: %v", err)
	}
	if _, err := mounted.Release(t.Context(), o, g); err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() error{
		"write":   func() error { return view.Write(t.Context(), "file", []byte("released")) },
		"setattr": func() error { return view.SetAttr(t.Context(), "file", storage.AttrChange{}) },
		"rename":  func() error { return view.Rename(t.Context(), "file", "file") },
	} {
		if err := call(); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("%s discarded the released proof: %v", name, err)
		}
	}
	content, err := view.Read(t.Context(), "file")
	if err != nil || string(content) != "confirmed scoped content" {
		t.Fatalf("ordinary snapshot read asserted live lease state: %q, %v", content, err)
	}
}

func TestLockControlsRemainAuthoritativeWhenReplicaStreamFails(t *testing.T) {
	server := serve(t, httprest.DefaultLimits())
	mounted, _ := mount(t, server)
	if err := mounted.Write(t.Context(), "file", []byte("before")); err != nil {
		t.Fatal(err)
	}
	server.events.cut()
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := mounted.Stat(t.Context(), "file")
		if errors.Is(err, syscall.EIO) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the replica did not observe its disconnected stream")
		}
		time.Sleep(time.Millisecond)
	}
	o := replicaLockOwner(t, mounted)
	g := replicaLockGrant(t, mounted, o)
	status, err := mounted.QueryGrant(t.Context(), o, g)
	if err != nil || status.State != locking.Active {
		t.Fatalf("grant query depended on metadata replica health: state=%s err=%v", status.State, err)
	}
	renewed, err := mounted.Renew(t.Context(), locking.RenewRequest{
		Owner: o, Request: "replica-renew", Grant: g, TTL: 10 * time.Second,
	})
	if err != nil || renewed.Receipt.Outcome != locking.Renewed {
		t.Fatalf("renewal depended on metadata replica health: outcome=%s err=%v", renewed.Receipt.Outcome, err)
	}
	receipt, err := mounted.QueryAction(t.Context(), o, "replica-renew")
	if err != nil || receipt.Receipt.Outcome != locking.Renewed {
		t.Fatalf("action reconciliation depended on replica health: outcome=%s err=%v", receipt.Receipt.Outcome, err)
	}
	if _, err := mounted.Cancel(t.Context(), o, "replica-acquire"); err != nil {
		t.Fatal(err)
	}
	if _, err := mounted.Release(t.Context(), o, g); err != nil {
		t.Fatal(err)
	}
	if err := mounted.RetireOwner(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	if err := mounted.CloseSession(t.Context(), o.Session); err != nil {
		t.Fatal(err)
	}
	if err := server.elsewhere.Write(t.Context(), "file", []byte("released")); err != nil {
		t.Fatalf("authoritative cleanup left a grant held: %v", err)
	}
}

func TestReplicaAnonymousScopeClearsBoundTransportProofs(t *testing.T) {
	server := serve(t, httprest.DefaultLimits())
	mounted, _ := mount(t, server)
	if err := mounted.Write(t.Context(), "file", []byte("before")); err != nil {
		t.Fatal(err)
	}
	o := replicaLockOwner(t, mounted)
	g := replicaLockGrant(t, mounted, o)
	remote, err := server.elsewhere.WithScope(locking.MutationScope{Owner: o, Grants: []locking.GrantRef{g}})
	if err != nil {
		t.Fatal(err)
	}
	local, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	bound, err := replicated.New(t.Context(), local, remote)
	if err != nil {
		if closeErr := local.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bound.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := bound.Write(t.Context(), "file", []byte("bound")); err != nil {
		t.Fatal(err)
	}
	anonymous, err := bound.Scope(locking.MutationScope{})
	if err != nil {
		t.Fatal(err)
	}
	if err := anonymous.Write(t.Context(), "file", []byte("anonymous")); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("explicit anonymous scope inherited the transport grant: %v", err)
	}
}
