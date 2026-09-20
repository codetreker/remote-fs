package httprest_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestReplicatedClientRetainsHTTPFileCapabilities(t *testing.T) {
	meta, backend := memoryfixture.New(t, "replicated-http-capabilities", 1<<20, locking.DefaultOptions())
	if err := backend.Write(t.Context(), "file", []byte("content")); err != nil {
		t.Fatal(err)
	}
	handler, err := httprest.NewHandler(backend, meta)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	remote, err := httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	local, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	copy, err := replicated.New(t.Context(), local, remote)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := copy.Close(); err != nil {
			t.Error(err)
		}
	})
	attr, err := copy.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session, err := copy.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, ExpectedID: attr.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close(context.Background())
	if current, err := file.Stat(t.Context()); err != nil || current.ID != attr.ID || current.Kind != storage.NodeRegular {
		t.Fatalf("retained HTTP stat = %+v, %v", current, err)
	}
	scoped, ok := file.(storage.ScopedReference)
	if !ok {
		t.Fatal("replicated file does not expose its HTTP scope")
	}
	scope, err := scoped.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	owners, ok := session.(storage.UseOwners)
	if !ok {
		t.Fatal("replicated session does not expose use owners")
	}
	owner, err := owners.NewUseOwner(t.Context(), attr.ID, scope, storage.OwnerOptions{Lifetime: storage.OwnerReference})
	if err != nil {
		t.Fatal(err)
	}
	if err := owners.RetireUseOwner(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
}
