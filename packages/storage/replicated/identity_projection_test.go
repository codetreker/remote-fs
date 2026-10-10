package replicated_test

import (
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestBackendIdentityProjectionUsesAuthorityFacts(t *testing.T) {
	authority := serve(t, httprest.DefaultLimits())
	replica, _ := mount(t, authority)
	expected, err := authority.storage.BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := replica.Scope(locking.MutationScope{})
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []storage.BackendIdentity{replica, scoped.(storage.BackendIdentity)} {
		if err := backend.CheckBackendIdentity(); err != nil {
			t.Fatal(err)
		}
		result, err := backend.BackendIdentity(t.Context())
		if result != expected || err != nil {
			t.Fatalf("authority identity projection = %+v, %v; expected %+v", result, err, expected)
		}
	}
	session := retainedSession(t, replica)
	identity, err := session.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if identity.Backend != expected || identity.SessionEpoch != status.Epoch {
		t.Fatalf("session identity = %+v; expected backend=%+v, epoch=%q", identity, expected, status.Epoch)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	authority.server.CloseClientConnections()
	authority.server.Close()
	for _, backend := range []storage.BackendIdentity{replica, scoped.(storage.BackendIdentity)} {
		if result, err := backend.BackendIdentity(t.Context()); result != (storage.BackendIdentityResult{}) || err == nil {
			t.Fatalf("unreachable backend identity = %+v, %v", result, err)
		}
	}
}
