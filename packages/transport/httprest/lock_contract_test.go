package httprest_test

import (
	"net/http/httptest"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestFileLockContract(t *testing.T) {
	lockcontract.Run(t, func(t *testing.T, options locking.Options) lockcontract.Fixture {
		backend := namespaceFixtureWithLocks(t, options)
		handler, err := httprest.NewHandler(backend, nil)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		t.Cleanup(func() {
			handler.Stop()
			server.Close()
		})
		client, err := httprest.Dial(server.URL, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		return lockcontract.Fixture{Storage: client, Locks: client, Scope: client.Scope}
	})
}
