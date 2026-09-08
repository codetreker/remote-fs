package cmd_test

import (
	"errors"
	"fmt"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestStoppedAuthorityCleanupKeepsLocalErrorsVisible(t *testing.T) {
	server := serveNamespace(t)
	remote, err := httprest.Dial(server.url, &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	session, err := remote.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	server.stop()
	remoteErr := session.Close(t.Context())
	if !errors.Is(remoteErr, syscall.EIO) {
		t.Fatalf("close against stopped authority = %v, want EIO", remoteErr)
	}
	localErr := fmt.Errorf("closing local metadata: %w", syscall.EIO)
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"remote only", errors.Join(remoteErr), true},
		{"nested remote failures", errors.Join(errors.Join(remoteErr), remoteErr), true},
		{"local only", localErr, false},
		{"remote and local", errors.Join(remoteErr, localErr), false},
		{"local and remote", errors.Join(localErr, remoteErr), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := onlyFailedFileSessionCleanup(test.err); got != test.want {
				t.Fatalf("cleanup classification = %t, want %t: %v", got, test.want, test.err)
			}
		})
	}
}
