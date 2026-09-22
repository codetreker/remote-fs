package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func closeCommand(id wire.FileID, flags uint16) wire.Request {
	body := make([]byte, 24)
	binary.LittleEndian.PutUint16(body, 24)
	binary.LittleEndian.PutUint16(body[2:], flags)
	copy(body[8:], id[:])
	return wire.Request{Header: wire.Header{Command: wire.Close}, Body: body}
}

func TestCloseCommandAuthorizesRetiresAndReleasesHandle(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	var requests []authz.AccessRequest
	registry.tree.export.share.Volume = "trusted"
	registry.tree.export.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		requests = append(requests, request)
		return nil
	})
	reference := &handleReferenceProbe{}
	id, _ := reserveFileHandle(t, registry, reference, 7)
	connection := &connection{server: registry.tree.export.server}
	body, status := connection.closeHandle(t.Context(), registry.tree, closeCommand(id, 1))
	if status != statusOK || len(body) != 60 || binary.LittleEndian.Uint16(body) != 60 || binary.LittleEndian.Uint16(body[2:]) != 0 {
		t.Fatalf("CLOSE response = %x, %x", body, status)
	}
	if len(requests) != 1 || requests[0].Volume != "trusted" || requests[0].Operation != storage.OpFileClose {
		t.Fatalf("authorization = %+v", requests)
	}
	if reference.calls() != 1 || registry.get(id) != nil {
		t.Fatalf("handle cleanup calls=%d retained=%v", reference.calls(), registry.get(id) != nil)
	}
}

func TestCloseCommandKeepsHandleOnAuthorizationOrCleanupFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		authorize error
		close     error
		status    uint32
	}{
		{name: "denied", authorize: authz.ErrDenied, status: statusDenied},
		{name: "cleanup unknown", close: syscall.EIO, status: statusIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := newHandleTestRegistry(t, 1)
			registry.tree.export.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
				return test.authorize
			})
			reference := &handleReferenceProbe{close: func(context.Context, int) error { return test.close }}
			id, handle := reserveFileHandle(t, registry, reference, 8)
			connection := &connection{server: registry.tree.export.server}
			body, status := connection.closeHandle(t.Context(), registry.tree, closeCommand(id, 0))
			if body != nil || status != test.status || registry.get(id) != handle {
				t.Fatalf("CLOSE failure = %x, %x, retained=%v", body, status, registry.get(id) == handle)
			}
			if test.authorize != nil && reference.calls() != 0 {
				t.Fatal("denied CLOSE reached reference")
			}
		})
	}
}

func TestCloseCommandRejectsMalformedFlagsAndUnknownFileID(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	reference := &handleReferenceProbe{}
	id, _ := reserveFileHandle(t, registry, reference, 9)
	connection := &connection{server: registry.tree.export.server}
	if body, status := connection.closeHandle(t.Context(), registry.tree, closeCommand(id, 2)); body != nil || status != statusInvalid {
		t.Fatalf("invalid flags = %x, %x", body, status)
	}
	if reference.calls() != 0 {
		t.Fatal("malformed CLOSE reached reference")
	}
	var unknown wire.FileID
	unknown[0] = 99
	if body, status := connection.closeHandle(t.Context(), registry.tree, closeCommand(unknown, 0)); body != nil || status != statusFileClosed {
		t.Fatalf("unknown handle = %x, %x", body, status)
	}
}

func TestCloseCommandPreservesJoinedCleanupFailureClassification(t *testing.T) {
	failure := errors.New("cleanup transport failed")
	classified := &fileAuthorizationError{cause: failure, classification: syscall.EIO}
	if !errors.Is(classified, failure) || storage.ErrnoOf(classified) != syscall.EIO || fileCommandStatus(classified) != statusIO {
		t.Fatalf("classification = %v / %v", storage.ErrnoOf(classified), fileCommandStatus(classified))
	}
}

func TestFileAuthorizationPreservesRequestCancellation(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	calls := 0
	registry.tree.export.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		calls++
		return context.Canceled
	})
	connection := &connection{server: registry.tree.export.server}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := connection.authorizeFileOperation(ctx, registry.tree, storage.OpFileClose)
	if !errors.Is(err, context.Canceled) || storage.ErrnoOf(err) != syscall.EINTR || fileCommandStatus(err) != statusCancelled || calls != 0 {
		t.Fatalf("canceled authorization = %v, errno=%v, status=%#x, calls=%d", err, storage.ErrnoOf(err), fileCommandStatus(err), calls)
	}
}

func TestFileAuthorizationRechecksCancellationAfterPolicy(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	ctx, cancel := context.WithCancel(t.Context())
	registry.tree.export.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		cancel()
		return nil
	})
	connection := &connection{server: registry.tree.export.server}
	err := connection.authorizeFileOperation(ctx, registry.tree, storage.OpFileClose)
	if !errors.Is(err, context.Canceled) || storage.ErrnoOf(err) != syscall.EINTR || fileCommandStatus(err) != statusCancelled {
		t.Fatalf("post-policy cancellation = %v, errno=%v, status=%#x", err, storage.ErrnoOf(err), fileCommandStatus(err))
	}
}
