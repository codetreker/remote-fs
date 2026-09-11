package authz_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
)

func TestAuthorizerFuncPreservesHostContextIntentAndError(t *testing.T) {
	type identityKey struct{}
	ctx := context.WithValue(t.Context(), identityKey{}, "host-owned-identity")
	request := authz.AccessRequest{Volume: "configured-volume", Operation: authz.FileOpen,
		Open: authz.OpenAccess{Read: true, Write: true, Create: true, Truncate: true, Exclusive: true}}
	cause := errors.New("policy lookup failed")
	calls := 0
	policy := authz.AuthorizerFunc(func(got context.Context, copied authz.AccessRequest) error {
		calls++
		if got != ctx || got.Value(identityKey{}) != "host-owned-identity" || copied != request {
			t.Fatalf("adapter changed host context or intent: %+v", copied)
		}
		copied.Volume = "different"
		copied.Open.Read = false
		return cause
	})
	var authorizer authz.Authorizer = policy
	if err := authorizer.Authorize(ctx, request); err != cause || calls != 1 {
		t.Fatalf("adapter calls=%d error=%v", calls, err)
	}
	if request.Volume != "configured-volume" || !request.Open.Read {
		t.Fatal("policy changed the caller's request value")
	}
	if err := authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil }).Authorize(ctx, request); err != nil {
		t.Fatalf("allow decision changed: %v", err)
	}
}

func TestDeniedMarkerSurvivesHostErrorWrappingAndJoining(t *testing.T) {
	for _, failure := range []error{authz.ErrDenied, fmt.Errorf("host policy: %w", authz.ErrDenied), errors.Join(errors.New("host detail"), authz.ErrDenied)} {
		if !errors.Is(failure, authz.ErrDenied) {
			t.Fatalf("explicit refusal marker was lost: %v", failure)
		}
	}
	if errors.Is(errors.New("access denied"), authz.ErrDenied) {
		t.Fatal("matching text invented an explicit policy decision")
	}
}

func TestOperationConstantsIdentifyDistinctSemanticActions(t *testing.T) {
	cases := map[authz.Operation]string{
		authz.VolumeStat:             "volume.stat",
		authz.VolumeList:             "volume.list",
		authz.VolumeRead:             "volume.read",
		authz.VolumeSpace:            "volume.space",
		authz.VolumeSetAttr:          "volume.set-attr",
		authz.VolumeWrite:            "volume.write",
		authz.VolumeCreate:           "volume.create",
		authz.VolumeMkdir:            "volume.mkdir",
		authz.VolumeRemove:           "volume.remove",
		authz.VolumeRemoveDir:        "volume.remove-dir",
		authz.VolumeRename:           "volume.rename",
		authz.ReplicationSubscribe:   "replication.subscribe",
		authz.ReplicationResubscribe: "replication.resubscribe",
		authz.ReplicationSnapshot:    "replication.snapshot",
		authz.FileSessionOpen:        "file.session-open",
		authz.FileStatus:             "file.status",
		authz.FileRenew:              "file.renew",
		authz.FileSessionClose:       "file.session-close",
		authz.FileStatNode:           "file.stat-node",
		authz.FileSetNodeAttr:        "file.set-node-attr",
		authz.FileOpen:               "file.open",
		authz.FileOpenNode:           "file.open-node",
		authz.FileAck:                "file.ack",
		authz.FileStat:               "file.stat",
		authz.FileRead:               "file.read",
		authz.FileWrite:              "file.write",
		authz.FileTruncate:           "file.truncate",
		authz.FileSetAttr:            "file.set-attr",
		authz.FileSync:               "file.sync",
		authz.FileGetLock:            "file.get-lock",
		authz.FileSetLock:            "file.set-lock",
		authz.FileUnlock:             "file.unlock",
		authz.FileQueryLock:          "file.query-lock",
		authz.FileCancelLock:         "file.cancel-lock",
		authz.FileDropLocks:          "file.drop-locks",
		authz.FileClose:              "file.close",
		authz.LockSessionEnrollment:  "lock.session-enrollment",
		authz.LockSessionOpen:        "lock.session-open",
		authz.LockSessionClose:       "lock.session-close",
		authz.LockOwnerCreate:        "lock.owner-create",
		authz.LockOwnerRetire:        "lock.owner-retire",
		authz.LockResolve:            "lock.resolve",
		authz.LockAcquire:            "lock.acquire",
		authz.LockRenew:              "lock.renew",
		authz.LockRelease:            "lock.release",
		authz.LockCancel:             "lock.cancel",
		authz.LockQueryAction:        "lock.query-action",
		authz.LockQueryGrant:         "lock.query-grant",
		authz.LockStatus:             "lock.status",
	}
	if len(cases) != 49 {
		t.Fatalf("operation vocabulary has %d entries, want49", len(cases))
	}
	for operation, want := range cases {
		if string(operation) != want {
			t.Errorf("operation %q, want %q", operation, want)
		}
	}
}
