package storage_test

import (
	"github.com/codetreker/remote-fs/packages/storage"
	"testing"
)

func TestOperationConstantsIdentifyDistinctSemanticActions(t *testing.T) {
	cases := map[storage.Operation]string{
		storage.OpVolumeStat:              "volume.stat",
		storage.OpVolumeList:              "volume.list",
		storage.OpVolumeRead:              "volume.read",
		storage.OpVolumeSpace:             "volume.space",
		storage.OpVolumeSetAttr:           "volume.set-attr",
		storage.OpVolumeWrite:             "volume.write",
		storage.OpVolumeCreate:            "volume.create",
		storage.OpVolumeMkdir:             "volume.mkdir",
		storage.OpVolumeRemove:            "volume.remove",
		storage.OpVolumeRemoveDir:         "volume.remove-dir",
		storage.OpVolumeRename:            "volume.rename",
		storage.OpReplicationSubscribe:    "replication.subscribe",
		storage.OpReplicationResubscribe:  "replication.resubscribe",
		storage.OpReplicationSnapshot:     "replication.snapshot",
		storage.OpReplicationCheckpoint:   "replication.checkpoint",
		storage.OpFileSessionOpen:         "file.session-open",
		storage.OpFileStatus:              "file.status",
		storage.OpFileRenew:               "file.renew",
		storage.OpFileSessionClose:        "file.session-close",
		storage.OpFileStatNode:            "file.stat-node",
		storage.OpFileSetNodeAttr:         "file.set-node-attr",
		storage.OpFileOpen:                "file.open",
		storage.OpFileOpenNode:            "file.open-node",
		storage.OpFileAck:                 "file.ack",
		storage.OpFileStat:                "file.stat",
		storage.OpFileRead:                "file.read",
		storage.OpFileWrite:               "file.write",
		storage.OpFileTruncate:            "file.truncate",
		storage.OpFileSetAttr:             "file.set-attr",
		storage.OpFileSync:                "file.sync",
		storage.OpFileGetLock:             "file.get-lock",
		storage.OpFileSetLock:             "file.set-lock",
		storage.OpFileUnlock:              "file.unlock",
		storage.OpFileQueryLock:           "file.query-lock",
		storage.OpFileCancelLock:          "file.cancel-lock",
		storage.OpFileDropLocks:           "file.drop-locks",
		storage.OpFileClose:               "file.close",
		storage.OpLockSessionEnrollment:   "lock.session-enrollment",
		storage.OpLockSessionOpen:         "lock.session-open",
		storage.OpLockSessionClose:        "lock.session-close",
		storage.OpLockOwnerCreate:         "lock.owner-create",
		storage.OpLockOwnerRetire:         "lock.owner-retire",
		storage.OpLockResolve:             "lock.resolve",
		storage.OpLockAcquire:             "lock.acquire",
		storage.OpLockRenew:               "lock.renew",
		storage.OpLockRelease:             "lock.release",
		storage.OpLockCancel:              "lock.cancel",
		storage.OpLockQueryAction:         "lock.query-action",
		storage.OpLockQueryGrant:          "lock.query-grant",
		storage.OpLockStatus:              "lock.status",
		storage.OpWindowsState:            "windows.state",
		storage.OpWindowsEnable:           "windows.enable",
		storage.OpWindowsQueryActivation:  "windows.query-activation",
		storage.OpWindowsSessionOpen:      "windows.session-open",
		storage.OpWindowsSessionClose:     "windows.session-close",
		storage.OpWindowsRenew:            "windows.renew",
		storage.OpWindowsStatus:           "windows.status",
		storage.OpWindowsOpen:             "windows.open",
		storage.OpWindowsStat:             "windows.stat",
		storage.OpWindowsRead:             "windows.read",
		storage.OpWindowsWrite:            "windows.write",
		storage.OpWindowsTruncate:         "windows.truncate",
		storage.OpWindowsSetAttr:          "windows.set-attr",
		storage.OpWindowsList:             "windows.list",
		storage.OpWindowsReadLink:         "windows.read-link",
		storage.OpWindowsSetLink:          "windows.set-link",
		storage.OpWindowsRename:           "windows.rename",
		storage.OpWindowsSetDeletePending: "windows.set-delete-pending",
		storage.OpWindowsLockBatch:        "windows.lock-batch",
		storage.OpWindowsSync:             "windows.sync",
		storage.OpWindowsClose:            "windows.close",
		storage.OpWindowsQueryAction:      "windows.query-action",
		storage.OpWindowsCancelAction:     "windows.cancel-action",
	}
	if len(cases) != 73 {
		t.Fatalf("operation vocabulary has %d entries, want 73", len(cases))
	}
	for operation, want := range cases {
		if string(operation) != want {
			t.Errorf("operation %q, want %q", operation, want)
		}
	}
}
