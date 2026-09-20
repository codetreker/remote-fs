package storage_test

import (
	"github.com/codetreker/remote-fs/packages/storage"
	"testing"
)

func TestOperationConstantsIdentifyDistinctSemanticActions(t *testing.T) {
	cases := map[storage.Operation]string{
		storage.OpVolumeStat:             "volume.stat",
		storage.OpVolumeList:             "volume.list",
		storage.OpVolumeRead:             "volume.read",
		storage.OpVolumeSpace:            "volume.space",
		storage.OpVolumeSetAttr:          "volume.set-attr",
		storage.OpVolumeWrite:            "volume.write",
		storage.OpVolumeCreate:           "volume.create",
		storage.OpVolumeMkdir:            "volume.mkdir",
		storage.OpVolumeRemove:           "volume.remove",
		storage.OpVolumeRemoveDir:        "volume.remove-dir",
		storage.OpVolumeRename:           "volume.rename",
		storage.OpReplicationSubscribe:   "replication.subscribe",
		storage.OpReplicationResubscribe: "replication.resubscribe",
		storage.OpReplicationSnapshot:    "replication.snapshot",
		storage.OpReplicationCheckpoint:  "replication.checkpoint",
		storage.OpFileSessionOpen:        "file.session-open",
		storage.OpFileStatus:             "file.status",
		storage.OpFileRenew:              "file.renew",
		storage.OpFileSessionClose:       "file.session-close",
		storage.OpFileStatNode:           "file.stat-node",
		storage.OpFileSetNodeAttr:        "file.set-node-attr",
		storage.OpFileOpen:               "file.open",
		storage.OpFileOpenNode:           "file.open-node",
		storage.OpFileOpenAt:             "file.open-at",
		storage.OpFileLookupAt:           "file.lookup-at",
		storage.OpFileMutateName:         "file.mutate-name",
		storage.OpFileOpenNodeRef:        "file.open-node-ref",
		storage.OpFileOpenChildRef:       "file.open-child-ref",
		storage.OpFileQueryAction:        "file.query-action",
		storage.OpFileQueryDeleteIntent:  "file.query-delete-intent",
		storage.OpFileAck:                "file.ack",
		storage.OpFileStat:               "file.stat",
		storage.OpFileState:              "file.state",
		storage.OpFileRead:               "file.read",
		storage.OpFileWrite:              "file.write",
		storage.OpFileTruncate:           "file.truncate",
		storage.OpFileSetAttr:            "file.set-attr",
		storage.OpFileSync:               "file.sync",
		storage.OpFileScope:              "file.scope",
		storage.OpFileSetNodeMetadata:    "file.set-node-metadata",
		storage.OpFileSetMetadata:        "file.set-metadata",
		storage.OpFileNewUseOwner:        "file.new-use-owner",
		storage.OpFileRetireUseOwner:     "file.retire-use-owner",
		storage.OpFileRangeGetConflict:   "file.range-get-conflict",
		storage.OpFileRangeApply:         "file.range-apply",
		storage.OpFileRangeQuery:         "file.range-query",
		storage.OpFileRangeCancel:        "file.range-cancel",
		storage.OpFileRangeDrop:          "file.range-drop",
		storage.OpFileSetPendingUnlink:   "file.set-pending-unlink",
		storage.OpFileClearPendingUnlink: "file.clear-pending-unlink",
		storage.OpFileMutate:             "file.mutate",
		storage.OpFileClose:              "file.close",
		storage.OpLockSessionEnrollment:  "lock.session-enrollment",
		storage.OpLockSessionOpen:        "lock.session-open",
		storage.OpLockSessionClose:       "lock.session-close",
		storage.OpLockOwnerCreate:        "lock.owner-create",
		storage.OpLockOwnerRetire:        "lock.owner-retire",
		storage.OpLockResolve:            "lock.resolve",
		storage.OpLockAcquire:            "lock.acquire",
		storage.OpLockRenew:              "lock.renew",
		storage.OpLockRelease:            "lock.release",
		storage.OpLockCancel:             "lock.cancel",
		storage.OpLockQueryAction:        "lock.query-action",
		storage.OpLockQueryGrant:         "lock.query-grant",
		storage.OpLockStatus:             "lock.status",
	}
	if len(cases) != 65 {
		t.Fatalf("operation vocabulary has %d entries, want 65", len(cases))
	}
	for operation, want := range cases {
		if string(operation) != want {
			t.Errorf("operation %q, want %q", operation, want)
		}
	}
}
