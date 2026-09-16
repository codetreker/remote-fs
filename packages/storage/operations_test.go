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
		storage.OpFileState:              "file.state",
		storage.OpFileSessionOpen:        "file.session-open",
		storage.OpFileStatus:             "file.status",
		storage.OpFileRenew:              "file.renew",
		storage.OpFileSessionClose:       "file.session-close",
		storage.OpFileStatNode:           "file.stat-node",
		storage.OpFileSetNodeAttr:        "file.set-node-attr",
		storage.OpFileRetain:             "file.retain",
		storage.OpFileRetainAt:           "file.retain-at",
		storage.OpFileCreateAndRetainAt:  "file.create-and-retain-at",
		storage.OpFileResetAndRetainAt:   "file.reset-and-retain-at",
		storage.OpFileReplaceAndRetainAt: "file.replace-and-retain-at",
		storage.OpFileReference:          "file.reference",
		storage.OpFileQueryAction:        "file.query-action",
		storage.OpFileCancelAction:       "file.cancel-action",
		storage.OpFileRetireRangeOwner:   "file.retire-range-owner",
		storage.OpFileStat:               "file.stat",
		storage.OpFileCheckObservation:   "file.check-observation",
		storage.OpFileRead:               "file.read",
		storage.OpFileWrite:              "file.write",
		storage.OpFileTruncate:           "file.truncate",
		storage.OpFileSetAttr:            "file.set-attr",
		storage.OpFileSetKind:            "file.set-kind",
		storage.OpFileLookupAt:           "file.lookup-at",
		storage.OpFileListAt:             "file.list-at",
		storage.OpFileRename:             "file.rename",
		storage.OpFileReplaceClaim:       "file.replace-claim",
		storage.OpFilePrepareRemoval:     "file.prepare-removal",
		storage.OpFileCancelPrepared:     "file.cancel-prepared",
		storage.OpFileDrainEntry:         "file.drain-entry",
		storage.OpFileCancelDrain:        "file.cancel-drain",
		storage.OpFileRangeSnapshot:      "file.range-snapshot",
		storage.OpFileReplaceRanges:      "file.replace-ranges",
		storage.OpFileWaitRanges:         "file.wait-ranges",
		storage.OpFileRetireRanges:       "file.retire-ranges",
		storage.OpFileSync:               "file.sync",
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
