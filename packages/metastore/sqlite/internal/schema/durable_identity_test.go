package schema

import (
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestDeleteIntentNamesConsumeIntegrityByteBudgetBeforeInspection(t *testing.T) {
	db := testDatabase(t, schema.Version())
	execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'v',1,0)`)
	execute(t, db, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,link_target)
		VALUES(1,1,2,0,0,0,0,0,X'')`)
	execute(t, db, `INSERT INTO delete_intents
		(intent,volume,node,parent,name,reference,request_hash,if_empty,outcome,failure,updated_sec,updated_nsec)
		VALUES('0123456789abcdef0123456789abcdef',1,1,1,X'6e616d65',zeroblob(16),zeroblob(32),1,?,NULL,0,0)`,
		storage.DeleteIntentArmed)
	volume := int64(1)
	if err := validateIntegrityBytes(t.Context(), db, &volume, 4, schema.Version()); err != nil {
		t.Fatalf("exact deletion-intent name budget = %v", err)
	}
	if err := validateIntegrityBytes(t.Context(), db, &volume, 3, schema.Version()); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("undersized deletion-intent name budget = %v", err)
	}
}

func TestDeleteIntentIntegrityRejectsUnknownCleanupErrno(t *testing.T) {
	db := testDatabase(t, schema.Version())
	execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'v',1,0)`)
	execute(t, db, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,link_target,pending_generation)
		VALUES(1,1,2,0,0,0,0,0,X'',1)`)
	execute(t, db, `INSERT INTO delete_intents
		(intent,volume,node,parent,name,reference,request_hash,if_empty,outcome,failure,updated_sec,updated_nsec)
		VALUES('0123456789abcdef0123456789abcdef',1,1,1,X'6e616d65',zeroblob(16),zeroblob(32),1,?,999,0,0)`,
		storage.DeleteIntentCleanupFailed)
	volume := int64(1)
	if err := validateDurableIdentity(t.Context(), db, &volume); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown cleanup errno = %v", err)
	}
}
