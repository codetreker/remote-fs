package schema

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestDeleteIntentNamesConsumeIntegrityByteBudgetBeforeInspection(t *testing.T) {
	db := testDatabase(t, schema.Version())
	execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'v',1,0)`)
	execute(t, db, `UPDATE volumes SET delete_intent_high_water=1 WHERE id=1`)
	execute(t, db, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,link_target)
		VALUES(1,1,2,0,0,0,0,0,X'')`)
	execute(t, db, `INSERT INTO delete_intents
		(intent,volume,owner,sequence,node,parent,name,reference,request_hash,if_empty,outcome,failure,updated_sec,updated_nsec)
		VALUES('0123456789abcdef0123456789abcdef',1,'owner',1,1,1,X'6e616d65',zeroblob(16),zeroblob(32),1,?,NULL,0,0)`,
		storage.DeleteIntentArmed)
	volume := int64(1)
	if err := validateIntegrityBytes(t.Context(), db, &volume, 9, schema.Version()); err != nil {
		t.Fatalf("exact deletion-intent name budget = %v", err)
	}
	if err := validateIntegrityBytes(t.Context(), db, &volume, 8, schema.Version()); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("undersized deletion-intent name budget = %v", err)
	}
}

func TestDeleteIntentOwnerAndSequenceIntegrity(t *testing.T) {
	for _, test := range []struct {
		name   string
		damage string
		check  func(context.Context, sqlvalue.Queryer, *int64) error
	}{
		{"invalid UTF-8 owner", `UPDATE delete_intents SET owner=CAST(X'ff' AS TEXT)`, func(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
			return validateDurableIdentity(ctx, db, volume, schema.Version())
		}},
		{"NUL owner", `UPDATE delete_intents SET owner=CAST(X'610062' AS TEXT)`, validateStorageClasses},
		{"sequence above high water", `UPDATE delete_intents SET sequence=2`, func(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
			return validateDurableIdentity(ctx, db, volume, schema.Version())
		}},
		{"negative high water", `UPDATE volumes SET delete_intent_high_water=-1`, validateStorageClasses},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, schema.Version())
			execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'v',1,0)`)
			execute(t, db, `UPDATE volumes SET delete_intent_high_water=1 WHERE id=1`)
			execute(t, db, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,link_target,pending_generation)
				VALUES(1,1,2,0,0,0,0,0,X'',1)`)
			execute(t, db, `INSERT INTO delete_intents
				(intent,volume,owner,sequence,node,parent,name,reference,request_hash,if_empty,outcome,failure,updated_sec,updated_nsec)
				VALUES('0123456789abcdef0123456789abcdef',1,'owner',1,1,1,X'6e616d65',zeroblob(16),zeroblob(32),1,?,NULL,0,0)`,
				storage.DeleteIntentArmed)
			execute(t, db, `PRAGMA ignore_check_constraints=ON`)
			execute(t, db, test.damage)
			volume := int64(1)
			if err := test.check(t.Context(), db, &volume); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid deletion-intent owner state = %v", err)
			}
		})
	}
}

func TestDeleteIntentIntegrityRejectsUnknownCleanupErrno(t *testing.T) {
	db := testDatabase(t, schema.Version())
	execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'v',1,0)`)
	execute(t, db, `UPDATE volumes SET delete_intent_high_water=1 WHERE id=1`)
	execute(t, db, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,link_target,pending_generation)
		VALUES(1,1,2,0,0,0,0,0,X'',1)`)
	execute(t, db, `INSERT INTO delete_intents
		(intent,volume,owner,sequence,node,parent,name,reference,request_hash,if_empty,outcome,failure,updated_sec,updated_nsec)
		VALUES('0123456789abcdef0123456789abcdef',1,'owner',1,1,1,X'6e616d65',zeroblob(16),zeroblob(32),1,?,999,0,0)`,
		storage.DeleteIntentCleanupFailed)
	volume := int64(1)
	if err := validateDurableIdentity(t.Context(), db, &volume, schema.Version()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown cleanup errno = %v", err)
	}
}
