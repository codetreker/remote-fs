package schema

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

func TestIntegrityAcceptsConsistentGlobalAndVolumeGraphs(t *testing.T) {
	db := testDatabase(t, 0)
	id, root := testVolume(t, db, "workspace")
	testFile(t, db, id, root, "file", 7, false)
	testFile(t, db, id, root, "unnamed", 2, true)
	testChange(t, db, id, root, "old")
	other, otherRoot := testVolume(t, db, "other")
	testFile(t, db, other, otherRoot, "other-file", 11, false)
	for _, scope := range []*int64{nil, &id, &other} {
		if err := validateIntegrity(t.Context(), db, scope, 1000, 1<<20, 5); err != nil {
			t.Fatal(err)
		}
	}
	execute(t, db, `UPDATE nodes SET size = -1 WHERE volume = ? AND id != ?`, other, otherRoot)
	if err := ValidateVolumeIntegrity(t.Context(), db, id, 1000, 1<<20); err != nil {
		t.Fatalf("unrelated volume corruption crossed the scoped check: %v", err)
	}
	if err := validateIntegrity(t.Context(), db, nil, 1000, 1<<20, 5); !errors.Is(err, syscall.EIO) {
		t.Fatalf("global check accepted another volume's corrupt node: %v", err)
	}
}

func TestIntegrityWorkAndNameBytesHaveExactBounds(t *testing.T) {
	db := testDatabase(t, 0)
	id, root := testVolume(t, db, "workspace")
	testFile(t, db, id, root, "name", 3, false)
	testChange(t, db, id, root, "old")
	for _, scope := range []*int64{nil, &id} {
		if err := validateIntegrityWork(t.Context(), db, scope, 7); err != nil {
			t.Fatalf("exact seven-row graph refused: %v", err)
		}
		if err := validateIntegrityWork(t.Context(), db, scope, 6); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("work limit accepted seven rows: %v", err)
		}
		if err := validateIntegrityBytes(t.Context(), db, scope, 7, 5); err != nil {
			t.Fatalf("exact seven-byte names refused: %v", err)
		}
		if err := validateIntegrityBytes(t.Context(), db, scope, 6, 5); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("change name escaped byte limit: %v", err)
		}
		if err := validateIntegrityBytes(t.Context(), db, scope, 3, 5); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("entry name escaped byte limit: %v", err)
		}
	}
	execute(t, db, `UPDATE changes SET from_name=X'6d6f766564'`)
	if err := validateIntegrityBytes(t.Context(), db, &id, 11, 5); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("from_name escaped byte accounting: %v", err)
	}
	execute(t, db, `UPDATE changes SET from_name='text'`)
	if err := validateIntegrityBytes(t.Context(), db, &id, 100, 5); !errors.Is(err, syscall.EIO) {
		t.Fatalf("text from_name coerced to bytes: %v", err)
	}
	execute(t, db, `UPDATE entries SET name='text'`)
	if err := validateIntegrityBytes(t.Context(), db, &id, 100, 5); !errors.Is(err, syscall.EIO) {
		t.Fatalf("text entry name coerced to bytes: %v", err)
	}
}

func TestIntegrityRejectsInvalidStoredClassesAndMetadata(t *testing.T) {
	for _, test := range []struct {
		name, mutation, diagnostic string
		check                      func(context.Context, sqlvalue.Queryer, *int64) error
	}{
		{"volume type", `UPDATE volumes SET used='bad'`, "volume rows", validateStorageClasses},
		{"node type", `UPDATE nodes SET size='bad'`, "nodes rows", validateStorageClasses},
		{"object type", `UPDATE objects SET size='bad'`, "objects rows", validateStorageClasses},
		{"entry name", `UPDATE entries SET name=X'2e2e'`, "entries rows", validateStorageClasses},
		{"log type", `UPDATE logs SET trimmed_by_age='bad'`, "logs rows", validateStorageClasses},
		{"change type", `UPDATE changes SET kind='bad'`, "changes rows", validateStorageClasses},
		{"durable type", `PRAGMA ignore_check_constraints=ON; UPDATE database_state SET generation='bad'`, "durable state rows", validateStorageClasses},
		{"negative file size", `UPDATE nodes SET size=-1 WHERE id=2`, "metadata values", validateNodeValues},
		{"invalid nanoseconds", `UPDATE nodes SET atime_nsec=1000000000 WHERE id=2`, "metadata values", validateNodeValues},
		{"invalid detached flag", `UPDATE nodes SET detached=2 WHERE id=2`, "metadata values", validateNodeValues},
		{"invalid revision", `UPDATE nodes SET content_revision=0 WHERE id=2`, "metadata values", validateNodeValues},
		{"unknown object state", `UPDATE objects SET state=99`, "unknown state", func(ctx context.Context, q sqlvalue.Queryer, ns *int64) error {
			return validateIntegrity(ctx, q, ns, 1000, 1<<20, 5)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testVolume(t, db, "workspace")
			testFile(t, db, id, root, "file", 3, false)
			testChange(t, db, id, root, "old")
			execute(t, db, test.mutation)
			for _, scope := range []*int64{nil, &id} {
				err := test.check(t.Context(), db, scope)
				if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), test.diagnostic) {
					t.Fatalf("got %v, want EIO describing %q", err, test.diagnostic)
				}
			}
		})
	}
}

func TestIntegrityCancellationPreservesTheCause(t *testing.T) {
	db := testDatabase(t, 0)
	id, _ := testVolume(t, db, "workspace")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, check := range []struct {
		name string
		run  func() error
	}{
		{"records", func() error { return validateIntegrityWork(ctx, db, &id, 1000) }},
		{"names", func() error { return validateIntegrityBytes(ctx, db, &id, 1<<20, 5) }},
		{"classes", func() error { return validateStorageClasses(ctx, db, &id) }},
		{"node values", func() error { return validateNodeValues(ctx, db, &id) }},
		{"node relationships", func() error { return validateNodeRelationships(ctx, db, &id) }},
		{"object relationships", func() error { return validateObjectRelationships(ctx, db, &id) }},
		{"history", func() error { return validateLogIntegrity(ctx, db, &id) }},
		{"accounting", func() error { return validateUsedAccounting(ctx, db, &id) }},
	} {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation changed to a metadata result: %v", err)
			}
		})
	}
	if err := ValidateVolumeIntegrity(t.Context(), db, id, 1000, 1<<20); err != nil {
		t.Fatalf("cancelled inspections damaged the volume: %v", err)
	}
}
