package schema

import (
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestHistoryRejectsDiscontinuousOrMalformedChanges(t *testing.T) {
	for _, test := range []struct{ name, mutation string }{
		{"tail ahead", `UPDATE logs SET committed_position=50`},
		{"trim beyond tail", `UPDATE logs SET trimmed_through=50`},
		{"missing predecessor", `UPDATE changes SET previous_position=1 WHERE position=1`},
		{"invalid kind", `UPDATE changes SET kind=99`},
		{"invalid name", `UPDATE changes SET name=X'2f'`},
		{"removed node payload", `UPDATE changes SET node=1`},
		{"rename without source", `UPDATE changes SET kind=3`},
		{"change outside namespace", `UPDATE changes SET namespace=999`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testNamespace(t, db, "workspace")
			testChange(t, db, id, root, "old")
			testChange(t, db, id, root, "another")
			for _, scope := range []*int64{nil, &id} {
				if err := validateLogIntegrity(t.Context(), db, scope); err != nil {
					t.Fatal(err)
				}
			}
			execute(t, db, test.mutation)
			err := validateLogIntegrity(t.Context(), db, nil)
			if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "invalid change rows") {
				t.Fatalf("corrupt history accepted: %v", err)
			}
		})
	}
}

func TestHistoricalLogValidatesShapeWithoutPredecessorColumn(t *testing.T) {
	db := testDatabase(t, 2)
	execute(t, db, `INSERT INTO namespaces VALUES(1,'legacy',1,0)`)
	execute(t, db, `INSERT INTO logs VALUES(1,'old-history',4,0,0)`)
	execute(t, db, `INSERT INTO changes (position,namespace,kind,parent,name,recorded_sec,recorded_nsec)
		VALUES(4,1,1,1,X'66',0,0)`)
	id := int64(1)
	for _, scope := range []*int64{nil, &id} {
		if err := validateVersionTwoLogIntegrity(t.Context(), db, scope); err != nil {
			t.Fatalf("valid sparse historical log refused: %v", err)
		}
	}
	execute(t, db, `UPDATE changes SET recorded_nsec='bad'`)
	if err := validateVersionTwoLogIntegrity(t.Context(), db, &id); !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "invalid SQLite storage classes") {
		t.Fatalf("historical scalar was coerced: %v", err)
	}
}
