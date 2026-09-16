package sqlite

import (
	"database/sql"
	"errors"
	"syscall"
	"testing"
)

func TestLocationWitnessRejectsAncestorChanges(t *testing.T) {
	for _, mutation := range []string{"sibling", "rename"} {
		t.Run(mutation, func(t *testing.T) {
			s := openLocationStore(t)
			ctx := t.Context()
			if err := s.Mkdir(ctx, "parent"); err != nil {
				t.Fatal(err)
			}
			if err := s.Mkdir(ctx, "parent/inner"); err != nil {
				t.Fatal(err)
			}
			if err := s.Create(ctx, "parent/inner/file"); err != nil {
				t.Fatal(err)
			}
			witness := observedLocation(t, s, "parent/inner/file")
			if mutation == "sibling" {
				if err := s.Create(ctx, "PARENT"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := s.Rename(ctx, "parent", "moved"); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.inspect(ctx, func(tx *sql.Tx) error {
				final := witness.Ancestors[len(witness.Ancestors)-1]
				if err := s.checkEntryCondition(ctx, tx, final); err != nil {
					t.Fatalf("final parent unexpectedly changed: %v", err)
				}
				return s.validateLocation(ctx, tx, witness)
			}); !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("old ancestor witness=%v", err)
			}
		})
	}
}

func TestEntryConditionRejectsChangedEntryIdentity(t *testing.T) {
	s := openLocationStore(t)
	ctx := t.Context()
	if err := s.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	at := observedLocation(t, s, "file").Ancestors[0]
	at.EntryID++
	if err := s.inspect(ctx, func(tx *sql.Tx) error { return s.checkEntryCondition(ctx, tx, at) }); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("wrong entry identity=%v", err)
	}
	at = observedLocation(t, s, "file").Ancestors[0]
	at.NodeID++
	if err := s.inspect(ctx, func(tx *sql.Tx) error { return s.checkEntryCondition(ctx, tx, at) }); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("wrong node identity=%v", err)
	}
}
