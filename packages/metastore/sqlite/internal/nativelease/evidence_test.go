package nativelease

import (
	"errors"
	"math"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestEvidenceIdentitiesRequireCanonicalFixedWidthHex(t *testing.T) {
	for _, id := range []string{strings.Repeat("0", 32), strings.Repeat("f", 32), "0123456789abcdef0123456789abcdef"} {
		if !ValidID(id) {
			t.Fatalf("canonical identity %q was rejected", id)
		}
	}
	for _, id := range []string{"", strings.Repeat("a", 31), strings.Repeat("a", 33),
		strings.Repeat("A", 32), strings.Repeat("g", 32), strings.Repeat("é", 16),
		strings.Repeat("a", 31) + " ", strings.Repeat("a", 31) + "\x00"} {
		if ValidID(id) {
			t.Fatalf("noncanonical identity %q was accepted", id)
		}
	}
}

func TestEvidenceValidationPreservesZeroAndMaximumCounters(t *testing.T) {
	for _, evidence := range []Evidence{
		{DatabaseID: strings.Repeat("a", 32), StateID: strings.Repeat("b", 32)},
		{DatabaseID: strings.Repeat("a", 32), StateID: strings.Repeat("b", 32), Generation: math.MaxInt64, MaxLease: time.Duration(math.MaxInt64)},
	} {
		before := evidence
		if err := ValidateEvidence(evidence); err != nil {
			t.Fatalf("valid evidence %+v: %v", evidence, err)
		}
		if evidence != before {
			t.Fatal("validation changed the persisted evidence")
		}
	}
	valid := Evidence{DatabaseID: strings.Repeat("a", 32), StateID: strings.Repeat("b", 32), Generation: 1, MaxLease: time.Second}
	for _, mutate := range []struct {
		name  string
		apply func(*Evidence)
	}{
		{"missing database", func(e *Evidence) { e.DatabaseID = "" }},
		{"noncanonical database", func(e *Evidence) { e.DatabaseID = strings.Repeat("A", 32) }},
		{"missing state", func(e *Evidence) { e.StateID = "" }},
		{"long state", func(e *Evidence) { e.StateID = strings.Repeat("b", 33) }},
		{"negative generation", func(e *Evidence) { e.Generation = -1 }},
		{"negative lease", func(e *Evidence) { e.MaxLease = -time.Nanosecond }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			evidence := valid
			mutate.apply(&evidence)
			if err := ValidateEvidence(evidence); !errors.Is(err, syscall.EIO) {
				t.Fatalf("corrupt evidence %+v returned %v", evidence, err)
			}
		})
	}
}
