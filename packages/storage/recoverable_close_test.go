package storage

import (
	"errors"
	"syscall"
	"testing"
)

func TestCloseAttemptAndOwnerStatusRejectInvalidRecoveryDirections(t *testing.T) {
	id, err := NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	attempt := CloseAttempt{Action: id, Generation: 1}
	if err := attempt.Check(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []CloseAttempt{{Action: id}, {Generation: 1}, {Action: "2:bad", Generation: 1}} {
		if !errors.Is(invalid.Check(), syscall.EINVAL) {
			t.Fatalf("invalid close attempt accepted: %+v", invalid)
		}
	}
	for _, valid := range []CloseOwnerStatus{
		{Ready: true, NextGeneration: 1, CurrentEpoch: 1},
		{Current: &attempt, CurrentOutcome: FileActionUnknown, NextGeneration: 1, CurrentEpoch: 2},
		{Released: true, NextGeneration: 1, CurrentEpoch: 2},
	} {
		if err := valid.Check(); err != nil {
			t.Fatalf("valid close owner state %+v: %v", valid, err)
		}
	}
	for _, invalid := range []CloseOwnerStatus{
		{Ready: true, CurrentEpoch: 1},
		{Ready: true, NextGeneration: 1},
		{Released: true, Ready: true, NextGeneration: 1, CurrentEpoch: 1},
		{CurrentOutcome: FileActionUnknown, NextGeneration: 1, CurrentEpoch: 1},
		{Current: &attempt, Ready: true, CurrentOutcome: FileActionUnknown, NextGeneration: 1, CurrentEpoch: 1},
		{Current: &attempt, CurrentOutcome: FileActionCompleted, NextGeneration: 1, CurrentEpoch: 1},
		{Current: &attempt, CurrentOutcome: FileActionUnknown, NextGeneration: 2, CurrentEpoch: 1},
	} {
		if !errors.Is(invalid.Check(), syscall.EINVAL) {
			t.Fatalf("invalid close owner state accepted: %+v", invalid)
		}
	}
}

func TestCloseEpochMissErrorCarriesRetryProofAndCurrentEpoch(t *testing.T) {
	missed := &CloseActionNotExecutedError{CurrentEpoch: 7}
	if missed.Error() == "" || !errors.Is(missed, syscall.ESTALE) {
		t.Fatalf("epoch miss lost diagnostic or stale classification: %v", missed)
	}
	var extracted *CloseActionNotExecutedError
	if !errors.As(missed, &extracted) || extracted.CurrentEpoch != 7 {
		t.Fatalf("epoch miss lost current epoch: %+v", extracted)
	}
}
