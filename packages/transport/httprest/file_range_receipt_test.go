package httprest

import (
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func capabilityTestAttempt(id storage.LockRequestID, commands []storage.RangeCommand) storage.RangeAttempt {
	result := storage.RangeAttempt{Request: id, State: storage.Granted, Commands: commands, EverGranted: true, HistoryRemaining: time.Minute}
	for index, command := range commands {
		effect := storage.RangeEffect{Command: command}
		if command.Edit == storage.AddExact {
			effect.Claim, _ = storage.NewClaimID(id, index)
			result.Claims = append(result.Claims, effect.Claim)
		}
		result.Effects = append(result.Effects, effect)
	}
	return result
}

func TestExactRangeReceiptRequiresEveryClaimIdentity(t *testing.T) {
	id, _ := storage.NewLockRequestID(1)
	command := storage.RangeCommand{Domain: storage.DomainEnforced, Mode: storage.RangeShared, Range: storage.Range{Kind: storage.Bytes, Length: 1}, Edit: storage.AddExact, Policy: storage.RangePolicy{DenySelf: storage.WriteData, DenyOthers: storage.WriteData}}
	original := capabilityTestAttempt(id, []storage.RangeCommand{command, command})
	request := fileRequest{Op: storage.OpFileRangeApply, Commands: original.Commands, LockID: id}
	if err := validateFileAttempt(request, original); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*storage.RangeAttempt)
	}{
		{"missing claims", func(a *storage.RangeAttempt) { a.Claims = nil }},
		{"missing effects", func(a *storage.RangeAttempt) { a.Effects = nil }},
		{"duplicate claims", func(a *storage.RangeAttempt) { a.Claims[1] = a.Claims[0] }},
		{"foreign claim", func(a *storage.RangeAttempt) {
			other, _ := storage.NewLockRequestID(1)
			a.Claims[0], _ = storage.NewClaimID(other, 0)
		}},
		{"effect identity", func(a *storage.RangeAttempt) { a.Effects[0].Claim = a.Claims[1] }},
		{"effect command", func(a *storage.RangeAttempt) { a.Effects[0].Command.Range.Start = 1 }},
		{"rejected with claims", func(a *storage.RangeAttempt) {
			a.State = storage.Rejected
			a.EverGranted = false
			a.Rejection = storage.RangeBlocked
		}},
		{"cancelled with claims", func(a *storage.RangeAttempt) { a.State = storage.Cancelled; a.EverGranted = false }},
		{"failed position on grant", func(a *storage.RangeAttempt) { index := 0; a.FailedAt = &index }},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := original.Clone()
			test.change(&value)
			if err := validateFileAttempt(request, value); err == nil {
				t.Fatal("accepted inconsistent exact receipt")
			}
		})
	}
}

func TestRejectedRangeReceiptPreservesReleasePrefixAndFailurePosition(t *testing.T) {
	id, _ := storage.NewLockRequestID(1)
	release := storage.RangeCommand{Domain: storage.DomainRecord, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Length: 1}, Edit: storage.Subtract}
	acquire := storage.RangeCommand{Domain: storage.DomainRecord, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Start: 1, Length: 1}, Edit: storage.Replace}
	failedAt := 1
	attempt := storage.RangeAttempt{Request: id, State: storage.Rejected, Commands: []storage.RangeCommand{release, acquire}, Rejection: storage.RangeBlocked, FailedAt: &failedAt, Effects: []storage.RangeEffect{{Command: release, Released: true}}}
	if err := validateFileAttempt(fileRequest{Op: storage.OpFileRangeApply, Commands: attempt.Commands, LockID: id}, attempt); err != nil {
		t.Fatal(err)
	}
	clone := attempt.Clone()
	if clone.FailedAt == attempt.FailedAt || *clone.FailedAt != failedAt {
		t.Fatal("failed command position was not independently retained")
	}
}

func TestRangeFailurePositionIsStrict(t *testing.T) {
	id, _ := storage.NewLockRequestID(1)
	command := storage.RangeCommand{Domain: storage.DomainRecord, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Length: 1}, Edit: storage.Replace}
	failedAt := 0
	valid := storage.RangeAttempt{Request: id, State: storage.Rejected, Commands: []storage.RangeCommand{command}, Rejection: storage.RangeBlocked, FailedAt: &failedAt}
	request := fileRequest{Op: storage.OpFileRangeApply, Commands: valid.Commands, LockID: id}
	for _, test := range []struct {
		name   string
		change func(*storage.RangeAttempt)
	}{
		{"missing", func(a *storage.RangeAttempt) { a.FailedAt = nil }},
		{"negative", func(a *storage.RangeAttempt) { value := -1; a.FailedAt = &value }},
		{"out of range", func(a *storage.RangeAttempt) { value := len(a.Commands); a.FailedAt = &value }},
		{"wrong state", func(a *storage.RangeAttempt) { a.State, a.Rejection = storage.Cancelled, "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			attempt := valid.Clone()
			test.change(&attempt)
			if err := validateFileAttempt(request, attempt); err == nil {
				t.Fatal("accepted invalid failing command position")
			}
		})
	}
	requestWide := valid.Clone()
	requestWide.Rejection, requestWide.FailedAt = storage.RangeTooLarge, nil
	if err := validateFileAttempt(request, requestWide); err != nil {
		t.Fatalf("request-wide rejection: %v", err)
	}
}

func TestBoundedRangeErrorPreservesReceipt(t *testing.T) {
	id, _ := storage.NewLockRequestID(1)
	commands := make([]storage.RangeCommand, storage.MaxRangeCommands)
	for index := range commands {
		commands[index] = storage.RangeCommand{Domain: storage.DomainEnforced, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Start: uint64(index), Length: 1}, Edit: storage.AddExact, Policy: storage.RangePolicy{DenyOthers: storage.ReadData | storage.WriteData}}
	}
	attempt := capabilityTestAttempt(id, commands)
	recorded := true
	limit := rangeResponseBound(commands)
	encoded, err := marshalBoundedErrorResponse(ErrorResponse{Errno: "EAGAIN", FileRecorded: &recorded, Attempt: &attempt, Message: strings.Repeat("detail", int(limit))}, limit)
	if err != nil || int64(len(encoded)) > limit {
		t.Fatalf("bounded range error bytes=%d limit=%d error=%v", len(encoded), limit, err)
	}
	var decoded ErrorResponse
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Attempt == nil || decoded.Attempt.Request != id || len(decoded.Attempt.Effects) != len(attempt.Effects) || decoded.Message != boundedErrorDetail {
		t.Fatalf("bounded range error lost receipt: %+v", decoded)
	}
}

func TestRangeResponseEnvelopeCoversMaximumReceipt(t *testing.T) {
	commands := make([]storage.RangeCommand, storage.MaxRangeCommands)
	for i := range commands {
		commands[i] = storage.RangeCommand{Domain: storage.DomainEnforced, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Start: ^uint64(0), Length: 1}, Edit: storage.AddExact, Policy: storage.RangePolicy{DenyOthers: storage.ReadData | storage.WriteData}}
	}
	if bound := rangeResponseBound(commands); bound <= DefaultMaxLockControlBytes || bound > MaxFileControlBytes {
		t.Fatalf("range bound %d outside declared response envelope", bound)
	}
	commands = []storage.RangeCommand{{Domain: storage.DomainWholeFile, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Length: ^uint64(0)}, Edit: storage.Replace, Conversion: storage.DropBeforeAcquire}}
	if bound := rangeResponseBound(commands); bound > MaxFileControlBytes {
		t.Fatalf("conversion bound %d outside declared response envelope", bound)
	}
}

func TestRangeReplyBudgetRefusesBeforeDispatch(t *testing.T) {
	commands := make([]storage.RangeCommand, storage.MaxRangeCommands)
	for index := range commands {
		commands[index] = storage.RangeCommand{Domain: storage.DomainEnforced, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Start: uint64(index), Length: 1}, Edit: storage.AddExact, Policy: storage.RangePolicy{DenyOthers: storage.ReadData | storage.WriteData}}
	}
	request, _ := storage.NewLockRequestID(1)
	session := &remoteFileSession{storage: &Storage{maxBodyBytes: DefaultMaxLockControlBytes}, capabilities: fileCapabilities{Ranges: true}}
	if _, err := session.Apply(t.Context(), 1, commands, request); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("maximum receipt was not refused before dispatch: %v", err)
	}
}

func TestConversionReceiptAllowsMixedPriorModes(t *testing.T) {
	id, _ := storage.NewLockRequestID(1)
	requested := storage.RangeCommand{Domain: storage.DomainWholeFile, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Length: 100}, Edit: storage.Replace, Conversion: storage.DropBeforeAcquire}
	oldShared := storage.RangeCommand{Domain: storage.DomainWholeFile, Mode: storage.RangeShared, Range: storage.Range{Kind: storage.Bytes, Length: 1}, Edit: storage.Replace}
	oldExclusive := oldShared
	oldExclusive.Mode = storage.RangeExclusive
	oldExclusive.Range.Start = 10
	attempt := storage.RangeAttempt{Request: id, State: storage.Granted, Commands: []storage.RangeCommand{requested}, EverGranted: true, Effects: []storage.RangeEffect{{Command: oldShared, Released: true}, {Command: oldExclusive, Released: true}, {Command: requested}}}
	if err := validateFileAttempt(fileRequest{Op: storage.OpFileRangeApply, Commands: attempt.Commands, LockID: id}, attempt); err != nil {
		t.Fatalf("valid mixed-mode conversion receipt: %v", err)
	}
}
