package httprest

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func rangeResponseBound(commands []storage.RangeCommand) int64 {
	request := storage.LockRequestID(strconv.FormatUint(math.MaxUint64, 10) + ":" + strings.Repeat("f", 32))
	claim, _ := storage.NewClaimID(request, storage.MaxRangeCommands-1)
	command := storage.RangeCommand{Domain: 255, Mode: 255, Range: storage.Range{Kind: 255, Start: math.MaxUint64, Length: math.MaxUint64, CutAt: math.MaxUint64}, Edit: 255, Claim: claim, Conversion: 255, Policy: storage.RangePolicy{DenySelf: 255, DenyOthers: 255}}
	effects := len(commands)
	if len(commands) > 0 && commands[0].Conversion == storage.DropBeforeAcquire {
		effects = storage.MaxRangeEffects
	}
	attempt := storage.RangeAttempt{Request: request, State: storage.Granted, Commands: commands, Conflict: storage.RangeConflict{Owner: storage.OwnerDiagnostic(math.MaxUint64), Range: command.Range, Mode: 255}, Rejection: "unsupported", HistoryRemaining: time.Duration(math.MaxInt64)}
	for _, c := range commands {
		if c.Edit == storage.AddExact {
			attempt.Claims = append(attempt.Claims, claim)
		}
	}
	for range effects {
		attempt.Effects = append(attempt.Effects, storage.RangeEffect{Claim: claim, Command: command})
	}
	encoded, err := json.Marshal(fileResponse{Epoch: math.MaxUint64, Data: []byte{}, Attempt: &attempt})
	if err != nil {
		panic(err)
	}
	return int64(len(encoded))
}

func validateRangeEffects(a storage.RangeAttempt) error {
	complete := a.State == storage.Granted || a.State == storage.Released
	prefix := len(a.Effects)
	var claims []storage.ClaimID
	if complete {
		if len(a.Effects) < len(a.Commands) {
			return errors.New("range receipt omits applied effects")
		}
		prefix -= len(a.Commands)
		for index, command := range a.Commands {
			effect := a.Effects[prefix+index]
			expected := storage.RangeEffect{Command: command, Released: command.Edit == storage.Subtract || command.Edit == storage.RemoveExact}
			switch command.Edit {
			case storage.AddExact:
				id, err := storage.NewClaimID(a.Request, index)
				if err != nil {
					return err
				}
				expected.Claim = id
				claims = append(claims, id)
			case storage.RemoveExact:
				expected.Claim = command.Claim
			}
			if effect != expected {
				return errors.New("range receipt changes an applied effect")
			}
		}
	}
	if len(a.Claims) != len(claims) || len(claims) > 0 && !reflect.DeepEqual(a.Claims, claims) {
		return errors.New("range receipt changes exact claim identities")
	}
	if prefix > 0 {
		if a.Commands[0].Conversion != storage.DropBeforeAcquire {
			return errors.New("range receipt invents prior release effects")
		}
		for _, effect := range a.Effects[:prefix] {
			if !effect.Released || effect.Claim != "" || effect.Command.Domain != storage.DomainWholeFile || effect.Command.Edit != storage.Replace || effect.Command.Range.Kind != storage.Bytes || effect.Command.Wait || effect.Command.Conversion != storage.PreserveBeforeAcquire || effect.Command.Policy != (storage.RangePolicy{}) {
				return errors.New("range receipt changes prior release effects")
			}
		}
	}
	return nil
}
