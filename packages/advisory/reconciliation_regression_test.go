package advisory

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestRangeRefundReleasesBackingCapacityAcrossOwners(t *testing.T) {
	config := DefaultConfig()
	config.MaxRanges = 136
	c := fixture(t, config)
	s := session(t, c)
	for n := range 8 {
		o := owner(t, s, 1, 0)
		for batch := range 2 {
			commands := make([]storage.RangeCommand, 64)
			for i := range commands {
				commands[i] = record(storage.RangeShared, uint64(2*(64*batch+i)), 1)
			}
			wantState(t, apply(t, s, 1, o, commands...), storage.Granted, "")
		}
		wantState(t, apply(t, s, 1, o, subtract(record(storage.RangeShared, 0, 254))), storage.Released, "")
		retainedCapacity := 0
		for _, state := range c.owners {
			retainedCapacity += cap(state.ranges)
		}
		if c.ranges != n+1 || retainedCapacity != c.ranges {
			t.Fatalf("refunded fragments retained allocation: ranges=%d capacity=%d", c.ranges, retainedCapacity)
		}
	}
	for o := range s.bindings {
		wantState(t, apply(t, s, 1, o, subtract(record(storage.RangeShared, 254, 1))), storage.Released, "")
	}
	if c.ranges != 0 || len(c.owners) != 0 {
		t.Fatal("empty range state retained ownership or capacity")
	}
}

func TestApplyRechecksSessionAfterNativeOrdering(t *testing.T) {
	for _, retiring := range []bool{false, true} {
		t.Run(map[bool]string{false: "retired", true: "retiring"}[retiring], func(t *testing.T) {
			c := fixture(t, DefaultConfig())
			holder := session(t, c)
			h := owner(t, holder, 1, 0)
			wantState(t, apply(t, holder, 1, h, whole(storage.RangeExclusive)), storage.Granted, "")
			entered, release := make(chan struct{}), make(chan struct{})
			fence := func() error { return nil }
			if retiring {
				fence = func() error { close(entered); <-release; return nil }
			}
			s, err := c.NewSession(storage.DefaultFileSessionOptions(), fence)
			if err != nil {
				t.Fatal(err)
			}
			o := owner(t, s, 1, 0)
			ended := make(chan error, 1)
			order := func(ctx context.Context, transition func() error) error {
				if err := transition(); err != nil {
					return err
				}
				if retiring {
					go func() { ended <- s.Retire(background) }()
					<-entered
					return nil
				}
				return s.Retire(background)
			}
			command := whole(storage.RangeExclusive)
			command.Wait = true
			result, err := s.Apply(background, 1, o, []storage.RangeCommand{command}, requestID(t, s), order)
			want := error(syscall.ESTALE)
			if retiring {
				want = syscall.EIO
				close(release)
				if cleanup := <-ended; cleanup != nil {
					t.Fatal(cleanup)
				}
			}
			if !errors.Is(err, want) || result.State != 0 {
				t.Fatalf("retired admission returned unretained receipt: %+v %v", result, err)
			}
		})
	}
}

func TestExactQueryReflectsPartialAndFinalRelease(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 0)
	command := enforced(storage.RangeShared, bytesRange(0, 10))
	granted := apply(t, s, 1, o, command, command)
	wantState(t, apply(t, s, 1, o, removal(command, granted.Claims[0])), storage.Released, "")
	partial, err := s.Query(background, 1, o, granted.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, partial, storage.Granted, "")
	wantState(t, apply(t, s, 1, o, removal(command, granted.Claims[1])), storage.Released, "")
	released, err := s.Query(background, 1, o, granted.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, released, storage.Released, "")
	if !released.EverGranted || !reflect.DeepEqual(released.Commands, granted.Commands) ||
		!reflect.DeepEqual(released.Claims, granted.Claims) || !reflect.DeepEqual(released.Effects, granted.Effects) {
		t.Fatalf("later release rewrote original acquisition evidence: %+v", released)
	}
	replayed, err := s.Apply(background, 1, o, []storage.RangeCommand{command, command}, granted.Request, ordered)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, replayed, storage.Released, "")
	if c.ranges != 0 {
		t.Fatal("released request replay reacquired exact claims")
	}
}

func TestRequestNodeUsesRetainedHistoryAfterOwnerRetirement(t *testing.T) {
	c := fixture(t, DefaultConfig())
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	s := session(t, c)
	o := owner(t, s, 7, 0)
	first := apply(t, s, 7, o, whole(storage.RangeShared))
	if err := s.RetireOwner(background, o); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OwnerNode(background, o); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired owner registration remained available: %v", err)
	}
	node, err := s.RequestNode(background, o, first.Request)
	if err != nil || node != 7 {
		t.Fatalf("retained action target = %d %v", node, err)
	}
	got, err := s.Query(background, node, o, first.Request)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, got, storage.Released, "")
	if _, err := s.RequestNode(background, o+1, first.Request); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("another owner accessed action target: %v", err)
	}
	ctx, cancel := context.WithCancel(background)
	cancel()
	if _, err := s.RequestNode(ctx, o, first.Request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled target query = %v", err)
	}
	now = now.Add(2 * s.options.History)
	if _, err := s.RequestNode(background, o, first.Request); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired history invented a target: %v", err)
	}
}

func TestConversionRejectsOversizedReceiptBeforeReleasingRanges(t *testing.T) {
	c := fixture(t, DefaultConfig())
	s := session(t, c)
	o := owner(t, s, 1, 0)
	commands := make([]storage.RangeCommand, storage.MaxRangeCommands)
	for i := range commands {
		commands[i] = record(storage.RangeShared, uint64(2*i), 1)
		commands[i].Domain = storage.DomainWholeFile
	}
	wantState(t, apply(t, s, 1, o, commands...), storage.Granted, "")
	id := requestID(t, s)
	conversion := whole(storage.RangeExclusive)
	result, err := s.Apply(background, 1, o, []storage.RangeCommand{conversion}, id, ordered)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, result, storage.Rejected, storage.RangeTooLarge)
	if c.ranges != storage.MaxRangeCommands || len(result.Effects) != 0 || result.EverGranted {
		t.Fatalf("oversized conversion released protection: %+v ranges=%d", result, c.ranges)
	}
	queried, err := s.Query(background, 1, o, id)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, queried, storage.Rejected, storage.RangeTooLarge)
	if err := s.Drop(background, 1, o, storage.DomainWholeFile); err != nil {
		t.Fatal(err)
	}
	replayed, err := s.Apply(background, 1, o, []storage.RangeCommand{conversion}, id, ordered)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, replayed, storage.Rejected, storage.RangeTooLarge)
	if c.ranges != 0 {
		t.Fatal("rejected conversion acquired on replay")
	}
	wantState(t, apply(t, s, 1, o, commands[:storage.MaxRangeEffects-1]...), storage.Granted, "")
	fit := apply(t, s, 1, o, conversion)
	wantState(t, fit, storage.Granted, "")
	if len(fit.Effects) != storage.MaxRangeEffects {
		t.Fatalf("maximum funded receipt has %d effects", len(fit.Effects))
	}
}
