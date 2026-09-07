package localdir

import (
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func pauseStateRaise(s *directoryState, phase string, after error) (<-chan struct{}, func()) {
	entered := make(chan struct{})
	resumed := make(chan struct{})
	var once sync.Once
	pause := func() { once.Do(func() { close(entered); <-resumed }) }
	original := s.ops
	if phase == "fsync" {
		s.ops.fsync = func(fd int) error {
			pause()
			if after != nil {
				return after
			}
			return original.fsync(fd)
		}
	} else {
		s.ops.setxattr = func(fd int, name string, data []byte, flags int) error {
			if name == witnessAttribute {
				pause()
				if after != nil {
					return after
				}
			}
			return original.setxattr(fd, name, data, flags)
		}
	}
	var resumeOnce sync.Once
	return entered, func() { resumeOnce.Do(func() { close(resumed) }) }
}

func stateResultBeforeTimeout(t *testing.T, done <-chan error, operation string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatalf("%s waited for unrelated durable lease preparation", operation)
		return nil
	}
}

func TestDirectoryStateHealthProgressesDuringDurableRaise(t *testing.T) {
	for _, phase := range []string{"fsync", "witness"} {
		for _, fail := range []bool{false, true} {
			name := phase
			if fail {
				name += " failure"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				cfg := stateTestConfig(t)
				if err := Init(ctx, cfg); err != nil {
					t.Fatal(err)
				}
				state := requireStateOpen(t, cfg)
				var injected error
				if fail {
					injected = syscall.EIO
				}
				entered, resume := pauseStateRaise(state, phase, injected)
				defer resume()
				raised := make(chan error, 1)
				go func() { raised <- state.RaiseMaxLease(ctx, time.Second) }()
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("raise did not reach its durable pause")
				}
				checked := make(chan error, 1)
				go func() { checked <- state.health() }()
				if err := stateResultBeforeTimeout(t, checked, "health"); err != nil {
					t.Fatal(err)
				}
				observed := make(chan error, 1)
				go func() {
					value, err := state.MaxLease(ctx)
					if err == nil && value != 0 {
						err = stateFailure("unacknowledged prepared duration became the accepted watermark")
					}
					observed <- err
				}()
				if err := stateResultBeforeTimeout(t, observed, "MaxLease"); err != nil {
					t.Fatal(err)
				}
				resume()
				err := stateResultBeforeTimeout(t, raised, "raise after resume")
				if fail {
					if !errors.Is(err, syscall.EIO) {
						t.Fatalf("raise failure lost its cause: %v", err)
					}
					if err := state.health(); !errors.Is(err, syscall.EIO) {
						t.Fatalf("failed raise did not fence health: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestDirectoryStateCloseDrainsPausedRaiseBeforeConsumingHandles(t *testing.T) {
	ctx := context.Background()
	cfg := stateTestConfig(t)
	if err := Init(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	state := requireStateOpen(t, cfg)
	entered, resume := pauseStateRaise(state, "witness", nil)
	defer resume()
	closedHandle := make(chan struct{}, 1)
	originalClose := state.ops.close
	stagingFD := state.stagingFD
	state.ops.close = func(fd int) error {
		if fd == stagingFD {
			closedHandle <- struct{}{}
		}
		return originalClose(fd)
	}
	raised := make(chan error, 1)
	go func() { raised <- state.RaiseMaxLease(ctx, time.Second) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("raise did not pause")
	}
	closed := make(chan error, 1)
	go func() { closed <- state.Close() }()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !errors.Is(state.stateError(), os.ErrClosed) {
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("Close did not reject new work while draining")
		}
	}
	select {
	case <-closedHandle:
		t.Fatal("Close consumed a handle needed by the paused raise")
	default:
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before its raise drained: %v", err)
	default:
	}
	if err := state.health(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closing state remained usable: %v", err)
	}
	resume()
	if err := stateResultBeforeTimeout(t, raised, "closing raise"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closing raise result: %v", err)
	}
	if err := stateResultBeforeTimeout(t, closed, "Close after raise drain"); err != nil {
		t.Fatal(err)
	}
	reopened := requireStateOpen(t, cfg)
	if duration, err := reopened.MaxLease(ctx); err != nil || duration != time.Second {
		t.Fatalf("drained raise lost its durable horizon: %v, %v", duration, err)
	}
}

func TestDirectoryStateFailedCloseRetainsOwnerAfterRaiseDrain(t *testing.T) {
	ctx := context.Background()
	cfg := stateTestConfig(t)
	if err := Init(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	state, err := openBoundState(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	owner := state.ownerFD
	defer unix.Close(owner)
	entered, resume := pauseStateRaise(state, "fsync", nil)
	defer resume()
	failedFD := state.stagingFD
	state.ops.close = func(fd int) error {
		err := unix.Close(fd)
		if fd == failedFD {
			return errors.Join(err, syscall.EIO)
		}
		return err
	}
	raised := make(chan error, 1)
	go func() { raised <- state.RaiseMaxLease(ctx, time.Second) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("raise did not pause")
	}
	closed := make(chan error, 1)
	go func() { closed <- state.Close() }()
	resume()
	if err := stateResultBeforeTimeout(t, raised, "raise drain"); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	if err := stateResultBeforeTimeout(t, closed, "uncertain Close"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("close uncertainty: %v", err)
	}
	if _, err := openBoundState(ctx, cfg); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("uncertain close released ownership: %v", err)
	}
}
