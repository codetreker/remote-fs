package replicated

import (
	"fmt"
	"math"
	"syscall"
	"time"
)

const (
	// DefaultConfirmationGrace is how long a caller waits for the server's barrier to be
	// applied locally. Expiry fails that mutation; it does not invalidate a current replica.
	DefaultConfirmationGrace = 10 * time.Second

	// DefaultMaxActiveConfirmations bounds mutations whose server result or local barrier
	// confirmation is still in flight. Each owns one fixed-size record.
	DefaultMaxActiveConfirmations = 64

	// DefaultMaxWaitingConfirmations bounds callers waiting to reserve an active
	// confirmation record.
	DefaultMaxWaitingConfirmations = 64
)

// Options bounds the resources retained while mutations wait for their replication barrier.
//
// A mutation reserves one fixed-size record before its request is sent.
// MaxWaitingConfirmations bounds callers waiting for one. Saturation and cancellation
// while waiting return EAGAIN because no mutation has reached the server. Once the server
// reports success, the operation returns success only after the local replica reaches its
// barrier; cancellation and timeout are then EIO because the namespace has changed.
type Options struct {
	ConfirmationGrace       time.Duration
	MaxActiveConfirmations  int
	MaxWaitingConfirmations int
}

// DefaultOptions returns the bounded replication settings used by New.
func DefaultOptions() Options {
	return Options{
		ConfirmationGrace:       DefaultConfirmationGrace,
		MaxActiveConfirmations:  DefaultMaxActiveConfirmations,
		MaxWaitingConfirmations: DefaultMaxWaitingConfirmations,
	}
}

// Check reports invalid bounds without opening a subscription or touching the replica.
func (o Options) Check() error {
	switch {
	case o.ConfirmationGrace <= 0:
		return fmt.Errorf("confirmation grace must be positive, not %v: %w", o.ConfirmationGrace, syscall.EINVAL)
	case o.MaxActiveConfirmations <= 0 || o.MaxActiveConfirmations == math.MaxInt:
		return fmt.Errorf("maximum active mutation confirmations must be positive and finite, not %d: %w", o.MaxActiveConfirmations, syscall.EINVAL)
	case o.MaxWaitingConfirmations < 0 || o.MaxWaitingConfirmations == math.MaxInt:
		return fmt.Errorf("maximum waiting mutation confirmations must be non-negative and finite, not %d: %w", o.MaxWaitingConfirmations, syscall.EINVAL)
	default:
		return nil
	}
}
