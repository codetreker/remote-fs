package storage

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
)

// PublicationResult describes the namespace effect, independently of the operation's
// returned error. An applied mutation may still fail while confirming durability.
type PublicationResult uint8

const (
	PublicationNotApplied PublicationResult = iota + 1
	PublicationApplied
	PublicationUnknown
)

// PublicationAccounting prepares a charge for an atomic namespace transition using
// its actual previous and next byte counts. Hooks run under publication ordering and
// must perform only bounded internal accounting, without external I/O or callbacks.
// Growth is reserved before publication; shrinking bytes remain charged until Applied.
//
// A returned settlement owns any reservation, including when returned with an error.
// An error with no settlement must leave no charge behind. Success requires a non-nil
// settlement. Hook composition depth is controlled by the embedder's storage wrappers.
type PublicationAccounting func(previous, next int64) (PublicationSettlement, error)

// PublicationSettlement consumes prepared accounting once. NotApplied undoes its
// reservation; Applied records the actual change; Unknown retains the conservative
// charge and fences further accounting until recovery. Settlement must not depend on
// request cancellation, and its errors must be retained with the publication error.
type PublicationSettlement func(PublicationResult) error

type publicationAccountingUncertain struct{ cause error }

func (e *publicationAccountingUncertain) Error() string {
	return "publication accounting is uncertain: " + e.cause.Error()
}

func (e *publicationAccountingUncertain) Unwrap() error         { return e.cause }
func (e *publicationAccountingUncertain) Classification() error { return syscall.EIO }
func (e *publicationAccountingUncertain) Is(target error) bool  { return target == syscall.EIO }

// IsPublicationAccountingUncertain identifies failed settlement, including a failed
// NotApplied unwind, or an Unknown publication. Such failures require fencing further
// accounting and publication until recovery, even when the namespace effect is known.
// Preparation refusal with a successful unwind remains an ordinary operation error.
// Wrapped and joined errors retain this distinction and all underlying causes.
func IsPublicationAccountingUncertain(err error) bool {
	var uncertain *publicationAccountingUncertain
	return errors.As(err, &uncertain)
}

type publicationAccountingKey struct{}

type publicationAccountingLink struct {
	hook     PublicationAccounting
	previous *publicationAccountingLink
}

// WithPublicationAccounting attaches a hook without changing earlier contexts. Hooks
// prepare in registration order and settle in reverse order. A nil hook panics.
func WithPublicationAccounting(ctx context.Context, hook PublicationAccounting) context.Context {
	if hook == nil {
		panic("storage: nil publication accounting hook")
	}
	previous, _ := ctx.Value(publicationAccountingKey{}).(*publicationAccountingLink)
	return context.WithValue(ctx, publicationAccountingKey{}, &publicationAccountingLink{
		hook: hook, previous: previous,
	})
}

// PreparePublication runs after the backend has resolved actual byte counts under its
// final publication ordering. It rejects negative counts with EINVAL. Cancellation or
// preparation failure unwinds all returned settlements as NotApplied, retaining every
// error. A successful hook without a settlement violates the contract and returns EIO.
//
// The returned settlement must run before publication ordering is released, even if
// the request is canceled. Invalid results return EINVAL without consuming it; a second
// valid invocation returns EINVAL. Unknown and every failed settlement are marked by
// IsPublicationAccountingUncertain and classified as EIO. Without accounting, known
// outcomes need no additional work; Unknown still reports accounting uncertainty.
func PreparePublication(ctx context.Context, previous, next int64) (PublicationSettlement, error) {
	if previous < 0 || next < 0 {
		return nil, fmt.Errorf("publication byte counts must be non-negative: %w", syscall.EINVAL)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	link, _ := ctx.Value(publicationAccountingKey{}).(*publicationAccountingLink)
	var hooks []PublicationAccounting
	for ; link != nil; link = link.previous {
		hooks = append(hooks, link.hook)
	}
	settlements := make([]PublicationSettlement, 0, len(hooks))
	for i := len(hooks) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, settlePublication(settlements, PublicationNotApplied))
		}
		settle, err := hooks[i](previous, next)
		if settle != nil {
			settlements = append(settlements, settle)
		}
		if err == nil && settle == nil {
			err = fmt.Errorf("publication accounting succeeded without a settlement: %w", syscall.EIO)
		}
		if err != nil {
			return nil, errors.Join(err, settlePublication(settlements, PublicationNotApplied))
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, settlePublication(settlements, PublicationNotApplied))
	}
	var consumed atomic.Bool
	return func(result PublicationResult) error {
		if result != PublicationNotApplied && result != PublicationApplied && result != PublicationUnknown {
			return fmt.Errorf("publication result %d is invalid: %w", result, syscall.EINVAL)
		}
		if !consumed.CompareAndSwap(false, true) {
			return fmt.Errorf("publication accounting can be settled exactly once: %w", syscall.EINVAL)
		}
		err := settlePublication(settlements, result)
		clear(settlements)
		return err
	}, nil
}

func settlePublication(settlements []PublicationSettlement, result PublicationResult) error {
	var failures []error
	if result == PublicationUnknown {
		failures = append(failures, syscall.EIO)
	}
	for i := len(settlements) - 1; i >= 0; i-- {
		if err := settlements[i](result); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return &publicationAccountingUncertain{cause: errors.Join(failures...)}
}
