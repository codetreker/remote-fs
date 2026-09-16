package storage

import (
	"context"
	"fmt"
	"syscall"
)

// AttrResultBudget checks one returned attribute representation from fixed
// facts and the canonical metadata byte length, before its payload is loaded.
// Callbacks are bounded internal accounting: no I/O, reentry or retained inputs.
// They return a known refusal when the caller's result budget cannot fit.
type AttrResultBudget func(scalar Attr, metadataBytes int64) error

type attrResultBudgetKey struct{}

// WithAttrResultBudget attaches a request-local result admission check. A nil
// callback is a configuration error. The native producer checks only attributes
// returned to this caller, not unrelated parent or internal state observations.
func WithAttrResultBudget(ctx context.Context, budget AttrResultBudget) context.Context {
	if budget == nil {
		panic("storage: nil attribute result budget")
	}
	return context.WithValue(ctx, attrResultBudgetKey{}, budget)
}

// CheckAttrResultBudget runs before loading returned metadata and before any
// mutation or retention that will return the proposed attributes. Without a
// caller callback the native hard metadata bound still applies. Fixed instants
// are copied so the callback cannot alter the producer's observations.
func CheckAttrResultBudget(ctx context.Context, scalar Attr, metadataBytes int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if scalar.Metadata != nil {
		return fmt.Errorf("attribute result admission requires unloaded metadata: %w", syscall.EINVAL)
	}
	if metadataBytes < 6 {
		return fmt.Errorf("attribute metadata has invalid encoded length: %w", syscall.EIO)
	}
	if metadataBytes > MaxMetadataBytes {
		return fmt.Errorf("attribute metadata exceeds native byte bound: %w", syscall.EFBIG)
	}
	budget, _ := ctx.Value(attrResultBudgetKey{}).(AttrResultBudget)
	if budget == nil {
		return nil
	}
	return budget(scalar.Clone(), metadataBytes)
}

// HasAttrResultBudget allows native producers to keep their single hard-bounded
// query when no caller-specific admission requires a preliminary header read.
func HasAttrResultBudget(ctx context.Context) bool {
	budget, _ := ctx.Value(attrResultBudgetKey{}).(AttrResultBudget)
	return budget != nil
}
