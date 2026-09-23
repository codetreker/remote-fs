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

type attrResultBudgetValue struct {
	limit int64
	check AttrResultBudget
}

// WithAttrResultBudget attaches a request-local result admission check. A nil
// callback is a configuration error. The native producer checks only attributes
// returned to this caller, not unrelated parent or internal state observations.
func WithAttrResultBudget(ctx context.Context, budget AttrResultBudget) context.Context {
	if budget == nil {
		panic("storage: nil attribute result budget")
	}
	return context.WithValue(ctx, attrResultBudgetKey{}, attrResultBudgetValue{check: budget})
}

// WithBoundedAttrResult attaches an attribute admission check and the caller's
// transport-independent encoded-result ceiling. Wrappers that cross another
// transport propagate the ceiling so the authority can refuse before mutation.
func WithBoundedAttrResult(ctx context.Context, limit int64, budget AttrResultBudget) context.Context {
	if limit <= 0 {
		panic("storage: non-positive attribute result bound")
	}
	if budget == nil {
		panic("storage: nil attribute result budget")
	}
	return context.WithValue(ctx, attrResultBudgetKey{}, attrResultBudgetValue{limit: limit, check: budget})
}

// WithAttrResultProjection changes the attributes seen by the caller's result
// budget before a producer admits a mutation. The encoded byte ceiling stays
// intact; wrappers use this when their returned attributes differ from the
// native producer's attributes.
func WithAttrResultProjection(ctx context.Context, project func(Attr) Attr) context.Context {
	if project == nil {
		panic("storage: nil attribute result projection")
	}
	value, _ := ctx.Value(attrResultBudgetKey{}).(attrResultBudgetValue)
	if value.check == nil {
		return ctx
	}
	prior := value.check
	value.check = func(scalar Attr, metadataBytes int64) error {
		return prior(project(scalar), metadataBytes)
	}
	return context.WithValue(ctx, attrResultBudgetKey{}, value)
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
	value, _ := ctx.Value(attrResultBudgetKey{}).(attrResultBudgetValue)
	if value.check == nil {
		return nil
	}
	return value.check(scalar.Clone(), metadataBytes)
}

// HasAttrResultBudget allows native producers to keep their single hard-bounded
// query when no caller-specific admission requires a preliminary header read.
func HasAttrResultBudget(ctx context.Context) bool {
	value, _ := ctx.Value(attrResultBudgetKey{}).(attrResultBudgetValue)
	return value.check != nil
}

// AttrResultByteLimit returns the encoded-result ceiling carried by
// WithBoundedAttrResult. A plain callback has no transferable numeric ceiling.
func AttrResultByteLimit(ctx context.Context) (int64, bool) {
	value, _ := ctx.Value(attrResultBudgetKey{}).(attrResultBudgetValue)
	return value.limit, value.limit > 0
}
