package storage

import "context"

type listResultByteLimitKey struct{}

// WithBoundedListResult carries the encoded-list ceiling imposed by an outer
// transport. Nested transports propagate the smallest ceiling before loading entries.
func WithBoundedListResult(ctx context.Context, limit int64) context.Context {
	if limit <= 0 {
		panic("storage: non-positive listing result bound")
	}
	if outer, ok := ListResultByteLimit(ctx); ok {
		limit = min(limit, outer)
	}
	return context.WithValue(ctx, listResultByteLimitKey{}, limit)
}

// ListResultByteLimit returns an outer transport's encoded-list ceiling.
func ListResultByteLimit(ctx context.Context) (int64, bool) {
	limit, _ := ctx.Value(listResultByteLimitKey{}).(int64)
	return limit, limit > 0
}
