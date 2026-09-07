package localdisk

import (
	"context"
	"sync"
)

// shardLocker bounds shard initialization coordination to the fixed on-disk fanout.
// Holding a token covers only opening, creating, and validating the shard directory and
// its identity marker; object I/O proceeds independently after the descriptor is returned.
type shardLocker struct {
	tokens [256]chan struct{}
}

func newShardLocker() *shardLocker {
	locks := &shardLocker{}
	for index := range locks.tokens {
		locks.tokens[index] = make(chan struct{}, 1)
		locks.tokens[index] <- struct{}{}
	}
	return locks
}

func (l *shardLocker) acquire(ctx context.Context, shard byte) (func(), error) {
	token := l.tokens[shard]
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-token:
		if err := ctx.Err(); err != nil {
			token <- struct{}{}
			return nil, err
		}
		var once sync.Once
		return func() {
			once.Do(func() { token <- struct{}{} })
		}, nil
	}
}
