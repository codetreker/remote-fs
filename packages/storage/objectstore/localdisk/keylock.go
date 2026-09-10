package localdisk

import (
	"context"
	"sync"
)

// keyLocker serializes the staging names of exactly one key. Entries are reference
// counted, so the map is bounded by the active and waiting operation limits. Unrelated
// keys never share a lock merely because their hashes happen to collide.
type keyLocker struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

type keyLock struct {
	token chan struct{}
	refs  int
}

func newKeyLocker() *keyLocker { return &keyLocker{locks: make(map[string]*keyLock)} }

func (l *keyLocker) acquire(ctx context.Context, key string) (func(), error) {
	l.mu.Lock()
	entry := l.locks[key]
	if entry == nil {
		entry = &keyLock{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		l.locks[key] = entry
	}
	entry.refs++
	l.mu.Unlock()

	select {
	case <-ctx.Done():
		l.drop(key, entry)
		return nil, ctx.Err()
	case <-entry.token:
		if err := ctx.Err(); err != nil {
			entry.token <- struct{}{}
			l.drop(key, entry)
			return nil, err
		}
		var once sync.Once
		return func() {
			once.Do(func() {
				entry.token <- struct{}{}
				l.drop(key, entry)
			})
		}, nil
	}
}

func (l *keyLocker) drop(key string, entry *keyLock) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry.refs--
	if entry.refs == 0 {
		delete(l.locks, key)
	}
}
