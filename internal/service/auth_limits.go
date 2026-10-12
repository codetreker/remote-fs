package service

import (
	"context"
	"crypto/sha256"
	"net"
	"net/http"
	"sync"
	"time"
)

type passwordAdmission struct {
	active   chan struct{}
	admitted chan struct{}
	wait     time.Duration
}

func newPasswordAdmission(wait time.Duration) passwordAdmission {
	return passwordAdmission{active: make(chan struct{}, 2), admitted: make(chan struct{}, 10), wait: wait}
}

func (l *passwordAdmission) acquire(ctx context.Context) (func(), error) {
	select {
	case l.admitted <- struct{}{}:
	default:
		return nil, ErrAuthCapacity
	}
	ctx, cancel := context.WithTimeout(ctx, l.wait)
	defer cancel()
	select {
	case l.active <- struct{}{}:
		return func() { <-l.active; <-l.admitted }, nil
	case <-ctx.Done():
		<-l.admitted
		return nil, ctx.Err()
	}
}

type loginBucket struct {
	attempts int
	expires  time.Time
}
type loginLimits struct {
	mu          sync.Mutex
	buckets     map[[32]byte]loginBucket
	maxBuckets  int
	maxAttempts int
	window      time.Duration
}

func (l *loginLimits) admit(r *http.Request, login string, now time.Time) error {
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		peer = r.RemoteAddr
	}
	keys := [][32]byte{sha256.Sum256([]byte("peer\x00" + peer)), sha256.Sum256([]byte("login\x00" + login))}
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, bucket := range l.buckets {
		if !now.Before(bucket.expires) {
			delete(l.buckets, key)
		}
	}
	missing := 0
	for _, key := range keys {
		bucket, exists := l.buckets[key]
		if !exists {
			missing++
		}
		if exists && bucket.attempts >= l.maxAttempts {
			return ErrAuthCapacity
		}
	}
	if len(l.buckets)+missing > l.maxBuckets {
		return ErrAuthCapacity
	}
	for _, key := range keys {
		bucket, exists := l.buckets[key]
		if !exists {
			bucket.expires = now.Add(l.window)
		}
		bucket.attempts++
		l.buckets[key] = bucket
	}
	return nil
}
