package localdir

import (
	"bytes"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/limited"
)

func TestExpiredStagedShrinkRetainsBytesAndQuota(t *testing.T) {
	clock := &operationClock{now: time.Now()}
	native := operationStorage(t, func(cfg *Config) { cfg.Locks.Clock = clock })
	quota, err := limited.New(t.Context(), native, limited.MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Repeat([]byte{'o'}, limited.MinLimit)
	if err := quota.Write(t.Context(), "one", original); err != nil {
		t.Fatal(err)
	}
	scope := operationGrant(t, native, "one", time.Second)
	replacement := []byte("shrunk")
	entered, resume := make(chan struct{}), make(chan struct{})
	var pause, release sync.Once
	t.Cleanup(func() { release.Do(func() { close(resume) }) })
	write := native.ops.write
	native.ops.write = func(fd int, body []byte) (int, error) {
		if bytes.Equal(body, replacement) {
			pause.Do(func() { close(entered); <-resume })
		}
		return write(fd, body)
	}
	done := make(chan error, 1)
	go func() { done <- quota.Write(locking.WithScope(t.Context(), scope), "one", replacement) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("shrinking write did not reach private staging")
	}
	assertCharge := func(want int64) {
		t.Helper()
		space, err := quota.Space(t.Context())
		if err != nil || space.Used != want || space.Avail != limited.MinLimit-want {
			t.Fatalf("quota = %+v, error %v; want used %d", space, err, want)
		}
	}
	assertFull := func() {
		t.Helper()
		assertCharge(limited.MinLimit)
		if err := quota.Write(t.Context(), "growth", []byte{'x'}); !errors.Is(err, syscall.EDQUOT) {
			t.Fatalf("growth against retained charge returned %v, want EDQUOT", err)
		}
	}
	assertFull()
	clock.advance(2 * time.Second)
	release.Do(func() { close(resume) })
	if err := operationAwait(t, done); locking.CodeOf(err) != locking.StaleGrant {
		t.Fatalf("expired staged shrink returned %v, want stale grant", err)
	}
	got, err := quota.Read(t.Context(), "one")
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("expired shrink changed committed bytes: length %d, error %v", len(got), err)
	}
	assertFull()
	if err := quota.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertFull()
	if err := quota.Write(t.Context(), "one", replacement); err != nil {
		t.Fatalf("valid shrink after expiry: %v", err)
	}
	assertCharge(int64(len(replacement)))
	if err := quota.Write(t.Context(), "two", original[len(replacement):]); err != nil {
		t.Fatalf("using the released bytes: %v", err)
	}
	assertFull()
	if err := quota.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertCharge(limited.MinLimit)
}
