// Package objectstoretest holds the executable obligations of objectstore.Objects.
// Every implementation runs the same cases so that create-only writes, immutable bytes,
// failure distinctions, capacity, cancellation, concurrent use, and lifecycle remain
// properties of the interface rather than of one backend.
package objectstoretest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

// NewObjects returns a fresh, empty object store. Run calls it once per case so that no
// case observes another case's keys.
type NewObjects func(t *testing.T) objectstore.Objects

// Run exercises the object store contract. A successful Put digest is deliberately left
// to backend-specific tests because the interface permits both a store-reported digest and
// nil.
func Run(t *testing.T, newObjects NewObjects) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			objects := newObjects(t)
			t.Cleanup(func() {
				if err := objects.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})
			c.run(t, objects)
		})
	}
}

type testCase struct {
	name string
	run  func(t *testing.T, objects objectstore.Objects)
}

var cases = []testCase{
	{name: "available capacity is nonnegative or unsupported", run: func(t *testing.T, objects objectstore.Objects) {
		first, firstErr := objects.Available(t.Context())
		second, secondErr := objects.Available(t.Context())

		switch {
		case firstErr == nil:
			if first < 0 {
				t.Errorf("the first Available returned %d, want a nonnegative byte count", first)
			}
			if secondErr != nil {
				t.Fatalf("the first Available returned a capacity and the second failed: %v", secondErr)
			}
			if second < 0 {
				t.Errorf("the second Available returned %d, want a nonnegative byte count", second)
			}
		case errors.Is(firstErr, syscall.ENOSYS):
			mustErrno(t, firstErr, syscall.ENOSYS)
			mustErrno(t, secondErr, syscall.ENOSYS)
		default:
			t.Fatalf("Available failed with %v, want a measured capacity or ENOSYS", firstErr)
		}
	}},

	{name: "a stored object round trips whole", run: func(t *testing.T, objects objectstore.Objects) {
		for i, content := range [][]byte{
			{},
			[]byte("a file's bytes, stored under an opaque key"),
			{0x00, 0xff, 0x01, 0xfe},
		} {
			key := fmt.Sprintf("round-trip-%d", i)
			if _, err := objects.Put(t.Context(), key, content); err != nil {
				t.Fatalf("Put %q: %v", key, err)
			}
			got, err := objects.Get(t.Context(), key)
			if err != nil {
				t.Fatalf("Get %q: %v", key, err)
			}
			if !bytes.Equal(got, content) {
				t.Errorf("Get %q returned %q, want %q", key, got, content)
			}
		}
	}},

	{name: "put is create only", run: func(t *testing.T, objects objectstore.Objects) {
		first := []byte("the object the metastore points at")
		if _, err := objects.Put(t.Context(), "taken", first); err != nil {
			t.Fatalf("the first Put: %v", err)
		}

		if _, err := objects.Put(t.Context(), "taken", []byte("a second writer's bytes")); err == nil {
			t.Fatal("the second Put succeeded, want EEXIST")
		} else {
			mustErrno(t, err, syscall.EEXIST)
		}

		got, err := objects.Get(t.Context(), "taken")
		if err != nil {
			t.Fatalf("Get after the refused Put: %v", err)
		}
		if !bytes.Equal(got, first) {
			t.Errorf("the refused Put replaced the object: Get returned %q, want %q", got, first)
		}
	}},

	{name: "absence is distinct from an unknown outcome", run: func(t *testing.T, objects objectstore.Objects) {
		if _, err := objects.Put(t.Context(), "kept", []byte("still here")); err != nil {
			t.Fatalf("Put: %v", err)
		}

		missing, err := objects.Get(t.Context(), "never-written")
		mustErrno(t, err, syscall.ENOENT)
		if missing != nil {
			t.Errorf("Get of an absent key returned %q alongside its error, want nil", missing)
		}

		expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		if _, err := objects.Get(expired, "kept"); err == nil {
			t.Fatal("Get past its deadline succeeded")
		} else {
			mustErrno(t, err, syscall.EIO)
		}
		if _, err := objects.Put(expired, "unwritten", []byte("must not arrive")); err == nil {
			t.Fatal("Put past its deadline succeeded")
		} else {
			mustErrno(t, err, syscall.EIO)
		}
		if err := objects.Delete(expired, "kept"); err == nil {
			t.Fatal("Delete past its deadline succeeded")
		} else {
			mustErrno(t, err, syscall.EIO)
		}

		mustHold(t, objects, "kept", []byte("still here"))
		if _, err := objects.Get(t.Context(), "unwritten"); err == nil {
			t.Fatal("the failed Put stored its object")
		} else {
			mustErrno(t, err, syscall.ENOENT)
		}
	}},

	{name: "delete is idempotent", run: func(t *testing.T, objects objectstore.Objects) {
		if _, err := objects.Put(t.Context(), "swept", []byte("unreferenced")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := objects.Delete(t.Context(), "swept"); err != nil {
			t.Fatalf("the first Delete: %v", err)
		}
		if _, err := objects.Get(t.Context(), "swept"); err == nil {
			t.Fatal("Get after Delete succeeded")
		} else {
			mustErrno(t, err, syscall.ENOENT)
		}
		if err := objects.Delete(t.Context(), "swept"); err != nil {
			t.Fatalf("the second Delete: %v", err)
		}
		if err := objects.Delete(t.Context(), "never-written"); err != nil {
			t.Fatalf("Delete of a key nobody wrote: %v", err)
		}
	}},

	{name: "stored objects share no bytes with the caller", run: func(t *testing.T, objects objectstore.Objects) {
		written := []byte("the bytes as they were written")
		want := bytes.Clone(written)
		if _, err := objects.Put(t.Context(), "immutable", written); err != nil {
			t.Fatalf("Put: %v", err)
		}
		copy(written, "OVERWRITTEN")

		first, err := objects.Get(t.Context(), "immutable")
		if err != nil {
			t.Fatalf("Get after the caller reused its buffer: %v", err)
		}
		if !bytes.Equal(first, want) {
			t.Fatalf("the caller's buffer reached the stored object: Get returned %q, want %q", first, want)
		}
		copy(first, "OVERWRITTEN")
		mustHold(t, objects, "immutable", want)
	}},

	{name: "cancellation interrupts every operation without changing state", run: func(t *testing.T, objects objectstore.Objects) {
		if _, err := objects.Put(t.Context(), "kept", []byte("still here")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		cancelled, cancel := context.WithCancel(t.Context())
		cancel()

		if _, err := objects.Get(cancelled, "kept"); err == nil {
			t.Fatal("Get on a cancelled context succeeded")
		} else {
			mustErrno(t, err, syscall.EINTR)
		}
		if _, err := objects.Put(cancelled, "unwritten", []byte("must not arrive")); err == nil {
			t.Fatal("Put on a cancelled context succeeded")
		} else {
			mustErrno(t, err, syscall.EINTR)
		}
		if err := objects.Delete(cancelled, "kept"); err == nil {
			t.Fatal("Delete on a cancelled context succeeded")
		} else {
			mustErrno(t, err, syscall.EINTR)
		}

		mustHold(t, objects, "kept", []byte("still here"))
		if _, err := objects.Get(t.Context(), "unwritten"); err == nil {
			t.Fatal("the interrupted Put stored its object")
		} else {
			mustErrno(t, err, syscall.ENOENT)
		}
	}},

	{name: "only one concurrent put wins one key", run: func(t *testing.T, objects objectstore.Objects) {
		const workers = 16
		start := make(chan struct{})
		ready := make(chan struct{}, workers)
		results := make(chan putResult, workers)

		for worker := range workers {
			go func() {
				content := []byte(fmt.Sprintf("writer-%d", worker))
				ready <- struct{}{}
				<-start
				_, err := objects.Put(t.Context(), "contended", content)
				results <- putResult{content: content, err: err}
			}()
		}
		for range workers {
			<-ready
		}
		close(start)

		completed := make([]putResult, 0, workers)
		for range workers {
			completed = append(completed, <-results)
		}

		var winner []byte
		for _, result := range completed {
			if result.err == nil {
				if winner != nil {
					t.Errorf("both %q and %q won the same key", winner, result.content)
				}
				winner = result.content
				continue
			}
			mustErrno(t, result.err, syscall.EEXIST)
		}
		if winner == nil {
			t.Fatal("no concurrent Put stored the key")
		}
		mustHold(t, objects, "contended", winner)
	}},

	{name: "different keys make progress independently", run: func(t *testing.T, objects objectstore.Objects) {
		const workers = 16
		start := make(chan struct{})
		ready := make(chan struct{}, workers)
		results := make(chan error, workers)

		for worker := range workers {
			go func() {
				key := fmt.Sprintf("worker-%d", worker)
				content := []byte(key + "'s object")
				ready <- struct{}{}
				<-start
				if _, err := objects.Put(t.Context(), key, content); err != nil {
					results <- fmt.Errorf("Put %q: %w", key, err)
					return
				}
				got, err := objects.Get(t.Context(), key)
				if err != nil {
					results <- fmt.Errorf("Get %q: %w", key, err)
					return
				}
				if !bytes.Equal(got, content) {
					results <- fmt.Errorf("Get %q returned %q, want %q", key, got, content)
					return
				}
				if err := objects.Delete(t.Context(), key); err != nil {
					results <- fmt.Errorf("Delete %q: %w", key, err)
					return
				}
				if _, err := objects.Get(t.Context(), key); !errors.Is(err, syscall.ENOENT) {
					results <- fmt.Errorf("Get %q after Delete returned %v, want ENOENT", key, err)
					return
				}
				results <- nil
			}()
		}
		for range workers {
			<-ready
		}
		close(start)
		for range workers {
			if err := <-results; err != nil {
				t.Error(err)
			}
		}
	}},

	{name: "reported failures use the errno vocabulary", run: func(t *testing.T, objects objectstore.Objects) {
		if _, err := objects.Put(t.Context(), "taken", []byte("bytes")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		cancelled, cancel := context.WithCancel(t.Context())
		cancel()
		expired, stop := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer stop()

		failures := []struct {
			name string
			run  func() error
		}{
			{name: "absent Get", run: func() error { _, err := objects.Get(t.Context(), "missing"); return err }},
			{name: "duplicate Put", run: func() error { _, err := objects.Put(t.Context(), "taken", nil); return err }},
			{name: "cancelled Get", run: func() error { _, err := objects.Get(cancelled, "taken"); return err }},
			{name: "cancelled Put", run: func() error { _, err := objects.Put(cancelled, "cancelled", nil); return err }},
			{name: "cancelled Delete", run: func() error { return objects.Delete(cancelled, "taken") }},
			{name: "expired Get", run: func() error { _, err := objects.Get(expired, "taken"); return err }},
			{name: "expired Put", run: func() error { _, err := objects.Put(expired, "expired", nil); return err }},
			{name: "expired Delete", run: func() error { return objects.Delete(expired, "taken") }},
		}
		for _, failure := range failures {
			t.Run(failure.name, func(t *testing.T) {
				err := failure.run()
				if err == nil {
					t.Fatal("operation succeeded")
				}
				errno := errnoOf(t, err)
				if _, ok := storage.ErrnoName(errno); !ok {
					t.Errorf("error reports %v, which is outside the errno vocabulary", errno)
				}
			})
		}
	}},
}

type putResult struct {
	content []byte
	err     error
}

func mustHold(t *testing.T, objects objectstore.Objects, key string, want []byte) {
	t.Helper()
	got, err := objects.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Get %q returned %q, want %q", key, got, want)
	}
}

func mustErrno(t *testing.T, err error, want syscall.Errno) {
	t.Helper()
	if err == nil {
		t.Fatalf("operation succeeded, want %s", want)
	}
	if got := errnoOf(t, err); got != want {
		t.Fatalf("error is %v, want %s: %v", got, want, err)
	}
}

func errnoOf(t *testing.T, err error) syscall.Errno {
	t.Helper()
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		t.Fatalf("%v carries no errno", err)
	}
	if _, ok := storage.ErrnoName(errno); !ok {
		t.Fatalf("%v reports %v, which is outside the errno vocabulary", err, errno)
	}
	return errno
}
