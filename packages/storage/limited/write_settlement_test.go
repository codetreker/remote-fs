package limited_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
)

func TestShrinkingWriteReleasesQuotaOnlyAfterSuccessfulPublication(t *testing.T) {
	for _, failure := range []error{syscall.EIO, nil} {
		name := "success"
		if failure != nil {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			backing := openDir(t, t.TempDir())
			original := bytes.Repeat([]byte{'a'}, limited.MinLimit)
			if err := backing.Write(t.Context(), "a", original); err != nil {
				t.Fatal(err)
			}
			paused := newPausedWrite(backing, "a", failure)
			s := newStorageOver(t, paused, limited.MinLimit)
			result := startPausedWrite(t, s, paused, t.Context(), nil)

			mustUse(t, s, limited.MinLimit)
			if space := spaceOf(t, s); space.Avail != 0 {
				t.Fatalf("pending shrink exposed %d bytes of quota", space.Avail)
			}
			if err := s.Write(t.Context(), "b", content(limited.MinLimit)); !errors.Is(err, syscall.EDQUOT) {
				t.Fatalf("competing growth returned %v, want EDQUOT", err)
			}
			paused.resume()
			if err := <-result; !errors.Is(err, failure) {
				t.Fatalf("shrinking write returned %v, want %v", err, failure)
			}
			want := original
			if failure == nil {
				want = nil
			}
			got, err := backing.Read(t.Context(), "a")
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("file after shrinking write = %d bytes, error %v; want %d unchanged bytes", len(got), err, len(want))
			}
			mustUse(t, s, int64(len(want)))
			if err := s.Recount(t.Context()); err != nil {
				t.Fatal(err)
			}
			mustUse(t, s, int64(len(want)))
			if failure == nil {
				mustWrite(t, s, "b", limited.MinLimit)
				mustUse(t, s, limited.MinLimit)
				if err := s.Write(t.Context(), "c", []byte{1}); !errors.Is(err, syscall.EDQUOT) {
					t.Fatalf("growth after consuming released quota returned %v, want EDQUOT", err)
				}
			} else if _, err := backing.Stat(t.Context(), "b"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("refused growth left a target: %v", err)
			}
		})
	}
}

func TestFailedGrowthReleasesOnlyItsOwnReservation(t *testing.T) {
	backing := openDir(t, t.TempDir())
	paused := newPausedWrite(backing, "a", syscall.EIO)
	s := newStorageOver(t, paused, limited.MinLimit)
	result := startPausedWrite(t, s, paused, t.Context(), content(2048))
	mustWrite(t, s, "b", 2048)
	mustUse(t, s, limited.MinLimit)
	if err := s.Write(t.Context(), "c", []byte{1}); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("growth beyond outstanding reservations returned %v, want EDQUOT", err)
	}
	paused.resume()
	if err := <-result; !errors.Is(err, syscall.EIO) {
		t.Fatalf("failed growth returned %v, want EIO", err)
	}
	if _, err := backing.Stat(t.Context(), "a"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed growth left a target: %v", err)
	}
	got, err := backing.Read(t.Context(), "b")
	if err != nil || !bytes.Equal(got, content(2048)) {
		t.Fatalf("failed growth changed the competing file: length %d, error %v", len(got), err)
	}
	mustUse(t, s, 2048)
	mustWrite(t, s, "c", 2048)
	mustUse(t, s, limited.MinLimit)
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, limited.MinLimit)
}

func TestCanceledShrinkRetainsContentAndQuota(t *testing.T) {
	backing := openDir(t, t.TempDir())
	original := bytes.Repeat([]byte{'a'}, limited.MinLimit)
	if err := backing.Write(t.Context(), "a", original); err != nil {
		t.Fatal(err)
	}
	paused := newPausedWrite(backing, "a", nil)
	s := newStorageOver(t, paused, limited.MinLimit)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := startPausedWrite(t, s, paused, ctx, nil)
	mustUse(t, s, limited.MinLimit)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled shrink returned %v, want cancellation", err)
	}
	got, err := backing.Read(t.Context(), "a")
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("canceled shrink changed the existing bytes: length %d, error %v", len(got), err)
	}
	mustUse(t, s, limited.MinLimit)
	if err := s.Write(t.Context(), "b", []byte{1}); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("growth after canceled shrink returned %v, want EDQUOT", err)
	}
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, limited.MinLimit)
}

func TestEqualSizeWriteDoesNotReserveOrReleaseQuota(t *testing.T) {
	for _, failure := range []error{syscall.EIO, nil} {
		name := "success"
		if failure != nil {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			backing := openDir(t, t.TempDir())
			original := bytes.Repeat([]byte{'a'}, limited.MinLimit)
			if err := backing.Write(t.Context(), "a", original); err != nil {
				t.Fatal(err)
			}
			paused := newPausedWrite(backing, "a", failure)
			s := newStorageOver(t, paused, limited.MinLimit)
			replacement := bytes.Repeat([]byte{'b'}, limited.MinLimit)
			result := startPausedWrite(t, s, paused, t.Context(), replacement)
			mustUse(t, s, limited.MinLimit)
			paused.resume()
			if err := <-result; !errors.Is(err, failure) {
				t.Fatalf("equal-size write returned %v, want %v", err, failure)
			}
			want := replacement
			if failure != nil {
				want = original
			}
			got, err := backing.Read(t.Context(), "a")
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("equal-size write returned incorrect bytes: length %d, error %v", len(got), err)
			}
			mustUse(t, s, limited.MinLimit)
			if err := s.Recount(t.Context()); err != nil {
				t.Fatal(err)
			}
			mustUse(t, s, limited.MinLimit)
		})
	}
}

func startPausedWrite(t *testing.T, s *limited.Storage, paused *pausedWrite, ctx context.Context, body []byte) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		result <- s.Write(ctx, paused.name, body)
	}()
	t.Cleanup(func() {
		paused.resume()
		<-finished
	})
	select {
	case <-paused.entered:
	case <-finished:
		t.Fatalf("write returned before reaching the paused backing: %v", <-result)
	}
	return result
}

type pausedWrite struct {
	storage.BoundedStorage
	name    string
	failure error
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newPausedWrite(backing storage.BoundedStorage, name string, failure error) *pausedWrite {
	return &pausedWrite{BoundedStorage: backing, name: name, failure: failure,
		entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *pausedWrite) resume() { s.once.Do(func() { close(s.release) }) }

func (s *pausedWrite) Write(ctx context.Context, name string, content []byte) error {
	if name == s.name {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if s.failure != nil {
			return s.failure
		}
	}
	return s.BoundedStorage.Write(ctx, name, content)
}
