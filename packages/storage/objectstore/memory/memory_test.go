package memory_test

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

// These cases pin the obligations objectstore.Objects states, against the one
// implementation that can be driven without a service to reach. They are deliberately the
// same obligations azblob is held to, and are not a substitute for holding it to them: the
// store exercised here has no wire, no credentials and no error codes, so passing says
// nothing about the run against Azurite that proves those.

func md5Of(content []byte) []byte {
	sum := md5.Sum(content)
	return sum[:]
}

// errnoOf is the errno err travels under, and fails the test where there is none: an error
// the contract's vocabulary cannot name reaches a caller as EIO, so one that reports a
// determinate refusal without an errno has already lost the distinction.
func errnoOf(t *testing.T, err error) syscall.Errno {
	t.Helper()
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		t.Fatalf("%v carries no errno", err)
	}
	return errno
}

func TestPutStoresWhatGetReturns(t *testing.T) {
	objects := memory.New()
	content := []byte("a file's bytes, stored under an opaque key")

	if _, err := objects.Put(context.Background(), "written-once", content); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := objects.Get(context.Background(), "written-once")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("Get returned %q, want %q", got, content)
	}
}

// The digest has to be the one the real store reports for the same bytes, or a caller that
// checks a recorded digest against its content passes here and fails there.
func TestPutReportsTheDigestOfTheBytesItStored(t *testing.T) {
	objects := memory.New()

	for _, size := range []int{0, 1, 4096} {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(i)
		}
		key := fmt.Sprintf("sized-%d", size)

		digest, err := objects.Put(context.Background(), key, content)
		if err != nil {
			t.Fatalf("Put of %d bytes: %v", size, err)
		}
		if want := md5Of(content); string(digest) != string(want) {
			t.Errorf("Put of %d bytes reported digest %x, want %x", size, digest, want)
		}
	}
}

// A key is reserved once and written once, so a second Put is a reservation handed out
// twice. It must be refused, and it must not replace what is already there.
func TestPutRefusesAKeyThatHasBeenWritten(t *testing.T) {
	objects := memory.New()
	first := []byte("the object the metastore points at")

	if _, err := objects.Put(context.Background(), "taken", first); err != nil {
		t.Fatalf("the first Put: %v", err)
	}

	digest, err := objects.Put(context.Background(), "taken", []byte("a second writer's bytes"))
	if !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("the second Put returned %v, want EEXIST", err)
	}
	if digest != nil {
		t.Errorf("the refused Put reported digest %x alongside its error, want nil", digest)
	}

	got, err := objects.Get(context.Background(), "taken")
	if err != nil {
		t.Fatalf("Get after the refused Put: %v", err)
	}
	if string(got) != string(first) {
		t.Errorf("the refused Put replaced the object: Get returned %q, want %q", got, first)
	}
}

func TestGetOfAKeyNobodyWroteIsAbsent(t *testing.T) {
	objects := memory.New()

	got, err := objects.Get(context.Background(), "never-written")
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("Get returned %v, want ENOENT", err)
	}
	if got != nil {
		t.Errorf("Get returned %q alongside its error, want nil", got)
	}
}

// Deletion is driven by a sweeper that may be running again after being interrupted
// between removing the object and recording that it did, so a second Delete must converge.
func TestDeleteRemovesTheObjectAndRunsAgainWithoutFailing(t *testing.T) {
	objects := memory.New()
	if _, err := objects.Put(context.Background(), "swept", []byte("unreferenced")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := objects.Delete(context.Background(), "swept"); err != nil {
		t.Fatalf("the first Delete: %v", err)
	}
	if _, err := objects.Get(context.Background(), "swept"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("Get after Delete returned %v, want ENOENT", err)
	}
	if err := objects.Delete(context.Background(), "swept"); err != nil {
		t.Fatalf("the second Delete: %v", err)
	}
	if err := objects.Delete(context.Background(), "never-written"); err != nil {
		t.Fatalf("Delete of a key nobody wrote: %v", err)
	}
}

// An object is immutable once written, and the only thing that makes that true here is
// that neither the buffer handed to Put nor the slice handed back by Get is the stored
// array. Serialising the bytes gives the real store this for nothing, which is exactly why
// a store that shared them would let this layer's callers pass here and corrupt objects
// there.
func TestTheStoredObjectSharesNoBytesWithTheCaller(t *testing.T) {
	objects := memory.New()
	written := []byte("the bytes as they were written")

	if _, err := objects.Put(context.Background(), "immutable", written); err != nil {
		t.Fatalf("Put: %v", err)
	}
	copy(written, "OVERWRITTEN")

	first, err := objects.Get(context.Background(), "immutable")
	if err != nil {
		t.Fatalf("Get after the caller reused its buffer: %v", err)
	}
	if want := "the bytes as they were written"; string(first) != want {
		t.Fatalf("the caller's buffer reached the stored object: Get returned %q, want %q", first, want)
	}
	copy(first, "OVERWRITTEN")

	second, err := objects.Get(context.Background(), "immutable")
	if err != nil {
		t.Fatalf("the second Get: %v", err)
	}
	if want := "the bytes as they were written"; string(second) != want {
		t.Errorf("writing into what Get returned changed the stored object: the second Get returned %q, want %q", second, want)
	}
}

// A request the caller withdrew is EINTR, as it is against the service, and it must leave
// the store as it found it: an operation that reported failure and stored the object
// anyway is the reverse of the fabricated success R-ERR-2 forbids and just as wrong.
func TestAWithdrawnRequestIsReportedAsInterrupted(t *testing.T) {
	objects := memory.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := objects.Put(ctx, "withdrawn", []byte("bytes")); !errors.Is(err, syscall.EINTR) {
		t.Errorf("Put on a cancelled context returned %v, want EINTR", err)
	}
	if _, err := objects.Get(ctx, "withdrawn"); !errors.Is(err, syscall.EINTR) {
		t.Errorf("Get on a cancelled context returned %v, want EINTR", err)
	}
	if err := objects.Delete(ctx, "withdrawn"); !errors.Is(err, syscall.EINTR) {
		t.Errorf("Delete on a cancelled context returned %v, want EINTR", err)
	}

	if _, err := objects.Get(context.Background(), "withdrawn"); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("the refused Put stored its object anyway: Get returned %v, want ENOENT", err)
	}
}

// A deadline that has passed is a failure the vocabulary has no word for, and EIO is what
// the contract reserves for those. What matters is the half it rules out: not ENOENT.
func TestAnExpiredDeadlineIsNotReportedAsAbsent(t *testing.T) {
	objects := memory.New()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	if _, err := objects.Get(ctx, "a-key"); !errors.Is(err, syscall.EIO) {
		t.Errorf("Get past its deadline returned %v, want EIO", err)
	}
	if _, err := objects.Put(ctx, "a-key", []byte("bytes")); !errors.Is(err, syscall.EIO) {
		t.Errorf("Put past its deadline returned %v, want EIO", err)
	}
	if err := objects.Delete(ctx, "a-key"); !errors.Is(err, syscall.EIO) {
		t.Errorf("Delete past its deadline returned %v, want EIO", err)
	}
}

// Every failure this store can report has to be sayable in the contract's vocabulary, and
// only one of them may be the word for an object that is not there. An implementation that
// answered ENOENT to something else would tell a sweeper an object was removed that is
// still there, and tell a reader a file is gone that is not.
func TestOnlyAnAbsentObjectIsReportedAsAbsent(t *testing.T) {
	objects := memory.New()
	if _, err := objects.Put(context.Background(), "taken", []byte("bytes")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()

	absent := func() error {
		_, err := objects.Get(context.Background(), "never-written")
		return err
	}
	for what, failing := range map[string]func() error{
		"a Get of a key nobody wrote": absent,
		"a Put of a key already written": func() error {
			_, err := objects.Put(context.Background(), "taken", []byte("more bytes"))
			return err
		},
		"a withdrawn Get": func() error {
			_, err := objects.Get(cancelled, "taken")
			return err
		},
		"a withdrawn Put": func() error {
			_, err := objects.Put(cancelled, "unwritten", []byte("bytes"))
			return err
		},
		"a withdrawn Delete": func() error { return objects.Delete(cancelled, "taken") },
		"a Get past its deadline": func() error {
			_, err := objects.Get(expired, "taken")
			return err
		},
		"a Delete past its deadline": func() error { return objects.Delete(expired, "taken") },
	} {
		err := failing()
		if err == nil {
			t.Errorf("%s succeeded", what)
			continue
		}
		errno := errnoOf(t, err)
		if _, ok := storage.ErrnoName(errno); !ok {
			t.Errorf("%s reports %v, which is outside the errno vocabulary", what, errno)
		}
		if errno == syscall.ENOENT && what != "a Get of a key nobody wrote" {
			t.Errorf("%s reads as an absent object", what)
		}
	}
}

// Every layer built on this store is concurrent, so a data race here would surface as a
// mystery in their tests rather than as a failure in this package. -race catches the race;
// what the assertions catch is the other half — that the exclusion is real and not merely
// quiet, and that traffic on one key does not disturb another.
//
// The barrier is the premise, so it is asserted rather than assumed. A worker counts
// itself in before it reports for the barrier and the barrier does not open until every
// worker has reported, which makes the count read afterwards exact: all of them are past
// the increment and none of them is past its work. A run where they trickled through one
// at a time would contend over nothing and would read exactly like a run that proved
// something.
func TestConcurrentUseIsSafe(t *testing.T) {
	const workers = 32
	objects := memory.New()

	var inflight, wins atomic.Int64
	arrived := make(chan struct{}, workers)
	start := make(chan struct{})
	var group sync.WaitGroup

	for worker := range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			inflight.Add(1)
			defer inflight.Add(-1)
			arrived <- struct{}{}
			<-start

			// One key they all write. Exactly one may store it; the rest are refused, and
			// what any of them reads back is the winner's bytes rather than a half-written
			// object or nothing.
			if _, err := objects.Put(context.Background(), "contended", []byte("one writer's bytes")); err == nil {
				wins.Add(1)
			} else if !errors.Is(err, syscall.EEXIST) {
				t.Errorf("the contended Put returned %v, want EEXIST", err)
			}
			if got, err := objects.Get(context.Background(), "contended"); err != nil {
				t.Errorf("Get of the contended key: %v", err)
			} else if string(got) != "one writer's bytes" {
				t.Errorf("Get of the contended key returned %q", got)
			}

			// And one key of its own, put through the whole life of an object while the
			// others are doing the same to theirs.
			own := fmt.Sprintf("worker-%d", worker)
			content := []byte(own + "'s object")
			digest, err := objects.Put(context.Background(), own, content)
			if err != nil {
				t.Errorf("Put of %q: %v", own, err)
				return
			}
			if want := md5Of(content); string(digest) != string(want) {
				t.Errorf("Put of %q reported digest %x, want %x", own, digest, want)
			}
			if got, err := objects.Get(context.Background(), own); err != nil {
				t.Errorf("Get of %q: %v", own, err)
			} else if string(got) != string(content) {
				t.Errorf("Get of %q returned %q, want %q", own, got, content)
			}
			if err := objects.Delete(context.Background(), own); err != nil {
				t.Errorf("Delete of %q: %v", own, err)
			}
			if _, err := objects.Get(context.Background(), own); !errors.Is(err, syscall.ENOENT) {
				t.Errorf("Get of %q after Delete returned %v, want ENOENT", own, err)
			}
		}()
	}

	for range workers {
		<-arrived
	}
	waiting := inflight.Load()
	close(start)
	group.Wait()

	if waiting != workers {
		t.Errorf("%d of %d workers had reached the barrier when it opened, so they did not go at the store together",
			waiting, workers)
	}
	if wins.Load() != 1 {
		t.Errorf("%d of %d workers stored the contended key, want exactly one", wins.Load(), workers)
	}
}
