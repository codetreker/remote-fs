package objectstore_test

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

// A namespace reaches its object store only through objectstore.Objects, so every way that
// half can fail arrives as an error from one of three methods. These cases inject those
// errors instead of provoking them.
//
// Provoking one means a real client aimed at an address nothing answers. That proves what
// the azblob package's own cases already prove — that a real failure is classified as a
// failure — and it leaves this package's own obligation checked against the single error a
// refused connection happens to carry. R-ERR-6 is not about that error. It is about every
// error: whatever the object store reports, the namespace above it must answer for the part
// that failed rather than with a fact about names. Injection is what sends the whole
// vocabulary through, including the failures that carry no errno at all and the ones no
// address could produce.

// failingObjects answers one method with a given error and passes the rest to the store
// underneath. Passing the rest through is what keeps a case honest: the object being read
// was written by a real Put, so the tree names a key that genuinely exists and the injected
// error is the only thing wrong with the namespace.
type failingObjects struct {
	objectstore.Objects
	onGet, onPut, onDelete error
}

func (f *failingObjects) Get(ctx context.Context, key string) ([]byte, error) {
	if f.onGet != nil {
		return nil, f.onGet
	}
	return f.Objects.Get(ctx, key)
}

func (f *failingObjects) Put(ctx context.Context, key string, content []byte) ([]byte, error) {
	if f.onPut != nil {
		return nil, f.onPut
	}
	return f.Objects.Put(ctx, key, content)
}

func (f *failingObjects) Delete(ctx context.Context, key string) error {
	if f.onDelete != nil {
		return f.onDelete
	}
	return f.Objects.Delete(ctx, key)
}

// failing returns a namespace over the same tree and the same objects as p, reached through
// a wrapper a case switches into failing once the namespace holds what the case needs.
// Nothing fails while the fixture is being built, so a case exercises only the failure it
// named.
//
// The tree is shared with p rather than opened a second time, and p's cleanup closes it.
// This namespace therefore borrows the tree and must not be closed.
func (p parts) failing() (*objectstore.Storage, *failingObjects) {
	objects := &failingObjects{Objects: p.objects}
	return objectstore.New(objects, p.meta), objects
}

// objectFailures are answers an Objects can give that are not "the object is not there".
var objectFailures = []struct {
	name string
	err  error
}{
	// What azblob answers for a service that did not respond at all.
	{"unreachable", fmt.Errorf("no answer from the service: %w", syscall.EIO)},
	// A credential the service understood and refused. It is not EIO, so a guard written as
	// "is this the one failure errno" rather than "is this an answer about a name" lets it by.
	{"refused", fmt.Errorf("the credential was rejected: %w", syscall.EACCES)},
	// A request that outlived its deadline carries no errno at all.
	{"withdrawn", context.DeadlineExceeded},
	// An implementation reporting something the contract has no name for. It must still not
	// become an answer about a name, and it is the case a switch over known errnos drops.
	{"outside the vocabulary", errors.New("the object store said something with no errno in it")},
}

// factsAboutNames are the errnos that read as settled truth about a path. R-ERR-2 forbids an
// outcome nobody observed from arriving as one of them: a caller told ENOENT deletes, a
// caller told EEXIST renames out of the way, and neither is recoverable.
var factsAboutNames = []syscall.Errno{
	syscall.ENOENT, syscall.EEXIST, syscall.EISDIR, syscall.ENOTDIR, syscall.ENOTEMPTY,
	syscall.EINVAL,
}

// requireReported checks that an operation failed with the failure it actually had.
func requireReported(t *testing.T, err, injected error) {
	t.Helper()
	if err == nil {
		t.Fatal("the operation reported success although the object store refused it")
	}
	if !errors.Is(err, injected) {
		t.Errorf("the operation failed with %v, which does not carry the object store's own failure (%v)", err, injected)
	}
	for _, errno := range factsAboutNames {
		if errors.Is(err, errno) {
			t.Errorf("a failure of the object store arrived as %v: %v", errno, err)
		}
	}
}

// A tree that answers and an object store that does not is the split R-ERR-6 names. The file
// is there — the tree says so, and still says so afterwards — and its bytes are what could
// not be fetched.
func TestReadingThroughAFailingObjectStoreReportsTheFailure(t *testing.T) {
	for _, failure := range objectFailures {
		t.Run(failure.name, func(t *testing.T) {
			p := newParts(t, 0)
			namespace, objects := p.failing()
			ctx := t.Context()
			if err := namespace.Write(ctx, "f", []byte("content")); err != nil {
				t.Fatalf("write: %v", err)
			}

			objects.onGet = failure.err
			content, err := namespace.Read(ctx, "f")
			if err == nil {
				t.Fatalf("read answered %q through an object store that refused it", content)
			}
			requireReported(t, err, failure.err)

			// The tree is the half that still answers, and nothing about the failure has been
			// written back into it: the file is there and still says how long it is.
			if attr, err := namespace.Stat(ctx, "f"); err != nil || attr.Size == 0 {
				t.Fatalf("after the object store refused a read the file stats as %+v (%v), want it unchanged", attr, err)
			}
		})
	}
}

// Bytes that never reached the store must leave no name behind and no charge behind. The
// reservation the write made is not usage: it is a key nobody will reference, and a sweep
// reclaims it once the grace period for a write in flight has passed.
func TestAWriteWhoseBytesNeverLandedLeavesNothingBehind(t *testing.T) {
	const allowance = 1 << 20
	for _, failure := range objectFailures {
		t.Run(failure.name, func(t *testing.T) {
			p := newParts(t, allowance)
			namespace, objects := p.failing()
			ctx := t.Context()

			objects.onPut = failure.err
			requireReported(t, namespace.Write(ctx, "f", []byte("bytes that never arrive")), failure.err)

			if _, err := namespace.Stat(ctx, "f"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("after a write whose bytes never landed the file is %v, want ENOENT", err)
			}
			space, err := namespace.Space(ctx)
			if err != nil {
				t.Fatalf("space: %v", err)
			}
			if space.Used != 0 {
				t.Errorf("a write whose bytes never landed charged %d bytes", space.Used)
			}
		})
	}
}

// A sweep that cannot delete must keep the record of what it was going to delete. Forgetting
// an object whose bytes are still there loses the only thing that says they are being paid
// for, and nothing would ever offer them up again.
func TestASweepThatCannotDeleteForgetsNothing(t *testing.T) {
	for _, failure := range objectFailures {
		t.Run(failure.name, func(t *testing.T) {
			p := newParts(t, 0)
			namespace, objects := p.failing()
			ctx := t.Context()

			const files = 3
			for i := range files {
				name := fmt.Sprintf("f%d", i)
				if err := namespace.Write(ctx, name, []byte(name)); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			// The backlog is made through the tree directly, so that no write clears the
			// garbage the removal before it made.
			for i := range files {
				if err := p.meta.Remove(ctx, fmt.Sprintf("f%d", i)); err != nil {
					t.Fatalf("removing f%d from the tree: %v", i, err)
				}
			}

			objects.onDelete = failure.err
			removed, err := namespace.Sweep(ctx, 100)
			requireReported(t, err, failure.err)
			if removed != 0 {
				t.Errorf("a sweep whose deletes all failed reported %d objects removed", removed)
			}

			// Nothing was forgotten while its bytes were still being paid for: with the store
			// answering again, the same backlog is still there to be cleared.
			objects.onDelete = nil
			if removed, err = namespace.Sweep(ctx, 100); err != nil || removed != files {
				t.Fatalf("the sweep after the store recovered removed %d objects (%v), want %d", removed, err, files)
			}
		})
	}
}

// A mutation that succeeded stays succeeded when the sweep it triggers cannot reach the
// store. The name points where it should and the only casualty is unreferenced bytes still
// being paid for, which the record that named them offers up again on the next sweep.
func TestAMutationSurvivesASweepItCouldNotFinish(t *testing.T) {
	p := newParts(t, 0)
	namespace, objects := p.failing()
	ctx := t.Context()

	if err := namespace.Write(ctx, "f", []byte("the first contents")); err != nil {
		t.Fatalf("write: %v", err)
	}
	objects.onDelete = errors.New("the object store is not answering deletes")
	if err := namespace.Write(ctx, "f", []byte("the second contents")); err != nil {
		t.Fatalf("a write failed because the object it displaced could not be deleted: %v", err)
	}
	if content, err := namespace.Read(ctx, "f"); err != nil || string(content) != "the second contents" {
		t.Fatalf("the file reads as %q (%v), want the contents the write stored", content, err)
	}

	objects.onDelete = nil
	if removed, err := namespace.Sweep(ctx, 100); err != nil || removed != 1 {
		t.Fatalf("the sweep after the store recovered removed %d objects (%v), want the one the write displaced", removed, err)
	}
}
