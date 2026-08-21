// The tests in this file are inside the package because what they put to it are answers
// no kernel gives: statfs figures that contradict one another, and a filesystem larger
// than a byte count can describe. Neither can be provoked through a real statfs, and both
// are the difference between reporting a failure and reporting a fabricated number.
package localdir

import (
	"errors"
	"math"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestSpaceRefusesFiguresThatCannotAllBeTrue(t *testing.T) {
	for _, st := range []unix.Statfs_t{
		{Bsize: 0, Blocks: 100, Bfree: 50, Bavail: 50},
		{Bsize: -4096, Blocks: 100, Bfree: 50, Bavail: 50},
		{Bsize: 4096, Blocks: 100, Bfree: 101, Bavail: 0},
		{Bsize: 4096, Blocks: 100, Bfree: 50, Bavail: 51},
	} {
		space, err := spaceOf(st)
		if !errors.Is(err, syscall.EIO) {
			t.Errorf("a block size of %d with %d blocks, %d free and %d available gave %+v with error %v, want EIO",
				st.Bsize, st.Blocks, st.Bfree, st.Bavail, space, err)
		}
	}
}

// Both ends of the product: one filesystem whose size exceeds what a byte count holds,
// and one whose size exceeds what the multiplication itself holds.
func TestSpaceRefusesAFilesystemTooLargeToDescribeInBytes(t *testing.T) {
	for _, blocks := range []uint64{math.MaxInt64/4096 + 1, math.MaxUint64} {
		st := unix.Statfs_t{Bsize: 4096, Blocks: blocks}
		space, err := spaceOf(st)
		if !errors.Is(err, syscall.EOVERFLOW) {
			t.Errorf("%d blocks of 4096 bytes gave %+v with error %v, want EOVERFLOW", blocks, space, err)
		}
	}
}

// The largest filesystem that can be described is described rather than refused, which an
// overflow check written one block too tight would get wrong.
func TestSpaceDescribesTheLargestFilesystemThatFits(t *testing.T) {
	const blocks = math.MaxInt64 / 4096
	space, err := spaceOf(unix.Statfs_t{Bsize: 4096, Blocks: blocks, Bfree: blocks, Bavail: blocks})
	if err != nil {
		t.Fatalf("%d blocks of 4096 bytes: %v", blocks, err)
	}
	want := storage.Space{Total: blocks * 4096, Used: 0, Avail: blocks * 4096}
	if space != want {
		t.Fatalf("space is %+v, want %+v", space, want)
	}
}

// The reserve only the superuser may spend is free without being available, and it is the
// whole reason the contract carries three figures rather than two. A real statfs on the
// machine the tests run on need not report one, so the case is put to the rendering
// directly.
func TestSpaceReportsTheSuperuserReserveAsFreeButNotAvailable(t *testing.T) {
	space, err := spaceOf(unix.Statfs_t{Bsize: 4096, Blocks: 100, Bfree: 30, Bavail: 20})
	if err != nil {
		t.Fatal(err)
	}
	want := storage.Space{Total: 100 * 4096, Used: 70 * 4096, Avail: 20 * 4096}
	if space != want {
		t.Fatalf("space is %+v, want %+v — the ten blocks of reserve are free and not available", space, want)
	}
	if !space.Coherent() {
		t.Fatalf("space %+v is not coherent", space)
	}
}
