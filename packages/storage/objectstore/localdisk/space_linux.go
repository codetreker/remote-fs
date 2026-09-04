package localdisk

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage"
)

// Space reports figures derived from statfs. Available subtracts the maintenance reserve,
// active publication reservations, and the largest envelope/key overhead. Filesystem
// allocation granularity and concurrent activity remain authoritative at Put time.
func (o *Objects) Space(ctx context.Context) (storage.Space, error) {
	if err := checkContext(ctx, "stat object-store space", ""); err != nil {
		return storage.Space{}, err
	}
	ticket, err := o.gate.acquire(ctx, 0)
	if err != nil {
		return storage.Space{}, fmt.Errorf("stat object-store space: %w", err)
	}
	defer ticket.release()
	if err := o.health.failure(); err != nil {
		return storage.Space{}, err
	}
	return o.spaceWhileAdmitted()
}

func (o *Objects) spaceWhileAdmitted() (storage.Space, error) {
	var st unix.Statfs_t
	if err := o.ops.fstatfs(o.rootFD, &st); err != nil {
		return storage.Space{}, &os.PathError{Op: "statfs object-store root", Path: o.rootPath, Err: err}
	}
	space, err := physicalSpace(st)
	if err != nil {
		return storage.Space{}, fmt.Errorf("the filesystem holding %s: %w", o.rootPath, err)
	}
	if space.Avail <= o.limits.maintenanceReserveBytes {
		space.Avail = 0
	} else {
		space.Avail -= o.limits.maintenanceReserveBytes
	}
	pending := o.capacity.pending()
	if space.Avail <= pending {
		space.Avail = 0
	} else {
		space.Avail -= pending
	}
	overhead := int64(fixedEnvelopeBytes + MaxKeyBytes)
	if space.Avail <= overhead {
		space.Avail = 0
	} else {
		space.Avail -= overhead
	}
	// A key whose lazy shard is absent needs the directory and its identity file, then a
	// recovery record and the staged/final object inode.
	if st.Files > 0 && st.Ffree < 4 {
		space.Avail = 0
	}
	return space, nil
}

func physicalSpace(st unix.Statfs_t) (storage.Space, error) {
	blockSize := int64(st.Bsize)
	if blockSize <= 0 {
		return storage.Space{}, fmt.Errorf("statfs reports a block size of %d bytes: %w", blockSize, syscall.EIO)
	}
	if st.Bfree > st.Blocks || st.Bavail > st.Bfree {
		return storage.Space{}, fmt.Errorf("statfs reports %d blocks, %d free and %d available: %w",
			st.Blocks, st.Bfree, st.Bavail, syscall.EIO)
	}
	if st.Files > 0 && st.Ffree > st.Files {
		return storage.Space{}, fmt.Errorf("statfs reports %d inodes of which %d are free: %w",
			st.Files, st.Ffree, syscall.EIO)
	}
	high, total := bits.Mul64(st.Blocks, uint64(blockSize))
	if high != 0 || total > math.MaxInt64 {
		return storage.Space{}, fmt.Errorf("statfs byte count overflows int64: %w", syscall.EOVERFLOW)
	}
	return storage.Space{
		Total: int64(total),
		Used:  int64(st.Blocks-st.Bfree) * blockSize,
		Avail: int64(st.Bavail) * blockSize,
	}, nil
}
