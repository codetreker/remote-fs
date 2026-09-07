// Package localdir implements storage.Storage over one directory on the local
// filesystem.
//
// It is both the implementation the reference server ships with and the one the mount is
// exercised against. Because the mount knows nothing but storage.Storage, mounting this
// produces a filesystem with no network anywhere in it, which can then be compared
// against a plain directory operation by operation.
package localdir

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Storage is a namespace held in one directory.
type Storage struct {
	root            string
	state           *directoryState
	authority       *locking.Authority
	runtime         *nativeRuntime
	ops             operationSyscalls
	lifecycle       sync.Mutex
	resources       sync.Mutex
	stagedBytes     int64
	snapshotBytes   int64
	snapshotEntries int
	failure         error
	handleFailure   error
	closed          bool
	closeErr        error
}

var _ storage.Storage = (*Storage)(nil)
var _ storage.BoundedStorage = (*Storage)(nil)

// New exclusively owns an unbound namespace rooted at dir until Close.
// The root must already be a directory; a permanently bound root requires Open.
// Content staging uses O_TMPFILE, and pinned descriptors are accessed through procfs.
func New(dir string) (*Storage, error) {
	limits, err := (Limits{}).Effective()
	if err != nil {
		return nil, err
	}
	state, err := openRawState(dir, limits)
	if err != nil {
		return nil, err
	}
	s := &Storage{root: state.rootPath, state: state, runtime: newNativeRuntime(state.limits), ops: defaultOperationSyscalls()}
	if err := s.probeOperations(context.Background()); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	return s, nil
}

// Open owns a previously initialized namespace until Close drains its handles.
func Open(ctx context.Context, config Config) (*Storage, error) {
	state, err := openBoundState(ctx, config)
	if err != nil {
		return nil, err
	}
	s := &Storage{root: state.rootPath, state: state, runtime: newNativeRuntime(state.limits), ops: defaultOperationSyscalls()}
	if err := s.probeOperations(ctx); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	s.authority, err = locking.New(ctx, config.Locks, s, state)
	if err != nil {
		return nil, errors.Join(err, s.Close())
	}
	return s, nil
}

// LockService returns the authority paired with this namespace, or nil for New.
func (s *Storage) LockService() locking.Service {
	if s.authority == nil {
		return nil
	}
	return s.authority
}

// CheckPublicationAccounting reports native final-transition accounting support.
func (s *Storage) CheckPublicationAccounting() error { return nil }

// Close stops admission and releases authority pins before directory ownership.
func (s *Storage) Close() error {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.runtime.stop()
	if s.authority != nil {
		s.closeErr = s.authority.Close()
	}
	s.runtime.wait()
	pinErr := s.runtime.closePins()
	s.closeErr = errors.Join(s.closeErr, pinErr)
	s.resources.Lock()
	s.closeErr = errors.Join(s.closeErr, s.handleFailure)
	if s.authority == nil {
		s.closeErr = errors.Join(s.closeErr, s.failure)
	}
	s.resources.Unlock()
	if pinErr == nil && s.handleFailure == nil {
		s.closeErr = errors.Join(s.closeErr, s.state.Close())
	}
	return s.closeErr
}

func readLocalBounded(ctx context.Context, from io.Reader, maxBytes int64) ([]byte, error) {
	reader := &contextReader{ctx: ctx, from: from}
	content, err := io.ReadAll(io.LimitReader(reader, maxBytes))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) < maxBytes {
		return content, nil
	}
	var excess [1]byte
	n, err := io.ReadFull(reader, excess[:])
	if n != 0 {
		return nil, syscall.EFBIG
	}
	if errors.Is(err, io.EOF) {
		return content, nil
	}
	return nil, err
}

type contextReader struct {
	ctx  context.Context
	from io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := localContextError(r.ctx, "read", ""); err != nil {
		return 0, err
	}
	return r.from.Read(p)
}

func localContextError(ctx context.Context, op, path string) error {
	if err := ctx.Err(); err != nil {
		return &os.PathError{Op: op, Path: path, Err: err}
	}
	return nil
}

// spaceOf renders what statfs(2) reported. It is kept apart from the call that obtained
// st because the answers it refuses are ones a working kernel does not give, so they can
// be put to it here and nowhere else.
func spaceOf(st unix.Statfs_t) (storage.Space, error) {
	// A filesystem with no block size, with more free blocks than it has, or with more
	// available than free, describes a state that cannot be true. Deriving figures from
	// it anyway would put either a filesystem of no size or a Used that wrapped into an
	// enormous one in front of a caller as measured fact, so an answer like this is a
	// failure to report rather than a set of figures to repair (R-ERR-2).
	blockSize := int64(st.Bsize)
	if blockSize <= 0 {
		return storage.Space{}, fmt.Errorf("statfs reports a block size of %d bytes: %w", blockSize, syscall.EIO)
	}
	if st.Bfree > st.Blocks || st.Bavail > st.Bfree {
		return storage.Space{}, fmt.Errorf("statfs reports %d blocks of which %d are free and %d are available, which cannot all be true: %w",
			st.Blocks, st.Bfree, st.Bavail, syscall.EIO)
	}

	// EOVERFLOW is the errno the kernel itself gives for a figure that does not fit, and
	// no filesystem is that large today — an int64 of bytes runs to eight exbibytes. The
	// product is checked rather than assumed because getting it wrong is silent: one that
	// wrapped would come back as a small or a negative figure with nothing marking it
	// wrong. Saturating at the maximum instead would describe a filesystem nobody has.
	high, low := bits.Mul64(st.Blocks, uint64(blockSize))
	if high != 0 || low > math.MaxInt64 {
		return storage.Space{}, fmt.Errorf("statfs reports %d blocks of %d bytes each, which is more than a byte count holds: %w",
			st.Blocks, blockSize, syscall.EOVERFLOW)
	}
	// The other two count fewer blocks than the total does, at the same block size, so
	// the product checked above settles them as well.
	return storage.Space{
		Total: int64(low),
		Used:  int64(st.Blocks-st.Bfree) * blockSize,
		Avail: int64(st.Bavail) * blockSize,
	}, nil
}

// attrOf reports host metadata. Lease identity is maintained separately by the
// native pin map and survives this backend's atomic content replacement.
func attrOf(info *nativeInfo) storage.Attr {
	system := info.stat
	// The host filesystem's inode number is this backend's node identity. It is unique
	// among the nodes alive at one moment on one filesystem, and it follows a node through
	// a rename, which is the clause of R-FS-5 this backend meets.
	//
	// It is not never-reused: the host hands a number back as soon as the node holding it
	// is gone, so a name removed and created again can come back with the number that just
	// left. Nothing above repairs that. The mount mints its own numbers and never reuses
	// one, but a number already given to the kernel then names two nodes in turn, and the
	// comparison this identity exists for does not fire — measured on ext4, a name removed
	// and recreated keeps its number on another mount every time. R-FS-5's third clause is
	// therefore not met by a namespace over a host filesystem, and a descriptor still open
	// on the node that left is the case it costs.
	return storage.Attr{
		ID:         system.Ino,
		Mode:       info.Mode(),
		Size:       info.Size(),
		AccessTime: time.Unix(system.Atim.Unix()),
		ModTime:    info.ModTime(),
	}
}
