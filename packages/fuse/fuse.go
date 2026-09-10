// Package fuse presents a remote volume as a Linux filesystem. Named tree
// operations use storage.Storage; open descriptors retain storage.File objects
// through rename and unlink. Reads capture current contents, and writes complete
// at the authority before the kernel receives success.
//
// Direct I/O and zero metadata timeouts keep ordinary read and pread independent
// of cached contents and lengths. Each mount renews a bounded file session and
// retires it after kernel teardown. Advisory locks use kernel owner identities;
// opening a file acquires neither advisory nor strong locks.
//
// The package changes no process-wide state. Diagnostics go only to the Logger
// supplied by the caller.
package fuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/storage"
)

// DefaultMaxFileSize bounds each file to one gibibyte. The object backend may
// materialize a whole revision while applying a range write.
const DefaultMaxFileSize = 1 << 30

// DefaultFlushTimeout bounds owner and reference cleanup when unspecified.
const DefaultFlushTimeout = 30 * time.Second

// Options configure a mount.
type Options struct {
	// Logger receives diagnostics from this package and the FUSE library.
	// A nil Logger discards them without selecting process output.
	Logger *log.Logger
	Debug  bool

	// MaxFileSize limits the size this mount opens or produces. Requests that
	// exceed it fail with EFBIG before allocating in proportion to that size.
	// Zero selects DefaultMaxFileSize; negative values are invalid.
	MaxFileSize int64

	// FlushTimeout bounds owner and reference cleanup after descriptor close,
	// session retirement, and mount setup. Cleanup ignores request cancellation
	// but preserves an earlier request deadline. It does not bound kernel
	// unmount or Mount.Wait. Zero selects DefaultFlushTimeout.
	FlushTimeout time.Duration

	// FileSession selects lifetime and resource bounds for this mount's dedicated
	// native session. Nil selects storage.DefaultFileSessionOptions. The supplied
	// value is copied; every explicitly supplied field must be valid. Its
	// MaxFileSize is replaced by the resolved mount ceiling above.
	FileSession *storage.FileSessionOptions
}

func (o Options) flushTimeout() (time.Duration, error) {
	if o.FlushTimeout < 0 {
		return 0, fmt.Errorf("fuse: FlushTimeout is %s; a cleanup timeout cannot be negative", o.FlushTimeout)
	}
	if o.FlushTimeout == 0 {
		return DefaultFlushTimeout, nil
	}
	return o.FlushTimeout, nil
}

// Mount owns one kernel connection and its dedicated file session.
type Mount struct {
	server interface{ Unmount() error }
	volume *volume
	done   chan struct{}
	err    error
}

// New mounts s at an existing directory and returns once kernel requests can
// be served. Every dependency must support storage.FileStorage. The mount owns
// its file session; the caller retains ownership of s. If setup fails after
// attachment and rollback cannot detach the kernel, New returns both a Mount
// and an error. The caller must retain s and finish unmounting that Mount.
func New(mountpoint string, s storage.Storage, opts Options) (*Mount, error) {
	// fusermount attaches its diagnostics to process stderr, so reject local
	// mountpoint errors before invoking it.
	info, err := os.Stat(mountpoint)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &os.PathError{Op: "mount", Path: mountpoint, Err: syscall.ENOTDIR}
	}
	if opts.MaxFileSize < 0 {
		return nil, fmt.Errorf("fuse: MaxFileSize is %d; a file size ceiling cannot be negative", opts.MaxFileSize)
	}
	if _, err := opts.flushTimeout(); err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	v, err := newVolume(context.Background(), s, opts, logger)
	if err != nil {
		return nil, err
	}
	v.owner = gofuse.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}
	ask, cancel := context.WithTimeout(context.Background(), v.flushTimeout)
	attr, err := s.Stat(ask, "")
	cancel()
	if err == nil && (attr.ID == 0 || !attr.Mode.IsDir()) {
		err = syscall.EIO
	}
	if err != nil {
		return nil, errors.Join(err, v.stopSession())
	}
	root := &node{volume: v, id: rootIdentity(attr.ID)}
	never := time.Duration(0)
	options := &fs.Options{
		EntryTimeout: &never, AttrTimeout: &never, NegativeTimeout: &never,
		Logger:         logger,
		RootStableAttr: &fs.StableAttr{Mode: syscall.S_IFDIR, Ino: root.id.ino},
		// Zero permission bits are a valid file mode, not an omitted default.
		NullPermissions: true,
		MountOptions: gofuse.MountOptions{
			FsName: "remote-fs", Name: "remote-fs", Logger: logger, Debug: opts.Debug,
			ExtraCapabilities: gofuse.CAP_ATOMIC_O_TRUNC,
			DisableXAttrs:     true, EnableLocks: true,
		},
	}
	raw := newRawFilesystem(fs.NewNodeFS(root, options), v)
	server, err := gofuse.NewServer(raw, mountpoint, &options.MountOptions)
	if err != nil {
		return nil, errors.Join(err, v.stopSession())
	}
	m := &Mount{server: server, volume: v, done: make(chan struct{})}
	go func() {
		server.Serve()
		m.err = v.stopSession()
		close(m.done)
	}()
	return m.ready(server.WaitMount())
}

func (m *Mount) ready(handshakeErr error) (*Mount, error) {
	if handshakeErr == nil {
		return m, nil
	}
	// WaitMount's poll probe can fail while Serve is still live. Detachment
	// must precede waiting for teardown; a failed rollback retains ownership.
	cleanupErr := m.Unmount()
	select {
	case <-m.done:
		return nil, errors.Join(handshakeErr, cleanupErr, m.Wait())
	default:
		return m, errors.Join(handshakeErr, cleanupErr)
	}
}

// Unmount detaches the kernel connection and waits for session retirement. A
// busy kernel connection leaves the mount and its renewals alive. A cleanup error after
// successful detachment is also returned; Done distinguishes these outcomes.
func (m *Mount) Unmount() error {
	if err := m.server.Unmount(); err != nil {
		return err
	}
	return m.Wait()
}

// Done closes after kernel teardown and the native session cleanup attempt.
// A closed channel does not imply cleanup succeeded; Wait returns its result.
func (m *Mount) Done() <-chan struct{} { return m.done }

// Wait waits for kernel teardown and session retirement, including when the
// kernel ends the connection externally. Unknown cleanup returns an error.
func (m *Mount) Wait() error {
	<-m.done
	return m.err
}

func errnoOf(err error) syscall.Errno { return storage.ErrnoOf(err) }
