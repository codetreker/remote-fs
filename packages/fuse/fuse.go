// Package fuse presents one storage.Storage as an ordinary directory tree on the local
// machine, over FUSE. Linux only (R-INT-8).
//
// FUSE ends here. Nothing above or beside this package speaks the protocol, and nothing
// below it is shaped by the kernel's demands: storage.Storage is a namespace addressed by
// path, and its consumers are this package and whoever else calls it. Everything the
// kernel requires and the namespace does not offer — a number identifying each node, an
// open file's contents, a mount's lifetime — is built and kept in this package.
//
// Nothing is cached. The kernel's entry and attribute timeouts are zero, so every call
// the kernel makes becomes a call on the storage, and every answer describes the
// namespace as it is at that moment. That is why this mount needs no change
// notification: every attribute and every byte it reports is read at the moment it is
// asked for.
//
// The one thing that is held locally is the contents of an open file, because
// storage.Read and storage.Write deal in whole files. Each open handle reads the file
// once and serves the kernel's requests out of that buffer; writes patch the buffer and
// are committed on close. Options.MaxFileSize bounds that buffer, and every operation
// that would grow one past it fails with EFBIG instead.
//
// One figure is held besides: what the namespace last said about the room left in it.
// Every change that would make a workspace hold more is weighed against it, so that one at
// its limit refuses at the call that asked — the write(2), the ftruncate(2) — rather than
// at the close(2) where the commit would discover it, with EDQUOT. That figure goes stale
// between refreshes by design, and nothing is settled on it — the commit is where the
// namespace itself refuses, and its answer is the one that stands.
//
// Linked into somebody else's process this package touches nothing of that process
// (R-INT-2): no signal handlers, no writes to its output, no exiting it, no work at
// import time. Diagnostics go to the Logger the caller supplies, or nowhere.
package fuse

import (
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

// DefaultMaxFileSize is the ceiling a zero Options gets: one gibibyte.
//
// The number is a judgement about what a mount can be asked to hold at once rather than
// about any one file. An open file's contents are held whole, the commit hands the same
// bytes to the storage, and several files can be open together, so the ceiling has to
// leave room for a small multiple of itself in a process that has other work to do. A
// gibibyte is far above the source files and documents a shared workspace holds, and low
// enough that a mount cannot be talked into exhausting an ordinary machine.
const DefaultMaxFileSize = 1 << 30

// DefaultFlushTimeout bounds the close commit attempt when Options.FlushTimeout is zero.
const DefaultFlushTimeout = 30 * time.Second

// Options configure a mount.
type Options struct {
	// Logger receives diagnostics, from this package and from the FUSE library beneath
	// it. A nil Logger discards them; there is deliberately no default destination,
	// because the only ones available would be the caller's own output.
	Logger *log.Logger

	// Debug adds a trace of every kernel request and reply to Logger.
	Debug bool

	// MaxFileSize is the largest file, in bytes, this mount will hold. A request to
	// exceed it fails with EFBIG, and a file the namespace reports as larger cannot be
	// opened. It is the configurable ceiling R-INT-3 requires on the one resource this
	// package accumulates.
	//
	// The ceiling exists because an open file's contents are held whole in memory, so
	// one caller's `truncate -s 1T` would otherwise ask the runtime for a terabyte. Go
	// answers an allocation it cannot satisfy with a fatal error rather than a panic:
	// neither recover nor the FUSE library's panic handling can catch it, and the
	// process this package is linked into dies with it (R-INT-1, R-INT-2).
	//
	// Zero means DefaultMaxFileSize. There is deliberately no value meaning "no ceiling":
	// a caller who needs a larger one names it, which is a decision, where an omission
	// would be an oversight.
	MaxFileSize int64

	// FlushTimeout bounds the storage context for one close commit attempt. Request
	// cancellation does not cancel that attempt; an earlier request deadline still
	// applies. Waiting for the handle lock consumes this budget but cannot itself be
	// interrupted, and this option does not bound Mount.Wait or Mount.Unmount.
	// Zero means DefaultFlushTimeout; negative values are invalid.
	FlushTimeout time.Duration
}

func (o Options) flushTimeout() (time.Duration, error) {
	if o.FlushTimeout < 0 {
		return 0, fmt.Errorf("fuse: FlushTimeout is %s; a close commit timeout cannot be negative", o.FlushTimeout)
	}
	if o.FlushTimeout == 0 {
		return DefaultFlushTimeout, nil
	}
	return o.FlushTimeout, nil
}

// Mount is one namespace presented at one mountpoint.
type Mount struct {
	server *gofuse.Server
}

// New mounts s at mountpoint, which must be an existing directory, and returns once the
// filesystem is answering kernel calls.
func New(mountpoint string, s storage.Storage, opts Options) (*Mount, error) {
	// The mountpoint is checked here rather than left to fusermount, which the FUSE
	// library runs with this process's own standard error attached and which would
	// therefore print its complaint into the caller's output (R-INT-2).
	info, err := os.Stat(mountpoint)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &os.PathError{Op: "mount", Path: mountpoint, Err: syscall.ENOTDIR}
	}
	if opts.MaxFileSize < 0 {
		return nil, fmt.Errorf("fuse: MaxFileSize is %d; a ceiling on a file's size cannot be negative",
			opts.MaxFileSize)
	}
	maxFileSize := opts.MaxFileSize
	if maxFileSize == 0 {
		maxFileSize = DefaultMaxFileSize
	}
	flushTimeout, err := opts.flushTimeout()
	if err != nil {
		return nil, err
	}

	logger := opts.Logger
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}

	root := &node{
		ns: &namespace{
			storage:      s,
			owner:        gofuse.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())},
			maxFileSize:  maxFileSize,
			flushTimeout: flushTimeout,
		},
		id: rootIdentity(),
	}

	// Zero everywhere: the kernel may not hold on to an entry, an attribute, or the
	// absence of a name for any length of time.
	never := time.Duration(0)
	options := &fs.Options{
		EntryTimeout:    &never,
		AttrTimeout:     &never,
		NegativeTimeout: &never,
		Logger:          logger,
		// The FUSE library takes the root's number from here and would otherwise report
		// it as zero, the number a filesystem uses to say a node has no identity. The
		// root is a node like any other in the tree this mount identifies.
		RootStableAttr: &fs.StableAttr{Mode: syscall.S_IFDIR, Ino: root.id.ino},
		// Without this the FUSE library replaces a mode with no permission bits in it by
		// 0644, or 0755 for a directory, on the way to the kernel. A file the namespace
		// holds at 0000 is a file somebody set to 0000, and reporting it as readable is a
		// fact this mount made up (R-ERR-2) — one that a program checking whether it may
		// read something would act on.
		NullPermissions: true,
		MountOptions: gofuse.MountOptions{
			FsName: "remote-fs",
			Name:   "remote-fs",
			Logger: logger,
			Debug:  opts.Debug,
			// With this negotiated the kernel carries O_TRUNC into Open, so opening a
			// file in order to replace it does not first fetch the contents that are
			// about to be discarded. Without it the kernel sends a separate truncation
			// instead, which Setattr handles; both paths have to work, because whether
			// it is negotiated is the kernel's decision.
			ExtraCapabilities: gofuse.CAP_ATOMIC_O_TRUNC,
			// The namespace has no extended attributes. Left to the FUSE library the
			// answer would be "this file has no such attribute", which is a claim about
			// the file; this makes the answer "this filesystem has none", after which
			// the kernel stops asking.
			DisableXAttrs: true,
		},
	}

	server, err := fs.Mount(mountpoint, root, options)
	if err != nil {
		return nil, err
	}
	return &Mount{server: server}, nil
}

// Unmount detaches the mountpoint. It fails with EBUSY while anything still holds a file
// or a working directory inside it.
func (m *Mount) Unmount() error { return m.server.Unmount() }

// Wait returns once the filesystem has stopped serving, whether because it was unmounted
// or because the kernel tore the connection down.
func (m *Mount) Wait() { m.server.Wait() }

// errnoOf converts once at a FUSE callback boundary; internal operations retain their causes.
func errnoOf(err error) syscall.Errno { return storage.ErrnoOf(err) }
