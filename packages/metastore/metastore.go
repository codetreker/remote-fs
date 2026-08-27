// Package metastore defines the contract for the part of a namespace that is not bytes:
// the tree of names, what kind of node each name holds, the attributes it carries, and —
// for a file — which stored object holds its contents.
//
// It exists because an object store is not a filesystem. A container of blobs has no
// directories, no permission bits, no access times, and no rename; it has a flat set of
// keys and the bytes under them. Everything else a filesystem is expected to have must be
// kept somewhere, and keeping it beside the bytes — one blob's worth of metadata attached
// to that blob — makes every one of those operations a separate network request against a
// service that offers no way to change two things at once. A database offers exactly that,
// so the split here is between what an object store is good at (holding bytes durably and
// cheaply) and what a database is good at (answering questions about a tree, and changing
// several rows or none).
//
// One Store is one namespace. A database may hold many, and an implementation is free to
// separate them however it likes; nothing above this interface names a namespace, because
// one namespace is one workspace and the storage contract carries no workspace parameter.
//
// The interface is deliberately not a way to run SQL. Every operation here is a complete
// tree operation with a defined outcome, so that an implementation may make it atomic by
// whatever means its engine offers — a transaction, a compare-and-swap, a stored procedure.
// An interface that exposed transactions would bind every implementation to the one engine
// whose transactions the caller had in mind.
package metastore

import (
	"context"
	"io/fs"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// Store is the tree of one namespace.
//
// Paths arrive here already cleaned by storage.CleanPath: slash-separated, no leading
// slash, the root is the empty string. An implementation does not re-derive that.
//
// Errors are syscall.Errno values reachable with errors.Is, from the same closed set the
// storage contract names. The reason is the same one that applies there, and it applies
// with more force here: this layer decides most of them itself. A local directory gets
// ENOTDIR for a component that is a file, ENOENT for a missing parent and ENOTEMPTY for a
// directory with entries out of the kernel's own path resolution, for free and correctly.
// A database has no kernel to defer to, so each of those is a query and a branch that
// somebody wrote, and a wrong one is not a crash but a lie.
//
// Nothing here reads or writes object contents. A Store records that a file's contents are
// under some key; moving the bytes to and from that key belongs to the caller. Keeping the
// two apart is what lets the crash window be reasoned about at all: the caller writes bytes
// and then asks a Store to point at them, and the Store's answer is the only thing that
// decides whether the write happened.
type Store interface {
	// Stat reports the node at path.
	Stat(ctx context.Context, path string) (Node, error)

	// List returns the children of the directory at path, sorted by name in byte order.
	//
	// Byte order rather than any collation: a name on Linux is an arbitrary byte sequence,
	// two names differing only in case are two different names, and a listing ordered by a
	// locale's rules would put entries in an order no other implementation of this system
	// agrees with. An engine whose default collation is anything else must say so in its
	// schema rather than rely on the default.
	List(ctx context.Context, path string) ([]Child, error)

	// SetAttr applies change to the node at path, leaving unnamed attributes as they are.
	SetAttr(ctx context.Context, path string, change storage.AttrChange) error

	// Create records an empty file at path, failing with syscall.EEXIST if anything is
	// already there. An empty file references no object: zero bytes are worth no round trip
	// to an object store, and a file that has never been written has nothing to point at.
	Create(ctx context.Context, path string) error

	// Mkdir records a directory at path.
	Mkdir(ctx context.Context, path string) error

	// Remove removes the file at path. Removing a directory is syscall.EISDIR. The object
	// the file referenced, if any, becomes garbage: it is recorded as such rather than
	// deleted, because this interface does not reach the object store.
	Remove(ctx context.Context, path string) error

	// RemoveDir removes the empty directory at path. A file is syscall.ENOTDIR, a directory
	// with entries is syscall.ENOTEMPTY, and the root is syscall.EBUSY.
	RemoveDir(ctx context.Context, path string) error

	// Rename moves the node at from to to, replacing an existing file there. Naming the root
	// as either operand is syscall.EBUSY. An object displaced by the move becomes garbage.
	//
	// Renaming a directory moves everything beneath it and must not cost more than moving
	// the directory itself. That is a property of the shape the tree is stored in, not of
	// this interface, but it is stated here because the shape that fails it — a row keyed by
	// the whole path — is the one an implementer reaches for first.
	Rename(ctx context.Context, from, to string) error

	// Reserve records the intent to write path's new contents and returns the key to write
	// them under. The key is opaque: nothing may derive it from the path, and nothing may
	// reuse it after the object it names is gone.
	//
	// It is called before the bytes are written, and that order is the whole point. An
	// object written under a key no committed record mentions is indistinguishable from an
	// object some other writer is about to commit, and a sweeper that cannot tell those
	// apart either deletes live data or waits out a grace period long enough to make its own
	// correctness a guess about how slow a write can be. A key that is on record before its
	// bytes exist is never ambiguous.
	//
	// Knowing the write it is for, a reservation refuses what the commit would refuse
	// anyway: a path whose parent directory is not there, a path holding a directory, and a
	// size the allowance has no room for. Those refusals are not the authority — Commit is,
	// because the namespace may change between the two — but making them here is what keeps
	// a write that cannot land from paying to upload its bytes first. Without it, a caller
	// sitting at its allowance can put an unbounded number of objects into the store at full
	// speed, each one billed and each one waiting on a sweep, which is the cost the
	// allowance exists to bound.
	Reserve(ctx context.Context, path string, size int64) (Key, error)

	// Commit points path at an object that has been written, creating the file if it is not
	// there, and accounts for the bytes. It fails with syscall.EDQUOT if the namespace has
	// an allowance and the new size would carry it past it — checked here, in the same
	// change that records the size, so that no window exists between deciding there is room
	// and taking it.
	//
	// The object previously at path, if any, becomes garbage. The object being committed
	// must still be reserved; a key that was never reserved, has already been committed, or
	// has been handed to a sweeper is syscall.EINVAL. That last case is why the state is
	// checked rather than merely the existence of a record: a write slow enough to have its
	// reservation swept must fail here, because its bytes are gone.
	//
	// An empty key commits a file with no contents at all, which is what a zero-byte write
	// is: no object is worth a round trip to hold no bytes, so nothing is reserved for one
	// and there is nothing to check. An empty key with a non-zero size is syscall.EINVAL.
	Commit(ctx context.Context, path string, object Object) error

	// Space reports the room the namespace has, or syscall.ENOSYS if it has no allowance.
	// A namespace's answer does not change over its life: one that answers answers always.
	//
	// Used is exact and costs no traversal, because it is maintained by the same changes
	// that move bytes in and out. Nothing here samples a filesystem: an object store has no
	// capacity to report, so a namespace held in one has no figures at all beyond the
	// allowance it was given and the bytes it is known to hold.
	Space(ctx context.Context) (storage.Space, error)

	// Garbage returns at most limit objects that nothing references, so that a caller may
	// delete them from the object store. Objects reserved but never committed are included
	// once they are older than grace, which bounds how long a write may take between
	// reserving a key and committing it.
	//
	// A key this returns is no longer committable, and it stops being committable in the
	// same change that hands it out. The caller is about to delete the bytes, so a write
	// still holding that key must fail rather than succeed onto an object that is gone —
	// and a caller that took the keys and then died has lost nothing, because a key nobody
	// can commit is garbage whether or not its bytes were reached.
	Garbage(ctx context.Context, limit int, grace time.Duration) ([]Key, error)

	// Forget drops the records of objects whose bytes are gone. A key passed here that is
	// still referenced is syscall.EINVAL: it would leave a name pointing at nothing.
	Forget(ctx context.Context, keys []Key) error

	// Close releases whatever the Store holds open.
	Close() error

	// Log is what a mount replicates from. Every Store provides it, because a position has
	// to be allocated inside the same change that applies the tree edit, and only the thing
	// that owns that change can do it.
	Log
}

// Key names one stored object. It is opaque to everything above the store that allocated
// it, and it is never reused: R-INT-11 turns on that, because a reused key lets something
// still holding the old one read bytes that belong to somebody else.
type Key string

// Node is one node of the tree.
type Node struct {
	// ID identifies the node itself rather than the name it currently has. It survives a
	// rename and is never reused after the node is gone.
	//
	// Nothing above uses it yet — the storage contract addresses nodes by path. It is here
	// because a tree stored as parent-and-name has it for free, and because the shape that
	// would not have it is the one that makes it expensive to add later.
	ID int64

	// Mode carries the type bits and the permission bits, in io/fs's layout rather than a
	// kernel's. The type bits say what kind of node this is.
	Mode fs.FileMode

	// Size is the length of a file's contents. It is zero for a directory, which the storage
	// contract leaves unspecified.
	Size int64

	AccessTime time.Time
	ModTime    time.Time

	// Content is the object holding this file's bytes, empty for a directory and for a file
	// that has never been written.
	Content Key
}

// IsDir reports whether the node is a directory.
func (n Node) IsDir() bool { return n.Mode.IsDir() }

// Attr renders the node as the storage contract describes it.
func (n Node) Attr() storage.Attr {
	return storage.Attr{Mode: n.Mode, Size: n.Size, AccessTime: n.AccessTime, ModTime: n.ModTime}
}

// Child is one member of a directory listing. The name is a byte sequence rather than a
// string because that is what a name is: encoding/json turns an invalid UTF-8 byte into
// U+FFFD, and a filesystem that renames a file by reading it back has lost it.
type Child struct {
	Name []byte
	Node Node
}

// Object describes bytes that have been written and are ready to be pointed at.
type Object struct {
	// Key is what Reserve returned for these bytes.
	Key Key

	// Size is their length.
	Size int64

	// Digest is what the object store reported for the bytes it stored, or nil if it
	// reported nothing. It is recorded so that identical content can be recognised later
	// without reading every object back.
	//
	// It is not a version. A digest cannot tell two objects with identical contents apart,
	// so using it to decide whether a file is still the one a caller read would answer
	// "these are the same bytes" to a question that asked "is this the same object".
	Digest []byte

	// ModTime is the instant the contents are recorded as having changed.
	ModTime time.Time
}

// --- the change log ------------------------------------------------------------------

// Log is what makes a namespace replicable: an ordered record of what changed, plus a way
// to take a consistent picture of the tree to start from.
//
// It is part of the Store contract rather than a capability beside it because the two
// cannot be separated safely. A position has to be allocated in the same atomic change
// that applies the tree edit; an implementation that appended to a log afterwards would
// leave a window in which a change is committed and its event is not, and once the log
// outlives the process that window stops healing itself. Only the thing that owns the
// transaction can close it.
//
// Nothing here writes. Recording a change is a side effect of the tree operation that
// caused it, so there is no Append: an implementation that let a caller append would let
// the log say something the tree does not.
type Log interface {
	// Snapshot opens a consistent picture of the whole tree and reports the position it is
	// taken at. Every row it yields reflects the same instant, and no change later than
	// that position is in it.
	//
	// A caller subscribes first and takes a snapshot second. The reverse order does not
	// converge: the scan takes time, changes accumulate while it runs, and if enough of
	// them accumulate to push the snapshot's position out of the retention window the
	// caller must start over — with a cost proportional to the size of the tree, so the
	// larger the namespace the less likely it is to ever finish. Subscribing first takes
	// the window off that path entirely.
	//
	// Consistency is required of the picture, not of the mechanism: a read transaction, a
	// multi-version read, or a lock held only long enough to take a cheap reference all
	// satisfy it. What an implementation may not do is hold a lock for the whole delivery,
	// because the rows are streamed to somewhere far away and the delivery is as slow as
	// the network.
	Snapshot(ctx context.Context) (Snap, Position, error)

	// Since returns at most limit changes recorded after the given position, oldest first,
	// along with what the log still holds.
	//
	// A caller compares the two to learn which of three things happened, because the
	// answers call for different actions and an implementation that could not tell them
	// apart would answer the worst of them silently:
	//
	//   after == Tail        — caught up; nothing was missed
	//   after >= Oldest-1    — resumable; the changes are returned
	//   otherwise            — the log no longer holds them, and the caller must rebuild
	Since(ctx context.Context, after Position, limit int) ([]Change, Retention, error)

	// Incarnation identifies this log as a continuation of itself.
	//
	// A caller resumes with the pair (incarnation, position); a position alone is not
	// enough. A log that lost its history — a fresh in-memory one after a restart, or one
	// whose tail did not survive a crash — would otherwise be asked to resume at a position
	// it has never heard of, and the reasonable-looking answer, "that is within my window,
	// you are caught up", loses every change in between with nothing left to notice it by.
	//
	// It changes whenever the log is not a verbatim continuation of what the caller last
	// saw, and it does not change merely because a process restarted.
	Incarnation(ctx context.Context) (Incarnation, error)

	// CommittedPosition is the newest position the tree itself was changed at, which for a
	// log kept beside the tree is the log's own tail.
	//
	// It exists for the one case where those two can disagree: a log kept somewhere that
	// does not share the tree's transaction can be missing its last entries after a crash.
	// Comparing the two at startup turns that from permanent silent divergence into one
	// honest rebuild, and an implementation that finds them apart must change its
	// incarnation.
	CommittedPosition(ctx context.Context) (Position, error)
}

// Snap is one consistent picture of a tree, delivered in pages.
//
// It holds a resource for as long as it is open — a read transaction, a version, a
// reference — so a caller closes it as soon as it is done, and an implementation is free
// to refuse one that has been open too long. Whatever it holds is released by Close.
type Snap interface {
	// Next returns at most limit rows and reports whether the picture is complete. A caller
	// that stops early still calls Close.
	Next(ctx context.Context, limit int) (rows []Row, done bool, err error)

	// Close releases what the picture holds.
	Close() error
}

// Position orders the changes to one namespace. It increases with every change and is
// never reused, so a caller that has applied everything up to some position can ask for
// what came after it. Zero is before every change there has ever been.
//
// Positions are dense in no particular way: an implementation may leave gaps, and a caller
// may only compare them.
type Position int64

// Incarnation names a run of history. It is compared for equality and nothing else; two
// values that differ mean the caller must start over.
type Incarnation string

// Retention is what a log still holds.
type Retention struct {
	// Oldest is the position of the oldest change still recorded, or 0 when the log holds
	// nothing.
	Oldest Position

	// Tail is the newest position recorded, whether or not the change at it is still
	// retained. It is kept apart from the entries for a reason that is easy to miss: a log
	// that has discarded everything cannot otherwise tell "you are caught up" from "you
	// missed everything", and those two answers differ by a full rebuild.
	Tail Position

	// TrimmedByAge reports whether anything was discarded for being old rather than for
	// being too much. A caller that fell out of the window is told which dimension pushed
	// it out, because the two say different things: age means this caller was away too
	// long, volume means the namespace changes faster than the log was configured to hold.
	TrimmedByAge bool
}

// ChangeKind says what happened to a name.
type ChangeKind int

const (
	// Created: the name did not exist and now holds a node.
	Created ChangeKind = iota

	// Removed: the name held a node and now holds nothing.
	Removed

	// Modified: the node at the name changed — its attributes, its size, its contents.
	Modified

	// Renamed: the node arrived at this name from another one, which is now empty.
	Renamed
)

// Change is one recorded change to the tree.
//
// It carries the node rather than only the name, so that a replica applies it without
// asking anything back. The alternative — an event saying only that something under a name
// changed — is cheaper to produce and much worse to consume: a directory rename is one row
// here and one row in a replica, whereas an invalidation of a directory forces a replica to
// discard everything beneath it and walk it again, and renaming directories is what build
// tools, version control and package managers do constantly.
//
// The price is that a replica is exactly as correct as this record is. There is no
// revalidation behind it and no timeout that repairs a change that was recorded wrongly, so
// producing these is an obligation of the same weight as applying them.
type Change struct {
	Position Position
	Kind     ChangeKind

	// Parent and Name are where the change landed. The parent is a node id rather than a
	// path, which is what makes a directory rename one row: everything beneath it keeps
	// pointing at the same parent and needs no event of its own.
	Parent int64
	Name   []byte

	// From is where a renamed node came from, and nil for every other kind.
	From *Location

	// Node is what the name holds afterwards, and nil for Removed.
	Node *Node
}

// Location is a name in a directory.
type Location struct {
	Parent int64
	Name   []byte
}

// Row is one node of a snapshot, named the way a Change names one.
type Row struct {
	// Parent is 0 and Name is nil for the root, which has no name and no parent.
	Parent int64
	Name   []byte
	Node   Node
}
