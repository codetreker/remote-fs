// Package storage defines the contract for one namespace: a tree of directories and
// files that the rest of the system reads and writes through a single interface.
//
// Two implementations exist and they sit on opposite sides of the network. The server
// side holds real data; the client side turns every call into one request. Everything
// above this interface — in particular the FUSE mount — works against either without
// knowing which one it has, which is what lets the mount be exercised against a plain
// local directory with no server in sight.
package storage

import (
	"context"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"syscall"
	"time"
)

// Storage is one namespace.
//
// Paths are slash-separated and relative to the root, with no leading slash; the root
// itself is the empty string. A path that is absolute, or that climbs out of the root,
// is rejected with syscall.EINVAL.
//
// The root is not a node any caller made, so removing it, moving it, or putting something
// else in its place are not operations offered here; each is syscall.EBUSY. A namespace
// whose root has been removed answers syscall.ENOENT to everything afterwards, which
// reads as "that file is not there" when the truth is that the namespace is not there.
//
// Errors are syscall.Errno values, reachable with errors.Is even when an implementation
// wraps them. A mount can therefore hand them to the kernel unchanged, and no
// implementation has to translate between two error vocabularies. Which errnos may be
// reported is itself part of the contract: Errnos enumerates them, and an errno outside
// that set cannot be named to anyone.
//
// Times cross this interface as time.Time and are neither rounded nor clamped on the way.
// What a namespace can keep is its own: one held in a directory keeps what that directory's
// filesystem keeps, with the range and the precision a plain file there has, so Stat
// reports the instant that was stored rather than the instant that was asked for. That is
// a fact about the node, not a report about the call.
//
// Every operation acts on the node that is at the name it was given, never on a node the
// one at that name refers to. A symbolic link is described as a link, by Stat and by List
// alike, and nothing reads through one, writes through one, or carries an attribute onto
// what it points at. Following would silently put a different object under the name the
// caller wrote, which is the class of answer this system exists not to give: nothing above
// can tell that it happened, and the bytes that come back, the bytes that go in and the
// node that is removed all belong to something the caller never named.
//
// The operations that could otherwise resolve a link therefore refuse one. Read and Write
// report syscall.ELOOP, which is what open(2) answers when it is told not to follow. A
// mode cannot be put on a link at all, since no Unix offers a way to set one, so a SetAttr
// naming a mode reports syscall.EOPNOTSUPP; times can be, and a SetAttr naming times
// succeeds and changes the link's own. A change naming both is thus one more change that
// may be applied in part.
//
// An implementation that cannot determine an outcome must report that failure. It must
// never substitute an empty listing or syscall.ENOENT, because both of those read as
// established fact to whatever runs on top, and acting on them destroys data.
type Storage interface {
	// Stat reports the attributes of the node at path. A symbolic link is described as a
	// link — its own mode and its own length — rather than as whatever it points at.
	Stat(ctx context.Context, path string) (Attr, error)

	// SetAttr applies change to the node at path. An attribute the change does not name
	// is left as it is.
	//
	// A change naming more than one attribute is not applied atomically: an
	// implementation may have applied part of it when it reports a failure. Nothing
	// above this interface depends on it being otherwise, because a kernel sends a mode
	// change and a time change as separate requests.
	SetAttr(ctx context.Context, path string, change AttrChange) error

	// List returns the entries of the directory at path, sorted by name.
	List(ctx context.Context, path string) ([]Entry, error)

	// Read returns the entire contents of the file at path.
	//
	// A read may report syscall.EAGAIN while the file is there: an implementation that
	// fetches a file in more than one step can lose to a writer that replaces it between
	// them, and one facing a writer that never pauses can lose every time it tries. That
	// is a report about this attempt rather than about the file, and it is distinct from
	// both syscall.ENOENT, which says the name is gone, and syscall.EIO, which says the
	// namespace no longer has bytes it still claims to hold. A caller that wants the
	// contents tries again; a caller that cannot has learned that it did not read them,
	// which is the one thing it must not be left guessing about.
	Read(ctx context.Context, path string) ([]byte, error)

	// Write replaces the contents of the file at path, creating the file if it is not
	// there. The replacement is atomic: a concurrent reader sees either the whole of
	// the old contents or the whole of the new ones, never a mixture of the two.
	Write(ctx context.Context, path string, content []byte) error

	// Create makes an empty file at path, failing with syscall.EEXIST if anything is
	// already there.
	Create(ctx context.Context, path string) error

	// Mkdir makes a directory at path.
	Mkdir(ctx context.Context, path string) error

	// Remove removes the file at path. Removing a directory is syscall.EISDIR.
	Remove(ctx context.Context, path string) error

	// RemoveDir removes the empty directory at path. Removing a file is
	// syscall.ENOTDIR; removing a directory that still has entries is
	// syscall.ENOTEMPTY. Removing the root is syscall.EBUSY.
	RemoveDir(ctx context.Context, path string) error

	// Rename moves the node at from to to. An existing file at to is replaced. Naming
	// the root as either operand is syscall.EBUSY.
	Rename(ctx context.Context, from, to string) error

	// Space reports the room the namespace has. It describes the whole namespace rather
	// than the part of it under any one path, because one namespace is one workspace
	// under one limit, and asking about a subtree is a question nothing here can answer.
	//
	// A namespace that has no room of its own to report answers syscall.ENOSYS. That is
	// a standing property of the implementation rather than a condition of the call: one
	// that answers answers for as long as it exists, and one that refuses never starts.
	// Nothing above may therefore treat a refusal as a transient failure to retry, and
	// nothing may infer a figure it was not given.
	Space(ctx context.Context) (Space, error)
}

// Space is the room a namespace has, in bytes.
//
// The three are separately measured rather than derived from one another, because for a
// namespace held in a directory they genuinely differ: a filesystem keeps a reserve only
// the superuser may spend, which makes Avail smaller than Total-Used. Reporting either as
// the other would state a quantity nobody measured.
type Space struct {
	// Total is what the namespace may hold in all.
	Total int64

	// Used is how much of Total is gone. What that counts is the namespace's own
	// business: one holding its own allowance counts the content it holds, while one
	// held in a directory reports what that whole filesystem has consumed, other
	// people's files included. Both answer the question Total asks — how much of the
	// room reported here is left — and neither is a census of this namespace's files.
	//
	// It may exceed Total, which is what an allowance lowered underneath content already
	// written looks like. Avail is then zero.
	Used int64

	// Avail is what may still be written. It is not Total-Used: an implementation that
	// knows of a tighter limit beneath it reports the tighter figure, because an
	// allowance is a ceiling on what may be written rather than evidence that the bytes
	// will fit.
	Avail int64
}

// Coherent reports whether s is an answer that can be true of anything.
//
// A negative field describes an impossibility, and Avail above what Total leaves offers
// room that cannot exist. Both matter beyond tidiness: these are byte counts on their way
// into a kernel reply whose fields are unsigned, where a negative becomes an enormous
// positive and a program that checks for room before writing is told it has space no disk
// anywhere holds. That is the fabricated fact R-ERR-2 forbids, so an incoherent answer is
// a failure to report rather than a set of numbers to repair.
func (s Space) Coherent() bool {
	if s.Total < 0 || s.Used < 0 || s.Avail < 0 {
		return false
	}
	return s.Avail <= max(s.Total-s.Used, 0)
}

// Attr describes one node — the one that is at the name it was asked about. A symbolic
// link's attributes are the link's own, and nothing here describes what it points at.
type Attr struct {
	// Mode carries the type bits and the permission bits. The type bits say what kind of
	// node is at the name, so a symbolic link reads as fs.ModeSymlink.
	Mode fs.FileMode

	// Size is the length of a file's contents in bytes, or of the name a symbolic link
	// holds. It is unspecified for a directory.
	Size int64

	// AccessTime is when the contents were last read.
	AccessTime time.Time

	// ModTime is when the contents last changed.
	ModTime time.Time
}

// IsDir reports whether the node is a directory.
func (a Attr) IsDir() bool { return a.Mode.IsDir() }

// SettableMode is every bit of a Mode a caller may set: the permission bits, and the
// three bits that change how they are applied.
//
// The rest of an fs.FileMode says what kind of node this is, and a node's kind is not a
// property a caller changes — a directory does not become a file by being chmod-ed. The
// bits no Linux filesystem has, fs.ModeAppend and fs.ModeExclusive among them, are
// outside it for a second reason: an implementation cannot store what its host has no
// place for, so accepting them would report a change that did not happen.
const SettableMode = fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky

// AttrChange names the attributes to set on a node.
//
// A nil field is an attribute the caller is not changing. They are pointers rather than
// values because no value could stand for "leave this alone": mode 0 is what chmod 000
// asks for, and the zero time.Time is an instant like any other. It matters because a
// kernel sends a mode change and a time change as separate requests, and neither may
// clear what the other set.
type AttrChange struct {
	Mode       *fs.FileMode
	AccessTime *time.Time
	ModTime    *time.Time
}

// Empty reports whether the change names no attribute at all. Such a change is still a
// statement about one node, and applying it still fails if that node is not there.
func (c AttrChange) Empty() bool {
	return c.Mode == nil && c.AccessTime == nil && c.ModTime == nil
}

// Check rejects a change no implementation may carry out, with syscall.EINVAL. Every
// implementation owes its callers this refusal, so it lives here rather than being
// written out again in each of them.
func (c AttrChange) Check() error {
	if c.Mode != nil && *c.Mode&^SettableMode != 0 {
		return fmt.Errorf("mode %v sets bits outside %v, which name a node's kind rather than its permissions: %w",
			*c.Mode, SettableMode, syscall.EINVAL)
	}
	return nil
}

// Entry is one member of a directory listing. Attributes travel alongside the name so
// that listing a directory of n entries costs one call rather than n+1 — the difference
// is invisible on a local directory and decisive over a network.
type Entry struct {
	Name string
	Attr Attr
}

// CleanPath normalizes a namespace path and rejects anything outside the root. The
// returned path has no leading, trailing, or repeated slashes, and the root is "".
func CleanPath(p string) (string, error) {
	if strings.HasPrefix(p, "/") {
		return "", syscall.EINVAL
	}
	if p == "" {
		return "", nil
	}
	cleaned := path.Clean(p)
	if cleaned == "." {
		return "", nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", syscall.EINVAL
	}
	return cleaned, nil
}
