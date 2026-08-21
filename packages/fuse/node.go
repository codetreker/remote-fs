package fuse

import (
	"context"
	iofs "io/fs"
	"maps"
	"slices"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/storage"
)

// namespace is the state one mount shares across every node in it.
type namespace struct {
	storage storage.Storage

	// owner is reported for every node. The namespace has no notion of ownership, and
	// the mount is reachable only by the user who made it (R-SEC-1), so that user owns
	// everything in it.
	owner gofuse.Owner

	// maxFileSize is Options.MaxFileSize, already resolved: never zero, never negative.
	maxFileSize int64

	// room is what this mount last heard about the space left in the namespace. It is
	// shared by every handle, because it describes the namespace rather than any one file.
	room roomGauge
}

// holds reports whether a file of size bytes can be held in memory, which every operation
// that allocates in proportion to a size asks before allocating (R-INT-3). Asking
// afterwards would be too late: Go answers an allocation it cannot satisfy with a fatal
// error rather than a panic, so an oversized request does not fail, it ends the process
// this package is linked into (R-INT-1, R-INT-2).
func (ns *namespace) holds(size int64) bool { return size <= ns.maxFileSize }

// node is one directory or file. Its place in the tree is owned by the FUSE library, and
// its path is read back out of the tree rather than stored: a stored path would be wrong
// from the moment an ancestor was renamed.
type node struct {
	fs.Inode
	ns *namespace

	// id is what the kernel knows this node by. It is this mount's to allocate and keep,
	// because the namespace has no notion of identity; see identity.go.
	id *identity

	// open holds the handles this node is presently read and written through, so that a
	// request which needs them can find them.
	//
	// The kernel does not carry a descriptor into every request that came from one:
	// futimens(3) reaches a filesystem as utimensat on the descriptor's own path, with
	// nothing left to say which descriptor it was (measured on Linux 6.8). A program that
	// writes a file and then dates it — tar -x and cp -p both do, through the descriptor,
	// before closing it — needs the two to happen in that order, and the order is this
	// node's to keep.
	mu   sync.Mutex
	open map[*handle]struct{}
}

func (n *node) track(h *handle) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.open == nil {
		n.open = map[*handle]struct{}{}
	}
	n.open[h] = struct{}{}
}

func (n *node) forget(h *handle) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.open, h)
}

// commitOpen writes back everything held for this node that the namespace does not have
// yet. Two handles holding different contents commit in no particular order, which is the
// order two descriptors closing would commit in as well.
func (n *node) commitOpen(ctx context.Context) syscall.Errno {
	n.mu.Lock()
	open := slices.Collect(maps.Keys(n.open))
	n.mu.Unlock()

	for _, h := range open {
		if errno := h.Flush(ctx); errno != 0 {
			return errno
		}
	}
	return 0
}

var (
	_ fs.NodeLookuper  = (*node)(nil)
	_ fs.NodeGetattrer = (*node)(nil)
	_ fs.NodeSetattrer = (*node)(nil)
	_ fs.NodeReaddirer = (*node)(nil)
	_ fs.NodeOpener    = (*node)(nil)
	_ fs.NodeCreater   = (*node)(nil)
	_ fs.NodeMkdirer   = (*node)(nil)
	_ fs.NodeUnlinker  = (*node)(nil)
	_ fs.NodeRmdirer   = (*node)(nil)
	_ fs.NodeRenamer   = (*node)(nil)
	_ fs.NodeStatfser  = (*node)(nil)
)

func (n *node) path() string { return n.Path(n.Root()) }

func (n *node) childPath(name string) string {
	if parent := n.path(); parent != "" {
		return parent + "/" + name
	}
	return name
}

func (n *node) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	attr, err := n.ns.storage.Stat(ctx, n.childPath(name))
	if err != nil {
		errno := errnoOf(err)
		if errno == syscall.ENOENT {
			// Whatever this mount had named here has been removed by somebody it never
			// heard from, so the name refers to nothing and its identity goes with it.
			// This is what keeps the record's size a property of the namespace rather
			// than of how long the mount has been up.
			n.id.forget(name)
		}
		return nil, errno
	}
	if errno := n.ns.fillAttr(&out.Attr, attr); errno != 0 {
		return nil, errno
	}
	return n.child(ctx, name, out.Attr.Mode), 0
}

func (n *node) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	attr, err := n.ns.storage.Stat(ctx, n.path())
	if err != nil {
		return errnoOf(err)
	}
	if errno := n.ns.fillAttr(&out.Attr, attr); errno != 0 {
		return errno
	}
	// A program that writes a file and then checks its size must see what it wrote,
	// even though the commit has not happened yet (R-CON-4).
	if h, ok := f.(*handle); ok {
		h.describeUncommitted(&out.Attr)
	}
	return 0
}

// Setattr carries out the changes the namespace can hold, and refuses the rest rather than
// accepting them and doing nothing.
func (n *node) Setattr(ctx context.Context, f fs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	change, errno := n.ns.requestedChange(in)
	if errno != 0 {
		return errno
	}

	// Size first. A request carrying both is asking for a file of that length dated that
	// instant, and a write stamps the namespace's own time on what it writes, so a time
	// applied before it would be overwritten by it.
	if in.Valid&gofuse.FATTR_SIZE != 0 {
		if errno := n.resize(ctx, f, int64(in.Size)); errno != 0 {
			return errno
		}
	}

	if !change.Empty() {
		// A time is being set on contents this process is still holding, so those contents
		// have to reach the namespace first: the write that commits them stamps the
		// namespace's own modification time on what it writes, and it would otherwise land
		// after the time being asked for here. A mode needs no such care, because replacing
		// a file's contents keeps the mode it already had.
		if change.AccessTime != nil || change.ModTime != nil {
			if errno := n.commitOpen(ctx); errno != 0 {
				return errno
			}
		}
		if err := n.ns.storage.SetAttr(ctx, n.path(), change); err != nil {
			return errnoOf(err)
		}
	}
	return n.Getattr(ctx, f, out)
}

// requestedChange picks out the attribute changes the namespace can hold, and refuses a
// request for anything it cannot.
//
// The bits a kernel sends were measured on Linux 6.8 rather than assumed, by recording
// every request one run of the everyday tools produced. chmod arrives as FATTR_MODE alone,
// with the node's kind still in the mode word; chown as FATTR_UID and FATTR_GID together
// or either alone; utimensat as FATTR_ATIME and FATTR_MTIME, together or alone, with the
// _NOW bits added when the caller asked for the current time; truncate(2) as
// FATTR_SIZE|FATTR_LOCKOWNER, and ftruncate(2) as that with FATTR_FH.
//
// FATTR_FH and FATTR_LOCKOWNER qualify a request rather than ask for anything.
// FATTR_CTIME does too: no system call sets a change time, and a kernel that attaches one
// is stating what follows from the change it is already asking for. Size is the caller's
// to apply, so it is not part of the change reported here.
func (ns *namespace) requestedChange(in *gofuse.SetAttrIn) (storage.AttrChange, syscall.Errno) {
	// Everything this filesystem knows what to do with. FATTR_KILL_SUIDGID is deliberately
	// outside it: it asks for the setuid and setgid bits to be cleared, which is a change,
	// and dropping it would leave those bits on a file the kernel had decided should lose
	// them. The kernel only sends it to a filesystem that negotiated
	// CAP_HANDLE_KILLPRIV_V2, which the library beneath this one never asks for, so it
	// clears them itself with an ordinary mode change instead.
	const understood = gofuse.FATTR_MODE | gofuse.FATTR_UID | gofuse.FATTR_GID | gofuse.FATTR_SIZE |
		gofuse.FATTR_ATIME | gofuse.FATTR_MTIME | gofuse.FATTR_ATIME_NOW | gofuse.FATTR_MTIME_NOW |
		gofuse.FATTR_CTIME | gofuse.FATTR_FH | gofuse.FATTR_LOCKOWNER

	var change storage.AttrChange
	if in.Valid&^understood != 0 {
		return change, syscall.EPERM
	}

	// Ownership has nowhere to go: the namespace carries none, and this mount reports
	// every node as belonging to whoever made it (R-SEC-1). A request naming that same
	// owner asks for what is already the case, so answering it states nothing untrue —
	// and refusing it would refuse cp -p and tar -x, which ask as a matter of course.
	if uid, ok := in.GetUID(); ok && uid != ns.owner.Uid {
		return change, syscall.EPERM
	}
	if gid, ok := in.GetGID(); ok && gid != ns.owner.Gid {
		return change, syscall.EPERM
	}

	if mode, ok := in.GetMode(); ok {
		requested := namespaceMode(mode)
		change.Mode = &requested
	}
	if accessed, ok := in.GetATime(); ok {
		change.AccessTime = &accessed
	}
	if changed, ok := in.GetMTime(); ok {
		change.ModTime = &changed
	}
	return change, 0
}

// resize applies a new length. An open handle already holds the contents, so the change
// belongs in that buffer and is committed with the rest of it; without one there is
// nothing to defer to and the change is written straight through.
func (n *node) resize(ctx context.Context, f fs.FileHandle, size int64) syscall.Errno {
	if h, ok := f.(*handle); ok {
		return h.resize(ctx, size)
	}

	if !n.ns.holds(size) {
		return syscall.EFBIG
	}
	// Nothing that is there survives a truncation to nothing, so the contents are not
	// fetched in order to be discarded — the reasoning Open applies to O_TRUNC. It also
	// means a file too large to hold can still be emptied, which is the one thing that
	// can be done to it without holding it.
	if size == 0 {
		return errnoOf(n.ns.storage.Write(ctx, n.path(), nil))
	}

	// Every other length keeps a prefix of what is there, so what is there has to be
	// fetched, and the size is read first because fetching a file this mount cannot hold
	// is the allocation the ceiling exists to prevent. Storage reads are whole-file;
	// fetching only the prefix that survives would need a ranged read, which the storage
	// contract does not have.
	attr, err := n.ns.storage.Stat(ctx, n.path())
	if err != nil {
		return errnoOf(err)
	}
	if !n.ns.holds(attr.Size) {
		return syscall.EFBIG
	}

	body, err := n.ns.storage.Read(ctx, n.path())
	if err != nil {
		return errnoOf(err)
	}
	return errnoOf(n.ns.storage.Write(ctx, n.path(), resized(body, size)))
}

func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Read before the listing rather than after it, so that a name which appears while
	// the listing is in flight is not one this listing goes on to call gone.
	before := n.id.given()

	entries, err := n.ns.storage.List(ctx, n.path())
	if err != nil {
		return nil, errnoOf(err)
	}

	listing := make([]gofuse.DirEntry, 0, len(entries))
	present := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		mode, errno := systemMode(e.Attr.Mode)
		if errno != 0 {
			return nil, errno
		}
		present[e.Name] = struct{}{}
		// The number a listing reports has to be the number a stat of the same name
		// reports, or programs that pair the two see two different files.
		listing = append(listing, gofuse.DirEntry{Name: e.Name, Mode: mode, Ino: n.id.child(e.Name, mode).ino})
	}
	n.id.keepOnly(present, before)

	return fs.NewListDirStream(listing), 0
}

func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// The contents are about to be replaced wholesale, so there is nothing worth
	// fetching. The empty buffer counts as uncommitted, because `> f` truncates a file
	// without ever writing a byte and the truncation still has to be committed.
	if flags&syscall.O_TRUNC != 0 {
		return newHandle(n, nil, uncommitted), 0, 0
	}

	// The whole file is read once here and served out of the buffer from then on.
	// Storage reads and writes deal in whole files, so serving each kernel-sized read
	// with its own storage read would make reading a file cost a multiple of its size.
	//
	// The size is read before the contents, because a file this mount cannot hold has to
	// be refused rather than fetched, and fetching it is the allocation being refused.
	// That costs a second call on every open. The two calls are separate, so a file that
	// grows between them is still fetched whole; closing that window needs a ranged read,
	// which the storage contract does not have.
	attr, err := n.ns.storage.Stat(ctx, n.path())
	if err != nil {
		return nil, 0, errnoOf(err)
	}
	if !n.ns.holds(attr.Size) {
		return nil, 0, syscall.EFBIG
	}

	body, err := n.ns.storage.Read(ctx, n.path())
	if err != nil {
		return nil, 0, errnoOf(err)
	}
	return newHandle(n, body, committed), 0, 0
}

// Create makes the file in the namespace straight away, rather than at the commit, so
// that another program looking for it sees it at once.
func (n *node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	path := n.childPath(name)
	if err := n.ns.storage.Create(ctx, path); err != nil {
		return nil, nil, 0, errnoOf(err)
	}
	if errno := n.ns.wearMode(ctx, path, mode); errno != 0 {
		return nil, nil, 0, errno
	}
	attr, err := n.ns.storage.Stat(ctx, path)
	if err != nil {
		return nil, nil, 0, errnoOf(err)
	}
	if errno := n.ns.fillAttr(&out.Attr, attr); errno != 0 {
		return nil, nil, 0, errno
	}
	// The file the handle holds is the empty one just created, so a handle that is
	// closed without a write has nothing to commit.
	child := n.child(ctx, name, out.Attr.Mode)
	return child, newHandle(child.Operations().(*node), nil, committed), 0, 0
}

func (n *node) Mkdir(ctx context.Context, name string, mode uint32, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	path := n.childPath(name)
	if err := n.ns.storage.Mkdir(ctx, path); err != nil {
		return nil, errnoOf(err)
	}
	if errno := n.ns.wearMode(ctx, path, mode); errno != 0 {
		return nil, errno
	}
	attr, err := n.ns.storage.Stat(ctx, path)
	if err != nil {
		return nil, errnoOf(err)
	}
	if errno := n.ns.fillAttr(&out.Attr, attr); errno != 0 {
		return nil, errno
	}
	return n.child(ctx, name, out.Attr.Mode), 0
}

// wearMode gives a node just made the permissions the caller asked for.
//
// Making a node and giving it its permissions are one step to the caller and two here,
// because the namespace makes a node with permissions of its own choosing. Leaving it at
// those would mean tar and unzip extracting programs that cannot be run, and a file
// created for one user readable by everybody; the kernel has already applied the caller's
// umask by this point, so what arrives is the mode the node is meant to end up with.
//
// The cost is a third call on every creation, and a moment in which the node is there
// under the wrong permissions. Both go away together, when the namespace grows a way to
// make a node with its attributes in one call.
func (ns *namespace) wearMode(ctx context.Context, path string, mode uint32) syscall.Errno {
	requested := namespaceMode(mode)
	return errnoOf(ns.storage.SetAttr(ctx, path, storage.AttrChange{Mode: &requested}))
}

func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	if err := n.ns.storage.Remove(ctx, n.childPath(name)); err != nil {
		return errnoOf(err)
	}
	n.id.forget(name)
	return 0
}

func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if err := n.ns.storage.RemoveDir(ctx, n.childPath(name)); err != nil {
		return errnoOf(err)
	}
	n.id.forget(name)
	return 0
}

func (n *node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	// The namespace offers one rename, which replaces whatever is at the destination.
	// RENAME_EXCHANGE and RENAME_NOREPLACE ask for something else, and EINVAL is what
	// renameat2 reports for a flag the filesystem cannot honour.
	if flags != 0 {
		return syscall.EINVAL
	}
	target := newParent.(*node)
	if err := n.ns.storage.Rename(ctx, n.childPath(name), target.childPath(newName)); err != nil {
		return errnoOf(err)
	}
	// The FUSE library is about to move the existing inode to the new name, carrying the
	// number this mount gave it. The name it leaves has to stop referring to that number
	// in the same breath, because a node created there afterwards is a different node and
	// the two are live at once.
	n.id.move(name, target.id, newName)
	return 0
}

// --- identity ------------------------------------------------------------------------

// child returns the inode for one name, reusing the one already in the tree when it is
// still the node this mount has that name's identity for. Reuse is what keeps a file's
// identity stable across a rename: the FUSE library moves the existing inode to the new
// name, so looking that name up has to find it again rather than mint a second inode for
// the same file.
func (n *node) child(ctx context.Context, name string, mode uint32) *fs.Inode {
	id := n.id.child(name, mode)
	if existing := n.GetChild(name); existing != nil && existing.StableAttr().Ino == id.ino {
		return existing
	}
	return n.NewInode(ctx, &node{ns: n.ns, id: id}, fs.StableAttr{Mode: mode, Ino: id.ino})
}

// --- attributes ----------------------------------------------------------------------

func (ns *namespace) fillAttr(out *gofuse.Attr, attr storage.Attr) syscall.Errno {
	mode, errno := systemMode(attr.Mode)
	if errno != 0 {
		return errno
	}
	out.Mode = mode
	if !attr.IsDir() {
		out.Size = uint64(attr.Size)
	}
	accessed, changed := attr.AccessTime, attr.ModTime
	out.Atime, out.Atimensec = uint64(accessed.Unix()), uint32(accessed.Nanosecond())
	// The namespace has no change time. The modification time is reported in its place
	// rather than an invented one, and it is the closest true thing there is: every change
	// the namespace can record is a change to the contents.
	out.Mtime, out.Ctime = uint64(changed.Unix()), uint64(changed.Unix())
	out.Mtimensec, out.Ctimensec = uint32(changed.Nanosecond()), uint32(changed.Nanosecond())
	out.Owner = ns.owner
	// Hard links do not exist here (R-FS-4). One is the honest count, and it is also the
	// count that keeps tools which walk a tree from concluding, from a directory's link
	// count, that the directory has no subdirectories worth descending into.
	out.Nlink = 1
	return 0
}

// systemMode renders a namespace mode in the bits the kernel uses. A node whose kind we
// cannot name is refused rather than presented as an ordinary file, because presenting it
// would invite reads and writes that cannot mean what they appear to.
func systemMode(mode iofs.FileMode) (uint32, syscall.Errno) {
	bits := uint32(mode.Perm()) | specialBits(mode)
	switch mode.Type() {
	case 0:
		return bits | syscall.S_IFREG, 0
	case iofs.ModeDir:
		return bits | syscall.S_IFDIR, 0
	case iofs.ModeSymlink:
		return bits | syscall.S_IFLNK, 0
	case iofs.ModeNamedPipe:
		return bits | syscall.S_IFIFO, 0
	case iofs.ModeSocket:
		return bits | syscall.S_IFSOCK, 0
	case iofs.ModeDevice:
		return bits | syscall.S_IFBLK, 0
	case iofs.ModeDevice | iofs.ModeCharDevice:
		return bits | syscall.S_IFCHR, 0
	}
	return 0, syscall.EIO
}

// namespaceMode renders a mode the kernel sent as a namespace mode.
//
// The kernel keeps a node's kind in the same word, and it is dropped rather than
// translated: what comes back is a mode to set, and a node's kind is not something a
// caller sets. storage.SettableMode is the set this produces.
func namespaceMode(mode uint32) iofs.FileMode {
	requested := iofs.FileMode(mode) & iofs.ModePerm
	for _, bit := range specialModeBits {
		if mode&bit.system != 0 {
			requested |= bit.namespace
		}
	}
	return requested
}

func specialBits(mode iofs.FileMode) uint32 {
	var bits uint32
	for _, bit := range specialModeBits {
		if mode&bit.namespace != 0 {
			bits |= bit.system
		}
	}
	return bits
}

// The three bits that are neither a node's kind nor one of the nine permission bits. Go
// keeps them well above the permission bits and the kernel keeps them just above, so
// neither direction is a matter of masking.
var specialModeBits = []struct {
	namespace iofs.FileMode
	system    uint32
}{
	{iofs.ModeSetuid, syscall.S_ISUID},
	{iofs.ModeSetgid, syscall.S_ISGID},
	{iofs.ModeSticky, syscall.S_ISVTX},
}
