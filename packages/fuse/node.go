package fuse

import (
	"context"
	"errors"
	iofs "io/fs"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/storage"
)

// Names are addressed by parent identity and raw leaf. Descriptor and inode
// attributes use the retained file or node identity, including after unlink.
type node struct {
	fs.Inode
	volume *volume
	id     *identity
}

func (n *node) checkAttr(attr storage.Attr) error {
	if attr.ID == 0 || attr.ID != n.id.node || !attr.IsDir() && attr.Size < 0 {
		return syscall.EIO
	}
	mode, errno := attributeMode(attr)
	if errno != 0 || mode&syscall.S_IFMT != n.id.kind {
		return syscall.EIO
	}
	return nil
}

var (
	_ fs.NodeLookuper  = (*node)(nil)
	_ fs.NodeGetattrer = (*node)(nil)
	_ fs.NodeSetattrer = (*node)(nil)
	_ fs.NodeOpener    = (*node)(nil)
	_ fs.NodeCreater   = (*node)(nil)
	_ fs.NodeMkdirer   = (*node)(nil)
	_ fs.NodeUnlinker  = (*node)(nil)
	_ fs.NodeRmdirer   = (*node)(nil)
	_ fs.NodeRenamer   = (*node)(nil)
	_ fs.NodeStatfser  = (*node)(nil)
)

func (n *node) childName(name string) storage.ChildName {
	return storage.ChildName{Parent: storage.DirectoryTarget{NodeID: n.id.node}, RawLeaf: []byte(name)}
}

func (v *volume) namespace() (storage.NamespaceAccess, error) {
	access, ok := v.files.(storage.NamespaceAccess)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := access.CheckNamespaceAccess(); err != nil {
		return nil, err
	}
	return access, nil
}

func (n *node) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return n.lookup(ctx, name, out, nil)
}

func (n *node) lookup(ctx context.Context, name string, out *gofuse.EntryOut, scope *storage.UseScope) (*fs.Inode, syscall.Errno) {
	if err := n.volume.check(); err != nil {
		return nil, errnoOf(err)
	}
	access, err := n.volume.namespace()
	if err != nil {
		return nil, errnoOf(err)
	}
	target := n.childName(name)
	target.Parent.Scope = scope
	attr, err := access.LookupAt(ctx, target)
	if err != nil {
		errno := errnoOf(err)
		if errno == syscall.ENOENT {
			// Whatever this mount had named here has been removed by somebody it never
			// heard from, so the name refers to nothing and its identity goes with it.
			// This is what keeps the record's size a property of the volume rather
			// than of how long the mount has been up.
			n.id.forget(name)
		}
		return nil, errno
	}
	if errno := n.volume.fillAttr(&out.Attr, attr); errno != 0 {
		return nil, errno
	}
	return n.child(ctx, name, out.Attr.Mode, attr.ID), 0
}

func (n *node) Getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	return errnoOf(n.getattr(ctx, f, out))
}

func (n *node) getattr(ctx context.Context, f fs.FileHandle, out *gofuse.AttrOut) error {
	if err := n.volume.check(); err != nil {
		return err
	}
	var attr storage.Attr
	var err error
	if h, ok := f.(attributeHandle); ok {
		attr, err = h.stat(ctx)
	} else {
		attr, err = n.volume.files.StatNode(ctx, n.id.node)
	}
	if err != nil {
		return err
	}
	if err := n.checkAttr(attr); err != nil {
		return err
	}
	if errno := n.volume.fillAttr(&out.Attr, attr); errno != 0 {
		return errno
	}
	return nil
}

// Setattr carries out the changes the volume can hold, and refuses the rest rather than
// accepting them and doing nothing.
func (n *node) Setattr(ctx context.Context, f fs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	return errnoOf(n.setattr(ctx, f, in, out))
}

func (n *node) setattr(ctx context.Context, f fs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) error {
	if err := n.volume.check(); err != nil {
		return err
	}
	change, errno := n.volume.requestedChange(in)
	if errno != 0 {
		return errno
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	changed := false
	if in.Valid&gofuse.FATTR_SIZE != 0 {
		if in.Size > uint64(n.volume.maxFileSize) {
			return syscall.EFBIG
		}
		if err := n.resize(ctx, f, int64(in.Size)); err != nil {
			return err
		}
		changed = true
	}
	if !change.AttrChange.Empty() {
		if err := ctx.Err(); err != nil {
			return afterMutation(changed, err)
		}
		var attr storage.Attr
		var err error
		if h, ok := f.(attributeHandle); ok {
			attr, err = h.setAttr(ctx, change.AttrChange)
		} else {
			attr, err = n.volume.files.SetNodeAttr(ctx, n.id.node, change.AttrChange)
		}
		if err != nil {
			return afterMutation(true, err)
		}
		if err := n.checkAttr(attr); err != nil {
			return err
		}
		changed = true
	}
	if change.Mode != nil {
		if err := ctx.Err(); err != nil {
			return afterMutation(changed, err)
		}
		if err := n.setPermissions(ctx, f, *change.Mode); err != nil {
			return afterMutation(changed, err)
		}
		changed = true
	}
	return afterMutation(changed, n.getattr(ctx, f, out))
}

// requestedChange picks out the attribute changes the volume can hold, and refuses a
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
func (v *volume) requestedChange(in *gofuse.SetAttrIn) (attributeChange, syscall.Errno) {
	// Everything this filesystem knows what to do with. FATTR_KILL_SUIDGID is deliberately
	// outside it: it asks for the setuid and setgid bits to be cleared, which is a change,
	// and dropping it would leave those bits on a file the kernel had decided should lose
	// them. The kernel only sends it to a filesystem that negotiated
	// CAP_HANDLE_KILLPRIV_V2, which the library beneath this one never asks for, so it
	// clears them itself with an ordinary mode change instead.
	const understood = gofuse.FATTR_MODE | gofuse.FATTR_UID | gofuse.FATTR_GID | gofuse.FATTR_SIZE |
		gofuse.FATTR_ATIME | gofuse.FATTR_MTIME | gofuse.FATTR_ATIME_NOW | gofuse.FATTR_MTIME_NOW |
		gofuse.FATTR_CTIME | gofuse.FATTR_FH | gofuse.FATTR_LOCKOWNER

	var change attributeChange
	if in.Valid&^understood != 0 {
		return change, syscall.EPERM
	}

	// Ownership has nowhere to go: the volume carries none, and this mount reports
	// every node as belonging to whoever made it (R-SEC-1). A request naming that same
	// owner asks for what is already the case, so answering it states nothing untrue —
	// and refusing it would refuse cp -p and tar -x, which ask as a matter of course.
	if uid, ok := in.GetUID(); ok && uid != v.owner.Uid {
		return change, syscall.EPERM
	}
	if gid, ok := in.GetGID(); ok && gid != v.owner.Gid {
		return change, syscall.EPERM
	}

	if mode, ok := in.GetMode(); ok {
		requested := storageMode(mode)
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

// A size request without a descriptor still retains the inode identity before
// changing it. Cleanup uses an independent finite context after caller interruption.
func (n *node) resize(ctx context.Context, f fs.FileHandle, size int64) error {
	if err := n.volume.check(); err != nil {
		return err
	}
	if size < 0 {
		return syscall.EINVAL
	}
	if !n.volume.holds(size) {
		return syscall.EFBIG
	}
	if h, ok := f.(*handle); ok {
		return h.resize(ctx, size)
	}
	file, err := n.volume.files.OpenNode(ctx, n.id.node, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Write: true}})
	if err != nil {
		return err
	}
	h := newHandle(n, file, false, true)
	err = h.resize(ctx, size)
	return n.volume.closeUnreturnedFile(ctx, file, err, err == nil)
}

func (n *node) readdir(ctx context.Context, scope storage.UseScope) (fs.DirStream, syscall.Errno) {
	if err := n.volume.check(); err != nil {
		return nil, errnoOf(err)
	}
	// Read before the listing rather than after it, so that a name which appears while
	// the listing is in flight is not one this listing goes on to call gone.
	before := n.id.given()

	access, err := n.volume.namespace()
	if err != nil {
		return nil, errnoOf(err)
	}
	observed, err := access.ReadDirNode(ctx, storage.DirectoryTarget{NodeID: n.id.node, Scope: &scope})
	if err != nil {
		return nil, errnoOf(err)
	}

	if observed.Observation.ParentID != n.id.node || len(observed.Observation.Revision) == 0 {
		return nil, syscall.EIO
	}
	entries := observed.Entries
	listing := make([]gofuse.DirEntry, 0, len(entries))
	present := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if e.Attr.ID == 0 || !e.Attr.IsDir() && e.Attr.Size < 0 {
			return nil, syscall.EIO
		}
		mode, errno := attributeMode(e.Attr)
		if errno != 0 {
			return nil, errno
		}
		name := string(e.RawLeaf)
		if err := storage.CheckLeaf(e.RawLeaf); err != nil {
			return nil, syscall.EIO
		}
		if _, duplicate := present[name]; duplicate {
			return nil, syscall.EIO
		}
		present[name] = struct{}{}
		// The number a listing reports has to be the number a stat of the same name
		// reports, or programs that pair the two see two different files.
		listing = append(listing, gofuse.DirEntry{Name: name, Mode: mode, Ino: n.id.child(name, mode, e.Attr.ID).ino})
	}
	n.id.keepOnly(present, before)

	return fs.NewListDirStream(listing), 0
}

func fileOpenOptions(flags uint32) (storage.FileOpenOptions, syscall.Errno) {
	var options storage.FileOpenOptions
	switch flags & syscall.O_ACCMODE {
	case syscall.O_RDONLY:
		options.Read = true
	case syscall.O_WRONLY:
		options.Write = true
	case syscall.O_RDWR:
		options.Read, options.Write = true, true
	default:
		return options, syscall.EINVAL
	}
	options.Truncate = flags&syscall.O_TRUNC != 0
	return options, 0
}

func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if err := n.volume.check(); err != nil {
		return nil, 0, errnoOf(err)
	}
	options, errno := fileOpenOptions(flags)
	if errno != 0 {
		return nil, 0, errno
	}
	options.ExpectedID = n.id.node
	if options.ExpectedID == 0 {
		return nil, 0, syscall.EIO
	}
	name, parent := n.Parent()
	if parent == nil {
		return nil, 0, syscall.ESTALE
	}
	parentNode, ok := parent.Operations().(*node)
	if !ok {
		return nil, 0, syscall.EIO
	}
	file, _, err := n.openFile(ctx, parentNode.childName(name), options)
	if err != nil {
		return nil, 0, errnoOf(err)
	}
	return newHandle(n, file, options.Read, options.Write), gofuse.FOPEN_DIRECT_IO, 0
}

func (n *node) openFile(ctx context.Context, name storage.ChildName, options storage.FileOpenOptions) (storage.File, storage.Attr, error) {
	opener, ok := n.volume.files.(storage.AtomicFileOpener)
	if !ok {
		return nil, storage.Attr{}, syscall.EOPNOTSUPP
	}
	if err := opener.CheckAtomicFileOpen(); err != nil {
		return nil, storage.Attr{}, err
	}
	target := storage.ChildCondition{State: storage.Any}
	if options.ExpectedID != 0 {
		target = storage.ChildCondition{State: storage.SameNode, NodeID: options.ExpectedID}
	}
	effect := storage.Keep
	if options.Truncate {
		effect = storage.ResetContent
	}
	var uses storage.Uses
	if options.Read {
		uses |= storage.ReadData
	}
	if options.Write {
		uses |= storage.WriteData
	}
	result, err := opener.OpenAt(ctx, name, storage.OpenAtOptions{
		Read: options.Read, Write: options.Write, Create: options.Create, Exclusive: options.Exclusive,
		Target: target, Existing: effect, Use: storage.UseClaim{Uses: uses},
		Initial: storage.InitialState{OnCreate: storage.InitialFields{Metadata: options.InitialMetadata}},
	})
	changed := result.Outcome == storage.Created || result.Outcome == storage.Reset || result.Outcome == storage.Replaced
	if result.File == nil {
		if err == nil {
			err = syscall.EIO
		}
		return nil, storage.Attr{}, afterMutation(changed, err)
	}
	if result.Outcome != storage.Opened && result.Outcome != storage.Created && result.Outcome != storage.Reset {
		err = errors.Join(syscall.EIO, err)
	}
	if err == nil {
		attr := result.Attr
		switch {
		case result.Outcome != storage.Opened && result.Outcome != storage.Created && result.Outcome != storage.Reset:
			err = syscall.EIO
		case attr.ID == 0 || attr.Size < 0 || attr.Kind != storage.NodeRegular:
			err = syscall.EIO
		case options.ExpectedID != 0 && attr.ID != options.ExpectedID:
			err = syscall.EIO
		case !n.volume.holds(attr.Size):
			err = syscall.EFBIG
		default:
			_, err = permissions(attr)
		}
	}
	if err != nil {
		return nil, storage.Attr{}, n.volume.closeUnreturnedFile(ctx, result.File, err, changed)
	}
	return result.File, result.Attr, nil
}

// Creation, exclusive existence checks, mode initialization, truncation and retention
// are one backend operation. A concurrent nonexclusive creator opens the winner.
func (n *node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if err := n.volume.check(); err != nil {
		return nil, nil, 0, errnoOf(err)
	}
	options, errno := fileOpenOptions(flags)
	if errno != 0 {
		return nil, nil, 0, errno
	}
	options.Create = true
	options.Exclusive = flags&syscall.O_EXCL != 0
	var err error
	options.InitialMetadata, err = initialPermissions(storageMode(mode))
	if err != nil {
		return nil, nil, 0, errnoOf(err)
	}
	file, attr, err := n.openFile(ctx, n.childName(name), options)
	if err != nil {
		return nil, nil, 0, errnoOf(err)
	}
	if errno := n.volume.fillAttr(&out.Attr, attr); errno != 0 {
		err := n.volume.closeUnreturnedFile(ctx, file, errno, true)
		return nil, nil, 0, errnoOf(err)
	}
	child := n.child(ctx, name, out.Attr.Mode, attr.ID)
	h := newHandle(child.Operations().(*node), file, options.Read, options.Write)
	return child, h, gofuse.FOPEN_DIRECT_IO, 0
}

func (n *node) Mkdir(ctx context.Context, name string, mode uint32, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if err := n.volume.check(); err != nil {
		return nil, errnoOf(err)
	}
	access, err := n.volume.namespace()
	if err != nil {
		return nil, errnoOf(err)
	}
	metadata, err := initialPermissions(storageMode(mode))
	if err != nil {
		return nil, errnoOf(err)
	}
	result, err := access.MutateName(ctx, storage.NameCommand{Kind: storage.NameMkdir, Name: n.childName(name), Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{Metadata: metadata}})
	if err != nil {
		return nil, errnoOf(afterMutation(result.Attr != nil, err))
	}
	if result.Attr == nil || result.Attr.Kind != storage.NodeDirectory {
		return nil, syscall.EIO
	}
	attr := *result.Attr
	if errno := n.volume.fillAttr(&out.Attr, attr); errno != 0 {
		return nil, errno
	}
	return n.child(ctx, name, out.Attr.Mode, attr.ID), 0
}

func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	if err := n.volume.check(); err != nil {
		return errnoOf(err)
	}
	access, err := n.volume.namespace()
	if err != nil {
		return errnoOf(err)
	}
	if result, err := access.MutateName(ctx, storage.NameCommand{Kind: storage.NameRemove, Name: n.childName(name), Target: storage.ChildCondition{State: storage.Any}}); err != nil {
		return errnoOf(afterMutation(result.Attr != nil, err))
	}
	n.id.forget(name)
	return 0
}

func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if err := n.volume.check(); err != nil {
		return errnoOf(err)
	}
	access, err := n.volume.namespace()
	if err != nil {
		return errnoOf(err)
	}
	if result, err := access.MutateName(ctx, storage.NameCommand{Kind: storage.NameRemoveDir, Name: n.childName(name), Target: storage.ChildCondition{State: storage.Any}}); err != nil {
		return errnoOf(afterMutation(result.Attr != nil, err))
	}
	n.id.forget(name)
	return 0
}

func (n *node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if err := n.volume.check(); err != nil {
		return errnoOf(err)
	}
	// The volume offers one rename, which replaces whatever is at the destination.
	// RENAME_EXCHANGE and RENAME_NOREPLACE ask for something else, and EINVAL is what
	// renameat2 reports for a flag the filesystem cannot honour.
	if flags != 0 {
		return syscall.EINVAL
	}
	target := newParent.(*node)
	access, err := n.volume.namespace()
	if err != nil {
		return errnoOf(err)
	}
	if result, err := access.MutateName(ctx, storage.NameCommand{Kind: storage.NameRename, Name: n.childName(name), Target: storage.ChildCondition{State: storage.Any}, Destination: &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: target.id.node}, ObservedLeaf: []byte(newName), OutputLeaf: []byte(newName), Expected: storage.ChildCondition{State: storage.Any}}}); err != nil {
		return errnoOf(afterMutation(result.Attr != nil, err))
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
func (n *node) child(ctx context.Context, name string, mode uint32, nodeID uint64) *fs.Inode {
	id := n.id.child(name, mode, nodeID)
	if existing := n.GetChild(name); existing != nil && existing.StableAttr().Ino == id.ino {
		return existing
	}
	return n.NewInode(ctx, &node{volume: n.volume, id: id}, fs.StableAttr{Mode: mode, Ino: id.ino})
}

// --- attributes ----------------------------------------------------------------------

func (v *volume) fillAttr(out *gofuse.Attr, attr storage.Attr) syscall.Errno {
	if attr.ID == 0 || !attr.IsDir() && attr.Size < 0 {
		return syscall.EIO
	}
	mode, errno := attributeMode(attr)
	if errno != 0 {
		return errno
	}
	out.Mode = mode
	if !attr.IsDir() {
		out.Size = uint64(attr.Size)
	}
	accessed, changed := attr.AccessTime, attr.ModTime
	out.Atime, out.Atimensec = uint64(accessed.Unix()), uint32(accessed.Nanosecond())
	out.Mtime, out.Mtimensec = uint64(changed.Unix()), uint32(changed.Nanosecond())
	// Linux presentation retains the modification time when historical change
	// time is unknown. The fallback is not written into authority metadata.
	changeTime := changed
	if attr.ChangeTime != nil {
		changeTime = *attr.ChangeTime
	}
	out.Ctime, out.Ctimensec = uint64(changeTime.Unix()), uint32(changeTime.Nanosecond())
	out.Owner = v.owner
	// Hard links do not exist here (R-FS-4). One is the honest count, and it is also the
	// count that keeps tools which walk a tree from concluding, from a directory's link
	// count, that the directory has no subdirectories worth descending into.
	out.Nlink = 1
	return 0
}

// systemMode renders a storage mode in the bits the kernel uses. A node whose kind we
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

// storageMode decodes the permission and special bits sent by the kernel.
//
// The kernel keeps a node's kind in the same word, and it is dropped rather than
// translated: what comes back is a mode to set, and a node's kind is not something a
// caller sets.
func storageMode(mode uint32) iofs.FileMode {
	requested := iofs.FileMode(mode) & iofs.ModePerm
	for _, bit := range specialModeBits {
		if mode&bit.system != 0 {
			requested |= bit.storage
		}
	}
	return requested
}

func specialBits(mode iofs.FileMode) uint32 {
	var bits uint32
	for _, bit := range specialModeBits {
		if mode&bit.storage != 0 {
			bits |= bit.system
		}
	}
	return bits
}

// The three bits that are neither a node's kind nor one of the nine permission bits. Go
// keeps them well above the permission bits and the kernel keeps them just above, so
// neither direction is a matter of masking.
var specialModeBits = []struct {
	storage iofs.FileMode
	system  uint32
}{
	{iofs.ModeSetuid, syscall.S_ISUID},
	{iofs.ModeSetgid, syscall.S_ISGID},
	{iofs.ModeSticky, syscall.S_ISVTX},
}
