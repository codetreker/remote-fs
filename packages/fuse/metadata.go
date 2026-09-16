package fuse

import (
	"encoding/binary"
	"github.com/codetreker/remote-fs/packages/storage"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"io/fs"
	"syscall"
	"time"
)

const posixMetadataKey = "posix"
const posixMetadataVersion = 1
const settableMode = fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky

// PermissionDefaults apply only when a node has no POSIX metadata. Explicit zero
// permissions are valid. A nil Options.Permissions selects 0644/0755/0777.
type PermissionDefaults struct {
	File      uint32
	Directory uint32
	Symlink   uint32
}

func (p PermissionDefaults) check() error {
	if (p.File|p.Directory|p.Symlink)&^uint32(07777) != 0 {
		return syscall.EINVAL
	}
	return nil
}

type posixAttributes struct {
	mode   uint32
	uid    uint32
	gid    uint32
	hasUID bool
	hasGID bool
}

func decodePOSIX(version uint32, data []byte) (posixAttributes, error) {
	var value posixAttributes
	if version != posixMetadataVersion || len(data) != 16 {
		return value, syscall.EIO
	}
	flags := binary.LittleEndian.Uint32(data)
	value.mode = binary.LittleEndian.Uint32(data[4:])
	value.uid = binary.LittleEndian.Uint32(data[8:])
	value.gid = binary.LittleEndian.Uint32(data[12:])
	value.hasUID, value.hasGID = flags&2 != 0, flags&4 != 0
	if flags&1 == 0 || flags&^uint32(7) != 0 || value.mode&^uint32(07777) != 0 || !value.hasUID && value.uid != 0 || !value.hasGID && value.gid != 0 {
		return posixAttributes{}, syscall.EIO
	}
	return value, nil
}

func encodePOSIX(value posixAttributes) []byte {
	data := make([]byte, 16)
	flags := uint32(1)
	if value.hasUID {
		flags |= 2
		binary.LittleEndian.PutUint32(data[8:], value.uid)
	}
	if value.hasGID {
		flags |= 4
		binary.LittleEndian.PutUint32(data[12:], value.gid)
	}
	binary.LittleEndian.PutUint32(data, flags)
	binary.LittleEndian.PutUint32(data[4:], value.mode)
	return data
}

func modeBits(mode fs.FileMode) (uint32, error) {
	if mode&^settableMode != 0 {
		return 0, syscall.EINVAL
	}
	return uint32(mode.Perm()) | specialBits(mode), nil
}

type attributeChange struct {
	Mode       *fs.FileMode
	AccessTime *time.Time
	ModTime    *time.Time
}

func (c attributeChange) Empty() bool {
	return c.Mode == nil && c.AccessTime == nil && c.ModTime == nil
}

// goMode converts kernel permission bits to Go permission bits.
//
// The kernel keeps a node's kind in the same word, and it is dropped rather than
// translated: what comes back is a mode to set, and a node's kind is not something a
// caller sets. settableMode is the set this produces.
func goMode(mode uint32) fs.FileMode {
	requested := fs.FileMode(mode) & fs.ModePerm
	for _, bit := range specialModeBits {
		if mode&bit.system != 0 {
			requested |= bit.mode
		}
	}
	return requested
}

func specialBits(mode fs.FileMode) uint32 {
	var bits uint32
	for _, bit := range specialModeBits {
		if mode&bit.mode != 0 {
			bits |= bit.system
		}
	}
	return bits
}

// The three bits that are neither a node's kind nor one of the nine permission bits. Go
// keeps them well above the permission bits and the kernel keeps them just above, so
// neither direction is a matter of masking.
var specialModeBits = []struct {
	mode   fs.FileMode
	system uint32
}{
	{fs.ModeSetuid, syscall.S_ISUID},
	{fs.ModeSetgid, syscall.S_ISGID},
	{fs.ModeSticky, syscall.S_ISVTX},
}

func (v *volume) posix(attr storage.Attr) (posixAttributes, error) {
	if err := attr.Metadata.Check(); err != nil {
		return posixAttributes{}, syscall.EIO
	}
	if value, present := attr.Metadata.Get(posixMetadataKey); present {
		return decodePOSIX(value.Version, value.Data)
	}
	var mode uint32
	switch attr.Kind {
	case storage.NodeRegular:
		mode = v.permissions.File
	case storage.NodeDirectory:
		mode = v.permissions.Directory
	case storage.NodeSymlink:
		mode = v.permissions.Symlink
	default:
		return posixAttributes{}, syscall.EIO
	}
	return posixAttributes{mode: mode}, nil
}

func (v *volume) attributes(attr storage.Attr) (uint32, gofuse.Owner, error) {
	value, err := v.posix(attr)
	if err != nil {
		return 0, gofuse.Owner{}, err
	}
	var kind uint32
	switch attr.Kind {
	case storage.NodeRegular:
		kind = syscall.S_IFREG
	case storage.NodeDirectory:
		kind = syscall.S_IFDIR
	case storage.NodeSymlink:
		kind = syscall.S_IFLNK
	default:
		return 0, gofuse.Owner{}, syscall.EIO
	}
	owner := v.owner
	if value.hasUID {
		owner.Uid = value.uid
	}
	if value.hasGID {
		owner.Gid = value.gid
	}
	return kind | value.mode, owner, nil
}

func initialMetadata(mode uint32) storage.Metadata {
	return storage.Metadata{{Key: posixMetadataKey, Version: posixMetadataVersion, Data: encodePOSIX(posixAttributes{mode: mode & 07777})}}
}

func (v *volume) mergeChange(attr storage.Attr, change attributeChange) (storage.AttrChange, error) {
	out := storage.AttrChange{AccessTime: change.AccessTime, ModTime: change.ModTime}
	if change.Mode == nil {
		return out, nil
	}
	if attr.MetadataRevision == 0 {
		return storage.AttrChange{}, syscall.EIO
	}
	metadata, err := WithPermissions(attr.Metadata, *change.Mode)
	if err != nil {
		return storage.AttrChange{}, err
	}
	out.ExpectedRevision, out.Metadata = attr.MetadataRevision, &metadata
	return out, nil
}

// WithPermissions updates the POSIX permission payload while preserving recorded
// ownership and every other client's metadata. Unknown or damaged POSIX values
// fail with EIO; non-permission Go mode bits fail with EINVAL. The caller must
// condition publication on the metadata revision that supplied metadata.
func WithPermissions(metadata storage.Metadata, mode fs.FileMode) (storage.Metadata, error) {
	bits, err := modeBits(mode)
	if err != nil {
		return nil, err
	}
	if err := metadata.Check(); err != nil {
		return nil, syscall.EIO
	}
	var value posixAttributes
	if encoded, present := metadata.Get(posixMetadataKey); present {
		value, err = decodePOSIX(encoded.Version, encoded.Data)
		if err != nil {
			return nil, err
		}
	}
	value.mode = bits
	return metadata.With(storage.OpaqueMetadata{Key: posixMetadataKey, Version: posixMetadataVersion, Data: encodePOSIX(value)})
}
