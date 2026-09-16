package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"slices"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

// captureLocation shares its transaction with the caller's metadata and target
// reads. It retains exact entry facts rather than an already interpreted path.
func (s *Store) captureLocation(ctx context.Context, tx *sql.Tx, rootID, nodeID int64) (storage.EntryLocation, error) {
	if rootID <= 0 || nodeID <= 0 {
		return storage.EntryLocation{}, syscall.EINVAL
	}
	if _, err := s.directoryRevision(ctx, tx, rootID); err != nil {
		return storage.EntryLocation{}, err
	}
	var detached bool
	if err := tx.QueryRowContext(ctx, `SELECT detached FROM nodes WHERE volume=? AND id=?`, s.volume, nodeID).Scan(&detached); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storage.EntryLocation{}, syscall.ESTALE
		}
		return storage.EntryLocation{}, err
	}
	location := storage.EntryLocation{RootNodeID: uint64(rootID), NodeID: uint64(nodeID)}
	if nodeID == rootID {
		if detached {
			return storage.EntryLocation{}, syscall.EIO
		}
		location.State = storage.LocationRoot
		return location, nil
	}
	if detached {
		location.State = storage.LocationDetached
		return location, nil
	}
	location.State = storage.LocationLinked
	seen := make(map[int64]bool)
	namesLeft := storage.MaxLocationNameBytes
	recordsLeft, bytesLeft := s.maxIntegrityRecords, s.maxIntegrityBytes
	id := nodeID
	for id != rootID {
		if err := ctx.Err(); err != nil {
			return storage.EntryLocation{}, err
		}
		if id == s.root {
			return storage.EntryLocation{}, syscall.EXDEV
		}
		if id <= 0 || seen[id] {
			return storage.EntryLocation{}, syscall.EIO
		}
		if len(location.Ancestors) >= storage.MaxLocationDepth || recordsLeft < 1 {
			return storage.EntryLocation{}, syscall.EFBIG
		}
		seen[id] = true
		var entry, parent, revision, length, kind int64
		var class string
		err := tx.QueryRowContext(ctx, `SELECT e.id,e.parent,n.directory_revision,length(e.name),typeof(e.name),n.kind FROM entries e JOIN nodes n ON n.volume=e.volume AND n.id=e.parent WHERE e.volume=? AND e.node=?`, s.volume, id).Scan(&entry, &parent, &revision, &length, &class, &kind)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.EntryLocation{}, syscall.EIO
		}
		if err != nil {
			return storage.EntryLocation{}, err
		}
		if class != "blob" || entry <= 0 || parent <= 0 || revision <= 0 || kind != int64(storage.NodeDirectory) || length <= 0 {
			return storage.EntryLocation{}, syscall.EIO
		}
		if length > storage.MaxEntryNameBytes || length > int64(namesLeft) || length > math.MaxInt64-256 || length+256 > bytesLeft {
			return storage.EntryLocation{}, syscall.EFBIG
		}
		recordsLeft--
		bytesLeft -= length + 256
		namesLeft -= int(length)
		var name []byte
		if err := tx.QueryRowContext(ctx, `SELECT name FROM entries WHERE volume=? AND id=?`, s.volume, entry).Scan(&name); err != nil {
			return storage.EntryLocation{}, err
		}
		if int64(len(name)) != length {
			return storage.EntryLocation{}, syscall.EIO
		}
		at := storage.EntryCondition{ParentID: uint64(parent), DirectoryRevision: storage.DirectoryRevision(revision), EntryID: storage.EntryID(entry), NodeID: uint64(id), Name: name}
		if err := at.Check(); err != nil {
			return storage.EntryLocation{}, errors.Join(syscall.EIO, err)
		}
		location.Ancestors = append(location.Ancestors, at)
		id = parent
	}
	slices.Reverse(location.Ancestors)
	if err := location.Check(); err != nil {
		return storage.EntryLocation{}, errors.Join(syscall.EIO, err)
	}
	return location, nil
}

func (s *Store) directoryRevision(ctx context.Context, tx *sql.Tx, id int64) (storage.DirectoryRevision, error) {
	if id <= 0 {
		return 0, syscall.EINVAL
	}
	var kind, revision int64
	err := tx.QueryRowContext(ctx, `SELECT kind,directory_revision FROM nodes WHERE volume=? AND id=?`, s.volume, id).Scan(&kind, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, syscall.ESTALE
	}
	if err != nil {
		return 0, err
	}
	if kind != int64(storage.NodeDirectory) {
		return 0, syscall.ENOTDIR
	}
	if revision <= 0 {
		return 0, syscall.EIO
	}
	return storage.DirectoryRevision(revision), nil
}
