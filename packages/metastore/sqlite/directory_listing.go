package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Store) listDirectoryPage(ctx context.Context, tx *sql.Tx, parent int64, request storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	if err := request.Check(); err != nil {
		return storage.DirectoryPage{}, err
	}
	if !request.Cursor.IsZero() && request.Cursor.ParentID != uint64(parent) {
		return storage.DirectoryPage{}, syscall.EINVAL
	}
	revision, err := s.directoryRevision(ctx, tx, parent)
	if err != nil {
		return storage.DirectoryPage{}, err
	}
	if request.Revision != 0 && request.Revision != revision {
		return storage.DirectoryPage{}, syscall.EAGAIN
	}
	if request.MaxBytes < storage.DirectoryPageBaseBytes {
		return storage.DirectoryPage{}, syscall.EFBIG
	}
	after := request.Cursor.After
	if after == nil {
		after = []byte{}
	}
	rows, err := tx.QueryContext(ctx, `SELECT e.id,e.node,length(e.name),length(n.metadata),typeof(e.name) FROM entries e LEFT JOIN nodes n ON n.volume=e.volume AND n.id=e.node WHERE e.volume=? AND e.parent=? AND e.name>? ORDER BY e.name LIMIT ?`, s.volume, parent, after, request.MaxEntries+1)
	if err != nil {
		return storage.DirectoryPage{}, err
	}
	type reservation struct{ entry, node, nameBytes, metadataBytes int64 }
	var reserved []reservation
	remaining := int64(request.MaxBytes - storage.DirectoryPageBaseBytes)
	page := storage.DirectoryPage{ParentID: uint64(parent), Revision: revision, Done: true}
	scanErr := func() error {
		defer rows.Close()
		for rows.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(reserved) >= request.MaxEntries {
				page.Done = false
				break
			}
			if int64(len(reserved)) >= s.maxIntegrityRecords {
				return syscall.EFBIG
			}
			var r reservation
			var metadataLength sql.NullInt64
			var class string
			if err := rows.Scan(&r.entry, &r.node, &r.nameBytes, &metadataLength, &class); err != nil {
				return err
			}
			if !metadataLength.Valid {
				return syscall.EIO
			}
			r.metadataBytes = metadataLength.Int64
			if r.entry <= 0 || r.node <= 0 || class != "blob" || r.nameBytes <= 0 || r.metadataBytes < 6 {
				return syscall.EIO
			}
			if r.nameBytes > storage.MaxEntryNameBytes || r.metadataBytes > storage.MaxMetadataBytes {
				return syscall.EFBIG
			}
			charge, err := storage.DirectoryEntryBytes(int(r.nameBytes), int(r.metadataBytes))
			if err != nil {
				return err
			}
			if int64(charge) > remaining {
				if len(reserved) == 0 {
					return syscall.EFBIG
				}
				page.Done = false
				break
			}
			if int64(request.MaxBytes-storage.DirectoryPageBaseBytes)-remaining+int64(charge) > s.maxIntegrityBytes {
				return syscall.EFBIG
			}
			remaining -= int64(charge)
			reserved = append(reserved, r)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	}()
	if scanErr != nil {
		return storage.DirectoryPage{}, scanErr
	}
	for _, r := range reserved {
		if err := ctx.Err(); err != nil {
			return storage.DirectoryPage{}, err
		}
		var name []byte
		if err := tx.QueryRowContext(ctx, `SELECT name FROM entries WHERE volume=? AND id=? AND parent=? AND node=?`, s.volume, r.entry, parent, r.node).Scan(&name); err != nil {
			return storage.DirectoryPage{}, err
		}
		if int64(len(name)) != r.nameBytes {
			return storage.DirectoryPage{}, syscall.EIO
		}
		var scan nodeAttrScan
		if err := tx.QueryRowContext(ctx, `SELECT `+nodeAttrColumns+` FROM nodes n WHERE n.volume=? AND n.id=?`, s.volume, r.node).Scan(scan.fields()...); err != nil {
			return storage.DirectoryPage{}, err
		}
		attr, err := scan.attr()
		if err != nil {
			return storage.DirectoryPage{}, err
		}
		size, err := attr.Metadata.EncodedSize()
		if err != nil || int64(size) != r.metadataBytes {
			return storage.DirectoryPage{}, syscall.EIO
		}
		page.Entries = append(page.Entries, storage.DirectoryEntry{EntryID: storage.EntryID(r.entry), Name: name, Attr: attr})
	}
	if !page.Done {
		page.Next = storage.DirectoryCursor{ParentID: uint64(parent), Revision: revision, After: bytes.Clone(page.Entries[len(page.Entries)-1].Name)}
	}
	if err := page.Check(); err != nil {
		return storage.DirectoryPage{}, err
	}
	return page, nil
}

func (s *Store) lookupDirectoryEntry(ctx context.Context, tx *sql.Tx, parent int64, name []byte) (storage.EntryLookup, error) {
	if err := storage.CheckEntryName(name); err != nil {
		return storage.EntryLookup{}, err
	}
	revision, err := s.directoryRevision(ctx, tx, parent)
	if err != nil {
		return storage.EntryLookup{}, err
	}
	result := storage.EntryLookup{ParentID: uint64(parent), DirectoryRevision: revision, Name: bytes.Clone(name)}
	var entry, node int64
	err = tx.QueryRowContext(ctx, `SELECT id,node FROM entries WHERE volume=? AND parent=? AND name=?`, s.volume, parent, name).Scan(&entry, &node)
	if errors.Is(err, sql.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return storage.EntryLookup{}, err
	}
	if entry <= 0 || node <= 0 {
		return storage.EntryLookup{}, syscall.EIO
	}
	var metadataBytes int64
	err = tx.QueryRowContext(ctx, `SELECT length(metadata) FROM nodes WHERE volume=? AND id=?`, s.volume, node).Scan(&metadataBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.EntryLookup{}, syscall.EIO
	}
	if err != nil {
		return storage.EntryLookup{}, err
	}
	if metadataBytes < 6 {
		return storage.EntryLookup{}, syscall.EIO
	}
	if metadataBytes > storage.MaxMetadataBytes {
		return storage.EntryLookup{}, syscall.EFBIG
	}
	charge, err := storage.DirectoryEntryBytes(len(name), int(metadataBytes))
	if err != nil {
		return storage.EntryLookup{}, err
	}
	if s.maxIntegrityRecords < 1 || int64(charge) > s.maxIntegrityBytes {
		return storage.EntryLookup{}, syscall.EFBIG
	}
	var scan nodeAttrScan
	if err := tx.QueryRowContext(ctx, `SELECT `+nodeAttrColumns+` FROM nodes n WHERE n.volume=? AND n.id=?`, s.volume, node).Scan(scan.fields()...); err != nil {
		return storage.EntryLookup{}, err
	}
	attr, err := scan.attr()
	if err != nil {
		return storage.EntryLookup{}, err
	}
	result.Found = true
	result.EntryID = storage.EntryID(entry)
	result.Attr = attr
	if err := result.Check(name); err != nil {
		return storage.EntryLookup{}, err
	}
	return result, nil
}

func (f *fileReference) LookupAt(ctx context.Context, name []byte) (storage.EntryLookup, error) {
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.EntryLookup{}, err
	}
	defer done()
	if err := f.check(0); err != nil {
		return storage.EntryLookup{}, err
	}
	var result storage.EntryLookup
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		result, err = f.session.store.lookupDirectoryEntry(ctx, tx, f.id, name)
		return err
	})
	return result, err
}
