package sqlite

import (
	"context"
	"database/sql"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *windowsFile) listWindowsChildren(ctx context.Context, tx *sql.Tx, result *storage.WindowsListResult) error {
	s := f.session.store
	rows, err := tx.QueryContext(ctx, `SELECT length(CAST(e.name AS BLOB)),`+nodeAttrColumns+`,n.windows_creation_sec,n.windows_creation_nsec,n.windows_change_sec,n.windows_change_nsec,n.windows_attributes FROM entries e JOIN nodes n ON n.id=e.node WHERE e.volume=? AND e.parent=? ORDER BY e.name`, s.volume, f.id)
	if err != nil {
		return err
	}
	type reserved struct {
		node        int64
		bytes       int64
		reservation *storage.WindowsListReservation
	}
	var entries []reserved
	for rows.Next() {
		var nameBytes, creationSec, changeSec int64
		var creationNsec, changeNsec int32
		var attributes uint32
		var node nodeAttrScan
		fields := append([]any{&nameBytes}, node.fields()...)
		fields = append(fields, &creationSec, &creationNsec, &changeSec, &changeNsec, &attributes)
		if err := rows.Scan(fields...); err != nil {
			rows.Close()
			return err
		}
		if creationNsec < 0 || creationNsec >= 1e9 || changeNsec < 0 || changeNsec >= 1e9 || attributes&^(storage.WindowsSettableDOSAttributes|storage.WindowsDOSDirectory) != 0 {
			rows.Close()
			return syscall.EIO
		}
		if err := checkWindowsKindAttributes(node.attr().Mode, attributes); err != nil {
			rows.Close()
			return err
		}
		if node.attr().IsDir() {
			attributes |= storage.WindowsDOSDirectory
		}
		attr := storage.WindowsBasicAttr{Attr: node.attr(), CreationTime: time.Unix(creationSec, int64(creationNsec)).UTC(), ChangeTime: time.Unix(changeSec, int64(changeNsec)).UTC(), DOSAttributes: attributes, DeletePending: s.fileDomain.windows.access.DeletePending(uint64(node.id))}
		reservation, err := result.Reserve(nameBytes, attr)
		if err != nil {
			rows.Close()
			return err
		}
		entries = append(entries, reserved{node: node.id, bytes: nameBytes, reservation: reservation})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, entry := range entries {
		var name []byte
		if err := tx.QueryRowContext(ctx, `SELECT CASE WHEN length(CAST(name AS BLOB))=? THEN name ELSE NULL END FROM entries WHERE volume=? AND parent=? AND node=?`, entry.bytes, s.volume, f.id, entry.node).Scan(&name); err != nil {
			return err
		}
		if int64(len(name)) != entry.bytes {
			return syscall.EIO
		}
		if err := entry.reservation.Commit(string(name)); err != nil {
			return err
		}
	}
	return nil
}
