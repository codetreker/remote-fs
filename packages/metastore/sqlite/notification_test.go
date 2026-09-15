package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/storage"
)

func notificationStore(t *testing.T) *Store {
	t.Helper()
	s, err := open(t.Context(), t.TempDir()+"/metadata.db", "notifications", "", 0, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func readNotifications(t *testing.T, s *Store, after metastore.Position) []metastore.Change {
	t.Helper()
	result, err := metastore.NewChangeResult(4<<20, 0, func(_ int, _ metastore.Change, l metastore.ChangePayloadLengths) (int64, error) {
		return 256 + l.Name + l.FromName + l.Content + l.Notification, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Since(t.Context(), after, 1000, result); err != nil {
		t.Fatal(err)
	}
	out, err := result.Changes()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestNotificationKeepsDeletedTypesAndHistoricalAncestors(t *testing.T) {
	s := notificationStore(t)
	for _, name := range []string{"parent", "target"} {
		if err := s.Mkdir(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	parent, err := s.Stat(t.Context(), "parent")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "parent/file"); err != nil {
		t.Fatal(err)
	}
	if err := s.Mkdir(t.Context(), "parent/dir"); err != nil {
		t.Fatal(err)
	}
	start, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "parent/file"); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveDir(t.Context(), "parent/dir"); err != nil {
		t.Fatal(err)
	}
	if err := s.Rename(t.Context(), "parent", "target/moved"); err != nil {
		t.Fatal(err)
	}
	var removed, renamed int
	for _, c := range readNotifications(t, s, start) {
		if err := metastore.ValidateNotification(c); err != nil {
			t.Fatal(err)
		}
		n := c.Notification
		switch c.Kind {
		case metastore.Removed:
			removed++
			if c.Node != nil || n.After != nil || len(n.Before.Ancestors) != 2 || n.Before.Ancestors[1].DirectoryID != parent.ID || string(n.Before.Ancestors[1].Name) != "parent" {
				t.Fatalf("lost removal history: %+v", n)
			}
			if (string(c.Name) == "dir") != (n.SubjectKind == fs.ModeDir) {
				t.Fatalf("wrong removed type: %+v", n)
			}
		case metastore.Renamed:
			renamed++
			if len(n.Before.Ancestors) != 1 || len(n.After.Ancestors) != 2 || string(n.Before.LeafName) != "parent" || string(n.After.LeafName) != "moved" {
				t.Fatalf("lost rename scope: %+v", n)
			}
		case metastore.Modified:
			if n.ChangeMask != metastore.ChangeModTime|metastore.ChangeTime {
				t.Fatalf("parent touch broadened: %v", n.ChangeMask)
			}
		}
	}
	if removed != 2 || renamed != 1 {
		t.Fatalf("removed=%d renamed=%d", removed, renamed)
	}
}

func TestNotificationModificationTracksOnlyActualAttributes(t *testing.T) {
	s := notificationStore(t)
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	start, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(123, 4)
	if err := s.SetAttr(t.Context(), "file", storage.AttrChange{AccessTime: &at}); err != nil {
		t.Fatal(err)
	}
	changes := readNotifications(t, s, start)
	if len(changes) != 1 || changes[0].Notification.ChangeMask != metastore.ChangeAccessTime|metastore.ChangeTime {
		t.Fatalf("attribute changes: %+v", changes)
	}
}

func TestNotificationDepthRefusalRollsBackMutation(t *testing.T) {
	s := notificationStore(t)
	path := strings.Repeat("d/", metastore.MaxNotificationAncestors-1)
	if err := s.mutate(t.Context(), func(tx *sql.Tx) error {
		parent := s.root
		for range metastore.MaxNotificationAncestors - 1 {
			id, err := dbstate.AllocateNodeID(t.Context(), tx)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(t.Context(), `INSERT INTO nodes(id,volume,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec) VALUES(?,?,?,0,0,0,0,0)`, id, s.volume, int64(fs.ModeDir|0755)); err != nil {
				return err
			}
			if err := s.link(t.Context(), tx, parent, []byte("d"), id); err != nil {
				return err
			}
			parent = id
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	start, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Mkdir(t.Context(), path+"last"); err != nil {
		t.Fatal(err)
	}
	stable, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stable <= start {
		t.Fatal("boundary mutation not recorded")
	}
	if err := s.Create(t.Context(), path+"last/file"); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("depth refusal: %v", err)
	}
	end, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if end != stable {
		t.Fatalf("failed mutation advanced history: %d != %d", end, stable)
	}
	if _, err := s.Stat(t.Context(), path+"last/file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed mutation left file: %v", err)
	}
}

func TestNotificationCombinesWindowsAndNodeMetadataOnce(t *testing.T) {
	s := notificationStore(t)
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	start, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.mutate(t.Context(), func(tx *sql.Tx) error {
		before, err := s.resolve(t.Context(), tx, "file")
		if err != nil {
			return err
		}
		at := time.Unix(400, 7)
		if err := applyChange(t.Context(), tx, before, storage.AttrChange{AccessTime: &at}); err != nil {
			return err
		}
		return s.recordChangedMask(t.Context(), tx, before, metastore.ChangeAttributes|metastore.ChangeCreationTime|metastore.ChangeTime)
	}); err != nil {
		t.Fatal(err)
	}
	changes := readNotifications(t, s, start)
	want := metastore.ChangeAccessTime | metastore.ChangeAttributes | metastore.ChangeCreationTime | metastore.ChangeTime
	if len(changes) != 1 || changes[0].Notification.ChangeMask != want {
		t.Fatalf("metadata notification %+v, want one mask %v", changes, want)
	}
}

func TestNotificationWindowsNativeMetadataAndContent(t *testing.T) {
	_, session := windowsAuthority(t)
	s := session.store
	file := windowsOpen(t, session, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	created, changed := time.Unix(100, 1), time.Unix(200, 2)
	attributes := uint32(storage.WindowsDOSReadOnly | storage.WindowsDOSHidden)
	start, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	result, err := file.SetAttr(t.Context(), storage.WindowsAttrChange{CreationTime: &created, ChangeTime: &changed, DOSAttributes: &attributes}, windowsActionID(t, session))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Attr.CreationTime.Equal(created) || !result.Attr.ChangeTime.Equal(changed) || result.Attr.DOSAttributes != attributes {
		t.Fatalf("explicit metadata overwritten: %+v", result.Attr)
	}
	changes := readNotifications(t, s, start)
	want := metastore.ChangeCreationTime | metastore.ChangeTime | metastore.ChangeAttributes
	if len(changes) != 1 || changes[0].Notification.ChangeMask != want {
		t.Fatalf("native metadata notification %+v, want %v", changes, want)
	}
	start = changes[0].Position
	attributes = storage.WindowsDOSArchive
	if _, err := file.SetAttr(t.Context(), storage.WindowsAttrChange{CreationTime: &created, ChangeTime: &changed, DOSAttributes: &attributes}, windowsActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	changes = readNotifications(t, s, start)
	if len(changes) != 1 || changes[0].Notification.ChangeMask != metastore.ChangeAttributes {
		t.Fatalf("unchanged timestamps notified: %+v", changes)
	}
	start = changes[0].Position
	windowsPublish(t, file, 0, 4)
	changes = readNotifications(t, s, start)
	want = metastore.ChangeSize | metastore.ChangeContent | metastore.ChangeModTime | metastore.ChangeTime
	if len(changes) != 1 || changes[0].Notification.ChangeMask != want {
		t.Fatalf("native content notification %+v, want %v", changes, want)
	}
	attr, err := file.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !attr.ChangeTime.After(changed) {
		t.Fatalf("content change timestamp not persisted: %+v", attr)
	}
}

func TestNotificationWindowsSymlinkSurvivesHistoryAndSnapshot(t *testing.T) {
	_, session := windowsAuthority(t)
	s := session.store
	file := windowsOpen(t, session, "link", storage.WindowsAllAccess, storage.WindowsShareAll)
	start, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.SetLink(t.Context(), "target", windowsActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	changes := readNotifications(t, s, start)
	if len(changes) != 1 || changes[0].Notification.SubjectKind != fs.ModeSymlink || changes[0].Node.Size != 6 {
		t.Fatalf("symlink history %+v", changes)
	}
	snapshot, _, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	rows, err := metastore.NewRowResult(4096, 0, func(_ int, _ metastore.Row, l metastore.RowPayloadLengths) (int64, error) {
		return 256 + l.Name + l.Content, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.Next(t.Context(), 100, rows); err != nil {
		t.Fatal(err)
	}
	nodes, err := rows.Rows()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, row := range nodes {
		if row.Node.ID == file.id {
			found = true
			if row.Node.Mode.Type() != fs.ModeSymlink || row.Node.Size != 6 {
				t.Fatalf("snapshot symlink %+v", row)
			}
		}
	}
	if !found {
		t.Fatal("snapshot lost symlink")
	}
}

func TestNotificationPreservesFileAndDirectorySymlinkHintsAfterRenameAndDelete(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(fmt.Sprint(directory), func(t *testing.T) {
			_, session := windowsAuthority(t)
			s := session.store
			if directory {
				if err := s.Mkdir(t.Context(), "link"); err != nil {
					t.Fatal(err)
				}
			}
			file := windowsOpen(t, session, "link", storage.WindowsAllAccess, storage.WindowsShareAll)
			if _, err := file.SetLink(t.Context(), "target", windowsActionID(t, session)); err != nil {
				t.Fatal(err)
			}
			start, err := s.CommittedPosition(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Rename(t.Context(), "link", "renamed"); err != nil {
				t.Fatal(err)
			}
			if err := s.Remove(t.Context(), "renamed"); err != nil {
				t.Fatal(err)
			}
			var renames, removes int
			for _, c := range readNotifications(t, s, start) {
				if c.Notification.SubjectID != file.id {
					continue
				}
				if c.Notification.SubjectKind != fs.ModeSymlink || c.Notification.Directory != directory {
					t.Fatalf("lost historical directory hint: %+v", c.Notification)
				}
				switch c.Kind {
				case metastore.Renamed:
					renames++
				case metastore.Removed:
					removes++
					if c.Node != nil {
						t.Fatal("removed notification changed replica semantics")
					}
				}
			}
			if renames != 1 || removes != 1 {
				t.Fatalf("rename=%d remove=%d", renames, removes)
			}
		})
	}
}

func TestNotificationSymlinkHintRejectsMissingOrCorruptMetadata(t *testing.T) {
	s := notificationStore(t)
	if err := s.Create(t.Context(), "link"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "link")
	if err != nil {
		t.Fatal(err)
	}
	node.Mode = fs.ModeSymlink
	for _, value := range []any{int64(0), int64(storage.WindowsDOSDirectory), "bad", int64(-1), int64(1) << 32, int64(1) << 30} {
		tx, err := s.write.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE nodes SET windows_attributes=? WHERE id=?`, value, node.ID); err != nil {
			t.Fatal(err)
		}
		directory, err := s.notificationDirectory(t.Context(), tx, node)
		switch value {
		case int64(0):
			if err != nil || directory {
				t.Fatalf("file link hint: %t %v", directory, err)
			}
		case int64(storage.WindowsDOSDirectory):
			if err != nil || !directory {
				t.Fatalf("directory link hint: %t %v", directory, err)
			}
		default:
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid hint %v: %v", value, err)
			}
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.inspect(t.Context(), func(tx *sql.Tx) error {
		missing := node
		missing.ID = -1
		_, err := s.notificationDirectory(t.Context(), tx, missing)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("missing hint defaulted: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
