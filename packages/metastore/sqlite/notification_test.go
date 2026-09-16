package sqlite

import (
	"database/sql"
	"errors"
	"reflect"
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
		return 256 + l.Name + l.FromName + l.Content + l.Metadata + l.Target + l.Notification, nil
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
			if c.Node != nil || n.After != nil || len(n.Before.Location.Ancestors) != 2 || n.Before.Location.Ancestors[0].NodeID != uint64(parent.ID) || string(n.Before.Location.Ancestors[0].Name) != "parent" {
				t.Fatalf("lost removal history: %+v", n)
			}
			if (string(c.Name) == "dir") != (n.SubjectKind == storage.NodeDirectory) {
				t.Fatalf("wrong removed type: %+v", n)
			}
		case metastore.Renamed:
			renamed++
			if len(n.Before.Location.Ancestors) != 1 || len(n.After.Location.Ancestors) != 2 || string(n.Before.Location.Ancestors[0].Name) != "parent" || string(n.After.Location.Ancestors[1].Name) != "moved" {
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
			if _, err := tx.ExecContext(t.Context(), `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,directory_revision) VALUES(?,?,?,0,0,0,0,0,1)`, id, s.volume, int64(storage.NodeDirectory)); err != nil {
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

func TestNotificationCombinesOpaqueMetadataAndTimesOnce(t *testing.T) {
	s := notificationStore(t)
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	before, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	start, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	created, changed, access := time.Unix(300, 6).UTC(), time.Unix(400, 7).UTC(), time.Unix(500, 8).UTC()
	metadata := storage.Metadata{{Key: "future.client", Version: 37, Data: []byte{0xff, 0, 0xfe}}}
	if err := s.SetAttr(t.Context(), "file", storage.AttrChange{ExpectedRevision: before.MetadataRevision, Metadata: &metadata, CreationTime: &created, ChangeTime: &changed, AccessTime: &access}); err != nil {
		t.Fatal(err)
	}
	events := readNotifications(t, s, start)
	want := metastore.ChangeAttributes | metastore.ChangeCreationTime | metastore.ChangeTime | metastore.ChangeAccessTime
	if len(events) != 1 || events[0].Notification.ChangeMask != want {
		t.Fatalf("metadata changes=%+v wantmask=%v", events, want)
	}
	after := events[0].Notification.After.Attr
	if after.MetadataRevision != before.MetadataRevision+1 || after.ChangeTime == nil || !after.ChangeTime.Equal(changed) || !reflect.DeepEqual(after.Metadata, metadata) {
		t.Fatalf("lost exact metadata result: %+v", after)
	}
	current, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	stable, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetAttr(t.Context(), "file", storage.AttrChange{ExpectedRevision: before.MetadataRevision, Metadata: &metadata}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("stale metadata update: %v", err)
	}
	unchanged, err := s.Stat(t.Context(), "file")
	if err != nil || !reflect.DeepEqual(unchanged, current) {
		t.Fatalf("stale update changed metadata: got %+v, want %+v, err=%v", unchanged, current, err)
	}
	position, err := s.CommittedPosition(t.Context())
	if err != nil || position != stable {
		t.Fatalf("stale update changed log position: got %d, want %d, err=%v", position, stable, err)
	}
}

func TestNotificationSymlinkOpaqueHintSurvivesRenameRemovalAndSnapshot(t *testing.T) {
	for _, hint := range []byte{0, 1} {
		t.Run(string(rune('0'+hint)), func(t *testing.T) {
			s, session := newFileAuthority(t)
			if err := s.Create(t.Context(), "link"); err != nil {
				t.Fatal(err)
			}
			before, err := s.Stat(t.Context(), "link")
			if err != nil {
				t.Fatal(err)
			}
			retained, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: uint64(before.ID), Claim: storage.AccessClaim{Uses: storage.WriteContent}}, fileActionID(t, session))
			if err != nil {
				t.Fatal(err)
			}
			file, live, err := session.Reference(t.Context(), retained.Reference)
			if err != nil || !live {
				t.Fatalf("retained reference: live=%v, err=%v", live, err)
			}
			metadata := storage.Metadata{{Key: "windows.file", Version: 9, Data: []byte{hint, 0xff}}}
			result, err := file.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: before.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("target"), Metadata: metadata}, fileActionID(t, session))
			if err != nil {
				t.Fatal(err)
			}
			if result.State != storage.FileActionCompleted || result.Observation.Attr.Kind != storage.NodeSymlink {
				t.Fatalf("link conversion did not complete: %+v", result)
			}
			if used, err := s.Usage(t.Context()); err != nil || used != 6 {
				t.Fatalf("link target accounting: used=%d, err=%v", used, err)
			}

			snap, _, err := s.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for !found {
				rows, err := metastore.NewRowResult(1<<20, 0, func(_ int, _ metastore.Row, l metastore.RowPayloadLengths) (int64, error) {
					return 256 + l.Name + l.Content + l.Metadata + l.Target, nil
				})
				if err != nil {
					t.Fatal(err)
				}
				done, err := snap.Next(t.Context(), 10, rows)
				if err != nil {
					t.Fatal(err)
				}
				values, err := rows.Rows()
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range values {
					if string(row.Name) == "link" {
						found = true
						if row.Node.Kind != storage.NodeSymlink || string(row.Node.LinkTarget) != "target" || !reflect.DeepEqual(row.Node.Metadata, metadata) {
							t.Fatalf("snapshot lost opaque link facts: %+v", row)
						}
					}
				}
				if done {
					break
				}
			}
			if err := snap.Close(); err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("link missing from snapshot")
			}
			start, err := s.CommittedPosition(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Rename(t.Context(), "link", "moved"); err != nil {
				t.Fatal(err)
			}
			if err := s.Remove(t.Context(), "moved"); err != nil {
				t.Fatal(err)
			}
			renamed, removed := 0, 0
			for _, event := range readNotifications(t, s, start) {
				if event.Kind == metastore.Renamed || event.Kind == metastore.Removed {
					if event.Kind == metastore.Renamed {
						renamed++
					} else {
						removed++
					}
					image := event.Notification.Before
					if event.Notification.SubjectID != before.ID || image == nil || image.Attr.ID != uint64(before.ID) || event.Notification.SubjectKind != storage.NodeSymlink || string(image.LinkTarget) != "target" || !reflect.DeepEqual(image.Attr.Metadata, metadata) {
						t.Fatalf("history lost opaque link metadata: %+v", event)
					}
					if event.Kind == metastore.Removed && (event.Node != nil || event.Notification.After != nil) {
						t.Fatal("removal recreated node")
					}
				}
			}
			if renamed != 1 || removed != 1 {
				t.Fatalf("link history: renamed=%d removed=%d", renamed, removed)
			}
			if used, err := s.Usage(t.Context()); err != nil || used != 6 {
				t.Fatalf("retained removed link accounting: used=%d, err=%v", used, err)
			}
			if _, err := file.Close(t.Context(), fileActionID(t, session)); err != nil {
				t.Fatal(err)
			}
			if used, err := s.Usage(t.Context()); err != nil || used != 0 {
				t.Fatalf("closed link accounting: used=%d, err=%v", used, err)
			}
		})
	}
}

func TestRenameKeepsEntryIdentityAndAdvancesBothParentRevisions(t *testing.T) {
	s := notificationStore(t)
	for _, name := range []string{"a", "b"} {
		if err := s.Mkdir(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Create(t.Context(), "a/file"); err != nil {
		t.Fatal(err)
	}
	a, err := s.Stat(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Stat(t.Context(), "b")
	if err != nil {
		t.Fatal(err)
	}
	start, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Rename(t.Context(), "a/file", "b/file"); err != nil {
		t.Fatal(err)
	}
	parents := map[int64]bool{}
	renames := 0
	for _, event := range readNotifications(t, s, start) {
		n := event.Notification
		if event.Kind == metastore.Renamed {
			renames++
			old := n.Before.Location.Ancestors[len(n.Before.Location.Ancestors)-1]
			next := n.After.Location.Ancestors[len(n.After.Location.Ancestors)-1]
			if old.EntryID == 0 || old.EntryID != next.EntryID || old.NodeID != next.NodeID || old.ParentID != uint64(a.ID) || next.ParentID != uint64(b.ID) {
				t.Fatalf("rename moved identities: %+v -> %+v", old, next)
			}
			if old.DirectoryRevision != a.DirectoryRevision || next.DirectoryRevision != b.DirectoryRevision+1 {
				t.Fatalf("rename witnesses carry wrong parent revisions: %+v -> %+v", old, next)
			}
		}
		if event.Kind == metastore.Modified && (event.Node.ID == a.ID || event.Node.ID == b.ID) {
			previous := a
			if event.Node.ID == b.ID {
				previous = b
			}
			if n.Before.Attr.DirectoryRevision != previous.DirectoryRevision || n.After.Attr.DirectoryRevision != previous.DirectoryRevision+1 {
				t.Fatalf("parent before/after revision lost: %+v", n)
			}
			parents[event.Node.ID] = true
		}
	}
	if renames != 1 || len(parents) != 2 {
		t.Fatalf("rename events=%d parent events=%d", renames, len(parents))
	}
}
