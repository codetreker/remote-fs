package changes

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNotificationHistoryValidation(t *testing.T) {
	_, tx := logFixture(t)
	for _, kind := range []metastore.ChangeKind{metastore.Created, metastore.Modified, metastore.Renamed, metastore.Removed} {
		if err := Record(t.Context(), tx, 1, fileChange(kind)); err != nil {
			t.Fatal(err)
		}
	}
	volume := int64(1)
	for _, scope := range []*int64{nil, &volume} {
		if err := ValidateNotifications(t.Context(), tx, scope, 1000, 8<<20); err != nil {
			t.Fatal(err)
		}
	}
	execLogSQL(t, tx, `UPDATE changes SET notification=CAST('{}' AS BLOB) WHERE position=1`)
	if err := ValidateNotifications(t.Context(), tx, nil, 1000, 8<<20); !errors.Is(err, syscall.EIO) {
		t.Fatalf("corrupt facts: %v", err)
	}
	execLogSQL(t, tx, `UPDATE changes SET kind=99 WHERE position=1`)
	if err := ValidateNotifications(t.Context(), tx, nil, 1000, 8<<20); !errors.Is(err, syscall.EIO) {
		t.Fatalf("invalid kind: %v", err)
	}
	execLogSQL(t, tx, `UPDATE changes SET kind='bad' WHERE position=1`)
	if err := ValidateNotifications(t.Context(), tx, nil, 1000, 8<<20); err == nil {
		t.Fatal("invalid SQL scalar accepted")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateNotifications(t.Context(), tx, nil, 1000, 8<<20); err == nil {
		t.Fatal("closed transaction accepted")
	}
}

func TestNotificationBytesAreReservedBeforePayloadLoad(t *testing.T) {
	for _, value := range []string{"X''", "'text'", "zeroblob(262145)", "CAST('{}' AS BLOB)"} {
		t.Run(value, func(t *testing.T) {
			_, tx := logFixture(t)
			appendLog(t, tx, 1)
			execLogSQL(t, tx, `UPDATE changes SET notification=`+value)
			result := changeResult(t, 1)
			_, err := ReadPage(t.Context(), tx, 1, 100, 0, 1, result)
			if err == nil {
				t.Fatal("invalid notification read succeeded")
			}
			if strings.HasPrefix(value, "zeroblob") && !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("oversized facts: %v", err)
			}
		})
	}
	_, tx := logFixture(t)
	change := fileChange(metastore.Created)
	data, err := metastore.EncodeNotification(change)
	if err != nil {
		t.Fatal(err)
	}
	if err := Record(t.Context(), tx, 1, change); err != nil {
		t.Fatal(err)
	}
	result, err := metastore.NewChangeResult(int64(len(data))-1, 0, func(_ int, c metastore.Change, l metastore.ChangePayloadLengths) (int64, error) {
		if c.Notification != nil || l.Notification != int64(len(data)) {
			t.Fatalf("payload loaded before reserve or wrong size: %+v %+v", c, l)
		}
		return l.Notification, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPage(t.Context(), tx, 1, 100, 0, 1, result); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("facts not charged: %v", err)
	}
	change.Notification = nil
	if err := Record(t.Context(), tx, 1, change); !errors.Is(err, syscall.EIO) {
		t.Fatalf("missing facts recorded: %v", err)
	}
}

func TestNotificationSymlinkTypeSurvivesLogPage(t *testing.T) {
	_, tx := logFixture(t)
	c := fileChange(metastore.Created)
	c.Node.Kind = storage.NodeSymlink
	c.Node.LinkTarget = []byte("target")
	c.Node.Content = ""
	c.Node.Size = 6
	c.Notification.SubjectKind = storage.NodeSymlink
	c.Notification.After.Attr = c.Node.Attr()
	c.Notification.After.LinkTarget = bytes.Clone(c.Node.LinkTarget)
	if err := Record(t.Context(), tx, 1, c); err != nil {
		t.Fatal(err)
	}
	result := changeResult(t, 1)
	if _, err := ReadPage(t.Context(), tx, 1, 100, 0, 1, result); err != nil {
		t.Fatal(err)
	}
	changes, err := result.Changes()
	if err != nil || len(changes) != 1 || changes[0].Notification.SubjectKind != storage.NodeSymlink {
		t.Fatalf("symlink notification %+v %v", changes, err)
	}
}

func TestRecordPayloadLimitBoundaryAndCallerRollback(t *testing.T) {
	for _, extra := range []int{0, 1} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			db, tx := logFixture(t)
			c := fileChange(metastore.Created)
			encoded, err := metastore.EncodeNotification(c)
			if err != nil {
				t.Fatal(err)
			}
			c.Node.Content = metastore.Key(strings.Repeat("x", metastore.MaxChangePayloadBytes-len(c.Name)-len(encoded)-6+extra))
			execLogSQL(t, tx, `UPDATE nodes SET metadata_revision=2 WHERE id=2`)
			err = Record(t.Context(), tx, 1, c)
			if extra == 0 {
				if err != nil {
					t.Fatalf("legal payload boundary: %v", err)
				}
				result := changeResult(t, 1)
				if _, err := ReadPage(t.Context(), tx, 1, 100, 0, 1, result); err != nil {
					t.Fatalf("boundary read: %v", err)
				}
				out, err := result.Changes()
				if err != nil || len(out) != 1 || out[0].Node.Content != c.Node.Content {
					t.Fatalf("boundary contents: %v", err)
				}
			} else if !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("overflow admitted: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			var revision, count, position int64
			if err := db.QueryRow(`SELECT metadata_revision,(SELECT count(*) FROM changes),(SELECT committed_position FROM logs WHERE volume=1) FROM nodes WHERE id=2`).Scan(&revision, &count, &position); err != nil {
				t.Fatal(err)
			}
			if revision != 1 || count != 0 || position != 0 {
				t.Fatalf("rolled-back mutation leaked: revision=%d rows=%d position=%d", revision, count, position)
			}
		})
	}
}

func TestReadPageRejectsAggregateOverflowBeforeReservingPayload(t *testing.T) {
	_, tx := logFixture(t)
	appendLog(t, tx, 1)
	execLogSQL(t, tx, `UPDATE changes SET content=?`, strings.Repeat("x", metastore.MaxChangePayloadBytes))
	called := false
	result, err := metastore.NewChangeResult(1<<20, 0, func(_ int, _ metastore.Change, l metastore.ChangePayloadLengths) (int64, error) {
		called = true
		return l.Name + l.FromName + l.Content + l.Metadata + l.Target + l.Notification, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPage(t.Context(), tx, 1, 100, 0, 1, result); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized stored row: %v", err)
	}
	if called {
		t.Fatal("oversized stored row reached reservation")
	}
}

type notificationAdmissionProbe struct {
	*sql.Tx
	payloadReads int
}

func (p *notificationAdmissionProbe) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	p.payloadReads++
	return p.Tx.QueryRowContext(ctx, query, args...)
}

func TestNotificationIntegrityRejectsOversizedPayloadBeforeLoad(t *testing.T) {
	for _, update := range []string{
		`notification=zeroblob(262145)`,
		`name=zeroblob(524289)`,
		`content=CAST(zeroblob(524289) AS TEXT)`,
	} {
		t.Run(update, func(t *testing.T) {
			_, tx := logFixture(t)
			appendLog(t, tx, 1)
			execLogSQL(t, tx, `UPDATE changes SET `+update)
			probe := &notificationAdmissionProbe{Tx: tx}
			if err := ValidateNotifications(t.Context(), probe, nil, 1000, 8<<20); !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("oversized history: %v", err)
			}
			if probe.payloadReads != 0 {
				t.Fatalf("loaded payload before admission: %d reads", probe.payloadReads)
			}
		})
	}
}

func TestHistoricalImagesOutliveRemovedEntriesAndLaterMetadata(t *testing.T) {
	_, tx := logFixture(t)
	c := fileChange(metastore.Removed)
	c.Notification.SubjectKind = storage.NodeSymlink
	before := c.Notification.Before
	before.Attr.Kind = storage.NodeSymlink
	before.LinkTarget = []byte("target")
	before.Attr.Size = int64(len(before.LinkTarget))
	before.Attr.Metadata = storage.Metadata{{Key: "client", Version: 3, Data: []byte{0xff, 1}}}
	if err := Record(t.Context(), tx, 1, c); err != nil {
		t.Fatal(err)
	}
	execLogSQL(t, tx, `DELETE FROM entries WHERE node=2`)
	execLogSQL(t, tx, `DELETE FROM nodes WHERE id=2`)
	before.Attr.Metadata[0].Data[1] = 2
	result := changeResult(t, 1)
	if _, err := ReadPage(t.Context(), tx, 1, 100, 0, 1, result); err != nil {
		t.Fatal(err)
	}
	changes, err := result.Changes()
	if err != nil || len(changes) != 1 || changes[0].Node != nil || changes[0].Notification.Before.Attr.Metadata[0].Data[1] != 1 || changes[0].Notification.Before.Location.Ancestors[0].EntryID != 5 {
		t.Fatalf("history lost removed image: %+v %v", changes, err)
	}
}

func TestHistoryIdentitySummaryMustMatchImages(t *testing.T) {
	for _, summary := range []int{4, 6} {
		t.Run(fmt.Sprint(summary), func(t *testing.T) {
			_, tx := logFixture(t)
			appendLog(t, tx, 1)
			execLogSQL(t, tx, `UPDATE changes SET identity_high_water=?`, summary)
			if err := ValidateNotifications(t.Context(), tx, nil, 1000, 8<<20); !errors.Is(err, syscall.EIO) {
				t.Fatalf("corrupt summary: %v", err)
			}
			result := changeResult(t, 1)
			if _, err := ReadPage(t.Context(), tx, 1, 100, 0, 1, result); !errors.Is(err, syscall.EIO) {
				t.Fatalf("corrupt summary page: %v", err)
			}
		})
	}
}

func TestRecordRoundTripOwnsOpaqueMetadataAndKnownTimeInstants(t *testing.T) {
	_, tx := logFixture(t)
	c := fileChange(metastore.Created)
	creation, changed := time.Unix(-100, 7).UTC(), time.Unix(0, 11).UTC()
	c.Node.CreationTime = &creation
	c.Node.ChangeTime = &changed
	c.Node.Metadata = storage.Metadata{{Key: "client", Version: 41, Data: []byte{0xff, 0, 1}}}
	c.Notification.After.Attr = c.Node.Attr()
	if err := Record(t.Context(), tx, 1, c); err != nil {
		t.Fatal(err)
	}
	c.Node.Metadata[0].Data[0] = 1
	creation = creation.Add(time.Second)
	result := changeResult(t, 1)
	if _, err := ReadPage(t.Context(), tx, 1, 100, 0, 1, result); err != nil {
		t.Fatal(err)
	}
	changes, err := result.Changes()
	if err != nil {
		t.Fatal(err)
	}
	got := changes[0].Node
	if got.Metadata[0].Version != 41 || got.Metadata[0].Data[0] != 0xff || got.CreationTime == nil || !got.CreationTime.Equal(time.Unix(-100, 7)) || got.ChangeTime == nil || !got.ChangeTime.Equal(time.Unix(0, 11)) {
		t.Fatalf("lost opaque metadata or time facts: %+v", got)
	}
	if err := ValidateNotifications(t.Context(), tx, nil, 1000, 8<<20); err != nil {
		t.Fatal(err)
	}
}
