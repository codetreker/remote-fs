package changes

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
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
		if err := ValidateNotifications(t.Context(), tx, scope); err != nil {
			t.Fatal(err)
		}
	}
	execLogSQL(t, tx, `UPDATE changes SET notification=CAST('{}' AS BLOB) WHERE position=1`)
	if err := ValidateNotifications(t.Context(), tx, nil); !errors.Is(err, syscall.EIO) {
		t.Fatalf("corrupt facts: %v", err)
	}
	execLogSQL(t, tx, `UPDATE changes SET kind=99 WHERE position=1`)
	if err := ValidateNotifications(t.Context(), tx, nil); !errors.Is(err, syscall.EIO) {
		t.Fatalf("invalid kind: %v", err)
	}
	execLogSQL(t, tx, `UPDATE changes SET kind='bad' WHERE position=1`)
	if err := ValidateNotifications(t.Context(), tx, nil); err == nil {
		t.Fatal("invalid SQL scalar accepted")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateNotifications(t.Context(), tx, nil); err == nil {
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
	c.Node.Mode = fs.ModeSymlink | 0777
	c.Node.Content = ""
	c.Node.Size = 6
	c.Notification.SubjectKind = fs.ModeSymlink
	if err := Record(t.Context(), tx, 1, c); err != nil {
		t.Fatal(err)
	}
	result := changeResult(t, 1)
	if _, err := ReadPage(t.Context(), tx, 1, 100, 0, 1, result); err != nil {
		t.Fatal(err)
	}
	changes, err := result.Changes()
	if err != nil || len(changes) != 1 || changes[0].Notification.SubjectKind != fs.ModeSymlink {
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
			c.Node.Content = metastore.Key(strings.Repeat("x", metastore.MaxChangePayloadBytes-len(c.Name)-len(encoded)+extra))
			execLogSQL(t, tx, `UPDATE nodes SET mode=384 WHERE id=2`)
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
			var mode, count, position int64
			if err := db.QueryRow(`SELECT mode,(SELECT count(*) FROM changes),(SELECT committed_position FROM logs WHERE volume=1) FROM nodes WHERE id=2`).Scan(&mode, &count, &position); err != nil {
				t.Fatal(err)
			}
			if mode != 420 || count != 0 || position != 0 {
				t.Fatalf("rolled-back mutation leaked: mode=%d rows=%d position=%d", mode, count, position)
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
		return l.Name + l.FromName + l.Content + l.Notification, nil
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
