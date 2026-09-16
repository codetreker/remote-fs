package changes

import (
	"context"
	"database/sql"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNotificationRootChecksEachPresentImage(t *testing.T) {
	image := func(root uint64) *metastore.EventImage {
		return &metastore.EventImage{Location: storage.EntryLocation{RootNodeID: root}}
	}
	for _, test := range []struct {
		name    string
		facts   *metastore.Notification
		invalid bool
	}{
		{"created", &metastore.Notification{After: image(1)}, false},
		{"removed", &metastore.Notification{Before: image(1)}, false},
		{"rename", &metastore.Notification{Before: image(1), After: image(1)}, false},
		{"missing", nil, true},
		{"wrong old side", &metastore.Notification{Before: image(3), After: image(1)}, true},
		{"wrong new side", &metastore.Notification{Before: image(1), After: image(3)}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateNotificationRoot(test.facts, 1)
			if test.invalid && !errors.Is(err, syscall.EIO) || !test.invalid && err != nil {
				t.Fatalf("root validation = %v, invalid=%v", err, test.invalid)
			}
		})
	}
	if err := validateNotificationRoot(&metastore.Notification{After: image(1)}, 0); !errors.Is(err, syscall.EIO) {
		t.Fatalf("invalid volume root = %v", err)
	}
}

type singleConnectionNotificationProbe struct {
	*sql.DB
	payloadReads int
}

func (p *singleConnectionNotificationProbe) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	p.payloadReads++
	return p.DB.QueryRowContext(ctx, query, args...)
}

func TestNotificationValidationSupportsOneConnectionAndCallerTransactions(t *testing.T) {
	db, tx := logFixture(t)
	appendLog(t, tx, 2)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := ValidateNotifications(ctx, db, nil, 2, 1<<20); err != nil {
		t.Fatalf("single-connection database validation: %v", err)
	}
	current, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateNotifications(ctx, current, nil, 2, 1<<20); err != nil {
		current.Rollback()
		t.Fatalf("caller transaction validation: %v", err)
	}
	if err := current.Rollback(); err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if err := ValidateNotifications(canceled, db, nil, 2, 1<<20); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled validation: %v", err)
	}
	if inUse := db.Stats().InUse; inUse != 0 {
		t.Fatalf("validation retained %d database connections", inUse)
	}
}

func TestNotificationValidationAdmitsGlobalBudgetsBeforePayload(t *testing.T) {
	for _, test := range []struct {
		name           string
		records, bytes int64
		corrupt        bool
	}{
		{"record budget", 0, 1 << 20, false},
		{"byte budget", 10, 0, false},
		{"oversized payload", 10, 1 << 20, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, tx := logFixture(t)
			appendLog(t, tx, 1)
			if test.corrupt {
				execLogSQL(t, tx, `UPDATE changes SET notification=zeroblob(262145)`)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			probe := &singleConnectionNotificationProbe{DB: db}
			if err := ValidateNotifications(t.Context(), probe, nil, test.records, test.bytes); !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("budget validation: %v", err)
			}
			if probe.payloadReads != 0 || db.Stats().InUse != 0 {
				t.Fatalf("unadmitted payload reads=%d active connections=%d", probe.payloadReads, db.Stats().InUse)
			}
		})
	}
}

func TestHistoricalImagesCannotClaimAnotherVolumeRoot(t *testing.T) {
	for _, kind := range []metastore.ChangeKind{metastore.Created, metastore.Removed, metastore.Modified, metastore.Renamed} {
		t.Run(string(rune('0'+kind)), func(t *testing.T) {
			_, tx := logFixture(t)
			appendLog(t, tx, 1)
			change := fileChange(kind)
			change.Parent = 3
			if change.From != nil {
				change.From.Parent = 3
			}
			for _, image := range []*metastore.EventImage{change.Notification.Before, change.Notification.After} {
				if image != nil {
					image.Location.RootNodeID = 3
					image.Location.Ancestors[0].ParentID = 3
				}
			}
			if err := Record(t.Context(), tx, 1, change); err != nil {
				t.Fatalf("self-consistent foreign-root fixture: %v", err)
			}
			volume := int64(1)
			for _, scope := range []*int64{nil, &volume} {
				if err := ValidateNotifications(t.Context(), tx, scope, 1000, 8<<20); !errors.Is(err, syscall.EIO) {
					t.Fatalf("foreign root accepted by integrity reader: %v", err)
				}
			}
			result := changeResult(t, 10)
			if _, err := ReadPage(t.Context(), tx, 1, 100, 0, 10, result); !errors.Is(err, syscall.EIO) {
				t.Fatalf("foreign root accepted by page reader: %v", err)
			} else {
				result.Fail(err)
			}
			if page, err := result.Changes(); page != nil || !errors.Is(err, syscall.EIO) {
				t.Fatalf("failed page exposed prefix: %+v, %v", page, err)
			}
		})
	}
}

func TestChangeSQLPreservesNonCalendarTimeInstants(t *testing.T) {
	for _, instant := range []time.Time{
		time.Date(10000, 2, 3, 4, 5, 6, 987654321, time.UTC),
		time.Date(-500, 2, 3, 4, 5, 6, 123456789, time.UTC),
	} {
		t.Run(instant.Format("2006"), func(t *testing.T) {
			_, tx := logFixture(t)
			change := fileChange(metastore.Created)
			change.Node.AccessTime, change.Node.ModTime = instant, instant
			change.Node.CreationTime, change.Node.ChangeTime = &instant, &instant
			change.Notification.After.Attr = change.Node.Attr()
			if err := Record(t.Context(), tx, 1, change); err != nil {
				t.Fatalf("recording non-calendar time: %v", err)
			}
			result := changeResult(t, 10)
			if _, err := ReadPage(t.Context(), tx, 1, 100, 0, 10, result); err != nil {
				t.Fatal(err)
			}
			page, err := result.Changes()
			if err != nil || len(page) != 1 {
				t.Fatalf("time page: %+v, %v", page, err)
			}
			for _, attr := range []storage.Attr{page[0].Node.Attr(), page[0].Notification.After.Attr} {
				if attr.CreationTime == nil || attr.ChangeTime == nil {
					t.Fatal("known time became unknown")
				}
				for _, actual := range []time.Time{attr.AccessTime, attr.ModTime, *attr.CreationTime, *attr.ChangeTime} {
					if actual.Unix() != instant.Unix() || actual.Nanosecond() != instant.Nanosecond() {
						t.Fatalf("time changed from (%d,%d) to (%d,%d)", instant.Unix(), instant.Nanosecond(), actual.Unix(), actual.Nanosecond())
					}
				}
			}
		})
	}
}
