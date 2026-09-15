package schema

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"syscall"
	"testing"
)

func TestWindowsMetadataRejectsCorruptTimestampsAndTargets(t *testing.T) {
	for _, mutation := range []string{
		`UPDATE nodes SET windows_creation_nsec=-1`,
		`UPDATE nodes SET windows_change_nsec=1000000000`,
		`UPDATE nodes SET windows_attributes=65536`,
		`UPDATE nodes SET windows_creation_sec='invalid'`,
		`UPDATE nodes SET windows_link_target='text'`,
		`UPDATE nodes SET windows_link_target=X'61'`,
		fmt.Sprintf(`UPDATE nodes SET mode=%d,windows_link_target=X'61',size=2,content=NULL`, fs.ModeSymlink|0o644),
	} {
		t.Run(mutation, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, _ := testVolume(t, db, "windows")
			for _, scope := range []*int64{nil, &id} {
				if err := validateWindowsNodes(t.Context(), db, scope); err != nil {
					t.Fatal(err)
				}
			}
			execute(t, db, mutation)
			for _, scope := range []*int64{nil, &id} {
				if err := validateWindowsNodes(t.Context(), db, scope); !errors.Is(err, syscall.EIO) {
					t.Fatalf("validation=%v", err)
				}
			}
		})
	}
	missing := testDatabase(t, 0)
	if err := validateWindowsNodes(t.Context(), missing, nil); err == nil {
		t.Fatal("missing schema accepted")
	}
}

func TestWindowsSymlinkPayloadConsumesIntegrityBudget(t *testing.T) {
	db := testDatabase(t, 0)
	id, root := testVolume(t, db, "windows")
	testFile(t, db, id, root, "link", 0, false)
	execute(t, db, `UPDATE nodes SET mode=?,size=4096,content=NULL,windows_link_target=? WHERE id=2`, int64(fs.ModeSymlink|0o644), []byte(strings.Repeat("x", 4096)))
	if err := validateWindowsNodes(t.Context(), db, &id); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []*int64{nil, &id} {
		if err := validateIntegrityBytes(t.Context(), db, scope, 4095, schema.Version()); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("payload budget=%v", err)
		}
	}
}

func TestWindowsPersistedLinkSyntaxIsValidated(t *testing.T) {
	for _, target := range []string{"", "\xff", "bad\x00target", `C:\host`, "//host/share"} {
		t.Run(fmt.Sprintf("%q", target), func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testVolume(t, db, "windows")
			testFile(t, db, id, root, "link", 0, false)
			execute(t, db, `UPDATE nodes SET mode=?,size=?,content=NULL,windows_link_target=? WHERE id=2`, int64(fs.ModeSymlink|0o644), len(target), []byte(target))
			if err := validateWindowsNodes(t.Context(), db, &id); !errors.Is(err, syscall.EIO) {
				t.Fatalf("target=%q error=%v", target, err)
			}
		})
	}
	db := testDatabase(t, 0)
	if err := validateWindowsLinkTargets(t.Context(), db, nil); err == nil {
		t.Fatal("missing target table accepted")
	}
}
