package changes

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	_ "modernc.org/sqlite"
)

func metadataValues() map[string]any {
	return map[string]any{
		"position": int64(7), "previous_position": int64(6), "identity_high_water": int64(5), "volume": int64(1), "kind": KindCreated,
		"parent": int64(1), "name": []byte("file"), "from_parent": nil, "from_name": nil,
		"node": int64(2), "node_kind": int64(storage.NodeRegular), "size": int64(4), "metadata_revision": int64(2), "directory_revision": int64(0),
		"atime_sec": int64(-100), "atime_nsec": int64(123), "mtime_sec": int64(100), "mtime_nsec": int64(456),
		"metadata": []byte{'R', 'F', 'M', 1, 0, 0}, "link_target": []byte{}, "notification": []byte("{}"), "content": "body", "recorded_sec": int64(200), "recorded_nsec": int64(0),
	}
}

func metadataQuery(db *sql.DB, values map[string]any) *sql.Row {
	columns := append(append([]string(nil), changeScalarNames...), changePayloadNames...)
	var aliases []string
	var args []any
	for _, column := range columns {
		aliases = append(aliases, "? AS "+column)
		args = append(args, values[column])
	}
	return db.QueryRow(`SELECT `+changeMetadataColumns+` FROM (SELECT `+strings.Join(aliases, ",")+`)`, args...)
}

func metadataDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMetadataDecoderPreservesScalarsAndDefersPayloads(t *testing.T) {
	db := metadataDB(t)
	for _, kind := range []metastore.ChangeKind{metastore.Created, metastore.Modified, metastore.Renamed, metastore.Removed} {
		values := metadataValues()
		value, err := storedKind(kind)
		if err != nil {
			t.Fatal(err)
		}
		values["kind"] = value
		want := fileChange(kind)
		want.Position = 7
		want.Name = []byte{}
		want.Notification = nil
		lengths := metastore.ChangePayloadLengths{Name: 4, Content: 4, Metadata: 6, Notification: 2}
		if kind == metastore.Removed {
			for _, column := range []string{"node", "node_kind", "size", "atime_sec", "atime_nsec", "mtime_sec", "mtime_nsec", "metadata_revision", "directory_revision", "metadata", "link_target", "content"} {
				values[column] = nil
			}
			lengths.Content = 0
			lengths.Metadata = 0
		} else {
			want.Node.Content = ""
		}
		if kind == metastore.Renamed {
			values["from_parent"] = int64(1)
			values["from_name"] = []byte("old")
			want.From.Name = []byte{}
			lengths.FromName = 3
		}
		got, gotLengths, previous, high, err := scanChangeMetadata(metadataQuery(db, values), 1)
		if got.Node != nil {
			got.Node.AccessTime = got.Node.AccessTime.UTC()
			got.Node.ModTime = got.Node.ModTime.UTC()
		}
		if err != nil || previous != 6 || high != 5 || gotLengths != lengths || !reflect.DeepEqual(got, want) {
			t.Fatalf("kind %v: got=%+v lengths=%+v previous=%d high=%d error=%v want=%+v", kind, got, gotLengths, previous, high, err, want)
		}
	}
	values := metadataValues()
	values["kind"] = KindModified
	values["parent"] = int64(0)
	values["name"] = nil
	values["node_kind"] = int64(storage.NodeDirectory)
	values["directory_revision"] = int64(9)
	values["size"] = int64(0)
	values["content"] = nil
	values["creation_sec"] = int64(-1)
	values["creation_nsec"] = int64(7)
	values["change_sec"] = int64(5)
	values["change_nsec"] = int64(11)
	got, _, _, _, err := scanChangeMetadata(metadataQuery(db, values), 1)
	if err != nil || !got.Node.IsDir() || got.Node.CreationTime == nil || !got.Node.CreationTime.Equal(time.Unix(-1, 7)) || got.Node.ChangeTime == nil || !got.Node.ChangeTime.Equal(time.Unix(5, 11)) {
		t.Fatalf("generic times/directory: %+v %v", got, err)
	}
}

func TestMetadataDecoderRejectsStorageClassesAndInconsistentFields(t *testing.T) {
	db := metadataDB(t)
	for _, column := range changeScalarNames {
		t.Run(column+" text", func(t *testing.T) {
			values := metadataValues()
			values[column] = "bad"
			_, _, _, _, err := scanChangeMetadata(metadataQuery(db, values), 1)
			if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), column) {
				t.Fatalf("bad %s: %v", column, err)
			}
		})
	}
	tests := []struct {
		name    string
		changes map[string]any
	}{
		{"wrong volume", map[string]any{"volume": int64(2)}}, {"zero position", map[string]any{"position": int64(0)}}, {"nil position", map[string]any{"position": nil}},
		{"negative parent", map[string]any{"parent": int64(-1)}}, {"unknown kind", map[string]any{"kind": int64(99)}}, {"self predecessor", map[string]any{"previous_position": int64(7)}},
		{"missing identity summary", map[string]any{"identity_high_water": int64(0)}}, {"name text", map[string]any{"name": "file"}}, {"source name without parent", map[string]any{"from_name": []byte("old")}},
		{"missing node", map[string]any{"node": nil}}, {"missing source", map[string]any{"kind": KindRenamed}}, {"bad kind", map[string]any{"node_kind": int64(0)}},
		{"negative size", map[string]any{"size": int64(-1)}}, {"metadata revision", map[string]any{"metadata_revision": int64(0)}}, {"directory revision", map[string]any{"directory_revision": int64(1)}},
		{"bad atime", map[string]any{"atime_nsec": int64(1e9)}}, {"bad mtime", map[string]any{"mtime_nsec": int64(-1)}}, {"bad record time", map[string]any{"recorded_nsec": int64(1e9)}},
		{"unknown creation fraction", map[string]any{"creation_sec": int64(0)}}, {"unknown change seconds", map[string]any{"change_nsec": int64(0)}}, {"bad creation fraction", map[string]any{"creation_sec": int64(0), "creation_nsec": int64(1e9)}},
		{"metadata absent", map[string]any{"metadata": nil}}, {"metadata text", map[string]any{"metadata": "bad"}}, {"target absent", map[string]any{"link_target": nil}},
		{"target for file", map[string]any{"link_target": []byte("x")}}, {"content blob", map[string]any{"content": []byte("body")}}, {"empty content key", map[string]any{"content": ""}},
		{"file size without content", map[string]any{"content": nil}}, {"directory size", map[string]any{"node_kind": int64(storage.NodeDirectory), "directory_revision": int64(1)}},
		{"symlink object", map[string]any{"node_kind": int64(storage.NodeSymlink), "link_target": []byte("four")}}, {"symlink wrong size", map[string]any{"node_kind": int64(storage.NodeSymlink), "content": nil, "link_target": []byte("x")}},
		{"notification empty", map[string]any{"notification": []byte{}}}, {"notification text", map[string]any{"notification": "bad"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := metadataValues()
			for key, value := range test.changes {
				values[key] = value
			}
			_, _, _, _, err := scanChangeMetadata(metadataQuery(db, values), 1)
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid facts: %v", err)
			}
		})
	}
	if _, _, _, _, err := scanChangeMetadata(db.QueryRow(`SELECT 1`), 1); err == nil {
		t.Fatal("short row accepted")
	}
}

func TestMetadataPayloadValidationPreservesExactNames(t *testing.T) {
	c := fileChange(metastore.Renamed)
	if err := validateChangePayload(c, []byte{0xff}, []byte("old"), sql.NullString{Valid: true, String: "key"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range [][]byte{nil, []byte("."), []byte(".."), []byte("a/b"), []byte{'a', 0}, make([]byte, storage.MaxEntryNameBytes+1)} {
		if err := validateChangePayload(c, name, []byte("old"), sql.NullString{}); !errors.Is(err, syscall.EIO) {
			t.Fatalf("bad destination: %v", err)
		}
		if err := validateChangePayload(c, []byte("file"), name, sql.NullString{}); !errors.Is(err, syscall.EIO) {
			t.Fatalf("bad source: %v", err)
		}
	}
	if err := validateChangePayload(c, []byte("file"), []byte("old"), sql.NullString{Valid: true}); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
}
