package changes

import (
	"database/sql"
	"errors"
	"io/fs"
	"math"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"

	_ "modernc.org/sqlite"
)

func TestChangeMetadataDecoderNamesInvalidScalarStorage(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, _, _, err = scanChangeMetadata(db.QueryRowContext(t.Context(), `
		SELECT
			7, typeof(7), 0, typeof(0), 1, typeof(1), 0, typeof(0),
			1, typeof(1), 0, typeof(NULL),
			'bad-parent', typeof('bad-parent'), 0, typeof(NULL),
			NULL, typeof(NULL), NULL, typeof(NULL), NULL, typeof(NULL),
			NULL, typeof(NULL), NULL, typeof(NULL),
			NULL, typeof(NULL), NULL, typeof(NULL),
			0, typeof(NULL),
			0, typeof(0), 0, typeof(0)`), 1)
	if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "change 7 stores from_parent as text") {
		t.Fatalf("decoding a text from_parent returned %v", err)
	}

	if _, err := requiredStoredInteger("position", "bad-position", "text"); !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "a change stores position as text") {
		t.Fatalf("decoding a text position returned %v", err)
	}
}

func metadataValues() map[string]any {
	return map[string]any{
		"position": int64(7), "previous_position": int64(6), "volume": int64(1), "kind": KindCreated,
		"parent": int64(1), "name": []byte("file"), "from_parent": nil, "from_name": nil,
		"node": int64(2), "mode": int64(0644), "size": int64(4),
		"atime_sec": int64(-100), "atime_nsec": int64(123), "mtime_sec": int64(100), "mtime_nsec": int64(456),
		"content": "body", "recorded_sec": int64(200), "recorded_nsec": int64(0),
	}
}

func metadataQuery(db *sql.DB, values map[string]any) *sql.Row {
	columns := []string{"position", "previous_position", "volume", "kind", "parent", "name", "from_parent", "from_name", "node", "mode", "size", "atime_sec", "atime_nsec", "mtime_sec", "mtime_nsec", "content", "recorded_sec", "recorded_nsec"}
	var aliases []string
	var args []any
	for _, column := range columns {
		aliases = append(aliases, "? AS "+column)
		args = append(args, values[column])
	}
	return db.QueryRow(`SELECT `+changeMetadataColumns+` FROM (SELECT `+strings.Join(aliases, ",")+`)`, args...)
}

func TestMetadataDecoderPreservesScalarsAndDefersPayloads(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/decode.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, kind := range []metastore.ChangeKind{metastore.Created, metastore.Modified, metastore.Renamed, metastore.Removed} {
		values := metadataValues()
		stored, err := storedKind(kind)
		if err != nil {
			t.Fatal(err)
		}
		values["kind"] = stored
		want := fileChange(kind)
		want.Position = 7
		want.Name = []byte{}
		lengths := metastore.ChangePayloadLengths{Name: 4, Content: 4}
		if kind == metastore.Removed {
			for _, column := range []string{"node", "mode", "size", "atime_sec", "atime_nsec", "mtime_sec", "mtime_nsec", "content"} {
				values[column] = nil
			}
			lengths.Content = 0
		} else {
			want.Node.Content = ""
		}
		if kind == metastore.Renamed {
			values["from_parent"] = int64(1)
			values["from_name"] = []byte("old")
			want.From.Name = []byte{}
			lengths.FromName = 3
		}
		got, gotLengths, previous, err := scanChangeMetadata(metadataQuery(db, values), 1)
		if got.Node != nil {
			got.Node.AccessTime = got.Node.AccessTime.UTC()
			got.Node.ModTime = got.Node.ModTime.UTC()
		}
		if err != nil || previous != 6 || gotLengths != lengths || !reflect.DeepEqual(got, want) {
			t.Fatalf("kind %v: got=%+v lengths=%+v previous=%d error=%v; want=%+v", kind, got, gotLengths, previous, err, want)
		}
	}
	values := metadataValues()
	values["kind"] = KindModified
	values["parent"] = int64(0)
	values["name"] = nil
	values["mode"] = int64(fs.ModeDir | 0755)
	values["size"] = int64(0)
	values["content"] = nil
	got, _, _, err := scanChangeMetadata(metadataQuery(db, values), 1)
	if err != nil || got.Parent != 0 || got.Name != nil || !got.Node.Mode.IsDir() {
		t.Fatalf("root directory metadata=%+v %v", got, err)
	}
}

func TestMetadataDecoderRejectsStorageClassesAndInconsistentFields(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/decode.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, column := range []string{"position", "previous_position", "volume", "kind", "parent", "from_parent", "node", "mode", "size", "atime_sec", "atime_nsec", "mtime_sec", "mtime_nsec", "recorded_sec", "recorded_nsec"} {
		t.Run(column+" as text", func(t *testing.T) {
			values := metadataValues()
			values[column] = "bad"
			_, _, _, err := scanChangeMetadata(metadataQuery(db, values), 1)
			if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), column) {
				t.Fatalf("bad %s = %v", column, err)
			}
		})
	}
	for _, test := range []struct {
		name    string
		changes map[string]any
	}{
		{"volume mismatch", map[string]any{"volume": int64(2)}},
		{"nonpositive position", map[string]any{"position": int64(0)}},
		{"negative parent", map[string]any{"parent": int64(-1)}},
		{"unknown kind", map[string]any{"kind": int64(9)}},
		{"negative predecessor", map[string]any{"previous_position": int64(-1)}},
		{"self predecessor", map[string]any{"previous_position": int64(7)}},
		{"name text", map[string]any{"name": "file"}},
		{"source name text", map[string]any{"from_name": "old"}},
		{"content blob", map[string]any{"content": []byte("body")}},
		{"missing node", map[string]any{"node": nil}},
		{"missing source", map[string]any{"kind": KindRenamed}},
		{"unexpected source", map[string]any{"from_parent": int64(1)}},
		{"empty content", map[string]any{"content": ""}},
		{"zero node", map[string]any{"node": int64(0)}},
		{"negative size", map[string]any{"size": int64(-1)}},
		{"negative mode", map[string]any{"mode": int64(-1)}},
		{"overflow mode", map[string]any{"mode": int64(math.MaxUint32) + 1}},
		{"special node", map[string]any{"mode": int64(fs.ModeSymlink)}},
		{"directory bytes", map[string]any{"mode": int64(fs.ModeDir)}},
		{"missing content", map[string]any{"content": nil}},
		{"missing created name", map[string]any{"name": nil}},
		{"modified unnamed nonroot", map[string]any{"kind": KindModified, "name": nil}},
		{"invalid rename source", map[string]any{"kind": KindRenamed, "from_parent": int64(0), "from_name": []byte("old")}},
		{"removed carries content", map[string]any{"kind": KindRemoved, "node": nil, "mode": nil, "size": nil, "atime_sec": nil, "atime_nsec": nil, "mtime_sec": nil, "mtime_nsec": nil}},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := metadataValues()
			for key, value := range test.changes {
				values[key] = value
			}
			_, _, _, err := scanChangeMetadata(metadataQuery(db, values), 1)
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("inconsistent metadata = %v", err)
			}
		})
	}
	for _, column := range []string{"atime_nsec", "mtime_nsec", "recorded_nsec"} {
		for _, value := range []int64{-1, int64(time.Second)} {
			values := metadataValues()
			values[column] = value
			_, _, _, err := scanChangeMetadata(metadataQuery(db, values), 1)
			if !errors.Is(err, syscall.EIO) {
				t.Errorf("%s=%d returned %v", column, value, err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := scanChangeMetadata(metadataQuery(db, metadataValues()), 1); err == nil {
		t.Fatal("closed database returned metadata")
	}
}

func TestChangePayloadRejectsInvalidComponentsAndEmptyContent(t *testing.T) {
	change := fileChange(metastore.Renamed)
	change.Position = 7
	for _, name := range [][]byte{nil, {}, []byte("."), []byte(".."), []byte("a/b"), {'a', 0}} {
		if validStoredComponent(name) {
			t.Errorf("invalid component accepted: %q", name)
		}
		if err := validateChangePayload(change, name, []byte("old"), sql.NullString{}); !errors.Is(err, syscall.EIO) {
			t.Errorf("invalid destination %q = %v", name, err)
		}
		if err := validateChangePayload(change, []byte("file"), name, sql.NullString{}); !errors.Is(err, syscall.EIO) {
			t.Errorf("invalid source %q = %v", name, err)
		}
	}
	if err := validateChangePayload(change, []byte("file"), []byte("old"), sql.NullString{Valid: true}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("empty content = %v", err)
	}
	if err := validateChangePayload(change, []byte{0xff}, []byte("old"), sql.NullString{String: "body", Valid: true}); err != nil {
		t.Fatalf("byte name = %v", err)
	}
}
