package changes

import (
	"database/sql"
	"errors"
	"math"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"

	_ "modernc.org/sqlite"
)

func TestChangeMetadataDecoderNamesInvalidScalarStorage(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	values := metadataValues()
	values["from_parent"] = "bad-parent"
	_, _, _, err = scanChangeMetadata(metadataQuery(db, values), 1)
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
		"node": int64(2), "node_kind": int64(storage.NodeRegular), "size": int64(4),
		"atime_sec": int64(-100), "atime_nsec": int64(123), "mtime_sec": int64(100), "mtime_nsec": int64(456),
		"metadata": []byte{'R', 'F', 'M', 1, 0, 0}, "link_target": []byte{}, "directory_revision": nil,
		"birth_sec": nil, "birth_nsec": nil, "change_sec": nil, "change_nsec": nil,
		"content": "body", "recorded_sec": int64(200), "recorded_nsec": int64(0),
	}
}

func metadataQuery(db *sql.DB, values map[string]any) *sql.Row {
	columns := []string{"position", "previous_position", "volume", "kind", "parent", "name", "from_parent", "from_name", "node", "node_kind", "size", "atime_sec", "atime_nsec", "mtime_sec", "mtime_nsec", "content", "recorded_sec", "recorded_nsec", "metadata", "link_target", "directory_revision", "birth_sec", "birth_nsec", "change_sec", "change_nsec"}
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
		lengths := metastore.ChangePayloadLengths{Name: 4, Content: 4, Metadata: 6}
		if kind == metastore.Removed {
			for _, column := range []string{"node", "node_kind", "size", "atime_sec", "atime_nsec", "mtime_sec", "mtime_nsec", "content", "metadata", "link_target", "directory_revision", "birth_sec", "birth_nsec", "change_sec", "change_nsec"} {
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
	values["node_kind"] = int64(storage.NodeDirectory)
	values["size"] = int64(0)
	values["content"] = nil
	got, _, _, err := scanChangeMetadata(metadataQuery(db, values), 1)
	if err != nil || got.Parent != 0 || got.Name != nil || !got.Node.IsDir() {
		t.Fatalf("root directory metadata=%+v %v", got, err)
	}
}

func TestMetadataDecoderRejectsStorageClassesAndInconsistentFields(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/decode.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, column := range []string{"position", "previous_position", "volume", "kind", "parent", "from_parent", "node", "node_kind", "size", "atime_sec", "atime_nsec", "mtime_sec", "mtime_nsec", "recorded_sec", "recorded_nsec"} {
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
		{"negative node kind", map[string]any{"node_kind": int64(-1)}},
		{"overflow node kind", map[string]any{"node_kind": int64(math.MaxUint32) + 1}},
		{"special node", map[string]any{"node_kind": int64(storage.NodeSymlink) + 1}},
		{"directory bytes", map[string]any{"node_kind": int64(storage.NodeDirectory)}},
		{"missing content", map[string]any{"content": nil}},
		{"missing created name", map[string]any{"name": nil}},
		{"modified unnamed nonroot", map[string]any{"kind": KindModified, "name": nil}},
		{"invalid rename source", map[string]any{"kind": KindRenamed, "from_parent": int64(0), "from_name": []byte("old")}},
		{"removed carries content", map[string]any{"kind": KindRemoved, "node": nil, "node_kind": nil, "size": nil, "atime_sec": nil, "atime_nsec": nil, "mtime_sec": nil, "mtime_nsec": nil}},
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

func TestEventMetadataRejectsMalformedOptionalFieldsBeforePayloadAdmission(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/extra.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, test := range []struct {
		name   string
		values map[string]any
	}{
		{"missing birth nanos", map[string]any{"birth_sec": int64(1)}},
		{"missing change seconds", map[string]any{"change_nsec": int64(0)}},
		{"text birth seconds", map[string]any{"birth_sec": "1", "birth_nsec": int64(0)}},
		{"birth nanos overflow", map[string]any{"birth_sec": int64(1), "birth_nsec": int64(time.Second)}},
		{"negative change nanos", map[string]any{"change_sec": int64(1), "change_nsec": int64(-1)}},
		{"null metadata", map[string]any{"metadata": nil}},
		{"text metadata", map[string]any{"metadata": "RFM"}},
		{"short metadata", map[string]any{"metadata": []byte{1}}},
		{"oversized metadata", map[string]any{"metadata": make([]byte, storage.MaxMetadataBytes+1)}},
		{"null target", map[string]any{"link_target": nil}},
		{"regular target", map[string]any{"link_target": []byte("target")}},
		{"oversized target", map[string]any{"link_target": make([]byte, storage.MaxLinkTargetBytes+1)}},
		{"text revision", map[string]any{"directory_revision": "revision"}},
		{"oversized revision", map[string]any{"directory_revision": make([]byte, storage.MaxObservationTokenBytes+1)}},
		{"regular directory revision", map[string]any{"directory_revision": []byte{1}}},
		{"link size mismatch", map[string]any{"node_kind": int64(storage.NodeSymlink), "link_target": []byte("abc"), "content": nil}},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := metadataValues()
			for k, v := range test.values {
				values[k] = v
			}
			if _, _, _, err := scanChangeMetadata(metadataQuery(db, values), 1); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid extra event field=%v", err)
			}
		})
	}
}
