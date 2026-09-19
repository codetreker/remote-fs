package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNodeScannerRejectsMalformedScalarAndPayloadBeforeExposure(t *testing.T) {
	for _, test := range []struct {
		name, damage string
		want         error
	}{
		{"kind type", `kind='file'`, syscall.EIO},
		{"kind value", `kind=99`, syscall.EIO},
		{"negative size", `size=-1`, syscall.EIO},
		{"access nanoseconds", `atime_nsec=1000000000`, syscall.EIO},
		{"modified nanoseconds", `mtime_nsec=-1`, syscall.EIO},
		{"partial birth", `birth_sec=NULL`, syscall.EIO},
		{"partial change", `change_nsec=NULL`, syscall.EIO},
		{"birth storage", `birth_sec=zeroblob(1048576)`, syscall.EIO},
		{"change nanoseconds", `change_nsec=1000000000`, syscall.EIO},
		{"metadata type", `metadata='bad'`, syscall.EIO},
		{"metadata version", `metadata=X'52464d020000'`, syscall.EIO},
		{"metadata truncation", `metadata=X'52464d010000ff'`, syscall.EIO},
		{"metadata bound", `metadata=zeroblob(65537)`, syscall.EFBIG},
		{"target type", `link_target='bad'`, syscall.EIO},
		{"target bound", `link_target=zeroblob(32769)`, syscall.EFBIG},
		{"regular target", `link_target=X'61'`, syscall.EIO},
		{"empty symlink", `kind=3`, syscall.EIO},
		{"symlink size", `kind=3,size=2,link_target=X'61'`, syscall.EIO},
		{"directory size", `kind=2,size=1`, syscall.EIO},
		{"regular directory token", `directory_revision=X'01'`, syscall.EIO},
		{"directory token type", `directory_revision='token'`, syscall.EIO},
		{"directory token bound", `directory_revision=zeroblob(65)`, syscall.EIO},
		{"empty content key", `content=''`, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := metadataTestStore(t)
			if err := s.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			if test.name == "empty content key" {
				if _, err := s.write.Exec(`PRAGMA foreign_keys=OFF`); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.write.Exec(`UPDATE nodes SET `+test.damage+` WHERE id<>?`, s.root); err != nil {
				t.Fatal(err)
			}
			node, err := scanNode(s.read.QueryRow(`SELECT `+nodeColumns+` FROM nodes n WHERE n.id<>?`, s.root))
			if err == nil || (test.name != "kind type" && !errors.Is(err, test.want)) || node.ID != 0 || node.Metadata != nil {
				t.Fatalf("corrupt node exposure=%+v, error=%v; want %v", node, err, test.want)
			}
		})
	}
}

func TestNodeScannerPreservesMetadataOnlyReplicaFactsAndOwnsBlobs(t *testing.T) {
	encoded, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{"source.value": {Version: []byte("source-token"), Data: []byte{0, 255}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []int64{1, 2} {
		header := nodeHeader{id: 7, kind: kind, valid: 1, metadataBytes: int64(len(encoded)),
			atimeSec: -123, atimeNsec: 456, mtimeSec: 789, mtimeNsec: 123}
		if kind == 1 {
			header.size = 99
		}
		scan := nodeScan{nodeHeader: header, metadata: bytes.Clone(encoded)}
		node, err := scan.node()
		if err != nil || node.ID != 7 || node.Size != header.size || node.Content != "" || node.BirthTime != nil || node.ChangeTime != nil || len(node.DirectoryRevision) != 0 || !reflect.DeepEqual(node.Metadata["source.value"].Version, []byte("source-token")) {
			t.Fatalf("metadata-only replica facts=%+v, error=%v", node, err)
		}
		scan.metadata[len(scan.metadata)-1] = 1
		if !bytes.Equal(node.Metadata["source.value"].Data, []byte{0, 255}) {
			t.Fatal("node retains scanner storage")
		}
		attr, err := header.attr()
		if err != nil || attr.Metadata != nil || attr.ID != 7 || attr.Size != node.Size || !attr.AccessTime.Equal(time.Unix(-123, 456)) {
			t.Fatalf("unloaded attribute header=%+v, error=%v", attr, err)
		}
	}
}

func TestNodeScannerRejectsPayloadMismatchAndDistinguishesUnknownTime(t *testing.T) {
	base := nodeScan{nodeHeader: nodeHeader{id: 1, kind: 1, valid: 1, metadataBytes: 6}, metadata: []byte{'R', 'F', 'M', 1, 0, 0}}
	for _, damage := range []func(*nodeScan){
		func(s *nodeScan) { s.metadata = s.metadata[:5] },
		func(s *nodeScan) { s.content = sql.NullString{String: "changed", Valid: true} },
		func(s *nodeScan) { s.target = []byte("changed") },
	} {
		scan := base
		damage(&scan)
		if _, err := scan.node(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("payload changed after reservation: %v", err)
		}
	}
	known := time.Time{}
	sec, nsec := sql.NullInt64{Int64: known.Unix(), Valid: true}, sql.NullInt64{Int64: 0, Valid: true}
	got, err := optionalStoredTime(sec, nsec)
	if err != nil || got == nil || !got.Equal(known) {
		t.Fatalf("known zero time=%v, error=%v", got, err)
	}
	got, err = optionalStoredTime(sql.NullInt64{}, sql.NullInt64{})
	if err != nil || got != nil {
		t.Fatalf("unknown time became known=%v, error=%v", got, err)
	}
}
