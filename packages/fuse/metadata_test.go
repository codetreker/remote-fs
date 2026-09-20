package fuse

import (
	"context"
	"errors"
	"io/fs"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/fuse/posix"
	"github.com/codetreker/remote-fs/packages/storage"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type metadataSessionProbe struct {
	storage.FileSession
	attr       storage.Attr
	nodeCalls  int
	updateNode uint64
	expected   []byte
}

func (s *metadataSessionProbe) CheckMetadataAccess() error { return nil }
func (s *metadataSessionProbe) StatNode(context.Context, uint64) (storage.Attr, error) {
	s.nodeCalls++
	return s.attr, nil
}
func (s *metadataSessionProbe) SetMetadata(_ context.Context, node uint64, _ string, expected, data []byte) (storage.OpaquePayload, error) {
	s.updateNode = node
	s.expected = append([]byte(nil), expected...)
	return storage.OpaquePayload{Version: []byte{2}, Data: append([]byte(nil), data...)}, nil
}

type metadataFileProbe struct {
	storage.File
	attr      storage.Attr
	statCalls int
	expected  []byte
}

func (f *metadataFileProbe) Stat(context.Context) (storage.Attr, error) {
	f.statCalls++
	return f.attr, nil
}
func (f *metadataFileProbe) CheckMetadataAccess() error { return nil }
func (f *metadataFileProbe) SetMetadata(_ context.Context, _ string, expected, data []byte) (storage.OpaquePayload, error) {
	f.expected = append([]byte(nil), expected...)
	return storage.OpaquePayload{Version: []byte{2}, Data: append([]byte(nil), data...)}, nil
}

func TestPermissionPresentationDefaultsDoNotCreateMetadata(t *testing.T) {
	for kind, want := range map[storage.NodeKind]fs.FileMode{storage.NodeRegular: 0644, storage.NodeDirectory: fs.ModeDir | 0755, storage.NodeSymlink: fs.ModeSymlink | 0777} {
		attr := storage.Attr{ID: 1, Kind: kind}
		got, err := permissions(attr)
		if err != nil || got != want {
			t.Fatalf("kind %d: %v %v, want %v", kind, got, err, want)
		}
		if attr.Metadata != nil {
			t.Fatal("presentation wrote missing permissions")
		}
	}
}

func TestPermissionPresentationPreservesSpecialBitsAndRejectsMalformedPayloads(t *testing.T) {
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory} {
		for _, mode := range []fs.FileMode{0, 0600, 0754 | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky, 0644} {
			metadata, err := initialPermissions(mode)
			if err != nil {
				t.Fatal(err)
			}
			attr := storage.Attr{ID: 1, Kind: kind, Metadata: map[string]storage.OpaquePayload{posix.Namespace: {Version: []byte{7}, Data: metadata[posix.Namespace]}, "foreign": {Version: []byte{9}, Data: []byte("untouched")}}}
			got, err := permissions(attr)
			if err != nil || got&posix.Settable != mode || (got.IsDir() != (kind == storage.NodeDirectory)) {
				t.Fatalf("projection %v %v", got, err)
			}
		}
	}
	for _, payload := range []storage.OpaquePayload{{Version: []byte{1}}, {Version: []byte{1}, Data: []byte{0, 16, 0, 0}}, {Data: []byte{0, 0, 0, 0}}} {
		attr := storage.Attr{ID: 1, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{posix.Namespace: payload}}
		if _, err := permissions(attr); !errors.Is(err, syscall.EIO) {
			t.Fatalf("malformed payload returned %v", err)
		}
		if _, errno := attributeMode(attr); errno != syscall.EIO {
			t.Fatalf("malformed mode returned %v", errno)
		}
	}
	if _, err := permissions(storage.Attr{Kind: 255}); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if _, err := initialPermissions(fs.ModeDir | 0700); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
}

func TestActualChangeTimeWinsOverLinuxHistoricalPresentation(t *testing.T) {
	modified := time.Unix(100, 123)
	changed := time.Unix(200, 456)
	v := &volume{}
	for _, actual := range []*time.Time{nil, &changed} {
		attr := storage.Attr{ID: 1, Kind: storage.NodeRegular, ModTime: modified, AccessTime: modified, ChangeTime: actual}
		var out gofuse.Attr
		if errno := v.fillAttr(&out, attr); errno != 0 {
			t.Fatal(errno)
		}
		want := modified
		if actual != nil {
			want = *actual
		}
		if out.Ctime != uint64(want.Unix()) || out.Ctimensec != uint32(want.Nanosecond()) {
			t.Fatalf("ctime %+v, want %v", out, want)
		}
		if out.Mtime != 100 || out.Mtimensec != 123 || attr.ChangeTime != actual {
			t.Fatal("presentation changed authoritative times")
		}
	}
}

func TestPermissionMutationUsesTheRetainedReferenceOrNodeCapability(t *testing.T) {
	version := []byte{1}
	attr := storage.Attr{ID: 7, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{posix.Namespace: {Version: version, Data: []byte{0xa4, 1, 0, 0}}}}
	session := &metadataSessionProbe{attr: attr}
	v := &volume{files: session, deadline: time.Now().Add(time.Minute)}
	n := &node{volume: v, id: &identity{node: 7, kind: syscall.S_IFREG}}

	file := &metadataFileProbe{attr: attr}
	h := newHandle(n, file, true, true)
	if err := n.setPermissions(t.Context(), h, 0600); err != nil {
		t.Fatal(err)
	}
	if file.statCalls != 1 || session.nodeCalls != 0 || string(file.expected) != string(version) {
		t.Fatalf("file chmod used session state: file stats=%d node stats=%d expected=%v", file.statCalls, session.nodeCalls, file.expected)
	}

	if err := n.setPermissions(t.Context(), nil, 0640); err != nil {
		t.Fatal(err)
	}
	if session.nodeCalls != 1 || session.updateNode != 7 || string(session.expected) != string(version) {
		t.Fatalf("node chmod missed identity-bound session metadata: stats=%d node=%d expected=%v", session.nodeCalls, session.updateNode, session.expected)
	}
}
