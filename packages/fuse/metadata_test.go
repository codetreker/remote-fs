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
	attr             storage.Attr
	attrs            []storage.Attr
	nodeCalls        int
	updateNode       uint64
	expected         []byte
	expectedVersions [][]byte
	updates          int
	conflicts        int
	updateErr        error
}

func (s *metadataSessionProbe) CheckMetadataAccess() error { return nil }
func (s *metadataSessionProbe) StatNode(context.Context, uint64) (storage.Attr, error) {
	s.nodeCalls++
	if len(s.attrs) != 0 {
		return s.attrs[min(s.nodeCalls-1, len(s.attrs)-1)], nil
	}
	return s.attr, nil
}
func (s *metadataSessionProbe) SetMetadata(_ context.Context, node uint64, _ string, expected, data []byte) (storage.OpaquePayload, error) {
	s.updates++
	s.updateNode = node
	s.expected = append([]byte(nil), expected...)
	s.expectedVersions = append(s.expectedVersions, append([]byte(nil), expected...))
	if s.conflicts > 0 {
		s.conflicts--
		return storage.OpaquePayload{}, storage.ErrConditionConflict
	}
	if s.updateErr != nil {
		return storage.OpaquePayload{}, s.updateErr
	}
	return storage.OpaquePayload{Version: []byte{2}, Data: append([]byte(nil), data...)}, nil
}

type metadataFileProbe struct {
	storage.File
	attr             storage.Attr
	attrs            []storage.Attr
	statCalls        int
	expected         []byte
	expectedVersions [][]byte
	updates          int
	truncates        int
	conflicts        int
	updateErr        error
}

func (f *metadataFileProbe) Stat(context.Context) (storage.Attr, error) {
	f.statCalls++
	if len(f.attrs) != 0 {
		return f.attrs[min(f.statCalls-1, len(f.attrs)-1)], nil
	}
	return f.attr, nil
}
func (f *metadataFileProbe) CheckMetadataAccess() error { return nil }
func (f *metadataFileProbe) SetMetadata(_ context.Context, _ string, expected, data []byte) (storage.OpaquePayload, error) {
	f.updates++
	f.expected = append([]byte(nil), expected...)
	f.expectedVersions = append(f.expectedVersions, append([]byte(nil), expected...))
	if f.conflicts > 0 {
		f.conflicts--
		return storage.OpaquePayload{}, storage.ErrConditionConflict
	}
	if f.updateErr != nil {
		return storage.OpaquePayload{}, f.updateErr
	}
	return storage.OpaquePayload{Version: []byte{2}, Data: append([]byte(nil), data...)}, nil
}
func (f *metadataFileProbe) Truncate(context.Context, int64) (storage.Attr, error) {
	f.truncates++
	return f.attr, nil
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

func TestPermissionMutationRefreshesTheSameIdentityAfterConditionConflict(t *testing.T) {
	first := storage.Attr{ID: 7, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{posix.Namespace: {Version: []byte{1}, Data: []byte{0xa4, 1, 0, 0}}}}
	second := first.Clone()
	second.Metadata[posix.Namespace] = storage.OpaquePayload{Version: []byte{2}, Data: []byte{0x80, 1, 0, 0}}

	t.Run("retained file", func(t *testing.T) {
		session := &metadataSessionProbe{attr: first}
		v := &volume{files: session, deadline: time.Now().Add(time.Minute)}
		n := &node{volume: v, id: &identity{node: 7, kind: syscall.S_IFREG}}
		file := &metadataFileProbe{attrs: []storage.Attr{first, second}, conflicts: 1}
		if err := n.setPermissions(t.Context(), newHandle(n, file, true, true), 0600); err != nil {
			t.Fatal(err)
		}
		if file.statCalls != 2 || file.updates != 2 || len(file.expectedVersions) != 2 || string(file.expectedVersions[0]) != "\x01" || string(file.expectedVersions[1]) != "\x02" || session.nodeCalls != 0 {
			t.Fatalf("retry used stats=%d updates=%d versions=%v session stats=%d", file.statCalls, file.updates, file.expectedVersions, session.nodeCalls)
		}
	})

	t.Run("node identity", func(t *testing.T) {
		session := &metadataSessionProbe{attrs: []storage.Attr{first, second}, conflicts: 1}
		v := &volume{files: session, deadline: time.Now().Add(time.Minute)}
		n := &node{volume: v, id: &identity{node: 7, kind: syscall.S_IFREG}}
		if err := n.setPermissions(t.Context(), nil, 0640); err != nil {
			t.Fatal(err)
		}
		if session.nodeCalls != 2 || session.updates != 2 || session.updateNode != 7 || len(session.expectedVersions) != 2 || string(session.expectedVersions[0]) != "\x01" || string(session.expectedVersions[1]) != "\x02" {
			t.Fatalf("retry used stats=%d updates=%d node=%d versions=%v", session.nodeCalls, session.updates, session.updateNode, session.expectedVersions)
		}
	})
}

func TestPermissionMutationRetriesOnlyKnownConditionConflicts(t *testing.T) {
	attr := storage.Attr{ID: 7, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{posix.Namespace: {Version: []byte{1}, Data: []byte{0xa4, 1, 0, 0}}}}
	v := &volume{deadline: time.Now().Add(time.Minute)}
	n := &node{volume: v, id: &identity{node: 7, kind: syscall.S_IFREG}}

	unknown := &metadataFileProbe{attr: attr, updateErr: context.Canceled}
	v.files = &metadataSessionProbe{attr: attr}
	err := n.setPermissions(t.Context(), newHandle(n, unknown, true, true), 0600)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.EIO) || unknown.statCalls != 1 || unknown.updates != 1 {
		t.Fatalf("unknown update returned %v after stats=%d updates=%d", err, unknown.statCalls, unknown.updates)
	}

	joinedFailure := errors.New("metadata durability unknown")
	joined := &metadataFileProbe{attr: attr, updateErr: errors.Join(storage.ErrConditionConflict, joinedFailure)}
	err = n.setPermissions(t.Context(), newHandle(n, joined, true, true), 0600)
	if !errors.Is(err, storage.ErrConditionConflict) || !errors.Is(err, joinedFailure) || errnoOf(err) != syscall.EIO || joined.statCalls != 1 || joined.updates != 1 {
		t.Fatalf("joined unknown update returned %v after stats=%d updates=%d", err, joined.statCalls, joined.updates)
	}

	conflicting := &metadataFileProbe{attr: attr, conflicts: maxPermissionCASAttempts}
	err = n.setPermissions(t.Context(), newHandle(n, conflicting, true, true), 0600)
	if !errors.Is(err, storage.ErrConditionConflict) || errors.Is(err, syscall.EIO) || conflicting.statCalls != maxPermissionCASAttempts || conflicting.updates != maxPermissionCASAttempts {
		t.Fatalf("bounded conflicts returned %v after stats=%d updates=%d", err, conflicting.statCalls, conflicting.updates)
	}
}

func TestCompoundSetattrDoesNotExposeRetryAfterEarlierMutation(t *testing.T) {
	attr := storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 0, Metadata: map[string]storage.OpaquePayload{posix.Namespace: {Version: []byte{1}, Data: []byte{0xa4, 1, 0, 0}}}}
	session := &metadataSessionProbe{attr: attr}
	v := &volume{files: session, maxFileSize: 1024, deadline: time.Now().Add(time.Minute)}
	n := &node{volume: v, id: &identity{node: 7, kind: syscall.S_IFREG}}
	file := &metadataFileProbe{attr: attr, conflicts: maxPermissionCASAttempts}
	err := n.setattr(t.Context(), newHandle(n, file, true, true), &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		Valid: gofuse.FATTR_SIZE | gofuse.FATTR_MODE, Size: 0, Mode: 0600,
	}}, &gofuse.AttrOut{})
	if !errors.Is(err, storage.ErrConditionConflict) || errnoOf(err) != syscall.EIO || file.truncates != 1 || file.updates != maxPermissionCASAttempts {
		t.Fatalf("compound setattr returned %v after truncates=%d updates=%d", err, file.truncates, file.updates)
	}
}

func TestChmodSymlinkIsUnsupportedWithoutMetadataMutation(t *testing.T) {
	attr := storage.Attr{ID: 9, Kind: storage.NodeSymlink, Size: 6}
	session := &metadataSessionProbe{attr: attr}
	v := &volume{files: session, deadline: time.Now().Add(time.Minute)}
	n := &node{volume: v, id: &identity{node: 9, kind: syscall.S_IFLNK}}
	errno := n.Setattr(t.Context(), nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{Valid: gofuse.FATTR_MODE, Mode: 0777}}, &gofuse.AttrOut{})
	if errno != syscall.EOPNOTSUPP || session.nodeCalls != 1 || session.updates != 0 {
		t.Fatalf("symlink chmod returned %v after stats=%d updates=%d", errno, session.nodeCalls, session.updates)
	}
}
