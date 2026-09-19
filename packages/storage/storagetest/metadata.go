package storagetest

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// RunMetadata requires a backend that exposes versioned opaque metadata through
// its retained file sessions. Missing capabilities fail the contract.
func RunMetadata(t *testing.T, newStorage NewStorage) {
	for name, run := range map[string]func(*testing.T, storage.Storage, storage.FileSession, storage.MetadataAccess){
		"metadata versions isolate namespaces and refuse stale updates": metadataVersions,
		"metadata survives writes renames listings and existing opens":  metadataPreservation,
		"metadata validates boundaries without changing stored values":  metadataRefusals,
	} {
		t.Run(name, func(t *testing.T) {
			s := newStorage(t)
			files, ok := s.(storage.FileStorage)
			if !ok {
				t.Fatal("metadata contract requires FileStorage")
			}
			mustSucceed(t, files.CheckFileStorage())
			session, err := files.NewFileSession(ctx(t), storage.DefaultFileSessionOptions())
			mustSucceed(t, err)
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				mustSucceed(t, session.Close(cleanup))
			})
			metadata, ok := session.(storage.MetadataAccess)
			if !ok {
				t.Fatal("metadata contract requires session MetadataAccess")
			}
			mustSucceed(t, metadata.CheckMetadataAccess())
			run(t, s, session, metadata)
		})
	}
}

func metadataVersions(t *testing.T, s storage.Storage, session storage.FileSession, metadata storage.MetadataAccess) {
	mustSucceed(t, s.Create(ctx(t), "f"))
	node, err := s.Stat(ctx(t), "f")
	mustSucceed(t, err)
	first, err := metadata.SetMetadata(ctx(t), node.ID, "test.primary", nil, []byte{0, 255, 1})
	mustSucceed(t, err)
	if len(first.Version) == 0 || !bytes.Equal(first.Data, []byte{0, 255, 1}) {
		t.Fatalf("initial metadata: %+v", first)
	}
	other, err := metadata.SetMetadata(ctx(t), node.ID, "unknown.namespace", nil, []byte{255, 0, 127})
	mustSucceed(t, err)
	if _, err := metadata.SetMetadata(ctx(t), node.ID, "test.primary", nil, []byte("overwrite")); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("absent CAS on existing namespace: %v", err)
	}
	updated, err := metadata.SetMetadata(ctx(t), node.ID, "test.primary", first.Version, nil)
	mustSucceed(t, err)
	if len(updated.Version) == 0 || bytes.Equal(updated.Version, first.Version) || len(updated.Data) != 0 {
		t.Fatalf("empty payload update: %+v", updated)
	}
	if _, err := metadata.SetMetadata(ctx(t), node.ID, "test.primary", first.Version, []byte("stale")); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale version: %v", err)
	}
	got, err := session.StatNode(ctx(t), node.ID)
	mustSucceed(t, err)
	if len(got.Metadata) != 2 || !samePayload(got.Metadata["test.primary"], updated) || !samePayload(got.Metadata["unknown.namespace"], other) {
		t.Fatalf("namespace update changed other data/version: %+v", got.Metadata)
	}
	expected := storage.CloneMetadata(got.Metadata)
	updated.Version[0] ^= 255
	other.Data[0] ^= 255
	again, err := session.StatNode(ctx(t), node.ID)
	mustSucceed(t, err)
	if !maps.EqualFunc(expected, again.Metadata, samePayload) {
		t.Fatal("returned payload aliases authoritative metadata")
	}
}

func metadataPreservation(t *testing.T, s storage.Storage, session storage.FileSession, metadata storage.MetadataAccess) {
	initial := map[string][]byte{"test.primary": {0, 255, 1}, "unknown.namespace": {7, 0, 9}}
	options := storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}, InitialMetadata: initial}
	file, err := session.OpenFile(ctx(t), "f", options)
	mustSucceed(t, err)
	defer func() { mustSucceed(t, file.Close(ctx(t))) }()
	original, err := file.Stat(ctx(t))
	mustSucceed(t, err)
	if len(original.Metadata) != 2 {
		t.Fatalf("initial metadata missing: %+v", original.Metadata)
	}
	for namespace, data := range initial {
		if !bytes.Equal(original.Metadata[namespace].Data, data) || len(original.Metadata[namespace].Version) == 0 {
			t.Fatalf("initial payload %s: %+v", namespace, original.Metadata[namespace])
		}
	}
	expected := storage.CloneMetadata(original.Metadata)
	initial["test.primary"][0] = 99
	mustSucceed(t, s.Write(ctx(t), "f", []byte("first")))
	mustSucceed(t, s.Rename(ctx(t), "f", "g"))
	accessed := time.Date(1902, 1, 1, 0, 0, 0, 7, time.UTC)
	_, err = file.SetAttr(ctx(t), storage.AttrChange{AccessTime: &accessed})
	mustSucceed(t, err)
	_, err = file.WriteAt(ctx(t), 0, []byte("second"))
	mustSucceed(t, err)
	_, err = file.Truncate(ctx(t), 3)
	mustSucceed(t, err)
	mustSucceed(t, file.Sync(ctx(t)))
	options.InitialMetadata = map[string][]byte{"test.primary": []byte("replacement"), "third.namespace": []byte("new")}
	opened, err := session.OpenFile(ctx(t), "g", options)
	mustSucceed(t, err)
	mustSucceed(t, opened.Close(ctx(t)))
	byName, err := s.Stat(ctx(t), "g")
	mustSucceed(t, err)
	byID, err := session.StatNode(ctx(t), original.ID)
	mustSucceed(t, err)
	retained, err := file.Stat(ctx(t))
	mustSucceed(t, err)
	entries, err := s.List(ctx(t), "")
	mustSucceed(t, err)
	if len(entries) != 1 || entries[0].Name != "g" {
		t.Fatalf("listing: %+v", entries)
	}
	for _, got := range []storage.Attr{byName, byID, retained, entries[0].Attr} {
		if got.ID != original.ID || got.Kind != storage.NodeRegular || !maps.EqualFunc(got.Metadata, expected, samePayload) {
			t.Fatalf("identity or opaque metadata changed: %+v", got)
		}
	}
	data, err := s.Read(ctx(t), "g")
	mustSucceed(t, err)
	if string(data) != "sec" {
		t.Fatalf("retained write/truncate lost content: %q", data)
	}
}

func metadataRefusals(t *testing.T, s storage.Storage, session storage.FileSession, metadata storage.MetadataAccess) {
	mustSucceed(t, s.Create(ctx(t), "f"))
	node, err := s.Stat(ctx(t), "f")
	mustSucceed(t, err)
	original, err := metadata.SetMetadata(ctx(t), node.ID, "test.primary", nil, []byte("original"))
	mustSucceed(t, err)
	for _, test := range []struct {
		name          string
		version, data []byte
	}{
		{"", nil, nil},
		{"Invalid.Namespace", nil, nil},
		{"test.primary", bytes.Repeat([]byte{1}, storage.MaxObservationTokenBytes+1), nil},
		{"test.primary", original.Version, make([]byte, storage.MaxMetadataValueBytes+1)},
	} {
		if _, err := metadata.SetMetadata(ctx(t), node.ID, test.name, test.version, test.data); err == nil {
			t.Fatalf("invalid metadata request succeeded: namespace=%q version=%d payload=%d", test.name, len(test.version), len(test.data))
		}
	}
	got, err := session.StatNode(ctx(t), node.ID)
	mustSucceed(t, err)
	if len(got.Metadata) != 1 || !samePayload(got.Metadata["test.primary"], original) {
		t.Fatalf("refused request changed metadata: %+v", got.Metadata)
	}
	mustSucceed(t, s.Remove(ctx(t), "f"))
	if _, err := metadata.SetMetadata(ctx(t), node.ID, "test.primary", original.Version, nil); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("removed identity metadata: %v", err)
	}
}

func samePayload(a, b storage.OpaquePayload) bool {
	return bytes.Equal(a.Version, b.Version) && bytes.Equal(a.Data, b.Data)
}
