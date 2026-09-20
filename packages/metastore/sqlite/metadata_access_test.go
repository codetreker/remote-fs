package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestMetadataNamespaceCASPreservesOtherNamespacesAndOwnsPayloads(t *testing.T) {
	store, file := openPublicationFile(t)
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET change_sec=1,change_nsec=0 WHERE volume=?`, store.volume); err != nil {
		t.Fatal(err)
	}
	initial, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	position, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte{1, 2, 3}
	first, err := store.SetMetadata(t.Context(), uint64(initial.ID), "client.one", nil, payload)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 9
	if len(first.Version) == 0 || !bytes.Equal(first.Data, []byte{1, 2, 3}) {
		t.Fatalf("first metadata result = %+v", first)
	}
	second, err := file.SetMetadata(t.Context(), "client.two", nil, []byte{})
	if err != nil || len(second.Version) == 0 || second.Data == nil {
		t.Fatalf("present empty metadata = %+v, %v", second, err)
	}
	after, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if after.ChangeTime == nil || initial.ChangeTime == nil || !after.ChangeTime.After(*initial.ChangeTime) ||
		!bytes.Equal(after.Metadata["client.one"].Data, []byte{1, 2, 3}) ||
		!bytes.Equal(after.Metadata["client.two"].Version, second.Version) {
		t.Fatalf("metadata update lost facts: before=%+v after=%+v", initial, after)
	}

	beforeConflict := after
	if _, err := store.SetMetadata(t.Context(), uint64(initial.ID), "client.one", nil, []byte("wrong")); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("absence CAS against present namespace = %v", err)
	}
	afterConflict, err := file.Node(t.Context())
	if err != nil || !reflect.DeepEqual(afterConflict, beforeConflict) {
		t.Fatalf("failed CAS changed node: %+v, %v; before %+v", afterConflict, err, beforeConflict)
	}
	if afterPosition, err := store.CommittedPosition(t.Context()); err != nil || afterPosition != position+2 {
		t.Fatalf("failed CAS changed log position: %d, %v; want %d", afterPosition, err, position+2)
	}

	updated, err := store.SetMetadata(t.Context(), uint64(initial.ID), "client.one", first.Version, []byte("next"))
	if err != nil || bytes.Equal(updated.Version, first.Version) || !bytes.Equal(updated.Data, []byte("next")) {
		t.Fatalf("versioned metadata update = %+v, %v", updated, err)
	}
	assertMetadataAccounting(t, store.Store)
}

func TestRetiredFileKeepsUseClaimUntilExplicitDrop(t *testing.T) {
	store, file := openPublicationFile(t)
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	opened, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true},
		Use:        storage.UseClaim{Deny: storage.ReadData},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if competing, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); competing != nil || !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("retired reference released use before drain: file=%v err=%v", competing, err)
	}
	if err := opened.DropUse(t.Context()); err != nil {
		t.Fatal(err)
	}
	competing, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := competing.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataGrowthOverLimitRollsBackPayloadChangeTimeAndLog(t *testing.T) {
	store, file := openPublicationFile(t)
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	position, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var used int64
	if err := store.read.QueryRowContext(t.Context(), `SELECT metadata_used FROM volumes WHERE id=?`, store.volume).Scan(&used); err != nil {
		t.Fatal(err)
	}
	store.maxMetadataBytes = used
	if _, err := store.SetMetadata(t.Context(), uint64(before.ID), "client.limit", nil, []byte{1}); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("metadata growth beyond limit = %v", err)
	}
	after, err := file.Node(t.Context())
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("refused metadata growth changed node: %+v, %v; before %+v", after, err, before)
	}
	if got, err := store.CommittedPosition(t.Context()); err != nil || got != position {
		t.Fatalf("refused metadata growth advanced log to %d, %v; want %d", got, err, position)
	}
	assertMetadataAccounting(t, store.Store)
}

func TestOpenTruncateUsesItsOwnDenyScope(t *testing.T) {
	store, file := openPublicationFile(t)
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	opened, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Write: true, Truncate: true},
		Use:        storage.UseClaim{Deny: storage.WriteData},
	})
	if err != nil {
		t.Fatalf("self-denying truncate open failed: %v", err)
	}
	state, err := opened.Node(t.Context())
	if err != nil || state.Size != 0 {
		t.Fatalf("truncate state = %+v, %v", state, err)
	}
	if err := opened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func assertMetadataAccounting(t *testing.T, store *Store) {
	t.Helper()
	var recorded, actual int64
	if err := store.read.QueryRowContext(t.Context(), `SELECT metadata_used FROM volumes WHERE id=?`, store.volume).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if err := store.read.QueryRowContext(t.Context(), `SELECT
		coalesce((SELECT sum(length(metadata)) FROM nodes WHERE volume=?),0)+
		coalesce((SELECT sum(coalesce(length(metadata),0)) FROM changes WHERE volume=?),0)`, store.volume, store.volume).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if recorded != actual || recorded <= 0 {
		t.Fatalf("metadata accounting recorded=%d actual=%d", recorded, actual)
	}
}

func TestAuthorityReopenRejectsNonNativeMetadataVersion(t *testing.T) {
	path := t.TempDir() + "/metadata.db"
	store, err := Open(t.Context(), path, "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	encoded, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{
		"client": {Version: []byte("foreign"), Data: []byte("value")},
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `UPDATE nodes SET metadata=? WHERE id=(SELECT node FROM entries WHERE name=CAST('file' AS BLOB))`, encoded); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path, "workspace", 0, DefaultWindow())
	if reopened != nil {
		reopened.Close()
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("authority accepted a foreign metadata version: %v", err)
	}
}
