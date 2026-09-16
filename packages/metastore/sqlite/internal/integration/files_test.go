package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
)

func ownedRetainedStore(t *testing.T, allowance int64, options sqlite.Options) (*sqlite.Store, string) {
	t.Helper()
	path := database(t)
	owned, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: path, Volume: "workspace", Allowance: allowance,
		SQLite: options, Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owned.Close(); err != nil {
			t.Error(err)
		}
	})
	return owned.Store, path
}

type retainedFile struct {
	metastore.File
	session metastore.FileSession
	epoch   uint64
}

func retainedSession(t *testing.T, store *sqlite.Store) (metastore.FileSession, uint64) {
	t.Helper()
	session, status, err := store.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Dispose(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return session, status.ActionEpoch
}

func retainedAction(t *testing.T, epoch uint64) storage.FileActionID {
	t.Helper()
	action, err := storage.NewFileActionID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return action
}

func retainedReference(t *testing.T, session metastore.FileSession, epoch uint64, receipt storage.FileActionReceipt) *retainedFile {
	t.Helper()
	file, live, err := session.Reference(t.Context(), receipt.Reference)
	if err != nil || !live {
		t.Fatalf("retained reference live=%v: %v", live, err)
	}
	return &retainedFile{File: file, session: session, epoch: epoch}
}

func retainNode(t *testing.T, store *sqlite.Store, path string, uses storage.AccessUse) *retainedFile {
	t.Helper()
	node, err := store.Stat(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	session, epoch := retainedSession(t, store)
	receipt, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: uint64(node.ID), Claim: storage.AccessClaim{Uses: uses}}, retainedAction(t, epoch))
	if err != nil {
		t.Fatal(err)
	}
	return retainedReference(t, session, epoch, receipt)
}

func createRetainedNode(t *testing.T, store *sqlite.Store, name string) *retainedFile {
	t.Helper()
	if err := store.Create(t.Context(), name); err != nil {
		t.Fatal(err)
	}
	return retainNode(t, store, name, storage.ReadContent|storage.WriteContent)
}

func retainedClose(t *testing.T, file *retainedFile) error {
	t.Helper()
	_, err := file.Close(t.Context(), retainedAction(t, file.epoch))
	return err
}

func retainedState(t *testing.T, file *retainedFile) metastore.FileState {
	t.Helper()
	state, err := file.Capture(t.Context(), storage.FileIO{})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func retainedCommit(t *testing.T, file *retainedFile, revision uint64, object metastore.Object) error {
	t.Helper()
	action := retainedAction(t, file.epoch)
	_, fresh, err := file.BeginContent(t.Context(), action, [32]byte{}, storage.FileIO{Write: true, Truncate: true, Size: object.Size})
	if err != nil {
		return err
	}
	if !fresh {
		t.Fatal("new content action was already known")
	}
	_, err = file.CommitContent(t.Context(), action, revision, object)
	return err
}

func retainedPublish(t *testing.T, file *retainedFile, size int64) metastore.FileState {
	t.Helper()
	before := retainedState(t, file)
	object := metastore.Object{Size: size, ModTime: time.Now()}
	if size != 0 {
		var err error
		object.Key, err = file.Reserve(t.Context(), size)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := retainedCommit(t, file, before.Revision, object); err != nil {
		t.Fatal(err)
	}
	return retainedState(t, file)
}

func retainedTarget(t *testing.T, parent *retainedFile, name string) storage.EntryTarget {
	t.Helper()
	lookup, err := parent.LookupAt(t.Context(), []byte(name))
	if err != nil {
		t.Fatal(err)
	}
	return storage.EntryTarget{Parent: parent.Reference(), ParentID: parent.NodeID(), Name: []byte(name), DirectoryRevision: lookup.DirectoryRevision, ExpectedEntryID: lookup.EntryID, ExpectedNodeID: lookup.Attr.ID, ExpectedMetadataRevision: lookup.Attr.MetadataRevision}
}

func assertRetainedUsage(t *testing.T, store *sqlite.Store, want int64) {
	t.Helper()
	used, err := store.Usage(t.Context())
	if err != nil || used != want {
		t.Fatalf("usage = %d, %v; want %d", used, err, want)
	}
	space, err := store.Space(t.Context())
	if err != nil || space.Used != want {
		t.Fatalf("space = %+v, %v; want %d used bytes", space, err, want)
	}
}

func TestNativeRetainedIdentitySurvivesRenameUnlinkAndNameReuse(t *testing.T) {
	store, _ := ownedRetainedStore(t, 100, sqlite.DefaultOptions())
	commit(t, store, "original", 5)
	file := retainNode(t, store, "original", storage.ReadContent|storage.WriteContent)
	initial := retainedState(t, file)
	if err := store.Rename(t.Context(), "original", "moved"); err != nil {
		t.Fatal(err)
	}
	if got := retainedState(t, file); got.ID != initial.ID || got.Revision != initial.Revision || got.Detached {
		t.Fatalf("renamed reference = %+v; initial %+v", got, initial)
	}
	retainedPublish(t, file, 7)
	if named, err := store.Stat(t.Context(), "moved"); err != nil || named.ID != initial.ID || named.Size != 7 {
		t.Fatalf("renamed publication = %+v, %v", named, err)
	}
	if err := store.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	commit(t, store, "moved", 3)
	retainedPublish(t, file, 9)
	metadata := opaqueMetadata(60)
	beforeAttr := retainedState(t, file)
	_, err := file.SetAttr(t.Context(), storage.AttrChange{ExpectedRevision: beforeAttr.MetadataRevision, Metadata: &metadata}, retainedAction(t, file.epoch))
	state := retainedState(t, file)
	if err != nil || state.ID != initial.ID || !state.Detached || state.Size != 9 || !reflect.DeepEqual(state.Metadata, metadata) {
		t.Fatalf("unnamed retained attributes = %+v, %v", state, err)
	}
	if named, err := store.Stat(t.Context(), "moved"); err != nil || named.ID == initial.ID || named.Size != 3 || len(named.Metadata) != 0 {
		t.Fatalf("replacement changed with detached reference: %+v, %v", named, err)
	}
	assertRetainedUsage(t, store, 12)
	if err := retainedClose(t, file); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 3)
	if _, err := file.session.StatNode(t.Context(), uint64(initial.ID), storage.ObservationOptions{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("last-closed old identity: %v; want ESTALE", err)
	}
}

func TestNativeRetainedRenameReplacementKeepsBothIdentities(t *testing.T) {
	store, _ := ownedRetainedStore(t, 100, sqlite.DefaultOptions())
	commit(t, store, "target", 5)
	commit(t, store, "source", 7)
	displaced := retainNode(t, store, "target", storage.ReadContent|storage.WriteContent)
	moved := retainNode(t, store, "source", storage.ReadContent)
	displacedID, movedID := retainedState(t, displaced).ID, retainedState(t, moved).ID
	if err := store.Rename(t.Context(), "source", "target"); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 12)
	if state := retainedState(t, displaced); state.ID != displacedID || !state.Detached || state.Size != 5 {
		t.Fatalf("displaced reference = %+v", state)
	}
	if state := retainedState(t, moved); state.ID != movedID || state.Detached || state.Size != 7 {
		t.Fatalf("moved reference = %+v", state)
	}
	retainedPublish(t, displaced, 11)
	if named, err := store.Stat(t.Context(), "target"); err != nil || named.ID != movedID || named.Size != 7 {
		t.Fatalf("named target = %+v, %v", named, err)
	}
	assertRetainedUsage(t, store, 18)
	if err := retainedClose(t, displaced); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 7)
}

func TestNativeRetainedLastPinUsesCurrentDetachedSize(t *testing.T) {
	store, _ := ownedRetainedStore(t, 20, sqlite.DefaultOptions())
	commit(t, store, "file", 5)
	first := retainNode(t, store, "file", storage.ReadContent|storage.WriteContent)
	second := retainNode(t, store, "file", storage.ReadContent|storage.WriteContent)
	if err := store.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	published := retainedPublish(t, first, 13)
	if observed := retainedState(t, second); observed.Size != 13 || observed.Content != published.Content || observed.Revision != published.Revision {
		t.Fatalf("second reference did not observe current publication: %+v; want %+v", observed, published)
	}
	assertRetainedUsage(t, store, 13)
	if _, err := store.Reserve(t.Context(), "new-file", 8); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("new file ignored retained quota: %v; want EDQUOT", err)
	}
	if err := retainedClose(t, first); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 13)
	retainedPublish(t, second, 4)
	assertRetainedUsage(t, store, 4)
	if err := retainedClose(t, second); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 0)
	if err := retainedClose(t, second); err != nil {
		t.Fatalf("idempotent last close: %v", err)
	}
	assertRetainedUsage(t, store, 0)
}

func TestNativeRetainedRevisionIsPerNodeAndRejectsEmptyABA(t *testing.T) {
	store, _ := ownedRetainedStore(t, 100, sqlite.DefaultOptions())
	file := createRetainedNode(t, store, "file")
	initial := retainedState(t, file)
	empty := retainedPublish(t, file, 0)
	if empty.Revision != initial.Revision+1 || empty.Content != "" || empty.Size != 0 {
		t.Fatalf("empty publication = %+v; initial %+v", empty, initial)
	}
	if err := retainedCommit(t, file, initial.Revision, metastore.Object{ModTime: time.Now()}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("empty ABA commit = %v; want EAGAIN", err)
	}
	commit(t, store, "other", 3)
	metadata := opaqueMetadata(64)
	if _, err := file.SetAttr(t.Context(), storage.AttrChange{ExpectedRevision: empty.MetadataRevision, Metadata: &metadata}, retainedAction(t, file.epoch)); err != nil {
		t.Fatal(err)
	}
	if err := store.Rename(t.Context(), "file", "renamed"); err != nil {
		t.Fatal(err)
	}
	err := retainedCommit(t, file, empty.Revision, metastore.Object{ModTime: time.Now()})
	state := retainedState(t, file)
	if err != nil || state.Revision != empty.Revision+1 {
		t.Fatalf("unrelated and metadata changes invalidated content revision: %+v, %v", state, err)
	}
}

func TestNativeRetainedRevisionExhaustionPreservesState(t *testing.T) {
	store, path := ownedRetainedStore(t, 100, sqlite.DefaultOptions())
	file := createRetainedNode(t, store, "file")
	before := retainedPublish(t, file, 5)
	damageDatabase(t, path, `UPDATE nodes SET content_revision = ? WHERE id = ?`, int64(math.MaxInt64), before.ID)
	exhausted := retainedState(t, file)
	if err := retainedCommit(t, file, exhausted.Revision, metastore.Object{ModTime: time.Now()}); !errors.Is(err, syscall.EOVERFLOW) {
		t.Fatalf("exhausted revision commit = %v; want EOVERFLOW", err)
	}
	if after := retainedState(t, file); after.Revision != exhausted.Revision || after.Size != before.Size || after.Content != before.Content {
		t.Fatalf("exhausted commit changed state: %+v; before %+v", after, before)
	}
	assertRetainedUsage(t, store, 5)
}

func TestNativeRetainedConditionalCreateIdentityClaimsAndReset(t *testing.T) {
	store, _ := ownedRetainedStore(t, 100, sqlite.DefaultOptions())
	root := retainNode(t, store, ".", storage.ReadContent)
	target := retainedTarget(t, root, "file")
	metadata := opaqueMetadata(60)
	receipt, err := root.session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular, Metadata: metadata}, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}, retainedAction(t, root.epoch))
	if err != nil {
		t.Fatal(err)
	}
	file := retainedReference(t, root.session, root.epoch, receipt)
	created := retainedState(t, file)
	if created.Kind != storage.NodeRegular || !reflect.DeepEqual(created.Metadata, metadata) || created.Size != 0 || created.Revision != 1 {
		t.Fatalf("created node = %+v", created)
	}
	before := retainedPublish(t, file, 5)
	currentTarget := retainedTarget(t, root, "file")
	receipt, err = root.session.RetainAt(t.Context(), storage.RetainAtRequest{Target: currentTarget, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, retainedAction(t, root.epoch))
	if err != nil {
		t.Fatal(err)
	}
	readOnly := retainedReference(t, root.session, root.epoch, receipt)
	if opened := retainedState(t, readOnly); opened.ID != before.ID || !reflect.DeepEqual(opened.Metadata, metadata) || opened.Revision != before.Revision {
		t.Fatalf("retained existing = %+v; before %+v", opened, before)
	}
	if _, err := readOnly.Reserve(t.Context(), 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only reserve = %v; want EBADF", err)
	}
	if err := retainedCommit(t, readOnly, before.Revision, metastore.Object{}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only content = %v; want EBADF", err)
	}
	absentTarget := currentTarget
	absentTarget.ExpectedEntryID, absentTarget.ExpectedNodeID, absentTarget.ExpectedMetadataRevision = 0, 0, 0
	if _, err := root.session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: absentTarget, Initial: storage.NodeInitial{Kind: storage.NodeRegular}}, retainedAction(t, root.epoch)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("conditional absence now present = %v; want ESTALE", err)
	}
	wrong := currentTarget
	wrong.ExpectedNodeID++
	if _, err := root.session.RetainAt(t.Context(), storage.RetainAtRequest{Target: wrong}, retainedAction(t, root.epoch)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("wrong identity = %v; want ESTALE", err)
	}
	if _, err := root.session.Retain(t.Context(), storage.RetainRequest{NodeID: uint64(before.ID), Claim: storage.AccessClaim{Uses: storage.AllAccessUses << 1}}, retainedAction(t, root.epoch)); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("unknown claim bits = %v; want EINVAL", err)
	}
	metadataOnly := retainNode(t, store, "file", 0)
	if _, err := metadataOnly.Stat(t.Context(), storage.ObservationOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := metadataOnly.Capture(t.Context(), storage.FileIO{}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata-only content capture = %v; want EBADF", err)
	}
	receipt, err = root.session.ResetAndRetainAt(t.Context(), storage.ResetAndRetainRequest{Target: currentTarget, ExpectedRevision: before.MetadataRevision, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}, retainedAction(t, root.epoch))
	if err != nil {
		t.Fatal(err)
	}
	truncated := retainedReference(t, root.session, root.epoch, receipt)
	if after := retainedState(t, truncated); after.ID != before.ID || !reflect.DeepEqual(after.Metadata, metadata) || after.Size != 0 || after.Revision != before.Revision+1 {
		t.Fatalf("atomic reset = %+v; before %+v", after, before)
	}
	assertRetainedUsage(t, store, 0)
	if err := store.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	directory := retainNode(t, store, "directory", storage.ReadContent)
	if _, err := directory.Capture(t.Context(), storage.FileIO{}); !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("directory content capture = %v; want EISDIR", err)
	}
}

func TestNativeRetainedFinalCloseIgnoresPendingAdmission(t *testing.T) {
	options := sqlite.DefaultOptions()
	options.ObjectLimits.MaxPendingObjects = 1
	store, _ := ownedRetainedStore(t, 100, options)
	file := createRetainedNode(t, store, "file")
	retainedPublish(t, file, 5)
	if _, err := store.Reserve(t.Context(), "pending", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	assertRetainedUsage(t, store, 5)
	if err := retainedClose(t, file); err != nil {
		t.Fatalf("last close at full pending admission: %v", err)
	}
	assertRetainedUsage(t, store, 0)
	status, err := store.ObjectStatus(t.Context())
	if err != nil || !status.OverLimit || status.ReservedCount != 1 || status.GarbageCount != 1 || status.GarbageBytes != 5 {
		t.Fatalf("last-close pending state = %+v, %v", status, err)
	}
}

func TestNativeRetainedAdmissionRequiresPhysicalRelease(t *testing.T) {
	options := sqlite.DefaultOptions()
	options.MaxRetainedFiles = 2
	store, _ := ownedRetainedStore(t, 100, options)
	root := retainNode(t, store, ".", 0)
	target := retainedTarget(t, root, "file")
	receipt, err := root.session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, retainedAction(t, root.epoch))
	if err != nil {
		t.Fatal(err)
	}
	file := retainedReference(t, root.session, root.epoch, receipt)
	id := retainedState(t, file).ID
	for _, retired := range []bool{false, true} {
		if retired {
			if err := file.Retire(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		_, err := root.session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: retainedTarget(t, root, "overflow"), Initial: storage.NodeInitial{Kind: storage.NodeRegular}}, retainedAction(t, root.epoch))
		if !errors.Is(err, syscall.EMFILE) {
			t.Fatalf("retired=%v capacity admission = %v; want EMFILE", retired, err)
		}
		if _, err := store.Stat(t.Context(), "overflow"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("failed admission created a name: %v", err)
		}
	}
	if err := retainedClose(t, file); err != nil {
		t.Fatal(err)
	}
	receipt, err = root.session.RetainAt(t.Context(), storage.RetainAtRequest{Target: retainedTarget(t, root, "file"), Claim: storage.AccessClaim{Uses: storage.ReadContent}}, retainedAction(t, root.epoch))
	if err != nil {
		t.Fatal(err)
	}
	reopened := retainedReference(t, root.session, root.epoch, receipt)
	if got := retainedState(t, reopened); got.ID != id {
		t.Fatalf("admission after release = %+v; want identity %d", got, id)
	}
}

func detachRecoveryFiles(t *testing.T, f objectIntegrityFixture) {
	t.Helper()
	detachIntegrityFile(t, f)
	damageDatabase(t, f.path, `UPDATE nodes SET detached = 1 WHERE content = ?`, f.foreign)
	damageDatabase(t, f.path, `DELETE FROM entries WHERE node = (SELECT id FROM nodes WHERE content = ?)`, f.foreign)
}

func TestExclusiveOwnerReapsEveryVolumeAtFullPendingAdmission(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachRecoveryFiles(t, f)
	options := sqlite.DefaultOptions()
	options.ObjectLimits.MaxPendingObjects = 1
	store, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: f.path, Volume: "workspace", Allowance: 100,
		SQLite: options, Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatalf("exclusive retained-file recovery: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	status, err := store.ObjectStatus(t.Context())
	if err != nil || !status.OverLimit || status.GarbageCount != 2 || status.GarbageBytes != 40 {
		t.Fatalf("recovered pending objects = %+v, %v", status, err)
	}
	db := raw(t, f.path)
	defer db.Close()
	var detached, used, retired int64
	if err := db.QueryRow(`SELECT
		(SELECT count(*) FROM nodes WHERE detached = 1),
		(SELECT sum(used) FROM volumes),
		(SELECT count(*) FROM objects WHERE key IN (?, ?) AND state = 2)`, f.live, f.foreign).Scan(
		&detached, &used, &retired); err != nil {
		t.Fatal(err)
	}
	if detached != 0 || used != 0 || retired != 2 {
		t.Fatalf("recovery retained=%d used=%d retired=%d; want 0, 0, 2", detached, used, retired)
	}
}

func TestExclusiveOwnerRefusesCorruptRetainedGraphBeforeReaping(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachRecoveryFiles(t, f)
	damageDatabase(t, f.path, `UPDATE volumes SET used = 0 WHERE name = 'neighbour'`)
	before := retainedRecoveryRows(t, f.path)
	beforeSchema := schemaOf(t, f.path)
	store, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: f.path, Volume: "workspace", Allowance: 100,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if store != nil {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("exclusive recovery with corrupt foreign retained quota: %v; want EIO", err)
	}
	if after := retainedRecoveryRows(t, f.path); !reflect.DeepEqual(after, before) {
		t.Fatal("refused retained recovery changed current tree, metadata, objects, quota, or history")
	}
	if after := schemaOf(t, f.path); after != beforeSchema {
		t.Fatal("refused recovery changed schema")
	}
}

func TestExclusiveOwnerReapRollsBackObjectAndQuotaChanges(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachRecoveryFiles(t, f)
	damageDatabase(t, f.path, `CREATE TRIGGER refuse_retained_delete BEFORE DELETE ON nodes
		WHEN OLD.detached = 1 BEGIN SELECT RAISE(ABORT, 'retained cleanup blocked'); END`)
	before := retainedRecoveryRows(t, f.path)
	state, err := sqlite.InspectDurableState(t.Context(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: f.path, Volume: "workspace", Allowance: 100,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if store != nil {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("recovery with failed node cleanup: %v; want EIO", err)
	}
	if after := retainedRecoveryRows(t, f.path); !reflect.DeepEqual(after, before) {
		t.Fatal("refused retained recovery changed current tree, metadata, objects, quota, or history")
	}
	after, err := sqlite.InspectDurableState(t.Context(), f.path)
	if err != nil || after != state {
		t.Fatalf("failed reap changed durable generation: %+v, %v; want %+v", after, err, state)
	}
}

func retainedRecoveryRows(t *testing.T, path string) map[string][][]any {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	result := make(map[string][][]any)
	for table, query := range map[string]string{
		"backing_store":   `SELECT * FROM backing_store ORDER BY singleton`,
		"database_state":  `SELECT * FROM database_state ORDER BY singleton`,
		"volumes":         `SELECT * FROM volumes ORDER BY id`,
		"nodes":           `SELECT * FROM nodes ORDER BY id`,
		"entries":         `SELECT * FROM entries ORDER BY volume, parent, name`,
		"objects":         `SELECT * FROM objects ORDER BY key`,
		"logs":            `SELECT * FROM logs ORDER BY volume`,
		"changes":         `SELECT * FROM changes ORDER BY position`,
		"removal_intents": `SELECT * FROM removal_intents ORDER BY volume, reference`,
		"sqlite_sequence": `SELECT * FROM sqlite_sequence ORDER BY name`,
	} {
		result[table] = nil
		rows, err := db.QueryContext(t.Context(), query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values, targets := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			for i, value := range values {
				if blob, ok := value.([]byte); ok {
					values[i] = bytes.Clone(blob)
				}
			}
			result[table] = append(result[table], values)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
	}
	return result
}
