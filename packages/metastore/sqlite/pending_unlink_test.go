package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
	"github.com/codetreker/remote-fs/packages/storage"
)

func pendingUnlinkStore(t *testing.T) *LockingStore {
	t.Helper()
	s, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func pendingUnlinkFile(t *testing.T, s *Store, name string, armed bool) *retainedFile {
	t.Helper()
	options := storage.OpenAtOptions{
		Read: true, Write: true, Create: true, Existing: storage.Keep,
		Target: storage.ChildCondition{State: storage.Any},
		Use:    storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName},
	}
	if armed {
		options.CloseIntent = &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile}
	}
	opened, err := s.OpenAt(t.Context(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: uint64(s.root)}, RawLeaf: []byte(name)}, options)
	if err != nil {
		t.Fatal(err)
	}
	f := opened.File.(*retainedFile)
	t.Cleanup(func() {
		if err := f.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return f
}

func pendingIntentCount(t *testing.T, s *Store, node int64) int {
	t.Helper()
	var count int
	if err := s.read.QueryRowContext(t.Context(), `SELECT count(*) FROM close_intents WHERE volume=? AND node=?`, s.volume, node).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func pendingChangesAfter(t *testing.T, s *Store, position metastore.Position) []metastore.Change {
	t.Helper()
	page, err := metastore.NewChangeResult(16, 0, func(_ int, _ metastore.Change, _ metastore.ChangePayloadLengths) (int64, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Since(t.Context(), position, 16, page); err != nil {
		t.Fatal(err)
	}
	changes, err := page.Changes()
	if err != nil {
		t.Fatal(err)
	}
	return changes
}

func TestPendingUnlinkCloseActivatesBeforeLastPinAndClearPreservesOtherIntents(t *testing.T) {
	s := pendingUnlinkStore(t)
	first := pendingUnlinkFile(t, s.Store, "file", true)
	second := pendingUnlinkFile(t, s.Store, "file", true)
	reader := pendingUnlinkFile(t, s.Store, "file", false)
	state, err := reader.State(t.Context())
	if err != nil || state.PendingUnlink || len(state.PendingGeneration) != 0 || pendingIntentCount(t, s.Store, first.id) != 2 {
		t.Fatalf("armed state=%+v error=%v", state, err)
	}
	if err := first.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, err = reader.State(t.Context())
	if err != nil || !state.PendingUnlink || pendingIntentCount(t, s.Store, first.id) != 1 {
		t.Fatalf("first close state=%+v error=%v", state, err)
	}
	firstGeneration := bytes.Clone(state.PendingGeneration)
	if _, err := s.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); !errors.Is(err, storage.ErrPendingDelete) {
		t.Fatalf("pending path open=%v", err)
	}
	state, err = reader.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: firstGeneration})
	if err != nil || state.PendingUnlink || len(state.PendingGeneration) != 0 || pendingIntentCount(t, s.Store, first.id) != 1 {
		t.Fatalf("clear state=%+v error=%v", state, err)
	}
	if err := second.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, err = reader.State(t.Context())
	if err != nil || !state.PendingUnlink || bytes.Equal(state.PendingGeneration, firstGeneration) || pendingIntentCount(t, s.Store, first.id) != 0 {
		t.Fatalf("second close state=%+v error=%v", state, err)
	}
	if _, err := reader.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: firstGeneration}); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale generation clear=%v", err)
	}
	if _, err := reader.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: state.PendingGeneration}); err != nil {
		t.Fatal(err)
	}
	for _, f := range []*retainedFile{first, second, reader} {
		if err := f.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Stat(t.Context(), "file"); err != nil {
		t.Fatalf("cleared file=%v", err)
	}
}

func TestPendingUnlinkNonemptyDirectoryConsumesArmedClose(t *testing.T) {
	s := pendingUnlinkStore(t)
	opened, err := s.OpenChildRef(t.Context(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: uint64(s.root)}, RawLeaf: []byte("dir")}, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Create: true, Target: storage.ChildCondition{State: storage.Absent},
		Use: storage.UseClaim{Uses: storage.ReadEntries | storage.DeleteName}, MetadataAccess: storage.ReadMetadata,
		CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: storage.UnlinkIfEmpty},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := s.Create(t.Context(), "dir/child"); err != nil {
		t.Fatalf("armed directory rejected child creation: %v", err)
	}
	deletion := opened.Reference.(metastore.DeleteIntent)
	if _, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkIfEmpty}); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("nonempty Now=%v", err)
	}
	before, err := opened.Reference.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	position, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Reference.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err := s.Stat(t.Context(), "dir")
	if err != nil || before.ChangeTime == nil || after.ChangeTime == nil || !before.ChangeTime.Equal(*after.ChangeTime) || len(pendingChangesAfter(t, s.Store, position)) != 0 {
		t.Fatalf("consumed nonempty intent changed node time or history: %+v error=%v", after, err)
	}
	if pendingIntentCount(t, s.Store, opened.State.ID) != 0 {
		t.Fatal("nonempty directory retained its triggered intent")
	}
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(t.Context(), "dir/child"); err != nil {
		t.Fatalf("nonempty close removed directory: %v", err)
	}
}

func TestPendingUnlinkEmptyDirectoryActivatesBeforeLastReferenceAndBlocksChildren(t *testing.T) {
	s := pendingUnlinkStore(t)
	armed, err := s.OpenChildRef(t.Context(), namespaceName(s.root, "dir"), storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Create: true, Target: storage.ChildCondition{State: storage.Absent},
		Use: storage.UseClaim{Uses: storage.DeleteName}, MetadataAccess: storage.ReadMetadata,
		CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: storage.UnlinkIfEmpty},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := armed.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	reader, err := s.OpenNodeRef(t.Context(), uint64(armed.State.ID), storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.Any}, MetadataAccess: storage.ReadMetadata,
		Use: storage.UseClaim{Uses: storage.ReadEntries},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := armed.Reference.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state, err := reader.Reference.(metastore.ReferenceStateAccess).State(t.Context()); err != nil || !state.PendingUnlink || state.State.Detached {
		t.Fatalf("pending directory=%+v error=%v", state, err)
	}
	if err := s.Create(t.Context(), "dir/child"); !errors.Is(err, storage.ErrPendingDelete) {
		t.Fatalf("pending directory accepted a child=%v", err)
	}
	if err := armed.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := reader.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(t.Context(), "dir"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("last directory reference did not finish unlink=%v", err)
	}
}

func TestPendingUnlinkTransitionsPublishChangeTimeAndOneCurrentNodeEvent(t *testing.T) {
	s := pendingUnlinkStore(t)
	f := pendingUnlinkFile(t, s.Store, "file", false)
	old := time.Unix(100, 0)
	if _, err := f.SetAttr(t.Context(), storage.AttrChange{ChangeTime: &old}); err != nil {
		t.Fatal(err)
	}
	position, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	armed := pendingUnlinkFile(t, s.Store, "file", true)
	state, err := f.State(t.Context())
	if err != nil || state.State.ChangeTime == nil || !state.State.ChangeTime.Equal(old) || len(pendingChangesAfter(t, s.Store, position)) != 0 {
		t.Fatalf("arming changed common facts=%+v error=%v", state, err)
	}
	var previousGeneration []byte
	for _, operation := range []string{"activate", "activate again", "clear", "trigger armed"} {
		position, err := s.CommittedPosition(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		switch operation {
		case "clear":
			state, err = f.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: state.PendingGeneration})
		case "trigger armed":
			err = armed.Retire(t.Context())
			if err == nil {
				state, err = f.State(t.Context())
			}
		default:
			state, err = f.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile})
		}
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		changes := pendingChangesAfter(t, s.Store, position)
		if len(changes) != 1 || changes[0].Kind != metastore.Modified || changes[0].Node == nil || changes[0].Node.ID != f.id || changes[0].Node.ChangeTime == nil || state.State.ChangeTime == nil || !changes[0].Node.ChangeTime.Equal(*state.State.ChangeTime) || !state.State.ChangeTime.After(old) {
			t.Fatalf("%s common facts/state mismatch: state=%+v changes=%+v", operation, state, changes)
		}
		if operation != "clear" {
			if !state.PendingUnlink || bytes.Equal(previousGeneration, state.PendingGeneration) {
				t.Fatalf("%s did not advance generation=%x", operation, state.PendingGeneration)
			}
			previousGeneration = bytes.Clone(state.PendingGeneration)
		}
	}
}

func TestPendingUnlinkFinalCloseUsesAnonymousStrongGateAndLiveAccounting(t *testing.T) {
	s := pendingUnlinkStore(t)
	f := pendingUnlinkFile(t, s.Store, "file", false)
	fixture := publicationFixture{store: s.Store, service: s.LockService()}
	fixture.put(t, t.Context(), "file", 5)
	if _, err := f.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile}); err != nil {
		t.Fatal(err)
	}
	owner := fixture.owner(t)
	grant := fixture.grant(t, owner, "file", locking.Exclusive)
	settlements := 0
	ctx := metastore.WithFilePublicationGuard(publicationScope(t.Context(), owner, grant), func() error { return syscall.ESTALE })
	ctx = storage.WithPublicationAccounting(ctx, func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous != 5 || next != 0 {
			return nil, fmt.Errorf("final close accounted %d -> %d", previous, next)
		}
		return func(result storage.PublicationResult) error {
			if result != storage.PublicationApplied {
				return fmt.Errorf("final close result=%v", result)
			}
			settlements++
			return nil
		}, nil
	})
	requirePublicationCode(t, f.Close(ctx), locking.Conflict)
	if f.active || f.closed || s.coordinator.pins[retainedNode{s.volume, f.id}] != 1 || settlements != 0 {
		t.Fatalf("blocked close active=%v closed=%v settlements=%d", f.active, f.closed, settlements)
	}
	if err := s.RetryPendingUnlinks(t.Context(), 1); err != nil {
		t.Fatalf("maintenance adopted live failed close: %v", err)
	}
	if _, err := fixture.service.Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(ctx); err != nil {
		t.Fatalf("anonymous fixed close=%v", err)
	}
	if settlements != 1 || !f.closed {
		t.Fatalf("close settlements=%d closed=%v", settlements, f.closed)
	}
	if _, err := s.StatNode(t.Context(), uint64(f.id)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("final node=%v", err)
	}
}

func TestPendingUnlinkArmedOpenUnderSharedStrongDefersActivation(t *testing.T) {
	s := pendingUnlinkStore(t)
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	fixture := publicationFixture{store: s.Store, service: s.LockService()}
	owner := fixture.owner(t)
	grant := fixture.grant(t, owner, "file", locking.Shared)
	f := pendingUnlinkFile(t, s.Store, "file", true)
	if state, err := f.State(t.Context()); err != nil || state.PendingUnlink {
		t.Fatalf("pure armed open under S=%+v error=%v", state, err)
	}
	requirePublicationCode(t, f.Retire(t.Context()), locking.Conflict)
	if f.active || f.closed || !f.closeIntent || pendingIntentCount(t, s.Store, f.id) != 1 || s.coordinator.pins[retainedNode{s.volume, f.id}] != 1 {
		t.Fatal("blocked activation lost reference ownership")
	}
	if err := s.fileDomain.coordinator.CheckUse(t.Context(), uint64(f.id), f.scope, storage.ReadData); err != nil {
		t.Fatalf("blocked activation released its use claim: %v", err)
	}
	if status, err := fixture.service.QueryGrant(t.Context(), owner, grant); err != nil || status.State != locking.Active {
		t.Fatalf("blocked activation changed S=%+v error=%v", status, err)
	}
	if _, err := fixture.service.Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("released activation did not finish unlink: %v", err)
	}
}

func TestPendingUnlinkRetireFailureKeepsIntentClaimAndPin(t *testing.T) {
	s := pendingUnlinkStore(t)
	opened, err := s.OpenAt(t.Context(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: uint64(s.root)}, RawLeaf: []byte("file")}, storage.OpenAtOptions{
		Read: true, Create: true, Existing: storage.Keep, Target: storage.ChildCondition{State: storage.Absent},
		Use:         storage.UseClaim{Uses: storage.ReadData | storage.DeleteName, Deny: storage.WriteData},
		CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	})
	if err != nil {
		t.Fatal(err)
	}
	f := opened.File.(*retainedFile)
	t.Cleanup(func() {
		if err := f.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.write.ExecContext(t.Context(), `CREATE TRIGGER reject_pending BEFORE UPDATE OF pending_unlink ON nodes BEGIN SELECT RAISE(ABORT,'pending rejected'); END`); err != nil {
		t.Fatal(err)
	}
	if err := f.Retire(t.Context()); err == nil {
		t.Fatal("intent activation failure reported success")
	}
	if f.active || f.closed || !f.closeIntent || pendingIntentCount(t, s.Store, f.id) != 1 || s.coordinator.pins[retainedNode{s.volume, f.id}] != 1 {
		t.Fatalf("failed retirement lost ownership: %+v", f)
	}
	if err := s.fileDomain.coordinator.CheckUse(t.Context(), uint64(f.id), storage.UseScope{}, storage.WriteData); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("failed retirement dropped the deny claim: %v", err)
	}
	if _, err := f.Node(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("failed retirement still permits I/O: %v", err)
	}
	if _, err := s.write.ExecContext(t.Context(), `DROP TRIGGER reject_pending`); err != nil {
		t.Fatal(err)
	}
	if err := f.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.closeIntent || pendingIntentCount(t, s.Store, f.id) != 0 {
		t.Fatal("retried retirement retained its consumed intent")
	}
	if err := f.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("retried cleanup=%v", err)
	}
}

func TestPendingUnlinkValidatesGuardsKindsScopesAndGeneration(t *testing.T) {
	s := pendingUnlinkStore(t)
	f := pendingUnlinkFile(t, s.Store, "file", false)
	for _, test := range []struct {
		name    string
		command storage.PendingUnlinkCommand
		want    error
	}{
		{"invalid condition", storage.PendingUnlinkCommand{}, syscall.EINVAL},
		{"wrong node kind", storage.PendingUnlinkCommand{Condition: storage.UnlinkIfEmpty}, syscall.ENOTDIR},
		{"changed parent", storage.PendingUnlinkCommand{Condition: storage.UnlinkFile, Guards: &storage.NamespaceGuards{RootID: uint64(f.id)}}, storage.ErrConditionConflict},
		{"unused target scope", storage.PendingUnlinkCommand{Condition: storage.UnlinkFile, Uses: []storage.TargetUse{{NodeID: uint64(s.root), Scope: f.scope}}}, syscall.EINVAL},
		{"unknown scope", storage.PendingUnlinkCommand{Condition: storage.UnlinkFile, Uses: []storage.TargetUse{{NodeID: uint64(f.id), Scope: storage.UseScope{Token: "0123456789abcdef0123456789abcdef"}}}}, storage.ErrInvalidScope},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := f.SetPendingUnlink(t.Context(), test.command); !errors.Is(err, test.want) {
				t.Fatalf("pending=%v want=%v", err, test.want)
			}
			state, err := f.State(t.Context())
			if err != nil || state.PendingUnlink {
				t.Fatalf("rejected request changed state=%+v error=%v", state, err)
			}
		})
	}
	state, err := f.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile, Uses: []storage.TargetUse{{NodeID: uint64(f.id), Scope: f.scope}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.PendingGeneration) != 8 || binary.BigEndian.Uint64(state.PendingGeneration) == 0 {
		t.Fatalf("invalid generation=%x", state.PendingGeneration)
	}
	for _, generation := range [][]byte{nil, {1}, binary.BigEndian.AppendUint64(nil, 0)} {
		if _, err := f.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: generation}); err == nil {
			t.Fatalf("clear accepted generation=%x", generation)
		}
	}
	if _, err := f.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: state.PendingGeneration}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: state.PendingGeneration}); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("already cleared generation=%v", err)
	}
}

func seedRecoveredCloseIntent(t *testing.T, s *Store, path string, size int64) int64 {
	t.Helper()
	if err := s.Create(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		fixture := publicationFixture{store: s, service: s.LockService()}
		fixture.put(t, t.Context(), path, size)
	}
	node, err := s.Stat(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	incarnation := [16]byte{0xff}
	reference := [16]byte{0xee}
	binary.BigEndian.PutUint64(reference[8:], uint64(node.ID))
	if err := s.mutate(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `INSERT INTO close_intents(volume,node,incarnation,reference,if_empty) VALUES(?,?,?,?,0)`, s.volume, node.ID, incarnation[:], reference[:])
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.fileDomain.recoveredReferences++
	s.coordinator.commit.release()
	return node.ID
}

func TestPendingUnlinkRecoveryIsBoundedFairAndUsesCurrentAccounting(t *testing.T) {
	config := lockingTestConfig(t)
	config.SQLite.MaxRetainedFiles = 1
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	pinned := pendingUnlinkFile(t, s.Store, "pinned", false)
	if _, err := pinned.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile}); err != nil {
		t.Fatal(err)
	}
	first := seedRecoveredCloseIntent(t, s.Store, "first", 5)
	second := seedRecoveredCloseIntent(t, s.Store, "second", 7)
	if s.fileDomain.recoveredReferences != 2 {
		t.Fatalf("recovered references=%d", s.fileDomain.recoveredReferences)
	}
	oldCalls, currentFreed := 0, int64(0)
	old := storage.PublicationAccountingChain{}.With(func(_, _ int64) (storage.PublicationSettlement, error) {
		oldCalls++
		return nil, errors.New("obsolete accounting chain")
	})
	if err := s.BindMaintenanceAccounting(t.Context(), old, func(int64) {}); err != nil {
		t.Fatal(err)
	}
	current := storage.PublicationAccountingChain{}.With(func(previous, next int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error {
			if result != storage.PublicationApplied {
				return fmt.Errorf("recovery publication=%v", result)
			}
			currentFreed += previous - next
			return nil
		}, nil
	})
	if err := s.BindMaintenanceAccounting(t.Context(), current, func(used int64) {
		if used != 12 {
			t.Fatalf("binding used=%d", used)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryPendingUnlinks(t.Context(), 64); err != nil {
		t.Fatal(err)
	}
	if pendingIntentCount(t, s.Store, first) != 1 || pendingIntentCount(t, s.Store, second) != 1 || currentFreed != 0 || s.fileDomain.recoveredReferences != 2 {
		t.Fatal("first bounded pass crossed the pinned candidate")
	}
	if err := s.RetryPendingUnlinks(t.Context(), 64); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StatNode(t.Context(), uint64(first)); !errors.Is(err, syscall.ESTALE) || pendingIntentCount(t, s.Store, second) != 1 || currentFreed != 5 || s.fileDomain.recoveredReferences != 1 {
		t.Fatalf("second bounded pass first=%v freed=%d", err, currentFreed)
	}
	if err := s.RetryPendingUnlinks(t.Context(), 64); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StatNode(t.Context(), uint64(second)); !errors.Is(err, syscall.ESTALE) || currentFreed != 12 || oldCalls != 0 || s.fileDomain.recoveredReferences != 0 {
		t.Fatalf("third bounded pass second=%v freed=%d old calls=%d", err, currentFreed, oldCalls)
	}
	if err := s.RetryPendingUnlinks(t.Context(), 0); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid limit=%v", err)
	}
}

func TestPendingUnlinkRestartRestoresReferenceBudgetWithoutDoubleCountingPendingNode(t *testing.T) {
	config := lockingTestConfig(t)
	config.SQLite.MaxRetainedFiles = 2
	clock := &publicationClock{now: time.Now()}
	config.Locks.Clock = clock
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	initial := s
	t.Cleanup(func() {
		if !initial.Terminal() {
			if err := initial.Abort(); err != nil {
				t.Error(err)
			}
		}
	})
	clock.advance(s.RecoveryStart().Sub(clock.Now()))
	options := storage.OpenAtOptions{
		Read: true, Create: true, Existing: storage.Keep, Target: storage.ChildCondition{State: storage.Any},
		Use:         storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
		CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	}
	name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: uint64(s.root)}, RawLeaf: []byte("old")}
	first, err := s.OpenAt(t.Context(), name, options)
	if err != nil {
		s.Abort()
		t.Fatal(err)
	}
	if _, err := s.OpenAt(t.Context(), name, options); err != nil {
		s.Abort()
		t.Fatal(err)
	}
	if _, err := first.File.(metastore.DeleteIntent).SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile}); err != nil {
		s.Abort()
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "new"); err != nil {
		s.Abort()
		t.Fatal(err)
	}
	fixture := publicationFixture{store: s.Store, service: s.LockService()}
	fixture.grant(t, fixture.owner(t), "old", locking.Shared)
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	s, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if s.fileDomain.files != 0 || s.fileDomain.recoveredReferences != 2 {
		t.Fatalf("restored budget live=%d recovered=%d", s.fileDomain.files, s.fileDomain.recoveredReferences)
	}
	status, err := s.LockService().(locking.StatusService).Status(t.Context())
	if err != nil || !status.Recovering || status.RecoveryRemainingMillis <= 0 || pendingIntentCount(t, s.Store, first.State.ID) != 2 {
		t.Fatalf("restart lost acknowledged protection or armed intents: %+v error=%v", status, err)
	}
	if opened, err := s.OpenFile(t.Context(), "new", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); !errors.Is(err, syscall.EAGAIN) {
		if opened != nil {
			opened.Close(t.Context())
		}
		t.Fatalf("new open bypassed recovered reference budget=%v", err)
	}
	requirePublicationCode(t, s.RetryPendingUnlinks(t.Context(), 2), locking.Recovering)
	if s.fileDomain.recoveredReferences != 2 {
		t.Fatalf("blocked recovery released its budget=%d", s.fileDomain.recoveredReferences)
	}
	watermark, err := s.MaxLease(t.Context())
	if err != nil || watermark <= 0 {
		t.Fatalf("persisted lease watermark=%v error=%v", watermark, err)
	}
	clock.advance(s.RecoveryStart().Add(watermark).Sub(clock.Now()))
	status, err = s.LockService().(locking.StatusService).Status(t.Context())
	if err != nil || status.Recovering {
		t.Fatalf("recovery interval did not end at its deadline: %+v error=%v", status, err)
	}
	if opened, err := s.OpenFile(t.Context(), "new", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); !errors.Is(err, syscall.EAGAIN) {
		if opened != nil {
			opened.Close(t.Context())
		}
		t.Fatalf("expired Strong recovery bypassed retained cleanup budget=%v", err)
	}
	if err := s.RetryPendingUnlinks(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	if s.fileDomain.recoveredReferences != 0 {
		t.Fatalf("recovered budget remained charged=%d", s.fileDomain.recoveredReferences)
	}
	opened, err := s.OpenFile(t.Context(), "new", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatalf("completed recovery did not free admission=%v", err)
	}
	if err := opened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPendingUnlinkOrdinaryNameRemovalReleasesRecoveredReferenceBudget(t *testing.T) {
	for _, operation := range []string{"path remove", "namespace remove", "rename replacement"} {
		t.Run(operation, func(t *testing.T) {
			s := pendingUnlinkStore(t)
			id := seedRecoveredCloseIntent(t, s.Store, "old", 5)
			var err error
			switch operation {
			case "path remove":
				err = s.Remove(t.Context(), "old")
			case "namespace remove":
				_, err = s.MutateName(t.Context(), storage.NameCommand{Kind: storage.NameRemove, Name: namespaceName(s.root, "old"), Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(id)}})
			case "rename replacement":
				source := namespaceCreate(t, s.Store, s.root, "source", storage.NameCreate)
				_, err = s.MutateName(t.Context(), namespaceRename(s.root, "source", source.ID, "old", uint64(id), "old"))
			}
			if err != nil {
				t.Fatal(err)
			}
			if s.fileDomain.recoveredReferences != 0 || pendingIntentCount(t, s.Store, id) != 0 {
				t.Fatalf("removed obligation stayed charged=%d", s.fileDomain.recoveredReferences)
			}
			if _, err := s.StatNode(t.Context(), uint64(id)); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("old identity survived removal=%v", err)
			}
			if used, err := s.Usage(t.Context()); err != nil || used != 0 {
				t.Fatalf("removed obligation usage=%d error=%v", used, err)
			}
		})
	}
}

func TestPendingUnlinkRecoveryPreservesBlockedIntentAndAdvancesOtherNodes(t *testing.T) {
	s := pendingUnlinkStore(t)
	blocked := seedRecoveredCloseIntent(t, s.Store, "blocked", 5)
	other := seedRecoveredCloseIntent(t, s.Store, "other", 7)
	fixture := publicationFixture{store: s.Store, service: s.LockService()}
	owner := fixture.owner(t)
	grant := fixture.grant(t, owner, "blocked", locking.Shared)
	opened, err := s.OpenFile(t.Context(), "blocked", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if opened != nil {
		opened.Close(t.Context())
	}
	if !errors.Is(err, storage.ErrPendingDelete) {
		t.Fatalf("old armed intent allowed open: %v", err)
	}
	requirePublicationCode(t, s.RetryPendingUnlinks(t.Context(), 8), locking.Conflict)
	if pendingIntentCount(t, s.Store, blocked) != 1 {
		t.Fatal("Strong-blocked recovery consumed the old armed intent")
	}
	if _, err := s.StatNode(t.Context(), uint64(other)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("blocked predecessor starved another node: %v", err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 5 {
		t.Fatalf("blocked recovery usage=%d error=%v", used, err)
	}
	if _, err := fixture.service.Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryPendingUnlinks(t.Context(), 8); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StatNode(t.Context(), uint64(blocked)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("released recovery=%v", err)
	}
}

func TestPendingUnlinkGenerationCannotWrap(t *testing.T) {
	s := pendingUnlinkStore(t)
	f := pendingUnlinkFile(t, s.Store, "file", false)
	if err := s.mutate(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `UPDATE nodes SET pending_generation=9223372036854775807 WHERE volume=? AND id=?`, s.volume, f.id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile}); !errors.Is(err, syscall.EOVERFLOW) {
		t.Fatalf("exhausted generation=%v", err)
	}
	state, err := f.State(t.Context())
	if err != nil || state.PendingUnlink {
		t.Fatalf("failed activation changed state=%+v error=%v", state, err)
	}
}

func TestPendingUnlinkStateRequiresMetadataReadButMutationKeepsItsResult(t *testing.T) {
	s := pendingUnlinkStore(t)
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := s.OpenNodeRef(t.Context(), uint64(node.ID), storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.Any}, Use: storage.UseClaim{Uses: storage.DeleteName},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, err := opened.Reference.(metastore.ReferenceStateAccess).State(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("state query bypassed metadata read permission=%v", err)
	}
	deletion := opened.Reference.(metastore.DeleteIntent)
	state, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile})
	if err != nil || !state.PendingUnlink || state.State.ID != node.ID {
		t.Fatalf("delete-only mutation lost its atomic result=%+v error=%v", state, err)
	}
	if state, err := deletion.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: state.PendingGeneration}); err != nil || state.PendingUnlink {
		t.Fatalf("delete-only clear=%+v error=%v", state, err)
	}
}

func TestPendingUnlinkResultBudgetRefusalDoesNotPublishStateOrConstrainCleanup(t *testing.T) {
	s := pendingUnlinkStore(t)
	f := pendingUnlinkFile(t, s.Store, "file", false)
	refusal := errors.New("reference result does not fit")
	calls := 0
	ctx := storage.WithAttrResultBudget(t.Context(), func(attr storage.Attr, metadataBytes int64) error {
		calls++
		if attr.ID != uint64(f.id) || attr.Metadata != nil || metadataBytes < 6 {
			t.Fatalf("result budget received internal or loaded attributes: %+v bytes=%d", attr, metadataBytes)
		}
		return refusal
	})
	if _, err := f.State(ctx); !errors.Is(err, refusal) {
		t.Fatalf("state budget=%v", err)
	}
	before, err := f.State(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	position, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.SetPendingUnlink(ctx, storage.PendingUnlinkCommand{Condition: storage.UnlinkFile}); !errors.Is(err, refusal) {
		t.Fatalf("pending result budget=%v", err)
	}
	after, err := f.State(t.Context())
	if err != nil || after.PendingUnlink || before.State.ChangeTime == nil || after.State.ChangeTime == nil || !before.State.ChangeTime.Equal(*after.State.ChangeTime) || len(pendingChangesAfter(t, s.Store, position)) != 0 {
		t.Fatalf("refused pending result changed state=%+v error=%v", after, err)
	}
	pending, err := f.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile})
	if err != nil {
		t.Fatal(err)
	}
	position, err = s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ClearPendingUnlink(ctx, storage.ClearPendingUnlinkCommand{Generation: pending.PendingGeneration}); !errors.Is(err, refusal) {
		t.Fatalf("clear result budget=%v", err)
	}
	after, err = f.State(t.Context())
	if err != nil || !after.PendingUnlink || !bytes.Equal(after.PendingGeneration, pending.PendingGeneration) || len(pendingChangesAfter(t, s.Store, position)) != 0 {
		t.Fatalf("refused clear result changed state=%+v error=%v", after, err)
	}
	beforeCleanup := calls
	if err := f.Close(ctx); err != nil {
		t.Fatalf("non-returning cleanup inherited result budget=%v", err)
	}
	if calls != beforeCleanup {
		t.Fatal("cleanup admitted internal attributes against a caller result budget")
	}
}

func TestPendingUnlinkRecoveryRefusesBeforeStrongInitialization(t *testing.T) {
	s := pendingUnlinkStore(t)
	id := seedRecoveredCloseIntent(t, s.Store, "file", 0)
	authority := s.locks
	s.locks = nil
	err := s.RetryPendingUnlinks(t.Context(), 2)
	s.locks = authority
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("recovery bypassed durable Strong evidence=%v", err)
	}
	if pendingIntentCount(t, s.Store, id) != 1 || s.fileDomain.recoveredReferences != 1 {
		t.Fatal("pre-authority recovery changed a named intent")
	}
	if err := s.RetryPendingUnlinks(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if pendingIntentCount(t, s.Store, id) != 0 || s.fileDomain.recoveredReferences != 0 {
		t.Fatal("initialized authority did not complete recovered cleanup")
	}
}

func pendingUnlinkStoreWithoutStrong(t *testing.T) *Store {
	t.Helper()
	config := lockingTestConfig(t)
	owner, err := nativelease.AcquireDatabase(config.Database, true, true)
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultOptions()
	options.leaseRecoveryOwner, options.leaseOwner = true, owner
	s, err := OpenWithOptions(t.Context(), config.Database, config.Volume, 0, options)
	if err != nil {
		if closeErr := owner.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
			return
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	if s.locks != nil || s.leaseRecovery != nil {
		t.Fatal("plain exclusive fixture unexpectedly configured Strong")
	}
	return s
}

func TestPendingUnlinkRecoveryWithoutStrongAcceptsInitializedNoWork(t *testing.T) {
	s := pendingUnlinkStoreWithoutStrong(t)
	if err := s.RetryPendingUnlinks(t.Context(), 8); err != nil {
		t.Fatalf("ordinary no-work maintenance=%v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryPendingUnlinks(t.Context(), 8); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("zero-work counter bypassed retired ownership=%v", err)
	}
}

func TestPendingUnlinkRecoveryWithoutStrongUsesNormalDurableAdmission(t *testing.T) {
	s := pendingUnlinkStoreWithoutStrong(t)
	id := seedRecoveredCloseIntent(t, s, "old", 5)
	if err := s.RetryPendingUnlinks(t.Context(), 1); err != nil {
		t.Fatalf("plain exclusive recovery=%v", err)
	}
	if s.fileDomain.recoveredReferences != 0 || pendingIntentCount(t, s, id) != 0 {
		t.Fatal("plain recovery retained its completed obligation")
	}
	if _, err := s.StatNode(t.Context(), uint64(id)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("plain recovery did not delete the accepted identity=%v", err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("plain recovery usage=%d error=%v", used, err)
	}
	if err := s.RetryPendingUnlinks(t.Context(), 1); err != nil {
		t.Fatalf("completed plain recovery no-work pass=%v", err)
	}
}

func TestPendingUnlinkRecoveryBoundsWaitingForNativeGate(t *testing.T) {
	config := lockingTestConfig(t)
	config.SQLite.Advisory.FileOperationTimeout = 20 * time.Millisecond
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	err = s.RetryPendingUnlinks(context.Background(), 1)
	s.coordinator.commit.release()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unbounded maintenance gate wait=%v", err)
	}
}

func TestPendingUnlinkUnknownActivationRetainsNativeOwnership(t *testing.T) {
	s, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := s.OpenAt(t.Context(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: uint64(s.root)}, RawLeaf: []byte("file")}, storage.OpenAtOptions{
		Read: true, Create: true, Existing: storage.Keep, Target: storage.ChildCondition{State: storage.Absent},
		Use:         storage.UseClaim{Uses: storage.ReadData | storage.DeleteName, Deny: storage.WriteData},
		CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	})
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	f := opened.File.(*retainedFile)
	failure := errors.New("pending activation witness unavailable")
	s.witness = &retainedFailureWitness{failure: failure}
	err = f.Retire(t.Context())
	if !errors.Is(err, failure) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown activation=%v", err)
	}
	if !f.closeIntent || f.active || f.closed || s.coordinator.pins[retainedNode{s.volume, f.id}] != 1 || len(s.files) != 1 {
		t.Fatal("unknown activation released retained ownership")
	}
	if err := s.fileDomain.coordinator.CheckUse(t.Context(), uint64(f.id), storage.UseScope{}, storage.WriteData); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("unknown activation dropped accepted use claim: %v", err)
	}
	if err := f.Close(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("unknown close lost failure=%v", err)
	}
	if err := s.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("store released unknown reference=%v", err)
	}
	// Process teardown follows the assertions that ordinary cleanup retained the
	// reference, claim and native ownership despite an uncertain durable result.
	if err := s.locks.Close(); err != nil && !errors.Is(err, failure) {
		t.Fatal(err)
	}
	s.locks = nil
	s.witness = nil
	s.files = nil
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
}
