package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func metadataTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.Context(), t.TempDir()+"/metadata.db", "workspace", 4096, DefaultWindow())
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

func metadataSet(t *testing.T, s *Store, id int64, namespace string, expected, data []byte, at time.Time) (storage.OpaquePayload, error) {
	t.Helper()
	var result storage.OpaquePayload
	err := s.mutate(t.Context(), func(tx *sql.Tx) error {
		var err error
		result, err = s.setNodeMetadata(t.Context(), tx, id, namespace, expected, data, at)
		if err != nil {
			return err
		}
		return s.recordNamedChanged(t.Context(), tx, id)
	})
	return result, err
}

func TestMetadataNamespaceCASPreservesOtherNamespacesAndOwnsPayloads(t *testing.T) {
	s := metadataTestStore(t)
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(10000, 2, 3, 4, 5, 6, 7, time.UTC)
	data := []byte("first")
	first, err := metadataSet(t, s, node.ID, "client.first", nil, data, at)
	if err != nil || binary.BigEndian.Uint64(first.Version) != 1 {
		t.Fatalf("first namespace=%+v, error=%v", first, err)
	}
	data[0] = 'x'
	if _, err := metadataSet(t, s, node.ID, "client.other", nil, []byte("opaque"), at); err != nil {
		t.Fatal(err)
	}
	empty, err := metadataSet(t, s, node.ID, "client.first", first.Version, nil, at)
	if err != nil || binary.BigEndian.Uint64(empty.Version) != 2 {
		t.Fatalf("empty replacement=%+v, error=%v", empty, err)
	}
	before, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Metadata) != 2 || len(before.Metadata["client.first"].Data) != 0 ||
		string(before.Metadata["client.other"].Data) != "opaque" || before.ChangeTime == nil || !before.ChangeTime.Equal(at) ||
		!before.BirthTime.Equal(*node.BirthTime) {
		t.Fatalf("namespace replacement changed unrelated facts: %+v", before)
	}
	barrier, err := s.Barrier(t.Context(), 128)
	if err != nil {
		t.Fatal(err)
	}
	var generation int64
	if err := s.read.QueryRow(`SELECT generation FROM database_state`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	for _, expected := range [][]byte{nil, first.Version, []byte("foreign")} {
		if _, err := metadataSet(t, s, node.ID, "client.first", expected, []byte("wrong"), time.Time{}); !errors.Is(err, storage.ErrConditionConflict) {
			t.Fatalf("stale version %x = %v", expected, err)
		}
	}
	empty.Version[7] = 99
	after, err := s.Stat(t.Context(), "file")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("conflict or caller mutation changed node: before=%+v after=%+v error=%v", before, after, err)
	}
	afterBarrier, err := s.Barrier(t.Context(), 128)
	var afterGeneration int64
	if err != nil || afterBarrier != barrier {
		t.Fatalf("conflict advanced log: before=%+v after=%+v error=%v", barrier, afterBarrier, err)
	}
	if err := s.read.QueryRow(`SELECT generation FROM database_state`).Scan(&afterGeneration); err != nil || afterGeneration != generation {
		t.Fatalf("conflict advanced generation from %d to %d: %v", generation, afterGeneration, err)
	}
}

func TestInitialNodeBudgetRejectsBeforeInsertAndRollsBackIdentity(t *testing.T) {
	s := metadataTestStore(t)
	if _, err := s.write.Exec(`CREATE TRIGGER reject_node_insert BEFORE INSERT ON nodes BEGIN SELECT RAISE(ABORT,'insert reached'); END`); err != nil {
		t.Fatal(err)
	}
	var before int64
	if err := s.read.QueryRow(`SELECT node_high_water FROM database_state`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(-123, 456)
	initial := storage.InitialFields{Metadata: map[string][]byte{"client.value": []byte("abc")}}
	calls := 0
	ctx := storage.WithAttrResultBudget(t.Context(), func(attr storage.Attr, length int64) error {
		calls++
		if attr.ID != uint64(before+1) || attr.Kind != storage.NodeRegular || attr.Metadata != nil || length != int64(6+8+len("client.value")+8+3) ||
			attr.BirthTime == nil || !attr.BirthTime.Equal(at) || attr.ChangeTime == nil || !attr.ChangeTime.Equal(at) {
			t.Errorf("incorrect unloaded insertion facts: %+v, bytes=%d", attr, length)
		}
		return syscall.EFBIG
	})
	err := s.mutate(ctx, func(tx *sql.Tx) error {
		_, err := s.insertNode(ctx, tx, storage.NodeRegular, initial, at)
		return err
	})
	if !errors.Is(err, syscall.EFBIG) || calls != 1 {
		t.Fatalf("insertion admission=%v, calls=%d", err, calls)
	}
	var after, sequence, count int64
	if err := s.read.QueryRow(`SELECT node_high_water,(SELECT seq FROM sqlite_sequence WHERE name='nodes'),(SELECT count(*) FROM nodes) FROM database_state`).Scan(&after, &sequence, &count); err != nil || after != before || sequence != before || count != 1 {
		t.Fatalf("refusal changed allocator/node state: highwater=%d sequence=%d count=%d error=%v", after, sequence, count, err)
	}
}

func TestInitialMetadataHasIndependentVersionsAndOwnedEmptyValues(t *testing.T) {
	input := map[string][]byte{"client.one": []byte("one"), "client.empty": nil}
	got, err := initialMetadata(input)
	if err != nil || len(got) != 2 {
		t.Fatalf("initial metadata=%+v, error=%v", got, err)
	}
	input["client.one"][0] = 'x'
	first := got["client.one"]
	first.Version[7] = 9
	if string(first.Data) != "one" || binary.BigEndian.Uint64(got["client.empty"].Version) != 1 {
		t.Fatalf("initial values share caller data or sibling versions: %+v", got)
	}
	if empty, err := initialMetadata(nil); err != nil || empty != nil {
		t.Fatalf("absent initial metadata=%+v, error=%v", empty, err)
	}
	if _, err := initialMetadata(map[string][]byte{"INVALID": nil}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid namespace=%v", err)
	}
}

func TestMetadataVersionExhaustionAndInvalidTokensDoNotPublish(t *testing.T) {
	for _, version := range [][]byte{binary.BigEndian.AppendUint64(nil, math.MaxUint64), make([]byte, 8), []byte("opaque")} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			s := metadataTestStore(t)
			if err := s.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			node, err := s.Stat(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{"client.value": {Version: version, Data: []byte("original")}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.write.Exec(`UPDATE nodes SET metadata=? WHERE id=?`, encoded, node.ID); err != nil {
				t.Fatal(err)
			}
			_, err = metadataSet(t, s, node.ID, "client.value", version, []byte("new"), time.Now())
			want := syscall.EIO
			if len(version) == 8 && binary.BigEndian.Uint64(version) == math.MaxUint64 {
				want = syscall.EOVERFLOW
			}
			if !errors.Is(err, want) {
				t.Fatalf("invalid/exhausted native token=%v; want %v", err, want)
			}
			var after []byte
			if err := s.read.QueryRow(`SELECT metadata FROM nodes WHERE id=?`, node.ID).Scan(&after); err != nil || !bytes.Equal(encoded, after) {
				t.Fatalf("refused token update changed payload: %x, error=%v", after, err)
			}
		})
	}
}

func TestInsertNodePreservesExplicitInstantsAndKindSpecificPayloads(t *testing.T) {
	s := metadataTestStore(t)
	at := time.Unix(345, 678)
	zero := time.Time{}
	ancient := time.Date(-500, 2, 3, 4, 5, 6, 789, time.FixedZone("offset", 3600))
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory, storage.NodeSymlink} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			tx, err := s.write.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			initial := storage.InitialFields{Attr: storage.AttrChange{BirthTime: &zero, ChangeTime: &ancient}, Metadata: map[string][]byte{"client.empty": nil}}
			if kind == storage.NodeSymlink {
				initial.LinkTarget = []byte("../raw\xff")
			}
			node, err := s.insertNode(t.Context(), tx, kind, initial, at)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := s.nodeByID(t.Context(), tx, node.ID)
			if err != nil || stored.Kind != kind || stored.BirthTime == nil || !stored.BirthTime.Equal(zero) || stored.ChangeTime == nil || !stored.ChangeTime.Equal(ancient) ||
				!stored.AccessTime.Equal(at) || !stored.ModTime.Equal(at) || !bytes.Equal(stored.LinkTarget, initial.LinkTarget) || stored.Size != int64(len(initial.LinkTarget)) ||
				binary.BigEndian.Uint64(stored.Metadata["client.empty"].Version) != 1 {
				t.Fatalf("inserted facts=%+v, error=%v", stored, err)
			}
			if kind == storage.NodeDirectory && !bytes.Equal(stored.DirectoryRevision, binary.BigEndian.AppendUint64(nil, 1)) || kind != storage.NodeDirectory && len(stored.DirectoryRevision) != 0 {
				t.Fatalf("wrong directory token=%x", stored.DirectoryRevision)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.mutate(ctx, func(tx *sql.Tx) error { return s.setNodeChangeTime(ctx, tx, s.root, at) }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled metadata operation=%v", err)
	}
}

func TestMetadataAdmissionUsesNetRetainedPayloadAfterTrim(t *testing.T) {
	options := DefaultOptions()
	options.Window = Window{Cap: 1, Floor: 1, Age: time.Hour}
	encoded, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{"client.value": {Version: binary.BigEndian.AppendUint64(nil, 1), Data: []byte("same")}})
	if err != nil {
		t.Fatal(err)
	}
	options.MaxMetadataBytes = int64(6 + 2*len(encoded))
	s, err := OpenWithOptions(t.Context(), t.TempDir()+"/bounded.db", "workspace", 4096, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	first, err := metadataSet(t, s, node.ID, "client.value", nil, []byte("same"), time.Unix(1, 2))
	if err != nil {
		t.Fatalf("exact node+retained-copy budget: %v", err)
	}
	second, err := metadataSet(t, s, node.ID, "client.value", first.Version, []byte("next"), time.Unix(3, 4))
	if err != nil {
		t.Fatalf("same-sized replacement was charged before trim: %v", err)
	}
	before, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	barrier, err := s.Barrier(t.Context(), 128)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadataSet(t, s, node.ID, "client.value", second.Version, []byte("extra"), time.Unix(5, 6)); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("over-budget replacement=%v", err)
	}
	after, err := s.Stat(t.Context(), "file")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("budget refusal changed node: before=%+v after=%+v error=%v", before, after, err)
	}
	afterBarrier, err := s.Barrier(t.Context(), 128)
	if err != nil || afterBarrier != barrier {
		t.Fatalf("budget refusal advanced history: %+v, %v", afterBarrier, err)
	}
	var used, rows int64
	if err := s.read.QueryRow(`SELECT metadata_used,(SELECT count(*) FROM changes) FROM volumes WHERE id=?`, s.volume).Scan(&used, &rows); err != nil || used != options.MaxMetadataBytes || rows != 1 {
		t.Fatalf("retained accounting=%d history=%d error=%v; want %d and1", used, rows, err, options.MaxMetadataBytes)
	}
}

func TestCommonBirthAndChangeTimesFollowNativeMutations(t *testing.T) {
	s := metadataTestStore(t)
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	created, err := s.Stat(t.Context(), "file")
	if err != nil || created.BirthTime == nil || created.ChangeTime == nil || !created.BirthTime.Equal(*created.ChangeTime) ||
		!created.ModTime.Equal(*created.BirthTime) || !created.AccessTime.Equal(*created.BirthTime) {
		t.Fatalf("new authority times=%+v, error=%v", created, err)
	}
	zero, oldChange := time.Time{}, time.Unix(-1000, 2)
	if err := s.SetAttr(t.Context(), "file", storage.AttrChange{BirthTime: &zero, ChangeTime: &oldChange}); err != nil {
		t.Fatal(err)
	}
	key, err := s.Reserve(t.Context(), "file", 3)
	if err != nil {
		t.Fatal(err)
	}
	modified := time.Date(10000, 1, 2, 3, 4, 5, 6, time.UTC)
	if err := s.Commit(t.Context(), "file", metastore.Object{Key: key, Size: 3, ModTime: modified}); err != nil {
		t.Fatal(err)
	}
	written, err := s.Stat(t.Context(), "file")
	if err != nil || written.BirthTime == nil || !written.BirthTime.Equal(zero) || written.ChangeTime == nil || written.ChangeTime.Equal(oldChange) || !written.ModTime.Equal(modified) {
		t.Fatalf("content publication changed common facts incorrectly: %+v, error=%v", written, err)
	}
	if err := s.SetAttr(t.Context(), "file", storage.AttrChange{ChangeTime: &oldChange}); err != nil {
		t.Fatal(err)
	}
	if err := s.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	moved, err := s.Stat(t.Context(), "moved")
	if err != nil || moved.ID != created.ID || moved.BirthTime == nil || !moved.BirthTime.Equal(zero) || moved.ChangeTime == nil || moved.ChangeTime.Equal(oldChange) || !moved.ModTime.Equal(modified) {
		t.Fatalf("rename changed common facts incorrectly: %+v, error=%v", moved, err)
	}
	result, err := metastore.NewChangeResult(1<<20, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
		return 128 + lengths.Name + lengths.FromName + lengths.Content + lengths.Metadata + lengths.Target, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Since(t.Context(), 0, 64, result); err != nil {
		t.Fatal(err)
	}
	events, err := result.Changes()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Kind == metastore.Renamed && event.Node != nil && event.Node.ID == moved.ID {
			found = true
			if !reflect.DeepEqual(event.Node.Clone(), moved.Clone()) {
				t.Fatalf("rename event differs from committed facts: event=%+v current=%+v", event.Node, moved)
			}
		}
	}
	if !found {
		t.Fatal("rename did not capture common timestamps")
	}
}
