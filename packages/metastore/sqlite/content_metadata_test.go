package sqlite

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func testContentEffect() storage.ContentMetadataEffect {
	return storage.ContentMetadataEffect{
		Namespace: "test.content", PayloadBytes: 4,
		PresentPrefix: []byte{'M', 1}, AbsentPayload: []byte{'M', 1, 0, 0},
		ClearMask: []byte{0, 0, 0x80, 0}, SetMask: []byte{0, 0, 0x20, 0},
	}
}

func openContentEffectFile(t *testing.T, effects []storage.ContentMetadataEffect) (*LockingStore, *retainedFile) {
	t.Helper()
	store, _, node, selection := guardedFileFixture(t)
	opened, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{
		Write: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.WriteData}, Existing: storage.Keep,
		ContentMetadataEffects: effects,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.File.(*retainedFile)
	t.Cleanup(func() {
		if err := file.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return store, file
}

func TestContentMetadataEnrollmentOwnsMasksAndObservationWithoutMetadataRights(t *testing.T) {
	effects := []storage.ContentMetadataEffect{testContentEffect()}
	store, file := openContentEffectFile(t, effects)
	if err := store.CheckOpenContentMetadata(); err != nil {
		t.Fatal(err)
	}
	if err := file.CheckContentMetadata(); err != nil {
		t.Fatal(err)
	}
	absent, err := file.ObserveContentMetadata(t.Context(), 0)
	if err != nil || absent.NodeID != uint64(file.id) || absent.Value != nil {
		t.Fatalf("absent observation = %+v, %v", absent, err)
	}
	initial, err := store.SetMetadata(t.Context(), uint64(file.id), "test.content", nil, []byte{'M', 1, 0x83, 0x55})
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.SetMetadata(t.Context(), uint64(file.id), "test.other", nil, []byte("preserved"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range [][]byte{effects[0].PresentPrefix, effects[0].AbsentPayload, effects[0].ClearMask, effects[0].SetMask} {
		for i := range p {
			p[i] = 0xff
		}
	}
	effects[0].Namespace = "wrong.namespace"
	effects[0].PayloadBytes = 1
	observed, err := file.ObserveContentMetadata(t.Context(), 0)
	if err != nil || observed.Value == nil || !bytes.Equal(observed.Value.Version, initial.Version) || !bytes.Equal(observed.Value.Data, initial.Data) {
		t.Fatalf("write-only observation = %+v, %v", observed, err)
	}
	if _, err := file.SetMetadata(t.Context(), "test.content", initial.Version, []byte("deny")); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("enrollment granted arbitrary metadata writes: %v", err)
	}
	at := time.Now()
	if _, err := file.SetAttr(t.Context(), storage.AttrChange{ModTime: &at}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("enrollment granted arbitrary attribute writes: %v", err)
	}
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key, err := file.Reserve(t.Context(), 4)
	if err != nil {
		t.Fatal(err)
	}
	command := storage.FileMutation{Action: fileAction(t), Kind: storage.MutateWriteAt, Data: []byte("next"),
		ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.content": observed.Value.Version}}
	result, err := file.CommitMutation(t.Context(), command, before.Revision, metastore.Object{Key: key, Size: 4, ModTime: at})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{'M', 1, 0x23, 0x55}
	if result.Content != key || result.Revision == before.Revision || !bytes.Equal(result.Metadata["test.content"].Data, want) ||
		bytes.Equal(result.Metadata["test.content"].Version, initial.Version) || !reflect.DeepEqual(result.Metadata["test.other"], other) {
		t.Fatalf("sealed publication = %+v", result)
	}
	observed.Value.Data[0] = 0xff
	observed.Value.Version[0] = 0xff
	fresh, err := file.ObserveContentMetadata(t.Context(), 0)
	if err != nil || fresh.Value == nil || !bytes.Equal(fresh.Value.Data, want) || bytes.Equal(fresh.Value.Version, observed.Value.Version) {
		t.Fatalf("observation aliased stored payload: %+v, %v", fresh, err)
	}
	assertMetadataAccounting(t, store.Store)
}

func TestContentMetadataCASAndRejectedRequestsLeaveContentTimesAndLogUntouched(t *testing.T) {
	store, file := openContentEffectFile(t, []storage.ContentMetadataEffect{testContentEffect()})
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command := storage.FileMutation{Action: fileAction(t), Kind: storage.MutateAppend, Data: []byte("x"),
		ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.content": nil}}
	key, err := file.Reserve(t.Context(), 5)
	if err != nil {
		t.Fatal(err)
	}
	after, err := file.CommitMutation(t.Context(), command, before.Revision, metastore.Object{Key: key, Size: 5, ModTime: time.Now()})
	if err != nil || !bytes.Equal(after.Metadata["test.content"].Data, []byte{'M', 1, 0x20, 0}) {
		t.Fatalf("absent sealed append = %+v, %v", after, err)
	}
	position, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	invalid := []struct {
		name   string
		change func(*storage.FileMutation)
		want   error
	}{
		{"stale absence", func(c *storage.FileMutation) {}, storage.ErrConditionConflict},
		{"missing condition", func(c *storage.FileMutation) { c.ExpectedMetadata = nil }, syscall.EINVAL},
		{"unknown index", func(c *storage.FileMutation) { c.ContentEffects = []uint16{1} }, syscall.EINVAL},
		{"duplicate index", func(c *storage.FileMutation) { c.ContentEffects = []uint16{0, 0} }, syscall.EINVAL},
		{"empty data effect", func(c *storage.FileMutation) { c.Data = nil }, syscall.EINVAL},
		{"arbitrary metadata", func(c *storage.FileMutation) {
			c.Metadata = map[string]storage.OpaquePayload{"test.other": {Data: []byte("bad")}}
		}, syscall.EINVAL},
		{"truncate effect", func(c *storage.FileMutation) { c.Kind, c.Size, c.Data = storage.MutateTruncate, 0, nil }, syscall.EINVAL},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			bad := command
			bad.Action = fileAction(t)
			test.change(&bad)
			if _, err := file.CommitMutation(t.Context(), bad, after.Revision, metastore.Object{Key: key, Size: 6, ModTime: time.Now()}); !errors.Is(err, test.want) {
				t.Fatalf("rejected mutation = %v, want %v", err, test.want)
			}
			unchanged, err := file.Node(t.Context())
			if err != nil || !reflect.DeepEqual(unchanged, after) {
				t.Fatalf("rejection changed revision/content/metadata/times = %+v, %v", unchanged, err)
			}
			if got, err := store.CommittedPosition(t.Context()); err != nil || got != position {
				t.Fatalf("rejection changed log = %d, %v", got, err)
			}
		})
	}
	assertMetadataAccounting(t, store.Store)
}

func TestContentMetadataObservationRequiresExactLiveWritableReference(t *testing.T) {
	store, file := openContentEffectFile(t, []storage.ContentMetadataEffect{testContentEffect()})
	if _, err := file.ObserveContentMetadata(t.Context(), 1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("unknown effect observation = %v", err)
	}
	otherSession := metastore.WithReferenceSession(t.Context(), new(advisory.Session))
	if _, err := file.ObserveContentMetadata(otherSession, 0); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("cross-session observation = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := file.ObserveContentMetadata(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled observation = %v", err)
	}
	denied := errors.New("reference authority stopped")
	guarded := metastore.WithFilePublicationGuard(t.Context(), func() error { return denied })
	if _, err := file.ObserveContentMetadata(guarded, 0); !errors.Is(err, denied) {
		t.Fatalf("publication guard observation = %v", err)
	}
	readOnly, err := store.OpenNode(t.Context(), uint64(file.id), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close(t.Context())
	if _, err := readOnly.(*retainedFile).ObserveContentMetadata(t.Context(), 0); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only observation = %v", err)
	}
	if err := file.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ObserveContentMetadata(t.Context(), 0); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired observation = %v", err)
	}
}

func TestContentMetadataGrowthLimitRollsBackTheWholeContentPublication(t *testing.T) {
	store, file := openContentEffectFile(t, []storage.ContentMetadataEffect{testContentEffect()})
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key, err := file.Reserve(t.Context(), 4)
	if err != nil {
		t.Fatal(err)
	}
	var usage int64
	if err := store.read.QueryRowContext(t.Context(), `SELECT metadata_used FROM volumes WHERE id=?`, store.volume).Scan(&usage); err != nil {
		t.Fatal(err)
	}
	store.maxMetadataBytes = usage
	position, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command := storage.FileMutation{Action: fileAction(t), Kind: storage.MutateWriteAt, Data: []byte("next"),
		ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.content": nil}}
	if _, err := file.CommitMutation(t.Context(), command, before.Revision, metastore.Object{Key: key, Size: 4, ModTime: time.Now()}); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("metadata growth refusal = %v", err)
	}
	after, err := file.Node(t.Context())
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("metadata growth refusal changed content/metadata/times: %+v, %v", after, err)
	}
	if got, err := store.CommittedPosition(t.Context()); err != nil || got != position {
		t.Fatalf("metadata growth refusal advanced log: %d, %v", got, err)
	}
	assertMetadataAccounting(t, store.Store)
}

func TestContentMetadataMalformedStoredPayloadDoesNotUseAbsenceDefault(t *testing.T) {
	for _, test := range []struct {
		name          string
		data, version []byte
	}{
		{"empty payload", []byte{}, binary.BigEndian.AppendUint64(nil, 1)},
		{"wrong prefix", []byte{'X', 1, 0, 0}, binary.BigEndian.AppendUint64(nil, 1)},
		{"wrong length", []byte{'M', 1, 0}, binary.BigEndian.AppendUint64(nil, 1)},
		{"unknown version format", []byte{'M', 1, 0, 0}, []byte{1}},
		{"zero version", []byte{'M', 1, 0, 0}, make([]byte, 8)},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, file := openContentEffectFile(t, []storage.ContentMetadataEffect{testContentEffect()})
			encoded, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{"test.content": {Version: test.version, Data: test.data}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`, encoded, store.volume, file.id); err != nil {
				t.Fatal(err)
			}
			if got, err := file.ObserveContentMetadata(t.Context(), 0); !errors.Is(err, syscall.EIO) || got.NodeID != 0 || got.Value != nil {
				t.Fatalf("corrupt observation = %+v, %v", got, err)
			}
			before, err := file.Node(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			key, err := file.Reserve(t.Context(), 4)
			if err != nil {
				t.Fatal(err)
			}
			position, err := store.CommittedPosition(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			command := storage.FileMutation{Action: fileAction(t), Kind: storage.MutateWriteAt, Data: []byte("next"),
				ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.content": test.version}}
			if _, err := file.CommitMutation(t.Context(), command, before.Revision, metastore.Object{Key: key, Size: 4, ModTime: time.Now()}); !errors.Is(err, syscall.EIO) {
				t.Fatalf("corrupt publication = %v", err)
			}
			after, err := file.Node(t.Context())
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("corrupt publication changed state: %+v, %v", after, err)
			}
			if got, err := store.CommittedPosition(t.Context()); err != nil || got != position {
				t.Fatalf("corrupt publication advanced log = %d, %v", got, err)
			}
		})
	}
}

func TestContentMetadataObservationRejectsCorruptAndUnboundedStoredEnvelope(t *testing.T) {
	for _, encoded := range [][]byte{{}, []byte("broken"), bytes.Repeat([]byte{0}, storage.MaxMetadataBytes+1)} {
		store, file := openContentEffectFile(t, []storage.ContentMetadataEffect{testContentEffect()})
		if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`, encoded, store.volume, file.id); err != nil {
			t.Fatal(err)
		}
		if got, err := file.ObserveContentMetadata(t.Context(), 0); !errors.Is(err, syscall.EIO) || got.NodeID != 0 || got.Value != nil {
			t.Fatalf("corrupt %d-byte envelope observation = %+v, %v", len(encoded), got, err)
		}
		// Restore a valid envelope so final native reference retirement can read
		// the node while checking the publication target.
		valid, err := storage.EncodeMetadata(nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`, valid, store.volume, file.id); err != nil {
			t.Fatal(err)
		}
	}
}
