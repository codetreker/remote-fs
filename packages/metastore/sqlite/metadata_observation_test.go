package sqlite

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func openMetadataObservationReference(t *testing.T, mode string, scopes []storage.MetadataObservationScope) (*LockingStore, *retainedFile) {
	t.Helper()
	store, _, node, selection := guardedFileFixture(t)
	var file *retainedFile
	switch mode {
	case "file":
		opened, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{Write: true, MetadataAccess: storage.WriteMetadata,
			MetadataObservations: scopes, Existing: storage.Keep, Action: fileAction(t),
			Use:    storage.UseClaim{Uses: storage.WriteData},
			Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}})
		if err != nil {
			t.Fatal(err)
		}
		file = opened.File.(*retainedFile)
	case "child":
		opened, err := store.OpenChildRef(t.Context(), selection, storage.NodeRefOptions{Kind: storage.NodeRegular,
			MetadataAccess: storage.WriteMetadata, MetadataObservations: scopes, Action: fileAction(t),
			Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}})
		if err != nil {
			t.Fatal(err)
		}
		file = opened.Reference.(*retainedNodeReference).file
	case "node":
		opened, err := store.OpenNodeRef(t.Context(), uint64(node.ID), storage.NodeRefOptions{Kind: storage.NodeRegular,
			MetadataAccess: storage.WriteMetadata, MetadataObservations: scopes, Action: fileAction(t),
			Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}})
		if err != nil {
			t.Fatal(err)
		}
		file = opened.Reference.(*retainedNodeReference).file
	default:
		t.Fatalf("unknown fixture mode %q", mode)
	}
	t.Cleanup(func() {
		if err := file.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return store, file
}

func TestMetadataObservationEnrollmentOwnsBoundsAcrossOpenPaths(t *testing.T) {
	for _, mode := range []string{"file", "child", "node"} {
		t.Run(mode, func(t *testing.T) {
			scopes := []storage.MetadataObservationScope{{Namespace: "test.observe", MaxPayloadBytes: 4}}
			store, file := openMetadataObservationReference(t, mode, scopes)
			if err := store.CheckOpenMetadataObservation(); err != nil {
				t.Fatal(err)
			}
			if err := file.CheckMetadataObservation(); err != nil {
				t.Fatal(err)
			}
			if mode != "file" {
				ref := &retainedNodeReference{file: file}
				if err := ref.CheckMetadataObservation(); err != nil {
					t.Fatal(err)
				}
				if got, err := ref.ObserveMetadata(t.Context(), 0); err != nil || got.NodeID != uint64(file.id) || got.Value != nil {
					t.Fatalf("node observer capability = %+v, %v", got, err)
				}
			}
			if got, err := file.ObserveMetadata(t.Context(), 0); err != nil || got.NodeID != uint64(file.id) || got.Value != nil {
				t.Fatalf("absence = %+v, %v", got, err)
			}
			value, err := store.SetMetadata(t.Context(), uint64(file.id), "test.observe", nil, []byte("data"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.SetMetadata(t.Context(), uint64(file.id), "test.private", nil, []byte("secret")); err != nil {
				t.Fatal(err)
			}
			scopes[0].Namespace, scopes[0].MaxPayloadBytes = "test.private", 1
			got, err := file.ObserveMetadata(t.Context(), 0)
			if err != nil || got.Value == nil || !bytes.Equal(got.Value.Data, value.Data) || !bytes.Equal(got.Value.Version, value.Version) {
				t.Fatalf("enrolled observation = %+v, %v", got, err)
			}
			got.Value.Data[0], got.Value.Version[0] = 0xff, 0xff
			fresh, err := file.ObserveMetadata(t.Context(), 0)
			if err != nil || fresh.Value == nil || !bytes.Equal(fresh.Value.Data, value.Data) || !bytes.Equal(fresh.Value.Version, value.Version) {
				t.Fatalf("owned observation = %+v, %v", fresh, err)
			}
			if _, err := file.State(t.Context()); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("observation granted full state: %v", err)
			}
			if got, err := file.ObserveMetadata(t.Context(), 1); !errors.Is(err, syscall.EINVAL) || got.NodeID != 0 || got.Value != nil {
				t.Fatalf("undeclared namespace = %+v, %v", got, err)
			}
		})
	}
}

func TestMetadataObservationEmptyPresentAndOversizedValue(t *testing.T) {
	store, file := openMetadataObservationReference(t, "node", []storage.MetadataObservationScope{{Namespace: "test.empty"}})
	value, err := store.SetMetadata(t.Context(), uint64(file.id), "test.empty", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := file.ObserveMetadata(t.Context(), 0)
	if err != nil || got.Value == nil || len(got.Value.Data) != 0 || !bytes.Equal(got.Value.Version, value.Version) {
		t.Fatalf("present empty = %+v, %v", got, err)
	}
	if _, err := store.SetMetadata(t.Context(), uint64(file.id), "test.empty", value.Version, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if got, err := file.ObserveMetadata(t.Context(), 0); !errors.Is(err, syscall.EFBIG) || got.NodeID != 0 || got.Value != nil {
		t.Fatalf("oversized enrolled value = %+v, %v", got, err)
	}
}

func TestMetadataObservationRejectsEnrollmentWithoutWriteMetadata(t *testing.T) {
	store, _, node, selection := guardedFileFixture(t)
	scopes := []storage.MetadataObservationScope{{Namespace: "test.observe", MaxPayloadBytes: 4}}
	for _, permissions := range []storage.MetadataPermissions{0, storage.ReadMetadata} {
		if got, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{Read: true, MetadataAccess: permissions,
			MetadataObservations: scopes, Existing: storage.Keep, Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadData},
			Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}}); !errors.Is(err, syscall.EINVAL) || got.File != nil {
			t.Fatalf("unprivileged file enrollment = %+v, %v", got, err)
		}
		if got, err := store.OpenChildRef(t.Context(), selection, storage.NodeRefOptions{Kind: storage.NodeRegular,
			MetadataAccess: permissions, MetadataObservations: scopes, Action: fileAction(t),
			Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}}); !errors.Is(err, syscall.EINVAL) || got.Reference != nil {
			t.Fatalf("unprivileged child enrollment = %+v, %v", got, err)
		}
		if got, err := store.OpenNodeRef(t.Context(), uint64(node.ID), storage.NodeRefOptions{Kind: storage.NodeRegular,
			MetadataAccess: permissions, MetadataObservations: scopes, Action: fileAction(t),
			Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}}); !errors.Is(err, syscall.EINVAL) || got.Reference != nil {
			t.Fatalf("unprivileged node enrollment = %+v, %v", got, err)
		}
	}
	read, err := store.OpenNode(t.Context(), uint64(node.ID), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close(context.Background())
	if got, err := read.(*retainedFile).ObserveMetadata(t.Context(), 0); !errors.Is(err, syscall.EINVAL) || got.NodeID != 0 || got.Value != nil {
		t.Fatalf("unprivileged observation = %+v, %v", got, err)
	}
	opened, err := store.OpenAt(t.Context(), selection, storage.OpenAtOptions{Read: true, Existing: storage.Keep,
		Use: storage.UseClaim{Uses: storage.ReadData}, Action: fileAction(t),
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)}})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.File.Close(context.Background())
	if got, err := opened.File.(*retainedFile).ObserveMetadata(t.Context(), 0); !errors.Is(err, syscall.EBADF) || got.NodeID != 0 || got.Value != nil {
		t.Fatalf("read-only data reference observed metadata = %+v, %v", got, err)
	}
}

func TestMetadataObservationRejectsMalformedEnvelopeAndNativeVersion(t *testing.T) {
	for _, version := range [][]byte{{1}, make([]byte, 8)} {
		store, file := openMetadataObservationReference(t, "node", []storage.MetadataObservationScope{{Namespace: "test.observe", MaxPayloadBytes: 4}})
		encoded, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{"test.observe": {Version: version, Data: []byte("data")}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`, encoded, store.volume, file.id); err != nil {
			t.Fatal(err)
		}
		if got, err := file.ObserveMetadata(t.Context(), 0); !errors.Is(err, syscall.EIO) || got.NodeID != 0 || got.Value != nil {
			t.Fatalf("malformed native version = %+v, %v", got, err)
		}
	}
	for _, encoded := range [][]byte{[]byte("broken"), bytes.Repeat([]byte{0}, storage.MaxMetadataBytes+1)} {
		store, file := openMetadataObservationReference(t, "node", []storage.MetadataObservationScope{{Namespace: "test.observe", MaxPayloadBytes: 4}})
		valid, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{"test.observe": {Version: binary.BigEndian.AppendUint64(nil, 1), Data: []byte("data")}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`, encoded, store.volume, file.id); err != nil {
			t.Fatal(err)
		}
		if got, err := file.ObserveMetadata(t.Context(), 0); !errors.Is(err, syscall.EIO) || got.NodeID != 0 || got.Value != nil {
			t.Fatalf("malformed envelope = %+v, %v", got, err)
		}
		if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`, valid, store.volume, file.id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMetadataObservationRequiresLiveSessionAndAuthority(t *testing.T) {
	_, file := openMetadataObservationReference(t, "node", []storage.MetadataObservationScope{{Namespace: "test.observe", MaxPayloadBytes: 4}})
	denied := errors.New("publication denied")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, test := range []struct {
		ctx  context.Context
		want error
	}{
		{metastore.WithReferenceSession(t.Context(), new(advisory.Session)), storage.ErrInvalidScope},
		{metastore.WithFilePublicationGuard(t.Context(), func() error { return denied }), denied},
		{ctx, context.Canceled},
	} {
		if got, err := file.ObserveMetadata(test.ctx, 0); !errors.Is(err, test.want) || got.NodeID != 0 || got.Value != nil {
			t.Fatalf("failed observation = %+v, %v", got, err)
		}
	}
	if err := file.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := file.ObserveMetadata(t.Context(), 0); !errors.Is(err, syscall.ESTALE) || got.NodeID != 0 || got.Value != nil {
		t.Fatalf("retired observation = %+v, %v", got, err)
	}
}
