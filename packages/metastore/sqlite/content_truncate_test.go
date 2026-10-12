package sqlite

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestContentMetadataTruncatePublishesLengthAndEffectTogether(t *testing.T) {
	for _, size := range []int64{0, 2, 7} {
		t.Run(strconv.FormatInt(size, 10), func(t *testing.T) {
			store, file := openContentEffectFile(t, []storage.ContentMetadataEffect{testContentEffect()})
			before, err := file.Node(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			other, err := store.SetMetadata(t.Context(), uint64(file.id), "test.other", nil, []byte("preserved"))
			if err != nil {
				t.Fatal(err)
			}
			before, err = file.Node(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			object := metastore.Object{Size: size, ModTime: time.Now()}
			if size > 0 {
				object.Key, err = file.Reserve(t.Context(), size)
				if err != nil {
					t.Fatal(err)
				}
			}
			command := storage.FileMutation{Action: fileAction(t), Kind: storage.MutateTruncate, Size: size,
				ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.content": nil}}
			after, err := file.CommitMutation(t.Context(), command, before.Revision, object)
			if err != nil || after.ID != before.ID || after.Revision == before.Revision || after.Size != size || after.Content != object.Key ||
				!bytes.Equal(after.Metadata["test.content"].Data, []byte{'M', 1, 0x20, 0}) || !reflect.DeepEqual(after.Metadata["test.other"], other) {
				t.Fatalf("atomic EOF publication = %+v, %v", after, err)
			}
			wantAllocation, err := allocatedSize(storage.NodeRegular, size)
			if err != nil {
				t.Fatal(err)
			}
			if after.AllocationSize != wantAllocation || !after.ModTime.Equal(object.ModTime) || after.ChangeTime == nil ||
				before.ChangeTime != nil && after.ChangeTime.Equal(*before.ChangeTime) {
				t.Fatalf("EOF allocation/time publication = %+v", after)
			}
			assertMetadataAccounting(t, store.Store)
		})
	}
}

func TestContentMetadataTruncateCASRejectsReadonlyChangeWithoutEffects(t *testing.T) {
	store, file := openContentEffectFile(t, []storage.ContentMetadataEffect{testContentEffect()})
	initial, err := store.SetMetadata(t.Context(), uint64(file.id), "test.content", nil, []byte{'M', 1, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := file.ObserveMetadata(t.Context(), storage.MetadataObservationRequest{Purpose: storage.MetadataObservationContentEffect, EffectIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetMetadata(t.Context(), uint64(file.id), "test.content", initial.Version, []byte{'M', 1, 1, 0}); err != nil {
		t.Fatal(err)
	}
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	position, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command := storage.FileMutation{Action: fileAction(t), Kind: storage.MutateTruncate, Size: 0,
		ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.content": observed.Value.Version}}
	if _, err := file.CommitMutation(t.Context(), command, before.Revision, metastore.Object{ModTime: time.Now()}); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("readonly CAS = %v", err)
	}
	if after, err := file.Node(t.Context()); err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected EOF changed content/metadata/time = %+v, %v", after, err)
	}
	if got, err := store.CommittedPosition(t.Context()); err != nil || got != position {
		t.Fatalf("rejected EOF advanced log = %d, %v", got, err)
	}
}

func TestContentMetadataTruncateRequiresWritableLiveReference(t *testing.T) {
	store, file := openContentEffectFile(t, []storage.ContentMetadataEffect{testContentEffect()})
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command := storage.FileMutation{Action: fileAction(t), Kind: storage.MutateTruncate, Size: 0,
		ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.content": nil}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := file.CommitMutation(ctx, command, before.Revision, metastore.Object{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled EOF = %v", err)
	}
	readOnly, err := store.OpenNode(t.Context(), uint64(file.id), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close(context.Background())
	plain := command
	plain.ContentEffects, plain.ExpectedMetadata = nil, nil
	if _, err := readOnly.(*retainedFile).CommitMutation(t.Context(), plain, before.Revision, metastore.Object{}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read-only EOF = %v", err)
	}
	if err := file.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := file.CommitMutation(t.Context(), command, before.Revision, metastore.Object{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired EOF = %v", err)
	}
}
