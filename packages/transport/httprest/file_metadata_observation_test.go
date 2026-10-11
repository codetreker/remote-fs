package httprest

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func TestHTTPMetadataObservationReadsOnlyRequestedNamespaceWithoutStat(t *testing.T) {
	_, backend := memoryfixture.New(t, "namespace-observation-http", 1<<20, locking.DefaultOptions())
	for _, path := range []string{"file", "node", "child", "directory"} {
		t.Run(path, func(t *testing.T) {
			client, _ := retainedHTTPPair(t, DefaultFileLimits(), backend)
			kind := storage.NodeRegular
			if path == "directory" {
				kind = storage.NodeDirectory
				if err := backend.Mkdir(t.Context(), path); err != nil {
					t.Fatal(err)
				}
			} else if err := backend.Write(t.Context(), path, []byte("body")); err != nil {
				t.Fatal(err)
			}
			root, err := backend.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			attr, err := backend.Stat(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			session := httpStateSession(t, client)
			request := storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "test.flags", MaxPayloadBytes: 2}
			var reference interface {
				Close(context.Context) error
				Stat(context.Context) (storage.Attr, error)
			}
			if path == "file" {
				reference = httpStateFileNamed(t, session, root.ID, attr.ID, storage.WriteMetadata, path)
			} else {
				options := storage.NodeRefOptions{Kind: kind, MetadataAccess: storage.WriteMetadata, Action: httpFileAction(t, session), Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}}
				var opened storage.NodeOpenResult
				if path == "node" {
					opened, err = session.OpenNodeRef(t.Context(), attr.ID, options)
				} else {
					opened, err = session.OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte(path)}}, options)
				}
				if err != nil {
					t.Fatal(err)
				}
				reference = opened.Reference
			}
			if _, err := reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("Stat granted=%v", err)
			}
			observer := reference.(storage.ReferenceMetadataAccess)
			if err := observer.CheckMetadataAccess(); err != nil {
				t.Fatal(err)
			}
			absent, err := observer.ObserveMetadata(t.Context(), request)
			if err != nil || absent.NodeID != attr.ID || absent.Value != nil {
				t.Fatalf("absent=%+v err=%v", absent, err)
			}
			metadata := storage.MetadataAccess(session)
			payload, err := metadata.SetMetadata(t.Context(), attr.ID, "test.flags", nil, []byte{})
			if err != nil {
				t.Fatal(err)
			}
			empty, err := observer.ObserveMetadata(t.Context(), request)
			if err != nil || empty.Value == nil || len(empty.Value.Data) != 0 || !bytes.Equal(empty.Value.Version, payload.Version) {
				t.Fatalf("present empty=%+v err=%v", empty, err)
			}
			payload, err = metadata.SetMetadata(t.Context(), attr.ID, "test.flags", payload.Version, []byte{1, 2})
			if err != nil {
				t.Fatal(err)
			}
			present, err := observer.ObserveMetadata(t.Context(), request)
			if err != nil || present.Value == nil || !bytes.Equal(present.Value.Data, []byte{1, 2}) || !bytes.Equal(present.Value.Version, payload.Version) {
				t.Fatalf("present=%+v err=%v", present, err)
			}
			present.Value.Data[0] = 9
			fresh, err := observer.ObserveMetadata(t.Context(), request)
			if err != nil || fresh.Value == nil || !bytes.Equal(fresh.Value.Data, []byte{1, 2}) {
				t.Fatalf("result aliases transport=%+v err=%v", fresh, err)
			}
			if got, err := observer.ObserveMetadata(t.Context(), storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "invalid namespace", MaxPayloadBytes: 2}); !errors.Is(err, syscall.EINVAL) || !reflect.DeepEqual(got, storage.MetadataObservation{}) {
				t.Fatalf("invalid namespace=%+v err=%v", got, err)
			}
			if _, err := metadata.SetMetadata(t.Context(), attr.ID, "test.flags", payload.Version, []byte{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			if got, err := observer.ObserveMetadata(t.Context(), request); !errors.Is(err, syscall.EFBIG) || !reflect.DeepEqual(got, storage.MetadataObservation{}) {
				t.Fatalf("oversize=%+v err=%v", got, err)
			}
			if err := reference.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got, err := observer.ObserveMetadata(t.Context(), request); err == nil || !reflect.DeepEqual(got, storage.MetadataObservation{}) {
				t.Fatalf("closed=%+v err=%v", got, err)
			}
		})
	}
}

func TestHTTPMetadataObservationRechecksCurrentNamespacePolicy(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	policy := &httpStatePolicy{}
	handler.authorizer = policy
	session := httpStateSession(t, client)
	file := httpStateFile(t, session, root.ID, attr.ID, storage.WriteMetadata)
	request := storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "test.flags", MaxPayloadBytes: 2}
	observer := file.(storage.ReferenceMetadataAccess)
	policy.mu.Lock()
	policy.requests = nil
	policy.mu.Unlock()
	if _, err := observer.ObserveMetadata(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	policy.deny(storage.OpFileObserveMetadata)
	if got, err := observer.ObserveMetadata(t.Context(), request); !errors.Is(err, syscall.EACCES) || !reflect.DeepEqual(got, storage.MetadataObservation{}) {
		t.Fatalf("revoked observation=%+v err=%v", got, err)
	}
	policy.mu.Lock()
	requests := append([]authz.AccessRequest(nil), policy.requests...)
	policy.mu.Unlock()
	var observations int
	for _, request := range requests {
		if request.Operation != storage.OpFileObserveMetadata {
			continue
		}
		observations++
		if !reflect.DeepEqual(request.MetadataObservation, &storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "test.flags", MaxPayloadBytes: 2}) {
			t.Fatalf("authorization omitted requested namespace=%+v", request)
		}
	}
	if observations != 2 {
		t.Fatalf("observation authorization calls=%d", observations)
	}
	policy.deny("")
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type httpObservationOnlyPolicy struct{}

func (httpObservationOnlyPolicy) Authorize(_ context.Context, request authz.AccessRequest) error {
	if request.Operation == storage.OpFileSetMetadata && request.MetadataObservation == nil {
		return authz.ErrDenied
	}
	return nil
}

func TestHTTPObservationPermissionDoesNotAuthorizeInitialMetadata(t *testing.T) {
	_, backend := memoryfixture.New(t, "namespace-enrollment-http", 1<<20, locking.DefaultOptions())
	for _, test := range []struct {
		name     string
		kind     storage.NodeKind
		existing storage.ExistingEffect
	}{
		{name: "create file", kind: storage.NodeRegular, existing: storage.Keep},
		{name: "reset file", kind: storage.NodeRegular, existing: storage.ResetContent},
		{name: "replace file", kind: storage.NodeRegular, existing: storage.ReplaceNode},
		{name: "create directory", kind: storage.NodeDirectory, existing: storage.Keep},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, handler := retainedHTTPPair(t, DefaultFileLimits(), backend)
			originalName, newName := strings.ReplaceAll(test.name, " ", "_")+"-original", strings.ReplaceAll(test.name, " ", "_")+"-new"
			root, err := backend.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Write(t.Context(), originalName, []byte("original")); err != nil {
				t.Fatal(err)
			}
			before, err := backend.Stat(t.Context(), originalName)
			if err != nil {
				t.Fatal(err)
			}
			session := httpStateSession(t, client)
			handler.authorizer = httpObservationOnlyPolicy{}
			fields := storage.InitialFields{Metadata: map[string][]byte{"arbitrary.secret": []byte("unauthorized")}}
			name := newName
			initial := storage.InitialState{OnCreate: fields}
			target := storage.ChildCondition{State: storage.Absent}
			uses := storage.UseClaim{Uses: storage.WriteData}
			if test.existing != storage.Keep {
				name = originalName
				target = storage.ChildCondition{State: storage.SameNode, NodeID: before.ID}
				initial = storage.InitialState{}
				if test.existing == storage.ResetContent {
					initial.OnReset = fields
				} else {
					initial.OnReplace = fields
					uses.Uses |= storage.DeleteName
				}
			}
			selection := storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte(name)}}
			if test.kind == storage.NodeRegular {
				opened, err := session.OpenAt(t.Context(), selection, storage.OpenAtOptions{Write: true, Create: true, Target: target, Existing: test.existing, Use: uses, Initial: initial, Action: httpFileAction(t, session)})
				if !errors.Is(err, syscall.EACCES) || opened.File != nil {
					t.Fatalf("arbitrary file metadata accepted=%+v err=%v", opened, err)
				}
			} else {
				opened, err := session.OpenChildRef(t.Context(), selection, storage.NodeRefOptions{Kind: test.kind, Create: true, Target: target, InitialState: initial, Action: httpFileAction(t, session)})
				if !errors.Is(err, syscall.EACCES) || opened.Reference != nil {
					t.Fatalf("arbitrary node metadata accepted=%+v err=%v", opened, err)
				}
			}
			if _, err := backend.Stat(t.Context(), newName); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("unauthorized open created node=%v", err)
			}
			after, err := backend.Stat(t.Context(), originalName)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("unauthorized open changed original=%+v err=%v", after, err)
			}
			data, err := backend.Read(t.Context(), originalName)
			if err != nil || !bytes.Equal(data, []byte("original")) {
				t.Fatalf("unauthorized open changed content=%q err=%v", data, err)
			}
		})
	}
}

func TestHTTPMetadataObservationUsesStrictWireShape(t *testing.T) {
	valid := []byte(`{"purpose":2,"effectIndex":0,"namespace":"test.flags","maxPayloadBytes":2}`)
	var request metadataObservationRequest
	if err := decodeFileJSON(valid, &request); err != nil {
		t.Fatal(err)
	}
	for name, malformed := range map[string]string{
		"missing purpose":   `{"effectIndex":0,"namespace":"test.flags","maxPayloadBytes":2}`,
		"missing namespace": `{"purpose":2,"effectIndex":0,"maxPayloadBytes":2}`,
		"missing bound":     `{"purpose":2,"effectIndex":0,"namespace":"test.flags"}`,
		"null namespace":    `{"purpose":2,"effectIndex":0,"namespace":null,"maxPayloadBytes":2}`,
		"null bound":        `{"purpose":2,"effectIndex":0,"namespace":"test.flags","maxPayloadBytes":null}`,
		"duplicate":         `{"purpose":2,"effectIndex":0,"namespace":"test.flags","namespace":"other","maxPayloadBytes":2}`,
		"unknown":           `{"purpose":2,"effectIndex":0,"namespace":"test.flags","maxPayloadBytes":2,"extra":1}`,
		"wrong case":        `{"purpose":2,"effectIndex":0,"Namespace":"test.flags","maxPayloadBytes":2}`,
		"fractional bound":  `{"purpose":2,"effectIndex":0,"namespace":"test.flags","maxPayloadBytes":2.5}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := decodeFileJSON([]byte(malformed), &request); err == nil {
				t.Fatalf("malformed observation accepted: %s", malformed)
			}
		})
	}
	for _, request := range []storage.MetadataObservationRequest{
		{Purpose: storage.MetadataObservationNamespace, Namespace: "invalid namespace", MaxPayloadBytes: 2},
		{Purpose: storage.MetadataObservationNamespace, Namespace: "test.flags", MaxPayloadBytes: -1},
		{Purpose: storage.MetadataObservationNamespace, Namespace: "test.flags", MaxPayloadBytes: storage.MaxMetadataValueBytes + 1},
		{Purpose: storage.MetadataObservationNamespace, EffectIndex: 1, Namespace: "test.flags", MaxPayloadBytes: 2},
		{Purpose: storage.MetadataObservationContentEffect, Namespace: "test.flags"},
		{},
	} {
		if err := metadataObservationRequestOf(request).storage().Check(); err == nil {
			t.Fatalf("invalid observation accepted: %+v", request)
		}
	}
}
