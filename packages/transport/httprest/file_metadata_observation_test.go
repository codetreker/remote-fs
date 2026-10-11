package httprest

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestHTTPMetadataObservationEnrollsOnlyBoundedNamespaceWithoutStat(t *testing.T) {
	for _, path := range []string{"file", "node", "child", "directory"} {
		t.Run(path, func(t *testing.T) {
			client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
			kind := storage.NodeRegular
			if path == "directory" {
				kind = storage.NodeDirectory
				if err := backend.Mkdir(t.Context(), "file"); err != nil {
					t.Fatal(err)
				}
			} else if err := backend.Write(t.Context(), "file", []byte("body")); err != nil {
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
			session := httpInformationSession(t, client)
			if err := storage.OpenMetadataObservation(session).CheckOpenMetadataObservation(); err != nil {
				t.Fatal(err)
			}
			scopes := []storage.MetadataObservationScope{{Namespace: "test.flags", MaxPayloadBytes: 2}}
			var reference interface {
				Close(context.Context) error
				Stat(context.Context) (storage.Attr, error)
			}
			if path == "file" {
				reference = httpInformationFile(t, session, root.ID, attr.ID, scopes)
			} else {
				options := storage.NodeRefOptions{Kind: kind, MetadataAccess: storage.WriteMetadata, MetadataObservations: scopes, Action: httpFileAction(t, session), Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}}
				var opened storage.NodeOpenResult
				if path == "node" {
					opened, err = session.OpenNodeRef(t.Context(), attr.ID, options)
				} else {
					opened, err = session.OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}, options)
				}
				if err != nil {
					t.Fatal(err)
				}
				reference = opened.Reference
			}
			scopes[0] = storage.MetadataObservationScope{Namespace: "wrong", MaxPayloadBytes: 0}
			if _, err := reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("Stat granted=%v", err)
			}
			observer := reference.(storage.ReferenceMetadataObserver)
			if err := observer.CheckMetadataObservation(); err != nil {
				t.Fatal(err)
			}
			absent, err := observer.ObserveMetadata(t.Context(), 0)
			if err != nil || absent.NodeID != attr.ID || absent.Value != nil {
				t.Fatalf("absent=%+v err=%v", absent, err)
			}
			metadata := storage.MetadataAccess(session)
			payload, err := metadata.SetMetadata(t.Context(), attr.ID, "test.flags", nil, []byte{})
			if err != nil {
				t.Fatal(err)
			}
			empty, err := observer.ObserveMetadata(t.Context(), 0)
			if err != nil || empty.Value == nil || len(empty.Value.Data) != 0 || !bytes.Equal(empty.Value.Version, payload.Version) {
				t.Fatalf("present empty=%+v err=%v", empty, err)
			}
			payload, err = metadata.SetMetadata(t.Context(), attr.ID, "test.flags", payload.Version, []byte{1, 2})
			if err != nil {
				t.Fatal(err)
			}
			present, err := observer.ObserveMetadata(t.Context(), 0)
			if err != nil || present.Value == nil || !bytes.Equal(present.Value.Data, []byte{1, 2}) || !bytes.Equal(present.Value.Version, payload.Version) {
				t.Fatalf("present=%+v err=%v", present, err)
			}
			present.Value.Data[0] = 9
			fresh, err := observer.ObserveMetadata(t.Context(), 0)
			if err != nil || fresh.Value == nil || !bytes.Equal(fresh.Value.Data, []byte{1, 2}) {
				t.Fatalf("result aliases transport=%+v err=%v", fresh, err)
			}
			if got, err := observer.ObserveMetadata(t.Context(), 1); !errors.Is(err, syscall.EINVAL) || !reflect.DeepEqual(got, storage.ContentMetadataObservation{}) {
				t.Fatalf("unenrolled scope=%+v err=%v", got, err)
			}
			if _, err := metadata.SetMetadata(t.Context(), attr.ID, "test.flags", payload.Version, []byte{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			if got, err := observer.ObserveMetadata(t.Context(), 0); !errors.Is(err, syscall.EFBIG) || !reflect.DeepEqual(got, storage.ContentMetadataObservation{}) {
				t.Fatalf("oversize=%+v err=%v", got, err)
			}
			if err := reference.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got, err := observer.ObserveMetadata(t.Context(), 0); err == nil || !reflect.DeepEqual(got, storage.ContentMetadataObservation{}) {
				t.Fatalf("closed=%+v err=%v", got, err)
			}
		})
	}
}

func TestHTTPMetadataObservationRechecksCurrentEnrolledScopePolicy(t *testing.T) {
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
	policy := &httpInformationPolicy{}
	handler.authorizer = policy
	session := httpInformationSession(t, client)
	file := httpInformationFile(t, session, root.ID, attr.ID, []storage.MetadataObservationScope{{Namespace: "test.flags", MaxPayloadBytes: 2}})
	observer := file.(storage.ReferenceMetadataObserver)
	policy.mu.Lock()
	policy.requests = nil
	policy.mu.Unlock()
	if _, err := observer.ObserveMetadata(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	policy.deny(storage.OpFileObserveMetadata)
	if got, err := observer.ObserveMetadata(t.Context(), 0); !errors.Is(err, syscall.EACCES) || !reflect.DeepEqual(got, storage.ContentMetadataObservation{}) {
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
		if !reflect.DeepEqual(request.MetadataObservations, []storage.MetadataObservationScope{{Namespace: "test.flags", MaxPayloadBytes: 2}}) {
			t.Fatalf("authorization omitted sealed scope=%+v", request)
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

type httpObservationEnrollmentOnlyPolicy struct{}

func (httpObservationEnrollmentOnlyPolicy) Authorize(_ context.Context, request authz.AccessRequest) error {
	if request.Operation == storage.OpFileSetMetadata && len(request.MetadataObservations) == 0 {
		return authz.ErrDenied
	}
	return nil
}

func TestHTTPObservationEnrollmentDoesNotAuthorizeInitialMetadata(t *testing.T) {
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
			client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
			root, err := backend.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Write(t.Context(), "original", []byte("original")); err != nil {
				t.Fatal(err)
			}
			before, err := backend.Stat(t.Context(), "original")
			if err != nil {
				t.Fatal(err)
			}
			session := httpInformationSession(t, client)
			handler.authorizer = httpObservationEnrollmentOnlyPolicy{}
			fields := storage.InitialFields{Metadata: map[string][]byte{"arbitrary.secret": []byte("unauthorized")}}
			scopes := []storage.MetadataObservationScope{{Namespace: "test.flags", MaxPayloadBytes: 2}}
			name := "new"
			initial := storage.InitialState{OnCreate: fields}
			target := storage.ChildCondition{State: storage.Absent}
			uses := storage.UseClaim{Uses: storage.WriteData}
			if test.existing != storage.Keep {
				name = "original"
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
				opened, err := session.OpenAt(t.Context(), selection, storage.OpenAtOptions{Write: true, Create: true, Target: target, Existing: test.existing, Use: uses, MetadataAccess: storage.WriteMetadata, MetadataObservations: scopes, Initial: initial, Action: httpFileAction(t, session)})
				if !errors.Is(err, syscall.EACCES) || opened.File != nil {
					t.Fatalf("arbitrary file metadata accepted=%+v err=%v", opened, err)
				}
			} else {
				opened, err := session.OpenChildRef(t.Context(), selection, storage.NodeRefOptions{Kind: test.kind, Create: true, Target: target, MetadataAccess: storage.WriteMetadata, MetadataObservations: scopes, InitialState: initial, Action: httpFileAction(t, session)})
				if !errors.Is(err, syscall.EACCES) || opened.Reference != nil {
					t.Fatalf("arbitrary node metadata accepted=%+v err=%v", opened, err)
				}
			}
			if _, err := backend.Stat(t.Context(), "new"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("unauthorized open created node=%v", err)
			}
			after, err := backend.Stat(t.Context(), "original")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("unauthorized open changed original=%+v err=%v", after, err)
			}
			data, err := backend.Read(t.Context(), "original")
			if err != nil || !bytes.Equal(data, []byte("original")) {
				t.Fatalf("unauthorized open changed content=%q err=%v", data, err)
			}
		})
	}
}

func TestHTTPMetadataObservationScopesUseStrictWireShape(t *testing.T) {
	valid := []byte(`[{"namespace":"test.flags","maxPayloadBytes":2}]`)
	var scopes []metadataObservationScope
	if err := decodeFileJSON(valid, &scopes); err != nil {
		t.Fatal(err)
	}
	for name, malformed := range map[string]string{
		"missing namespace": `[{"maxPayloadBytes":2}]`,
		"missing bound":     `[{"namespace":"test.flags"}]`,
		"null namespace":    `[{"namespace":null,"maxPayloadBytes":2}]`,
		"null bound":        `[{"namespace":"test.flags","maxPayloadBytes":null}]`,
		"duplicate":         `[{"namespace":"test.flags","namespace":"other","maxPayloadBytes":2}]`,
		"unknown":           `[{"namespace":"test.flags","maxPayloadBytes":2,"extra":1}]`,
		"wrong case":        `[{"Namespace":"test.flags","maxPayloadBytes":2}]`,
		"fractional bound":  `[{"namespace":"test.flags","maxPayloadBytes":2.5}]`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := decodeFileJSON([]byte(malformed), &scopes); err == nil {
				t.Fatalf("malformed scope accepted: %s", malformed)
			}
		})
	}
	for _, scopes := range [][]storage.MetadataObservationScope{
		{{Namespace: "invalid namespace", MaxPayloadBytes: 2}},
		{{Namespace: "test.flags", MaxPayloadBytes: -1}},
		{{Namespace: "test.flags", MaxPayloadBytes: storage.MaxMetadataValueBytes + 1}},
		{{Namespace: "test.flags", MaxPayloadBytes: 2}, {Namespace: "test.flags", MaxPayloadBytes: 2}},
	} {
		if err := storage.CheckMetadataObservationScopes(metadataObservationScopesStorage(metadataObservationScopesOf(scopes))); err == nil {
			t.Fatalf("invalid enrollment accepted: %+v", scopes)
		}
	}
}
