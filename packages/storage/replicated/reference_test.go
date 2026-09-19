package replicated

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type referenceCapabilityStub struct {
	*fileAuthorityStub
	barrier  *httprest.MutationBarrier
	state    storage.ReferenceState
	seen     any
	checkErr error
}

func (r *referenceCapabilityStub) CheckScopedReference() error         { return r.checkErr }
func (r *referenceCapabilityStub) CheckReferenceState() error          { return r.checkErr }
func (r *referenceCapabilityStub) CheckMetadataAccess() error          { return r.checkErr }
func (r *referenceCapabilityStub) CheckDeleteIntent() error            { return r.checkErr }
func (r *referenceCapabilityStub) CheckConditionalFileMutation() error { return r.checkErr }
func (r *referenceCapabilityStub) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: "bound"}, nil
}
func (r *referenceCapabilityStub) State(context.Context) (storage.ReferenceState, error) {
	return r.state, nil
}
func (r *referenceCapabilityStub) Stat(context.Context) (storage.Attr, error) {
	return r.state.Attr, nil
}
func (r *referenceCapabilityStub) SetAttrWithBarrier(_ context.Context, c storage.AttrChange) (storage.Attr, *httprest.MutationBarrier, error) {
	r.seen = c
	return r.state.Attr, r.barrier, nil
}
func (r *referenceCapabilityStub) SetMetadataWithBarrier(_ context.Context, namespace string, version, payload []byte) (storage.OpaquePayload, *httprest.MutationBarrier, error) {
	r.seen = struct {
		Namespace        string
		Version, Payload []byte
	}{namespace, version, payload}
	return storage.OpaquePayload{Version: []byte{8}, Data: payload}, r.barrier, nil
}
func (r *referenceCapabilityStub) SetMetadata(context.Context, string, []byte, []byte) (storage.OpaquePayload, error) {
	panic("mutation bypassed barrier")
}
func (r *referenceCapabilityStub) SetPendingUnlinkWithBarrier(_ context.Context, c storage.PendingUnlinkCommand) (storage.ReferenceState, *httprest.MutationBarrier, error) {
	r.seen = c
	return r.state, r.barrier, nil
}
func (r *referenceCapabilityStub) ClearPendingUnlinkWithBarrier(_ context.Context, c storage.ClearPendingUnlinkCommand) (storage.ReferenceState, *httprest.MutationBarrier, error) {
	r.seen = c
	return r.state, r.barrier, nil
}
func (r *referenceCapabilityStub) SetPendingUnlink(context.Context, storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	panic("mutation bypassed barrier")
}
func (r *referenceCapabilityStub) ClearPendingUnlink(context.Context, storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	panic("mutation bypassed barrier")
}
func (r *referenceCapabilityStub) MutateFileWithBarrier(_ context.Context, c storage.FileMutation) (storage.Attr, *httprest.MutationBarrier, error) {
	r.seen = c
	return r.state.Attr, r.barrier, nil
}
func (r *referenceCapabilityStub) MutateFile(context.Context, storage.FileMutation) (storage.Attr, error) {
	panic("mutation bypassed barrier")
}

type testedReference interface {
	storage.NodeReference
	storage.ScopedReference
	storage.ReferenceStateAccess
	storage.ReferenceMetadataAccess
	storage.DeleteIntent
}

func TestReferenceCapabilitiesPreserveAuthorityStateAndBarriers(t *testing.T) {
	for _, kind := range []string{"file", "node"} {
		t.Run(kind, func(t *testing.T) {
			session := retainedTestSession(t, nil)
			closed := 0
			state := storage.ReferenceState{Attr: storage.Attr{ID: 9, Kind: storage.NodeRegular}, Detached: true, PendingUnlink: true, PendingGeneration: []byte{0xff}}
			if kind == "node" {
				state.Attr.Kind = storage.NodeSymlink
				state.Attr.Size = 2
				state.LinkTarget = []byte{0xfe, 0xff}
			}
			remote := &referenceCapabilityStub{fileAuthorityStub: &fileAuthorityStub{close: func(context.Context) error { closed++; return nil }}, barrier: &httprest.MutationBarrier{Incarnation: "log"}, state: state}
			var ref testedReference
			if kind == "file" {
				ref = &retainedFile{session: session, remote: remote}
			} else {
				ref = &nodeReference{session: session, remote: remote}
			}
			for _, check := range []func() error{ref.CheckScopedReference, ref.CheckReferenceState, ref.CheckMetadataAccess, ref.CheckDeleteIntent} {
				if err := check(); err != nil {
					t.Fatal(err)
				}
			}
			if got, err := ref.Scope(t.Context()); err != nil || got.Token != "bound" {
				t.Fatalf("reference scope: %+v, %v", got, err)
			}
			if got, err := ref.State(t.Context()); err != nil || !reflect.DeepEqual(got, remote.state) {
				t.Fatalf("captured reference state: %+v, %v", got, err)
			}
			if got, err := ref.Stat(t.Context()); err != nil || !reflect.DeepEqual(got, remote.state.Attr) {
				t.Fatalf("reference stat: %+v, %v", got, err)
			}
			change := storage.AttrChange{}
			if got, err := ref.SetAttr(t.Context(), change); err != nil || !reflect.DeepEqual(got, remote.state.Attr) {
				t.Fatalf("reference setattr: %+v, %v", got, err)
			}
			payload, err := ref.SetMetadata(t.Context(), "test.blob", []byte{2}, []byte{0xff})
			if err != nil || !reflect.DeepEqual(payload, storage.OpaquePayload{Version: []byte{8}, Data: []byte{0xff}}) {
				t.Fatalf("reference metadata: %+v, %v", payload, err)
			}
			if !reflect.DeepEqual(remote.seen, struct {
				Namespace        string
				Version, Payload []byte
			}{"test.blob", []byte{2}, []byte{0xff}}) {
				t.Fatal("reference metadata condition changed")
			}
			pending := storage.PendingUnlinkCommand{Condition: storage.UnlinkIfEmpty}
			if got, err := ref.SetPendingUnlink(t.Context(), pending); err != nil || !reflect.DeepEqual(got, remote.state) || !reflect.DeepEqual(remote.seen, pending) {
				t.Fatalf("pending unlink: %+v, %v", got, err)
			}
			clear := storage.ClearPendingUnlinkCommand{Generation: []byte{0xff}}
			if got, err := ref.ClearPendingUnlink(t.Context(), clear); err != nil || !reflect.DeepEqual(got, remote.state) || !reflect.DeepEqual(remote.seen, clear) {
				t.Fatalf("clear pending unlink: %+v, %v", got, err)
			}
			remote.barrier = nil
			if got, err := ref.SetMetadata(t.Context(), "test.blob", nil, nil); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, storage.OpaquePayload{}) {
				t.Fatalf("metadata accepted no publication barrier: %+v, %v", got, err)
			}
			if err := ref.Close(t.Context()); err != nil || closed != 1 {
				t.Fatalf("reference close: %v; calls %d", err, closed)
			}
			if err := ref.Close(t.Context()); err != nil || closed != 1 {
				t.Fatalf("reference close repeated cleanup: %v; calls %d", err, closed)
			}
		})
	}
}

func TestConditionalFileMutationConfirmsCapturedAttributes(t *testing.T) {
	session := retainedTestSession(t, nil)
	remote := &referenceCapabilityStub{fileAuthorityStub: &fileAuthorityStub{}, barrier: &httprest.MutationBarrier{Incarnation: "log"}, state: storage.ReferenceState{Attr: storage.Attr{ID: 3, Size: 11}}}
	file := &retainedFile{session: session, remote: remote}
	if err := file.CheckConditionalFileMutation(); err != nil {
		t.Fatal(err)
	}
	mutation := storage.FileMutation{Kind: storage.MutateTruncate, Size: 11}
	attr, err := file.MutateFile(t.Context(), mutation)
	if err != nil || !reflect.DeepEqual(attr, remote.state.Attr) || !reflect.DeepEqual(remote.seen, mutation) {
		t.Fatalf("conditional mutation: %+v, %v", attr, err)
	}
	remote.barrier = nil
	if attr, err := file.MutateFile(t.Context(), mutation); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(attr, storage.Attr{}) {
		t.Fatalf("conditional mutation lacked barrier: %+v, %v", attr, err)
	}
}

func TestReferenceCapabilitiesRequireBackingSupport(t *testing.T) {
	session := retainedTestSession(t, nil)
	cause := errors.New("backing capability unavailable")
	remote := &referenceCapabilityStub{fileAuthorityStub: &fileAuthorityStub{}, checkErr: cause}
	file := &retainedFile{session: session, remote: remote}
	node := &nodeReference{session: session, remote: remote}
	for _, check := range []func() error{file.CheckScopedReference, file.CheckReferenceState, file.CheckMetadataAccess, file.CheckDeleteIntent, file.CheckConditionalFileMutation, node.CheckScopedReference, node.CheckReferenceState, node.CheckMetadataAccess, node.CheckDeleteIntent} {
		if !errors.Is(check(), cause) {
			t.Fatal("reference capability check lost backend failure")
		}
	}
	file.remote = &fileAuthorityStub{}
	node.remote = &fileAuthorityStub{}
	for _, check := range []func() error{file.CheckScopedReference, file.CheckReferenceState, file.CheckMetadataAccess, file.CheckDeleteIntent, file.CheckConditionalFileMutation, node.CheckScopedReference, node.CheckReferenceState, node.CheckMetadataAccess, node.CheckDeleteIntent} {
		if !errors.Is(check(), syscall.EOPNOTSUPP) {
			t.Fatal("reference capability check invented backend support")
		}
	}
	if _, err := file.State(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing state support returned fabricated facts: %v", err)
	}
	session.base.failure = errors.New("stream disconnected")
	if _, err := node.Scope(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unhealthy reference ordinary operation: %v", err)
	}
}
