package httprest

import (
	"context"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type scalarStateFlagsReference struct {
	retainedReference
	storage.ReferenceStateAccess
	storage.ReferenceIdentity
}

func (r *scalarStateFlagsReference) State(ctx context.Context, projection storage.StateProjection) (storage.ReferenceState, error) {
	state, err := r.ReferenceStateAccess.State(ctx, projection)
	if err == nil && projection == storage.StateScalars {
		state.Detached = true
		state.PendingUnlink = true
		state.PendingGeneration = []byte{1}
	}
	return state, err
}

func TestHTTPScalarStateRoundTripPreservesIndependentDeletionFacts(t *testing.T) {
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
	session := httpStateSession(t, client)
	file := httpStateFile(t, session, root.ID, attr.ID, 0).(*remoteFile)
	handler.files.mu.Lock()
	served := handler.files.sessions[session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	entry := served.files[file.id]
	original := entry.native
	entry.native = &scalarStateFlagsReference{
		retainedReference:    original,
		ReferenceStateAccess: original.(storage.ReferenceStateAccess),
		ReferenceIdentity:    original.(storage.ReferenceIdentity),
	}
	served.mu.Unlock()
	defer func() {
		served.mu.Lock()
		entry.native = original
		served.mu.Unlock()
	}()

	state, err := file.State(t.Context(), storage.StateScalars)
	if err != nil || state.Attr.ID != attr.ID || !state.Detached || !state.PendingUnlink {
		t.Fatalf("independent deletion facts=%+v error=%v", state, err)
	}
	if err := state.CheckProjection(attr.ID, storage.StateScalars); err != nil {
		t.Fatal(err)
	}
	full := referenceStateOf(storage.ReferenceState{Attr: attr, Detached: true, PendingUnlink: true, PendingGeneration: []byte{1}})
	if err := validateReferenceState(full); err == nil {
		t.Fatal("full mutation receipt accepted detached pending unlink")
	}
}
