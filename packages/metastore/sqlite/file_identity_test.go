package sqlite

import (
	"errors"
	"strconv"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestBackendIdentityPreservesVolumeAcrossAuthorityReopen(t *testing.T) {
	config := lockingTestConfig(t)
	first, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	for _, check := range []func() error{first.CheckBackendIdentity, first.CheckStableReferenceIdentity, first.CheckOpenMetadataAccess, first.CheckInlineCloseSettlement} {
		if err := check(); err != nil {
			t.Fatal(err)
		}
	}
	before, err := first.BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	state, err := first.DurableState(t.Context())
	if err != nil || before.Volume != storage.VolumeID(state.DatabaseID+"/"+strconv.FormatInt(first.root, 10)) || before.RootNodeID != uint64(first.root) {
		t.Fatalf("durable volume identity=%+v, state=%+v, %v", before, state, err)
	}
	if repeated, err := first.BackendIdentity(t.Context()); err != nil || repeated != before {
		t.Fatalf("identity changed within native authority: %+v, %v", repeated, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.BackendIdentity(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired native authority returned identity: %v", err)
	}
	config.Initialize = false
	second, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	after, err := second.BackendIdentity(t.Context())
	if err != nil || after.Volume != before.Volume || after.RootNodeID != before.RootNodeID || after.Authority == before.Authority {
		t.Fatalf("reopen identity=%+v, prior=%+v, %v", after, before, err)
	}
}

func TestBackendIdentityRefusesUnverifiedRootAndDurableID(t *testing.T) {
	store, file := openPublicationFile(t)
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	before, err := store.BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE volumes SET root=? WHERE id=?`, store.root+1, store.volume); err != nil {
		t.Fatal(err)
	}
	if got, err := store.BackendIdentity(t.Context()); !errors.Is(err, syscall.EIO) || got != (storage.BackendIdentityResult{}) {
		t.Fatalf("unverified root returned identity=%+v, %v", got, err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE volumes SET root=? WHERE id=?`, store.root, store.volume); err != nil {
		t.Fatal(err)
	}
	state, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE database_state SET database_id='INVALID0000000000000000000000000'`); err != nil {
		t.Fatal(err)
	}
	if got, err := store.BackendIdentity(t.Context()); !errors.Is(err, syscall.EIO) || got != (storage.BackendIdentityResult{}) {
		t.Fatalf("unverified database returned identity=%+v, %v", got, err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE database_state SET database_id=?`, state.DatabaseID); err != nil {
		t.Fatal(err)
	}
	if got, err := store.BackendIdentity(t.Context()); err != nil || got != before {
		t.Fatalf("restored identity=%+v, %v", got, err)
	}
}
