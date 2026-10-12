package service

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/internal/management"
)

func TestMaintenanceInitializationAndOfflineAdmin(t *testing.T) {
	c := rootTestConfig(t)
	if err := InitState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if err := InitState(t.Context(), c); err == nil {
		t.Fatal("initialized root was recreated")
	}
	user, err := InitializeAdmin(t.Context(), c, "first", "First", "correct-admin-password")
	if err != nil {
		t.Fatal(err)
	}
	m, err := OpenMaintenance(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(t.Context())
	if m.StateRoot() != c.StateRoot {
		t.Fatalf("maintenance uses a different stable root: %s", m.StateRoot())
	}
	if _, err := OpenMaintenance(t.Context(), c); !errors.Is(err, ErrRootBusy) {
		t.Fatalf("concurrent offline maintenance: %v", err)
	}
	second, err := m.AddAdmin(t.Context(), "second", "Second", "second-admin-password")
	if err != nil || second.ID == user.ID {
		t.Fatalf("second administrator: %+v %v", second, err)
	}
	reset, err := m.ResetAdminPassword(t.Context(), "first", "replacement-password")
	if err != nil || reset.AuthEpoch != user.AuthEpoch+1 {
		t.Fatalf("reset administrator: %+v %v", reset, err)
	}
	if _, err := m.ResetAdminPassword(t.Context(), "missing", "replacement-password"); !errors.Is(err, management.ErrNotFound) {
		t.Fatalf("missing administrator reset: %v", err)
	}
	if err := m.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AddAdmin(t.Context(), "third", "Third", "third-admin-password"); err == nil {
		t.Fatal("closed maintenance accepted an administrator mutation")
	}
	if _, err := m.ResetAdminPassword(t.Context(), "first", "replacement-password"); err == nil {
		t.Fatal("closed maintenance accepted a password reset")
	}
	reopened, err := OpenMaintenance(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(t.Context())
	credential, err := reopened.store.AdminCredential(t.Context(), "first")
	if err != nil || credential.User.AuthEpoch != reset.AuthEpoch {
		t.Fatalf("password reset did not persist across reopen: %+v %v", credential, err)
	}
}

func TestMaintenanceFailurePreservesTerminalCloseError(t *testing.T) {
	c := rootTestConfig(t)
	if err := InitState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	m, err := OpenMaintenance(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.root.directories[len(m.root.directories)-1].Close(); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("offline operation failed")
	failed := finishMaintenance(t.Context(), m, cause)
	var terminal *MaintenanceFailure
	if !errors.As(failed, &terminal) || terminal.Maintenance != m || !errors.Is(failed, cause) || !errors.Is(failed, os.ErrClosed) {
		t.Fatalf("terminal close lost its owner or error chain: %v", failed)
	}
	if !strings.Contains(terminal.Error(), cause.Error()) || !errors.Is(terminal.Unwrap(), os.ErrClosed) {
		t.Fatalf("terminal diagnostic lost the operation or close failure: %s", terminal.Error())
	}
	if err := terminal.Close(t.Context()); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("same terminal owner reported successful cleanup: %v", err)
	}
	if m.store != nil || !m.root.closed {
		t.Fatal("terminal root close did not finish the catalog and file handles")
	}
	reopened, err := OpenMaintenance(t.Context(), c)
	if err != nil {
		t.Fatalf("terminal file close retained the released singleton lock: %v", err)
	}
	if err := reopened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
