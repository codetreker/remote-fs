package service

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"golang.org/x/sys/unix"
)

func copyMaintenanceFixture(t *testing.T, source, destination string) {
	t.Helper()
	if err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			if err := os.Mkdir(target, 0700); err != nil {
				return err
			}
			return copyMaintenanceAttributes(path, target)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(data)
		return errors.Join(writeErr, copyMaintenanceAttributes(path, target), file.Sync(), file.Close())
	}); err != nil {
		t.Fatal(err)
	}
}

func copyMaintenanceAttributes(source, target string) error {
	size, err := unix.Listxattr(source, nil)
	if err != nil {
		return err
	}
	names := make([]byte, size)
	if _, err := unix.Listxattr(source, names); err != nil {
		return err
	}
	for _, name := range strings.Split(string(names), "\x00") {
		if name == "" {
			continue
		}
		size, err := unix.Getxattr(source, name, nil)
		if err != nil {
			return err
		}
		value := make([]byte, size)
		if _, err := unix.Getxattr(source, name, value); err != nil {
			return err
		}
		if err := unix.Setxattr(target, name, value, unix.XATTR_CREATE); err != nil {
			return err
		}
	}
	return nil
}

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
	if err := m.Validate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := m.Quiesce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AddAdmin(t.Context(), "third", "Third", "third-admin-password"); err == nil {
		t.Fatal("administrator mutation after quiescence succeeded")
	}
	if _, err := OpenMaintenance(t.Context(), c); !errors.Is(err, ErrRootBusy) {
		t.Fatalf("quiescence released stable anchor: %v", err)
	}
	if err := m.Validate(t.Context()); err != nil {
		t.Fatalf("reopen quiescent catalog: %v", err)
	}
}

func TestMaintenanceDetectsUncoordinatedDataReplacement(t *testing.T) {
	c := rootTestConfig(t)
	if err := InitState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	m, err := OpenMaintenance(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(t.Context())
	old := filepath.Join(c.StateRoot, "previous")
	if err := os.Rename(m.DataRoot(), old); err != nil {
		t.Fatal(err)
	}
	copyMaintenanceFixture(t, old, m.DataRoot())
	if err := m.Validate(t.Context()); !errors.Is(err, management.ErrCorrupt) {
		t.Fatalf("uncoordinated replacement: %v", err)
	}
	if err := os.RemoveAll(m.DataRoot()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(old, m.DataRoot()); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceNativeEvidenceRequired(t *testing.T) {
	f := newWholeServiceAuthFixture(t)
	f.close()
	m, err := OpenMaintenance(t.Context(), f.config)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(t.Context())
	if err := m.Validate(t.Context()); err != nil {
		t.Fatalf("complete native roots: %v", err)
	}
	root := filepath.Join(m.root.VolumesPath, f.adminVolume.StorageName)
	witnessPath := filepath.Join(root, "METASTORE")
	witness, err := os.ReadFile(witnessPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(witnessPath); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(t.Context()); err == nil {
		t.Fatal("missing native witness was accepted")
	}
	if err := os.WriteFile(witnessPath, witness, 0600); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(t.Context()); err != nil {
		t.Fatalf("restored witness: %v", err)
	}
	const attribute = "user.remote-fs.lease-state"
	size, err := unix.Getxattr(root, attribute, nil)
	if err != nil {
		t.Fatal(err)
	}
	lease := make([]byte, size)
	if _, err := unix.Getxattr(root, attribute, lease); err != nil {
		t.Fatal(err)
	}
	if err := unix.Removexattr(root, attribute); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(t.Context()); err == nil {
		t.Fatal("missing native lease xattr was accepted")
	}
	if err := unix.Setxattr(root, attribute, lease, 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(t.Context()); err != nil {
		t.Fatalf("restored native lease xattr: %v", err)
	}
	namespace, err := m.IssueNamespace(t.Context(), f.admin.ID)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := m.store.CreateVolume(t.Context(), f.admin.ID,
		management.ActionKey{Namespace: namespace.Sequence, Nonce: "uninitialized-create"}, "Pending creation", limited.MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(t.Context()); err != nil {
		t.Fatalf("accepted creation remains verifiable without initializing a native volume: %v", err)
	}
	if _, err := os.Stat(filepath.Join(m.root.VolumesPath, string(accepted.VolumeID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("offline validation initialized accepted creation: %v", err)
	}
	if err := m.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceRetainsNativeOwnerUntilCloseSettles(t *testing.T) {
	f := newWholeServiceAuthFixture(t)
	f.close()
	m, err := OpenMaintenance(t.Context(), f.config)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(t.Context())
	catalog := m.store
	root := filepath.Join(m.root.VolumesPath, f.adminVolume.StorageName)
	config, err := volumeConfig(m.config, root, f.adminVolume, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	m.native, err = localstore.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	retained := m.native
	snapshot, _, err := m.native.Log().Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err := m.Validate(t.Context()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("native settlement failure: %v", err)
	}
	if m.native != retained || m.store != catalog {
		t.Fatal("validation lost the native owner or closed the surrounding catalog")
	}
	if err := m.Close(t.Context()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("native close did not preserve the cause: %v", err)
	}
	if m.native != retained || m.store != catalog || m.closed {
		t.Fatal("unsettled native close released surrounding ownership")
	}
	cause := errors.New("offline operation failed")
	failed := finishMaintenance(t.Context(), m, cause)
	var ownership *MaintenanceFailure
	if !errors.As(failed, &ownership) || ownership.Maintenance != m || !errors.Is(failed, cause) || !errors.Is(failed, syscall.EBUSY) {
		t.Fatalf("lost exact cleanup owner: %v", failed)
	}
	if !strings.Contains(ownership.Error(), cause.Error()) {
		t.Fatalf("retained ownership diagnostic hides the cause: %s", ownership.Error())
	}
	if _, err := OpenMaintenance(t.Context(), f.config); !errors.Is(err, ErrRootBusy) {
		t.Fatalf("unsettled native owner released root: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ownership.Close(t.Context()); err != nil {
		t.Fatalf("retry the retained native owner: %v", err)
	}
	if m.native != nil || m.store != nil || !m.closed {
		t.Fatal("successful retry did not finish surrounding ownership")
	}
	reopened, err := OpenMaintenance(t.Context(), f.config)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
