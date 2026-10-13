package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestServiceActiveVolumeMissingStorageKeepsCatalogAndReportsUnavailable(t *testing.T) {
	config := rootTestConfig(t)
	if err := InitState(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	admin, err := InitializeAdmin(t.Context(), config, "root", "Administrator", testAuthPassword)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	f := &authFixture{t: t, store: s.maintenance.store, auth: s.auth, admin: admin, database: s.maintenance.root.ManagementPath}
	_, bearer := routeCredential(t, f, admin, []string{ScopeVolumesRead, ScopeFiles}, true, nil)
	volume, err := f.store.CreateVolume(t.Context(), admin.ID, "Existing volume", 4096)
	if err != nil {
		t.Fatal(err)
	}
	volume, err = s.processVolume(t.Context(), volume)
	if err != nil {
		t.Fatal(err)
	}
	root := s.runtimes.volumeRoot(volume.ID)
	if err := s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	s, err = Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	response := routeRequest(s, http.MethodGet, "/volumes/"+string(volume.ID), bearer, "")
	if response.Code != http.StatusOK {
		t.Fatalf("volume catalog response: %d %s", response.Code, response.Body.String())
	}
	var result serviceapi.Volume
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != string(management.VolumeActive) || result.Summary != nil || result.SummaryError == nil || result.SummaryError.Code != "metadata_unavailable" {
		t.Fatalf("missing storage reported a successful summary: %+v", result)
	}
	current, err := s.maintenance.store.GetVolume(t.Context(), admin.ID, volume.ID)
	if err != nil || current.State != management.VolumeActive || current.ManagementRevision != volume.ManagementRevision {
		t.Fatalf("storage failure changed catalog: %+v %v", current, err)
	}
	response = routeRequest(s, http.MethodGet, "/volumes/"+string(volume.ID)+"/files", bearer, "")
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "storage_unavailable") {
		t.Fatalf("file listing reported storage as missing path: %d %s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(filepath.Join(root, "metastore.sqlite")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active metadata database recreated: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("active volume storage initialized: %v %v", entries, err)
	}
}

func TestServiceReadOnlyFilesOwnerIsolationAndVolumeCRUD(t *testing.T) {
	config := rootTestConfig(t)
	if err := InitState(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	admin, err := InitializeAdmin(t.Context(), config, "root", "Administrator", testAuthPassword)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	f := &authFixture{t: t, store: s.maintenance.store, auth: s.auth, admin: admin, database: s.maintenance.root.ManagementPath}
	ordinary := routeUser(t, f)
	_, adminBearer := routeCredential(t, f, admin, []string{ScopeVolumesRead, ScopeVolumesWrite, ScopeFiles, ScopeAdminSummaries}, true, nil)
	_, userBearer := routeCredential(t, f, ordinary, []string{ScopeVolumesRead, ScopeFiles}, true, nil)
	response := routeRequest(s, "POST", "/volumes", adminBearer, `{"name":"Owner volume","quotaBytes":"4096"}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("create=%d %s", response.Code, response.Body.String())
	}
	var created serviceapi.Volume
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	private, err := f.store.CreateVolume(t.Context(), ordinary.ID, "Private volume", 4096)
	if err != nil {
		t.Fatal(err)
	}
	private, err = s.processVolume(t.Context(), private)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.runtimes.acquire(t.Context(), private)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Store.Mkdir(t.Context(), "folder"); err != nil {
		t.Fatal(err)
	}
	if err := lease.Store.Write(t.Context(), "folder/private.txt", []byte("private")); err != nil {
		t.Fatal(err)
	}
	root, err := lease.Store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := lease.Store.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(t.Context())
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	link, err := session.(storage.NodeReferences).OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{
		Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("link"),
	}}, storage.NodeRefOptions{Kind: storage.NodeSymlink, Target: storage.ChildCondition{State: storage.Absent}, Action: action,
		Create: true, Exclusive: true, MetadataAccess: storage.ReadMetadata,
		InitialState: storage.InitialState{OnCreate: storage.InitialFields{LinkTarget: []byte("folder/private.txt")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	response = routeRequest(s, "GET", "/volumes/"+string(private.ID)+"/files", adminBearer, "")
	if response.Code != http.StatusForbidden {
		t.Fatalf("adminotherfiles=%d", response.Code)
	}
	response = routeRequest(s, "GET", "/volumes/"+created.ID+"/files", userBearer, "")
	if response.Code != http.StatusForbidden {
		t.Fatalf("ordinaryotherfiles=%d", response.Code)
	}
	response = routeRequest(s, "GET", "/volumes/"+string(private.ID)+"/files", userBearer, "")
	if response.Code != http.StatusOK {
		t.Fatalf("root metadata=%d %s", response.Code, response.Body.String())
	}
	var rootDirectory serviceapi.FileDirectory
	if err := json.Unmarshal(response.Body.Bytes(), &rootDirectory); err != nil {
		t.Fatal(err)
	}
	kinds := make(map[string]string)
	for _, entry := range rootDirectory.Entries {
		kinds[entry.Name] = entry.Kind
	}
	if kinds["folder"] != "directory" || kinds["link"] != "symlink" {
		t.Fatalf("root metadata kinds=%v", kinds)
	}
	if strings.Contains(response.Body.String(), "pathBase64\":\"Zm9sZGVy") {
		t.Fatal("directory repeated a child full path")
	}
	encoded := base64.StdEncoding.EncodeToString([]byte("folder"))
	response = routeRequest(s, "GET", "/volumes/"+string(private.ID)+"/files?path="+encoded, userBearer, "")
	if response.Code != http.StatusOK {
		t.Fatalf("list=%d %s", response.Code, response.Body.String())
	}
	var directory serviceapi.FileDirectory
	if err := json.Unmarshal(response.Body.Bytes(), &directory); err != nil {
		t.Fatal(err)
	}
	if len(directory.Entries) != 1 || directory.Entries[0].Name != "private.txt" || directory.Entries[0].SizeBytes != "7" {
		t.Fatalf("metadata=%+v", directory)
	}
	response = routeRequest(s, "GET", "/admin/users/"+string(ordinary.ID)+"/volumes", adminBearer, "")
	if response.Code != http.StatusOK {
		t.Fatalf("summary=%d %s", response.Code, response.Body.String())
	}
	var summaries serviceapi.UserVolumeList
	if err := json.Unmarshal(response.Body.Bytes(), &summaries); err != nil {
		t.Fatal(err)
	}
	if len(summaries.Volumes) != 1 || summaries.Volumes[0].Summary == nil || summaries.Volumes[0].Summary.FileCount != "2" || summaries.Volumes[0].Summary.DirectoryCount != "1" || summaries.Volumes[0].Summary.UsedBytes != "4096" {
		t.Fatalf("summary=%+v", summaries)
	}
	response = routeRequest(s, "PATCH", "/volumes/"+created.ID, adminBearer, `{"name":"Renamed"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("rename=%d %s", response.Code, response.Body.String())
	}
	response = routeRequest(s, "DELETE", "/volumes/"+created.ID, adminBearer, "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete=%d %s", response.Code, response.Body.String())
	}
	response = routeRequest(s, "POST", "/volumes/"+string(private.ID)+"/files", userBearer, `{}`)
	if response.Code != http.StatusNotFound && response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("filemutation=%d", response.Code)
	}
	for _, endpoint := range []string{"/health/live", "/health/ready"} {
		response := httptest.NewRecorder()
		s.ServeHTTP(response, httptest.NewRequest(http.MethodGet, endpoint, nil))
		if response.Code != http.StatusNoContent {
			t.Fatalf("%s=%d", endpoint, response.Code)
		}
	}
}

func TestServiceRequiresAdministratorAndReleasesRejectedOpen(t *testing.T) {
	config := rootTestConfig(t)
	if err := InitState(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	if service, err := Open(t.Context(), config); service != nil || err == nil {
		t.Fatal("service accepted missing administrator")
	}
	maintenance, err := OpenMaintenance(t.Context(), config)
	if err != nil {
		t.Fatalf("rejected startup retained lock: %v", err)
	}
	if err := maintenance.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestServiceFailedConstructionRetainsNativeCleanupOwner(t *testing.T) {
	config := rootTestConfig(t)
	if err := InitState(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	admin, err := InitializeAdmin(t.Context(), config, "root", "Administrator", testAuthPassword)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	volume, err := s.maintenance.store.CreateVolume(t.Context(), admin.ID, "Pending startup", 4096)
	if err != nil {
		t.Fatal(err)
	}
	volume, err = s.processVolume(t.Context(), volume)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.runtimes.acquire(t.Context(), volume)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	snapshot, _, err := lease.Store.Log().Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("service construction failed")
	failed := s.openFailed(cause)
	var ownership *OpenFailure
	if !errors.As(failed, &ownership) || !errors.Is(failed, cause) || !errors.Is(failed, syscall.EBUSY) {
		t.Fatalf("failed startup lost native cleanup cause: %v", failed)
	}
	if !strings.Contains(ownership.Error(), cause.Error()) {
		t.Fatalf("startup diagnostic hides cause: %s", ownership.Error())
	}
	if _, err := OpenMaintenance(t.Context(), config); !errors.Is(err, ErrRootBusy) {
		t.Fatalf("startup released root with open snapshot: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ownership.Close(t.Context()); err != nil {
		t.Fatalf("retry startup cleanup: %v", err)
	}
	reopened, err := OpenMaintenance(t.Context(), config)
	if err != nil {
		t.Fatalf("settled startup retained root: %v", err)
	}
	if err := reopened.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	stopped := httptest.NewRecorder()
	s.ready(stopped, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if stopped.Code != http.StatusServiceUnavailable {
		t.Fatalf("stopped ready=%d", stopped.Code)
	}
}
