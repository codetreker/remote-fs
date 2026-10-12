package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
)

func TestServiceRequiresAdministratorAndReleasesRejectedOpen(t *testing.T) {
	config := rootTestConfig(t)
	if err := InitState(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	if owner, err := Open(t.Context(), config); owner != nil || err == nil {
		t.Fatal("uninitialized administrator served requests")
	}
	maintenance, err := OpenMaintenance(t.Context(), config)
	if err != nil {
		t.Fatalf("rejected startup retained root: %v", err)
	}
	if err := maintenance.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

}

func TestServiceWorkerCompletesVolumeAndQuotaActions(t *testing.T) {
	f := newWholeServiceAuthFixture(t)
	response := f.request("POST", serviceapi.Prefix+"/action-namespaces", f.adminCredential, f.adminSecret, struct{}{})
	var namespace serviceapi.ActionNamespace
	decodeServiceResponse(t, response, http.StatusCreated, &namespace)
	create := serviceapi.CreateVolumeRequest{Action: serviceapi.Action{Namespace: namespace.Namespace, Nonce: "worker-create"}, Name: "Worker lifecycle", QuotaBytes: "8192"}
	response = f.request("POST", serviceapi.Prefix+"/volumes", f.adminCredential, f.adminSecret, create)
	var operation serviceapi.Operation
	decodeServiceResponse(t, response, http.StatusAccepted, &operation)
	created := waitServiceOperation(t, f, operation.ID)
	if created.State != "succeeded" || !created.Settled {
		t.Fatalf("creation=%+v", created)
	}
	response = f.request("GET", serviceapi.Prefix+"/volumes/"+created.VolumeID, f.adminCredential, f.adminSecret, nil)
	var volume serviceapi.Volume
	decodeServiceResponse(t, response, http.StatusOK, &volume)
	if volume.Summary == nil || volume.Summary.FileCount != "0" || volume.Summary.DirectoryCount != "0" || volume.Summary.QuotaBytes != "8192" {
		t.Fatalf("initial summary=%+v", volume.Summary)
	}
	quota := serviceapi.SetQuotaRequest{Action: serviceapi.Action{Namespace: namespace.Namespace, Nonce: "worker-zero-quota"}, ExpectedRevision: volume.Summary.QuotaRevision, QuotaBytes: "0"}
	response = f.request("PUT", serviceapi.Prefix+"/volumes/"+created.VolumeID+"/quota", f.adminCredential, f.adminSecret, quota)
	decodeServiceResponse(t, response, http.StatusAccepted, &operation)
	updated := waitServiceOperation(t, f, operation.ID)
	if updated.State != "succeeded" || !updated.Settled {
		t.Fatalf("quota=%+v", updated)
	}
	response = f.request("GET", serviceapi.Prefix+"/volumes/"+created.VolumeID, f.adminCredential, f.adminSecret, nil)
	decodeServiceResponse(t, response, http.StatusOK, &volume)
	if volume.Summary == nil || volume.Summary.QuotaBytes != "0" || volume.Summary.QuotaRevision != "2" {
		t.Fatalf("quota summary=%+v", volume.Summary)
	}
	response = f.request("GET", "/health/ready", management.Credential{}, "", nil)
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("ready=%d", response.StatusCode)
	}
	response.Body.Close()
}

func decodeServiceResponse(t *testing.T, response *http.Response, status int, value any) {
	t.Helper()
	defer response.Body.Close()
	if response.StatusCode != status {
		var failure any
		_ = json.NewDecoder(response.Body).Decode(&failure)
		t.Fatalf("status=%d want=%d body=%v", response.StatusCode, status, failure)
	}
	if err := json.NewDecoder(response.Body).Decode(value); err != nil {
		t.Fatal(err)
	}
}

func waitServiceOperation(t *testing.T, f *wholeServiceAuthFixture, id string) serviceapi.Operation {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		response := f.request("GET", serviceapi.Prefix+"/operations/"+id, f.adminCredential, f.adminSecret, nil)
		var operation serviceapi.Operation
		decodeServiceResponse(t, response, http.StatusOK, &operation)
		if operation.State == "succeeded" || operation.State == "failed" {
			return operation
		}
		select {
		case <-ctx.Done():
			t.Fatalf("operation remained %s/%s: %v", operation.State, operation.Phase, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestServiceReadyReportsSupervisorFailure(t *testing.T) {
	service := &Service{workerError: errors.New("durable worker failed")}
	response := httptest.NewRecorder()
	service.ready(response, httptest.NewRequest("GET", "/health/ready", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatal("worker failure reported ready")
	}
	service.workerError = nil
	service.namespaceError = errors.New("namespace worker failed")
	response = httptest.NewRecorder()
	service.ready(response, httptest.NewRequest("GET", "/health/ready", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatal("namespace failure reported ready")
	}
}
