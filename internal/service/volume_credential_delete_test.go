package service

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
)

func TestVolumeDeletionPreservesCredentialWithZeroVolumeAuthority(t *testing.T) {
	f := newAuthFixture(t, nil)
	selected := routeVolume(t, f, f.admin, "Selected")
	other := routeVolume(t, f, f.admin, "Other")
	credential, bearer := routeCredential(t, f, f.admin, []string{ScopeVolumesRead, ScopeVolumesWrite}, false, []management.ID{selected.ID})
	_, secret, ok := strings.Cut(bearer, ".")
	if !ok {
		t.Fatal("credential fixture did not supply a bearer secret")
	}
	mux := http.NewServeMux()
	mux.Handle(serviceapi.Prefix+"/auth/", f.auth.Handler())
	mux.Handle(serviceapi.Prefix+"/", routeHandler(t, f, exactRouteSummary, nil))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client, err := serviceapi.NewClient(server.URL, server.Client(), serviceapi.Credential{ID: string(credential.ID), Secret: secret})
	if err != nil {
		t.Fatal(err)
	}
	before, err := client.Credential(t.Context(), string(credential.ID))
	if err != nil || before.AllOwnedVolumes || len(before.VolumeIDs) != 1 || before.VolumeIDs[0] != string(selected.ID) {
		t.Fatalf("initial credential selection: %+v %v", before, err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, server.URL+serviceapi.Prefix+"/volumes/"+string(selected.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.HTTPClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusNoContent || len(body) != 0 {
		t.Fatalf("selected volume DELETE: status=%d body=%s read=%v close=%v", response.StatusCode, body, readErr, closeErr)
	}
	status, err := client.Credential(t.Context(), string(credential.ID))
	if err != nil || status.ID != string(credential.ID) || status.AllOwnedVolumes || status.VolumeIDs == nil || len(status.VolumeIDs) != 0 {
		t.Fatalf("public zero-volume credential status: %+v %v", status, err)
	}
	volumes, err := client.ListVolumes(t.Context())
	if err != nil || len(volumes) != 0 {
		t.Fatalf("zero-volume credential list: %+v %v", volumes, err)
	}
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{"read other", func() error { _, err := client.GetVolume(t.Context(), string(other.ID)); return err }},
		{"create volume", func() error {
			_, err := client.CreateVolume(t.Context(), serviceapi.CreateVolumeRequest{Name: "Forbidden", QuotaBytes: "4096"})
			return err
		}},
	} {
		err := operation.run()
		var denied *serviceapi.APIError
		if !errors.As(err, &denied) || denied.Status != http.StatusForbidden {
			t.Fatalf("zero-volume credential %s: %v", operation.name, err)
		}
	}
	if err := client.RevokeCredential(t.Context(), string(credential.ID)); err != nil {
		t.Fatalf("zero-volume credential could not revoke itself: %v", err)
	}
	_, err = client.Credential(t.Context(), string(credential.ID))
	var revoked *serviceapi.APIError
	if !errors.As(err, &revoked) || revoked.Status != http.StatusUnauthorized {
		t.Fatalf("revoked credential remains authenticated: %v", err)
	}
}
