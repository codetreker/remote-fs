package console

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerServesRealApplicationAndProtectsNamespaces(t *testing.T) {
	h := Handler()
	for _, route := range []string{"/", "/volumes/example"} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, route, nil))
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "/assets/") {
			t.Fatalf("%s: %d %s", route, response.Code, response.Body.String())
		}
		if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Content-Security-Policy") == "" {
			t.Fatalf("missing Console security headers: %v", response.Header())
		}
	}
	for _, route := range []string{"/api/service/v1/auth/session", "/assets/missing.js", "/build-manifest.json"} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, route, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s: %d", route, response.Code)
		}
	}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatal(response.Code)
	}
}

func TestBuiltAssetsAreEmbeddedWithImmutableCaching(t *testing.T) {
	manifest, err := fs.ReadFile(assets, "dist/build-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Version     int    `json:"version"`
		SourcesHash string `json:"sourcesHash"`
		AssetsHash  string `json:"assetsHash"`
	}
	if err := json.Unmarshal(manifest, &decoded); err != nil || decoded.Version != 1 || len(decoded.SourcesHash) != 64 || len(decoded.AssetsHash) != 64 {
		t.Fatalf("invalid build manifest: %s, %v", manifest, err)
	}
	entries, err := fs.ReadDir(assets, "dist/assets")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		response := httptest.NewRecorder()
		Handler().ServeHTTP(response, httptest.NewRequest(http.MethodHead, "/assets/"+entry.Name(), nil))
		if response.Code != http.StatusOK || response.Body.Len() != 0 || response.Header().Get("Content-Length") == "" || !strings.Contains(response.Header().Get("Cache-Control"), "immutable") {
			t.Fatalf("invalid asset response %s: %d %v", entry.Name(), response.Code, response.Header())
		}
	}
}
