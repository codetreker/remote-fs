package serviceapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNegotiatedSessionLimitsPreserveExactPolicy(t *testing.T) {
	options := storage.DefaultFileSessionOptions()
	options.History = time.Second
	options.MaxFiles = 16
	options.MaxOperations = 4
	options.MaxCloseActions = 33
	options.MaxWaiters = 0
	options.MaxFileSize = 9007199254740993
	limits := FileSessionLimitsFromOptions(options)
	result, err := limits.NativeOptions()
	if err != nil || result != options {
		t.Fatalf("options changed: %+v %v", result, err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != Prefix+"/capabilities" {
			t.Errorf("unexpected route %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(Capabilities{FileSession: limits, MaxTransferBytes: "9007199254740993"})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client(), Credential{})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := client.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	actual, err := capabilities.FileSession.NativeOptions()
	if err != nil || actual != options {
		t.Fatalf("HTTP policy changed: %+v %v", actual, err)
	}
	for _, mutate := range []func(*FileSessionLimits){
		func(l *FileSessionLimits) { l.LeaseNanoseconds = "0" },
		func(l *FileSessionLimits) { l.HistoryNanoseconds = "18446744073709551615" },
		func(l *FileSessionLimits) { l.MaxFiles = "17" },
		func(l *FileSessionLimits) { l.MaxWaiters = "-1" },
	} {
		invalid := limits
		mutate(&invalid)
		if _, err := invalid.NativeOptions(); err == nil {
			t.Fatalf("invalid policy accepted %+v", invalid)
		}
	}
}
