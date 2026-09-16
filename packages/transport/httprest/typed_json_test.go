package httprest

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestFileJSONOpaqueMaps(t *testing.T) {
	type payload struct {
		Version []byte `json:"version"`
		Data    []byte `json:"data"`
	}
	type envelope struct {
		Metadata map[string]payload `json:"metadata"`
	}
	for _, body := range []string{
		`{"metadata":{"client.v1":{"version":"AQ==","data":"AP8="}}}`,
		`{"metadata":{}}`,
	} {
		var decoded envelope
		if err := decodeFileJSON([]byte(body), &decoded); err != nil {
			t.Fatalf("valid metadata %s: %v", body, err)
		}
	}
	for _, body := range []string{
		`{"metadata":null}`,
		`{"metadata":{"client.v1":null}}`,
		`{"metadata":{"client.v1":{"version":"AQ==","data":null}}}`,
		`{"metadata":{"client.v1":{"version":"AQ==","data":""},"client.v1":{"version":"Ag==","data":""}}}`,
		`{"metadata":{"client.v1":{"version":"AQ==","version":"Ag==","data":""}}}`,
		`{"metadata":{"client.v1":{"version":"AQ==","data":"","extra":1}}}`,
		`{"metadata":{"client.v1":{"version":"AQ=="}}}`,
	} {
		var decoded envelope
		if err := decodeFileJSON([]byte(body), &decoded); err == nil {
			t.Fatalf("accepted malformed metadata %s", body)
		}
	}
}

func TestFileJSONDirectoryListsDoNotInheritLockBatchLimit(t *testing.T) {
	type envelope struct {
		IDs []uint64 `json:"ids"`
	}
	body := `{"ids":[` + strings.Repeat("1,", 16) + `1]}`
	var decoded envelope
	if err := decodeFileJSON([]byte(body), &decoded); err != nil || len(decoded.IDs) != 17 {
		t.Fatalf("file list: %v, %v", decoded.IDs, err)
	}
	decoder := json.NewDecoder(strings.NewReader(body))
	if err := checkLockJSON(decoder, reflect.TypeOf(envelope{})); err == nil {
		t.Fatal("lock control accepted an oversized batch")
	}
}
