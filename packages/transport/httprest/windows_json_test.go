package httprest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestWindowsJSONRejectsMissingDuplicateAndWrongTypedOperands(t *testing.T) {
	id := windowsTestID(t)
	request := windowsRequest{Op: storage.OpWindowsRead, Session: strings.Repeat("a", 64), File: strings.Repeat("b", 64), Action: "", Offset: 0, Length: 1, Data: []byte{}, Ranges: []storage.WindowsLockRange{}}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	source := string(encoded)
	for name, body := range map[string]string{
		"missing offset":   strings.Replace(source, `"offset":0,`, "", 1),
		"duplicate offset": strings.Replace(source, `"offset":0`, `"offset":0,"offset":0`, 1),
		"null bytes":       strings.Replace(source, `"data":""`, `"data":null`, 1),
		"wrong number":     strings.Replace(source, `"length":1`, `"length":"1"`, 1),
		"unknown member":   strings.Replace(source, `"offset":0`, `"offset":0,"unknown":false`, 1),
		"wrong case":       strings.Replace(source, `"offset":0`, `"Offset":0`, 1),
		"trailing":         source + `{}`,
		"invalid utf8":     strings.Replace(source, `"data":""`, "\"data\":\"\xff\"", 1),
	} {
		t.Run(name, func(t *testing.T) {
			var got windowsRequest
			if err := decodeWindowsJSON([]byte(body), &got); err == nil {
				t.Fatal("malformed Windows request accepted")
			}
		})
	}
	var decoded windowsRequest
	if err := decodeWindowsJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateWindowsRequest(decoded, storage.DefaultFileSessionOptions()); err != nil {
		t.Fatal(err)
	}
	decoded.Action = id
	if err := validateWindowsRequest(decoded, storage.DefaultFileSessionOptions()); err == nil {
		t.Fatal("read accepted an unrelated action identity")
	}
}

func TestWindowsJSONRetainsOrderedLockBatchesBeyondAdvisoryProofLimit(t *testing.T) {
	ranges := make([]storage.WindowsLockRange, 32)
	for i := range ranges {
		ranges[i] = storage.WindowsLockRange{Offset: uint64(i), Length: 1, Type: storage.Exclusive}
	}
	request := windowsRequest{Op: storage.OpWindowsLockBatch, Session: strings.Repeat("a", 64), File: strings.Repeat("b", 64), Action: windowsTestID(t), Data: []byte{}, Ranges: ranges}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded windowsRequest
	if err := decodeWindowsJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateWindowsRequest(decoded, storage.DefaultFileSessionOptions()); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Ranges) != len(ranges) {
		t.Fatalf("lock batch truncated: %d", len(decoded.Ranges))
	}
	for i, r := range decoded.Ranges {
		if r != ranges[i] {
			t.Fatalf("lock order or element changed at %d: %+v", i, r)
		}
	}
	if windowsControl(request.Op) {
		t.Fatal("large Windows lock batches consume the small control-body quota")
	}
}

func TestWindowsJSONRejectsUnpairedSurrogatesWithoutAliasingReplacementNames(t *testing.T) {
	for name, encoded := range map[string]string{
		"high":                           `{"name":"\ud800"}`,
		"low":                            `{"name":"\udfff"}`,
		"high followed by ascii":         `{"name":"\ud800x"}`,
		"high followed by high":          `{"name":"\ud800\ud800"}`,
		"high followed by scalar escape": `{"name":"\ud800\u0061"}`,
		"reversed":                       `{"name":"\udc00\ud800"}`,
	} {
		t.Run(name, func(t *testing.T) {
			var value struct {
				Name string `json:"name"`
			}
			if err := decodeWindowsJSON([]byte(encoded), &value); err == nil {
				t.Fatalf("invalid Unicode aliased name %q", value.Name)
			}
		})
	}
	for encoded, want := range map[string]string{
		`{"name":"\ud83d\ude00"}`: "😀",
		`{"name":"�"}`:            "�",
		`{"name":"\ufffd"}`:       "�",
		`{"name":"\\ud800"}`:      `\ud800`,
	} {
		var value struct {
			Name string `json:"name"`
		}
		if err := decodeWindowsJSON([]byte(encoded), &value); err != nil || value.Name != want {
			t.Fatalf("valid Unicode %s = %q, %v", encoded, value.Name, err)
		}
	}
}
