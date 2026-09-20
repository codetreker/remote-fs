package httprest

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
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

func TestV4ResponsesRejectAmbiguousJSONAndNoncanonicalBytes(t *testing.T) {
	attr := AttrOf(storage.Attr{ID: 1, Kind: storage.NodeRegular, AccessTime: time.Unix(1, 0), ModTime: time.Unix(2, 0)})
	entry, _ := json.Marshal(Entry{Name: []byte("a"), Attr: attr})
	stat, _ := json.Marshal(StatResponse{Attr: attr})
	list, _ := json.Marshal(ListResponse{Entries: []Entry{{Name: []byte("a"), Attr: attr}}})
	space, _ := json.Marshal(SpaceOf(storage.Space{Total: 3, Used: 1, Avail: 2}))
	spaceResponse, _ := json.Marshal(SpaceResponse{Space: SpaceOf(storage.Space{Total: 3, Used: 1, Avail: 2})})
	barrier, _ := json.Marshal(MutationBarrier{Incarnation: "run", Position: 7})
	mutation, _ := json.Marshal(MutationResponse{Barrier: &MutationBarrier{Incarnation: "run", Position: 7}})
	for _, test := range []struct {
		name string
		body string
		into func() any
	}{
		{"entry duplicate", strings.Replace(string(entry), `"name":`, `"name":"YQ==","name":`, 1), func() any { return new(Entry) }},
		{"entry unknown", string(entry[:len(entry)-1]) + `,"extra":0}`, func() any { return new(Entry) }},
		{"entry noncanonical name", strings.Replace(string(entry), `"YQ=="`, `"YR=="`, 1), func() any { return new(Entry) }},
		{"stat duplicate", strings.Replace(string(stat), `"attr":`, `"attr":null,"attr":`, 1), func() any { return new(StatResponse) }},
		{"stat unknown", string(stat[:len(stat)-1]) + `,"extra":0}`, func() any { return new(StatResponse) }},
		{"list duplicate", strings.Replace(string(list), `"entries":`, `"entries":[],"entries":`, 1), func() any { return new(ListResponse) }},
		{"list unknown", string(list[:len(list)-1]) + `,"extra":0}`, func() any { return new(ListResponse) }},
		{"list noncanonical name", strings.Replace(string(list), `"YQ=="`, `"YR=="`, 1), func() any { return new(ListResponse) }},
		{"space duplicate", strings.Replace(string(space), `"total":`, `"total":3,"total":`, 1), func() any { return new(Space) }},
		{"space unknown", string(space[:len(space)-1]) + `,"extra":0}`, func() any { return new(Space) }},
		{"space response duplicate", strings.Replace(string(spaceResponse), `"space":`, `"space":null,"space":`, 1), func() any { return new(SpaceResponse) }},
		{"barrier duplicate position", strings.Replace(string(barrier), `"position":`, `"position":0,"position":`, 1), func() any { return new(MutationBarrier) }},
		{"barrier unknown", string(barrier[:len(barrier)-1]) + `,"extra":0}`, func() any { return new(MutationBarrier) }},
		{"mutation duplicate barrier", strings.Replace(string(mutation), `"barrier":`, `"barrier":null,"barrier":`, 1), func() any { return new(MutationResponse) }},
		{"mutation unknown", string(mutation[:len(mutation)-1]) + `,"extra":0}`, func() any { return new(MutationResponse) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(test.body), test.into()); err == nil {
				t.Fatalf("accepted %s", test.body)
			}
		})
	}
}

func TestV4ErrorEnvelopeRejectsAmbiguousMembers(t *testing.T) {
	client := &Storage{}
	for _, body := range []string{
		`{"errno":"ENOENT","errno":"ENOENT","message":"missing"}`,
		`{"errno":"ENOENT","message":"missing","extra":0}`,
		`{"errno":"EBUSY","message":"occupied","fileRecorded":true,"lockCode":"conflict"}`,
	} {
		err := client.storageError(Request{Op: OpFile}, []byte(body))
		var operation *operationError
		if !errors.As(err, &operation) || !operation.unknown || !errors.Is(err, syscall.EIO) {
			t.Fatalf("ambiguous error %s decoded as %v", body, err)
		}
	}
}
