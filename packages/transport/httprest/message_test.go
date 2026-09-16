package httprest_test

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func messageAttr(t *testing.T, a storage.Attr) *httprest.Attr {
	t.Helper()
	wire, err := httprest.AttrOf(a)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}
func messageChange(t *testing.T, c storage.AttrChange) *httprest.AttrChange {
	t.Helper()
	wire, err := httprest.AttrChangeOf(c)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}
func messageEntries(t *testing.T, entries []storage.Entry) []httprest.Entry {
	t.Helper()
	wire, err := httprest.EntriesOf(entries)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestAttrSurvivesJSON(t *testing.T) {
	created, changed := time.Unix(2, 3), time.Unix(4, 5)
	cases := []storage.Attr{
		{ID: 9, Kind: storage.NodeRegular, MetadataRevision: 1, AccessTime: time.Unix(0, 0), ModTime: time.Unix(0, 0)},
		{ID: 9, Kind: storage.NodeDirectory, MetadataRevision: 2, DirectoryRevision: 3, AccessTime: time.Now(), ModTime: time.Now()},
		{ID: 9, Kind: storage.NodeRegular, MetadataRevision: 4, Size: 1 << 40, AccessTime: time.Unix(1600000000, 1), ModTime: time.Unix(1755000000, 123456789)},
		{ID: 9, Kind: storage.NodeSymlink, MetadataRevision: 5, Size: 1, CreationTime: &created, ChangeTime: &changed, Metadata: storage.Metadata{{Key: "client", Version: 7, Data: []byte{0, 255, 1}}}},
	}
	for _, want := range cases {
		encoded, err := json.Marshal(messageAttr(t, want))
		if err != nil {
			t.Fatal(err)
		}
		var wire httprest.Attr
		if err := json.Unmarshal(encoded, &wire); err != nil {
			t.Fatal(err)
		}
		got, err := wire.Storage()
		if err != nil || !reflect.DeepEqual(got.Clone(), want.Clone()) || got.IsDir() != want.IsDir() {
			t.Fatalf("roundtrip of %+v through %s = %+v %v", want, encoded, got, err)
		}
	}
}

func TestAnAttrChangeSurvivesJSON(t *testing.T) {
	metadata := storage.Metadata{{Key: "client", Version: 2, Data: []byte{0, 255}}}
	empty := storage.Metadata{}
	accessed := time.Unix(-2208988800, 7)
	changed := time.Unix(1755000000, 123456789)
	cases := map[string]storage.AttrChange{
		"nothing at all": {}, "metadata alone": {ExpectedRevision: 3, Metadata: &metadata}, "clear metadata": {ExpectedRevision: 3, Metadata: &empty},
		"the access time": {AccessTime: &accessed}, "the modification one": {ModTime: &changed}, "both times": {AccessTime: &accessed, ModTime: &changed},
		"everything": {ExpectedRevision: 3, Metadata: &metadata, AccessTime: &accessed, ModTime: &changed, CreationTime: &accessed, ChangeTime: &changed},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(httprest.SetAttrRequest{Change: messageChange(t, want)})
			if err != nil {
				t.Fatal(err)
			}
			var request httprest.SetAttrRequest
			if err := json.Unmarshal(encoded, &request); err != nil {
				t.Fatal(err)
			}
			got, err := request.Change.Storage()
			if err != nil {
				t.Fatal(err)
			}
			if got.ExpectedRevision != want.ExpectedRevision || (got.Metadata == nil) != (want.Metadata == nil) {
				t.Fatalf("metadata presence/revision changed: %+v", got)
			}
			if got.Metadata != nil {
				actual, e := storage.EncodeMetadata(*got.Metadata)
				if e != nil {
					t.Fatal(e)
				}
				expected, e := storage.EncodeMetadata(*want.Metadata)
				if e != nil {
					t.Fatal(e)
				}
				if string(actual) != string(expected) {
					t.Fatalf("opaque metadata changed: %v != %v", actual, expected)
				}
			}
			for _, pair := range [][2]*time.Time{{got.AccessTime, want.AccessTime}, {got.ModTime, want.ModTime}, {got.CreationTime, want.CreationTime}, {got.ChangeTime, want.ChangeTime}} {
				if (pair[0] == nil) != (pair[1] == nil) || pair[0] != nil && !pair[0].Equal(*pair[1]) {
					t.Fatalf("optional time changed through %s: %v", encoded, pair)
				}
			}
		})
	}
}

func TestEntriesSurviveJSON(t *testing.T) {
	want := []storage.Entry{
		{Name: "a file", Attr: storage.Attr{ID: 9, Kind: storage.NodeRegular, MetadataRevision: 1, Size: 3, AccessTime: time.Unix(9, 0), ModTime: time.Unix(1, 0)}},
		{Name: "中文", Attr: storage.Attr{ID: 10, Kind: storage.NodeDirectory, MetadataRevision: 2, DirectoryRevision: 1, ModTime: time.Unix(2, 0)}},
		{Name: "\xff not utf-8", Attr: storage.Attr{ID: 11, Kind: storage.NodeRegular, MetadataRevision: 3, Size: 7, ModTime: time.Unix(3, 0), Metadata: storage.Metadata{{Key: "application", Version: 1, Data: []byte{255, 0}}}}},
	}
	encoded, err := json.Marshal(httprest.ListResponse{Entries: messageEntries(t, want)})
	if err != nil {
		t.Fatal(err)
	}
	var response httprest.ListResponse
	if err := json.Unmarshal(encoded, &response); err != nil {
		t.Fatal(err)
	}
	got, err := response.Storage()
	if err != nil || len(got) != len(want) {
		t.Fatalf("entries=%+v %v", got, err)
	}
	for i := range want {
		if got[i].Name != want[i].Name || !reflect.DeepEqual(got[i].Attr.Clone(), want[i].Attr.Clone()) {
			t.Fatalf("entry %d changed: %+v != %+v", i, got[i], want[i])
		}
	}
}

func TestAnEmptyListingEncodesAsAList(t *testing.T) {
	encoded, err := json.Marshal(httprest.ListResponse{Entries: messageEntries(t, nil)})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"entries":[]}` {
		t.Fatalf("empty listing=%s", encoded)
	}
}

func TestATimeOutsideTheNanosecondRange(t *testing.T) {
	cases := map[string]time.Time{"the zero time": {}, "the last year UnixNano can hold": time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC), "the first year it cannot": time.Date(2262, 4, 12, 0, 0, 0, 1, time.UTC), "a time before the epoch": time.Date(1600, 3, 4, 5, 6, 7, 89, time.UTC), "a time this system may outlive": time.Date(2500, 1, 2, 3, 4, 5, 678, time.UTC), "the far future": time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), "a date archives are known to hold": time.Date(-4000, 1, 1, 0, 0, 0, 0, time.UTC)}
	for name, want := range cases {
		t.Run("reported "+name, func(t *testing.T) {
			encoded, err := json.Marshal(messageAttr(t, storage.Attr{ID: 9, Kind: storage.NodeRegular, MetadataRevision: 1, AccessTime: want, ModTime: want, CreationTime: &want, ChangeTime: &want}))
			if err != nil {
				t.Fatal(err)
			}
			var wire httprest.Attr
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			got, err := wire.Storage()
			if err != nil {
				t.Fatal(err)
			}
			for _, instant := range []*time.Time{&got.AccessTime, &got.ModTime, got.CreationTime, got.ChangeTime} {
				if instant == nil || !instant.Equal(want) {
					t.Fatalf("time %v through %s became %v", want, encoded, instant)
				}
			}
		})
		t.Run("set to "+name, func(t *testing.T) {
			encoded, err := json.Marshal(httprest.SetAttrRequest{Change: messageChange(t, storage.AttrChange{AccessTime: &want, ModTime: &want, CreationTime: &want, ChangeTime: &want})})
			if err != nil {
				t.Fatal(err)
			}
			var request httprest.SetAttrRequest
			if err := json.Unmarshal(encoded, &request); err != nil {
				t.Fatal(err)
			}
			got, err := request.Change.Storage()
			if err != nil {
				t.Fatal(err)
			}
			for _, instant := range []*time.Time{got.AccessTime, got.ModTime, got.CreationTime, got.ChangeTime} {
				if instant == nil || !instant.Equal(want) {
					t.Fatalf("set time %v through %s became %v", want, encoded, instant)
				}
			}
		})
	}
}
func TestASpaceReportSurvivesJSON(t *testing.T) {
	cases := []storage.Space{
		{},
		{Total: 1 << 40, Used: 1 << 20, Avail: 1<<40 - 1<<20},
		{Total: 4096, Used: 4096},
		// An allowance lowered underneath content already written, with nothing available.
		{Total: 4096, Used: 8192},
		// The superuser reserve a filesystem keeps, which is why the contract carries
		// three figures rather than deriving the third.
		{Total: 1 << 40, Used: 1 << 30, Avail: 1 << 20},
		{Total: math.MaxInt64, Used: math.MaxInt64, Avail: 0},
	}
	for _, want := range cases {
		encoded, err := json.Marshal(httprest.SpaceResponse{Space: httprest.SpaceOf(want)})
		if err != nil {
			t.Fatalf("marshal %+v: %v", want, err)
		}
		var resp httprest.SpaceResponse
		if err := json.Unmarshal(encoded, &resp); err != nil {
			t.Fatalf("unmarshal %s: %v", encoded, err)
		}
		if got := resp.Space.Storage(); got != want {
			t.Fatalf("round trip of %+v through %s gave %+v", want, encoded, got)
		}
	}
}

// A count that never arrived reads as zero, and zero available is a volume that
// refuses every write. Neither an absent count nor figures that could not all be true of
// anything may be delivered as a report.
func TestABodyThatCarriesNoSpaceReport(t *testing.T) {
	cases := map[string]bool{
		`{}`:                                 false,
		`null`:                               false,
		`{"space":null}`:                     false,
		`{"space":{}}`:                       false,
		`{"attr":{"mode":0}}`:                false,
		`{"space":{"used":0,"avail":0}}`:     false,
		`{"space":{"total":4096,"avail":0}}`: false,
		`{"space":{"total":4096,"used":0}}`:  false,
		`{"space":{"total":4096,"used":0,"free":4096}}`:   false,
		`{"space":{"total":"4096","used":0,"avail":0}}`:   false,
		`{"space":{"total":-1,"used":0,"avail":0}}`:       false,
		`{"space":{"total":4096,"used":-1,"avail":0}}`:    false,
		`{"space":{"total":4096,"used":0,"avail":-1}}`:    false,
		`{"space":{"total":4096,"used":4096,"avail":1}}`:  false,
		`{"space":{"total":0,"used":0,"avail":0}}`:        true,
		`{"space":{"total":4096,"used":1024,"avail":10}}`: true,
		`{"space":{"total":4096,"used":8192,"avail":0}}`:  true,
	}
	for body, want := range cases {
		t.Run(body, func(t *testing.T) {
			var resp httprest.SpaceResponse
			err := json.Unmarshal([]byte(body), &resp)
			if got := err == nil; got != want {
				t.Fatalf("%s decoded as %+v, %v", body, resp.Space, err)
			}
		})
	}
}

func TestABodyThatCarriesNoAttributes(t *testing.T) {
	attr := `{"id":9,"kind":1,"size":0,"access_time":{"unix_sec":0,"nanos":0},"mod_time":{"unix_sec":0,"nanos":0},"metadata_revision":1,"directory_revision":0,"metadata":"UkZNAQAA"}`
	statCases := map[string]bool{`{}`: false, `null`: false, `{"attr":null}`: false, `{"entries":[]}`: false, `{"attr":{"kind":1}}`: false, `{"attr":{"id":0,"kind":1,"size":7}}`: false, `{"attr":` + attr + `}`: true, `{"attr":` + attr + `,"entries":[]}`: true, `{"attr":` + strings.Replace(attr, `"id":9`, `"id":0`, 1) + `}`: false, `{"attr":` + strings.Replace(attr, `"metadata_revision":1`, `"metadata_revision":0`, 1) + `}`: false}
	for body, want := range statCases {
		t.Run("stat "+body, func(t *testing.T) {
			var response httprest.StatResponse
			err := json.Unmarshal([]byte(body), &response)
			if (err == nil) != want {
				t.Fatalf("body=%s attr=%+v err=%v", body, response.Attr, err)
			}
		})
	}
	listCases := map[string]bool{`{}`: false, `{"entries":null}`: false, `{"entries":[{"name":"Zg=="}]}`: false, `{"entries":[{"name":"Zg==","attr":null}]}`: false, `{"entries":[{"name":7,"attr":` + attr + `}]}`: false, `{"entries":[]}`: true, `{"entries":[{"name":"Zg==","attr":{"kind":1}}]}`: false, `{"entries":[{"name":"Zg==","attr":` + attr + `}]}`: true}
	for body, want := range listCases {
		t.Run("list "+body, func(t *testing.T) {
			var response httprest.ListResponse
			err := json.Unmarshal([]byte(body), &response)
			if (err == nil) != want {
				t.Fatalf("body=%s entries=%+v err=%v", body, response.Entries, err)
			}
		})
	}
	setAttrCases := map[string]bool{`{}`: false, `null`: false, `{"change":null}`: false, `{"change":{"mode":"0644"}}`: false, `{"change":{}}`: false, `{"change":{"expected_revision":0}}`: true, `{"change":{"expected_revision":0,"mod_time":{"unix_sec":-1,"nanos":1}}}`: true}
	for body, want := range setAttrCases {
		t.Run("setattr "+body, func(t *testing.T) {
			var request httprest.SetAttrRequest
			err := json.Unmarshal([]byte(body), &request)
			if (err == nil) != want {
				t.Fatalf("body=%s change=%+v err=%v", body, request.Change, err)
			}
		})
	}
}

func TestTheWireForm(t *testing.T) {
	attr := storage.Attr{ID: 77, Kind: storage.NodeDirectory, MetadataRevision: 2, DirectoryRevision: 3, AccessTime: time.Unix(1700000000, 1), ModTime: time.Unix(1755000000, 123456789)}
	encoded, err := json.Marshal(httprest.StatResponse{Attr: messageAttr(t, attr)})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"attr":{"id":77,"kind":2,"size":0,"access_time":{"unix_sec":1700000000,"nanos":1},"mod_time":{"unix_sec":1755000000,"nanos":123456789},"metadata_revision":2,"directory_revision":3,"metadata":"UkZNAQAA"}}`
	if string(encoded) != want {
		t.Fatalf("stat wire=%s want=%s", encoded, want)
	}
	metadata := storage.Metadata{}
	changed := time.Unix(1755000000, 123456789)
	encoded, err = json.Marshal(httprest.SetAttrRequest{Change: messageChange(t, storage.AttrChange{ExpectedRevision: 2, Metadata: &metadata, ModTime: &changed})})
	if err != nil {
		t.Fatal(err)
	}
	want = `{"change":{"expected_revision":2,"metadata":"UkZNAQAA","mod_time":{"unix_sec":1755000000,"nanos":123456789}}}`
	if string(encoded) != want {
		t.Fatalf("metadata-time wire=%s want=%s", encoded, want)
	}
	encoded, err = json.Marshal(httprest.SetAttrRequest{Change: messageChange(t, storage.AttrChange{})})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"change":{"expected_revision":0}}` {
		t.Fatalf("empty change=%s", encoded)
	}
	encoded, err = json.Marshal(httprest.SpaceResponse{Space: httprest.SpaceOf(storage.Space{Total: 4096})})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"space":{"total":4096,"used":0,"avail":0}}` {
		t.Fatalf("space=%s", encoded)
	}
}

func TestSetAttrRequestRejectsMalformedChangesBeforeDispatch(t *testing.T) {
	cases := map[string]string{
		"removed mode":       `{"change":{"expected_revision":0,"mode":420}}`,
		"missing revision":   `{"change":{}}`,
		"missing seconds":    `{"change":{"expected_revision":0,"mod_time":{"nanos":0}}}`,
		"missing nanos":      `{"change":{"expected_revision":0,"mod_time":{"unix_sec":0}}}`,
		"negative nanos":     `{"change":{"expected_revision":0,"mod_time":{"unix_sec":0,"nanos":-1}}}`,
		"overflow nanos":     `{"change":{"expected_revision":0,"mod_time":{"unix_sec":0,"nanos":1000000000}}}`,
		"null optional time": `{"change":{"expected_revision":0,"mod_time":null}}`,
		"null metadata":      `{"change":{"expected_revision":1,"metadata":null}}`,
		"corrupt metadata":   `{"change":{"expected_revision":1,"metadata":"AA=="}}`,
		"unknown":            `{"change":{"expected_revision":0,"extra":1}}`,
		"duplicate revision": `{"change":{"expected_revision":0,"expected_revision":0}}`,
		"duplicate change":   `{"change":{"expected_revision":0},"change":{"expected_revision":0}}`,
		"wrong case":         `{"change":{"Expected_revision":0}}`,
		"trailing":           `{"change":{"expected_revision":0}}{}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			var request httprest.SetAttrRequest
			if err := json.Unmarshal([]byte(body), &request); err == nil {
				t.Fatalf("malformed change accepted: %s", body)
			}
		})
	}
}
