package httprest_test

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestAttrSurvivesJSON(t *testing.T) {
	cases := []storage.Attr{
		{ID: 9, Kind: storage.NodeRegular, Size: 0, AccessTime: time.Unix(0, 0), ModTime: time.Unix(0, 0)},
		{ID: 9, Kind: storage.NodeDirectory, Size: 4096, AccessTime: time.Now(), ModTime: time.Now()},
		{ID: 9, Kind: storage.NodeRegular, Size: 1 << 40, AccessTime: time.Unix(1600000000, 1), ModTime: time.Unix(1755000000, 123456789)},
		{ID: 9, Kind: storage.NodeRegular, Size: 1},
	}
	for _, want := range cases {
		encoded, err := json.Marshal(httprest.AttrOf(want))
		if err != nil {
			t.Fatalf("marshal %+v: %v", want, err)
		}
		var wire httprest.Attr
		if err := json.Unmarshal(encoded, &wire); err != nil {
			t.Fatalf("unmarshal %s: %v", encoded, err)
		}
		got := wire.Storage()
		if got.Kind != want.Kind || !reflect.DeepEqual(got.Metadata, want.Metadata) || got.Size != want.Size ||
			!got.AccessTime.Equal(want.AccessTime) || !got.ModTime.Equal(want.ModTime) {
			t.Fatalf("round trip of %+v through %s gave %+v", want, encoded, got)
		}
		if got.IsDir() != want.IsDir() {
			t.Fatalf("round trip lost the directory bit of %v", want.Kind)
		}
	}
}

// Unspecified times must stay absent even when another time is explicitly zero.
func TestAnAttrChangeSurvivesJSON(t *testing.T) {
	birth := time.Time{}
	accessed := time.Unix(-2208988800, 7)
	changed := time.Unix(1755000000, 123456789)
	cases := map[string]storage.AttrChange{
		"nothing at all":       {},
		"the birth time":       {BirthTime: &birth},
		"the access time":      {AccessTime: &accessed},
		"the modification one": {ModTime: &changed},
		"both times":           {AccessTime: &accessed, ModTime: &changed},
		"everything":           {BirthTime: &birth, ChangeTime: &changed, AccessTime: &accessed, ModTime: &changed},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(httprest.SetAttrRequest{Change: httprest.AttrChangeOf(want)})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var request httprest.SetAttrRequest
			if err := json.Unmarshal(encoded, &request); err != nil {
				t.Fatalf("unmarshal %s: %v", encoded, err)
			}
			got := request.Change.Storage()

			for _, times := range [][2]*time.Time{
				{got.AccessTime, want.AccessTime}, {got.ModTime, want.ModTime}, {got.BirthTime, want.BirthTime}, {got.ChangeTime, want.ChangeTime},
			} {
				if (times[0] == nil) != (times[1] == nil) {
					t.Fatalf("round trip through %s changed whether a time is named", encoded)
				}
				if times[0] != nil && !times[0].Equal(*times[1]) {
					t.Fatalf("a time round-tripped through %s as %v, want %v", encoded, *times[0], *times[1])
				}
			}
		})
	}
}

func TestEntriesSurviveJSON(t *testing.T) {
	want := []storage.Entry{
		{Name: "a file", Attr: storage.Attr{ID: 9, Kind: storage.NodeRegular, Size: 3, AccessTime: time.Unix(9, 0), ModTime: time.Unix(1, 0)}},
		{Name: "日本語", Attr: storage.Attr{ID: 9, Kind: storage.NodeDirectory, ModTime: time.Unix(2, 0)}},
		{Name: "\xff not utf-8", Attr: storage.Attr{ID: 9, Kind: storage.NodeRegular, Size: 7, ModTime: time.Unix(3, 0)}},
	}
	encoded, err := json.Marshal(httprest.ListResponse{Entries: httprest.EntriesOf(want)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var resp httprest.ListResponse
	if err := json.Unmarshal(encoded, &resp); err != nil {
		t.Fatalf("unmarshal %s: %v", encoded, err)
	}
	got := resp.Storage()
	if len(got) != len(want) {
		t.Fatalf("round trip of %d entries gave %d: %s", len(want), len(got), encoded)
	}
	for i := range want {
		if got[i].Name != want[i].Name {
			t.Fatalf("entry %d round-tripped as name %q, want %q (wire form %s)", i, got[i].Name, want[i].Name, encoded)
		}
		if got[i].Attr.Kind != want[i].Attr.Kind || got[i].Attr.Size != want[i].Attr.Size {
			t.Fatalf("entry %d round-tripped as %+v, want %+v", i, got[i], want[i])
		}
		if !got[i].Attr.AccessTime.Equal(want[i].Attr.AccessTime) || !got[i].Attr.ModTime.Equal(want[i].Attr.ModTime) {
			t.Fatalf("entry %d round-tripped with times %v and %v, want %v and %v", i,
				got[i].Attr.AccessTime, got[i].Attr.ModTime, want[i].Attr.AccessTime, want[i].Attr.ModTime)
		}
	}
}

// An empty directory must encode as an empty list, never as JSON null: null and "no
// answer" are too easy to confuse on the far side.
func TestAnEmptyListingEncodesAsAList(t *testing.T) {
	encoded, err := json.Marshal(httprest.ListResponse{Entries: httprest.EntriesOf(nil)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if want := `{"entries":[]}`; string(encoded) != want {
		t.Fatalf("an empty listing encodes as %s, want %s", encoded, want)
	}
}

// A time outside 1678-09-21 to 2262-04-11 is where time.Time.UnixNano is undefined, and
// what it produces there is not an error but a different, entirely plausible date. A
// filesystem hands times to whatever walks the tree — a build system deciding what is
// stale, an archiver deciding what changed — so a wrong one that looks right is the answer
// this system is least able to survive. Both directions are checked: a time being reported
// and a time being set.
func TestATimeOutsideTheNanosecondRange(t *testing.T) {
	cases := map[string]time.Time{
		"the zero time":                     {},
		"the last year UnixNano can hold":   time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC),
		"the first year it cannot":          time.Date(2262, 4, 12, 0, 0, 0, 1, time.UTC),
		"a time before the epoch":           time.Date(1600, 3, 4, 5, 6, 7, 89, time.UTC),
		"a time this system may outlive":    time.Date(2500, 1, 2, 3, 4, 5, 678, time.UTC),
		"the far future":                    time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
		"a date archives are known to hold": time.Date(-4000, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for name, want := range cases {
		t.Run("reported "+name, func(t *testing.T) {
			encoded, err := json.Marshal(httprest.AttrOf(storage.Attr{ID: 9, Kind: storage.NodeRegular, AccessTime: want, ModTime: want}))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var wire httprest.Attr
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatalf("unmarshal %s: %v", encoded, err)
			}
			got := wire.Storage()
			if !got.ModTime.Equal(want) || !got.AccessTime.Equal(want) {
				t.Fatalf("%v round-tripped through %s as %v and %v", want, encoded, got.AccessTime, got.ModTime)
			}
		})
		t.Run("set to "+name, func(t *testing.T) {
			encoded, err := json.Marshal(httprest.SetAttrRequest{
				Change: httprest.AttrChangeOf(storage.AttrChange{AccessTime: &want, ModTime: &want}),
			})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var request httprest.SetAttrRequest
			if err := json.Unmarshal(encoded, &request); err != nil {
				t.Fatalf("unmarshal %s: %v", encoded, err)
			}
			got := request.Change.Storage()
			if !got.ModTime.Equal(want) || !got.AccessTime.Equal(want) {
				t.Fatalf("%v round-tripped through %s as %v and %v", want, encoded, *got.AccessTime, *got.ModTime)
			}
		})
	}
}

// The three counts a space report carries are byte counts whose zero is a legitimate
// figure, so a field lost on the way across cannot be told from one that says nothing is
// there. The shape is what tells them apart, and the decoding is what refuses it.
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

// Missing metadata must not become a plausible report about a node.
func TestABodyThatCarriesNoAttributes(t *testing.T) {
	statCases := map[string]bool{
		`{}`:             false,
		`null`:           false,
		`{"attr":null}`:  false,
		`{"entries":[]}`: false,
		// Attributes carrying no identity are an absence too, and the one that does not
		// show in the shape of the body: every comparison of a zero identity above
		// returns equal, so a peer that omits it reads as saying every node is one node.
		`{"attr":{"mode":0}}`:                   false,
		`{"attr":{"id":0,"mode":420,"size":7}}`: false,
		`{"attr":{"id":9,"kind":1,"size":0,"access_time":{"unix_sec":0,"nanos":0},"mod_time":{"unix_sec":0,"nanos":0}}}`: true,
		`{"attr":{"id":9,"kind":1,"size":7}}`:     false,
		`{"attr":{"id":9,"kind":1},"entries":[]}`: false,
		`{"attr":{"id":9,"kind":2,"size":0}}`:     false,
	}
	for body, want := range statCases {
		t.Run("stat "+body, func(t *testing.T) {
			var resp httprest.StatResponse
			err := json.Unmarshal([]byte(body), &resp)
			if got := err == nil; got != want {
				t.Fatalf("%s decoded as %+v, %v", body, resp.Attr, err)
			}
		})
	}

	listCases := map[string]bool{
		`{}`:                            false,
		`{"entries":null}`:              false,
		`{"entries":[{"name":"Zg=="}]}`: false,
		`{"entries":[{"name":"Zg==","attr":null}]}`:  false,
		`{"entries":[{"name":7,"attr":{"mode":0}}]}`: false,
		`{"entries":[]}`: true,
		`{"entries":[{"name":"Zg==","attr":{"mode":0}}]}`: false,
		`{"entries":[{"name":"Zg==","attr":{"id":9,"kind":1,"size":0,"access_time":{"unix_sec":0,"nanos":0},"mod_time":{"unix_sec":0,"nanos":0}}}]}`: true,
	}
	for body, want := range listCases {
		t.Run("list "+body, func(t *testing.T) {
			var resp httprest.ListResponse
			err := json.Unmarshal([]byte(body), &resp)
			if got := err == nil; got != want {
				t.Fatalf("%s decoded as %+v, %v", body, resp.Entries, err)
			}
		})
	}

	// A change naming nothing is a legitimate request — it asks whether the node is there
	// — so the values cannot tell a whole body from one that lost its contents. Only the
	// enclosing object can.
	setAttrCases := map[string]bool{
		`{}`:                         false,
		`null`:                       false,
		`{"change":null}`:            false,
		`{"change":{"mode":"0644"}}`: false,
		`{"change":{}}`:              true,
		`{"change":{"birth_time":{"unix_sec":0,"nanos":0}}}`: true,
		`{"change":{"mod_time":{"unix_sec":-1,"nanos":1}}}`:  true,
	}
	for body, want := range setAttrCases {
		t.Run("setattr "+body, func(t *testing.T) {
			var request httprest.SetAttrRequest
			err := json.Unmarshal([]byte(body), &request)
			if got := err == nil; got != want {
				t.Fatalf("%s decoded as %+v, %v", body, request.Change, err)
			}
		})
	}
}

// The wire form is the contract between the two sides, so it is pinned here rather than
// left to whatever the struct tags happen to say: a field renamed on one side alone
// arrives as absence on the other, which is a failure this protocol reports but a
// needless one to walk into.
func TestTheWireForm(t *testing.T) {
	attr := storage.Attr{
		ID:         77,
		Kind:       storage.NodeDirectory,
		Size:       4096,
		AccessTime: time.Unix(1700000000, 1),
		ModTime:    time.Unix(1755000000, 123456789),
	}
	encoded, err := json.Marshal(httprest.StatResponse{Attr: httprest.AttrOf(attr)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"attr":{"id":77,"kind":2,"size":4096,` +
		`"access_time":{"unix_sec":1700000000,"nanos":1},` +
		`"mod_time":{"unix_sec":1755000000,"nanos":123456789}}}`
	if string(encoded) != want {
		t.Fatalf("a stat answer encodes as %s, want %s", encoded, want)
	}

	// An attribute the change does not name is absent from the body rather than present
	// with a value standing for "unchanged". There is no such value: every
	// instant is one a caller may ask for.
	changed := time.Unix(1755000000, 123456789)
	encoded, err = json.Marshal(httprest.SetAttrRequest{
		Change: httprest.AttrChangeOf(storage.AttrChange{BirthTime: &changed, ModTime: &changed}),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want = `{"change":{"mod_time":{"unix_sec":1755000000,"nanos":123456789},"birth_time":{"unix_sec":1755000000,"nanos":123456789}}}`
	if string(encoded) != want {
		t.Fatalf("a birth-and-modification-time change encodes as %s, want %s", encoded, want)
	}

	encoded, err = json.Marshal(httprest.SetAttrRequest{Change: httprest.AttrChangeOf(storage.AttrChange{})})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if want := `{"change":{}}`; string(encoded) != want {
		t.Fatalf("a change naming nothing encodes as %s, want %s", encoded, want)
	}

	// Every count is written out, zero included: they are pointers so that an absent one
	// can be refused, and omitting the zeroes would send exactly the shape that refusal
	// exists to catch.
	encoded, err = json.Marshal(httprest.SpaceResponse{Space: httprest.SpaceOf(storage.Space{Total: 4096})})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if want := `{"space":{"total":4096,"used":0,"avail":0}}`; string(encoded) != want {
		t.Fatalf("a space report encodes as %s, want %s", encoded, want)
	}
}

func TestMetadataFactsPreserveUnknownAndOpaqueBytes(t *testing.T) {
	zero := time.Time{}
	future := time.Date(2500, 1, 2, 3, 4, 5, 6, time.UTC)
	for _, birth := range []*time.Time{nil, &zero, &future} {
		want := storage.Attr{ID: 11, Kind: storage.NodeSymlink, BirthTime: birth, ChangeTime: &future,
			Metadata: map[string]storage.OpaquePayload{
				"client.alpha.v1": {Version: []byte{0xff, 0, 1}, Data: []byte{0xfe, 0, 0xff}},
				"client.beta.v1":  {Version: []byte{1}, Data: []byte{}},
			}}
		wire := httprest.AttrOf(want)
		encoded, err := json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		var decoded httprest.Attr
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		got := decoded.Storage()
		if (got.BirthTime == nil) != (birth == nil) || birth != nil && !got.BirthTime.Equal(*birth) ||
			got.ChangeTime == nil || !got.ChangeTime.Equal(future) || !reflect.DeepEqual(got.Metadata, want.Metadata) {
			t.Fatalf("metadata round trip: got %+v, want %+v", got, want)
		}
		wire.Metadata["client.alpha.v1"].Data[0] = 1
		got.Metadata["client.alpha.v1"].Version[0] = 1
		if want.Metadata["client.alpha.v1"].Data[0] != 0xfe || decoded.Metadata["client.alpha.v1"].Version[0] != 0xff {
			t.Fatal("metadata conversion retained mutable caller bytes")
		}
	}
}

func TestMetadataWireRejectsIncompleteOrNoncanonicalFacts(t *testing.T) {
	base := `{"id":9,"kind":1,"size":0,"access_time":{"unix_sec":0,"nanos":0},"mod_time":{"unix_sec":0,"nanos":0}`
	for name, suffix := range map[string]string{
		"unknown kind":        `,"kind":255}`,
		"duplicate identity":  `,"id":9}`,
		"platform mode":       `,"mode":420}`,
		"unknown birth":       `,"birth_time":null}`,
		"partial time":        `,"birth_time":{"unix_sec":0}}`,
		"invalid nanos":       `,"change_time":{"unix_sec":0,"nanos":1000000000}}`,
		"duplicate time":      `,"birth_time":{"unix_sec":0,"unix_sec":1,"nanos":0}}`,
		"unknown time field":  `,"birth_time":{"unix_sec":0,"nanos":0,"extra":1}}`,
		"invalid namespace":   `,"metadata":{"UPPER":{"version":"AQ==","data":""}}}`,
		"missing version":     `,"metadata":{"client.v1":{"data":""}}}`,
		"empty version":       `,"metadata":{"client.v1":{"version":"","data":""}}}`,
		"null payload":        `,"metadata":{"client.v1":{"version":"AQ==","data":null}}}`,
		"duplicate namespace": `,"metadata":{"client.v1":{"version":"AQ==","data":""},"client.v1":{"version":"Ag==","data":""}}}`,
		"noncanonical base64": `,"metadata":{"client.v1":{"version":"AR==","data":""}}}`,
		"invalid base64":      `,"metadata":{"client.v1":{"version":"AQ==","data":"%%%"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var attr httprest.Attr
			if err := json.Unmarshal([]byte(base+suffix), &attr); err == nil {
				t.Fatal("invalid metadata accepted")
			}
		})
	}
	for name, metadata := range map[string]map[string]storage.OpaquePayload{
		"long version": {"client.v1": {Version: make([]byte, storage.MaxObservationTokenBytes+1)}},
		"long payload": {"client.v1": {Version: []byte{1}, Data: make([]byte, storage.MaxMetadataValueBytes+1)}},
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(httprest.AttrOf(storage.Attr{ID: 9, Kind: storage.NodeRegular, Metadata: metadata}))
			if err != nil {
				t.Fatal(err)
			}
			var attr httprest.Attr
			if err := json.Unmarshal(encoded, &attr); err == nil {
				t.Fatal("unbounded metadata accepted")
			}
		})
	}
}
