package httprest

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileReceiptPreservesIndependentPreparedAndDrainConditions(t *testing.T) {
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	removal := storage.RemovalStatus{IntentID: 5, EntryID: 7, Prepared: true, PreparedCondition: storage.RemovalFile, State: storage.EntryDraining, Generation: 11, DrainCondition: storage.RemovalIfEmpty}
	native := storage.FileActionReceipt{Action: id, Operation: storage.OpFilePrepareRemoval, State: storage.FileActionCompleted, Effects: storage.EffectPreparedChanged, Reference: 3, Removal: removal}
	wire, err := fileReceiptOf(native)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := marshalFileJSON(wire)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fileReceipt
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileReceipt(fileRequest{Op: storage.OpFilePrepareRemoval, Action: id, Reference: 3}, decoded); err != nil {
		t.Fatal(err)
	}
	got, err := decoded.storage()
	if err != nil || got.Removal != removal {
		t.Fatalf("independent conditions changed: %+v %v", got.Removal, err)
	}
	legacy := bytes.Replace(encoded, []byte(`"PreparedCondition"`), []byte(`"Condition"`), 1)
	if err := decodeFileJSON(legacy, &decoded); err == nil {
		t.Fatal("ambiguous removed condition field was accepted")
	}
	invalid := got
	invalid.Removal.Prepared = false
	invalid.Removal.IntentID = 0
	if _, err := (fileReceipt{Removal: invalid.Removal}).storage(); err == nil {
		t.Fatal("unprepared receipt retained a prepared condition")
	}
}

func TestFileKindOwnerPreservesAnonymousZeroAndOpaqueValues(t *testing.T) {
	zero, maximum := storage.RangeOwnerID(0), storage.RangeOwnerID(^uint64(0))
	for name, owner := range map[string]*storage.RangeOwnerID{"anonymous": nil, "zero": &zero, "opaque": &maximum} {
		t.Run(name, func(t *testing.T) {
			want := storage.SetKindRequest{Owner: owner, ExpectedRevision: 3, Kind: storage.NodeSymlink, LinkTarget: []byte{0xff, 't'}, Metadata: storage.Metadata{{Key: "client", Version: 7, Data: []byte{0, 0xff}}}}
			wire, err := fileKindOf(want)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := marshalFileJSON(wire)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(encoded, []byte(`"owner"`)) != (owner != nil) {
				t.Fatalf("owner presence changed: %s", encoded)
			}
			var decoded fileKindRequest
			if err := decodeFileJSON(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			got, err := decoded.storage()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("kind operands changed: got %#v want %#v", got, want)
			}
		})
	}
	var decoded fileKindRequest
	for _, value := range []string{"null", "-1", `"0"`} {
		data := []byte(`{"owner":` + value + `,"expectedRevision":3,"kind":3,"linkTarget":"dA==","metadata":"UkZNAQAA"}`)
		if err := decodeFileJSON(data, &decoded); err == nil {
			t.Fatalf("accepted malformed owner %s", value)
		}
	}
}
