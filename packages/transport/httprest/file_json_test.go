package httprest

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileJSONRejectsMissingDuplicateAndWrongTypedOperands(t *testing.T) {
	request := fileRequest{Op: storage.OpFileRead, Session: strings.Repeat("a", 64), Reference: 11, Read: &fileReadRequest{Offset: 0, Length: 1}}
	encoded, err := marshalFileJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	source := string(encoded)
	for name, body := range map[string]string{
		"missing zero": strings.Replace(source, `"offset":0,`, "", 1),
		"duplicate":    strings.Replace(source, `"offset":0`, `"offset":0,"offset":0`, 1),
		"wrong type":   strings.Replace(source, `"length":1`, `"length":"1"`, 1),
		"null":         strings.Replace(source, `"offset":0`, `"offset":null`, 1),
		"unknown":      strings.Replace(source, `"offset":0`, `"offset":0,"unknown":false`, 1),
		"wrong case":   strings.Replace(source, `"offset":0`, `"Offset":0`, 1),
		"trailing":     source + `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			if body == source {
				t.Fatal("malformed fixture did not change request")
			}
			var decoded fileRequest
			if err := decodeFileJSON([]byte(body), &decoded); err == nil {
				t.Fatal("malformed operand accepted")
			}
		})
	}
	var decoded fileRequest
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileRequest(decoded); err != nil {
		t.Fatal(err)
	}
	decoded.Action = protocolID(t)
	if err := validateFileRequest(decoded); err == nil {
		t.Fatal("read accepted unrelated action")
	}
	decoded.Action = ""
	decoded.Write = &fileWriteRequest{Data: []byte{}}
	if err := validateFileRequest(decoded); err == nil {
		t.Fatal("read accepted unrelated mutation operands")
	}
}

func TestFileJSONPreservesRangeIdentityAndMultiplicity(t *testing.T) {
	ranges := make([]storage.RangeAcquisition, 32)
	for i := range ranges {
		ranges[i] = storage.RangeAcquisition{ID: storage.RangeAcquisitionID(i + 1), Start: 1, End: 9}
	}
	request := fileRequest{Op: storage.OpFileReplaceRanges, Session: strings.Repeat("a", 64), Reference: 11, Action: protocolID(t), Ranges: &storage.RangeReplaceRequest{Owner: 7, Scope: storage.RangeScope{Domain: 9}, ExpectedRevision: 2, Ranges: ranges}}
	encoded, err := marshalFileJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fileRequest
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileRequest(decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Ranges.Ranges) != 32 {
		t.Fatalf("ranges collapsed to %d", len(decoded.Ranges.Ranges))
	}
	for i, r := range decoded.Ranges.Ranges {
		if r != ranges[i] {
			t.Fatalf("acquisition %d changed: %+v", i, r)
		}
	}
	if fileControl(request.Op) {
		t.Fatal("large range replacement used cleanup admission")
	}
	decoded.Ranges.Ranges[1].ID = decoded.Ranges.Ranges[0].ID
	if err := validateFileRequest(decoded); err == nil {
		t.Fatal("duplicate acquisition identity accepted")
	}
}

func TestFileJSONRejectsMissingCanonicalMetadataAndEffectMembers(t *testing.T) {
	req := fileRequest{Op: storage.OpFileQueryAction, Action: protocolID(t)}
	receipt := protocolReceipt(req)
	receipt.Operation = storage.OpFileRetain
	receipt.Reference = 11
	receipt.Effects = storage.EffectRetained
	receipt.Observation = protocolObservation(t, protocolAttr())
	encoded, err := json.Marshal(fileResponse{Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	source := string(encoded)
	for name, body := range map[string]string{
		"effects":        strings.Replace(source, `"effects":1,`, "", 1),
		"range revision": strings.Replace(source, `"rangeRevision":0,`, "", 1),
		"metadata null":  strings.Replace(source, `"metadata":"UkZNAQEACwAHAAAAAwAAAGFwcGxpY2F0aW9uAP8B"`, `"metadata":null`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "metadata null" {
				var value map[string]any
				if err := json.Unmarshal(encoded, &value); err != nil {
					t.Fatal(err)
				}
				value["receipt"].(map[string]any)["observation"].(map[string]any)["attr"].(map[string]any)["metadata"] = nil
				raw, e := json.Marshal(value)
				if e != nil {
					t.Fatal(e)
				}
				body = string(raw)
			}
			if body == source {
				t.Fatal("malformed fixture unchanged")
			}
			var response fileResponse
			if err := decodeFileJSON([]byte(body), &response); err == nil {
				t.Fatal("incomplete receipt accepted")
			}
		})
	}
}

func TestFileJSONEntryTargetPreservesOptionalWitness(t *testing.T) {
	root := storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 1, NodeID: 1, Ancestors: []storage.EntryCondition{}}
	for _, witness := range []*storage.EntryLocation{nil, &root} {
		target := storage.EntryTarget{Parent: 1, ParentID: 1, Name: []byte{255, 'x'}, DirectoryRevision: 2, Witness: witness}
		data, err := marshalFileJSON(fileTargetOf(target))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), `"witness":`) != (witness != nil) {
			t.Fatalf("optional witness presence changed: %s", data)
		}
		var wire fileEntryTarget
		if err := decodeFileJSON(data, &wire); err != nil {
			t.Fatal(err)
		}
		got := wire.storage()
		if err := got.Check(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, target) {
			t.Fatalf("target roundtrip=%+v want=%+v", got, target)
		}
	}
}

func TestFileJSONRenameDistinguishesAbsentAndEmptyNewName(t *testing.T) {
	source := storage.EntryTarget{Parent: 1, ParentID: 1, Name: []byte("source"), DirectoryRevision: 2, ExpectedEntryID: 3, ExpectedNodeID: 7}
	destination := storage.EntryTarget{Parent: 1, ParentID: 1, Name: []byte("dest"), DirectoryRevision: 2}
	request := fileRequest{Op: storage.OpFileRename, Session: strings.Repeat("a", 64), Reference: 11, Action: protocolID(t), Rename: fileRenameOf(storage.RenameRequest{Source: source, Destination: destination})}
	data, err := marshalFileJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"newName":`) {
		t.Fatalf("nil spelling did not remain absent: %s", data)
	}
	var decoded fileRequest
	if err := decodeFileJSON(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileRequest(decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Rename.NewName != nil {
		t.Fatal("absent spelling became explicit empty")
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	raw["rename"].(map[string]any)["newName"] = ""
	invalid, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var empty fileRequest
	if err := decodeFileJSON(invalid, &empty); err != nil {
		t.Fatal(err)
	}
	if empty.Rename.NewName == nil {
		t.Fatal("raw empty spelling lost presence")
	}
	if err := validateFileRequest(empty); err == nil {
		t.Fatalf("empty spelling accepted: %s", invalid)
	}
}
