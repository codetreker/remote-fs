package httprest

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestMessageJSONPreservesRawBytesAndScalarValues(t *testing.T) {
	type nested struct {
		Data  []byte `json:"data"`
		Count uint64 `json:"count"`
	}
	type envelope struct {
		Items    []nested `json:"items"`
		Optional *string  `json:"optional,omitempty"`
	}
	var got envelope
	if err := decodeMessageJSON([]byte(`{"items":[{"data":"/wA=","count":18446744073709551615}],"optional":"\ud83d\ude00"}`), &got, 1); err != nil {
		t.Fatal(err)
	}
	if len(got.Items) != 1 || string(got.Items[0].Data) != "\xff\x00" || got.Items[0].Count != ^uint64(0) || got.Optional == nil || *got.Optional != "😀" {
		t.Fatalf("wire values changed: %#v", got)
	}
}
func TestMessageJSONRejectsAmbiguousOrIncompleteObjects(t *testing.T) {
	type value struct {
		Name     string  `json:"name"`
		Bytes    []byte  `json:"bytes"`
		Optional *string `json:"optional,omitempty"`
	}
	cases := []string{
		`{}`, `null`, `[]`, `{"name":"x"}`, `{"name":"x","bytes":null}`, `{"name":"x","bytes":[]}`,
		`{"name":"x","bytes":"","unknown":0}`, `{"name":"x","name":"y","bytes":""}`,
		`{"name":"x","bytes":"","optional":null}`, `{"name":"x","bytes":""} {}`,
		`{"name":"\ud800","bytes":""}`, `{"name":"\udc00","bytes":""}`, `{"name":"\ud800\u0041","bytes":""}`,
		`{"name":"\uZZZZ","bytes":""}`, `{"name":"\ud800`, "{\"name\":\"\xff\",\"bytes\":\"\"}",
		`{"name":1,"bytes":""}`, `{"name":"x","bytes":"?"}`, `{"name":{"nested":1},"bytes":""}`,
	}
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			var got value
			if err := decodeMessageJSON([]byte(input), &got, 10); err == nil {
				t.Fatalf("accepted invalid JSON: %s", input)
			}
		})
	}
	var got value
	if err := decodeMessageJSON([]byte(`{"name":"\\ud800","bytes":""}`), &got, 0); err != nil {
		t.Fatalf("literal escape rejected: %v", err)
	}
	if got.Name != `\ud800` {
		t.Fatalf("literal escape changed: %q", got.Name)
	}
}
func TestMessageJSONBoundsArraysAndRequiresWritableDestination(t *testing.T) {
	type value struct {
		Items []int `json:"items"`
	}
	for _, destination := range []any{nil, value{}, (*value)(nil)} {
		if err := decodeMessageJSON([]byte(`{"items":[]}`), destination, 0); err == nil {
			t.Fatalf("accepted destination %#v", destination)
		}
	}
	var got value
	for _, input := range []string{`{"items":[1]}`, `{"items":{}}`, `{"items":[null]}`} {
		if err := decodeMessageJSON([]byte(input), &got, 0); err == nil {
			t.Fatalf("accepted unbounded items %s", input)
		}
	}
	if err := decodeMessageJSON([]byte(`{"items":[]}`), &got, -1); err == nil {
		t.Fatal("accepted negative limit")
	}
	if err := decodeMessageJSON([]byte(`{"items":[]}`), &got, 0); err != nil || got.Items == nil {
		t.Fatalf("empty array must be present: %#v %v", got, err)
	}
}
func TestMessageJSONFlattensEmbeddedFieldsAndChecksTypedScalars(t *testing.T) {
	type Embedded struct {
		ID uint64 `json:"id"`
	}
	type value struct {
		Embedded
		Name string `json:"name"`
	}
	var got value
	sentinel := errors.New("forbidden name")
	check := func(typ reflect.Type, token any) error {
		if typ.Kind() == reflect.String && token == "forbidden" {
			return sentinel
		}
		return nil
	}
	if err := decodeCheckedMessageJSON([]byte(`{"id":7,"name":"allowed"}`), &got, 0, check); err != nil || got.ID != 7 {
		t.Fatalf("embedded identity lost: %#v %v", got, err)
	}
	if err := decodeCheckedMessageJSON([]byte(`{"id":7,"name":"forbidden"}`), &got, 0, check); !errors.Is(err, sentinel) {
		t.Fatalf("scalar rejection lost: %v", err)
	}
	type duplicate struct {
		Embedded
		Other uint64 `json:"id"`
	}
	var collision duplicate
	if err := decodeMessageJSON([]byte(`{"id":1}`), &collision, 0); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous schema accepted: %v", err)
	}
}
