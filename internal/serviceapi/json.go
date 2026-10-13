package serviceapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

// DecodeJSON rejects duplicate keys before decoding, including duplicates with
// identical values, so every accepted body has one unambiguous interpretation.
func DecodeJSON(data []byte, target any) error {
	if !utf8.Valid(data) {
		return errors.New("service JSON contains invalid UTF-8")
	}
	if err := checkJSONUnicode(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := checkJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after JSON value")
	}
	shapeDecoder := json.NewDecoder(bytes.NewReader(data))
	shapeDecoder.UseNumber()
	var shape any
	if err := shapeDecoder.Decode(&shape); err != nil {
		return err
	}
	if err := checkJSONShape(shape, reflect.TypeOf(target)); err != nil {
		return err
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

var jsonUnmarshaler = reflect.TypeFor[json.Unmarshaler]()

func checkJSONShape(value any, target reflect.Type) error {
	if target == nil {
		return errors.New("JSON target is required")
	}
	for target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	if reflect.PointerTo(target).Implements(jsonUnmarshaler) {
		return nil
	}
	switch target.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return errors.New("service value must be a JSON object")
		}
		fields := make(map[string]reflect.Type)
		for i := 0; i < target.NumField(); i++ {
			field := target.Field(i)
			if !field.IsExported() {
				continue
			}
			tag := strings.Split(field.Tag.Get("json"), ",")
			if tag[0] == "-" {
				continue
			}
			name := tag[0]
			if name == "" {
				name = field.Name
			}
			fields[name] = field.Type
			optional := false
			for _, option := range tag[1:] {
				if option == "omitempty" {
					optional = true
				}
			}
			if _, exists := object[name]; !exists && !optional {
				return errors.New("missing service JSON field")
			}
		}
		for name, child := range object {
			field, ok := fields[name]
			if !ok {
				return errors.New("unknown service JSON field")
			}
			if child == nil {
				if field.Kind() != reflect.Pointer && field.Kind() != reflect.Slice {
					return errors.New("null service scalar field")
				}
				continue
			}
			if err := checkJSONShape(child, field); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		if value == nil {
			return nil
		}
		array, ok := value.([]any)
		if !ok {
			return errors.New("service value must be a JSON array")
		}
		for _, child := range array {
			if err := checkJSONShape(child, target.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkJSONUnicode(data []byte) error {
	quoted := false
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted || data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) {
			return errors.New("truncated JSON escape")
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return errors.New("truncated Unicode escape")
		}
		unit, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return errors.New("invalid Unicode escape")
		}
		i += 4
		if unit >= 0xdc00 && unit <= 0xdfff {
			return errors.New("unpaired Unicode surrogate")
		}
		if unit < 0xd800 || unit > 0xdbff {
			continue
		}
		if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
			return errors.New("unpaired Unicode surrogate")
		}
		low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return errors.New("unpaired Unicode surrogate")
		}
		i += 6
	}
	return nil
}

func checkJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("JSON nesting exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil && depth == 0 {
		return errors.New("null is not an accepted service value")
	}
	delimiter, container := token.(json.Delim)
	if !container {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return errors.New("invalid JSON object key")
			}
			if _, exists := seen[key]; exists {
				return errors.New("duplicate JSON object key")
			}
			seen[key] = struct{}{}
			if err := checkJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := checkJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	end, err := decoder.Token()
	if err != nil {
		return err
	}
	if (delimiter == '{' && end != json.Delim('}')) || (delimiter == '[' && end != json.Delim(']')) {
		return errors.New("invalid JSON container")
	}
	return nil
}

func validErrorCode(code string) bool {
	switch code {
	case "authentication_unavailable", "authentication_required", "access_denied", "invalid_request", "authentication_capacity", "not_found", "condition_conflict", "acceptance_expired", "history_expired", "resource_capacity", "storage_unavailable", "volume_unavailable", "metadata_unavailable":
		return true
	default:
		return false
	}
}
