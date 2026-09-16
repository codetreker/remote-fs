package httprest

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

func decodeMessageJSON(data []byte, target any, maxArrayElements int) error {
	return decodeCheckedMessageJSON(data, target, maxArrayElements, nil)
}

type jsonScalarCheck func(reflect.Type, any) error

func decodeCheckedMessageJSON(data []byte, target any, maxArrayElements int, scalarCheck jsonScalarCheck) error {
	if maxArrayElements < 0 {
		return errors.New("JSON array limit cannot be negative")
	}
	if !utf8.Valid(data) {
		return errors.New("JSON must be UTF-8")
	}
	typ := reflect.TypeOf(target)
	if typ == nil || typ.Kind() != reflect.Pointer || reflect.ValueOf(target).IsNil() {
		return errors.New("JSON destination must be a pointer")
	}
	if err := checkJSONEscapes(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := checkMessageJSON(decoder, typ.Elem(), maxArrayElements, scalarCheck); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("JSON has trailing content")
	}
	return json.Unmarshal(data, target)
}

func checkMessageJSON(decoder *json.Decoder, typ reflect.Type, maxArrayElements int, scalarCheck jsonScalarCheck) error {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return errors.New("JSON member cannot be null")
	}
	if scalarCheck != nil {
		if err := scalarCheck(typ, token); err != nil {
			return err
		}
	}
	switch typ.Kind() {
	case reflect.Struct:
		if token != json.Delim('{') {
			return errors.New("JSON requires an object")
		}
		fields := make(map[string]reflect.StructField, typ.NumField())
		if err := collectMessageJSONFields(typ, fields); err != nil {
			return err
		}
		seen := make(map[string]bool, len(fields))
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("invalid JSON key")
			}
			field, ok := fields[name]
			if !ok || seen[name] {
				return errors.New("JSON has an unknown or duplicate member")
			}
			seen[name] = true
			if err := checkMessageJSON(decoder, field.Type, maxArrayElements, scalarCheck); err != nil {
				return err
			}
		}
		for name, field := range fields {
			if !seen[name] && !strings.Contains(field.Tag.Get("json"), ",omitempty") {
				return errors.New("JSON is missing a required member")
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("JSON object is incomplete")
		}
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			if _, ok := token.(string); !ok {
				return errors.New("byte payloads require base64 text")
			}
			return nil
		}
		if token != json.Delim('[') {
			return errors.New("JSON requires an array")
		}
		count := 0
		for decoder.More() {
			count++
			if count > maxArrayElements {
				return errors.New("JSON array exceeds its element bound")
			}
			if err := checkMessageJSON(decoder, typ.Elem(), maxArrayElements, scalarCheck); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("JSON array is incomplete")
		}
	default:
		if _, nested := token.(json.Delim); nested {
			return errors.New("JSON scalar has an invalid type")
		}
	}
	return nil
}

func checkJSONEscapes(data []byte) error {
	quoted := false
	for index := 0; index < len(data); index++ {
		if data[index] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted || data[index] != '\\' {
			continue
		}
		index++
		if index >= len(data) {
			return errors.New("incomplete JSON escape")
		}
		if data[index] != 'u' {
			continue
		}
		if index+4 >= len(data) {
			return errors.New("incomplete JSON Unicode escape")
		}
		value, err := strconv.ParseUint(string(data[index+1:index+5]), 16, 16)
		if err != nil {
			return errors.New("invalid JSON Unicode escape")
		}
		index += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return errors.New("unpaired JSON low surrogate")
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if index+6 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
			return errors.New("unpaired JSON high surrogate")
		}
		low, err := strconv.ParseUint(string(data[index+3:index+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return errors.New("unpaired JSON high surrogate")
		}
		index += 6
	}
	return nil
}

func collectMessageJSONFields(typ reflect.Type, fields map[string]reflect.StructField) error {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "-" || !field.IsExported() {
			continue
		}
		if field.Anonymous && field.Type.Kind() == reflect.Struct && name == "" {
			if err := collectMessageJSONFields(field.Type, fields); err != nil {
				return err
			}
			continue
		}
		if name == "" {
			name = field.Name
		}
		if _, exists := fields[name]; exists {
			return errors.New("JSON schema has ambiguous members")
		}
		fields[name] = field
	}
	return nil
}
