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

	"github.com/codetreker/remote-fs/packages/storage"
)

func decodeWindowsJSON(data []byte, target any) error {
	if !utf8.Valid(data) {
		return errors.New("Windows JSON must be UTF-8")
	}
	typ := reflect.TypeOf(target)
	if typ == nil || typ.Kind() != reflect.Pointer || reflect.ValueOf(target).IsNil() {
		return errors.New("Windows JSON destination must be a pointer")
	}
	if err := checkWindowsJSONEscapes(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := checkWindowsJSON(decoder, typ.Elem()); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("Windows JSON has trailing content")
	}
	return json.Unmarshal(data, target)
}

func checkWindowsJSON(decoder *json.Decoder, typ reflect.Type) error {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return errors.New("Windows JSON member cannot be null")
	}
	switch typ.Kind() {
	case reflect.Struct:
		if token != json.Delim('{') {
			return errors.New("Windows JSON requires an object")
		}
		fields := make(map[string]reflect.StructField, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() {
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "" {
				name = field.Name
			}
			if name != "-" {
				fields[name] = field
			}
		}
		seen := make(map[string]bool, len(fields))
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("invalid Windows JSON key")
			}
			field, ok := fields[name]
			if !ok || seen[name] {
				return errors.New("Windows JSON has an unknown or duplicate member")
			}
			seen[name] = true
			if err := checkWindowsJSON(decoder, field.Type); err != nil {
				return err
			}
		}
		for name, field := range fields {
			if !seen[name] && !strings.Contains(field.Tag.Get("json"), ",omitempty") {
				return errors.New("Windows JSON is missing a required member")
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("Windows JSON object is incomplete")
		}
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			if _, ok := token.(string); !ok {
				return errors.New("Windows bytes require base64 text")
			}
			return nil
		}
		if token != json.Delim('[') {
			return errors.New("Windows JSON requires an array")
		}
		count := 0
		for decoder.More() {
			count++
			if typ == reflect.TypeOf([]storage.WindowsLockRange{}) && count > storage.WindowsMaxLockBatch {
				return errors.New("Windows lock batch exceeds its element bound")
			}
			if err := checkWindowsJSON(decoder, typ.Elem()); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("Windows JSON array is incomplete")
		}
	default:
		if _, nested := token.(json.Delim); nested {
			return errors.New("Windows scalar has an invalid type")
		}
	}
	return nil
}

func checkWindowsJSONEscapes(data []byte) error {
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
			return errors.New("incomplete Windows JSON escape")
		}
		if data[index] != 'u' {
			continue
		}
		if index+4 >= len(data) {
			return errors.New("incomplete Windows JSON Unicode escape")
		}
		value, err := strconv.ParseUint(string(data[index+1:index+5]), 16, 16)
		if err != nil {
			return errors.New("invalid Windows JSON Unicode escape")
		}
		index += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return errors.New("unpaired Windows JSON low surrogate")
		}
		if value < 0xd800 || value > 0xdbff {
			continue
		}
		if index+6 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
			return errors.New("unpaired Windows JSON high surrogate")
		}
		low, err := strconv.ParseUint(string(data[index+3:index+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return errors.New("unpaired Windows JSON high surrogate")
		}
		index += 6
	}
	return nil
}
