package sqlite

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"reflect"
	"syscall"
	"time"
)

const maxFingerprintDepth = 64

type fingerprintEncoder struct {
	hash    hash.Hash
	scratch [256]byte
}

// The hash representation follows stored time facts: location and monotonic
// readings are not persisted and cannot change an action's identity.
func fileFingerprint(value any) ([32]byte, error) {
	encoder := fingerprintEncoder{hash: sha256.New()}
	if err := encoder.value(reflect.ValueOf(value), 0); err != nil {
		return [32]byte{}, fileNotAdmitted(err)
	}
	var result [32]byte
	copy(result[:], encoder.hash.Sum(result[:0]))
	return result, nil
}

func (e *fingerprintEncoder) number(value uint64) {
	binary.BigEndian.PutUint64(e.scratch[:8], value)
	e.hash.Write(e.scratch[:8])
}
func (e *fingerprintEncoder) text(value string) {
	e.number(uint64(len(value)))
	for len(value) > 0 {
		n := copy(e.scratch[:], value)
		e.hash.Write(e.scratch[:n])
		value = value[n:]
	}
}

func (e *fingerprintEncoder) value(value reflect.Value, depth int) error {
	if depth > maxFingerprintDepth {
		return fmt.Errorf("file action nesting exceeds its bound: %w", syscall.EINVAL)
	}
	if !value.IsValid() {
		e.number(0)
		return nil
	}
	if !value.CanInterface() {
		return fmt.Errorf("file action contains an inaccessible value: %w", syscall.EINVAL)
	}
	kind := value.Kind()
	e.number(uint64(kind) + 1)
	e.text(value.Type().PkgPath())
	e.text(value.Type().String())
	if value.Type() == reflect.TypeFor[time.Time]() {
		instant := value.Interface().(time.Time)
		e.number(uint64(instant.Unix()))
		e.number(uint64(instant.Nanosecond()))
		return nil
	}
	switch kind {
	case reflect.Bool:
		if value.Bool() {
			e.number(1)
		} else {
			e.number(0)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		e.number(uint64(value.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		e.number(value.Uint())
	case reflect.String:
		e.text(value.String())
	case reflect.Pointer, reflect.Interface:
		if value.IsNil() {
			e.number(0)
			return nil
		}
		e.number(1)
		return e.value(value.Elem(), depth+1)
	case reflect.Slice, reflect.Array:
		if kind == reflect.Slice {
			if value.IsNil() {
				e.number(0)
				return nil
			}
			e.number(1)
		}
		e.number(uint64(value.Len()))
		if kind == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8 {
			e.hash.Write(value.Bytes())
			return nil
		}
		for i := 0; i < value.Len(); i++ {
			if err := e.value(value.Index(i), depth+1); err != nil {
				return err
			}
		}
	case reflect.Struct:
		e.number(uint64(value.NumField()))
		for i := 0; i < value.NumField(); i++ {
			e.text(value.Type().Field(i).Name)
			if err := e.value(value.Field(i), depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("file action contains unsupported %s: %w", kind, syscall.EINVAL)
	}
	return nil
}
