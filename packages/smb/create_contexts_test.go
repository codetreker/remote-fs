package smb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func testCreateContext(name string, size int) wire.CreateContext {
	return wire.CreateContext{Name: []byte(name), Data: make([]byte, size)}
}

func contextField(context wire.CreateContext, offset int, value uint32) wire.CreateContext {
	context.Data = bytes.Clone(context.Data)
	binary.LittleEndian.PutUint32(context.Data[offset:offset+4], value)
	return context
}

func TestValidateCreateContexts(t *testing.T) {
	durableV1 := testCreateContext("DHnQ", 16)
	durableV1.Data = bytes.Repeat([]byte{0xff}, 16)
	durableV2 := testCreateContext("DH2Q", 32)
	durableV2.Data = bytes.Repeat([]byte{0xff}, 32)
	durableV2 = contextField(durableV2, 4, 2)
	leaseV1 := testCreateContext("RqLs", 32)
	leaseV1.Data = bytes.Repeat([]byte{0xff}, 32)
	leaseV1 = contextField(leaseV1, 16, 7)
	leaseV2 := testCreateContext("RqLs", 52)
	leaseV2.Data = bytes.Repeat([]byte{0xff}, 52)
	leaseV2 = contextField(leaseV2, 16, 7)
	leaseV2 = contextField(leaseV2, 20, 4)
	allocation := testCreateContext("AlSi", 8)
	allocation.Data = bytes.Repeat([]byte{0xff}, 8)
	for _, test := range []struct {
		name     string
		contexts []wire.CreateContext
		want     error
	}{
		{"none", nil, nil},
		{"durable v1 reserved ignored", []wire.CreateContext{durableV1}, nil},
		{"durable v2", []wire.CreateContext{testCreateContext("DH2Q", 32)}, nil},
		{"persistent request without grant", []wire.CreateContext{durableV2}, nil},
		{"lease v1 ignored fields", []wire.CreateContext{leaseV1}, nil},
		{"lease v2 ignored fields", []wire.CreateContext{leaseV2}, nil},
		{"allocation ignored", []wire.CreateContext{allocation}, nil},
		{"reserved GUID empty", []wire.CreateContext{testCreateContext(reservedCreateContext, 0)}, nil},
		{"reserved GUID arbitrary data", []wire.CreateContext{testCreateContext(reservedCreateContext, 19)}, nil},
		{"arbitrary order", []wire.CreateContext{allocation, leaseV2, durableV2}, nil},
		{"bad durable v1", []wire.CreateContext{testCreateContext("DHnQ", 15)}, syscall.EINVAL},
		{"bad durable v2", []wire.CreateContext{testCreateContext("DH2Q", 31)}, syscall.EINVAL},
		{"durable v2 unknown flags", []wire.CreateContext{contextField(testCreateContext("DH2Q", 32), 4, 1)}, syscall.EINVAL},
		{"durable v2 combined unknown flags", []wire.CreateContext{contextField(testCreateContext("DH2Q", 32), 4, 6)}, syscall.EINVAL},
		{"bad lease", []wire.CreateContext{testCreateContext("RqLs", 33)}, syscall.EINVAL},
		{"lease v1 unknown state", []wire.CreateContext{contextField(testCreateContext("RqLs", 32), 16, 8)}, syscall.EINVAL},
		{"lease v2 unknown state", []wire.CreateContext{contextField(testCreateContext("RqLs", 52), 16, 8)}, syscall.EINVAL},
		{"lease v2 unknown flags", []wire.CreateContext{contextField(testCreateContext("RqLs", 52), 20, 1)}, syscall.EINVAL},
		{"lease v2 combined unknown flags", []wire.CreateContext{contextField(testCreateContext("RqLs", 52), 20, 6)}, syscall.EINVAL},
		{"bad allocation", []wire.CreateContext{testCreateContext("AlSi", 0)}, syscall.EINVAL},
		{"bad reconnect v1", []wire.CreateContext{testCreateContext("DHnC", 15)}, syscall.EINVAL},
		{"bad reconnect v2", []wire.CreateContext{testCreateContext("DH2C", 35)}, syscall.EINVAL},
		{"reconnect v2 unknown flags", []wire.CreateContext{contextField(testCreateContext("DH2C", 36), 32, 4)}, syscall.EINVAL},
		{"bad maximal access", []wire.CreateContext{testCreateContext("MxAc", 7)}, syscall.EINVAL},
		{"bad disk identity", []wire.CreateContext{testCreateContext("QFid", 1)}, syscall.EINVAL},
		{"duplicate lease", []wire.CreateContext{testCreateContext("RqLs", 32), testCreateContext("RqLs", 52)}, syscall.EINVAL},
		{"duplicate reserved", []wire.CreateContext{testCreateContext(reservedCreateContext, 0), testCreateContext(reservedCreateContext, 0)}, syscall.EINVAL},
		{"duplicate unsupported", []wire.CreateContext{testCreateContext("unknown", 0), testCreateContext("unknown", 0)}, syscall.EINVAL},
		{"mixed durable", []wire.CreateContext{testCreateContext("DHnQ", 16), testCreateContext("DH2Q", 32)}, syscall.EINVAL},
		{"unsupported before malformed", []wire.CreateContext{testCreateContext("unknown", 0), testCreateContext("AlSi", 0)}, syscall.EINVAL},
		{"unsupported before mixed durable", []wire.CreateContext{testCreateContext("unknown", 0), testCreateContext("DHnQ", 16), testCreateContext("DH2Q", 32)}, syscall.EINVAL},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateCreateContexts(wire.CreateRequest{Contexts: test.contexts}); !errors.Is(err, test.want) {
				t.Fatalf("validate = %v, want %v", err, test.want)
			}
		})
	}
}

func TestUnsupportedCreateContextIntents(t *testing.T) {
	for _, context := range []wire.CreateContext{
		testCreateContext("DHnC", 16), testCreateContext("DH2C", 36),
		contextField(testCreateContext("DH2C", 36), 32, 2),
		testCreateContext("MxAc", 0), testCreateContext("MxAc", 8), testCreateContext("QFid", 0),
		testCreateContext("ExtA", 0), testCreateContext("SecD", 0), testCreateContext("TWrp", 0), testCreateContext("unknown", 0),
		testCreateContext("\x45\xbc\xa6\x6a\xef\xa7\xf7\x4a\x90\x08\xfa\x46\x2e\x14\x4d\x74", 0),
		testCreateContext("\xb9\x82\xd0\xb7\x3b\x56\x07\x4f\xa0\x7b\x52\x4a\x81\x16\xa0\x10", 0),
	} {
		t.Run(string(context.Name), func(t *testing.T) {
			contexts := []wire.CreateContext{testCreateContext("AlSi", 8), context}
			if err := validateCreateContexts(wire.CreateRequest{Contexts: contexts}); !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("unsupported intent = %v", err)
			}
		})
	}
}
