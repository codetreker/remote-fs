package smb

import (
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func TestValidateCreateContexts(t *testing.T) {
	context := func(name string, size int) wire.CreateContext {
		return wire.CreateContext{Name: []byte(name), Data: make([]byte, size)}
	}
	for _, test := range []struct {
		name     string
		contexts []wire.CreateContext
		want     error
	}{
		{"none", nil, nil},
		{"durable v1", []wire.CreateContext{context("DHnQ", 16)}, nil},
		{"durable v2", []wire.CreateContext{context("DH2Q", 32)}, nil},
		{"lease v1", []wire.CreateContext{context("RqLs", 32)}, nil},
		{"lease v2", []wire.CreateContext{context("RqLs", 52)}, nil},
		{"allocation may be ignored", []wire.CreateContext{context("AlSi", 8)}, nil},
		{"arbitrary order", []wire.CreateContext{context("AlSi", 8), context("RqLs", 52), context("DH2Q", 32)}, nil},
		{"bad durable v1", []wire.CreateContext{context("DHnQ", 15)}, syscall.EINVAL},
		{"bad durable v2", []wire.CreateContext{context("DH2Q", 31)}, syscall.EINVAL},
		{"bad lease", []wire.CreateContext{context("RqLs", 33)}, syscall.EINVAL},
		{"bad allocation", []wire.CreateContext{context("AlSi", 0)}, syscall.EINVAL},
		{"duplicate", []wire.CreateContext{context("RqLs", 32), context("RqLs", 52)}, syscall.EINVAL},
		{"mixed durable", []wire.CreateContext{context("DHnQ", 16), context("DH2Q", 32)}, syscall.EINVAL},
		{"unsupported", []wire.CreateContext{context("ExtA", 8)}, syscall.EOPNOTSUPP},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateCreateContexts(wire.CreateRequest{Contexts: test.contexts}); !errors.Is(err, test.want) {
				t.Fatalf("validate = %v, want %v", err, test.want)
			}
		})
	}
}

func TestAllocationContextKeepsOrdinaryOpenIntent(t *testing.T) {
	request := wire.CreateRequest{
		DesiredAccess: accessReadData,
		ShareAccess:   7,
		Disposition:   1,
		Contexts:      []wire.CreateContext{{Name: []byte("AlSi"), Data: make([]byte, 8)}},
	}
	intent, err := classifyCreate(request)
	if err != nil || !intent.read || intent.write || intent.create || intent.reset {
		t.Fatalf("ordinary open with AlSi = %+v, %v", intent, err)
	}
	request.Contexts[0].Data = nil
	if _, err := classifyCreate(request); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("malformed AlSi = %v", err)
	}
}
