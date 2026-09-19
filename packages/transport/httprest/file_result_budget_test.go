package httprest

import (
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestAttributeResultBudgetIncludesMetadataResidency(t *testing.T) {
	handler := &Handler{maxBodyBytes: 1500}
	request := fileRequest{Op: storage.OpFileStat, ResultBytes: 1500}
	budget := handler.attrResultBudget(request)
	scalar := storage.Attr{ID: 7, Kind: storage.NodeRegular}
	if err := budget(scalar, 6); err != nil {
		t.Fatalf("empty metadata should fit small response: %v", err)
	}
	if err := budget(scalar, 166); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("small payload with many namespace allocations: %v", err)
	}
	if err := budget(scalar, storage.MaxMetadataBytes); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("maximum metadata: %v", err)
	}
}
