package limited

import (
	"context"
	"errors"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type allocationReferenceProbe struct {
	storage.NodeReference
	attr storage.Attr
	err  error
}

func (p allocationReferenceProbe) Stat(context.Context) (storage.Attr, error) {
	return p.attr, p.err
}

func TestNodeReferenceStatMasksBackingAllocationEvenOnPartialError(t *testing.T) {
	cause := errors.New("stat completed with a later failure")
	probe := allocationReferenceProbe{attr: storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 1, AllocationKnown: true, AllocationSize: 4096}, err: cause}
	reference := wrapNodeReference(&Storage{}, probe)
	got, err := reference.Stat(t.Context())
	if !errors.Is(err, cause) || got.ID != 7 || got.Size != 1 || got.AllocationKnown || got.AllocationSize != 0 {
		t.Fatalf("masked partial reference stat = %+v, %v", got, err)
	}
}
