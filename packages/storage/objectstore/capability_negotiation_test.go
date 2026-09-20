package objectstore

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type missingCapabilityAuthority struct{ metastore.FileStore }

type refusingCapabilityAuthority struct {
	metastore.FileStore
	fileStoreErr error
	err          error
}

func (a *refusingCapabilityAuthority) CheckFileStore() error      { return a.fileStoreErr }
func (a *refusingCapabilityAuthority) CheckMetadataAccess() error { return a.err }
func (a *refusingCapabilityAuthority) SetMetadata(context.Context, uint64, string, []byte, []byte) (storage.OpaquePayload, error) {
	panic("capability check must refuse before dispatch")
}
func (a *refusingCapabilityAuthority) CheckUseOwners() error    { return a.err }
func (a *refusingCapabilityAuthority) CheckRangeControl() error { return a.err }

func TestOptionalCapabilityChecksRequireTheCompleteNativeAuthority(t *testing.T) {
	missing := &fileSession{native: &missingCapabilityAuthority{}}
	for name, check := range map[string]func() error{
		"metadata": missing.CheckMetadataAccess,
		"owners":   missing.CheckUseOwners,
		"ranges":   missing.CheckRangeControl,
	} {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("missing %s capability=%v", name, err)
		}
	}

	refusal := errors.New("native capability refused")
	brokenStore := &fileSession{native: &refusingCapabilityAuthority{fileStoreErr: refusal, err: errors.New("capability check should not run")}}
	for name, check := range map[string]func() error{
		"metadata": brokenStore.CheckMetadataAccess,
		"owners":   brokenStore.CheckUseOwners,
		"ranges":   brokenStore.CheckRangeControl,
	} {
		if err := check(); !errors.Is(err, refusal) {
			t.Fatalf("broken file store behind %s capability=%v", name, err)
		}
	}

	rejected := &fileSession{native: &refusingCapabilityAuthority{err: refusal}}
	for name, check := range map[string]func() error{
		"metadata": rejected.CheckMetadataAccess,
		"owners":   rejected.CheckUseOwners,
		"ranges":   rejected.CheckRangeControl,
	} {
		if err := check(); !errors.Is(err, refusal) {
			t.Fatalf("refused %s capability=%v", name, err)
		}
	}
}

var (
	_ metadataAuthority = (*refusingCapabilityAuthority)(nil)
	_ useOwnerAuthority = (*refusingCapabilityAuthority)(nil)
	_ rangeAuthority    = (*refusingCapabilityAuthority)(nil)
)
