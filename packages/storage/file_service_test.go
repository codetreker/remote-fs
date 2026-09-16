package storage_test

import (
	"errors"
	"math"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileServiceLimitsRemainExplicitAndCoverBothContentBuffers(t *testing.T) {
	options := storage.DefaultFileServiceOptions()
	if err := options.Check(); err != nil {
		t.Fatal(err)
	}
	if options.MaxSessions != 1024 || options.MaxRanges != 262144 || options.MaxMaterializedBytes != 2<<30 || options.MaxFileBytes != 1<<30 {
		t.Fatal("service defaults changed")
	}
	if err := (storage.FileServiceOptions{}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	for _, change := range []func(*storage.FileServiceOptions){
		func(o *storage.FileServiceOptions) { o.MaxSnapshotRanges = storage.MaxRangeSnapshotRanges + 1 },
		func(o *storage.FileServiceOptions) { o.MaxSessions = 0 }, func(o *storage.FileServiceOptions) { o.MaxClaims = math.MaxInt }, func(o *storage.FileServiceOptions) { o.MaxRanges = -1 },
		func(o *storage.FileServiceOptions) { o.MaxWaits = -1 }, func(o *storage.FileServiceOptions) { o.MaxWaits = math.MaxInt }, func(o *storage.FileServiceOptions) { o.MaxDependencies = -1 }, func(o *storage.FileServiceOptions) { o.MaxDependencies = math.MaxInt },
		func(o *storage.FileServiceOptions) { o.MaxOwnerRanges = o.MaxRanges + 1 }, func(o *storage.FileServiceOptions) { o.MaxSetRanges = o.MaxOwnerRanges + 1 },
		func(o *storage.FileServiceOptions) { o.MaxMaterializedBytes = 0 }, func(o *storage.FileServiceOptions) { o.MaxFileBytes = 0 }, func(o *storage.FileServiceOptions) { o.MaxFileBytes = o.MaxMaterializedBytes/2 + 1 }, func(o *storage.FileServiceOptions) { o.FileOperationTimeout = 0 },
	} {
		bad := options
		change(&bad)
		if err := bad.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid limits %+v: %v", bad, err)
		}
	}
	options.MaxWaits = 0
	options.MaxDependencies = 0
	if err := options.Check(); err != nil {
		t.Fatal("disabled waits/dependencies rejected")
	}
}
