package storage_test

import (
	"errors"
	"math"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestReferenceInformationRejectsSubstitutionAndContradictoryFacts(t *testing.T) {
	valid := storage.ReferenceInformation{NodeID: 7, Kind: storage.NodeRegular, Size: 3, AllocationKnown: true, AllocationSize: 4096}
	for _, fact := range []storage.ReferenceInformation{
		valid,
		{NodeID: 7, Kind: storage.NodeDirectory, Detached: true, PendingUnlink: true},
		{NodeID: 7, Kind: storage.NodeDirectory, Size: -1},
		{NodeID: 7, Kind: storage.NodeSymlink, Size: 8},
	} {
		if err := fact.Check(7); err != nil {
			t.Fatalf("valid scalar facts = %+v, %v", fact, err)
		}
	}
	for _, fact := range []storage.ReferenceInformation{
		{},
		{NodeID: 8, Kind: storage.NodeRegular},
		{NodeID: 7, Kind: 0},
		{NodeID: 7, Kind: storage.NodeRegular, Size: -1},
		{NodeID: 7, Kind: storage.NodeRegular, AllocationSize: 1},
		{NodeID: 7, Kind: storage.NodeRegular, AllocationKnown: true, AllocationSize: -1},
	} {
		if err := fact.Check(7); !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid scalar facts = %+v, %v", fact, err)
		}
	}
	if err := valid.Check(0); !errors.Is(err, syscall.EIO) {
		t.Fatalf("zero expected identity = %v", err)
	}
}

func TestVolumeGeometryIsBoundToTrustedVolumeAndMultipliesWithoutWrapping(t *testing.T) {
	for _, geometry := range []storage.VolumeGeometry{
		{Volume: "db/root", BytesPerSector: 4096, SectorsPerAllocationUnit: 1},
		{Volume: "db/root", BytesPerSector: math.MaxUint32, SectorsPerAllocationUnit: math.MaxUint32},
	} {
		if err := geometry.CheckVolume("db/root"); err != nil {
			t.Fatal(err)
		}
		want := uint64(geometry.BytesPerSector) * uint64(geometry.SectorsPerAllocationUnit)
		if got, err := geometry.AllocationUnitBytes(); err != nil || got != want {
			t.Fatalf("allocation unit = %d, %v; want %d", got, err, want)
		}
		if err := geometry.CheckVolume("other/root"); !errors.Is(err, syscall.EIO) {
			t.Fatalf("substituted volume accepted: %v", err)
		}
		if err := geometry.CheckVolume(""); !errors.Is(err, syscall.EIO) {
			t.Fatalf("unknown expected volume accepted: %v", err)
		}
	}
	for _, invalid := range []storage.VolumeGeometry{
		{},
		{Volume: "not canonical", BytesPerSector: 4096, SectorsPerAllocationUnit: 1},
		{Volume: "db/root", BytesPerSector: 0, SectorsPerAllocationUnit: 1},
		{Volume: "db/root", BytesPerSector: 4096, SectorsPerAllocationUnit: 0},
	} {
		if err := invalid.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid geometry accepted: %v", err)
		}
		if got, err := invalid.AllocationUnitBytes(); got != 0 || !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid allocation unit = %d, %v", got, err)
		}
	}
}
