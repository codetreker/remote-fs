package objectstore_test

import (
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/storage/storagetest"
)

func TestRetainedMetadataContract(t *testing.T) {
	storagetest.RunMetadata(t, func(t *testing.T) storage.Storage {
		volume, _ := fileVolume(t, memory.New(), 0, nil)
		return volume
	})
}
