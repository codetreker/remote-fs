package sqlite

import (
	"context"
	"encoding/binary"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.OpenContentMetadata = (*Store)(nil)

func (s *Store) CheckOpenContentMetadata() error { return s.CheckFileStore() }

// The caller holds commit ordering so retirement cannot interleave with the
// observation or with the final content publication.
func (f *retainedFile) checkContentReference(ctx context.Context) error {
	if !f.write {
		return syscall.EBADF
	}
	if err := f.check(); err != nil {
		return err
	}
	if err := f.store.checkFileOwnership(); err != nil {
		return err
	}
	if _, err := f.store.resolveUseScope(ctx, f.scope, uint64(f.id), storage.WriteData); err != nil {
		return err
	}
	return metastore.CheckFilePublication(ctx)
}

func checkContentMetadataValue(value *storage.OpaquePayload) error {
	if value != nil && (len(value.Version) != 8 || binary.BigEndian.Uint64(value.Version) == 0) {
		return syscall.EIO
	}
	return nil
}
