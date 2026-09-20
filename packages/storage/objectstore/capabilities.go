package objectstore

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.MetadataAccess          = (*fileSession)(nil)
	_ storage.ScopedReference         = (*openFile)(nil)
	_ storage.ReferenceMetadataAccess = (*openFile)(nil)
)

func (fs *fileSession) CheckMetadataAccess() error {
	native, ok := fs.native.(storage.MetadataAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckMetadataAccess()
}

func (fs *fileSession) SetMetadata(ctx context.Context, id uint64, namespace string, expected, data []byte) (storage.OpaquePayload, error) {
	if err := fs.CheckMetadataAccess(); err != nil {
		return storage.OpaquePayload{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	ctx = metastore.WithFilePublicationGuard(ctx, fs.publicationAllowed)
	done, err := fs.begin(ctx, true)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	defer done()
	return fs.native.(storage.MetadataAccess).SetMetadata(ctx, id, namespace, expected, data)
}

func (f *openFile) CheckScopedReference() error { return f.native.CheckScopedReference() }

func (f *openFile) Scope(ctx context.Context) (storage.UseScope, error) {
	if err := f.CheckScopedReference(); err != nil {
		return storage.UseScope{}, err
	}
	ctx, done, err := f.admit(ctx, fileAdvisoryOperation)
	if err != nil {
		return storage.UseScope{}, err
	}
	defer done()
	scope, err := f.native.Scope(ctx)
	if err != nil {
		return storage.UseScope{}, err
	}
	if err := scope.Check(); err != nil {
		return storage.UseScope{}, err
	}
	f.session.mu.Lock()
	if f.uses.nodeID == 0 {
		f.session.mu.Unlock()
		state, err := f.native.Node(ctx)
		if err != nil {
			return storage.UseScope{}, err
		}
		f.session.mu.Lock()
		f.uses.nodeID = uint64(state.ID)
	}
	f.uses.scope = scope
	f.session.mu.Unlock()
	return scope, nil
}

func (f *openFile) CheckMetadataAccess() error { return f.native.CheckMetadataAccess() }

func (f *openFile) SetMetadata(ctx context.Context, namespace string, expected, data []byte) (storage.OpaquePayload, error) {
	if err := f.CheckMetadataAccess(); err != nil {
		return storage.OpaquePayload{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	defer done()
	return f.native.SetMetadata(ctx, namespace, expected, data)
}
