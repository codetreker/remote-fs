package objectstore

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.AtomicFileOpener = (*fileSession)(nil)
var _ storage.NodeReferences = (*fileSession)(nil)
var _ storage.NamespaceAccess = (*fileSession)(nil)
var _ storage.MetadataAccess = (*fileSession)(nil)

func (fs *fileSession) CheckAtomicFileOpen() error {
	native, ok := fs.native.(metastore.AtomicFileOpener)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckAtomicFileOpen()
}

func (fs *fileSession) CheckNodeReferences() error {
	native, ok := fs.native.(metastore.NodeReferences)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckNodeReferences()
}

func (fs *fileSession) CheckNamespaceAccess() error {
	native, ok := fs.native.(metastore.NamespaceAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckNamespaceAccess()
}

func (fs *fileSession) CheckMetadataAccess() error {
	native, ok := fs.native.(storage.MetadataAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckMetadataAccess()
}

func (fs *fileSession) beginOpen(ctx context.Context) (func(), error) {
	done, err := fs.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	fs.mu.Lock()
	if len(fs.files)+fs.opening >= fs.options.MaxFiles {
		fs.mu.Unlock()
		done()
		return nil, syscall.EMFILE
	}
	fs.opening++
	fs.mu.Unlock()
	return done, nil
}

func (fs *fileSession) finishOpen(reference retainedReference, err error) error {
	fs.mu.Lock()
	fs.opening--
	if reference != nil {
		fs.files[reference] = struct{}{}
	}
	active := fs.active && time.Now().Before(fs.expires)
	fs.mu.Unlock()
	if reference != nil && !active {
		return errors.Join(err, syscall.ESTALE, reference.retire())
	}
	if reference == nil && err == nil {
		return syscall.EIO
	}
	return err
}

func (fs *fileSession) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	if err := fs.CheckAtomicFileOpen(); err != nil {
		return storage.OpenResult{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	ctx = metastore.WithFilePublicationGuard(ctx, fs.publicationAllowed)
	done, err := fs.beginOpen(ctx)
	if err != nil {
		return storage.OpenResult{}, err
	}
	defer done()
	result, err := fs.native.(metastore.AtomicFileOpener).OpenAt(ctx, name, options)
	opened := storage.OpenResult{Attr: result.State.Attr(), Outcome: result.Outcome}
	if result.File != nil {
		f := &openFile{session: fs, native: result.File, active: true, uses: referenceUses{nodeID: uint64(result.State.ID)},
			options: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: options.Read, Write: options.Write}}}
		opened.File = f
		err = fs.finishOpen(f, err)
	} else {
		err = fs.finishOpen(nil, err)
	}
	return opened, err
}

func (fs *fileSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if err := fs.CheckNodeReferences(); err != nil {
		return storage.NodeOpenResult{}, err
	}
	return fs.openNodeReference(ctx, func(ctx context.Context) (metastore.NodeOpenResult, error) {
		return fs.native.(metastore.NodeReferences).OpenNodeRef(ctx, id, options)
	})
}

func (fs *fileSession) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if err := fs.CheckNodeReferences(); err != nil {
		return storage.NodeOpenResult{}, err
	}
	return fs.openNodeReference(ctx, func(ctx context.Context) (metastore.NodeOpenResult, error) {
		return fs.native.(metastore.NodeReferences).OpenChildRef(ctx, name, options)
	})
}

func (fs *fileSession) openNodeReference(ctx context.Context, open func(context.Context) (metastore.NodeOpenResult, error)) (storage.NodeOpenResult, error) {
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	ctx = metastore.WithFilePublicationGuard(ctx, fs.publicationAllowed)
	done, err := fs.beginOpen(ctx)
	if err != nil {
		return storage.NodeOpenResult{}, err
	}
	defer done()
	result, err := open(ctx)
	opened := storage.NodeOpenResult{Attr: result.State.Attr(), Outcome: result.Outcome}
	if result.Reference != nil {
		ref := &nodeReference{session: fs, native: result.Reference, active: true, uses: referenceUses{nodeID: uint64(result.State.ID)}}
		opened.Reference = ref
		err = fs.finishOpen(ref, err)
	} else {
		err = fs.finishOpen(nil, err)
	}
	return opened, err
}

func (fs *fileSession) ReadDirNode(ctx context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	if err := fs.CheckNamespaceAccess(); err != nil {
		return storage.ObservedDirectory{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.begin(ctx, true)
	if err != nil {
		return storage.ObservedDirectory{}, err
	}
	defer done()
	return fs.native.(metastore.NamespaceAccess).ReadDirNode(ctx, target)
}

func (fs *fileSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	if err := fs.CheckNamespaceAccess(); err != nil {
		return storage.Attr{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.begin(ctx, true)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	return fs.native.(metastore.NamespaceAccess).LookupAt(ctx, name)
}

func (fs *fileSession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (observation storage.DirectoryObservation, err error) {
	if result == nil {
		return observation, syscall.EINVAL
	}
	defer func() {
		if err != nil {
			result.Fail(err)
		}
	}()
	if err := fs.CheckNamespaceAccess(); err != nil {
		return observation, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.begin(ctx, true)
	if err != nil {
		return observation, err
	}
	defer done()
	return fs.native.(metastore.NamespaceAccess).ReadDirNodeBounded(ctx, target, result)
}

func (fs *fileSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	if err := fs.CheckNamespaceAccess(); err != nil {
		return storage.NameResult{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	ctx = metastore.WithFilePublicationGuard(ctx, fs.publicationAllowed)
	done, err := fs.begin(ctx, true)
	if err != nil {
		return storage.NameResult{}, err
	}
	defer done()
	result, err := fs.native.(metastore.NamespaceAccess).MutateName(ctx, command)
	fs.storage.sweepAfterMutation()
	return result, err
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
