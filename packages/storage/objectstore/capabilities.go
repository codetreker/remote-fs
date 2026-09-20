package objectstore

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.AtomicFileOpener        = (*fileSession)(nil)
	_ storage.NamespaceAccess         = (*fileSession)(nil)
	_ storage.DirectoryReader         = (*fileSession)(nil)
	_ storage.NodeReferences          = (*fileSession)(nil)
	_ storage.MetadataAccess          = (*fileSession)(nil)
	_ storage.ScopedReference         = (*openFile)(nil)
	_ storage.ReferenceMetadataAccess = (*openFile)(nil)
)

func (fs *fileSession) CheckAtomicFileOpen() error {
	if err := fs.native.CheckFileStore(); err != nil {
		return err
	}
	native, ok := fs.native.(metastore.AtomicFileOpener)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckAtomicFileOpen()
}

func (fs *fileSession) CheckNodeReferences() error {
	if err := fs.native.CheckFileStore(); err != nil {
		return err
	}
	native, ok := fs.native.(metastore.NodeReferences)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckNodeReferences()
}

func (fs *fileSession) CheckNamespaceAccess() error {
	if err := fs.native.CheckFileStore(); err != nil {
		return err
	}
	native, ok := fs.native.(metastore.NamespaceAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckNamespaceAccess()
}

func (fs *fileSession) CheckDirectoryRead() error {
	if err := fs.native.CheckFileStore(); err != nil {
		return err
	}
	native, ok := fs.native.(metastore.DirectoryReader)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckDirectoryRead()
}

func (fs *fileSession) CheckMetadataAccess() error {
	native, ok := fs.native.(metadataAuthority)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := native.CheckFileStore(); err != nil {
		return err
	}
	return native.CheckMetadataAccess()
}

func (fs *fileSession) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	if err := fs.CheckAtomicFileOpen(); err != nil {
		return storage.OpenResult{}, err
	}
	if err := name.Check(); err != nil {
		return storage.OpenResult{}, err
	}
	if err := options.Check(); err != nil {
		return storage.OpenResult{}, err
	}
	input := openAtActionInput{Name: name, Options: options}
	return runFileAction(ctx, fs, options.Action, storage.OpFileOpenAt, input, cloneOpenResult,
		func(result storage.OpenResult) bool {
			return result.File != nil || result.Attr.ID != 0 || result.Outcome != 0
		},
		func() (storage.OpenResult, error) { return fs.openAt(ctx, name, options) })
}

func (fs *fileSession) openAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
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
	if result.File == nil {
		return opened, fs.finishOpen(nil, err)
	}
	file := &openFile{
		session: fs,
		native:  result.File,
		uses:    referenceUses{nodeID: uint64(result.State.ID)},
		options: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: options.Read, Write: options.Write}},
		active:  true,
	}
	opened.File = file
	err = fs.finishOpen(file, err)
	return opened, err
}

func (fs *fileSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if err := fs.CheckNodeReferences(); err != nil {
		return storage.NodeOpenResult{}, err
	}
	if err := options.Check(); err != nil {
		return storage.NodeOpenResult{}, err
	}
	input := nodeRefActionInput{Node: id, Options: options}
	return runFileAction(ctx, fs, options.Action, storage.OpFileOpenNodeRef, input, cloneNodeOpenResult,
		func(result storage.NodeOpenResult) bool {
			return result.Reference != nil || result.Attr.ID != 0 || result.Outcome != 0
		},
		func() (storage.NodeOpenResult, error) {
			return fs.openNodeReference(ctx, func(ctx context.Context) (metastore.NodeOpenResult, error) {
				return fs.native.(metastore.NodeReferences).OpenNodeRef(ctx, id, options)
			})
		})
}

func (fs *fileSession) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if err := fs.CheckNodeReferences(); err != nil {
		return storage.NodeOpenResult{}, err
	}
	if err := name.Check(); err != nil {
		return storage.NodeOpenResult{}, err
	}
	if err := options.Check(); err != nil {
		return storage.NodeOpenResult{}, err
	}
	input := nodeRefActionInput{Name: &name, Options: options}
	return runFileAction(ctx, fs, options.Action, storage.OpFileOpenChildRef, input, cloneNodeOpenResult,
		func(result storage.NodeOpenResult) bool {
			return result.Reference != nil || result.Attr.ID != 0 || result.Outcome != 0
		},
		func() (storage.NodeOpenResult, error) {
			return fs.openNodeReference(ctx, func(ctx context.Context) (metastore.NodeOpenResult, error) {
				return fs.native.(metastore.NodeReferences).OpenChildRef(ctx, name, options)
			})
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
	if result.Reference == nil {
		return opened, fs.finishOpen(nil, err)
	}
	reference := &nodeReference{session: fs, native: result.Reference, uses: referenceUses{nodeID: uint64(result.State.ID)}, active: true}
	opened.Reference = reference
	err = fs.finishOpen(reference, err)
	return opened, err
}

func (fs *fileSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	if err := fs.CheckDirectoryRead(); err != nil {
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

func (fs *fileSession) ReadDirNode(ctx context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	if err := fs.CheckDirectoryRead(); err != nil {
		return storage.ObservedDirectory{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.begin(ctx, true)
	if err != nil {
		return storage.ObservedDirectory{}, err
	}
	defer done()
	observed, err := fs.native.(metastore.DirectoryReader).ReadDirNode(ctx, target)
	if err != nil {
		return storage.ObservedDirectory{}, err
	}
	if err := observed.Check(); err != nil {
		return storage.ObservedDirectory{}, err
	}
	if observed.Observation.ParentID != target.NodeID {
		return storage.ObservedDirectory{}, syscall.EIO
	}
	return observed.Clone(), nil
}

func (fs *fileSession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (observation storage.DirectoryObservation, err error) {
	if result == nil {
		return observation, syscall.EINVAL
	}
	defer func() {
		if err != nil {
			result.Fail(err)
			observation = storage.DirectoryObservation{}
		}
	}()
	if err := fs.CheckDirectoryRead(); err != nil {
		return observation, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.begin(ctx, true)
	if err != nil {
		return observation, err
	}
	defer done()
	observation, err = fs.native.(metastore.DirectoryReader).ReadDirNodeBounded(ctx, target, result)
	if err != nil {
		return observation, err
	}
	if err := observation.Check(); err != nil {
		return observation, err
	}
	if observation.ParentID != target.NodeID {
		return observation, syscall.EIO
	}
	return observation.Clone(), nil
}

func (fs *fileSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	if err := fs.CheckNamespaceAccess(); err != nil {
		return storage.NameResult{}, err
	}
	if err := command.Check(); err != nil {
		return storage.NameResult{}, err
	}
	return runFileAction(ctx, fs, command.Action, storage.OpFileMutateName, command, cloneNameResult,
		func(result storage.NameResult) bool { return result.Attr != nil },
		func() (storage.NameResult, error) { return fs.mutateName(ctx, command) })
}

func (fs *fileSession) mutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
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
	return fs.native.(metadataAuthority).SetMetadata(ctx, id, namespace, expected, data)
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

func (f *openFile) CheckReferenceState() error {
	capability, ok := any(f.native).(metastore.ReferenceStateAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckReferenceState()
}

func (f *openFile) State(ctx context.Context) (storage.ReferenceState, error) {
	if err := f.CheckReferenceState(); err != nil {
		return storage.ReferenceState{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	state, err := any(f.native).(metastore.ReferenceStateAccess).State(ctx)
	return publicReferenceState(state), err
}

func (f *openFile) CheckDeleteIntent() error {
	capability, ok := any(f.native).(metastore.DeleteIntent)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckDeleteIntent()
}

func (f *openFile) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	if err := f.CheckDeleteIntent(); err != nil {
		return storage.ReferenceState{}, err
	}
	if err := command.Check(); err != nil {
		return storage.ReferenceState{}, err
	}
	target, err := f.session.referenceActionTarget(ctx, f)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	input := pendingUnlinkActionInput{Target: target, Command: command}
	return runFileAction(ctx, f.session, command.Action, storage.OpFileSetPendingUnlink, input, cloneReferenceState,
		func(result storage.ReferenceState) bool { return result.Attr.ID != 0 },
		func() (storage.ReferenceState, error) { return f.setPendingUnlink(ctx, command) })
}

func (f *openFile) setPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	state, err := any(f.native).(metastore.DeleteIntent).SetPendingUnlink(ctx, command)
	return publicReferenceState(state), err
}

func (f *openFile) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	if err := f.CheckDeleteIntent(); err != nil {
		return storage.ReferenceState{}, err
	}
	if err := command.Check(); err != nil {
		return storage.ReferenceState{}, err
	}
	target, err := f.session.referenceActionTarget(ctx, f)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	input := clearPendingUnlinkActionInput{Target: target, Command: command}
	return runFileAction(ctx, f.session, command.Action, storage.OpFileClearPendingUnlink, input, cloneReferenceState,
		func(result storage.ReferenceState) bool { return result.Attr.ID != 0 },
		func() (storage.ReferenceState, error) { return f.clearPendingUnlink(ctx, command) })
}

func (f *openFile) clearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	state, err := any(f.native).(metastore.DeleteIntent).ClearPendingUnlink(ctx, command)
	return publicReferenceState(state), err
}

var (
	_ storage.ReferenceStateAccess = (*openFile)(nil)
	_ storage.DeleteIntent         = (*openFile)(nil)
)
