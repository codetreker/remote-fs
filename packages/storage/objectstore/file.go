package objectstore

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type openFile struct {
	session     *fileSession
	native      metastore.File
	active      bool
	closed      bool
	operations  int
	idle        chan struct{}
	closePermit chan struct{}
}

var _ storage.File = (*openFile)(nil)

func (f *openFile) Reference() storage.FileReferenceID { return f.native.Reference() }
func (f *openFile) NodeID() uint64                     { return f.native.NodeID() }

func (f *openFile) begin(ctx context.Context, class fileOperationClass) (context.Context, func(), error) {
	ctx, done, err := f.session.begin(ctx, class)
	if err != nil {
		return nil, nil, err
	}
	f.session.mu.Lock()
	if !f.active {
		f.session.mu.Unlock()
		done()
		return nil, nil, syscall.EBADF
	}
	if f.operations == 0 {
		f.idle = make(chan struct{})
	}
	f.operations++
	f.session.mu.Unlock()
	return ctx, func() {
		f.session.mu.Lock()
		f.operations--
		if f.operations == 0 {
			close(f.idle)
		}
		f.session.mu.Unlock()
		done()
	}, nil
}

func (f *openFile) Stat(ctx context.Context, options storage.ObservationOptions) (storage.FileObservation, error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileObservation{}, err
	}
	defer done()
	return f.native.Stat(ctx, options)
}

func (f *openFile) ListAt(ctx context.Context, request storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.DirectoryPage{}, err
	}
	defer done()
	return f.native.ListAt(ctx, request)
}

func (f *openFile) SetAttr(ctx context.Context, request storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return f.native.SetAttr(ctx, request, id)
}

func (f *openFile) SetKind(ctx context.Context, request storage.SetKindRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return f.native.SetKind(ctx, request, id)
}

func (f *openFile) Rename(ctx context.Context, request storage.RenameRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	result, err := f.native.Rename(ctx, request, id)
	f.session.storage.sweepAfterMutation()
	return result, err
}

func (f *openFile) ReplaceClaim(ctx context.Context, claim storage.AccessClaim, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileControlOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return f.native.ReplaceClaim(ctx, claim, id)
}

func (f *openFile) PrepareRemoval(ctx context.Context, request storage.PrepareRemovalRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return f.native.PrepareRemoval(ctx, request, id)
}

func (f *openFile) DrainEntry(ctx context.Context, request storage.DrainEntryRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return f.native.DrainEntry(ctx, request, id)
}

func (f *openFile) CancelDrain(ctx context.Context, request storage.CancelDrainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileCleanupOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return f.native.CancelDrain(ctx, request, id)
}

func (f *openFile) ReplaceRanges(ctx context.Context, request storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileRangeOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return f.native.ReplaceRanges(ctx, request, id)
}

func (f *openFile) WaitRanges(ctx context.Context, request storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileWaitOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return f.native.WaitRanges(ctx, request, id)
}

func (f *openFile) RangeSnapshot(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope) (storage.RangeSnapshot, error) {
	ctx, done, err := f.begin(ctx, fileRangeOperation)
	if err != nil {
		return storage.RangeSnapshot{}, err
	}
	defer done()
	return f.native.RangeSnapshot(ctx, owner, scope)
}

func (f *openFile) Close(ctx context.Context, id storage.FileActionID) (receipt storage.FileActionReceipt, err error) {
	if _, err := id.Epoch(); err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	f.session.mu.Lock()
	closed := f.closed || f.session.closed
	f.session.mu.Unlock()
	if closed {
		return storage.FileActionReceipt{Operation: storage.OpFileClose, State: storage.FileActionRetired, Reference: f.Reference()}, nil
	}
	ctx, done, err := f.session.begin(ctx, fileCleanupOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	select {
	case <-f.closePermit:
		defer func() { f.closePermit <- struct{}{} }()
	case <-ctx.Done():
		return storage.FileActionReceipt{}, beforeFileAdmission(ctx.Err())
	}
	admitted, proceed, err := f.native.BeginClose(ctx, id)
	if !proceed || err != nil {
		if err == nil && admitted.Reference == f.Reference() && admitted.Errno == 0 && (admitted.State == storage.FileActionRetired || admitted.State == storage.FileActionCompleted && admitted.Effects&storage.EffectReferenceRetired != 0) {
			f.session.mu.Lock()
			if !f.active {
				f.closed = true
				delete(f.session.files, f.Reference())
			}
			f.session.mu.Unlock()
		}
		return admitted, err
	}
	defer func() { err = afterFileAdmission(err) }()
	f.session.mu.Lock()
	f.active = false
	idle := f.idle
	f.session.mu.Unlock()
	select {
	case <-idle:
	case <-ctx.Done():
		return admitted, ctx.Err()
	}
	cleanup, cancel := f.session.cleanupOperation(ctx)
	stop := context.AfterFunc(ctx, cancel)
	result, err := f.native.Close(cleanup, id)
	stop()
	cancel()
	if (result.State == storage.FileActionCompleted || result.State == storage.FileActionRetired) && result.Errno == 0 {
		f.session.mu.Lock()
		f.closed = true
		delete(f.session.files, f.Reference())
		f.session.mu.Unlock()
		f.session.storage.sweepAfterMutation()
	}
	return result, err
}

func (f *openFile) CheckObservation(ctx context.Context, condition storage.ObservationCondition) (storage.FileObservation, error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.FileObservation{}, err
	}
	defer done()
	return f.native.CheckObservation(ctx, condition)
}

func (f *openFile) CancelPrepared(ctx context.Context, intent storage.RemovalIntentID, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileCleanupOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return f.native.CancelPrepared(ctx, intent, id)
}

func (f *openFile) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope, id storage.FileActionID) (storage.FileActionReceipt, error) {
	ctx, done, err := f.begin(ctx, fileCleanupOperation)
	if err != nil {
		return storage.FileActionReceipt{}, beforeFileAdmission(err)
	}
	defer done()
	return f.native.RetireRangeOwner(ctx, owner, scope, id)
}

func (f *openFile) LookupAt(ctx context.Context, name []byte) (storage.EntryLookup, error) {
	ctx, done, err := f.begin(ctx, fileDataOperation)
	if err != nil {
		return storage.EntryLookup{}, err
	}
	defer done()
	return f.native.LookupAt(ctx, name)
}
