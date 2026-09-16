package replicated

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type retainedFile struct {
	session *fileSession
	remote  httprest.FileWithBarrier
}

func fileCall[T any](ctx context.Context, session *fileSession, ordinary bool, call func(context.Context) (T, error)) (T, error) {
	ctx, done, err := session.begin(ctx, ordinary)
	if err != nil {
		var zero T
		return zero, err
	}
	defer done()
	return call(ctx)
}

func (f *retainedFile) Reference() storage.FileReferenceID { return f.remote.Reference() }
func (f *retainedFile) Stat(ctx context.Context, options storage.ObservationOptions) (storage.FileObservation, error) {
	return fileCall(ctx, f.session, true, func(ctx context.Context) (storage.FileObservation, error) { return f.remote.Stat(ctx, options) })
}
func (f *retainedFile) ReadAt(ctx context.Context, req storage.FileReadRequest) (storage.FileRead, error) {
	return fileCall(ctx, f.session, true, func(ctx context.Context) (storage.FileRead, error) { return f.remote.ReadAt(ctx, req) })
}
func (f *retainedFile) ListAt(ctx context.Context, req storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	return fileCall(ctx, f.session, true, func(ctx context.Context) (storage.DirectoryPage, error) { return f.remote.ListAt(ctx, req) })
}
func (f *retainedFile) RangeSnapshot(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope) (storage.RangeSnapshot, error) {
	return fileCall(ctx, f.session, false, func(ctx context.Context) (storage.RangeSnapshot, error) {
		return f.remote.RangeSnapshot(ctx, owner, scope)
	})
}
func (f *retainedFile) Sync(ctx context.Context) error {
	_, err := fileCall(ctx, f.session, true, func(ctx context.Context) (struct{}, error) { return struct{}{}, f.remote.Sync(ctx) })
	return err
}

func (f *retainedFile) WriteAt(ctx context.Context, req storage.FileWriteRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.session.perform(ctx, "WriteAt", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return f.remote.WriteAtWithBarrier(ctx, req, action)
	})
}

func (f *retainedFile) Truncate(ctx context.Context, req storage.FileTruncateRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.session.perform(ctx, "Truncate", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return f.remote.TruncateWithBarrier(ctx, req, action)
	})
}

func (f *retainedFile) SetAttr(ctx context.Context, req storage.AttrChange, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.session.perform(ctx, "SetAttr", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return f.remote.SetAttrWithBarrier(ctx, req, action)
	})
}

func (f *retainedFile) SetKind(ctx context.Context, req storage.SetKindRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.session.perform(ctx, "SetKind", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return f.remote.SetKindWithBarrier(ctx, req, action)
	})
}

func (f *retainedFile) Rename(ctx context.Context, req storage.RenameRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.session.perform(ctx, "Rename", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return f.remote.RenameWithBarrier(ctx, req, action)
	})
}

func (f *retainedFile) PrepareRemoval(ctx context.Context, req storage.PrepareRemovalRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.session.perform(ctx, "PrepareRemoval", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return f.remote.PrepareRemovalWithBarrier(ctx, req, action)
	})
}

func (f *retainedFile) DrainEntry(ctx context.Context, req storage.DrainEntryRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.session.perform(ctx, "DrainEntry", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return f.remote.DrainEntryWithBarrier(ctx, req, action)
	})
}

func (f *retainedFile) CancelDrain(ctx context.Context, req storage.CancelDrainRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.session.perform(ctx, "CancelDrain", true, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return f.remote.CancelDrainWithBarrier(ctx, req, action)
	})
}

func (f *retainedFile) ReplaceClaim(ctx context.Context, req storage.AccessClaim, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileCall(ctx, f.session, false, func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.remote.ReplaceClaim(ctx, req, action)
	})
}

func (f *retainedFile) ReplaceRanges(ctx context.Context, req storage.RangeReplaceRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileCall(ctx, f.session, false, func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.remote.ReplaceRanges(ctx, req, action)
	})
}

func (f *retainedFile) WaitRanges(ctx context.Context, req storage.RangeWaitRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileCall(ctx, f.session, false, func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.remote.WaitRanges(ctx, req, action)
	})
}

func (f *retainedFile) Close(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.session.perform(ctx, "close-file", false, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return f.remote.CloseWithBarrier(ctx, action)
	})
}

func (f *retainedFile) CheckObservation(ctx context.Context, condition storage.ObservationCondition) (storage.FileObservation, error) {
	return fileCall(ctx, f.session, true, func(ctx context.Context) (storage.FileObservation, error) {
		return f.remote.CheckObservation(ctx, condition)
	})
}
func (f *retainedFile) CancelPrepared(ctx context.Context, intent storage.RemovalIntentID, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.session.perform(ctx, "cancel-prepared", false, func(ctx context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return f.remote.CancelPreparedWithBarrier(ctx, intent, action)
	})
}
func (f *retainedFile) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return fileCall(ctx, f.session, false, func(ctx context.Context) (storage.FileActionReceipt, error) {
		return f.remote.RetireRangeOwner(ctx, owner, scope, action)
	})
}

func (f *retainedFile) LookupAt(ctx context.Context, name []byte) (storage.EntryLookup, error) {
	return fileCall(ctx, f.session, true, func(ctx context.Context) (storage.EntryLookup, error) { return f.remote.LookupAt(ctx, name) })
}

func (f *retainedFile) NodeID() uint64 { return f.remote.NodeID() }
