package fuse

import (
	"context"
	"errors"
	"path"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type openOptions struct {
	Read, Write, Create, Exclusive, Truncate bool
	ExpectedID                               uint64
	Metadata                                 storage.Metadata
}

func (o openOptions) claim() storage.AccessClaim {
	var uses storage.AccessUse
	if o.Read {
		uses |= storage.ReadContent
	}
	if o.Write {
		uses |= storage.WriteContent
	}
	return storage.AccessClaim{Uses: uses}
}

func (v *volume) retainNode(ctx context.Context, id uint64, claim storage.AccessClaim) (storage.File, storage.Attr, error) {
	receipt, err := v.fileAction(ctx, storage.OpFileRetain, func(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
		return v.files.Retain(ctx, storage.RetainRequest{NodeID: id, Claim: claim}, action)
	})
	file, attr, err := v.retainedResult(ctx, receipt, err)
	if err == nil && attr.ID != id {
		return nil, storage.Attr{}, v.closeUnreturnedFile(ctx, file, syscall.EIO, false)
	}
	return file, attr, err
}

func (v *volume) retainedResult(ctx context.Context, receipt storage.FileActionReceipt, cause error) (storage.File, storage.Attr, error) {
	if receipt.Reference == 0 || receipt.Effects&storage.EffectRetained == 0 {
		if cause != nil {
			return nil, storage.Attr{}, cause
		}
		err := syscall.EIO
		v.fence(err)
		return nil, storage.Attr{}, err
	}
	cleanup, cancel := v.cleanupContext(ctx)
	defer cancel()
	file, err := v.files.Reference(cleanup, receipt.Reference)
	if err != nil {
		err = errors.Join(cause, err, syscall.EIO)
		v.fence(err)
		return nil, storage.Attr{}, err
	}
	if cause != nil {
		return nil, storage.Attr{}, v.closeUnreturnedFile(ctx, file, cause, receipt.Effects & ^storage.EffectRetained != 0)
	}
	if err := ctx.Err(); err != nil {
		return nil, storage.Attr{}, v.closeUnreturnedFile(ctx, file, err, receipt.Effects & ^storage.EffectRetained != 0)
	}
	return file, receipt.Observation.Attr, nil
}

func (v *volume) entryTarget(ctx context.Context, parent storage.File, parentID uint64, name string) (storage.EntryTarget, storage.Attr, error) {
	lookup, err := parent.LookupAt(ctx, []byte(name))
	if err != nil {
		return storage.EntryTarget{}, storage.Attr{}, err
	}
	if err := lookup.Check([]byte(name)); err != nil {
		return storage.EntryTarget{}, storage.Attr{}, err
	}
	if lookup.ParentID != parentID {
		return storage.EntryTarget{}, storage.Attr{}, syscall.EIO
	}
	target := storage.EntryTarget{Parent: parent.Reference(), ParentID: parentID, Name: []byte(name), DirectoryRevision: lookup.DirectoryRevision}
	if lookup.Found {
		target.ExpectedEntryID, target.ExpectedNodeID = lookup.EntryID, lookup.Attr.ID
		target.ExpectedMetadataRevision = lookup.Attr.MetadataRevision
	}
	return target, lookup.Attr, nil
}

func (n *node) openNamed(ctx context.Context, name string, options openOptions, kind storage.NodeKind) (storage.File, storage.Attr, error) {
	leaf := path.Base(name)
	parentID := n.id.node
	if !options.Create {
		recordedName, parentInode := n.Parent()
		if parentInode == nil {
			return nil, storage.Attr{}, syscall.ESTALE
		}
		parentNode, ok := parentInode.Operations().(*node)
		if !ok || recordedName != leaf {
			return nil, storage.Attr{}, syscall.EIO
		}
		parentID = parentNode.id.node
	}
	parent, _, err := n.volume.retainNode(ctx, parentID, storage.AccessClaim{})
	if err != nil {
		return nil, storage.Attr{}, err
	}
	file, attr, err := n.openAt(ctx, parent, parentID, leaf, options, kind)
	cleanup := n.volume.closeUnreturnedFile(ctx, parent, err, file != nil)
	if cleanup != nil {
		if file != nil {
			cleanup = n.volume.closeUnreturnedFile(ctx, file, cleanup, true)
		}
		return nil, storage.Attr{}, cleanup
	}
	return file, attr, nil
}

func (n *node) openAt(ctx context.Context, parent storage.File, parentID uint64, name string, options openOptions, kind storage.NodeKind) (storage.File, storage.Attr, error) {
	for range 8 {
		target, existing, err := n.volume.entryTarget(ctx, parent, parentID, name)
		if err != nil {
			return nil, storage.Attr{}, err
		}
		if options.ExpectedID != 0 && target.ExpectedNodeID != options.ExpectedID {
			return nil, storage.Attr{}, syscall.ESTALE
		}
		if target.ExpectedNodeID == 0 && !options.Create {
			return nil, storage.Attr{}, syscall.ENOENT
		}
		if target.ExpectedNodeID != 0 && options.Exclusive {
			return nil, storage.Attr{}, syscall.EEXIST
		}
		if target.ExpectedNodeID != 0 {
			if existing.Kind == storage.NodeDirectory {
				return nil, storage.Attr{}, syscall.EISDIR
			}
			if existing.Kind == storage.NodeSymlink {
				return nil, storage.Attr{}, syscall.ELOOP
			}
			if existing.Kind != storage.NodeRegular {
				return nil, storage.Attr{}, syscall.EIO
			}
			if _, _, err := n.volume.attributes(existing); err != nil {
				return nil, storage.Attr{}, err
			}
		}
		op := storage.OpFileRetainAt
		if target.ExpectedNodeID == 0 {
			op = storage.OpFileCreateAndRetainAt
		} else if options.Truncate {
			op = storage.OpFileResetAndRetainAt
		}
		receipt, err := n.volume.fileAction(ctx, op, func(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
			if target.ExpectedNodeID == 0 {
				return n.volume.files.CreateAndRetainAt(ctx, storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: kind, Metadata: options.Metadata}, Claim: options.claim()}, action)
			}
			if options.Truncate {
				return n.volume.files.ResetAndRetainAt(ctx, storage.ResetAndRetainRequest{Target: target, ExpectedRevision: existing.MetadataRevision, Claim: options.claim()}, action)
			}
			return n.volume.files.RetainAt(ctx, storage.RetainAtRequest{Target: target, Claim: options.claim()}, action)
		})
		if err != nil && receipt.State == storage.FileActionNotApplied && receipt.Conflict != nil && receipt.Conflict.Kind == storage.ConflictRevision && errors.Is(err, syscall.EAGAIN) {
			continue
		}
		file, attr, err := n.volume.retainedResult(ctx, receipt, err)
		if err == nil && (attr.ID == 0 || target.ExpectedNodeID != 0 && attr.ID != target.ExpectedNodeID) {
			return nil, storage.Attr{}, n.volume.closeUnreturnedFile(ctx, file, syscall.EIO, receipt.Effects & ^storage.EffectRetained != 0)
		}
		return file, attr, err
	}
	return nil, storage.Attr{}, syscall.EAGAIN
}

func (v *volume) statNode(ctx context.Context, id uint64) (storage.Attr, error) {
	observation, err := v.files.StatNode(ctx, id, storage.ObservationOptions{})
	return observation.Attr, err
}
