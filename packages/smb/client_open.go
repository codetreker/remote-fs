package smb

import (
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/authz"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func windowsClaim(intent windowsOpenIntent, kind storage.NodeKind) storage.AccessClaim {
	if intent.Access&(windowsReadData|windowsWriteData|windowsAppendData|windowsDelete) == 0 {
		return storage.AccessClaim{}
	}
	var claim storage.AccessClaim
	if kind == storage.NodeRegular {
		if intent.Access&windowsReadData != 0 {
			claim.Uses |= storage.ReadContent
		}
		if intent.Access&(windowsWriteData|windowsAppendData) != 0 {
			claim.Uses |= storage.WriteContent
		}
		if intent.Share&windowsShareRead == 0 {
			claim.Excludes |= storage.ReadContent
		}
		if intent.Share&windowsShareWrite == 0 {
			claim.Excludes |= storage.WriteContent
		}
	}
	if intent.Access&windowsDelete != 0 {
		claim.Uses |= storage.RemoveEntry
	}
	if intent.Share&windowsShareDelete == 0 {
		claim.Excludes |= storage.RemoveEntry
	}
	return claim
}
func (s *clientSession) Open(ctx context.Context, request windowsOpenRequest, id windowsActionID) (windowsOpenResult, error) {
	actual := id
	for attempt := 0; attempt < 8; attempt++ {
		result, err := s.openAttempt(ctx, request, id, actual)
		if errors.Is(err, syscall.EIO) || storage.ErrnoOf(err) != syscall.EAGAIN || result.File != nil {
			return result, err
		}
		a := s.action(id)
		if a != nil && !a.retryable {
			return result, err
		}
		if attempt == 7 {
			return result, err
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if a != nil {
			next, e := s.next(ctx)
			if e != nil {
				return result, e
			}
			actual = next
		}
	}
	panic("bounded open loop")
}
func (s *clientSession) openAttempt(ctx context.Context, request windowsOpenRequest, original, id windowsActionID) (windowsOpenResult, error) {
	if err := request.windowsOpenIntent.Check(); err != nil {
		return windowsOpenResult{}, err
	}
	if !validDOSAttributes(request.DOSAttributes) {
		return windowsOpenResult{}, syscall.EINVAL
	}
	var target storage.EntryTarget
	var observed storage.FileObservation
	var exists bool
	var err error
	root := request.Lookup.ParentID == 0
	if root {
		if err = request.Lookup.Check(); err != nil {
			return windowsOpenResult{}, err
		}
		if err = s.authorize(ctx, storage.OpFileStatNode, 0, storage.AccessClaim{}, 0, s.state.RootID, 0, 0); err != nil {
			return windowsOpenResult{}, err
		}
		observed, err = s.raw.StatNode(ctx, s.state.RootID, storage.ObservationOptions{IncludeLocation: true})
		if err != nil {
			return windowsOpenResult{}, err
		}
		exists = true
		if observed.Location == nil || observed.Location.State != storage.LocationRoot {
			return windowsOpenResult{}, syscall.EIO
		}
	} else {
		target, observed, exists, err = s.lookup(ctx, request.Lookup)
		if err != nil {
			return windowsOpenResult{}, err
		}
	}
	disposition := request.Disposition
	redirect := exists && observed.Attr.Kind == storage.NodeSymlink && !request.OpenReparsePoint
	if redirect {
		disposition = windowsOpen
	}
	if exists && disposition == windowsCreate {
		return windowsOpenResult{}, syscall.EEXIST
	}
	if !exists && (disposition == windowsOpen || disposition == windowsOverwrite) {
		return windowsOpenResult{}, syscall.ENOENT
	}
	kind := storage.NodeRegular
	if request.Kind == windowsDirectory {
		kind = storage.NodeDirectory
	}
	if exists {
		kind = observed.Attr.Kind
	}
	var existing windowsAttr
	if exists {
		existing, err = projectWindowsAttr(observed, s.backend.defaults)
		if err != nil {
			return windowsOpenResult{}, err
		}
	}
	directory := kind == storage.NodeDirectory || kind == storage.NodeSymlink && existing.DOSAttributes&dosDirectory != 0
	if exists && !redirect && request.Kind == windowsDirectory && !directory {
		return windowsOpenResult{}, syscall.ENOTDIR
	}
	if exists && !redirect && request.Kind == windowsRegularFile && directory {
		return windowsOpenResult{}, syscall.EISDIR
	}
	mutating := exists && !redirect && (disposition == windowsSupersede || disposition == windowsOverwrite || disposition == windowsOverwriteIf)
	if mutating && directory {
		return windowsOpenResult{}, syscall.EISDIR
	}
	if mutating && kind == storage.NodeSymlink && disposition != windowsSupersede {
		return windowsOpenResult{}, syscall.EINVAL
	}
	if root && (mutating || request.DeleteOnClose) {
		return windowsOpenResult{}, syscall.EACCES
	}
	if exists && !redirect && !directory && existing.DOSAttributes&dosReadOnly != 0 && (mutating || request.DeleteOnClose || request.Access&(windowsWriteData|windowsAppendData) != 0) {
		return windowsOpenResult{}, syscall.EACCES
	}
	if !exists && kind != storage.NodeDirectory && request.DeleteOnClose && request.DOSAttributes&dosReadOnly != 0 {
		return windowsOpenResult{}, syscall.EACCES
	}
	if mutating {
		if existing.DOSAttributes&(dosHidden|dosSystem)&^request.DOSAttributes != 0 {
			return windowsOpenResult{}, syscall.EACCES
		}
		request.DOSAttributes = (request.DOSAttributes &^ dosNormal) | dosArchive
		if disposition == windowsSupersede {
			kind = storage.NodeRegular
		}
	}
	if mutating && !request.MaximumAllowed && disposition != windowsSupersede && request.Access&windowsWriteData == 0 {
		return windowsOpenResult{}, syscall.EACCES
	}
	if mutating && !request.MaximumAllowed && disposition == windowsSupersede && request.Access&windowsDelete == 0 {
		return windowsOpenResult{}, syscall.EACCES
	}
	claim := windowsClaim(request.windowsOpenIntent, kind)
	if redirect {
		claim = storage.AccessClaim{}
	}
	var prepared *storage.RemovalCondition
	if request.DeleteOnClose && !redirect {
		condition := storage.RemovalFile
		if kind == storage.NodeDirectory {
			condition = storage.RemovalIfEmpty
		}
		prepared = &condition
	}
	action := clientAction{redirect: redirect, access: request.Access, create: windowsOpened}
	op := storage.OpFileRetainAt
	effects := storage.EffectRetained
	if root {
		op = storage.OpFileRetain
	}
	if !exists {
		op = storage.OpFileCreateAndRetainAt
		effects |= storage.EffectCreated
		action.create = windowsCreated
	}
	if mutating {
		op = storage.OpFileResetAndRetainAt
		effects |= storage.EffectContentChanged | storage.EffectMetadataChanged
		action.create = windowsOverwritten
		if disposition == windowsSupersede {
			op = storage.OpFileReplaceAndRetainAt
			effects |= storage.EffectCreated | storage.EffectEntryDetached
			action.create = windowsSuperseded
		}
	}
	effects = fileEffects(op)
	if prepared != nil {
		effects |= storage.EffectPreparedChanged
	}
	if request.MaximumAllowed && !redirect {
		granted, e := maximumWindowsAccess(ctx, request.windowsOpenIntent, kind, func(candidate windowsOpenIntent) error {
			if exists {
				attr, e := projectWindowsAttr(observed, s.backend.defaults)
				if e != nil {
					return e
				}
				if attr.DOSAttributes&dosReadOnly != 0 && !directory && candidate.Access&(windowsWriteData|windowsAppendData) != 0 {
					return authz.ErrDenied
				}
			}
			return s.authorize(ctx, op, effects, windowsClaim(candidate, kind), 0, observed.Attr.ID, target.Parent, 0)
		}, func(candidateOp storage.Operation) error {
			return s.authorize(ctx, candidateOp, fileEffects(candidateOp), storage.AccessClaim{}, 0, observed.Attr.ID, target.Parent, 0)
		})
		if e != nil {
			return windowsOpenResult{}, e
		}
		request.Access = granted
		request.MaximumAllowed = false
		action.access = granted
		claim = windowsClaim(request.windowsOpenIntent, kind)
	}
	if err = s.authorize(ctx, op, effects, claim, 0, observed.Attr.ID, target.Parent, 0); err != nil {
		return windowsOpenResult{}, err
	}
	var metadata storage.Metadata
	if op == storage.OpFileCreateAndRetainAt || op == storage.OpFileReplaceAndRetainAt || op == storage.OpFileResetAndRetainAt {
		payload, e := encodeWindowsMetadata(windowsMetadata{Attributes: request.DOSAttributes})
		if e != nil {
			return windowsOpenResult{}, e
		}
		var base storage.Metadata
		if op == storage.OpFileResetAndRetainAt {
			base = observed.Attr.Metadata
		}
		metadata, e = base.With(storage.OpaqueMetadata{Key: windowsMetadataKey, Version: windowsMetadataVersion, Data: payload})
		if e != nil {
			return windowsOpenResult{}, e
		}
	}
	if err = s.rememberOpen(original, id, action); err != nil {
		return windowsOpenResult{}, err
	}
	var receipt storage.FileActionReceipt
	switch op {
	case storage.OpFileRetain:
		receipt, err = s.raw.Retain(ctx, storage.RetainRequest{NodeID: s.state.RootID, ExpectedMetadataRevision: observed.Attr.MetadataRevision, Claim: claim, Witness: observed.Location}, id)
	case storage.OpFileRetainAt:
		receipt, err = s.raw.RetainAt(ctx, storage.RetainAtRequest{Target: target, Claim: claim, Prepared: prepared}, id)
	case storage.OpFileCreateAndRetainAt, storage.OpFileReplaceAndRetainAt:
		create := storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: kind, Metadata: metadata}, Claim: claim, Prepared: prepared}
		if op == storage.OpFileCreateAndRetainAt {
			receipt, err = s.raw.CreateAndRetainAt(ctx, create, id)
		} else {
			receipt, err = s.raw.ReplaceAndRetainAt(ctx, create, id)
		}
	case storage.OpFileResetAndRetainAt:
		receipt, err = s.raw.ResetAndRetainAt(ctx, storage.ResetAndRetainRequest{Target: target, ExpectedRevision: observed.Attr.MetadataRevision, Change: storage.AttrChange{ExpectedRevision: observed.Attr.MetadataRevision, Metadata: &metadata}, Claim: claim, Prepared: prepared}, id)
	}
	if storage.IsFileCallNotAdmitted(err) {
		s.forgetAction(original, id)
		return windowsOpenResult{notAdmitted: true}, err
	}
	if receipt.Action != id && receipt.State != storage.FileActionUnknown {
		err = errors.Join(syscall.EIO, err)
	}
	s.recordOpen(original, receipt)
	result, err := s.project(ctx, receipt, err, &action)
	opened := windowsOpenResult{GrantedAccess: result.GrantedAccess, proof: result.proof, File: result.File, Attr: result.Attr, CreateAction: result.CreateAction}
	if err != nil {
		if opened.File != nil && receipt.State != storage.FileActionUnknown && receipt.State != storage.FileActionPending {
			if closeErr := s.closeTemporary(ctx, opened.File.(*clientFile).raw); closeErr != nil {
				return opened, errors.Join(err, closeErr)
			}
			opened.File = nil
		}
		return opened, err
	}
	return opened, nil
}
