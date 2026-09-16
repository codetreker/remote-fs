package smb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type clientBackend struct {
	stateMu   sync.Mutex
	identity  windowsState
	failed    bool
	source    storage.FileStorage
	limits    Limits
	defaults  windowsMetadata
	authorize authz.Authorizer
	volume    string
}

func (b *clientBackend) Space(ctx context.Context) (storage.Space, error) {
	if err := b.authorize.Authorize(ctx, authz.AccessRequest{Volume: b.volume, Operation: storage.OpVolumeSpace}); err != nil {
		return storage.Space{}, err
	}
	return b.source.Space(ctx)
}
func (b *clientBackend) Check() error { return b.source.CheckFileStorage() }
func (b *clientBackend) State(ctx context.Context) (windowsState, error) {
	s, err := b.source.FileState(ctx)
	if err != nil {
		return windowsState{}, err
	}
	if s.VolumeIdentity == "" || s.RootID == 0 || s.MaxEventBytes <= 0 {
		return windowsState{}, syscall.EIO
	}
	sum := sha256.Sum256([]byte(s.VolumeIdentity))
	state := windowsState{VolumeIdentity: s.VolumeIdentity, VolumeSerial: binary.LittleEndian.Uint64(sum[:8]), RootID: s.RootID, MaxEventBytes: s.MaxEventBytes}
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.failed {
		return windowsState{}, syscall.EIO
	}
	if b.identity.VolumeIdentity != "" && (b.identity.VolumeIdentity != state.VolumeIdentity || b.identity.RootID != state.RootID || b.identity.MaxEventBytes != state.MaxEventBytes) {
		b.failed = true
		return windowsState{}, syscall.EIO
	}
	b.identity = state
	return state, nil
}
func (b *clientBackend) NewSession(ctx context.Context, options storage.FileSessionOptions) (windowsSession, error) {
	state, err := b.State(ctx)
	if err != nil {
		return nil, err
	}
	raw, status, err := b.source.NewFileSession(ctx, options)
	if raw == nil {
		if err == nil {
			err = syscall.EIO
		}
		return nil, err
	}
	s := &clientSession{backend: b, raw: raw, state: state, options: options, status: status, actions: make(map[windowsActionID]*clientAction)}
	s.principal, _ = PrincipalFromContext(ctx)
	return s, err
}

type clientSession struct {
	closeMu    sync.Mutex
	retirement func(context.Context) error
	principal  Principal
	backend    *clientBackend
	raw        storage.FileSession
	state      windowsState
	options    storage.FileSessionOptions
	mu         sync.Mutex
	status     storage.FileSessionStatus
	actions    map[windowsActionID]*clientAction
	planBytes  int64
	closeID    storage.FileActionID
	closed     bool
}
type clientAction struct {
	appendOnly  bool
	actual      storage.FileActionID
	retryable   bool
	terminal    bool
	expires     time.Time
	rangeAction *clientRangeAction
	redirect    bool
	access      windowsAccess
	create      windowsCreateAction
	applied     int
	finalErr    error
}
type clientFile struct {
	rangeMu     sync.Mutex
	nextRangeID storage.RangeAcquisitionID
	session     *clientSession
	raw         storage.File
	access      windowsAccess
	owner       storage.RangeOwnerID
}

func (s *clientSession) authorize(ctx context.Context, op storage.Operation, effects storage.FileEffects, claim storage.AccessClaim, ref storage.FileReferenceID, node uint64, parent, dest storage.FileReferenceID) error {
	s.backend.stateMu.Lock()
	failed := s.backend.failed
	s.backend.stateMu.Unlock()
	if failed {
		return syscall.EIO
	}
	if p, ok := PrincipalFromContext(ctx); !ok && s.principal.SID != "" {
		ctx = WithPrincipal(ctx, s.principal)
	} else if ok && s.principal.SID != "" && p.SID != s.principal.SID {
		return authz.ErrDenied
	}
	return s.backend.authorize.Authorize(ctx, authz.AccessRequest{Volume: s.backend.volume, Operation: op, Effects: effects, Claim: claim, Reference: ref, Node: node, Parent: parent, Destination: dest})
}
func (s *clientSession) remember(id windowsActionID, a clientAction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return syscall.ESTALE
	}
	if _, ok := s.actions[id]; ok {
		return syscall.EINVAL
	}
	if len(s.actions) >= s.options.MaxActions {
		return syscall.ENOMEM
	}
	a.actual = id
	s.actions[id] = &a
	return nil
}
func (s *clientSession) action(id windowsActionID) *clientAction {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.actions[id]
	if a == nil {
		return nil
	}
	copy := *a
	return &copy
}
func (s *clientSession) next(ctx context.Context) (storage.FileActionID, error) {
	status, err := s.Status(ctx)
	if err != nil {
		return "", err
	}
	if status.Retired || status.Fenced {
		return "", syscall.EIO
	}
	return storage.NewFileActionID(status.ActionEpoch)
}
func (s *clientSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	status, err := s.raw.Status(ctx)
	if err != nil {
		return status, err
	}
	s.mu.Lock()
	s.status = status
	s.pruneActionsLocked(time.Now())
	s.mu.Unlock()
	return status, nil
}
func (s *clientSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	status, err := s.raw.Renew(ctx)
	if err != nil {
		return status, err
	}
	s.mu.Lock()
	s.status = status
	s.pruneActionsLocked(time.Now())
	s.mu.Unlock()
	return status, nil
}
func (s *clientSession) Close(ctx context.Context) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	if s.closeID == "" {
		id, err := storage.NewFileActionID(s.status.ActionEpoch)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		s.closeID = id
	}
	id := s.closeID
	s.mu.Unlock()
	r, err := s.raw.Close(ctx, id)
	if err == nil && r.Errno != 0 {
		err = r.Errno
	}
	terminal := r.Action == id && (r.State == storage.FileActionCompleted || r.State == storage.FileActionNotApplied) && !storage.IsFileCallNotAdmitted(err)
	if err != nil || r.State == storage.FileActionNotApplied {
		if terminal {
			s.mu.Lock()
			s.closeID = ""
			s.mu.Unlock()
		}
		if err == nil {
			err = syscall.EIO
		}
		return err
	}
	if !terminal && r.State != storage.FileActionRetired {
		return syscall.EIO
	}
	s.mu.Lock()
	s.closed = true
	clear(s.actions)
	s.actions = nil
	s.planBytes = 0
	s.mu.Unlock()
	return nil
}
func (s *clientSession) bindRetirement(retirement func(context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retirement = retirement
}
func (s *clientSession) requestRetirement(ctx context.Context) error {
	s.mu.Lock()
	retire := s.retirement
	s.mu.Unlock()
	if retire != nil {
		return retire(ctx)
	}
	return s.Close(ctx)
}
func (s *clientSession) QueryAction(ctx context.Context, id windowsActionID) (windowsActionResult, error) {
	a := s.action(id)
	if a != nil && a.rangeAction != nil {
		return s.rangeQuery(ctx, id, a.rangeAction)
	}
	actual := id
	if a != nil {
		actual = a.actual
	}
	r, err := s.raw.QueryAction(ctx, actual)
	if a != nil {
		s.recordOpen(id, r)
	}
	result, err := s.project(ctx, r, err, a)
	if r.Action == actual {
		result.Action = id
	} else if r.State != storage.FileActionUnknown && r.State != 0 {
		return windowsActionResult{}, errors.Join(syscall.EIO, err)
	}
	return result, err
}
func (s *clientSession) CancelAction(ctx context.Context, id windowsActionID) (windowsActionResult, error) {
	a := s.action(id)
	if a != nil && a.rangeAction != nil {
		return s.rangeCancel(ctx, id, a.rangeAction)
	}
	actual := id
	if a != nil {
		actual = a.actual
	}
	r, err := s.raw.CancelAction(ctx, actual)
	if a != nil {
		s.recordOpen(id, r)
	}
	result, err := s.project(ctx, r, err, a)
	if r.Action == actual {
		result.Action = id
	} else if r.State != storage.FileActionUnknown && r.State != 0 {
		return windowsActionResult{}, errors.Join(syscall.EIO, err)
	}
	return result, err
}
func (s *clientSession) project(ctx context.Context, r storage.FileActionReceipt, err error, a *clientAction) (windowsActionResult, error) {
	result := windowsActionResult{Action: r.Action, Receipt: r, HistoryRemaining: r.HistoryRemaining, Errno: r.Errno, Failure: windowsFailureOf(err)}
	switch r.State {
	case storage.FileActionUnknown:
		err = errors.Join(syscall.EIO, err)
	case storage.FileActionPending:
		result.State = windowsActionPending
	case storage.FileActionCompleted:
		result.State = windowsActionCompleted
	case storage.FileActionNotApplied:
		result.State = windowsActionRejected
	case storage.FileActionRetired:
		result.State = windowsActionCompleted
	}
	if a != nil && a.appendOnly && r.State == storage.FileActionNotApplied && r.Conflict != nil && r.Conflict.Kind == storage.ConflictRevision && r.Errno == syscall.EAGAIN {
		if err == nil {
			err = r.Errno
		}
		err = &windowsError{Code: syscall.EACCES, Err: err}
		result.Errno = syscall.EACCES
	}
	if a != nil {
		result.GrantedAccess = a.access
		result.CreateAction = a.create
		result.Applied = a.applied
		if r.State == storage.FileActionCompleted && err == nil && a.finalErr != nil {
			err = a.finalErr
			result.Errno = storage.ErrnoOf(err)
			result.Failure = windowsFailureOf(err)
		}
	}
	if r.Observation.Location != nil {
		result.proof = &nameProof{Location: r.Observation.Location.Clone(), Observation: r.Observation.Clone()}
	}
	if r.Observation.Attr.ID != 0 {
		var e error
		result.Attr, e = projectWindowsAttr(r.Observation, s.backend.defaults)
		if e != nil {
			err = errors.Join(err, e)
		}
	}
	if r.Reference != 0 && r.Effects&storage.EffectRetained != 0 {
		raw, e := s.raw.Reference(ctx, r.Reference)
		if e != nil {
			return result, errors.Join(err, e)
		}
		if a == nil || raw == nil || raw.Reference() != r.Reference || raw.NodeID() != r.Observation.Attr.ID {
			return result, errors.Join(syscall.EIO, err)
		}
		result.File = &clientFile{session: s, raw: raw, access: a.access, owner: storage.RangeOwnerID(r.Reference)}
	}
	if a != nil && a.redirect && result.File != nil && err == nil && r.State == storage.FileActionCompleted && r.Errno == 0 {
		info := windowsSymlinkInfo{Target: string(r.Observation.LinkTarget), Location: result.Attr.NameInfo}
		_, linkErr := relativeLink(info.Target, info.Location)
		if closeErr := s.closeTemporary(ctx, result.File.(*clientFile).raw); closeErr != nil {
			return result, closeErr
		}
		result.File = nil
		if linkErr != nil {
			return result, linkErr
		}
		result.Symlink = &info
		result.Errno = syscall.ELOOP
		result.State = windowsActionRejected
		return result, &windowsSymlinkError{windowsSymlinkInfo: info, Err: syscall.ELOOP}
	}
	return result, err
}
func (f *clientFile) Reference() storage.FileReferenceID { return f.raw.Reference() }
func (f *clientFile) require(access windowsAccess) error {
	if f.access&access != access {
		return syscall.EACCES
	}
	return nil
}
func (f *clientFile) authorize(ctx context.Context, op storage.Operation, effects storage.FileEffects) error {
	return f.session.authorize(ctx, op, effects, storage.AccessClaim{}, f.Reference(), f.raw.NodeID(), 0, 0)
}
func (f *clientFile) result(ctx context.Context, id windowsActionID, r storage.FileActionReceipt, err error) (windowsActionResult, error) {
	if storage.IsFileCallNotAdmitted(err) {
		return rejectedInvocation(id, err)
	}
	if marker, ok := ctx.Value(mutationAttemptKey{}).(*mutationAttempt); ok {
		marker.submitted = true
	}
	return f.session.project(ctx, r, err, f.session.action(id))
}
func (f *clientFile) Stat(ctx context.Context) (windowsAttr, error) {
	if err := f.authorize(ctx, storage.OpFileStat, 0); err != nil {
		return windowsAttr{}, err
	}
	r, err := f.raw.Stat(ctx, storage.ObservationOptions{})
	if err != nil {
		return windowsAttr{}, err
	}
	return projectWindowsAttr(r, f.session.backend.defaults)
}
func (f *clientFile) Sync(ctx context.Context) error {
	if err := f.authorize(ctx, storage.OpFileSync, 0); err != nil {
		return err
	}
	return f.raw.Sync(ctx)
}
func (f *clientFile) Close(ctx context.Context, id windowsActionID) (windowsActionResult, error) {
	r, err := f.raw.Close(ctx, id)
	result, err := f.result(ctx, id, r, err)
	if r.State == storage.FileActionRetired && r.Reference == f.Reference() && r.Operation == storage.OpFileClose && err == nil {
		result.Action = id
	}
	return result, err
}

func (s *clientSession) rememberOpen(original, actual storage.FileActionID, a clientAction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return syscall.ESTALE
	}
	previous := s.actions[original]
	if previous != nil && !previous.retryable {
		return syscall.EINVAL
	}
	if previous == nil && len(s.actions) >= s.options.MaxActions {
		return syscall.ENOMEM
	}
	a.actual = actual
	s.actions[original] = &a
	return nil
}
func (s *clientSession) recordOpen(original storage.FileActionID, r storage.FileActionReceipt) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.actions[original]
	if a == nil || a.actual != r.Action {
		return
	}
	a.retryable = r.State == storage.FileActionNotApplied && r.Effects == 0 && r.Reference == 0 && r.Conflict != nil && r.Conflict.Kind == storage.ConflictRevision
	a.terminal = r.State == storage.FileActionCompleted || r.State == storage.FileActionNotApplied
	if a.terminal {
		a.expires = time.Now().Add(r.HistoryRemaining)
	}
}

func (s *clientSession) pruneActionsLocked(now time.Time) {
	for id, a := range s.actions {
		current, terminal, expires := a.actual, a.terminal, a.expires
		if a.rangeAction != nil {
			r := a.rangeAction.retention()
			current, terminal, expires = r.current, r.terminal, r.expires
		}
		epoch, err := current.Epoch()
		if err == nil && terminal && !expires.IsZero() && !now.Before(expires) && epoch < s.status.ActionEpoch {
			delete(s.actions, id)
		}
	}
}
func (s *clientSession) resizeRangePlan(a *clientRangeAction, bytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		a.charge = 0
		if bytes == 0 {
			return nil
		}
		return syscall.ESTALE
	}
	if bytes < 0 || bytes > s.backend.limits.MaxDirectoryBytes-(s.planBytes-a.charge) {
		return syscall.ENOMEM
	}
	s.planBytes += bytes - a.charge
	a.charge = bytes
	return nil
}
