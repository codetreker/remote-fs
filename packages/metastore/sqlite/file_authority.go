package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileSession struct {
	store            *Store
	options          storage.FileSessionOptions
	id               uint64
	epoch            string
	actionEpoch      uint64
	firstActionEpoch uint64
	closeAction      *fileAction
	cleanupActions   map[storage.FileActionID]*fileAction
	rotates, expires time.Time
	revision         uint64
	active, closed   bool
	shutdown         atomic.Bool
	historyOnly      atomic.Bool
	historyDone      atomic.Bool
	files            map[storage.FileReferenceID]*fileReference
	actions          map[storage.FileActionID]*fileAction
	timer            *time.Timer
	cleanup          context.Context
	cleanupTimeout   time.Duration
	owners           map[storage.RangeOwnerID]struct{}
	rangeCounts      map[fileRangeKey]int
	waiters          int
	activeIO         int
	cleanupPending   bool
	maxRangeSet      int
}
type fileAction struct {
	fingerprint [32]byte
	result      storage.FileActionReceipt
	err         error
	expires     time.Time
	file        *fileReference
	io          storage.FileIO
	wait        *fileaccess.Wait
	waitOwner   storage.RangeOwnerID
	waitScope   storage.RangeScope
}
type fileReference struct {
	session        *fileSession
	id             int64
	reference      storage.FileReferenceID
	claim          storage.AccessClaim
	active, closed bool
	entryID        storage.EntryID
	intent         storage.RemovalIntentID
	closeAction    *fileAction
	ioUsers        int
	claimReleased  bool
}

func (s *Store) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (metastore.FileSession, storage.FileSessionStatus, error) {
	if err := options.Check(); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	capabilityErr := s.checkFileAuthority(ctx)
	s.coordinator.commit.release()
	if capabilityErr != nil {
		return nil, storage.FileSessionStatus{}, capabilityErr
	}
	release, err := s.beginFileSession(ctx, options.Lease)
	if err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	defer release()
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileAuthority(ctx); err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	d := s.fileDomain
	if len(d.sessions) >= d.config.MaxSessions {
		return nil, storage.FileSessionStatus{}, syscall.EAGAIN
	}
	id, err := d.allocateSession()
	if err != nil {
		return nil, storage.FileSessionStatus{}, err
	}
	var nonce [16]byte
	rand.Read(nonce[:])
	var random [8]byte
	rand.Read(random[:])
	epoch := binary.LittleEndian.Uint64(random[:]) & math.MaxInt64
	if epoch == 0 {
		epoch = 1
	}
	now := time.Now()
	session := &fileSession{store: s, options: options, id: id, epoch: hex.EncodeToString(nonce[:]), actionEpoch: epoch, firstActionEpoch: epoch, cleanupActions: make(map[storage.FileActionID]*fileAction), rotates: now.Add(options.History), expires: now.Add(options.Lease), revision: 1, active: true,
		files: make(map[storage.FileReferenceID]*fileReference), actions: make(map[storage.FileActionID]*fileAction), owners: make(map[storage.RangeOwnerID]struct{}), rangeCounts: make(map[fileRangeKey]int), cleanup: locking.WithScope(context.WithoutCancel(ctx), locking.MutationScope{}), cleanupTimeout: d.contentConfig.FileOperationTimeout, maxRangeSet: min(options.MaxRanges, d.accessConfig.MaxSetRanges)}
	d.sessions[session] = struct{}{}
	session.timer = time.AfterFunc(options.Lease, session.expire)
	return session, session.status(), nil
}

func (s *fileSession) acquire(ctx context.Context) (func(), error) {
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return nil, err
	}
	if err := s.check(ctx); err != nil {
		s.store.coordinator.commit.release()
		return nil, err
	}
	return s.store.coordinator.commit.release, nil
}
func (s *fileSession) check(ctx context.Context) error {
	if err := s.store.checkFileAuthority(ctx); err != nil {
		return err
	}
	if !s.active || s.closed || !time.Now().Before(s.expires) {
		return syscall.ESTALE
	}
	return nil
}
func (s *fileSession) rotate() {
	now := time.Now()
	if !now.Before(s.rotates) {
		if s.actionEpoch == math.MaxUint64 {
			s.active = false
		} else {
			s.actionEpoch++
		}
		s.rotates = now.Add(s.options.History)
	}
	for id, a := range s.actions {
		epoch, _ := id.Epoch()
		if (epoch != s.actionEpoch || s.closed) && a.result.State != storage.FileActionPending && a.result.State != storage.FileActionUnknown && !now.Before(a.expires) {
			delete(s.actions, id)
		}
	}
	for id, a := range s.cleanupActions {
		epoch, _ := id.Epoch()
		if (epoch != s.actionEpoch || s.closed) && a.result.State != storage.FileActionPending && a.result.State != storage.FileActionUnknown && !now.Before(a.expires) {
			delete(s.cleanupActions, id)
		}
	}

}
func (s *fileSession) admit(id storage.FileActionID, fingerprint [32]byte, f *fileReference) (*fileAction, bool, error) {
	if s.shutdown.Load() {
		return nil, false, fileNotAdmitted(syscall.ESTALE)
	}
	epoch, err := id.Epoch()
	if err != nil {
		return nil, false, fileNotAdmitted(err)
	}
	s.rotate()
	if a := s.actions[id]; a != nil {
		if a.fingerprint != fingerprint || f != nil && a.file != f {
			return nil, false, fileNotAdmitted(syscall.EINVAL)
		}
		return a, false, nil
	}
	if s.cleanupActions[id] != nil {
		return nil, false, fileNotAdmitted(syscall.EINVAL)
	}
	if epoch != s.actionEpoch {
		return nil, false, fileNotAdmitted(syscall.ESTALE)
	}
	if err := s.check(context.Background()); err != nil {
		return nil, false, fileNotAdmitted(err)
	}
	pending, total := 0, 0
	for _, a := range s.actions {
		if a.result.State == storage.FileActionPending {
			pending++
		}
	}
	for session := range s.store.fileDomain.sessions {
		total += len(session.actions)
	}
	if len(s.actions) >= s.options.MaxActions || pending >= s.options.MaxPendingActions || total >= s.store.fileDomain.config.MaxActions {
		return nil, false, fileNotAdmitted(syscall.EAGAIN)
	}
	a := &fileAction{fingerprint: fingerprint, file: f, result: storage.FileActionReceipt{Action: id, State: storage.FileActionPending}}
	s.actions[id] = a
	return a, true, nil
}

func (s *fileSession) result(a *fileAction) (storage.FileActionReceipt, error) {
	r := a.result.Clone()
	r.HistoryRemaining = s.options.History
	if !a.expires.IsZero() {
		r.HistoryRemaining = max(time.Until(a.expires), 0)
	}
	return r, a.err
}
func (s *fileSession) finish(a *fileAction, err error) (storage.FileActionReceipt, error) {
	if a.wait != nil {
		a.wait = nil
		s.waiters--
		if cleanupErr := s.releaseIdleRangeOwner(a.waitOwner); cleanupErr != nil {
			err = &storage.FileError{Code: syscall.EIO, Cause: errors.Join(err, cleanupErr)}
		}
	}

	a.err = fileAccessError(err)
	if a.err != nil {
		var classified *storage.FileError
		if errors.As(a.err, &classified) {
			a.err = &storage.FileError{Code: storage.ErrnoOf(a.err), Conflict: classified.Conflict, Cause: a.err}
		}
	}
	a.result.Errno = 0
	a.result.State = storage.FileActionCompleted
	if err != nil {
		a.result.Errno = storage.ErrnoOf(a.err)
		if s.store.coordinator.healthy() != nil {
			a.result.State = storage.FileActionUnknown
			s.active = false
		} else if a.result.Effects == 0 {
			a.result.State = storage.FileActionNotApplied
		}
		var classified *storage.FileError
		if errors.As(a.err, &classified) {
			a.result.Conflict = classified.Conflict
		}
	}
	a.expires = time.Now().Add(s.options.History)
	return s.result(a)
}
func (s *fileSession) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if _, err := id.Epoch(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer s.store.coordinator.commit.release()
	if s.shutdown.Load() {
		return storage.FileActionReceipt{}, syscall.ESTALE
	}
	s.rotate()
	a := s.actions[id]
	if a == nil {
		a = s.cleanupActions[id]
	}
	if a == nil {
		return storage.FileActionReceipt{}, syscall.ESTALE
	}
	return s.result(a)
}
func (s *fileSession) CancelAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if _, err := id.Epoch(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer s.store.coordinator.commit.release()
	a := s.actions[id]
	if a == nil {
		a = s.cleanupActions[id]
	}
	if a == nil {
		return storage.FileActionReceipt{}, syscall.ESTALE
	}
	if a.result.State != storage.FileActionPending {
		return s.result(a)
	}
	if s.closeAction == a || a.file != nil && a.file.closeAction == a {
		return s.result(a)
	}
	if a.wait != nil {
		a.wait.Cancel()
	}
	// Final publication and receipt completion share this gate. A still-pending
	// action can be cancelled before a later publisher acquires it.
	return s.finish(a, errors.Join(context.Canceled, syscall.EINTR))
}
func (s *fileSession) status() storage.FileSessionStatus {
	return storage.FileSessionStatus{Epoch: s.epoch, Remaining: max(time.Until(s.expires), 0), Revision: s.revision, ActionEpoch: s.actionEpoch, HistoryRemaining: max(time.Until(s.rotates), 0), Retired: !s.active, Fenced: s.store.coordinator.healthy() != nil}
}
func (s *fileSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer s.store.coordinator.commit.release()
	s.rotate()
	if !time.Now().Before(s.expires) {
		s.active = false
	}
	return s.status(), nil
}
func (s *fileSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	done, err := s.acquire(ctx)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	done()
	if err := s.store.RaiseFileMaxLease(ctx, s.options.Lease); err != nil {
		return storage.FileSessionStatus{}, err
	}
	done, err = s.acquire(ctx)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer done()
	if s.revision == math.MaxUint64 {
		return storage.FileSessionStatus{}, syscall.EOVERFLOW
	}
	s.rotate()
	s.revision++
	s.expires = time.Now().Add(s.options.Lease)
	s.timer.Reset(time.Until(s.expires))
	return s.status(), nil
}
func (s *fileSession) Retire(ctx context.Context) error {
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.store.coordinator.commit.release()
	s.active = false
	for _, f := range s.files {
		f.active = false
	}
	s.retireWaitsLocked(nil)

	return nil
}
func (s *fileSession) closeLocked(ctx context.Context) (storage.FileEffects, error) {
	s.active = false
	var effects storage.FileEffects
	var failures []error
	ioPending := false
	for _, f := range s.files {
		if f.active {
			effects |= storage.EffectReferenceRetired
		}
		f.active = false
	}
	s.retireWaitsLocked(nil)
	for _, f := range s.files {
		effect, err := f.closeLocked(ctx)
		effects |= effect
		if err == errFileIOActive {
			ioPending = true
		} else if err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		return effects, errors.Join(failures...)
	}
	if err := s.store.fileDomain.access.RetireSession(s.id); err != nil {
		return effects, fileAccessError(err)
	}
	clear(s.owners)
	clear(s.rangeCounts)
	for _, f := range s.files {
		f.claimReleased = true
	}
	if ioPending {
		return effects, errFileIOActive
	}
	for _, a := range s.actions {
		if a.result.State == storage.FileActionPending {
			if a.wait != nil {
				a.wait.Cancel()
			}
			s.finish(a, syscall.ESTALE)
		}
	}
	s.closed = true
	s.historyOnly.Store(true)
	s.cleanupPending = false
	for _, action := range s.cleanupActions {
		if action.result.State == storage.FileActionPending && (action.file == nil || action.file.closed) {
			if action.file != nil {
				action.result.Effects |= storage.EffectReferenceRetired
			}
			s.finish(action, nil)
		}
	}
	if len(s.actions) == 0 && len(s.cleanupActions) == 0 {
		s.historyDone.Store(true)
		delete(s.store.fileDomain.sessions, s)
		if s.timer != nil {
			s.timer.Stop()
		}
	} else {
		s.scheduleHistoryLocked()
	}
	return effects, nil
}
func (s *fileSession) Dispose(ctx context.Context) error {
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer s.store.coordinator.commit.release()
	if s.closed {
		return nil
	}
	_, err := s.closeLocked(ctx)
	return err
}
func (s *fileSession) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	fp, err := fileFingerprint(storage.OpFileSessionClose)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer s.store.coordinator.commit.release()
	a, proceed, err := s.admitClose(id, fp, nil)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if !proceed {
		if a == nil {
			return s.retiredClose(nil), nil
		}
		return s.result(a)
	}
	effects, err := s.closeLocked(ctx)
	return s.finishClose(a, nil, effects, err)
}
func (s *fileSession) BeginClose(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, bool, error) {
	fp, err := fileFingerprint(storage.OpFileSessionClose)
	if err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	defer s.store.coordinator.commit.release()
	a, proceed, err := s.admitClose(id, fp, nil)
	if err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	if !proceed {
		if a == nil {
			return s.retiredClose(nil), false, nil
		}
		r, e := s.result(a)
		return r, false, e
	}
	s.active = false
	for _, f := range s.files {
		if f.active && a != nil {
			a.result.Effects |= storage.EffectReferenceRetired
		}
		f.active = false
	}
	s.retireWaitsLocked(nil)
	if a == nil {
		return storage.FileActionReceipt{}, true, nil
	}
	r, e := s.result(a)
	return r, true, e
}

func (s *fileSession) expire() {
	ctx, cancel := context.WithTimeout(s.cleanup, s.cleanupTimeout)
	defer cancel()
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		if !s.shutdown.Load() && !s.historyDone.Load() {
			if s.historyOnly.Load() {
				s.timer.Reset(s.options.History)
			} else {
				s.store.coordinator.poisonWith(err)
			}
		}
		return
	}
	defer s.store.coordinator.commit.release()
	if s.shutdown.Load() || s.historyDone.Load() {
		return
	}
	if s.closed {
		s.rotate()
		if len(s.actions) == 0 && len(s.cleanupActions) == 0 {
			s.historyDone.Store(true)
			s.timer.Stop()
			delete(s.store.fileDomain.sessions, s)
			return
		}
		s.scheduleHistoryLocked()
		return
	}
	if time.Now().Before(s.expires) {
		s.timer.Reset(time.Until(s.expires))
		return
	}
	s.cleanupPending = true
	if _, err := s.closeLocked(ctx); err != nil && !errors.Is(err, errFileIOActive) {
		s.store.coordinator.poisonWith(err)
	}
}
func (f *fileReference) Reference() storage.FileReferenceID { return f.reference }
func (s *fileSession) Reference(ctx context.Context, id storage.FileReferenceID) (metastore.File, bool, error) {
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return nil, false, err
	}
	defer s.store.coordinator.commit.release()
	if s.shutdown.Load() || id == 0 {
		return nil, false, syscall.ESTALE
	}
	s.rotate()
	if f := s.files[id]; f != nil {
		return f, f.active && s.active && !s.closed && !f.closed && time.Now().Before(s.expires), nil
	}
	for _, records := range []map[storage.FileActionID]*fileAction{s.actions, s.cleanupActions} {
		for _, action := range records {
			if action.file != nil && action.file.reference == id && action.file.closed {
				return action.file, false, nil
			}
		}
	}
	return nil, false, syscall.ESTALE
}

func (s *fileSession) StatNode(ctx context.Context, id uint64, options storage.ObservationOptions) (storage.FileObservation, error) {
	done, err := s.acquire(ctx)
	if err != nil {
		return storage.FileObservation{}, err
	}
	defer done()
	if id == 0 || id > math.MaxInt64 {
		return storage.FileObservation{}, syscall.EINVAL
	}
	f := &fileReference{session: s, id: int64(id), active: true}
	var result storage.FileObservation
	err = s.store.inspect(ctx, func(tx *sql.Tx) error { var err error; result, err = f.observation(ctx, tx, options); return err })
	return result, err
}

// shutdownFileHistoryLocked runs after physical reference cleanup and before
// Store.Close releases its domain. Shutdown retires the store's receipt service.
func (s *Store) shutdownFileHistoryLocked() error {
	d := s.fileDomain
	if d == nil {
		return nil
	}
	for session := range d.sessions {
		if session.store != s {
			continue
		}
		if !session.closed {
			return syscall.EBUSY
		}
		for _, records := range []map[storage.FileActionID]*fileAction{session.actions, session.cleanupActions} {
			for _, action := range records {
				if action.result.State == storage.FileActionPending || action.result.State == storage.FileActionUnknown {
					return syscall.EBUSY
				}
			}
		}
	}
	for session := range d.sessions {
		if session.store != s {
			continue
		}
		session.shutdown.Store(true)
		clear(session.actions)
		clear(session.cleanupActions)
		if session.timer != nil {
			session.timer.Stop()
		}
		delete(d.sessions, session)
	}
	return nil
}

func (s *fileSession) retiredClose(f *fileReference) storage.FileActionReceipt {
	if f == nil {
		return storage.FileActionReceipt{Operation: storage.OpFileSessionClose, State: storage.FileActionRetired}
	}
	return storage.FileActionReceipt{Operation: storage.OpFileClose, State: storage.FileActionRetired, Reference: f.reference}
}

// Each admitted identity owns one cleanup receipt slot. Subsequent cleanup
// attempts can complete physical retirement without creating additional history.
func (s *fileSession) admitClose(id storage.FileActionID, fp [32]byte, f *fileReference) (*fileAction, bool, error) {
	epoch, err := id.Epoch()
	if err != nil {
		return nil, false, fileNotAdmitted(err)
	}
	if s.shutdown.Load() {
		return nil, false, fileNotAdmitted(syscall.ESTALE)
	}
	s.rotate()
	if epoch < s.firstActionEpoch || epoch > s.actionEpoch {
		return nil, false, fileNotAdmitted(syscall.ESTALE)
	}
	if s.actions[id] != nil {
		return nil, false, fileNotAdmitted(syscall.EINVAL)
	}
	if old := s.cleanupActions[id]; old != nil && (old.fingerprint != fp || old.file != f) {
		return nil, false, fileNotAdmitted(syscall.EINVAL)
	}
	closed := s.closed
	slot := s.closeAction
	if f != nil {
		if f.session != s {
			return nil, false, fileNotAdmitted(syscall.EINVAL)
		}
		closed = f.closed
		slot = f.closeAction
	}
	if closed {
		return nil, false, nil
	}
	if err := s.store.checkFileAuthority(context.Background()); err != nil {
		return nil, false, fileNotAdmitted(err)
	}
	if old := s.cleanupActions[id]; old != nil {
		return old, old.result.State == storage.FileActionPending, nil
	}
	if slot != nil || epoch != s.actionEpoch {
		return nil, true, nil
	}
	operation := storage.OpFileSessionClose
	var reference storage.FileReferenceID
	if f != nil {
		operation = storage.OpFileClose
		reference = f.reference
	}
	action := &fileAction{fingerprint: fp, file: f, result: storage.FileActionReceipt{Action: id, Operation: operation, Reference: reference, State: storage.FileActionPending}}
	s.cleanupActions[id] = action
	if f == nil {
		s.closeAction = action
	} else {
		f.closeAction = action
	}
	return action, true, nil
}
func (s *fileSession) finishClose(a *fileAction, f *fileReference, effects storage.FileEffects, err error) (storage.FileActionReceipt, error) {
	if a != nil {
		a.result.Effects |= effects
		if err == errFileIOActive {
			return s.result(a)
		}
		return s.finish(a, err)
	}
	if err == errFileIOActive {
		return storage.FileActionReceipt{}, &storage.FileError{Code: syscall.EBUSY, Cause: err}
	}
	if err != nil {
		return storage.FileActionReceipt{}, fileAccessError(err)
	}
	return s.retiredClose(f), nil
}

// Logical retirement ends waits before a byte-serving adapter drains its work.
// Held ranges remain governed by their explicit owner and session lifetimes.
func (s *fileSession) retireWaitsLocked(f *fileReference) {
	for _, action := range s.actions {
		if action.wait == nil || f != nil && action.file != f {
			continue
		}
		action.wait.Cancel()
		s.finish(action, fileaccess.ErrRetired)
	}
}

func (s *fileSession) scheduleHistoryLocked() {
	var next time.Time
	for _, records := range []map[storage.FileActionID]*fileAction{s.actions, s.cleanupActions} {
		for _, action := range records {
			if action.result.State == storage.FileActionPending || action.result.State == storage.FileActionUnknown {
				continue
			}
			if next.IsZero() || action.expires.Before(next) {
				next = action.expires
			}
		}
	}
	if next.IsZero() {
		s.timer.Stop()
		return
	}
	s.timer.Reset(max(time.Until(next), time.Nanosecond))
}

func fileNotAdmitted(err error) error {
	if err == nil {
		return nil
	}
	cause := fileAccessError(err)
	result := &storage.FileError{Code: storage.ErrnoOf(cause), Cause: cause, NotAdmitted: true}
	var existing *storage.FileError
	if errors.As(cause, &existing) && existing.Conflict != nil {
		conflict := *existing.Conflict
		if conflict.Range != nil {
			held := *conflict.Range
			conflict.Range = &held
		}
		result.Conflict = &conflict
	}
	return result
}
