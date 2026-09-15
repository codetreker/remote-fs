package sqlite

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/windowsaccess"
	"github.com/codetreker/remote-fs/packages/storage"
)

type windowsDomain struct {
	access            *windowsaccess.Engine
	sessions          map[*windowsSession]struct{}
	nextHandle        uint64
	recoveryUntil     time.Time
	activationEpoch   uint64
	activationRotates time.Time
	activations       map[storage.WindowsActionID]windowsActivationRecord
	waiters           []*windowsAction
}

type windowsActivationRecord struct {
	result  storage.WindowsActivation
	err     error
	expires time.Time
}

type windowsSession struct {
	store          *Store
	options        storage.FileSessionOptions
	epoch          string
	actionEpoch    uint64
	rotates        time.Time
	expires        time.Time
	revision       uint64
	active         bool
	closed         bool
	files          map[string]*windowsFile
	actions        map[storage.WindowsActionID]*windowsAction
	timer          *time.Timer
	cleanup        context.Context
	cleanupTimeout time.Duration
}

type windowsAction struct {
	fingerprint [32]byte
	result      metastore.WindowsResult
	err         error
	expires     time.Time
	file        *windowsFile
	io          metastore.WindowsIO
	wait        *storage.WindowsLockBatch
}

type windowsFile struct {
	session   *windowsSession
	id        int64
	handle    uint64
	reference string
	access    storage.WindowsAccess
	active    bool
	closed    bool
}

func newWindowsDomain(maxFiles, maxRanges int) (*windowsDomain, error) {
	access, err := windowsaccess.New(windowsaccess.Limits{MaxOpens: maxFiles, MaxRanges: maxRanges, MaxBatchEntries: storage.WindowsMaxLockBatch})
	if err != nil {
		return nil, err
	}
	return &windowsDomain{access: access, sessions: make(map[*windowsSession]struct{}), activations: make(map[storage.WindowsActionID]windowsActivationRecord), activationEpoch: randomWindowsEpoch(), activationRotates: time.Now().Add(time.Minute)}, nil
}

func randomWindowsEpoch() uint64 {
	var b [8]byte
	rand.Read(b[:])
	return binary.LittleEndian.Uint64(b[:])&math.MaxInt64 + 1
}

func (d *windowsDomain) allocateHandle() (uint64, error) {
	if d.nextHandle == math.MaxUint64 {
		return 0, syscall.EOVERFLOW
	}
	d.nextHandle++
	return d.nextHandle, nil
}

func windowsAccess(a storage.WindowsAccess) windowsaccess.Access {
	var result windowsaccess.Access
	if a&storage.WindowsReadData != 0 {
		result |= windowsaccess.Read
	}
	if a&(storage.WindowsWriteData|storage.WindowsAppendData) != 0 {
		result |= windowsaccess.Write
	}
	if a&storage.WindowsDelete != 0 {
		result |= windowsaccess.Delete
	}
	return result
}

func windowsError(err error) error {
	if err == nil {
		return nil
	}
	var failure storage.WindowsFailure
	var errno syscall.Errno
	switch {
	case errors.Is(err, windowsaccess.ErrSharing):
		failure, errno = storage.WindowsSharingViolation, syscall.EACCES
	case errors.Is(err, windowsaccess.ErrDeletePending):
		failure, errno = storage.WindowsDeletePending, syscall.EACCES
	case errors.Is(err, windowsaccess.ErrConflict):
		failure, errno = storage.WindowsLockConflict, syscall.EAGAIN
	case errors.Is(err, windowsaccess.ErrNotLocked):
		failure, errno = storage.WindowsRangeNotLocked, syscall.ENOLCK
	case errors.Is(err, windowsaccess.ErrHandle):
		errno = syscall.EBADF
	case errors.Is(err, windowsaccess.ErrAccess):
		errno = syscall.EACCES
	case errors.Is(err, windowsaccess.ErrCapacity):
		errno = syscall.ENOLCK
	case errors.Is(err, windowsaccess.ErrInvalid), errors.Is(err, windowsaccess.ErrRange):
		errno = syscall.EINVAL
	default:
		return err
	}
	return &storage.WindowsError{Failure: failure, Err: &windowsEngineError{cause: errors.Join(errno, err), errno: errno}}
}

func (s *Store) CheckWindowsStore() error {
	if err := s.CheckFileStore(); err != nil {
		return err
	}
	if s.leaseRecovery == nil {
		return syscall.EOPNOTSUPP
	}
	return nil
}

func (s *Store) checkWindowsLocked(ctx context.Context) error {
	if err := s.checkFileOwnership(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if time.Now().Before(s.fileDomain.windows.recoveryUntil) {
		return syscall.EAGAIN
	}
	return nil
}

func (s *Store) NewWindowsSession(ctx context.Context, options storage.FileSessionOptions) (metastore.WindowsSession, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	if err := s.CheckWindowsStore(); err != nil {
		return nil, err
	}
	if err := s.RaiseMaxLease(ctx, options.Lease); err != nil {
		return nil, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return nil, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkWindowsLocked(ctx); err != nil {
		return nil, err
	}
	var version uint32
	if err := s.inspect(ctx, func(tx *sql.Tx) error { var err error; version, err = s.windowsNameVersion(ctx, tx); return err }); err != nil {
		return nil, err
	}
	if version == 0 {
		return nil, syscall.EOPNOTSUPP
	}
	d := s.fileDomain.windows
	if len(d.sessions) >= s.fileDomain.config.MaxSessions {
		return nil, syscall.EAGAIN
	}
	var nonce [16]byte
	rand.Read(nonce[:])
	now := time.Now()
	ws := &windowsSession{store: s, options: options, epoch: hex.EncodeToString(nonce[:]), actionEpoch: randomWindowsEpoch(), rotates: now.Add(options.History), expires: now.Add(options.Lease), revision: 1, active: true, files: make(map[string]*windowsFile), actions: make(map[storage.WindowsActionID]*windowsAction), cleanup: locking.WithScope(context.WithoutCancel(ctx), locking.MutationScope{}), cleanupTimeout: s.fileDomain.config.FileOperationTimeout}
	d.sessions[ws] = struct{}{}
	ws.timer = time.AfterFunc(options.Lease, ws.expire)
	return ws, nil
}

func (s *windowsSession) acquire(ctx context.Context) (func(), error) {
	if err := s.store.coordinator.commit.acquire(ctx); err != nil {
		return nil, err
	}
	if err := s.check(ctx); err != nil {
		s.store.coordinator.commit.release()
		return nil, err
	}
	return s.store.coordinator.commit.release, nil
}

func (s *windowsSession) check(ctx context.Context) error {
	if err := s.store.checkWindowsLocked(ctx); err != nil {
		return err
	}
	if !s.active || !time.Now().Before(s.expires) {
		return syscall.ESTALE
	}
	return nil
}

func (s *windowsSession) rotate() {
	now := time.Now()
	if !now.Before(s.rotates) {
		s.actionEpoch++
		s.rotates = now.Add(s.options.History)
	}
	for id, a := range s.actions {
		if a.result.State != storage.WindowsActionPending && !now.Before(a.expires) {
			delete(s.actions, id)
		}
	}
}

func windowsFingerprint(value any) ([32]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func (s *windowsSession) admit(id storage.WindowsActionID, fingerprint [32]byte, f *windowsFile) (*windowsAction, bool, error) {
	s.rotate()
	epoch, err := id.Epoch()
	if err != nil {
		return nil, false, err
	}
	if a := s.actions[id]; a != nil {
		if a.fingerprint != fingerprint || a.file != f {
			return nil, false, syscall.EINVAL
		}
		return a, false, nil
	}
	if epoch != s.actionEpoch {
		return nil, false, syscall.ESTALE
	}
	if len(s.actions) >= s.options.MaxLockActions {
		return nil, false, syscall.EAGAIN
	}
	count := 0
	for session := range s.store.fileDomain.windows.sessions {
		count += len(session.actions)
	}
	if count >= s.store.fileDomain.config.MaxRequests {
		return nil, false, syscall.EAGAIN
	}
	a := &windowsAction{fingerprint: fingerprint, file: f, result: metastore.WindowsResult{WindowsActionResult: storage.WindowsActionResult{Action: id, State: storage.WindowsActionPending}}}
	s.actions[id] = a
	return a, true, nil
}

func (s *windowsSession) result(a *windowsAction) (metastore.WindowsResult, error) {
	r := a.result
	if r.Symlink != nil {
		info := *r.Symlink
		r.Symlink = &info
	}
	if !a.expires.IsZero() {
		r.HistoryRemaining = max(time.Until(a.expires), 0)
	} else {
		r.HistoryRemaining = s.options.History
	}
	return r, a.err
}

func (s *windowsSession) finish(a *windowsAction, err error) (metastore.WindowsResult, error) {
	if err != nil && s.store.coordinator.healthy() != nil {
		a.err = errors.Join(syscall.EIO, err)
		s.active = false
		return s.result(a)
	}
	a.err = windowsError(err)
	a.result.State = storage.WindowsActionCompleted
	if err != nil {
		a.result.State = storage.WindowsActionRejected
		a.result.Errno = storage.ErrnoOf(a.err)
		a.result.Failure = storage.WindowsFailureOf(a.err)
	}
	a.expires = time.Now().Add(s.options.History)
	a.wait = nil
	return s.result(a)
}

func (s *windowsSession) status() storage.FileSessionStatus {
	s.rotate()
	return storage.FileSessionStatus{Epoch: s.epoch, Remaining: max(time.Until(s.expires), 0), Revision: s.revision, ActionEpoch: s.actionEpoch, HistoryRemaining: max(time.Until(s.rotates), 0), Retired: !s.active}
}

func (s *windowsSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	done, err := s.acquire(ctx)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer done()
	return s.status(), nil
}

func (s *windowsSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	done, err := s.acquire(ctx)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer done()
	s.expires = time.Now().Add(s.options.Lease)
	s.revision++
	s.timer.Reset(s.options.Lease)
	return s.status(), nil
}

type windowsEngineError struct {
	cause error
	errno syscall.Errno
}

func (e *windowsEngineError) Error() string         { return e.cause.Error() }
func (e *windowsEngineError) Unwrap() error         { return e.cause }
func (e *windowsEngineError) Classification() error { return e.errno }
