package objectstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileAuthority interface {
	metastore.FileStore
}

// Heartbeats, lock acquisition, and lock reconciliation have independent capacity.
// Staged data and new acquisitions cannot consume release or renewal admission.
const (
	maxFileControlOperations  = 2
	maxFileAdvisoryOperations = 2
	maxFileCleanupOperations  = 2
)

type fileOperationClass uint8

const (
	fileDataOperation fileOperationClass = iota
	fileControlOperation
	fileAdvisoryOperation
	fileCleanupOperation
)

type fileSession struct {
	storage            *Storage
	native             fileAuthority
	domain             *advisory.Coordinator
	locks              *advisory.Session
	options            storage.FileSessionOptions
	cleanup            context.Context
	epoch              string
	mu                 sync.Mutex
	active             bool
	expires            time.Time
	revision           uint64
	operations         int
	controls           int
	advisoryOperations int
	cleanupOperations  int
	files              map[*openFile]struct{}
	opening            int
	identityOps        sync.WaitGroup
	timer              *time.Timer
	closeMu            sync.Mutex
	closeDone          chan struct{}
	closeErr           error
}

var _ storage.FileStorage = (*Storage)(nil)
var _ storage.FileSession = (*fileSession)(nil)

func (s *Storage) CheckFileStorage() error {
	native, ok := s.meta.(fileAuthority)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := native.CheckFileStore(); err != nil {
		return err
	}
	if _, ok := s.objects.(BoundedObjects); !ok {
		return syscall.EOPNOTSUPP
	}
	return s.CheckBounded()
}

// Usage includes linked and retained detached file contents. Native staging
// reservations have independent admission limits and are not committed usage.
func (s *Storage) Usage(ctx context.Context) (int64, error) {
	native, ok := s.meta.(metastore.FileStore)
	if !ok {
		return 0, syscall.EOPNOTSUPP
	}
	if err := s.beginOperation(); err != nil {
		return 0, err
	}
	defer s.endOperation()
	return native.Usage(ctx)
}

func (s *Storage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	if err := s.CheckFileStorage(); err != nil {
		return nil, err
	}
	if err := s.beginOperation(); err != nil {
		return nil, err
	}
	defer s.endOperation()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	native := s.meta.(fileAuthority)
	domain, err := native.Advisory(ctx)
	if err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	if s.filesClosing {
		return nil, syscall.ESTALE
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	maxBytes, _, _ := domain.FileOperationLimits()
	options.MaxFileSize = min(options.MaxFileSize, maxBytes)
	fs := &fileSession{storage: s, native: native, domain: domain, options: options,
		cleanup: context.WithoutCancel(ctx), epoch: hex.EncodeToString(nonce[:]),
		active: true, expires: time.Now().Add(options.Lease), revision: 1,
		files: make(map[*openFile]struct{})}
	fs.locks, err = domain.NewSession(options, fs.fence)
	if err != nil {
		return nil, err
	}
	if s.fileSessions == nil {
		s.fileSessions = make(map[*fileSession]struct{})
	}
	s.fileSessions[fs] = struct{}{}
	fs.timer = time.AfterFunc(options.Lease, fs.expire)
	return fs, nil
}

func (fs *fileSession) expire() {
	fs.mu.Lock()
	if fs.active && time.Now().Before(fs.expires) {
		fs.timer.Reset(time.Until(fs.expires))
		fs.mu.Unlock()
		return
	}
	fs.mu.Unlock()
	fs.startClose()
}

func (fs *fileSession) begin(ctx context.Context, identity bool) (func(), error) {
	return fs.admit(ctx, identity, fileDataOperation)
}

func (fs *fileSession) beginControl(ctx context.Context) (func(), error) {
	return fs.admit(ctx, false, fileControlOperation)
}

func (fs *fileSession) admit(ctx context.Context, identity bool, class fileOperationClass) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := fs.storage.beginOperation(); err != nil {
		return nil, err
	}
	fs.mu.Lock()
	if !fs.active || !time.Now().Before(fs.expires) {
		fs.mu.Unlock()
		fs.storage.endOperation()
		fs.startClose()
		return nil, syscall.ESTALE
	}
	active, limit := &fs.operations, fs.options.MaxOperations
	switch class {
	case fileControlOperation:
		active, limit = &fs.controls, maxFileControlOperations
	case fileAdvisoryOperation:
		active, limit = &fs.advisoryOperations, maxFileAdvisoryOperations
	case fileCleanupOperation:
		active, limit = &fs.cleanupOperations, maxFileCleanupOperations
	}
	if *active >= limit {
		fs.mu.Unlock()
		fs.storage.endOperation()
		return nil, syscall.EAGAIN
	}
	*active++
	if identity {
		fs.identityOps.Add(1)
	}
	fs.mu.Unlock()
	return func() {
		fs.mu.Lock()
		*active--
		fs.mu.Unlock()
		if identity {
			fs.identityOps.Done()
		}
		fs.storage.endOperation()
	}, nil
}

func (fs *fileSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	ctx = metastore.WithFilePublicationGuard(ctx, fs.publicationAllowed)
	clean, err := storage.CleanPath(path)
	if err != nil {
		return nil, err
	}
	if err := options.Check(); err != nil {
		return nil, err
	}
	return fs.open(ctx, options, func() (metastore.File, error) { return fs.native.OpenFile(ctx, clean, options) })
}

func (fs *fileSession) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (storage.File, error) {
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	ctx = metastore.WithFilePublicationGuard(ctx, fs.publicationAllowed)
	if err := options.CheckNode(id); err != nil {
		return nil, err
	}
	return fs.open(ctx, options, func() (metastore.File, error) { return fs.native.OpenNode(ctx, id, options) })
}

func (fs *fileSession) open(ctx context.Context, options storage.FileOpenOptions, open func() (metastore.File, error)) (storage.File, error) {
	done, err := fs.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	defer done()
	fs.mu.Lock()
	if len(fs.files)+fs.opening >= fs.options.MaxFiles {
		fs.mu.Unlock()
		return nil, syscall.EMFILE
	}
	fs.opening++
	fs.mu.Unlock()
	native, err := open()
	fs.mu.Lock()
	fs.opening--
	if err != nil {
		fs.mu.Unlock()
		return nil, err
	}
	f := &openFile{session: fs, native: native, options: options, active: true, flock: make(map[storage.LockOwner]uint64)}
	fs.files[f] = struct{}{}
	active := fs.active && time.Now().Before(fs.expires)
	fs.mu.Unlock()
	if !active {
		return nil, errors.Join(syscall.ESTALE, f.retire())
	}
	return f, nil
}

func (fs *fileSession) StatNode(ctx context.Context, id uint64) (storage.Attr, error) {
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.begin(ctx, true)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	node, err := fs.native.StatNode(ctx, id)
	return node.Attr(), err
}

func (fs *fileSession) SetNodeAttr(ctx context.Context, id uint64, change storage.AttrChange) (storage.Attr, error) {
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	ctx = metastore.WithFilePublicationGuard(ctx, fs.publicationAllowed)
	if err := change.Check(); err != nil {
		return storage.Attr{}, err
	}
	done, err := fs.begin(ctx, true)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	node, err := fs.native.SetNodeAttr(ctx, id, change)
	return node.Attr(), err
}

func (fs *fileSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.beginControl(ctx)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer done()
	if err := fs.health(ctx); err != nil {
		return storage.FileSessionStatus{}, err
	}
	fs.mu.Lock()
	if !fs.active || !time.Now().Before(fs.expires) {
		fs.mu.Unlock()
		return storage.FileSessionStatus{}, syscall.ESTALE
	}
	fs.expires = time.Now().Add(fs.options.Lease)
	fs.revision++
	fs.timer.Reset(fs.options.Lease)
	fs.mu.Unlock()
	return fs.status(ctx)
}

func (fs *fileSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.beginControl(ctx)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	defer done()
	if err := fs.health(ctx); err != nil {
		return storage.FileSessionStatus{}, err
	}
	return fs.status(ctx)
}

func (fs *fileSession) health(ctx context.Context) error {
	if _, err := fs.native.Usage(ctx); err != nil {
		return err
	}
	return fs.locks.IOHealth(ctx, 1)
}

func (fs *fileSession) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	_, _, timeout := fs.domain.FileOperationLimits()
	return context.WithTimeout(ctx, timeout)
}

func (fs *fileSession) publicationAllowed() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if !fs.active || !time.Now().Before(fs.expires) {
		return syscall.ESTALE
	}
	return nil
}

func (fs *fileSession) status(ctx context.Context) (storage.FileSessionStatus, error) {
	epoch, history, err := fs.locks.History(ctx)
	if err != nil {
		return storage.FileSessionStatus{}, err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	remaining := time.Until(fs.expires)
	if !fs.active || remaining <= 0 {
		return storage.FileSessionStatus{}, syscall.ESTALE
	}
	return storage.FileSessionStatus{Epoch: fs.epoch, Remaining: remaining, Revision: fs.revision,
		ActionEpoch: epoch, HistoryRemaining: history}, nil
}

func (fs *fileSession) fence() error {
	// An opening reference is not available to retire until native Open returns.
	// Keep advisory grants until those operations join the retained set.
	fs.identityOps.Wait()
	fs.mu.Lock()
	files := make([]*openFile, 0, len(fs.files))
	for f := range fs.files {
		files = append(files, f)
	}
	fs.mu.Unlock()
	var errs []error
	for _, f := range files {
		errs = append(errs, f.retire())
	}
	return errors.Join(errs...)
}

func (fs *fileSession) startClose() <-chan struct{} {
	fs.closeMu.Lock()
	defer fs.closeMu.Unlock()
	if fs.closeDone != nil {
		select {
		case <-fs.closeDone:
			if fs.closeErr == nil {
				return fs.closeDone
			}
		default:
			return fs.closeDone
		}
	}
	fs.closeDone = make(chan struct{})
	fs.mu.Lock()
	fs.active = false
	if fs.timer != nil {
		fs.timer.Stop()
	}
	fs.mu.Unlock()
	go fs.finishClose()
	return fs.closeDone
}

func (fs *fileSession) finishClose() {
	err := fs.locks.Retire(fs.cleanup)
	if err == nil {
		fs.mu.Lock()
		files := make([]*openFile, 0, len(fs.files))
		for f := range fs.files {
			files = append(files, f)
		}
		fs.mu.Unlock()
		for _, f := range files {
			err = errors.Join(err, f.Close(fs.cleanup))
		}
	}
	if err == nil {
		fs.storage.fileMu.Lock()
		delete(fs.storage.fileSessions, fs)
		fs.storage.fileMu.Unlock()
	}
	fs.closeMu.Lock()
	fs.closeErr = err
	close(fs.closeDone)
	fs.closeMu.Unlock()
}

func (fs *fileSession) Close(ctx context.Context) error {
	done := fs.startClose()
	select {
	case <-done:
		fs.closeMu.Lock()
		defer fs.closeMu.Unlock()
		return fs.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CloseFileSessions stops file-session admission and drains the references
// before either durable half can release its ownership. Cleanup failure keeps
// the sessions and their native pins owned and returns the original failure.
func (s *Storage) CloseFileSessions() error {
	s.fileCloseMu.Lock()
	defer s.fileCloseMu.Unlock()
	s.fileMu.Lock()
	s.filesClosing = true
	sessions := make([]*fileSession, 0, len(s.fileSessions))
	for fs := range s.fileSessions {
		sessions = append(sessions, fs)
	}
	s.fileMu.Unlock()
	for _, fs := range sessions {
		fs.startClose()
	}
	var errs []error
	for _, fs := range sessions {
		errs = append(errs, fs.Close(context.Background()))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("closing retained file sessions: %w", err)
	}
	return nil
}
