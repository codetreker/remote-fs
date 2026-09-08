package fuse

import (
	"context"
	"errors"
	"log"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/storage"
)

type namespace struct {
	storage        storage.Storage
	files          storage.FileSession
	owner          gofuse.Owner
	maxFileSize    int64
	flushTimeout   time.Duration
	sessionOptions storage.FileSessionOptions
	logger         *log.Logger
	raw            *rawMetadata

	mu           sync.Mutex
	status       storage.FileSessionStatus
	deadline     time.Time
	fault        error
	stopping     bool
	stop         chan struct{}
	done         chan struct{}
	closeErr     error
	renewContext context.Context
	cancelRenew  context.CancelFunc
}

func (ns *namespace) holds(size int64) bool { return size >= 0 && size <= ns.maxFileSize }

func (ns *namespace) check() error {
	ns.mu.Lock()
	fault, stopping, deadline := ns.fault, ns.stopping, ns.deadline
	ns.mu.Unlock()
	if fault != nil {
		return fault
	}
	if stopping {
		return syscall.ESTALE
	}
	if !time.Now().Before(deadline) {
		ns.fence(syscall.ESTALE)
		return errors.Join(syscall.EIO, syscall.ESTALE)
	}
	return nil
}

func (ns *namespace) fence(cause error) {
	ns.mu.Lock()
	if ns.fault == nil {
		ns.fault = errors.Join(syscall.EIO, cause)
	}
	if !ns.stopping {
		ns.stopping = true
		close(ns.stop)
		if ns.cancelRenew != nil {
			ns.cancelRenew()
		}
	}
	ns.mu.Unlock()
}

func (ns *namespace) stopSession() error {
	ns.mu.Lock()
	if !ns.stopping {
		ns.stopping = true
		close(ns.stop)
		if ns.cancelRenew != nil {
			ns.cancelRenew()
		}
	}
	ns.mu.Unlock()
	<-ns.done
	ns.mu.Lock()
	defer ns.mu.Unlock()
	return ns.closeErr
}

// Deadlines start before the round trip so a delayed reply cannot extend a
// reference beyond the lifetime the authority confirmed.
func (ns *namespace) confirm(start time.Time, status storage.FileSessionStatus) error {
	if status.Epoch == "" || status.Revision == 0 || status.ActionEpoch == 0 ||
		status.Retired || status.Fenced {
		return syscall.ESTALE
	}
	deadline := start.Add(max(status.Remaining, 0))
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if ns.stopping {
		return syscall.ESTALE
	}
	if ns.status.Epoch != "" && status.Epoch != ns.status.Epoch {
		return syscall.ESTALE
	}
	if status.Revision < ns.status.Revision {
		if !time.Now().Before(ns.deadline) {
			return syscall.ESTALE
		}
		return nil
	}
	if ns.status.Epoch == "" && (status.Remaining <= 0 || status.HistoryRemaining <= 0) {
		return syscall.ESTALE
	}
	if ns.deadline.After(deadline) {
		deadline = ns.deadline
	}
	if !time.Now().Before(deadline) {
		return syscall.ESTALE
	}
	if status.ActionEpoch < ns.status.ActionEpoch {
		status.ActionEpoch = ns.status.ActionEpoch
		status.HistoryRemaining = ns.status.HistoryRemaining
	}
	ns.status = status
	ns.deadline = deadline
	return nil
}

func (ns *namespace) actionEpoch(ctx context.Context) (uint64, error) {
	if err := ns.check(); err != nil {
		return 0, err
	}
	start := time.Now()
	ask, cancel := context.WithTimeout(ctx, ns.flushTimeout)
	defer cancel()
	status, err := ns.files.Status(ask)
	if err != nil {
		if errnoOf(err) == syscall.ESTALE && errnoOf(ns.check()) != syscall.ESTALE {
			ns.fence(err)
		}
		return 0, err
	}
	if err := ns.confirm(start, status); err != nil {
		if errnoOf(ns.check()) != syscall.ESTALE {
			ns.fence(err)
		}
		return 0, err
	}
	ns.mu.Lock()
	defer ns.mu.Unlock()
	return ns.status.ActionEpoch, nil
}

func newNamespace(ctx context.Context, s storage.Storage, opts Options, logger *log.Logger) (*namespace, error) {
	capability, ok := s.(storage.FileStorage)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := capability.CheckFileStorage(); err != nil {
		return nil, err
	}
	limits := storage.DefaultFileSessionOptions()
	if opts.FileSession != nil {
		limits = *opts.FileSession
	}
	if err := limits.Check(); err != nil {
		return nil, err
	}
	maxFileSize := opts.MaxFileSize
	if maxFileSize == 0 {
		maxFileSize = DefaultMaxFileSize
	}
	limits.MaxFileSize = maxFileSize
	if err := limits.Check(); err != nil {
		return nil, err
	}
	timeout, err := opts.flushTimeout()
	if err != nil {
		return nil, err
	}
	ask, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	files, err := capability.NewFileSession(ask, limits)
	if err != nil {
		return nil, err
	}
	ns := &namespace{
		storage: s, files: files, maxFileSize: maxFileSize,
		flushTimeout: timeout, sessionOptions: limits, logger: logger,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	start := time.Now()
	status, err := files.Status(ask)
	if err == nil {
		err = ns.confirm(start, status)
	}
	if err != nil {
		cleanup, finish := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer finish()
		return nil, errors.Join(err, files.Close(cleanup))
	}
	ns.renewContext, ns.cancelRenew = context.WithCancel(context.Background())
	go ns.maintain()
	return ns, nil
}

func (ns *namespace) maintain() {
	defer close(ns.done)
	for {
		ns.mu.Lock()
		left := time.Until(ns.deadline)
		ns.mu.Unlock()
		if left <= 0 {
			ns.fence(syscall.ESTALE)
			break
		}
		timer := time.NewTimer(max(left/3, time.Nanosecond))
		select {
		case <-ns.stop:
			timer.Stop()
			goto retire
		case <-timer.C:
		}

		ns.mu.Lock()
		deadline := ns.deadline
		ns.mu.Unlock()
		start := time.Now()
		ask, cancel := context.WithDeadline(ns.renewContext, minTime(deadline, start.Add(ns.flushTimeout)))
		status, err := ns.files.Renew(ask)
		cancel()
		select {
		case <-ns.stop:
			goto retire
		default:
		}
		if err == nil {
			err = ns.confirm(start, status)
		}
		if err != nil && (errnoOf(err) == syscall.ESTALE || !time.Now().Before(deadline)) {
			select {
			case <-ns.stop:
				goto retire
			default:
			}
			ns.fence(err)
			break
		}
	}
retire:
	cleanup, cancel := context.WithTimeout(context.Background(), ns.flushTimeout)
	err := ns.files.Close(cleanup)
	cancel()
	ns.mu.Lock()
	ns.closeErr = errors.Join(ns.fault, err)
	result := ns.closeErr
	ns.mu.Unlock()
	if result != nil && ns.logger != nil {
		ns.logger.Printf("file session retirement: %v", result)
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
