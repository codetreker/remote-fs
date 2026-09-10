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

type volume struct {
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

func (v *volume) holds(size int64) bool { return size >= 0 && size <= v.maxFileSize }

func (v *volume) check() error {
	v.mu.Lock()
	fault, stopping, deadline := v.fault, v.stopping, v.deadline
	v.mu.Unlock()
	if fault != nil {
		return fault
	}
	if stopping {
		return syscall.ESTALE
	}
	if !time.Now().Before(deadline) {
		v.fence(syscall.ESTALE)
		return errors.Join(syscall.EIO, syscall.ESTALE)
	}
	return nil
}

func (v *volume) fence(cause error) {
	v.mu.Lock()
	if v.fault == nil {
		v.fault = errors.Join(syscall.EIO, cause)
	}
	if !v.stopping {
		v.stopping = true
		close(v.stop)
		if v.cancelRenew != nil {
			v.cancelRenew()
		}
	}
	v.mu.Unlock()
}

func (v *volume) stopSession() error {
	v.mu.Lock()
	if !v.stopping {
		v.stopping = true
		close(v.stop)
		if v.cancelRenew != nil {
			v.cancelRenew()
		}
	}
	v.mu.Unlock()
	<-v.done
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.closeErr
}

// Deadlines start before the round trip so a delayed reply cannot extend a
// reference beyond the lifetime the authority confirmed.
func (v *volume) confirm(start time.Time, status storage.FileSessionStatus) error {
	if status.Epoch == "" || status.Revision == 0 || status.ActionEpoch == 0 ||
		status.Retired || status.Fenced {
		return syscall.ESTALE
	}
	deadline := start.Add(max(status.Remaining, 0))
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.stopping {
		return syscall.ESTALE
	}
	if v.status.Epoch != "" && status.Epoch != v.status.Epoch {
		return syscall.ESTALE
	}
	if status.Revision < v.status.Revision {
		if !time.Now().Before(v.deadline) {
			return syscall.ESTALE
		}
		return nil
	}
	if v.status.Epoch == "" && (status.Remaining <= 0 || status.HistoryRemaining <= 0) {
		return syscall.ESTALE
	}
	if v.deadline.After(deadline) {
		deadline = v.deadline
	}
	if !time.Now().Before(deadline) {
		return syscall.ESTALE
	}
	if status.ActionEpoch < v.status.ActionEpoch {
		status.ActionEpoch = v.status.ActionEpoch
		status.HistoryRemaining = v.status.HistoryRemaining
	}
	v.status = status
	v.deadline = deadline
	return nil
}

func (v *volume) actionEpoch(ctx context.Context) (uint64, error) {
	if err := v.check(); err != nil {
		return 0, err
	}
	start := time.Now()
	ask, cancel := context.WithTimeout(ctx, v.flushTimeout)
	defer cancel()
	status, err := v.files.Status(ask)
	if err != nil {
		if errnoOf(err) == syscall.ESTALE && errnoOf(v.check()) != syscall.ESTALE {
			v.fence(err)
		}
		return 0, err
	}
	if err := v.confirm(start, status); err != nil {
		if errnoOf(v.check()) != syscall.ESTALE {
			v.fence(err)
		}
		return 0, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.status.ActionEpoch, nil
}

func newVolume(ctx context.Context, s storage.Storage, opts Options, logger *log.Logger) (*volume, error) {
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
	v := &volume{
		storage: s, files: files, maxFileSize: maxFileSize,
		flushTimeout: timeout, sessionOptions: limits, logger: logger,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	start := time.Now()
	status, err := files.Status(ask)
	if err == nil {
		err = v.confirm(start, status)
	}
	if err != nil {
		cleanup, finish := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer finish()
		return nil, errors.Join(err, files.Close(cleanup))
	}
	v.renewContext, v.cancelRenew = context.WithCancel(context.Background())
	go v.maintain()
	return v, nil
}

func (v *volume) maintain() {
	defer close(v.done)
	for {
		v.mu.Lock()
		left := time.Until(v.deadline)
		v.mu.Unlock()
		if left <= 0 {
			v.fence(syscall.ESTALE)
			break
		}
		timer := time.NewTimer(max(left/3, time.Nanosecond))
		select {
		case <-v.stop:
			timer.Stop()
			goto retire
		case <-timer.C:
		}

		v.mu.Lock()
		deadline := v.deadline
		v.mu.Unlock()
		start := time.Now()
		ask, cancel := context.WithDeadline(v.renewContext, minTime(deadline, start.Add(v.flushTimeout)))
		status, err := v.files.Renew(ask)
		cancel()
		select {
		case <-v.stop:
			goto retire
		default:
		}
		if err == nil {
			err = v.confirm(start, status)
		}
		if err != nil && (errnoOf(err) == syscall.ESTALE || !time.Now().Before(deadline)) {
			select {
			case <-v.stop:
				goto retire
			default:
			}
			v.fence(err)
			break
		}
	}
retire:
	cleanup, cancel := context.WithTimeout(context.Background(), v.flushTimeout)
	err := v.files.Close(cleanup)
	cancel()
	v.mu.Lock()
	v.closeErr = errors.Join(v.fault, err)
	result := v.closeErr
	v.mu.Unlock()
	if result != nil && v.logger != nil {
		v.logger.Printf("file session retirement: %v", result)
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
