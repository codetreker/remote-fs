package fuse

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

type sessionLifecycleStorage struct {
	storage.FileStorage
	wrap func(storage.FileSession) storage.FileSession
}

func (s sessionLifecycleStorage) NewFileSession(ctx context.Context, opts storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	fileSession, status, err := s.FileStorage.NewFileSession(ctx, opts)
	if err != nil {
		return nil, status, err
	}
	return s.wrap(fileSession), status, nil
}

type sessionLifecycleProbe struct {
	storage.FileSession
	renew func(context.Context) (storage.FileSessionStatus, error)
	close func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
}

func (s *sessionLifecycleProbe) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	if s.renew != nil {
		return s.renew(ctx)
	}
	return s.FileSession.Renew(ctx)
}

func (s *sessionLifecycleProbe) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if s.close != nil {
		return s.close(ctx, id)
	}
	return s.FileSession.Close(ctx, id)
}

func lifecycleVolume(t *testing.T, wrap func(storage.FileSession) storage.FileSession) (*volume, storage.FileStorage) {
	t.Helper()
	_, backing := memoryfixture.New(t, "fuse-session", 0, locking.DefaultOptions())
	if err := backing.Write(t.Context(), "file", []byte("current")); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.Lease = 150 * time.Millisecond
	v, err := newVolume(t.Context(), sessionLifecycleStorage{FileStorage: backing, wrap: wrap}, Options{
		FileSession: &options, FlushTimeout: time.Second,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.stopSession() })
	return v, backing
}

func waitLifecycle(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("file session lifecycle did not finish")
	}
}

func TestFileSessionRenewalKeepsReferencesLiveAndRetiresThem(t *testing.T) {
	renewed := make(chan struct{})
	var calls atomic.Int32
	v, backing := lifecycleVolume(t, func(native storage.FileSession) storage.FileSession {
		return &sessionLifecycleProbe{FileSession: native, renew: func(ctx context.Context) (storage.FileSessionStatus, error) {
			status, err := native.Renew(ctx)
			if err == nil && calls.Add(1) == 4 {
				close(renewed)
			}
			return status, err
		}}
	})
	file, err := retainLifecycleFile(t.Context(), v, storage.ReadContent)
	if err != nil {
		t.Fatal(err)
	}
	waitLifecycle(t, renewed)
	if err := v.check(); err != nil {
		t.Fatalf("renewed volume: %v", err)
	}
	read, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 7})
	if err != nil || string(read.Data) != "current" {
		t.Fatalf("live reference after renewal: %q, %v", read.Data, err)
	}
	if err := v.stopSession(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 7}); errnoOf(err) != syscall.ESTALE && errnoOf(err) != syscall.EBADF {
		t.Fatalf("retired reference read: %v", err)
	}
	if _, err := backing.Read(t.Context(), "file"); err != nil {
		t.Fatalf("mount retired caller-owned storage: %v", err)
	}
}

func TestFileSessionContinuityLossFencesAndRetiresTheAuthority(t *testing.T) {
	v, _ := lifecycleVolume(t, func(native storage.FileSession) storage.FileSession {
		return &sessionLifecycleProbe{FileSession: native, renew: func(context.Context) (storage.FileSessionStatus, error) {
			return storage.FileSessionStatus{}, syscall.ESTALE
		}}
	})
	file, err := retainLifecycleFile(t.Context(), v, storage.ReadContent|storage.WriteContent)
	if err != nil {
		t.Fatal(err)
	}
	waitLifecycle(t, v.done)
	if errnoOf(v.check()) != syscall.EIO || errnoOf(v.stopSession()) != syscall.EIO {
		t.Fatalf("lost continuity did not persist failure: check=%v close=%v", v.check(), v.stopSession())
	}
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("lost")}, lifecycleAction(t, v)); errnoOf(err) != syscall.ESTALE && errnoOf(err) != syscall.EBADF {
		t.Fatalf("authority accepted write after local session fence: %v", err)
	}
}

func TestFileSessionFailedRenewalCannotExtendItsConfirmedLifetime(t *testing.T) {
	entered := make(chan struct{})
	v, _ := lifecycleVolume(t, func(native storage.FileSession) storage.FileSession {
		return &sessionLifecycleProbe{FileSession: native, renew: func(ctx context.Context) (storage.FileSessionStatus, error) {
			close(entered)
			<-ctx.Done()
			return storage.FileSessionStatus{}, ctx.Err()
		}}
	})
	waitLifecycle(t, entered)
	waitLifecycle(t, v.done)
	if errnoOf(v.check()) != syscall.EIO {
		t.Fatalf("expired renewal accepted I/O: %v", v.check())
	}
	if _, err := retainLifecycleFile(t.Context(), v, storage.ReadContent); errnoOf(err) != syscall.ESTALE {
		t.Fatalf("expired authority accepted a new reference: %v", err)
	}
}

func TestFileSessionStopCancelsRenewalAndReturnsCleanupFailureOnce(t *testing.T) {
	entered := make(chan struct{})
	fault := errors.New("retirement response unavailable")
	var closes atomic.Int32
	v, _ := lifecycleVolume(t, func(native storage.FileSession) storage.FileSession {
		return &sessionLifecycleProbe{FileSession: native,
			renew: func(ctx context.Context) (storage.FileSessionStatus, error) {
				close(entered)
				<-ctx.Done()
				return storage.FileSessionStatus{}, ctx.Err()
			},
			close: func(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
				closes.Add(1)
				receipt, err := native.Close(ctx, id)
				receipt.Errno = syscall.EIO
				return receipt, errors.Join(err, fault)
			},
		}
	})
	waitLifecycle(t, entered)
	first := v.stopSession()
	second := v.stopSession()
	if !errors.Is(first, fault) || second != first || closes.Load() != 1 {
		t.Fatalf("cleanup result first=%v second=%v calls=%d", first, second, closes.Load())
	}
	if err := v.check(); err != syscall.ESTALE {
		t.Fatalf("normal retirement invented renewal continuity failure: %v", err)
	}
}

func TestFileSessionStatusDoesNotAddReplyTransitToItsLifetime(t *testing.T) {
	v := &volume{stop: make(chan struct{}), done: make(chan struct{})}
	status := storage.FileSessionStatus{Epoch: "epoch", Revision: 1, ActionEpoch: 1,
		Remaining: time.Second, HistoryRemaining: time.Second}
	if err := v.confirm(time.Now().Add(-2*time.Second), status); err != syscall.ESTALE {
		t.Fatalf("late reply received a fresh lifetime: %v", err)
	}
	if !v.deadline.IsZero() {
		t.Fatalf("rejected reply extended lifetime to %v", v.deadline)
	}
}

type delayedStatusSession struct {
	storage.FileSession
	entered chan struct{}
	release chan struct{}
	status  storage.FileSessionStatus
}

func (s delayedStatusSession) Status(context.Context) (storage.FileSessionStatus, error) {
	close(s.entered)
	<-s.release
	return s.status, nil
}

func TestFileSessionDelayedStatusCannotUndoAConfirmedRenewal(t *testing.T) {
	start := time.Now()
	older := storage.FileSessionStatus{Epoch: "epoch", Revision: 1, ActionEpoch: 1,
		Remaining: 0, HistoryRemaining: 0}
	session := delayedStatusSession{entered: make(chan struct{}), release: make(chan struct{}), status: older}
	v := &volume{files: session, flushTimeout: time.Second, status: older,
		deadline: start.Add(time.Minute), stop: make(chan struct{}), done: make(chan struct{})}
	type result struct {
		epoch uint64
		err   error
	}
	completed := make(chan result, 1)
	go func() { epoch, err := v.actionEpoch(t.Context()); completed <- result{epoch, err} }()
	<-session.entered
	newer := older
	newer.Revision, newer.ActionEpoch, newer.Remaining = 2, 3, time.Minute
	if err := v.confirm(start, newer); err != nil {
		t.Fatal(err)
	}
	close(session.release)
	got := <-completed
	if got.err != nil || got.epoch != newer.ActionEpoch {
		t.Fatalf("delayed action epoch = %d, %v", got.epoch, got.err)
	}
	if v.status.Revision != 2 || !v.deadline.Equal(start.Add(time.Minute)) || v.fault != nil {
		t.Fatalf("late reply changed confirmed lifetime: revision=%d deadline=%v fault=%v", v.status.Revision, v.deadline, v.fault)
	}
}

func TestFileSessionSameRevisionPreservesConfirmedDeadlineAndActionEpoch(t *testing.T) {
	start := time.Now()
	status := storage.FileSessionStatus{Epoch: "epoch", Revision: 2, ActionEpoch: 3,
		Remaining: time.Minute, HistoryRemaining: time.Minute}
	v := &volume{stop: make(chan struct{}), done: make(chan struct{})}
	if err := v.confirm(start, status); err != nil {
		t.Fatal(err)
	}
	status.Remaining, status.ActionEpoch = time.Nanosecond, 2
	if err := v.confirm(start.Add(-time.Second), status); err != nil {
		t.Fatal(err)
	}
	if !v.deadline.Equal(start.Add(time.Minute)) || v.status.ActionEpoch != 3 {
		t.Fatalf("same revision regressed confirmed state: deadline=%v action=%d", v.deadline, v.status.ActionEpoch)
	}
}

type handshakeKernel struct {
	unmount func() error
}

func (k handshakeKernel) Unmount() error { return k.unmount() }

func TestFailedMountHandshakeDetachesBeforeWaitingForSessionCleanup(t *testing.T) {
	v, backing := lifecycleVolume(t, func(native storage.FileSession) storage.FileSession { return native })
	unmounted := make(chan struct{})
	m := &Mount{volume: v, done: make(chan struct{}), server: handshakeKernel{unmount: func() error {
		close(unmounted)
		return nil
	}}}
	go func() {
		<-unmounted
		m.err = v.stopSession()
		close(m.done)
	}()
	failed := errors.New("mount readiness probe failed")
	got, err := m.ready(failed)
	if got != nil || !errors.Is(err, failed) {
		t.Fatalf("failed handshake = %v, %v", got, err)
	}
	select {
	case <-m.Done():
	default:
		t.Fatal("failed mount returned before session cleanup")
	}
	if _, err := retainLifecycleFile(t.Context(), v, storage.ReadContent); errnoOf(err) != syscall.ESTALE {
		t.Fatalf("failed mount retained active session: %v", err)
	}
	if _, err := backing.Read(t.Context(), "file"); err != nil {
		t.Fatalf("failed mount closed caller storage: %v", err)
	}
}

func TestFailedMountRollbackReturnsItsLiveOwnerAndKeepsRenewing(t *testing.T) {
	renewed := make(chan struct{})
	var calls atomic.Int32
	v, _ := lifecycleVolume(t, func(native storage.FileSession) storage.FileSession {
		return &sessionLifecycleProbe{FileSession: native, renew: func(ctx context.Context) (storage.FileSessionStatus, error) {
			status, err := native.Renew(ctx)
			if err == nil && calls.Add(1) == 4 {
				close(renewed)
			}
			return status, err
		}}
	})
	m := &Mount{volume: v, done: make(chan struct{}), server: handshakeKernel{unmount: func() error { return syscall.EBUSY }}}
	failed := errors.New("mount readiness probe failed")
	got, err := m.ready(failed)
	if got != m || !errors.Is(err, failed) || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("busy rollback lost live ownership: mount=%v error=%v", got, err)
	}
	waitLifecycle(t, renewed)
	if err := v.check(); err != nil {
		t.Fatalf("busy failed mount lost renewal: %v", err)
	}
	select {
	case <-m.Done():
		t.Fatal("busy mount reported completed teardown")
	default:
	}
	m.server = handshakeKernel{unmount: func() error {
		m.err = v.stopSession()
		close(m.done)
		return nil
	}}
	if err := m.Unmount(); err != nil {
		t.Fatal(err)
	}
}

func TestFailedMountHandshakeRetainsIndependentTeardownErrors(t *testing.T) {
	handshakeErr := errors.New("readiness failed")
	unmountErr := errors.New("unmount result unavailable")
	retirementErr := errors.New("session retirement unavailable")
	m := &Mount{done: make(chan struct{})}
	m.server = handshakeKernel{unmount: func() error {
		m.err = retirementErr
		close(m.done)
		return unmountErr
	}}
	got, err := m.ready(handshakeErr)
	if got != nil || !errors.Is(err, handshakeErr) || !errors.Is(err, unmountErr) || !errors.Is(err, retirementErr) {
		t.Fatalf("completed failed setup lost a cause: mount=%v error=%v", got, err)
	}
}

func retainLifecycleFile(ctx context.Context, v *volume, uses storage.AccessUse) (storage.File, error) {
	if err := v.check(); err != nil {
		return nil, err
	}
	attr, err := v.storage.Stat(ctx, "file")
	if err != nil {
		return nil, err
	}
	file, _, err := v.retainNode(ctx, attr.ID, storage.AccessClaim{Uses: uses})
	return file, err
}
func lifecycleAction(t *testing.T, v *volume) storage.FileActionID {
	t.Helper()
	v.mu.Lock()
	epoch := v.status.ActionEpoch
	v.mu.Unlock()
	id, err := storage.NewFileActionID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
