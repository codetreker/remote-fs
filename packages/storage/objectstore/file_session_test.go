package objectstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/internal/filebudget"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type sessionFixtureAuthority struct {
	metastore.Store
	metastore.FileStore
	metastore.BoundedLister
	*filebudget.Budget
	session metastore.FileSession
}

func (*sessionFixtureAuthority) CheckFileStore() error { return nil }
func (a *sessionFixtureAuthority) FileOperationLimits() (int64, int, time.Duration) {
	return a.Budget.FileOperationLimits()
}
func (a *sessionFixtureAuthority) AcquireMaterialization(ctx context.Context, n int64) (func(), error) {
	return a.Budget.AcquireMaterialization(ctx, n)
}
func (a *sessionFixtureAuthority) NewFileSession(_ context.Context, o storage.FileSessionOptions) (metastore.FileSession, storage.FileSessionStatus, error) {
	return a.session, storage.FileSessionStatus{Epoch: "test-session", ActionEpoch: 1, Revision: 1, Remaining: o.Lease, HistoryRemaining: o.History}, nil
}

type sessionFixtureObjects struct{ BoundedObjects }

type sessionFixtureNative struct {
	metastore.FileSession
	file       metastore.File
	retired    chan struct{}
	retireOnce sync.Once
	closeCalls atomic.Int32
	dispose    func() error
	beginClose func(context.Context, storage.FileActionID) (storage.FileActionReceipt, bool, error)
}

func (s *sessionFixtureNative) Reference(context.Context, storage.FileReferenceID) (metastore.File, bool, error) {
	return s.file, true, nil
}
func (s *sessionFixtureNative) Retire(context.Context) error {
	s.retireOnce.Do(func() { close(s.retired) })
	return nil
}
func (s *sessionFixtureNative) BeginClose(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, bool, error) {
	if s.beginClose != nil {
		return s.beginClose(ctx, id)
	}
	if err := s.Retire(ctx); err != nil {
		return storage.FileActionReceipt{}, false, err
	}
	return storage.FileActionReceipt{Action: id, State: storage.FileActionPending}, true, nil
}

func (s *sessionFixtureNative) Dispose(context.Context) error {
	if s.dispose != nil {
		return s.dispose()
	}
	return nil
}
func (s *sessionFixtureNative) Close(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s.closeCalls.Add(1)
	return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted}, nil
}

type sessionFixtureFile struct {
	metastore.File
	entered, release chan struct{}
	once             sync.Once
}

func (*sessionFixtureFile) Reference() storage.FileReferenceID { return 1 }
func (f *sessionFixtureFile) Stat(ctx context.Context, _ storage.ObservationOptions) (storage.FileObservation, error) {
	f.once.Do(func() { close(f.entered) })
	select {
	case <-f.release:
		return storage.FileObservation{Attr: storage.Attr{ID: 1, Kind: storage.NodeRegular}}, nil
	case <-ctx.Done():
		return storage.FileObservation{}, ctx.Err()
	}
}

func sessionFixture(t *testing.T, native *sessionFixtureNative) (*Storage, storage.FileSession) {
	t.Helper()
	budget, err := filebudget.New(filebudget.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	authority := &sessionFixtureAuthority{Budget: budget, session: native}
	volume := &Storage{meta: authority, objects: sessionFixtureObjects{}, cleanupContext: context.Background(), now: time.Now}
	session, _, err := volume.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := volume.CloseFileSessions(); err != nil {
			t.Error(err)
		}
	})
	return volume, session
}

func TestFileSessionCloseFencesBeforeDrainingContentOperations(t *testing.T) {
	nativeFile := &sessionFixtureFile{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(nativeFile.release) })
	defer release()
	native := &sessionFixtureNative{file: nativeFile, retired: make(chan struct{})}
	_, session := sessionFixture(t, native)
	file, err := session.Reference(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { _, err := file.Stat(t.Context(), storage.ObservationOptions{}); readDone <- err }()
	<-nativeFile.entered
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { _, err := session.Close(t.Context(), action); closeDone <- err }()
	<-native.retired
	if native.closeCalls.Load() != 0 {
		t.Fatal("native pin released before the admitted operation drained")
	}
	if _, err := file.Stat(t.Context(), storage.ObservationOptions{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("new operation after fence: %v", err)
	}
	release()
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if native.closeCalls.Load() != 1 {
		t.Fatal("native close was not completed once")
	}
}

func TestFileSessionShutdownRetainsFailedCleanupForRetry(t *testing.T) {
	var attempts atomic.Int32
	native := &sessionFixtureNative{retired: make(chan struct{}), dispose: func() error {
		if attempts.Add(1) == 1 {
			return syscall.EIO
		}
		return nil
	}}
	volume, _ := sessionFixture(t, native)
	if err := volume.CloseFileSessions(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("first cleanup=%v", err)
	}
	if err := volume.CloseFileSessions(); err != nil {
		t.Fatalf("cleanup retry=%v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("cleanup attempts=%d", attempts.Load())
	}
}

type sessionWaitFile struct {
	metastore.File
	entered, release chan struct{}
}

func (*sessionWaitFile) Reference() storage.FileReferenceID { return 1 }
func (f *sessionWaitFile) WaitRanges(ctx context.Context, _ storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	close(f.entered)
	select {
	case <-f.release:
		if err := ctx.Err(); err != nil {
			return storage.FileActionReceipt{}, err
		}
		return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted}, nil
	case <-ctx.Done():
		return storage.FileActionReceipt{}, ctx.Err()
	}
}
func (*sessionWaitFile) Stat(ctx context.Context, _ storage.ObservationOptions) (storage.FileObservation, error) {
	<-ctx.Done()
	return storage.FileObservation{}, ctx.Err()
}

func TestFileRangeWaitSurvivesTheContentOperationDeadline(t *testing.T) {
	nativeFile := &sessionWaitFile{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(nativeFile.release) })
	defer release()
	native := &sessionFixtureNative{file: nativeFile, retired: make(chan struct{})}
	volume, session := sessionFixture(t, native)
	config := filebudget.DefaultConfig()
	config.FileOperationTimeout = time.Millisecond
	budget, err := filebudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	volume.meta.(*sessionFixtureAuthority).Budget = budget
	file, err := session.Reference(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		_, err := file.WaitRanges(t.Context(), storage.RangeWaitRequest{ExpectedRevision: 1}, action)
		waitDone <- err
	}()
	<-nativeFile.entered
	if _, err := file.Stat(t.Context(), storage.ObservationOptions{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded operation=%v", err)
	}
	release()
	if err := <-waitDone; err != nil {
		t.Fatalf("revision wait inherited content deadline: %v", err)
	}
}

type orderedRenewSession struct {
	*sessionFixtureNative
	calls                      atomic.Int32
	firstEntered, releaseFirst chan struct{}
}

func (s *orderedRenewSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	if s.calls.Add(1) == 1 {
		close(s.firstEntered)
		select {
		case <-s.releaseFirst:
			return storage.FileSessionStatus{Revision: 2, ActionEpoch: 1, Remaining: time.Nanosecond}, nil
		case <-ctx.Done():
			return storage.FileSessionStatus{}, ctx.Err()
		}
	}
	return storage.FileSessionStatus{Revision: 3, ActionEpoch: 1, Remaining: time.Hour}, nil
}
func (*orderedRenewSession) Status(context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{Revision: 3, ActionEpoch: 1, Remaining: time.Hour}, nil
}

func TestFileSessionOlderRenewalCannotShortenConfirmedLifetime(t *testing.T) {
	base := &sessionFixtureNative{retired: make(chan struct{})}
	_, session := sessionFixture(t, base)
	native := &orderedRenewSession{sessionFixtureNative: base, firstEntered: make(chan struct{}), releaseFirst: make(chan struct{})}
	release := sync.OnceFunc(func() { close(native.releaseFirst) })
	defer release()
	session.(*fileSession).native = native
	firstDone := make(chan error, 1)
	go func() { _, err := session.Renew(t.Context()); firstDone <- err }()
	<-native.firstEntered
	if status, err := session.Renew(t.Context()); err != nil || status.Retired || status.Remaining < time.Minute {
		t.Fatalf("newer renewal=%+v,%v", status, err)
	}
	release()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if status, err := session.Status(t.Context()); err != nil || status.Retired || status.Remaining < time.Minute {
		t.Fatalf("older reply shortened newer confirmed lifetime: %+v,%v", status, err)
	}
}

type rejectedCloseFile struct {
	metastore.File
	rejection   syscall.Errno
	retireCalls atomic.Int32
}

func (*rejectedCloseFile) Reference() storage.FileReferenceID { return 1 }
func (*rejectedCloseFile) NodeID() uint64                     { return 1 }
func (*rejectedCloseFile) Stat(context.Context, storage.ObservationOptions) (storage.FileObservation, error) {
	return storage.FileObservation{Attr: storage.Attr{ID: 1, Kind: storage.NodeRegular}}, nil
}
func (f *rejectedCloseFile) BeginClose(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, bool, error) {
	return storage.FileActionReceipt{}, false, &storage.FileError{Code: f.rejection, NotAdmitted: true}
}
func (f *rejectedCloseFile) Retire(context.Context) error { f.retireCalls.Add(1); return nil }

func TestRejectedCloseAdmissionLeavesLiveReferencesUsable(t *testing.T) {
	for _, scope := range []string{"reference", "session"} {
		for _, failure := range []struct {
			name  string
			epoch uint64
			errno syscall.Errno
		}{
			{"future epoch", 2, syscall.ESTALE}, {"conflicting action", 1, syscall.EINVAL},
		} {
			t.Run(scope+"/"+failure.name, func(t *testing.T) {
				nativeFile := &rejectedCloseFile{rejection: failure.errno}
				native := &sessionFixtureNative{file: nativeFile, retired: make(chan struct{})}
				native.beginClose = func(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, bool, error) {
					return storage.FileActionReceipt{}, false, &storage.FileError{Code: failure.errno, NotAdmitted: true}
				}
				_, session := sessionFixture(t, native)
				file, err := session.Reference(t.Context(), 1)
				if err != nil {
					t.Fatal(err)
				}
				action, err := storage.NewFileActionID(failure.epoch)
				if err != nil {
					t.Fatal(err)
				}
				var result storage.FileActionReceipt
				if scope == "reference" {
					result, err = file.Close(t.Context(), action)
				} else {
					result, err = session.Close(t.Context(), action)
				}
				if !errors.Is(err, failure.errno) || result.State != 0 || !storage.IsFileCallNotAdmitted(err) {
					t.Fatalf("close=%+v,%v", result, err)
				}
				if _, err := file.Stat(t.Context(), storage.ObservationOptions{}); err != nil {
					t.Fatalf("rejected close retired live access: %v", err)
				}
				if nativeFile.retireCalls.Load() != 0 || native.closeCalls.Load() != 0 {
					t.Fatal("rejected action performed lifetime effects")
				}
				select {
				case <-native.retired:
					t.Fatal("rejected session close retired native session")
				default:
				}
			})
		}
	}
}

type historyReference struct {
	metastore.File
	id   storage.FileReferenceID
	live bool
}

func (f *historyReference) Reference() storage.FileReferenceID { return f.id }
func (f *historyReference) NodeID() uint64                     { return uint64(f.id) }
func (f *historyReference) Stat(context.Context, storage.ObservationOptions) (storage.FileObservation, error) {
	if !f.live {
		return storage.FileObservation{}, syscall.EBADF
	}
	return storage.FileObservation{Attr: storage.Attr{ID: uint64(f.id), Kind: storage.NodeRegular}}, nil
}
func (f *historyReference) BeginClose(context.Context, storage.FileActionID) (storage.FileActionReceipt, bool, error) {
	return storage.FileActionReceipt{State: storage.FileActionRetired, Reference: f.id}, false, nil
}

type historyReferenceSession struct{ *sessionFixtureNative }

func (*historyReferenceSession) Reference(_ context.Context, id storage.FileReferenceID) (metastore.File, bool, error) {
	live := id == 99
	return &historyReference{id: id, live: live}, live, nil
}

func TestTerminalReferenceHistoryPreservesCleanupWithoutConsumingLiveCapacity(t *testing.T) {
	base := &sessionFixtureNative{retired: make(chan struct{})}
	_, session := sessionFixture(t, base)
	adapter := session.(*fileSession)
	adapter.native = &historyReferenceSession{sessionFixtureNative: base}
	adapter.options.MaxFiles = 1
	for id := storage.FileReferenceID(1); id <= 3; id++ {
		file, err := session.Reference(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Stat(t.Context(), storage.ObservationOptions{}); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("terminal reference has data access: %v", err)
		}
	}
	live, err := session.Reference(t.Context(), 99)
	if err != nil {
		t.Fatalf("history consumed live reference capacity: %v", err)
	}
	if _, err := live.Stat(t.Context(), storage.ObservationOptions{}); err != nil {
		t.Fatal(err)
	}
	closeID, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Close(t.Context(), closeID); err != nil {
		t.Fatal(err)
	}
	terminal, err := session.Reference(t.Context(), 1)
	if err != nil {
		t.Fatalf("closed-session cleanup resolution=%v", err)
	}
	result, err := terminal.Close(t.Context(), closeID)
	if err != nil || result.State != storage.FileActionRetired || result.Reference != 1 || result.Action != "" {
		t.Fatalf("terminal cleanup=%+v,%v", result, err)
	}
	if _, err := terminal.Stat(t.Context(), storage.ObservationOptions{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("closed-session data access=%v", err)
	}
	if _, err := terminal.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("x")}, closeID); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("closed-session write=%v", err)
	}
}

type cleanupAdmissionProbe struct {
	metastore.FileSession
	entered     chan struct{}
	release     chan struct{}
	cancelCalls atomic.Int32
}

func (p *cleanupAdmissionProbe) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	p.entered <- struct{}{}
	select {
	case <-p.release:
		return storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionPending}, nil
	case <-ctx.Done():
		return storage.FileActionReceipt{}, ctx.Err()
	}
}

func (p *cleanupAdmissionProbe) CancelAction(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	p.cancelCalls.Add(1)
	return storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: storage.FileActionNotApplied, Errno: syscall.EINTR}, syscall.EINTR
}

func TestFileCancellationAdmissionPreservesTheTargetActionState(t *testing.T) {
	probe := &cleanupAdmissionProbe{entered: make(chan struct{}, 2), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(probe.release) })
	defer release()
	_, session := sessionFixture(t, &sessionFixtureNative{FileSession: probe, retired: make(chan struct{})})
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := session.QueryAction(t.Context(), action)
			finished <- err
		}()
		<-probe.entered
	}
	receipt, err := session.CancelAction(t.Context(), action)
	if !errors.Is(err, syscall.EAGAIN) || !storage.IsFileCallNotAdmitted(err) || receipt.State != 0 || receipt.Action != "" || probe.cancelCalls.Load() != 0 {
		t.Errorf("capacity refusal changed target action: %+v, %v; calls=%d", receipt, err, probe.cancelCalls.Load())
	}
	release()
	for range 2 {
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
	}
	receipt, err = session.CancelAction(t.Context(), action)
	if !errors.Is(err, syscall.EINTR) || storage.IsFileCallNotAdmitted(err) || receipt.Action != action || receipt.State != storage.FileActionNotApplied || receipt.Effects != 0 || probe.cancelCalls.Load() != 1 {
		t.Fatalf("admitted cancellation lost target result: %+v, %v; calls=%d", receipt, err, probe.cancelCalls.Load())
	}
}
