package fuse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/storage"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type rangeSessionFixture struct {
	storage.FileSession
	engine        *advisory.Session
	pending       chan struct{}
	afterApply    func(storage.RangeAttempt)
	afterConflict func()
	loseReply     bool
	cancelErr     error
	cleanCancel   bool
	retireEntered chan struct{}
	retireRelease chan struct{}
	retireErr     error
	lastOptions   storage.OwnerOptions
	conflict      *storage.RangeConflict
}

func (s *rangeSessionFixture) CheckUseOwners() error    { return nil }
func (s *rangeSessionFixture) CheckRangeControl() error { return nil }
func (s *rangeSessionFixture) NewUseOwner(ctx context.Context, node uint64, scope storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	s.lastOptions = options
	return s.engine.NewOwner(ctx, node, scope, options)
}
func (s *rangeSessionFixture) RetireUseOwner(ctx context.Context, owner storage.UseOwner) error {
	if s.retireEntered != nil {
		select {
		case s.retireEntered <- struct{}{}:
		default:
		}
		select {
		case <-s.retireRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.retireErr != nil {
		return s.retireErr
	}
	return s.engine.RetireOwner(ctx, owner)
}
func (s *rangeSessionFixture) GetConflict(ctx context.Context, owner storage.UseOwner, c storage.RangeCommand) (storage.RangeConflict, error) {
	if s.conflict != nil {
		return *s.conflict, nil
	}
	node, err := s.engine.OwnerNode(ctx, owner)
	if err != nil {
		return storage.RangeConflict{}, err
	}
	result, err := s.engine.GetConflict(ctx, node, owner, c, rangeFixtureOrder)
	if err == nil && s.afterConflict != nil {
		s.afterConflict()
	}
	return result, err
}
func (s *rangeSessionFixture) Apply(ctx context.Context, owner storage.UseOwner, c []storage.RangeCommand, id storage.LockRequestID) (storage.RangeAttempt, error) {
	node, err := s.engine.OwnerNode(ctx, owner)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	result, err := s.engine.Apply(ctx, node, owner, c, id, rangeFixtureOrder)
	if err == nil && result.State == storage.Pending && s.pending != nil {
		select {
		case s.pending <- struct{}{}:
		default:
		}
	}
	if err == nil && s.afterApply != nil {
		s.afterApply(result)
	}
	if err == nil && s.loseReply {
		return storage.RangeAttempt{}, context.Canceled
	}
	return result, err
}
func (s *rangeSessionFixture) Query(ctx context.Context, owner storage.UseOwner, id storage.LockRequestID) (storage.RangeAttempt, error) {
	node, err := s.engine.OwnerNode(ctx, owner)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return s.engine.Query(ctx, node, owner, id)
}
func (s *rangeSessionFixture) Cancel(ctx context.Context, owner storage.UseOwner, id storage.LockRequestID) (storage.RangeAttempt, error) {
	_, deadline := ctx.Deadline()
	s.cleanCancel = ctx.Err() == nil && deadline
	if s.cancelErr != nil {
		return storage.RangeAttempt{}, s.cancelErr
	}
	node, err := s.engine.OwnerNode(ctx, owner)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return s.engine.Cancel(ctx, node, owner, id)
}
func (s *rangeSessionFixture) Drop(ctx context.Context, owner storage.UseOwner, domain storage.ConflictDomain) error {
	node, err := s.engine.OwnerNode(ctx, owner)
	if err != nil {
		return err
	}
	return s.engine.Drop(ctx, node, owner, domain)
}
func (s *rangeSessionFixture) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	epoch, err := s.engine.Epoch(ctx)
	return storage.FileSessionStatus{Epoch: "range-fixture", Revision: 1, ActionEpoch: epoch, Remaining: time.Minute, HistoryRemaining: time.Minute}, err
}
func rangeFixtureOrder(ctx context.Context, apply func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return apply()
}

type rangeFileFixture struct {
	storage.File
	scope    string
	checkErr error
	scopeErr error
}

func (f *rangeFileFixture) CheckScopedReference() error { return f.checkErr }
func (f *rangeFileFixture) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: f.scope}, f.scopeErr
}

func localRangeFixture(t *testing.T, owners int) (*volume, *rangeSessionFixture) {
	t.Helper()
	c, err := advisory.New(advisory.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxLockOwners = owners
	engine, err := c.NewSession(options, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := engine.Retire(context.Background()); err != nil {
			t.Error(err)
		}
	})
	session := &rangeSessionFixture{engine: engine, pending: make(chan struct{}, 1)}
	v := &volume{files: session, flushTimeout: time.Second, sessionOptions: options, stop: make(chan struct{}), done: make(chan struct{})}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := v.confirm(time.Now(), status); err != nil {
		t.Fatal(err)
	}
	return v, session
}
func localRangeHandle(v *volume, id uint64, scope string) *handle {
	return newHandle(&node{volume: v, id: &identity{node: id, kind: syscall.S_IFREG}}, &rangeFileFixture{scope: scope}, true, true)
}

func TestLockOwnersRequireValidatedReferenceScopes(t *testing.T) {
	v, _ := localRangeFixture(t, 4)
	cause := errors.New("scope capability unavailable")
	for _, file := range []*rangeFileFixture{
		{scope: "scope", checkErr: cause},
		{scope: "scope", scopeErr: cause},
		{scope: ""},
	} {
		h := newHandle(&node{volume: v, id: &identity{node: 1, kind: syscall.S_IFREG}}, file, true, true)
		if _, err := h.lockOwner(t.Context(), 1, storage.DomainRecord, 1, true); err == nil {
			t.Fatal("unvalidated reference scope registered an owner")
		}
		if len(v.lockOwners) != 0 || len(v.lockGroups) != 0 {
			t.Fatal("failed scope validation retained local owner state")
		}
	}
}

func TestFailedOwnerRetirementCannotMasqueradeAsInterruptedAcquisition(t *testing.T) {
	v, session := localRangeFixture(t, 4)
	h := localRangeHandle(v, 1, "scope")
	owner, err := h.lockOwner(t.Context(), 9, storage.DomainRecord, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.releaseLockOwner(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	session.retireEntered = make(chan struct{}, 1)
	session.retireRelease = make(chan struct{})
	session.retireErr = errors.New("retirement outcome unknown")
	retired := make(chan error, 1)
	go func() { retired <- h.dropRecordOwner(t.Context(), 9) }()
	select {
	case <-session.retireEntered:
	case <-time.After(time.Second):
		t.Fatal("owner retirement did not start")
	}
	retried := make(chan error, 1)
	go func() {
		_, err := h.lockOwner(t.Context(), 9, storage.DomainRecord, 1, true)
		retried <- err
	}()
	select {
	case err := <-retried:
		t.Fatalf("acquisition classified unfinished retirement as %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(session.retireRelease)
	if err := <-retired; !errors.Is(err, session.retireErr) {
		t.Fatalf("retirement returned %v", err)
	}
	if err := <-retried; !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EINTR) {
		t.Fatalf("acquisition after failed retirement returned %v", err)
	}
	if errnoOf(v.check()) != syscall.EIO {
		t.Fatal("failed retirement did not fence the mount")
	}
}

func TestKernelOwnersUseOpaqueGroupsAndReclaimAfterClose(t *testing.T) {
	v, session := localRangeFixture(t, 4)
	a, b := localRangeHandle(v, 1, "a"), localRangeHandle(v, 2, "b")
	first, err := a.lockOwner(t.Context(), 0, storage.DomainRecord, 123, true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := b.lockOwner(t.Context(), 0, storage.DomainRecord, 123, true)
	if err != nil {
		t.Fatal(err)
	}
	other, err := a.lockOwner(t.Context(), math.MaxUint64, storage.DomainRecord, 456, true)
	if err != nil {
		t.Fatal(err)
	}
	if first.id == second.id || first.id == other.id || v.lockGroups[0].id == 0 || v.lockGroups[0].id == v.lockGroups[math.MaxUint64].id {
		t.Fatal("kernel identities were collapsed or exposed as absent groups")
	}
	if v.lockGroups[0].owners != 2 || session.lastOptions.Diagnostic != 456 {
		t.Fatal("cross-file process group or diagnostic PID was lost")
	}
	for _, owner := range []*localLockOwner{first, second, other} {
		if err := a.releaseLockOwner(t.Context(), owner); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.dropRecordOwner(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	if err := b.dropRecordOwner(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	if err := a.dropRecordOwner(t.Context(), math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	if len(v.lockOwners) != 0 || len(v.lockGroups) != 0 {
		t.Fatal("closed owners retained local capacity")
	}
	fresh, err := a.lockOwner(t.Context(), 0, storage.DomainRecord, 123, true)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.id == first.id {
		t.Fatal("close reused the retired authority owner")
	}
}

func TestGetlkRejectsMissingOrOversizedConflictPID(t *testing.T) {
	for _, diagnostic := range []storage.OwnerDiagnostic{0, storage.OwnerDiagnostic(math.MaxUint32) + 1} {
		t.Run(fmt.Sprintf("diagnostic=%d", diagnostic), func(t *testing.T) {
			v, session := localRangeFixture(t, 4)
			session.conflict = &storage.RangeConflict{Found: true, Owner: diagnostic, Range: storage.Range{Kind: storage.Bytes, Length: 1}, Mode: storage.RangeExclusive}
			h := localRangeHandle(v, 1, "scope")
			lk := gofuse.FileLock{Start: 0, End: 0, Typ: syscall.F_WRLCK, Pid: 10}
			var out gofuse.FileLock
			if errno := h.Getlk(t.Context(), 1, &lk, 0, &out); errno != syscall.EIO {
				t.Fatalf("Getlk returned %v with output %+v", errno, out)
			}
		})
	}
}

func TestGetlkOnlyOwnersDoNotConsumePersistentCapacity(t *testing.T) {
	v, _ := localRangeFixture(t, 1)
	h := localRangeHandle(v, 1, "query")
	lk := gofuse.FileLock{Start: 0, End: math.MaxInt64, Typ: syscall.F_WRLCK}
	for cookie := uint64(0); cookie < 32; cookie++ {
		var out gofuse.FileLock
		if errno := h.Getlk(t.Context(), cookie, &lk, 0, &out); errno != 0 || out.Typ != syscall.F_UNLCK {
			t.Fatalf("query %d: %+v %v", cookie, out, errno)
		}
		if len(v.lockOwners) != 0 || len(v.lockGroups) != 0 {
			t.Fatal("observational owner leaked after query")
		}
	}
	if errno := h.Setlk(t.Context(), 0, &lk, 0); errno != 0 {
		t.Fatal(errno)
	}
}

func TestAnyDescriptorCloseRetiresPendingRecordOwner(t *testing.T) {
	v, session := localRangeFixture(t, 8)
	holder, waiter, closing := localRangeHandle(v, 1, "holder"), localRangeHandle(v, 1, "waiter"), localRangeHandle(v, 1, "another-fd")
	lk := gofuse.FileLock{Start: 10, End: 19, Typ: syscall.F_WRLCK, Pid: 42}
	if errno := holder.Setlk(t.Context(), 1, &lk, 0); errno != 0 {
		t.Fatal(errno)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	result := make(chan syscall.Errno, 1)
	go func() { result <- waiter.Setlkw(ctx, 2, &lk, 0) }()
	select {
	case <-session.pending:
	case <-ctx.Done():
		t.Fatal("waiter failed to enroll")
	}
	if err := closing.dropRecordOwner(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	select {
	case errno := <-result:
		if errno != syscall.EINTR {
			t.Fatalf("closed pending owner returned %v", errno)
		}
	case <-ctx.Done():
		t.Fatal("closed pending owner did not finish")
	}
	if err := holder.dropRecordOwner(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if errno := closing.Setlk(t.Context(), 3, &lk, 0); errno != 0 {
		t.Fatalf("retired waiter reacquired after release: %v", errno)
	}
}

func TestKernelLockCommandsKeepDomainsAndConversionRules(t *testing.T) {
	for _, flags := range []uint32{0, gofuse.FUSE_LK_FLOCK} {
		for _, kind := range []uint32{syscall.F_RDLCK, syscall.F_WRLCK, syscall.F_UNLCK} {
			t.Run(fmt.Sprintf("%d/%d", flags, kind), func(t *testing.T) {
				command, err := kernelLock(&gofuse.FileLock{Start: 12, End: 23, Typ: kind, Pid: 123}, flags, true)
				if err != nil {
					t.Fatal(err)
				}
				if flags == 0 {
					if command.Domain != storage.DomainRecord || command.Range.Start != 12 || command.Range.Length != 12 || command.Conversion != storage.PreserveBeforeAcquire {
						t.Fatal(command)
					}
				} else {
					if command.Domain != storage.DomainWholeFile || command.Range.Start != 0 || command.Range.Length != 1<<63 {
						t.Fatal(command)
					}
					if kind != syscall.F_UNLCK && command.Conversion != storage.DropBeforeAcquire {
						t.Fatal(command)
					}
				}
				if kind == syscall.F_UNLCK && (command.Edit != storage.Subtract || command.Wait) {
					t.Fatal(command)
				}
			})
		}
	}
}

func TestRecordDeadlockUsesProcessGroupAcrossFiles(t *testing.T) {
	v, session := localRangeFixture(t, 8)
	aFirst, aSecond := localRangeHandle(v, 1, "a-first"), localRangeHandle(v, 2, "a-second")
	bFirst, bSecond := localRangeHandle(v, 1, "b-first"), localRangeHandle(v, 2, "b-second")
	lk := gofuse.FileLock{Start: 0, End: 99, Typ: syscall.F_WRLCK}
	if errno := aFirst.Setlk(t.Context(), 0, &lk, 0); errno != 0 {
		t.Fatal(errno)
	}
	if errno := bSecond.Setlk(t.Context(), math.MaxUint64, &lk, 0); errno != 0 {
		t.Fatal(errno)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pending := make(chan syscall.Errno, 1)
	go func() { pending <- aSecond.Setlkw(ctx, 0, &lk, 0) }()
	select {
	case <-session.pending:
	case <-time.After(time.Second):
		t.Fatal("first process failed to enter wait graph")
	}
	if errno := bFirst.Setlkw(t.Context(), math.MaxUint64, &lk, 0); errno != syscall.EDEADLK {
		t.Fatalf("cross-file process cycle returned %v", errno)
	}
	cancel()
	select {
	case errno := <-pending:
		if errno != syscall.EINTR {
			t.Fatal(errno)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled process remained queued")
	}
}

func TestRangeRejectionsKeepTheirLinuxErrors(t *testing.T) {
	cases := map[storage.RejectionCode]syscall.Errno{"": 0, storage.RangeBlocked: syscall.EAGAIN, storage.RangeNotHeld: syscall.EINVAL, storage.RangeExhausted: syscall.ENOLCK, storage.RangeTooLarge: syscall.EFBIG, storage.RangeDeadlock: syscall.EDEADLK, storage.RangeInvalid: syscall.EINVAL, storage.RangeUnsupported: syscall.EOPNOTSUPP, storage.RangeExpired: syscall.ESTALE, "unknown": syscall.EIO}
	for code, want := range cases {
		if got := rangeErrno(code); got != want {
			t.Fatalf("%s=%v want=%v", code, got, want)
		}
	}
}

func TestRangeCommandCancellationDistinguishesGrantFromUnknownOutcome(t *testing.T) {
	for _, test := range []struct {
		name                  string
		blocked, lost, failed bool
		want                  syscall.Errno
	}{
		{"pending cancellation", true, false, false, syscall.EINTR},
		{"lost pending response", true, true, false, syscall.EINTR},
		{"grant survived lost response", false, true, false, 0},
		{"unknown cancellation", true, true, true, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			v, session := localRangeFixture(t, 4)
			holder, waiter := localRangeHandle(v, 1, "holder"), localRangeHandle(v, 1, "waiter")
			lk := gofuse.FileLock{Start: 0, End: math.MaxInt64, Typ: syscall.F_WRLCK}
			if test.blocked {
				if errno := holder.Setlk(t.Context(), 1, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
					t.Fatal(errno)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			session.afterApply = func(storage.RangeAttempt) { cancel() }
			session.loseReply = test.lost
			if test.failed {
				session.cancelErr = errors.New("lost cancellation response")
			}
			if errno := waiter.Setlkw(ctx, 2, &lk, gofuse.FUSE_LK_FLOCK); errno != test.want {
				t.Fatalf("cancellation=%v want=%v", errno, test.want)
			}
			if !session.cleanCancel {
				t.Fatal("cancellation did not use a finite independent context")
			}
			if test.failed {
				if errnoOf(v.check()) != syscall.EIO {
					t.Fatal("unknown grant did not fence subsequent I/O")
				}
				return
			}
			if err := v.check(); err != nil {
				t.Fatal(err)
			}
			session.afterApply = nil
			session.loseReply = false
			if test.blocked {
				if err := holder.retireDescriptionOwners(t.Context()); err != nil {
					t.Fatal(err)
				}
				next := localRangeHandle(v, 1, "next")
				if errno := next.Setlk(t.Context(), 3, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
					t.Fatalf("cancelled waiter retained a grant: %v", errno)
				}
			} else {
				next := localRangeHandle(v, 1, "next")
				if errno := next.Setlk(t.Context(), 3, &lk, gofuse.FUSE_LK_FLOCK); errno != syscall.EAGAIN {
					t.Fatalf("reported grant was not retained: %v", errno)
				}
			}
		})
	}
}

func (s *rangeSessionFixture) Close(ctx context.Context) error { return s.engine.Retire(ctx) }

func TestQueryOwnerCleanupAfterSuccessfulSessionClose(t *testing.T) {
	v, session := localRangeFixture(t, 1)
	h := localRangeHandle(v, 1, "query")
	session.afterConflict = func() {
		v.mu.Lock()
		v.stopping = true
		close(v.stop)
		v.mu.Unlock()
		go v.maintain()
		select {
		case <-v.done:
		case <-time.After(time.Second):
			t.Fatal("session close did not finish")
		}
	}
	lk := gofuse.FileLock{Start: 0, End: 99, Typ: syscall.F_WRLCK}
	var out gofuse.FileLock
	if errno := h.Getlk(t.Context(), 1, &lk, 0, &out); errno != 0 {
		t.Fatal(errno)
	}
	if len(v.lockOwners) != 0 || len(v.lockGroups) != 0 || v.closeErr != nil || v.fault != nil {
		t.Fatal("query cleanup recreated or faulted closed session ownership")
	}
}
