package smb

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type authorityTestSession struct {
	storage.FileSession
	statusFn                             func(context.Context) (storage.FileSessionStatus, error)
	renewFn                              func(context.Context, int32) (storage.FileSessionStatus, error)
	closeFn                              func(context.Context, int32) error
	statuses, renewals, closes, metadata atomic.Int32
}

func authorityTestStatus() storage.FileSessionStatus {
	return storage.FileSessionStatus{Epoch: "authority", Revision: 1, Remaining: 9 * time.Second, ActionEpoch: 1, HistoryRemaining: 30 * time.Second}
}

func newAuthorityTestSession() *authorityTestSession {
	return &authorityTestSession{
		statusFn: func(context.Context) (storage.FileSessionStatus, error) { return authorityTestStatus(), nil },
		renewFn: func(context.Context, int32) (storage.FileSessionStatus, error) {
			return storage.FileSessionStatus{}, syscall.EIO
		},
		closeFn: func(context.Context, int32) error { return nil },
	}
}

func (s *authorityTestSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	s.statuses.Add(1)
	return s.statusFn(ctx)
}
func (s *authorityTestSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	return s.renewFn(ctx, s.renewals.Add(1))
}
func (s *authorityTestSession) Close(ctx context.Context) error {
	return s.closeFn(ctx, s.closes.Add(1))
}
func (s *authorityTestSession) StatNode(context.Context, uint64) (storage.Attr, error) {
	s.metadata.Add(1)
	return storage.Attr{}, syscall.EACCES
}
func (s *authorityTestSession) SetNodeAttr(context.Context, uint64, storage.AttrChange) (storage.Attr, error) {
	s.metadata.Add(1)
	return storage.Attr{}, syscall.EACCES
}

type authorityTestBackend struct {
	storage.FileStorage
	raw                   storage.FileSession
	openErr               error
	opens, checks, closes atomic.Int32
}

func (b *authorityTestBackend) CheckFileStorage() error { b.checks.Add(1); return nil }
func (b *authorityTestBackend) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, error) {
	b.opens.Add(1)
	return b.raw, b.openErr
}
func (b *authorityTestBackend) Close() error { b.closes.Add(1); return nil }

func authorityTestConnection(t *testing.T, backend *authorityTestBackend, configure func(*Config)) (*connection, *session, *Export) {
	t.Helper()
	config := testConfig()
	config.Limits.FileSession.Lease = 9 * time.Second
	if configure != nil {
		configure(&config)
	}
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	c := registryConnection(t, server)
	s := &session{principal: Principal{SID: "S-1-5-21-31"}, trees: make(map[uint32]*tree)}
	if err := server.sessions.add(c, s); err != nil {
		t.Fatal(err)
	}
	c.sessions[s.id] = s
	export, err := server.Publish(Share{Name: "share", Volume: "trusted-volume", Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	return c, s, export
}

func authorityTestConnect(t *testing.T, c *connection, s *session, export *Export) *tree {
	t.Helper()
	h := wire.Header{SessionID: s.id}
	if _, status := c.connectVolume(WithPrincipal(t.Context(), s.principal), s, export.key, &h); status != statusOK {
		t.Fatalf("volume tree connect: %x", status)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trees[h.TreeID]
}

func authorityTestReleaseOrphans(t *testing.T, c *connection, s *session) {
	t.Helper()
	s.mu.Lock()
	err := c.closeOrphansLocked(t.Context(), s, nil)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func authorityTestConfirmation(a *authoritySession) (time.Time, uint64) {
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	return a.deadline, a.revision
}

func authorityTestCounts(a *authoritySession) (int, int) {
	a.mu.Lock()
	trees := a.refs
	a.mu.Unlock()
	a.export.server.mu.Lock()
	references := a.export.refs
	a.export.server.mu.Unlock()
	return trees, references
}

func TestAuthorityStatusKeepsConfirmedDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := &authoritySession{}
		initial := authorityTestStatus()
		initial.ActionEpoch, initial.HistoryRemaining = 0, 0
		start := time.Now()
		if err := a.acceptStatus(initial); err != nil {
			t.Fatal(err)
		}
		if !a.deadline.Equal(start.Add(initial.Remaining)) {
			t.Fatalf("initial deadline: %v", a.deadline)
		}
		time.Sleep(2 * time.Second)
		newer := initial
		newer.Revision, newer.Remaining = 2, 3*time.Second
		if err := a.acceptStatus(newer); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(newer.Remaining)
		if !a.deadline.Equal(deadline) {
			t.Fatalf("receipt-based deadline: %v, want %v", a.deadline, deadline)
		}
		for _, revision := range []uint64{1, 2} {
			stale := initial
			stale.Revision, stale.Remaining = revision, time.Hour
			if err := a.acceptStatus(stale); err != nil {
				t.Fatal(err)
			}
			if !a.deadline.Equal(deadline) || a.revision != 2 {
				t.Fatal("old reply extended a newer confirmation")
			}
		}
		for _, change := range []func(*storage.FileSessionStatus){
			func(s *storage.FileSessionStatus) { s.Epoch = "other" },
			func(s *storage.FileSessionStatus) { s.Epoch = "" },
			func(s *storage.FileSessionStatus) { s.Revision = 0 },
			func(s *storage.FileSessionStatus) { s.Remaining = 0 },
			func(s *storage.FileSessionStatus) { s.Retired = true },
			func(s *storage.FileSessionStatus) { s.Fenced = true },
		} {
			invalid := newer
			invalid.Revision = 3
			change(&invalid)
			if err := a.acceptStatus(invalid); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid status %+v: %v", invalid, err)
			}
			if !a.deadline.Equal(deadline) || a.epoch != initial.Epoch || a.revision != 2 {
				t.Fatal("invalid status changed confirmed authority")
			}
		}
		time.Sleep(time.Until(deadline))
		h := &fileHandle{authority: a}
		if release, err := h.borrow(); release != nil || !errors.Is(err, syscall.EBADF) {
			t.Fatalf("expired borrow: %v", err)
		}
		registry := &handleRegistry{tree: &tree{authority: a}}
		if p, err := registry.reserve(); p != nil || !errors.Is(err, syscall.EIO) {
			t.Fatalf("expired reservation: %v", err)
		}
		late := newer
		late.Revision, late.Remaining = 3, time.Hour
		if err := a.acceptStatus(late); !errors.Is(err, syscall.EIO) || !a.deadline.Equal(deadline) || a.revision != 2 {
			t.Fatalf("late confirmation resurrected expired continuity: %v", err)
		}
	})
}

func TestAuthorityTwoTreesShareOneRenewalWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		raw := newAuthorityTestSession()
		type renewal struct {
			principal   Principal
			deadline    time.Time
			hasDeadline bool
		}
		renewed := make(chan renewal, 4)
		raw.renewFn = func(ctx context.Context, count int32) (storage.FileSessionStatus, error) {
			principal, _ := PrincipalFromContext(ctx)
			deadline, ok := ctx.Deadline()
			renewed <- renewal{principal, deadline, ok}
			status := authorityTestStatus()
			status.Revision = uint64(count) + 1
			return status, nil
		}
		var openPolicies, statusPolicies, renewPolicies atomic.Int32
		backend := &authorityTestBackend{raw: raw}
		c, s, export := authorityTestConnection(t, backend, func(config *Config) {
			config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
				principal, ok := PrincipalFromContext(ctx)
				if !ok || principal.SID != "S-1-5-21-31" || request.Volume != "trusted-volume" {
					return authz.ErrDenied
				}
				switch request.Operation {
				case storage.OpFileSessionOpen:
					openPolicies.Add(1)
				case storage.OpFileStatus:
					statusPolicies.Add(1)
				case storage.OpFileRenew:
					renewPolicies.Add(1)
				default:
					return authz.ErrDenied
				}
				return nil
			})
		})
		first, second := authorityTestConnect(t, c, s, export), authorityTestConnect(t, c, s, export)
		if first.authority != second.authority || first.id == second.id || backend.opens.Load() != 1 || raw.statuses.Load() != 1 {
			t.Fatal("sharing trees opened independent authorities")
		}
		a := first.authority
		_, ownedRefs := authorityTestCounts(a)
		confirmed, _ := authorityTestConfirmation(a)
		for index := int32(1); index <= 2; index++ {
			time.Sleep(3 * time.Second)
			synctest.Wait()
			var call renewal
			select {
			case call = <-renewed:
			default:
				t.Fatal("renewal missed its configured interval")
			}
			if !reflect.DeepEqual(call.principal, s.principal) || !call.hasDeadline || call.deadline.After(confirmed) || !call.deadline.After(time.Now()) {
				t.Fatalf("renewal context: %+v, confirmed %v", call, confirmed)
			}
			currentDeadline, revision := authorityTestConfirmation(a)
			if raw.renewals.Load() != index || renewPolicies.Load() != index || revision != uint64(index)+1 {
				t.Fatalf("renewal workers=%d policies=%d revision=%d", raw.renewals.Load(), renewPolicies.Load(), revision)
			}
			confirmed = currentDeadline
			if index == 1 {
				if err := c.closeTreeContext(t.Context(), first); err != nil {
					t.Fatal(err)
				}
				trees, references := authorityTestCounts(a)
				if raw.closes.Load() != 0 || trees != 1 || references != ownedRefs-1 {
					t.Fatal("first tree closed the shared authority")
				}
			}
		}
		if err := c.closeTreeContext(t.Context(), second); err != nil {
			t.Fatal(err)
		}
		if err := a.wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		authorityTestReleaseOrphans(t, c, s)
		trees, references := authorityTestCounts(a)
		if !a.isClosed() || trees != 0 || references != 0 || raw.closes.Load() != 1 || backend.closes.Load() != 0 || raw.metadata.Load() != 0 {
			t.Fatal("last-tree cleanup did not retain one native close owner")
		}
		if openPolicies.Load() != 2 || statusPolicies.Load() != 1 || backend.opens.Load() != 1 || raw.statuses.Load() != 1 {
			t.Fatal("tree/operation admission probed extra lifecycle state")
		}
	})
}

func TestAuthorityRenewalFailureFencesSharingTreesAndRetainsOwnership(t *testing.T) {
	renewFailure, cleanupFailure := errors.New("renewal rejected"), errors.New("native cleanup unknown")
	for _, refusal := range []string{"renew", "policy", "epoch"} {
		t.Run(refusal, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				raw := newAuthorityTestSession()
				var cleanupAllowed atomic.Bool
				raw.closeFn = func(ctx context.Context, _ int32) error {
					principal, ok := PrincipalFromContext(ctx)
					if !ok || principal.SID != "S-1-5-21-31" {
						return syscall.EACCES
					}
					if !cleanupAllowed.Load() {
						return cleanupFailure
					}
					return nil
				}
				raw.renewFn = func(context.Context, int32) (storage.FileSessionStatus, error) {
					if refusal == "epoch" {
						status := authorityTestStatus()
						status.Epoch, status.Revision = "replacement", 2
						return status, nil
					}
					return storage.FileSessionStatus{}, renewFailure
				}
				backend := &authorityTestBackend{raw: raw}
				c, s, export := authorityTestConnection(t, backend, func(config *Config) {
					config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
						if refusal == "policy" && request.Operation == storage.OpFileRenew {
							return authz.ErrDenied
						}
						return nil
					})
				})
				t.Cleanup(func() { cleanupAllowed.Store(true) })
				first, second := authorityTestConnect(t, c, s, export), authorityTestConnect(t, c, s, export)
				a := first.authority
				deadline, _ := authorityTestConfirmation(a)
				_, ownedRefs := authorityTestCounts(a)
				time.Sleep(3 * time.Second)
				synctest.Wait()
				select {
				case <-a.done:
				default:
					t.Fatal("failed renewal did not finish retirement")
				}
				currentDeadline, _ := authorityTestConfirmation(a)
				trees, references := authorityTestCounts(a)
				if !a.isStopping() || a.isClosed() || !currentDeadline.Equal(deadline) || trees != 2 || references != ownedRefs || raw.closes.Load() != 1 {
					t.Fatal("failed retirement released or renewed sharing authority")
				}
				cause := renewFailure
				if refusal == "policy" {
					cause = authz.ErrDenied
				} else if refusal == "epoch" {
					cause = syscall.EIO
				}
				c.server.mu.Lock()
				failure := c.server.cleanupErr
				c.server.mu.Unlock()
				if !errors.Is(failure, cause) || !errors.Is(failure, cleanupFailure) {
					t.Fatalf("retirement lost its causes: %v", failure)
				}
				for _, tree := range []*tree{first, second} {
					if p, err := tree.files.reserve(); p != nil || !errors.Is(err, syscall.EIO) {
						t.Fatalf("fenced tree reservation: %v", err)
					}
				}
				wantRenewals := int32(1)
				if refusal == "policy" {
					wantRenewals = 0
				}
				if raw.renewals.Load() != wantRenewals || raw.statuses.Load() != 1 {
					t.Fatal("denied renewal reached the native authority")
				}
				cleanupAllowed.Store(true)
				for _, tree := range []*tree{first, second} {
					if err := c.closeTreeContext(t.Context(), tree); err != nil {
						t.Fatal(err)
					}
				}
				authorityTestReleaseOrphans(t, c, s)
				_, references = authorityTestCounts(a)
				if !a.isClosed() || raw.closes.Load() != 2 || references != 0 || raw.metadata.Load() != 0 {
					t.Fatal("confirmed retry did not release original authority")
				}
			})
		})
	}
}

func TestAuthorityPartialSessionErrorRetainsCleanupOwner(t *testing.T) {
	raw := newAuthorityTestSession()
	openFailure, closeFailure := errors.New("session result unknown"), errors.New("session cleanup unknown")
	raw.closeFn = func(ctx context.Context, count int32) error {
		principal, ok := PrincipalFromContext(ctx)
		if !ok || principal.SID != "S-1-5-21-31" {
			return syscall.EACCES
		}
		if count == 1 {
			return closeFailure
		}
		return nil
	}
	backend := &authorityTestBackend{raw: raw, openErr: openFailure}
	c, s, export := authorityTestConnection(t, backend, nil)
	h := wire.Header{SessionID: s.id}
	ctx := WithPrincipal(t.Context(), s.principal)
	if _, status := c.connectVolume(ctx, s, export.key, &h); status != statusIO {
		t.Fatalf("partial authority result: %x", status)
	}
	a := s.authorities[export]
	if a == nil || a.raw != raw || !a.orphan || a.isClosed() || !a.isStopping() || export.refs != 1 || export.active != 0 {
		t.Fatal("failed initialization lost its original native owner or charge")
	}
	if raw.statuses.Load() != 0 || raw.closes.Load() != 1 || backend.opens.Load() != 1 {
		t.Fatal("partial result was probed or reopened")
	}
	if _, status := c.connectVolume(ctx, s, export.key, &h); status != statusIO {
		t.Fatalf("retained initialization failure: %x", status)
	}
	if backend.opens.Load() != 1 || s.authorities[export] != a || export.refs != 1 {
		t.Fatal("retry silently replaced unresolved authority")
	}
	s.mu.Lock()
	err := c.closeOrphansLocked(ctx, s, nil)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if s.authorities[export] != nil || export.refs != 0 || raw.closes.Load() != 2 || raw.metadata.Load() != 0 || backend.closes.Load() != 0 {
		t.Fatal("orphan cleanup failed to release the same authority")
	}
}

func TestAuthorityRenewalDeadlineFencesAStalledCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		raw := newAuthorityTestSession()
		started := make(chan time.Time, 1)
		raw.renewFn = func(ctx context.Context, _ int32) (storage.FileSessionStatus, error) {
			deadline, _ := ctx.Deadline()
			started <- deadline
			<-ctx.Done()
			return storage.FileSessionStatus{}, ctx.Err()
		}
		raw.closeFn = func(ctx context.Context, _ int32) error { return ctx.Err() }
		backend := &authorityTestBackend{raw: raw}
		c, s, export := authorityTestConnection(t, backend, nil)
		first, second := authorityTestConnect(t, c, s, export), authorityTestConnect(t, c, s, export)
		a := first.authority
		confirmed, _ := authorityTestConfirmation(a)
		time.Sleep(3 * time.Second)
		synctest.Wait()
		var deadline time.Time
		select {
		case deadline = <-started:
		default:
			t.Fatal("renewal missed its configured interval")
		}
		if !deadline.After(time.Now()) || deadline.After(confirmed) || deadline.Sub(time.Now()) > 3*time.Second {
			t.Fatalf("unbounded renewal deadline: %v, confirmed %v", deadline, confirmed)
		}
		time.Sleep(time.Until(deadline))
		synctest.Wait()
		select {
		case <-a.done:
		default:
			t.Fatal("renewal outlived its bounded call deadline")
		}
		currentDeadline, _ := authorityTestConfirmation(a)
		if !a.isStopping() || !a.isClosed() || !currentDeadline.Equal(confirmed) || raw.closes.Load() != 1 {
			t.Fatal("timed-out renewal did not fence and clean up")
		}
		h := wire.Header{SessionID: s.id}
		if _, status := c.connectVolume(WithPrincipal(t.Context(), s.principal), s, export.key, &h); status != statusSessionDeleted || backend.opens.Load() != 1 {
			t.Fatalf("fenced authority was reopened: %x, opens %d", status, backend.opens.Load())
		}
		for _, tree := range []*tree{first, second} {
			if err := c.closeTreeContext(t.Context(), tree); err != nil {
				t.Fatal(err)
			}
		}
		authorityTestReleaseOrphans(t, c, s)
		_, references := authorityTestCounts(a)
		if raw.closes.Load() != 1 || raw.statuses.Load() != 1 || raw.renewals.Load() != 1 || references != 0 {
			t.Fatal("retirement repeated native operations or retained charges")
		}
	})
}

func TestAuthorityInitializationRejectsUnknownState(t *testing.T) {
	for _, kind := range []string{"nil session", "status failure", "invalid status", "status policy"} {
		t.Run(kind, func(t *testing.T) {
			raw := newAuthorityTestSession()
			backend := &authorityTestBackend{raw: raw}
			wantStatus, wantCalls, wantClose := statusIO, int32(1), int32(1)
			switch kind {
			case "nil session":
				backend.raw = nil
				wantCalls, wantClose = 0, 0
			case "status failure":
				raw.statusFn = func(context.Context) (storage.FileSessionStatus, error) {
					return storage.FileSessionStatus{}, syscall.EIO
				}
			case "invalid status":
				raw.statusFn = func(context.Context) (storage.FileSessionStatus, error) {
					status := authorityTestStatus()
					status.Remaining = 0
					return status, nil
				}
			case "status policy":
				wantStatus, wantCalls = statusDenied, 0
			}
			c, s, export := authorityTestConnection(t, backend, func(config *Config) {
				config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
					if kind == "status policy" && request.Operation == storage.OpFileStatus {
						return authz.ErrDenied
					}
					return nil
				})
			})
			h := wire.Header{SessionID: s.id}
			if _, status := c.connectVolume(WithPrincipal(t.Context(), s.principal), s, export.key, &h); status != wantStatus {
				t.Fatalf("initialization: %x, want %x", status, wantStatus)
			}
			if len(s.trees) != 0 || len(s.authorities) != 0 || export.refs != 0 || export.active != 0 || raw.statuses.Load() != wantCalls || raw.closes.Load() != wantClose || raw.metadata.Load() != 0 || backend.closes.Load() != 0 {
				t.Fatal("failed initialization retained known-clean resources or queried metadata")
			}
		})
	}
}

func TestAuthorityCloseSharesOneImmutableAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		raw := newAuthorityTestSession()
		failure := errors.New("first cleanup refused")
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		raw.closeFn = func(_ context.Context, count int32) error {
			if count == 1 {
				close(entered)
				<-release
				return failure
			}
			return nil
		}
		a := &authoritySession{raw: raw}
		var workers sync.WaitGroup
		results := make(chan error, 2)
		invoke := func() { workers.Add(1); go func() { defer workers.Done(); results <- a.close(context.Background()) }() }
		t.Cleanup(func() { unblock(); workers.Wait() })
		invoke()
		<-entered
		invoke()
		synctest.Wait()
		unblock()
		for range 2 {
			if err := <-results; !errors.Is(err, failure) {
				t.Errorf("waiter lost original attempt result: %v", err)
			}
		}
		if raw.closes.Load() != 1 || a.isClosed() || !a.isStopping() {
			t.Errorf("failed attempt retried or released native ownership: calls %d", raw.closes.Load())
		}
		if err := a.close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if raw.closes.Load() != 2 || !a.isClosed() {
			t.Fatal("later retry did not finish the original cleanup")
		}
		if err := a.close(t.Context()); err != nil || raw.closes.Load() != 2 {
			t.Fatalf("idempotent close: %v, calls %d", err, raw.closes.Load())
		}
	})
}

func TestAuthorityCloseWaiterCancellationKeepsRunningOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		raw := newAuthorityTestSession()
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		raw.closeFn = func(_ context.Context, count int32) error {
			if count == 1 {
				close(entered)
				<-release
			}
			return nil
		}
		a := &authoritySession{raw: raw}
		ctx, cancel := context.WithCancel(t.Context())
		var workers sync.WaitGroup
		t.Cleanup(func() { cancel(); unblock(); workers.Wait() })
		owner := make(chan error, 1)
		workers.Add(1)
		go func() { defer workers.Done(); owner <- a.close(context.Background()) }()
		<-entered
		waiter := make(chan error, 1)
		workers.Add(1)
		go func() { defer workers.Done(); waiter <- a.close(ctx) }()
		synctest.Wait()
		cancel()
		if err := <-waiter; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled waiter: %v", err)
		}
		if a.isClosed() || raw.closes.Load() != 1 {
			t.Fatal("waiter cancellation released the running native owner")
		}
		unblock()
		if err := <-owner; err != nil || !a.isClosed() {
			t.Fatalf("original cleanup: %v", err)
		}
	})
}

func TestAuthorityLastTreeRetiresDespiteReferenceCleanupFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		raw := newAuthorityTestSession()
		referenceFailure, sessionFailure := errors.New("reference cleanup refused"), errors.New("session cleanup refused")
		var cleanupAllowed atomic.Bool
		raw.closeFn = func(context.Context, int32) error {
			if !cleanupAllowed.Load() {
				return sessionFailure
			}
			return nil
		}
		raw.renewFn = func(context.Context, int32) (storage.FileSessionStatus, error) { return authorityTestStatus(), nil }
		c, s, export := authorityTestConnection(t, &authorityTestBackend{raw: raw}, nil)
		t.Cleanup(func() { cleanupAllowed.Store(true) })
		tree := authorityTestConnect(t, c, s, export)
		reference := &handleTestReference{closeFn: func(context.Context, int32) error {
			if !cleanupAllowed.Load() {
				return referenceFailure
			}
			return nil
		}}
		reservation, err := tree.files.reserve()
		if err != nil {
			t.Fatal(err)
		}
		reservation.attachNode(storage.NodeOpenResult{Reference: reference, Attr: storage.Attr{ID: 61, Kind: storage.NodeRegular}, Outcome: storage.Opened})
		if _, err := reservation.install(0, 0); err != nil {
			t.Fatal(err)
		}
		if err := reservation.finish(t.Context()); err != nil {
			t.Fatal(err)
		}
		_, ownedRefs := authorityTestCounts(tree.authority)
		err = c.closeTreeContext(t.Context(), tree)
		if !errors.Is(err, referenceFailure) || !errors.Is(err, sessionFailure) {
			t.Fatalf("last-tree cleanup lost an attempted owner's error: %v", err)
		}
		a := tree.authority
		if raw.closes.Load() != 1 || reference.closeCalls.Load() != 1 || !a.isStopping() || a.isClosed() || tree.closed {
			t.Fatal("reference failure prevented authority retirement")
		}
		synctest.Wait()
		select {
		case <-a.done:
		default:
			t.Fatal("last-tree retirement left the renewal worker running")
		}
		trees, references := authorityTestCounts(a)
		if references != ownedRefs || trees != 1 {
			t.Fatal("failed last-tree cleanup released original authority/tree charges")
		}
		handleTestAccounting(t, tree.files, 1, 0, 1, 0)
		time.Sleep(9 * time.Second)
		if raw.renewals.Load() != 0 {
			t.Fatal("retired last tree renewed its authority")
		}
		cleanupAllowed.Store(true)
		if err := c.closeTreeContext(t.Context(), tree); err != nil {
			t.Fatal(err)
		}
		authorityTestReleaseOrphans(t, c, s)
		handleTestAccounting(t, tree.files, 0, 0, 0, 0)
		_, references = authorityTestCounts(a)
		if raw.closes.Load() != 2 || !a.isClosed() || !tree.closed || references != 0 || raw.metadata.Load() != 0 || reference.attributeCalls.Load() != 0 {
			t.Fatal("known cleanup retry lost ownership or required metadata")
		}
	})
}
