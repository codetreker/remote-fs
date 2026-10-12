package smb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type deleteRecoveryBackendStub struct {
	endpointStorage
	mu                       sync.Mutex
	identity                 storage.BackendIdentityResult
	intents                  []storage.DeleteIntentStatus
	sessions                 []*deleteRecoverySessionStub
	newSession               func(*deleteRecoverySessionStub)
	listEntered, listRelease chan struct{}
	onList                   func(storage.DeleteIntentCursor)
}

func (b *deleteRecoveryBackendStub) BackendIdentity(context.Context) (storage.BackendIdentityResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.identity, nil
}

func (b *deleteRecoveryBackendStub) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	raw := &deleteRecoverySessionStub{endpointFileSession: newEndpointFileSession(), backend: b, identity: b.identity}
	raw.status.Epoch = fmt.Sprintf("recovery-%d", len(b.sessions)+1)
	if b.newSession != nil {
		b.newSession(raw)
	}
	b.sessions = append(b.sessions, raw)
	return raw, nil
}

type deleteRecoverySessionStub struct {
	*endpointFileSession
	backend           *deleteRecoveryBackendStub
	identity          storage.BackendIdentityResult
	stateMu           sync.Mutex
	acks              []storage.AcknowledgeDeleteIntentCommand
	ackErr            error
	deleteBeforeError bool
	closeResult       storage.ReferenceCloseResult
	settlementErr     error
	queryReceipt      storage.FileActionOutcome
}

func (s *deleteRecoverySessionStub) FileSessionIdentity(context.Context) (storage.FileSessionIdentityResult, error) {
	return storage.FileSessionIdentityResult{Backend: s.identity, SessionEpoch: s.status.Epoch}, nil
}

func (s *deleteRecoverySessionStub) QueryDeleteIntent(_ context.Context, _ storage.DeleteIntentOwner, id storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	for _, status := range s.backend.intents {
		if status.ID == id {
			return status, nil
		}
	}
	return storage.DeleteIntentStatus{ID: id, Outcome: storage.DeleteIntentUnknown}, nil
}

func (s *deleteRecoverySessionStub) ListDeleteIntents(ctx context.Context, _ storage.DeleteIntentOwner, after storage.DeleteIntentCursor, limit int) (storage.DeleteIntentPage, error) {
	if s.backend.onList != nil {
		s.backend.onList(after)
	}
	if s.backend.listEntered != nil {
		select {
		case <-s.backend.listEntered:
		default:
			close(s.backend.listEntered)
		}
		select {
		case <-s.backend.listRelease:
		case <-ctx.Done():
			return storage.DeleteIntentPage{}, ctx.Err()
		}
	}
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	page := storage.DeleteIntentPage{Next: after}
	for index := int(after); index < len(s.backend.intents) && len(page.Intents) < limit; index++ {
		status := s.backend.intents[index]
		if status.ID != "" {
			page.Intents = append(page.Intents, status)
			page.Next = storage.DeleteIntentCursor(index + 1)
		}
	}
	return page, nil
}

func (s *deleteRecoverySessionStub) QueryFileAction(_ context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	outcome := s.queryReceipt
	if outcome == 0 {
		outcome = storage.FileActionUnknown
	}
	return storage.FileActionReceipt{Action: action, Operation: storage.OpFileAcknowledgeDeleteIntent, Outcome: outcome}, nil
}

func (s *deleteRecoverySessionStub) AcknowledgeDeleteIntent(_ context.Context, command storage.AcknowledgeDeleteIntentCommand) error {
	s.stateMu.Lock()
	s.acks = append(s.acks, command)
	err, remove := s.ackErr, s.ackErr == nil || s.deleteBeforeError
	s.stateMu.Unlock()
	if remove {
		s.backend.mu.Lock()
		for index := range s.backend.intents {
			if s.backend.intents[index].ID == command.Intent {
				s.backend.intents[index].ID = ""
			}
		}
		s.backend.mu.Unlock()
	}
	return err
}

func (s *deleteRecoverySessionStub) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.closes++
	if s.settlementErr != nil {
		return s.closeResult, s.settlementErr
	}
	return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
}

func deleteRecoveryFixture(t *testing.T, config Config, outcomes ...storage.DeleteIntentOutcome) (*Server, *Export, *deleteRecoveryBackendStub, context.Context) {
	t.Helper()
	backend := &deleteRecoveryBackendStub{identity: endpointBackendIdentity()}
	for index, outcome := range outcomes {
		status := storage.DeleteIntentStatus{ID: storage.DeleteIntentID(fmt.Sprintf("%032x", index+1)), NodeID: uint64(index + 2), Outcome: outcome}
		if outcome == storage.DeleteIntentCleanupFailed {
			status.Failure = syscall.EBUSY
		}
		backend.intents = append(backend.intents, status)
	}
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	share := treeIdentityShare()
	share.DeleteIntentOwner, share.Backend = "recovery-owner", backend
	export, err := server.Publish(share)
	if err != nil {
		t.Fatal(err)
	}
	return server, export, backend, WithPrincipal(t.Context(), testPrincipal("0123456789abcdef"))
}

func TestDeleteRecoveryFinitePagesAndFreshPendingPass(t *testing.T) {
	config := endpointConfig()
	config.Limits.MaxUnresolvedOwners = 2
	server, export, backend, ctx := deleteRecoveryFixture(t, config, storage.DeleteIntentPending, storage.DeleteIntentCompleted, storage.DeleteIntentNotExecuted, storage.DeleteIntentArmed, storage.DeleteIntentCompleted)
	for step := 0; step < 2; step++ {
		if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("step %d: %v", step, err)
		}
		if export.deleteRecovery.ready.Load() {
			t.Fatal("partial pass became ready")
		}
	}
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if got := server.Status(); got.DeleteRecoveryReadyExports != 1 || got.DeleteRecoveryCleanupOwners != 0 {
		t.Fatalf("completed status = %+v", got)
	}
	if len(backend.sessions) != 1 || len(backend.sessions[0].acks) != 3 {
		t.Fatalf("sessions/acks = %d/%d", len(backend.sessions), len(backend.sessions[0].acks))
	}
	backend.intents[0].Outcome = storage.DeleteIntentCompleted
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(backend.sessions) != 2 || len(backend.sessions[1].acks) != 1 {
		t.Fatal("fresh pass did not revisit early pending")
	}
}

func TestDeleteRecoveryRequiresCurrentPrincipalAndIdentity(t *testing.T) {
	for _, scenario := range []string{"missing", "invalid", "revoked"} {
		t.Run(scenario, func(t *testing.T) {
			config := endpointConfig()
			if scenario == "revoked" {
				config.AuthorizeIdentity = IdentityAuthorizerFunc(func(context.Context, Principal) error { return ErrIdentityDenied })
			}
			_, export, backend, ctx := deleteRecoveryFixture(t, config)
			if scenario == "missing" {
				ctx = t.Context()
			}
			if scenario == "invalid" {
				ctx = WithPrincipal(t.Context(), Principal{})
			}
			if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, ErrIdentityDenied) {
				t.Fatal(err)
			}
			if len(backend.sessions) != 0 || export.deleteRecovery.ready.Load() {
				t.Fatal("identity rejection enrolled session or became ready")
			}
		})
	}
}

func TestDeleteRecoveryCurrentOperationAuthorization(t *testing.T) {
	for _, denied := range []storage.Operation{storage.OpFileBackendIdentity, storage.OpFileSessionOpen, storage.OpFileStatus, storage.OpFileListDeleteIntents, storage.OpFileQueryDeleteIntent, storage.OpFileAcknowledgeDeleteIntent, storage.OpFileSessionClose} {
		t.Run(string(denied), func(t *testing.T) {
			config := endpointConfig()
			reject := true
			config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
				principal, ok := PrincipalFromContext(ctx)
				if !ok || principal.Name != "current-host" || request.Volume != "authorization-volume" {
					t.Errorf("authorization identity/volume = %+v/%+v", principal, request)
				}
				if reject && request.Operation == denied {
					return authz.ErrDenied
				}
				return nil
			})
			_, export, _, ctx := deleteRecoveryFixture(t, config, storage.DeleteIntentCompleted)
			p, _ := PrincipalFromContext(ctx)
			p.Name = "current-host"
			ctx = WithPrincipal(ctx, p)
			if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, authz.ErrDenied) {
				t.Fatal(err)
			}
			if export.deleteRecovery.ready.Load() {
				t.Fatal("denied pass became ready")
			}
			reject = false
			if err := export.RecoverDeleteIntents(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDeleteRecoveryUnknownAckUsesTerminalWitness(t *testing.T) {
	for _, removed := range []bool{false, true} {
		t.Run(fmt.Sprintf("removed-%v", removed), func(t *testing.T) {
			_, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig(), storage.DeleteIntentCompleted)
			backend.newSession = func(raw *deleteRecoverySessionStub) { raw.ackErr, raw.deleteBeforeError = syscall.EIO, removed }
			if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			attempt := export.deleteRecovery.attempt
			first := attempt.action
			if !attempt.terminalObserved || !attempt.issued || attempt.acked || export.deleteRecovery.index != 0 {
				t.Fatalf("unknown ACK lost responsibility: %+v", attempt)
			}
			raw := backend.sessions[0]
			raw.ackErr = nil
			if err := export.RecoverDeleteIntents(ctx); err != nil {
				t.Fatal(err)
			}
			if !attempt.acked {
				t.Fatal("terminal witness did not settle ACK")
			}
			if removed {
				if len(raw.acks) != 1 {
					t.Fatal("missing record was acknowledged twice")
				}
			} else if len(raw.acks) != 2 || raw.acks[1].Action == first || attempt.priorUnknown != first {
				t.Fatalf("cached Unknown did not retain original/new exact ACK: %+v %+v", raw.acks, attempt)
			}
		})
	}
}

func TestDeleteRecoveryMissingIntentNeedsIssuedAck(t *testing.T) {
	_, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig())
	rawAny, err := backend.NewFileSession(ctx, endpointConfig().Limits.FileSession)
	if err != nil {
		t.Fatal(err)
	}
	id := storage.DeleteIntentID(fmt.Sprintf("%032x", 91))
	for _, observed := range []bool{false, true} {
		attempt := &deleteIntentAttempt{id: id, outcome: storage.DeleteIntentCompleted, terminalObserved: observed}
		p, _ := PrincipalFromContext(ctx)
		if err := export.ackDeleteIntent(ctx, rawAny, p, attempt); !errors.Is(err, syscall.EIO) || attempt.acked {
			t.Fatalf("missing was treated as completed: %v %+v", err, attempt)
		}
	}
}

func TestDeleteRecoveryCleanupFailurePreservesPage(t *testing.T) {
	_, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig(), storage.DeleteIntentCleanupFailed, storage.DeleteIntentCompleted)
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EBUSY) {
		t.Fatal(err)
	}
	if export.deleteRecovery.cursor != 0 || export.deleteRecovery.index != 0 || len(backend.sessions[0].acks) != 0 {
		t.Fatal("failure advanced past its page entry")
	}
	backend.intents[0].Outcome, backend.intents[0].Failure = storage.DeleteIntentPending, 0
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if !export.deleteRecovery.ready.Load() || len(backend.sessions) != 1 {
		t.Fatal("same-session retry failed to finish")
	}
}

func TestDeleteRecoveryReleasedSessionNeedsSettlement(t *testing.T) {
	server, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig())
	backend.newSession = func(raw *deleteRecoverySessionStub) {
		raw.closeResult = storage.ReferenceCloseResult{Released: true, Determined: true}
		raw.settlementErr = &storage.CloseSettlementError{State: storage.CloseSettlementUnknown, Cause: syscall.EIO}
	}
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if export.deleteRecovery.ready.Load() || server.Status().DeleteRecoveryCleanupOwners != 1 {
		t.Fatal("Released=true erased unsettled session")
	}
	backend.sessions[0].settlementErr = nil
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if !export.deleteRecovery.ready.Load() || len(backend.sessions) != 1 || backend.sessions[0].closes != 2 || server.Status().DeleteRecoveryCleanupOwners != 0 {
		t.Fatal("settlement did not retry exact same session")
	}
}

func TestDeleteRecoveryAuthorityChangeKeepsOldOwnerAndCurrentReady(t *testing.T) {
	server, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig())
	backend.newSession = func(raw *deleteRecoverySessionStub) { raw.settlementErr = syscall.ESTALE }
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	backend.identity.Authority = "new-authority"
	backend.newSession = nil
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	if !export.deleteRecovery.ready.Load() || len(backend.sessions) != 2 || server.Status().DeleteRecoveryCleanupOwners != 1 || server.deleteRecoverySlots != 2 {
		t.Fatalf("new discovery hid old owner: status=%+v sessions=%d slots=%d", server.Status(), len(backend.sessions), server.deleteRecoverySlots)
	}
	if err := export.closeDeleteRecovery(ctx); !errors.Is(err, syscall.ESTALE) {
		t.Fatal("stop hid old session failure", err)
	}
	backend.sessions[0].settlementErr = nil
	if err := export.closeDeleteRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if server.deleteRecoverySlots != 1 || server.Status().DeleteRecoveryCleanupOwners != 0 {
		t.Fatal("settled retired owner did not return extra global charge")
	}
}

func TestDeleteRecoveryCannotEnrollThirdUnsettledSession(t *testing.T) {
	server, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig())
	backend.newSession = func(raw *deleteRecoverySessionStub) { raw.settlementErr = syscall.ESTALE }
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	backend.identity.Authority = "second-authority"
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	backend.identity.Authority = "third-authority"
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	if len(backend.sessions) != 2 || server.deleteRecoverySlots != 2 || server.Status().DeleteRecoveryCleanupOwners != 2 || export.deleteRecovery.ready.Load() {
		t.Fatal("third session escaped the shared budget")
	}
}

func TestDeleteRecoveryReplacementNeedsExistingGlobalCapacity(t *testing.T) {
	config := endpointConfig()
	config.Limits.MaxUnresolvedOwners = 1
	server, export, backend, ctx := deleteRecoveryFixture(t, config)
	backend.newSession = func(raw *deleteRecoverySessionStub) { raw.settlementErr = syscall.ESTALE }
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	backend.identity.Authority = "new-authority"
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EMFILE) || !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	if len(backend.sessions) != 1 || server.deleteRecoverySlots != 1 || server.Status().DeleteRecoveryCleanupOwners != 1 {
		t.Fatal("failed replacement reservation changed original owner")
	}
}

func TestDeleteRecoveryConcurrencyCancellationAndLifecycle(t *testing.T) {
	_, export, backend, baseCtx := deleteRecoveryFixture(t, endpointConfig())
	backend.listEntered, backend.listRelease = make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(baseCtx)
	result := make(chan error, 1)
	go func() { result <- export.RecoverDeleteIntents(ctx) }()
	<-backend.listEntered
	if err := export.RecoverDeleteIntents(baseCtx); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	if err := export.Unpublish(baseCtx); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if export.deleteRecovery.ready.Load() || export.active != 0 || export.deleteRecovery.current == nil {
		t.Fatal("cancellation lost gate or retained session")
	}
	close(backend.listRelease)
	if err := export.RecoverDeleteIntents(baseCtx); err != nil {
		t.Fatal(err)
	}
	if len(backend.sessions) != 1 {
		t.Fatal("cancellation enrolled a second session")
	}
}

func TestDeleteRecoveryStopRequiresCurrentPrincipalOnlyForOwnedCleanup(t *testing.T) {
	_, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig())
	if err := export.closeDeleteRecovery(t.Context()); err != nil {
		t.Fatal("unused recovery required fabricated identity", err)
	}
	backend.newSession = func(raw *deleteRecoverySessionStub) { raw.settlementErr = syscall.EIO }
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if err := export.closeDeleteRecovery(t.Context()); !errors.Is(err, ErrIdentityDenied) {
		t.Fatal(err)
	}
	backend.sessions[0].settlementErr = nil
	export.stopping = true
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(backend.sessions) != 1 || export.deleteRecovery.current != nil {
		t.Fatal("stop enrolled new discovery work")
	}
}

func TestDeleteRecoveryLocalAckLossSurvivesEmptyDurableList(t *testing.T) {
	server, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig(), storage.DeleteIntentCompleted)
	tr := &tree{export: export, fileActive: 1}
	h, err := tr.reserveFileHandle(&session{id: 41}, 1)
	if err != nil {
		t.Fatal(err)
	}
	h.deleteAttempt = &deleteIntentAttempt{id: backend.intents[0].ID}
	h.recordOpenDiagnostic()
	h.deleteRefSettled.Store(true)
	tr.releaseFileHandle(h)
	initial, err := backend.NewFileSession(ctx, endpointConfig().Limits.FileSession)
	if err != nil {
		t.Fatal(err)
	}
	raw := initial.(*deleteRecoverySessionStub)
	raw.ackErr, raw.deleteBeforeError = syscall.EIO, true
	p, _ := PrincipalFromContext(ctx)
	if err := export.ackDeleteIntent(ctx, raw, p, h.deleteAttempt); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if server.Status().LocalDeleteOwners != 1 || backend.intents[0].ID != "" {
		t.Fatal("lost ACK did not retain only the original diagnostic charge")
	}
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if !h.deleteAttempt.acked || server.Status().LocalDeleteOwners != 0 || len(server.HandleOwners()) != 0 || len(backend.sessions[1].acks) != 0 {
		t.Fatal("empty durable list lost original terminal witness or replayed effect")
	}
}

func TestDeleteRecoverySessionExpirationKeepsFiniteScan(t *testing.T) {
	_, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig(), storage.DeleteIntentPending)
	backend.newSession = func(raw *deleteRecoverySessionStub) { raw.status.Remaining = 0 }
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	if export.deleteRecovery.ready.Load() || len(backend.sessions) != 1 {
		t.Fatal("expired scan claimed readiness")
	}
	backend.newSession = nil
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if !export.deleteRecovery.ready.Load() || len(backend.sessions) != 2 {
		t.Fatal("settled old session did not allow finite retry")
	}
}

func TestDeleteRecoveryPreservesSemanticCloseFailure(t *testing.T) {
	_, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig())
	backend.newSession = func(raw *deleteRecoverySessionStub) {
		raw.closeResult = storage.ReferenceCloseResult{Released: true, Determined: true}
		raw.settlementErr = &storage.CloseSettlementError{State: storage.CloseSettlementPending, SemanticErr: syscall.EACCES, Cause: syscall.EIO}
	}
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EACCES) || !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	backend.sessions[0].settlementErr = nil
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EACCES) {
		t.Fatal("settlement erased original semantic failure", err)
	}
	if export.deleteRecovery.ready.Load() || export.deleteRecovery.current != nil {
		t.Fatal("semantic-error retry claimed readiness or retained released session")
	}
}

func TestDeleteRecoveryCurrentActionQueryCannotBeBypassed(t *testing.T) {
	config := endpointConfig()
	deny := false
	config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		if deny && request.Operation == storage.OpFileQueryAction {
			return authz.ErrDenied
		}
		return nil
	})
	_, export, backend, ctx := deleteRecoveryFixture(t, config, storage.DeleteIntentCompleted)
	backend.newSession = func(raw *deleteRecoverySessionStub) { raw.ackErr = syscall.EIO }
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	first := export.deleteRecovery.attempt.action
	deny = true
	backend.sessions[0].ackErr = nil
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, authz.ErrDenied) {
		t.Fatal(err)
	}
	if export.deleteRecovery.attempt.action != first || len(backend.sessions[0].acks) != 1 {
		t.Fatal("denied action query generated another ACK")
	}
	deny = false
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteRecoveryLateReferenceCloseSettlesBeforeReady(t *testing.T) {
	server, export, backend, ctx := deleteRecoveryFixture(t, endpointConfig(), storage.DeleteIntentPending)
	tr := &tree{export: export, fileActive: 1}
	h, err := tr.reserveFileHandle(&session{id: 42}, 1)
	if err != nil {
		t.Fatal(err)
	}
	h.deleteAttempt = &deleteIntentAttempt{id: backend.intents[0].ID}
	h.recordOpenDiagnostic()
	var closeOnce sync.Once
	backend.onList = func(after storage.DeleteIntentCursor) {
		if after != 0 {
			closeOnce.Do(func() { h.deleteRefSettled.Store(true); tr.releaseFileHandle(h) })
		}
	}
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if !export.deleteRecovery.ready.Load() || server.Status().LocalDeleteOwners != 0 || len(server.HandleOwners()) != 0 {
		t.Fatal("late close remained owned after successful recovery")
	}
}

func TestDeleteRecoveryStopCannotDiscardUnknownAck(t *testing.T) {
	config := endpointConfig()
	deny := false
	config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		if deny && request.Operation == storage.OpFileQueryDeleteIntent {
			return authz.ErrDenied
		}
		return nil
	})
	server, export, backend, ctx := deleteRecoveryFixture(t, config, storage.DeleteIntentCompleted)
	backend.newSession = func(raw *deleteRecoverySessionStub) { raw.ackErr, raw.deleteBeforeError = syscall.EIO, true }
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	attempt := export.deleteRecovery.attempt
	deny = true
	if err := export.closeDeleteRecovery(ctx); !errors.Is(err, authz.ErrDenied) {
		t.Fatal(err)
	}
	if export.deleteRecovery.attempt != attempt || export.deleteRecovery.current == nil || server.Status().DeleteRecoveryCleanupOwners != 1 {
		t.Fatal("stop discarded unknown ACK or exact session")
	}
	deny = false
	if err := export.closeDeleteRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	if !attempt.acked || export.deleteRecovery.attempt != nil || export.deleteRecovery.current != nil || server.Status().DeleteRecoveryCleanupOwners != 0 {
		t.Fatal("fixed-ID stop retry did not settle original ACK")
	}
}
