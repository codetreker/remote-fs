package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/codetreker/remote-fs/internal/console"
	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Service owns every runtime and accepted management responsibility beneath
// its exclusive state-root anchor. A failed Close retains that ownership.
type Service struct {
	config         Config
	maintenance    *Maintenance
	auth           *Authenticator
	runtimes       *runtimeManager
	lifecycle      *Lifecycle
	handler        http.Handler
	lifetime       context.Context
	cancel         context.CancelFunc
	wake           chan struct{}
	workerDone     chan struct{}
	namespaceDone  chan struct{}
	mu             sync.Mutex
	stopping       bool
	active         int
	drained        chan struct{}
	workerError    error
	namespaceError error
	closeMu        sync.Mutex
	closed         bool
}

func Open(ctx context.Context, config Config) (_ *Service, returned error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	maintenance, err := OpenMaintenance(ctx, config)
	if err != nil {
		return nil, err
	}
	var owner *Service
	defer func() {
		if returned != nil && owner == nil {
			returned = finishMaintenance(context.WithoutCancel(ctx), maintenance, returned)
		}
	}()
	admin, err := maintenance.store.HasLocalAdmin(ctx)
	if err != nil {
		return nil, err
	}
	if !admin {
		return nil, errors.New("service requires an initialized administrator")
	}
	auth, err := NewAuthenticator(maintenance.store, config.Auth)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(context.Background())
	s := &Service{config: config, maintenance: maintenance, auth: auth, lifetime: lifetime, cancel: cancel, wake: make(chan struct{}, 1), workerDone: make(chan struct{}), namespaceDone: make(chan struct{}), drained: make(chan struct{})}
	owner = s
	s.runtimes = newRuntimeManager(config, maintenance.root, auth, maintenance.store)
	s.lifecycle, err = NewLifecycle(LifecycleConfig{Management: maintenance.store, VolumesDir: maintenance.root.VolumesPath, QuarantineDir: maintenance.root.QuarantinePath, MaxReclaimEntries: 4096, MaxConcurrentOperations: config.MaxLifecycleWorkers, Hooks: LifecycleHooks{Initialize: s.runtimes.Initialize, Acquire: s.lifecycleAcquire, Freeze: s.runtimes.Freeze}})
	if err != nil {
		return nil, s.openFailed(err)
	}
	managementHandler, err := newManagementHandler(auth, maintenance.store, s.summary, s.accepted, maintenance.IssueNamespace)
	if err != nil {
		return nil, s.openFailed(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/api/service/v1/auth/", auth.Handler())
	mux.Handle("GET /api/service/v1/capabilities", auth.Middleware(http.HandlerFunc(s.capabilities)))
	mux.Handle("/api/service/v1/", managementHandler)
	mux.Handle("/api/volumes/", http.HandlerFunc(s.serveVolume))
	mux.HandleFunc("GET /health/live", s.live)
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.Handle("/", console.Handler())
	s.handler = mux
	go s.work()
	go s.retireNamespaces()
	s.accepted(management.Operation{})
	return s, nil
}

// OpenFailure retains the owner when constructor cleanup cannot be confirmed.
// The caller can retry Close; abandoning the error also abandons that cleanup.
type OpenFailure struct {
	service *Service
	cause   error
}

func (e *OpenFailure) Error() string {
	return fmt.Sprintf("service initialization cleanup remains unresolved: %v", e.cause)
}
func (e *OpenFailure) Unwrap() error                   { return e.cause }
func (e *OpenFailure) Close(ctx context.Context) error { return e.service.Close(ctx) }

func (s *Service) openFailed(cause error) error {
	close(s.workerDone)
	close(s.namespaceDone)
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		return &OpenFailure{service: s, cause: errors.Join(cause, err)}
	}
	return cause
}

func (s *Service) lifecycleAcquire(ctx context.Context, volume management.Volume) (LifecycleLease, error) {
	lease, err := s.runtimes.acquire(ctx, volume, true)
	if err != nil {
		return LifecycleLease{}, err
	}
	return LifecycleLease{Store: lease.Store, Release: lease.Release}, nil
}

func (s *Service) summary(ctx context.Context, volume management.Volume) (storage.VolumeSummaryResult, error) {
	return s.runtimes.Summary(ctx, volume)
}

func (s *Service) accepted(op management.Operation) {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Service) work() {
	defer close(s.workerDone)
	jobs := make(chan management.Operation, s.config.MaxLifecycleWorkers)
	results := make(chan lifecycleResult, s.config.MaxLifecycleWorkers)
	var workers sync.WaitGroup
	for i := 0; i < s.config.MaxLifecycleWorkers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for op := range jobs {
				_, err := s.lifecycle.Run(s.lifetime, op)
				select {
				case results <- lifecycleResult{id: op.ID, err: err}:
				case <-s.lifetime.Done():
					return
				}
			}
		}()
	}
	defer func() { close(jobs); workers.Wait() }()
	inFlight := make(map[management.ID]bool)
	failures := make(map[management.ID]error)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.lifetime.Done():
			return
		case result := <-results:
			delete(inFlight, result.id)
			if result.err == nil {
				delete(failures, result.id)
			} else {
				failures[result.id] = result.err
			}
		case <-s.wake:
		case <-ticker.C:
		}
		err := s.queueOperations(jobs, inFlight)
		if s.lifetime.Err() != nil {
			return
		}
		allErrors := []error{err}
		for _, failure := range failures {
			allErrors = append(allErrors, failure)
		}
		s.mu.Lock()
		s.workerError = errors.Join(allErrors...)
		s.mu.Unlock()
	}
}

type lifecycleResult struct {
	id  management.ID
	err error
}

func (s *Service) queueOperations(jobs chan<- management.Operation, inFlight map[management.ID]bool) error {
	var after management.ID
	for len(inFlight) < 2*s.config.MaxLifecycleWorkers {
		operations, err := s.maintenance.store.PendingOperations(s.lifetime, after, managementPageLimit)
		if err != nil {
			return err
		}
		if len(operations) == 0 {
			return nil
		}
		for _, op := range operations {
			after = op.ID
			if inFlight[op.ID] {
				continue
			}
			select {
			case jobs <- op:
				inFlight[op.ID] = true
			default:
				return nil
			}
			if len(inFlight) >= 2*s.config.MaxLifecycleWorkers {
				return nil
			}
		}
	}
	return nil
}

// Stop fences new requests and interrupts owned persistent outputs before
// HTTP shutdown waits for connections. Close confirms the remaining settlement.
func (s *Service) Stop() {
	s.mu.Lock()
	if !s.stopping {
		s.stopping = true
		if s.active == 0 {
			close(s.drained)
		}
	}
	s.mu.Unlock()
	s.cancel()
	s.runtimes.stop()
}

func (s *Service) Close(ctx context.Context) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.Stop()
	select {
	case <-s.workerDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.namespaceDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := s.auth.Close(); err != nil {
		return err
	}
	select {
	case <-s.drained:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := s.runtimes.Close(ctx); err != nil {
		return err
	}
	if err := s.maintenance.Close(ctx); err != nil {
		return err
	}
	s.closed = true
	return nil
}

func (s *Service) retireNamespaces() {
	defer close(s.namespaceDone)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.lifetime.Done():
			return
		case <-ticker.C:
		}
		err := s.lifecycle.RetireAllNamespaces(s.lifetime)
		if s.lifetime.Err() == nil {
			s.mu.Lock()
			s.namespaceError = err
			s.mu.Unlock()
		}
	}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.serveHTTP(w, r)
}
func (s *Service) serveHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	s.active++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.active--
		if s.stopping && s.active == 0 {
			close(s.drained)
		}
		s.mu.Unlock()
	}()
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(s.lifetime, cancel)
	defer func() { stop(); cancel() }()
	w.Header().Set("X-Content-Type-Options", "nosniff")
	forwarded, err := proxyClientRequest(r.WithContext(ctx), s.config.TrustedProxyCIDRs)
	if err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	s.handler.ServeHTTP(w, forwarded)
}

func (s *Service) live(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }
func (s *Service) capabilities(w http.ResponseWriter, r *http.Request) {
	p, ok := CurrentPrincipal(r.Context())
	if !ok {
		writeManagementError(w, ErrUnauthenticated)
		return
	}
	if err := s.auth.RequireScope(r.Context(), p, ScopeFiles); err != nil {
		writeManagementError(w, err)
		return
	}
	writeAuthJSON(w, http.StatusOK, serviceapi.Capabilities{FileSession: serviceapi.FileSessionLimitsFromOptions(fileSessionOptions(s.config)), MaxTransferBytes: serviceapi.DecimalFromInt64(serviceTransferMaxBytes)})
}
func (s *Service) ready(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	err := errors.Join(s.workerError, s.namespaceError)
	stopping := s.stopping
	s.mu.Unlock()
	if stopping || err != nil {
		http.Error(w, "service recovery pending", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
