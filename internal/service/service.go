package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/codetreker/remote-fs/internal/console"
	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
)

type Service struct {
	config      Config
	maintenance *Maintenance
	auth        *Authenticator
	runtimes    *runtimeManager
	handler     http.Handler
	lifetime    context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	stopping    bool
	active      int
	drained     chan struct{}
	closeMu     sync.Mutex
	closed      bool
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
	s := &Service{config: config, maintenance: maintenance, auth: auth, lifetime: lifetime, cancel: cancel, drained: make(chan struct{})}
	owner = s
	s.runtimes = newRuntimeManager(config, maintenance.root, auth, maintenance.store)
	if err := s.resumeVolumes(ctx); err != nil {
		return nil, s.openFailed(err)
	}
	managementHandler, err := newManagementHandler(auth, maintenance.store, s.runtimes.Summary, s.processVolume)
	if err != nil {
		return nil, s.openFailed(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/api/service/v1/auth/", auth.Handler())
	mux.Handle("GET "+serviceapi.Prefix+"/volumes/{id}/files", auth.Middleware(http.HandlerFunc(s.listFiles)))
	mux.Handle("/api/service/v1/", managementHandler)
	mux.HandleFunc("GET /health/live", s.live)
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.Handle("/", console.Handler())
	s.handler = mux
	return s, nil
}

func (s *Service) resumeVolumes(ctx context.Context) error {
	var after management.ID
	for {
		volumes, err := s.maintenance.store.ListCatalogVolumes(ctx, after, s.config.Management.MaxPageSize)
		if err != nil {
			return err
		}
		for _, volume := range volumes {
			if volume.State == management.VolumeCreating || volume.State == management.VolumeDeleting {
				if _, err := s.processVolume(ctx, volume); err != nil {
					return err
				}
			}
			after = volume.ID
		}
		if len(volumes) < s.config.Management.MaxPageSize {
			return nil
		}
	}
}

// OpenFailure retains ownership when constructor cleanup cannot finish.
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
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		return &OpenFailure{service: s, cause: errors.Join(cause, err)}
	}
	return cause
}

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

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
func (s *Service) ready(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	stopping := s.stopping
	s.mu.Unlock()
	if stopping {
		http.Error(w, "service stopping", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
