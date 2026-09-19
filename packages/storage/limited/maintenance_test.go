package limited

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type maintenanceBackend struct {
	storage.BoundedStorage
	mu                    sync.Mutex
	used                  int64
	chain                 storage.PublicationAccountingChain
	checkError, bindError error
	bound, measured       int
	binding, resume       chan struct{}
}

func (*maintenanceBackend) CheckPublicationAccounting() error   { return nil }
func (b *maintenanceBackend) CheckMaintenanceAccounting() error { return b.checkError }
func (b *maintenanceBackend) Usage(context.Context) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.measured++
	return b.used, nil
}
func (b *maintenanceBackend) BindMaintenanceAccounting(ctx context.Context, chain storage.PublicationAccountingChain, initialize func(int64)) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.bindError != nil {
		return b.bindError
	}
	if b.binding != nil {
		close(b.binding)
		<-b.resume
		b.binding = nil
	}
	initialize(b.used)
	b.chain = chain
	b.bound++
	return nil
}
func (b *maintenanceBackend) transition(next int64, result storage.PublicationResult) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	ctx := storage.WithPublicationAccountingChain(context.Background(), b.chain)
	settle, err := storage.PreparePublication(ctx, b.used, next)
	if err != nil {
		return err
	}
	if result == storage.PublicationApplied {
		b.used = next
	}
	return settle(result)
}
func requireCount(t *testing.T, s *Storage, want int64) {
	t.Helper()
	s.countMu.Lock()
	defer s.countMu.Unlock()
	if s.count != want {
		t.Fatalf("count=%d,want%d", s.count, want)
	}
}

func TestMaintenanceBindingInitializesAndReplacesTheCompleteNestedChain(t *testing.T) {
	backing := &maintenanceBackend{used: 1024}
	inner, err := New(t.Context(), backing, MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := New(t.Context(), inner, MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	if backing.bound != 2 || backing.measured != 0 {
		t.Fatalf("binding=%d ordinarymeasure=%d", backing.bound, backing.measured)
	}
	requireCount(t, inner, 1024)
	requireCount(t, outer, 1024)
	if err := backing.transition(768, storage.PublicationApplied); err != nil {
		t.Fatal(err)
	}
	requireCount(t, inner, 768)
	requireCount(t, outer, 768)
	third, err := New(t.Context(), outer, MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	if err := backing.transition(512, storage.PublicationApplied); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*Storage{inner, outer, third} {
		requireCount(t, s, 512)
	}
	if err := backing.transition(4097, storage.PublicationApplied); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("maintenance bypassed nested limit: %v", err)
	}
	for _, s := range []*Storage{inner, outer, third} {
		requireCount(t, s, 512)
	}
}

func TestMaintenanceBindingFailurePreservesTheInstalledOwner(t *testing.T) {
	for _, name := range []string{"failure", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			backing := &maintenanceBackend{used: 1024}
			inner, err := New(t.Context(), backing, MinLimit)
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			want := errors.New("binding refused before installation")
			if name == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			} else {
				backing.bindError = want
			}
			if failed, err := New(ctx, inner, MinLimit); failed != nil || !errors.Is(err, want) {
				t.Fatalf("constructor=%v,%v", failed, err)
			}
			if backing.bound != 1 {
				t.Fatalf("failed constructor installed%dchains", backing.bound)
			}
			backing.bindError = nil
			if err := backing.transition(512, storage.PublicationApplied); err != nil {
				t.Fatal(err)
			}
			requireCount(t, inner, 512)
		})
	}
}

func TestMaintenanceUnknownSettlementKeepsChargeAndFencesEveryOwner(t *testing.T) {
	backing := &maintenanceBackend{used: 512}
	inner, err := New(t.Context(), backing, MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := New(t.Context(), inner, MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	if err := backing.transition(1024, storage.PublicationUnknown); !storage.IsPublicationAccountingUncertain(err) {
		t.Fatalf("unknown settlement=%v", err)
	}
	for _, s := range []*Storage{inner, outer} {
		requireCount(t, s, 1024)
		if err := s.CheckMaintenanceAccounting(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("unknown owner remained usable: %v", err)
		}
		if err := s.BindMaintenanceAccounting(t.Context(), storage.PublicationAccountingChain{}, func(int64) { t.Fatal("fenced owner initialized new wrapper") }); !errors.Is(err, syscall.EIO) {
			t.Fatalf("fenced rebind=%v", err)
		}
	}
}

func TestMaintenanceCapabilityRefusalCannotHideARealFailure(t *testing.T) {
	for _, want := range []error{syscall.EIO, errors.Join(syscall.EOPNOTSUPP, syscall.EIO)} {
		backing := &maintenanceBackend{used: 512, checkError: want}
		if s, err := New(t.Context(), backing, MinLimit); s != nil || !errors.Is(err, want) {
			t.Fatalf("failed capability=%v,%v", s, err)
		}
		if backing.measured != 0 || backing.bound != 0 {
			t.Fatal("failed maintenance capability fell back to ordinary measurement")
		}
	}
	backing := &maintenanceBackend{used: 512, checkError: syscall.EOPNOTSUPP}
	s, err := New(t.Context(), backing, MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	requireCount(t, s, 512)
	if backing.measured != 1 || backing.bound != 0 {
		t.Fatalf("unsupported maintenance measured=%d,bound=%d", backing.measured, backing.bound)
	}
	if err := s.CheckMaintenanceAccounting(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported check=%v", err)
	}
	if err := s.BindMaintenanceAccounting(t.Context(), storage.PublicationAccountingChain{}, func(int64) { t.Fatal("unsupported initialized") }); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported bind=%v", err)
	}
}

func TestMaintenanceBindingOrdersInitialCountBeforeConcurrentCleanup(t *testing.T) {
	backing := &maintenanceBackend{used: 1024, binding: make(chan struct{}), resume: make(chan struct{})}
	type opened struct {
		s   *Storage
		err error
	}
	constructed := make(chan opened, 1)
	go func() { s, err := New(t.Context(), backing, MinLimit); constructed <- opened{s, err} }()
	<-backing.binding
	started := make(chan struct{})
	cleaned := make(chan error, 1)
	go func() { close(started); cleaned <- backing.transition(512, storage.PublicationApplied) }()
	<-started
	select {
	case err := <-cleaned:
		t.Fatalf("cleanup crossed initialization ordering: %v", err)
	default:
	}
	close(backing.resume)
	result := <-constructed
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := <-cleaned; err != nil {
		t.Fatal(err)
	}
	requireCount(t, result.s, 512)
}
