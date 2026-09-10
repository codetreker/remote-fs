package limited_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
)

func TestNativeStagingDoesNotHoldPathStripes(t *testing.T) {
	p := newPublicationProbe(t)
	s := newStorageOver(t, p, limited.MinLimit)
	staged, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	p.stage = func(ctx context.Context, name string) error {
		if name == "a" {
			close(staged)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	aResult := make(chan error, 1)
	aFinished := make(chan struct{})
	go func() {
		defer close(aFinished)
		aResult <- s.Write(t.Context(), "a", content(1024))
	}()
	t.Cleanup(func() { resume(); <-aFinished })
	<-staged
	// These distinct paths share the fallback stripe. Native staging must leave it free.
	bResult := make(chan error, 1)
	bFinished := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() {
		defer close(bFinished)
		bResult <- s.Write(ctx, "file78", content(1024))
	}()
	t.Cleanup(func() { resume(); <-bFinished })
	select {
	case err := <-bResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("staging one file prevented publication of another file")
	}
	if got, err := p.Read(t.Context(), "file78"); err != nil || !bytes.Equal(got, content(1024)) {
		t.Fatalf("independent publication returned incorrect bytes: length %d, error %v", len(got), err)
	}
	resume()
	if err := <-aResult; err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 2048)
}

func TestNativePublicationUsesTheFinalTargetAfterDirectoryRename(t *testing.T) {
	p := newPublicationProbe(t)
	s := newStorageOver(t, p, limited.MinLimit)
	if err := s.Mkdir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, s, "d/f", 2048)
	staged, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	p.stage = func(ctx context.Context, name string) error {
		if name == "d/f" {
			close(staged)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	result := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		result <- s.Write(t.Context(), "d/f", content(4096))
	}()
	t.Cleanup(func() { resume(); <-finished })
	<-staged
	if err := s.Rename(t.Context(), "d", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.Mkdir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "d/f"); err != nil {
		t.Fatal(err)
	}
	resume()
	if err := <-result; !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("publication against the new empty target returned %v, want EDQUOT", err)
	}
	for name, want := range map[string][]byte{"moved/f": content(2048), "d/f": {}} {
		got, err := p.Read(t.Context(), name)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("file %q after refused publication: length %d, error %v; want %d bytes", name, len(got), err, len(want))
		}
	}
	mustUse(t, s, 2048)
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 2048)
}

func TestAppliedPublicationSettlesQuotaEvenWhenItsReplyFails(t *testing.T) {
	p := newPublicationProbe(t)
	s := newStorageOver(t, p, limited.MinLimit)
	mustWrite(t, s, "a", limited.MinLimit)
	failure := errors.New("injected confirmation failure")
	p.failure = failure
	if err := s.Write(t.Context(), "a", content(1024)); !errors.Is(err, failure) {
		t.Fatalf("applied shrinking write returned %v, want original confirmation failure", err)
	}
	got, err := p.Read(t.Context(), "a")
	if err != nil || !bytes.Equal(got, content(1024)) {
		t.Fatalf("applied shrinking write did not publish expected bytes: length %d, error %v", len(got), err)
	}
	mustUse(t, s, 1024)
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1024)
}

func TestUnknownPublicationFencesQuotaAndRecount(t *testing.T) {
	p := newPublicationProbe(t)
	s := newStorageOver(t, p, limited.MinLimit)
	mustWrite(t, s, "a", 1024)
	p.outcome = storage.PublicationUnknown
	failure := errors.New("injected unknown publication")
	p.failure = failure
	if err := s.Write(t.Context(), "a", content(2048)); !errors.Is(err, failure) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown publication returned %v, want EIO and original cause", err)
	}
	for name, operation := range map[string]func() error{
		"capability":       func() error { return s.CheckPublicationAccounting() },
		"space":            func() error { _, err := s.Space(t.Context()); return err },
		"recount":          func() error { return s.Recount(t.Context()) },
		"write":            func() error { return s.Write(t.Context(), "b", nil) },
		"remove":           func() error { return s.Remove(t.Context(), "a") },
		"rename":           func() error { return s.Rename(t.Context(), "a", "b") },
		"create":           func() error { return s.Create(t.Context(), "b") },
		"mkdir":            func() error { return s.Mkdir(t.Context(), "b") },
		"remove directory": func() error { return s.RemoveDir(t.Context(), "b") },
		"setattr":          func() error { return s.SetAttr(t.Context(), "a", storage.AttrChange{}) },
	} {
		if err := operation(); !errors.Is(err, syscall.EIO) {
			t.Errorf("%s after unknown publication returned %v, want EIO", name, err)
		}
	}
	if got, err := p.Read(t.Context(), "a"); err != nil || !bytes.Equal(got, content(1024)) {
		t.Fatalf("fenced operations changed previous content: length %d, error %v", len(got), err)
	}
}

func TestNativePublicationFailureRefundsGrowthAndPreservesShrink(t *testing.T) {
	for _, scenario := range []struct {
		name string
		size int
	}{{"shrink", 0}, {"equal", 1024}, {"growth", 2048}} {
		t.Run(scenario.name, func(t *testing.T) {
			p := newPublicationProbe(t)
			s := newStorageOver(t, p, limited.MinLimit)
			mustWrite(t, s, "a", 1024)
			p.outcome = storage.PublicationNotApplied
			failure := errors.New("injected prepublication failure")
			p.failure = failure
			if err := s.Write(t.Context(), "a", content(scenario.size)); !errors.Is(err, failure) {
				t.Fatalf("rejected publication returned %v, want original failure", err)
			}
			if got, err := p.Read(t.Context(), "a"); err != nil || !bytes.Equal(got, content(1024)) {
				t.Fatalf("rejected publication changed prior content: length %d, error %v", len(got), err)
			}
			mustUse(t, s, 1024)
			p.outcome, p.failure = storage.PublicationApplied, nil
			mustWrite(t, s, "b", 3072)
			mustUse(t, s, limited.MinLimit)
		})
	}
}

func TestUnknownPublicationAlsoFencesAlreadyStagedWrites(t *testing.T) {
	p := newPublicationProbe(t)
	s := newStorageOver(t, p, limited.MinLimit)
	staged, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	p.stage = func(ctx context.Context, name string) error {
		if name == "a" {
			close(staged)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	result := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		result <- s.Write(t.Context(), "a", content(1024))
	}()
	t.Cleanup(func() { resume(); <-finished })
	<-staged
	p.outcome = storage.PublicationUnknown
	if err := s.Write(t.Context(), "b", content(1024)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown publication returned %v, want EIO", err)
	}
	p.outcome = storage.PublicationApplied
	resume()
	if err := <-result; !errors.Is(err, syscall.EIO) {
		t.Fatalf("previously staged write returned %v after unknown publication, want EIO", err)
	}
	if _, err := p.Stat(t.Context(), "a"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("previously staged write published after the quota was fenced: %v", err)
	}
}

func TestNestedNativeAllowancesUnwindRejectedPreparation(t *testing.T) {
	p := newPublicationProbe(t)
	inner := newStorageOver(t, p, limited.MinLimit)
	outer := newStorageOver(t, inner, 8192)
	if err := outer.Write(t.Context(), "a", content(5000)); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("inner allowance rejection returned %v, want EDQUOT", err)
	}
	mustUse(t, inner, 0)
	mustUse(t, outer, 0)
	if _, err := p.Stat(t.Context(), "a"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("rejected nested publication left a file: %v", err)
	}
	mustWrite(t, outer, "a", 4096)
	mustWrite(t, outer, "a", 1024)
	mustUse(t, inner, 1024)
	mustUse(t, outer, 1024)
}

func TestNativeReplacementAndRemovalSettleDefinitiveReleasedBytes(t *testing.T) {
	p := newPublicationProbe(t)
	s := newStorageOver(t, p, limited.MinLimit)
	kept := bytes.Repeat([]byte{'a'}, 1024)
	if err := s.Write(t.Context(), "a", kept); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, s, "b", 2048)
	if err := s.Rename(t.Context(), "a", "b"); err != nil {
		t.Fatal(err)
	}
	if got, err := p.Read(t.Context(), "b"); err != nil || !bytes.Equal(got, kept) {
		t.Fatalf("replacement lost the source bytes: length %d, error %v", len(got), err)
	}
	mustUse(t, s, 1024)
	p.outcome, p.failure = storage.PublicationNotApplied, syscall.EACCES
	if err := s.Remove(t.Context(), "b"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("refused removal returned %v, want EACCES", err)
	}
	mustUse(t, s, 1024)
	if got, err := p.Read(t.Context(), "b"); err != nil || !bytes.Equal(got, kept) {
		t.Fatalf("refused removal changed the bytes: length %d, error %v", len(got), err)
	}
	p.outcome = storage.PublicationApplied
	if err := s.Remove(t.Context(), "b"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("applied removal lost its confirmation error: %v", err)
	}
	if _, err := p.Stat(t.Context(), "b"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("applied removal left a target: %v", err)
	}
	mustUse(t, s, 0)
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
}

func TestNativeAllowancePreservesScopeAndDelegatesLifecycle(t *testing.T) {
	p := newPublicationProbe(t)
	p.service = &probeLockService{}
	s := newStorageOver(t, p, limited.MinLimit)
	if s.LockService() != p.service {
		t.Fatal("allowance changed the volume's paired lock service")
	}
	if err := s.CheckPublicationAccounting(); err != nil {
		t.Fatal(err)
	}
	scope := locking.MutationScope{Owner: locking.OwnerRef{Session: "session", Owner: "owner"}}
	ctx := locking.WithScope(t.Context(), scope)
	if err := s.Write(ctx, "a", []byte{1}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.scope, scope) {
		t.Fatal("allowance changed the mutation scope delivered to its native backend")
	}
	failure := errors.New("injected close failure")
	p.closeErr = failure
	if err := s.Close(); err != failure || p.closes != 1 {
		t.Fatalf("delegated close returned %v after %d closes", err, p.closes)
	}
}

func TestAllowanceRejectsLockServiceWithoutNativeAccounting(t *testing.T) {
	p := newPublicationProbe(t)
	p.service = &probeLockService{}
	p.check = syscall.ENOSYS
	if _, err := limited.New(t.Context(), p, limited.MinLimit); !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("lock service without accounting returned %v, want ENOSYS", err)
	}
	failure := errors.Join(syscall.ENOSYS, syscall.EIO)
	p.check = failure
	if _, err := limited.New(t.Context(), p, limited.MinLimit); err != failure {
		t.Fatalf("joined capability failure returned %v, want original error", err)
	}
}

func TestGenericAllowanceRemainsComposableWithoutNativeCapability(t *testing.T) {
	backing := &faulty{BoundedStorage: newBacking(t)}
	inner := newStorageOver(t, backing, limited.MinLimit)
	outer := newStorageOver(t, inner, limited.MinLimit)
	if err := outer.CheckPublicationAccounting(); err != syscall.ENOSYS {
		t.Fatalf("generic accounting capability returned %v, want ENOSYS", err)
	}
	if outer.LockService() != nil {
		t.Fatal("generic allowance fabricated a lock service")
	}
	mustWrite(t, outer, "a", 1024)
	mustUse(t, inner, 1024)
	mustUse(t, outer, 1024)
	if err := outer.Close(); err != nil {
		t.Fatal(err)
	}
}

type probeLockService struct{ locking.Service }

// The probe injects final outcomes over a real volume. Its target mutex models
// the native ordering required by the hook contract.
type publicationProbe struct {
	storage.BoundedStorage
	// applyCtx excludes the incoming publication hooks: the probe prepares and settles
	// them once, independently of the underlying volume's own publication.
	applyCtx context.Context
	mu       sync.Mutex
	stage    func(context.Context, string) error
	outcome  storage.PublicationResult
	failure  error
	check    error
	service  locking.Service
	scope    locking.MutationScope
	closeErr error
	closes   int
}

func newPublicationProbe(t *testing.T) *publicationProbe {
	return &publicationProbe{
		BoundedStorage: newBacking(t), applyCtx: t.Context(), outcome: storage.PublicationApplied,
	}
}

func (p *publicationProbe) CheckPublicationAccounting() error { return p.check }
func (p *publicationProbe) LockService() locking.Service      { return p.service }
func (p *publicationProbe) Close() error {
	p.closes++
	return p.closeErr
}

func (p *publicationProbe) publish(ctx context.Context, previous, next int64, apply func() error) error {
	settle, err := storage.PreparePublication(ctx, previous, next)
	if err != nil {
		return err
	}
	if p.outcome == storage.PublicationApplied {
		if err := apply(); err != nil {
			return errors.Join(err, settle(storage.PublicationNotApplied))
		}
	}
	return errors.Join(p.failure, settle(p.outcome))
}

func (p *publicationProbe) Write(ctx context.Context, name string, body []byte) error {
	if p.stage != nil {
		if err := p.stage(ctx, name); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scope = locking.ScopeFromContext(ctx)
	previous, err := p.fileBytes(ctx, name)
	if err != nil {
		return err
	}
	return p.publish(ctx, previous, int64(len(body)), func() error {
		return p.BoundedStorage.Write(p.applyCtx, name, body)
	})
}

func (p *publicationProbe) Rename(ctx context.Context, from, to string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	previous, err := p.fileBytes(ctx, to)
	if err != nil {
		return err
	}
	return p.publish(ctx, previous, 0, func() error {
		return p.BoundedStorage.Rename(p.applyCtx, from, to)
	})
}

func (p *publicationProbe) Remove(ctx context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	previous, err := p.fileBytes(ctx, name)
	if err != nil {
		return err
	}
	return p.publish(ctx, previous, 0, func() error {
		return p.BoundedStorage.Remove(p.applyCtx, name)
	})
}

func (p *publicationProbe) fileBytes(ctx context.Context, name string) (int64, error) {
	attr, err := p.BoundedStorage.Stat(ctx, name)
	if errors.Is(err, syscall.ENOENT) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if attr.IsDir() {
		return 0, nil
	}
	return attr.Size, nil
}
