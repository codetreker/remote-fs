package limited_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func TestNativePublicationUsesTheFinalTargetAfterDirectoryRename(t *testing.T) {
	for _, allowance := range []struct {
		name string
		size int64
	}{{"unbounded native", 0}, {"larger native", 8192}} {
		t.Run(allowance.name, func(t *testing.T) {
			for _, scenario := range []struct {
				name             string
				original, target int
				next             int
				wantErr          error
				used             int64
			}{
				{"refused growth", 3072, 1024, 3072, syscall.EDQUOT, 4096},
				{"successful shrink", 1024, 3072, 2048, nil, 3072},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					meta, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "volume.db"), "quota", allowance.size, sqlite.DefaultWindow())
					if err != nil {
						t.Fatal(err)
					}
					pending := bytes.Repeat([]byte{'p'}, scenario.next)
					objects := &publicationObjects{
						Objects: memory.New(), body: pending,
						staged: make(chan struct{}), release: make(chan struct{}),
					}
					backing := objectstore.New(objects, meta)
					t.Cleanup(func() {
						if err := backing.Close(); err != nil {
							t.Errorf("closing native volume: %v", err)
						}
					})
					s := newStorageOver(t, backing, limited.MinLimit)
					for _, name := range []string{"d", "e"} {
						if err := s.Mkdir(ctx, name); err != nil {
							t.Fatal(err)
						}
					}
					original := bytes.Repeat([]byte{'o'}, scenario.original)
					target := bytes.Repeat([]byte{'t'}, scenario.target)
					for name, body := range map[string][]byte{"d/a": original, "e/a": target} {
						if err := s.Write(ctx, name, body); err != nil {
							t.Fatal(err)
						}
					}
					mustUse(t, s, 4096)
					result := make(chan error, 1)
					finished := make(chan struct{})
					var once sync.Once
					resume := func() { once.Do(func() { close(objects.release) }) }
					go func() {
						defer close(finished)
						result <- s.Write(ctx, "d/a", pending)
					}()
					t.Cleanup(func() { resume(); cancel(); <-finished })
					select {
					case <-objects.staged:
					case <-ctx.Done():
						t.Fatal("write did not reach the stored-object gate")
					}
					target = bytes.Repeat([]byte{'u'}, scenario.target)
					if err := s.Write(ctx, "e/a", target); err != nil {
						t.Fatalf("equal-size independent overwrite while Put was paused: %v", err)
					}
					if got, err := s.Read(ctx, "e/a"); err != nil || !bytes.Equal(got, target) {
						t.Fatalf("independent overwrite returned %d bytes, %v", len(got), err)
					}
					for _, names := range [][2]string{{"d", "old"}, {"e", "d"}} {
						if err := s.Rename(ctx, names[0], names[1]); err != nil {
							t.Fatal(err)
						}
					}
					resume()
					if err := <-result; !errors.Is(err, scenario.wantErr) {
						t.Fatalf("retargeted publication returned %v, want %v", err, scenario.wantErr)
					}
					<-finished
					if scenario.wantErr == nil {
						target = pending
					}
					for name, want := range map[string][]byte{"old/a": original, "d/a": target} {
						got, err := s.Read(ctx, name)
						if err != nil || !bytes.Equal(got, want) {
							t.Fatalf("file %q after retargeted publication: %d bytes, %v; want %d bytes", name, len(got), err, len(want))
						}
					}
					if _, err := s.Stat(ctx, "e"); !errors.Is(err, syscall.ENOENT) {
						t.Fatalf("renamed source directory returned %v, want ENOENT", err)
					}
					used, err := meta.Usage(ctx)
					if err != nil || used != scenario.used {
						t.Fatalf("native usage = %d, %v; want %d", used, err, scenario.used)
					}
					mustUse(t, s, used)
					if err := s.Recount(ctx); err != nil {
						t.Fatal(err)
					}
					mustUse(t, s, used)
				})
			}
		})
	}
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

func TestAllowanceRejectsNativeAccountingCapabilityFailures(t *testing.T) {
	for _, paired := range []bool{false, true} {
		for _, failure := range []error{syscall.ENOSYS, syscall.EOPNOTSUPP, errors.Join(syscall.ENOSYS, syscall.EIO)} {
			p := newPublicationProbe(t)
			if paired {
				p.service = &probeLockService{}
			}
			p.check = failure
			if got, err := limited.New(t.Context(), p, limited.MinLimit); got != nil || err != failure {
				t.Errorf("capability %v with paired service %t returned %v, %v; want nil and original error", failure, paired, got, err)
			}
		}
	}
}

func TestNativeAllowanceRemainsComposableWithoutLockService(t *testing.T) {
	backing := newPublicationProbe(t)
	inner := newStorageOver(t, backing, limited.MinLimit)
	outer := newStorageOver(t, inner, limited.MinLimit)
	if err := outer.CheckPublicationAccounting(); err != nil {
		t.Fatalf("nested native accounting capability returned %v", err)
	}
	if outer.LockService() != nil {
		t.Fatal("native allowance fabricated a lock service")
	}
	mustWrite(t, outer, "a", 1024)
	mustUse(t, inner, 1024)
	mustUse(t, outer, 1024)
	if err := outer.Close(); err != nil {
		t.Fatal(err)
	}
	if backing.closes != 1 {
		t.Fatalf("nested native close reached its backend %d times, want 1", backing.closes)
	}
}

type publicationObjects struct {
	*memory.Objects
	body            []byte
	staged, release chan struct{}
}

func (o *publicationObjects) Put(ctx context.Context, key string, body []byte) ([]byte, error) {
	digest, err := o.Objects.Put(ctx, key, body)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(body, o.body) {
		close(o.staged)
		select {
		case <-o.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return digest, nil
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
