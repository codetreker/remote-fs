package limited_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func retainedSession(t *testing.T, backing storage.FileStorage, ctx context.Context, options storage.FileSessionOptions) storage.FileSession {
	t.Helper()
	session, err := backing.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("closing retained session: %v", err)
		}
	})
	return session
}

func retainedFile(t *testing.T, session storage.FileSession, name string, create bool) storage.File {
	t.Helper()
	file, err := session.OpenFile(t.Context(), name, storage.FileOpenOptions{Read: true, Write: true, Create: create, Mode: 0o600})
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func TestRetainedQuotaSurvivesUnlinkAndSettlesFinalCloseOnce(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	first := retainedFile(t, session, "file", true)
	second := retainedFile(t, session, "file", false)
	if _, err := first.WriteAt(t.Context(), 0, content(1024)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1024)
	if _, err := first.WriteAt(t.Context(), 1024, content(1024)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 2048)
	if err := s.Write(t.Context(), "new", content(3072)); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("detached file did not retain its charge: %v", err)
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 2048)
	if err := second.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
	mustWrite(t, s, "new", 512)
	if err := second.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 512)
	if _, err := s.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("retained writes recreated the unlinked name: %v", err)
	}
}

func TestRetainedQuotaExpiryUsesUncancelledLifetimeAccounting(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	ctx, cancel := context.WithCancel(t.Context())
	options := storage.DefaultFileSessionOptions()
	options.Lease = 80 * time.Millisecond
	session := retainedSession(t, s, ctx, options)
	file := retainedFile(t, session, "file", true)
	if _, err := file.WriteAt(t.Context(), 0, content(512)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 512)
	cancel()
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		space, err := s.Space(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if space.Used == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expired detached file retained %d charged bytes", space.Used)
		case <-ticker.C:
		}
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired file returned %v, want ESTALE", err)
	}
}

func TestRetainedQuotaFailedCleanupKeepsCharge(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	failure := errors.New("injected detached cleanup refusal")
	var reject atomic.Bool
	reject.Store(true)
	ctx := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous > 0 && next == 0 && reject.Load() {
			return nil, failure
		}
		return func(storage.PublicationResult) error { return nil }, nil
	})
	session := retainedSession(t, s, ctx, storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	if _, err := file.WriteAt(t.Context(), 0, content(512)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("cleanup failure returned %v", err)
	}
	mustUse(t, s, 512)
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 512)
	reject.Store(false)
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
}

func TestRetainedQuotaStartupAndRecountIncludeExistingDetachedFiles(t *testing.T) {
	_, backing := memoryfixture.New(t, "retained-usage", 0, locking.DefaultOptions())
	session := retainedSession(t, backing, t.Context(), storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	if _, err := file.WriteAt(t.Context(), 0, content(1024)); err != nil {
		t.Fatal(err)
	}
	if err := backing.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	s := newStorageOver(t, backing, limited.MinLimit)
	mustUse(t, s, 1024)
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1024)
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
}

func TestRetainedQuotaRecountRetriesAcrossCleanupSettlement(t *testing.T) {
	_, backing := memoryfixture.New(t, "retained-recount", 0, locking.DefaultOptions())
	p := &pausedUsage{Storage: backing}
	s := newStorageOver(t, p, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	if _, err := file.WriteAt(t.Context(), 0, content(512)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	p.sampled, p.resume = make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(p.resume) }) }
	t.Cleanup(resume)
	done := make(chan error, 1)
	go func() { done <- s.Recount(t.Context()) }()
	select {
	case <-p.sampled:
	case <-time.After(3 * time.Second):
		t.Fatal("recount did not sample authoritative usage")
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	resume()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recount deadlocked with final cleanup")
	}
	mustUse(t, s, 0)
}

type pausedUsage struct {
	*objectstore.Storage
	once    sync.Once
	sampled chan struct{}
	resume  chan struct{}
}

func (p *pausedUsage) Usage(ctx context.Context) (int64, error) {
	count, err := p.Storage.Usage(ctx)
	if p.sampled != nil {
		p.once.Do(func() {
			close(p.sampled)
			select {
			case <-p.resume:
			case <-ctx.Done():
				err = ctx.Err()
			}
		})
	}
	return count, err
}

func TestRetainedQuotaRequiresNativeAccountingAndAuthoritativeUsage(t *testing.T) {
	backing := newBacking(t)
	unaccounted := newStorageOver(t, &faulty{BoundedStorage: backing}, limited.MinLimit)
	if _, err := unaccounted.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); err != syscall.EOPNOTSUPP {
		t.Fatalf("unaccounted retained file capability returned %v", err)
	}
	missing := &missingRetainedUsage{BoundedStorage: backing}
	if _, err := limited.New(t.Context(), missing, limited.MinLimit); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("retained namespace without authoritative usage returned %v", err)
	}
	for _, result := range []struct {
		usage int64
		err   error
		want  error
	}{{-1, nil, syscall.EIO}, {0, errors.Join(syscall.ENOSYS, syscall.EIO), syscall.EIO}} {
		p := &invalidRetainedUsage{missingRetainedUsage: missing, usage: result.usage, err: result.err}
		if _, err := limited.New(t.Context(), p, limited.MinLimit); !errors.Is(err, result.want) {
			t.Fatalf("invalid authoritative usage returned %v, want %v", err, result.want)
		}
	}
}

type missingRetainedUsage struct {
	storage.BoundedStorage
}

func (*missingRetainedUsage) CheckFileStorage() error           { return nil }
func (*missingRetainedUsage) CheckPublicationAccounting() error { return nil }
func (*missingRetainedUsage) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, error) {
	panic("measurement must precede file session creation")
}

type invalidRetainedUsage struct {
	*missingRetainedUsage
	usage int64
	err   error
}

func (p *invalidRetainedUsage) Usage(context.Context) (int64, error) { return p.usage, p.err }
