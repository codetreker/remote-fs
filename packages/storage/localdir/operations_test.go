package localdir

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type operationClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *operationClock) Now() time.Time                       { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *operationClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }
func (c *operationClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func operationStorage(t *testing.T, adjust func(*Config)) *Storage {
	t.Helper()
	cfg := stateTestConfig(t)
	if adjust != nil {
		adjust(&cfg)
	}
	if err := Init(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	s, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func operationOwner(t *testing.T, s *Storage) locking.OwnerRef {
	t.Helper()
	service := s.LockService()
	ticket, err := service.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := service.CreateOwner(t.Context(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	return owner.Ref
}
func operationGrant(t *testing.T, s *Storage, name string, ttl time.Duration) locking.MutationScope {
	t.Helper()
	owner := operationOwner(t, s)
	resource, err := s.LockService().Resolve(t.Context(), owner, name)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := s.LockService().Acquire(t.Context(), locking.AcquireRequest{Owner: owner, Request: "grant", Resource: resource, Mode: locking.Exclusive, TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	return locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant.Grant.Ref}}
}
func operationAwait(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not finish")
		return nil
	}
}

func TestWriteRevalidatesProofAfterPrivateStaging(t *testing.T) {
	clock := &operationClock{now: time.Now()}
	s := operationStorage(t, func(cfg *Config) { cfg.Locks.Clock = clock; cfg.Limits.MaxStagingBytes = 5 })
	if err := s.Write(t.Context(), "one", []byte("old")); err != nil {
		t.Fatal(err)
	}
	scope := operationGrant(t, s, "one", time.Second)
	entered, resume := make(chan struct{}), make(chan struct{})
	original := s.ops.write
	s.ops.write = func(fd int, p []byte) (int, error) {
		if string(p) == "stale" {
			close(entered)
			<-resume
		}
		return original(fd, p)
	}
	done := make(chan error, 1)
	go func() { done <- s.Write(locking.WithScope(t.Context(), scope), "one", []byte("stale")) }()
	<-entered
	entries, err := s.List(t.Context(), "")
	if err != nil || len(entries) != 1 || entries[0].Name != "one" {
		t.Fatalf("private stage exposed: %+v, %v", entries, err)
	}
	if err := s.Write(t.Context(), "two", []byte("x")); locking.CodeOf(err) != locking.Capacity {
		t.Fatalf("aggregate staging limit: %v", err)
	}
	if err := s.Create(t.Context(), "two"); err != nil {
		t.Fatal(err)
	}
	clock.advance(2 * time.Second)
	close(resume)
	if err := operationAwait(t, done); locking.CodeOf(err) != locking.StaleGrant {
		t.Fatalf("expired staged proof: %v", err)
	}
	content, err := s.Read(t.Context(), "one")
	if err != nil || string(content) != "old" {
		t.Fatalf("expired write changed target: %q %v", content, err)
	}
	if err := s.Write(t.Context(), "two", []byte("x")); err != nil {
		t.Fatalf("stage reservation leaked: %v", err)
	}
}

func TestPausedPublicationOrdersViewsWithoutBlockingOtherFiles(t *testing.T) {
	s := operationStorage(t, nil)
	if err := s.Write(t.Context(), "one", []byte("old")); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	original := s.ops.rename
	s.ops.rename = func(a int, b string, c int, d string) error {
		if d == "one" {
			close(entered)
			<-resume
		}
		return original(a, b, c, d)
	}
	done := make(chan error, 1)
	go func() { done <- s.Write(t.Context(), "one", []byte("new")) }()
	<-entered
	other := make(chan error, 1)
	go func() { other <- s.Write(t.Context(), "two", []byte("other")) }()
	if err := operationAwait(t, other); err != nil {
		t.Fatal(err)
	}
	observed := make(chan error, 1)
	go func() {
		attr, err := s.Stat(t.Context(), "one")
		if err == nil && attr.Size != 3 {
			err = errors.New("wrong published size")
		}
		observed <- err
	}()
	select {
	case err := <-observed:
		t.Fatalf("view overtook publication: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(resume)
	if err := operationAwait(t, done); err != nil {
		t.Fatal(err)
	}
	if err := operationAwait(t, observed); err != nil {
		t.Fatal(err)
	}
}

func TestListChargingRetainsSnapshotOutsideMutationOrdering(t *testing.T) {
	s := operationStorage(t, func(cfg *Config) { cfg.Limits.MaxSnapshotEntries = 1 })
	if err := s.Write(t.Context(), "one", []byte("old")); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	result, err := storage.NewListResult(1024, 0, func(_ int, _ int64, attr storage.Attr) (int64, error) {
		close(entered)
		<-resume
		if attr.Size != 3 {
			t.Errorf("snapshot changed: %d", attr.Size)
		}
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.ListBounded(t.Context(), "", result) }()
	<-entered
	if err := s.Write(t.Context(), "one", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.List(t.Context(), ""); locking.CodeOf(err) != locking.Capacity {
		t.Fatalf("snapshot capacity was not retained: %v", err)
	}
	close(resume)
	if err := operationAwait(t, done); err != nil {
		t.Fatal(err)
	}
	entries, err := s.List(t.Context(), "")
	if err != nil || entries[0].Attr.Size != 11 {
		t.Fatalf("fresh snapshot: %+v %v", entries, err)
	}
}

func TestSetAttrKnownPartialEffectSettlesApplied(t *testing.T) {
	s := operationStorage(t, nil)
	if err := s.Write(t.Context(), "one", []byte("old")); err != nil {
		t.Fatal(err)
	}
	when := time.Unix(123456789, 0)
	mode := os.FileMode(0600)
	s.ops.chmod = func(int, uint32) error { return syscall.EPERM }
	var settled storage.PublicationResult
	ctx := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous != 0 || next != 0 {
			t.Errorf("metadata accounting %d -> %d", previous, next)
		}
		return func(result storage.PublicationResult) error { settled = result; return nil }, nil
	})
	if err := s.SetAttr(ctx, "one", storage.AttrChange{Mode: &mode, ModTime: &when}); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("mode failure: %v", err)
	}
	if settled != storage.PublicationApplied {
		t.Fatalf("partial effect settled %v", settled)
	}
	attr, err := s.Stat(t.Context(), "one")
	if err != nil || !attr.ModTime.Equal(when) {
		t.Fatalf("known times effect lost: %+v %v", attr, err)
	}
}

func TestAppliedWriteSyncFailurePreservesLogicalBinding(t *testing.T) {
	s := operationStorage(t, nil)
	if err := s.Write(t.Context(), "one", []byte("old")); err != nil {
		t.Fatal(err)
	}
	scope := operationGrant(t, s, "one", time.Minute)
	original := s.ops.sync
	s.ops.sync = func(fd int) error {
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			return err
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			return syscall.EIO
		}
		return original(fd)
	}
	var settled storage.PublicationResult
	ctx := storage.WithPublicationAccounting(locking.WithScope(t.Context(), scope), func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous != 3 || next != 11 {
			t.Errorf("write accounting %d -> %d", previous, next)
		}
		return func(result storage.PublicationResult) error { settled = result; return nil }, nil
	})
	if err := s.Write(ctx, "one", []byte("replacement")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("post-effect sync error: %v", err)
	}
	if settled != storage.PublicationApplied {
		t.Fatalf("known application settled %v", settled)
	}
	content, err := s.Read(t.Context(), "one")
	if err != nil || string(content) != "replacement" {
		t.Fatalf("known published contents: %q %v", content, err)
	}
	resource, err := s.LockService().Resolve(t.Context(), scope.Owner, "one")
	if err != nil || resource.ID != scope.Grants[0].Resource {
		t.Fatalf("logical identity did not transfer: %+v %v", resource, err)
	}
}

func TestStagingCleanupFailureRetainsBudgetAndFences(t *testing.T) {
	cfg := stateTestConfig(t)
	cfg.Limits.MaxStagingBytes = 4
	if err := Init(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	s, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.ops.write = func(int, []byte) (int, error) { return 0, syscall.ENOSPC }
	s.ops.unlink = func(int, string, int) error { return syscall.EPERM }
	if err := s.Write(t.Context(), "one", []byte("full")); !errors.Is(err, syscall.ENOSPC) || !errors.Is(err, syscall.EPERM) {
		t.Fatalf("cleanup failure chain: %v", err)
	}
	if s.stagedBytes != 4 {
		t.Fatalf("orphan released reusable staging bytes: %d", s.stagedBytes)
	}
	if err := s.Create(t.Context(), "two"); locking.CodeOf(err) != locking.Unavailable {
		t.Fatalf("cleanup failure remained writable: %v", err)
	}
	if err := s.Close(); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("fence missing from close: %v", err)
	}
	recovered, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("clean close did not release ownership: %v", err)
	}
	defer recovered.Close()
	if err := recovered.Write(t.Context(), "one", []byte("okay")); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownWriteFencesBeforeViewsAndSettlesUnknown(t *testing.T) {
	cfg := stateTestConfig(t)
	if err := Init(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	s, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Write(t.Context(), "one", []byte("old")); err != nil {
		t.Fatal(err)
	}
	original := s.ops.rename
	s.ops.rename = func(a int, b string, c int, d string) error {
		if err := original(a, b, c, d); err != nil {
			return err
		}
		return syscall.EIO
	}
	var settled storage.PublicationResult
	ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error { settled = result; return nil }, nil
	})
	if err := s.Write(ctx, "one", []byte("new")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown rename: %v", err)
	}
	if settled != storage.PublicationUnknown {
		t.Fatalf("unknown application settled %v", settled)
	}
	if _, err := s.Read(t.Context(), "one"); locking.CodeOf(err) != locking.Unavailable {
		t.Fatalf("unknown mapping reopened views: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(cfg.Root, "one"))
	if err != nil || string(content) != "new" {
		t.Fatalf("fault did not model applied syscall: %q %v", content, err)
	}
	if err := s.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown effect forgotten at Close: %v", err)
	}
}

func TestRawWriteStagingIsAnonymousUntilPublication(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	entered, resume := make(chan struct{}), make(chan struct{})
	original := s.ops.write
	s.ops.write = func(fd int, p []byte) (int, error) { close(entered); <-resume; return original(fd, p) }
	done := make(chan error, 1)
	go func() { done <- s.Write(context.Background(), "one", []byte("new")) }()
	<-entered
	entries, err := s.List(t.Context(), "")
	if err != nil || len(entries) != 0 {
		t.Fatalf("raw staged bytes exposed: %+v %v", entries, err)
	}
	close(resume)
	if err := operationAwait(t, done); err != nil {
		t.Fatal(err)
	}
	entries, err = s.List(t.Context(), "")
	if err != nil || len(entries) != 1 || entries[0].Name != "one" {
		t.Fatalf("published raw entry: %+v %v", entries, err)
	}
}

func TestRawUncertainStageLinkIsCleanedAndFenced(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	original := s.ops.link
	s.ops.link = func(a int, b string, c int, d string, flags int) error {
		if err := original(a, b, c, d, flags); err != nil {
			return err
		}
		return syscall.EIO
	}
	if err := s.Write(t.Context(), "one", []byte("new")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("uncertain stage link: %v", err)
	}
	if _, err := s.List(t.Context(), ""); locking.CodeOf(err) != locking.Unavailable {
		t.Fatalf("uncertain stage link left namespace admitted: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("uncertain staging link escaped cleanup: %+v %v", entries, err)
	}
}

func TestUnrelatedWriteProgressesDuringDurableLeasePreparation(t *testing.T) {
	s := operationStorage(t, nil)
	if err := s.Write(t.Context(), "one", []byte("old")); err != nil {
		t.Fatal(err)
	}
	owner := operationOwner(t, s)
	resource, err := s.LockService().Resolve(t.Context(), owner, "one")
	if err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	original := s.state.ops.fsync
	s.state.ops.fsync = func(fd int) error { once.Do(func() { close(entered); <-resume }); return original(fd) }
	done := make(chan error, 1)
	go func() {
		_, err := s.LockService().Acquire(t.Context(), locking.AcquireRequest{Owner: owner, Request: "raise", Resource: resource, Mode: locking.Exclusive, TTL: time.Second})
		done <- err
	}()
	<-entered
	unrelated := make(chan error, 1)
	go func() { unrelated <- s.Write(t.Context(), "two", []byte("other")) }()
	select {
	case err := <-unrelated:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("unrelated write blocked behind durable lease preparation")
	}
	close(resume)
	if err := operationAwait(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestFailedAccountingUnwindFencesBeforeNamespaceEffect(t *testing.T) {
	cfg := stateTestConfig(t)
	if err := Init(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	s, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error {
			if result != storage.PublicationNotApplied {
				t.Errorf("unwind result %v", result)
			}
			return syscall.EPERM
		}, nil
	})
	ctx := storage.WithPublicationAccounting(first, func(int64, int64) (storage.PublicationSettlement, error) { return nil, syscall.ENOSPC })
	err = s.Write(ctx, "one", []byte("new"))
	if !storage.IsPublicationAccountingUncertain(err) || !errors.Is(err, syscall.EPERM) || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("unwind error chain: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Root, "one")); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed accounting changed target: %v", err)
	}
	if err := s.Create(t.Context(), "two"); locking.CodeOf(err) != locking.Unavailable {
		t.Fatalf("uncertain accounting did not fence namespace: %v", err)
	}
}

func TestRejectedAccountingWithCleanUnwindPreservesAvailability(t *testing.T) {
	s := operationStorage(t, nil)
	ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) { return nil, syscall.EIO })
	if err := s.Create(ctx, "one"); !errors.Is(err, syscall.EIO) || storage.IsPublicationAccountingUncertain(err) {
		t.Fatalf("clean rejection: %v", err)
	}
	if err := s.Create(t.Context(), "one"); err != nil {
		t.Fatalf("clean rejection fenced namespace: %v", err)
	}
}

func TestCapabilityProbeRejectsUnavailableDescriptorOperations(t *testing.T) {
	for _, operation := range []string{"chmod", "link"} {
		t.Run(operation, func(t *testing.T) {
			s, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if operation == "chmod" {
				s.ops.chmod = func(int, uint32) error { return syscall.EOPNOTSUPP }
			} else {
				s.ops.link = func(int, string, int, string, int) error { return syscall.EOPNOTSUPP }
			}
			if err := s.probeOperations(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("missing capability probe: %v", err)
			}
			entries, err := os.ReadDir(s.root)
			if err != nil || len(entries) != 0 || s.stagedBytes != 0 {
				t.Fatalf("probe artifacts remained: %+v %d %v", entries, s.stagedBytes, err)
			}
		})
	}
}

func TestKnownWriteFailuresDoNotPublishOrRetainStageCapacity(t *testing.T) {
	for _, operation := range []string{"write", "zero write", "stage sync", "rename"} {
		t.Run(operation, func(t *testing.T) {
			s := operationStorage(t, nil)
			if err := s.Write(t.Context(), "one", []byte("old")); err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "write":
				s.ops.write = func(int, []byte) (int, error) { return 0, syscall.ENOSPC }
			case "zero write":
				s.ops.write = func(int, []byte) (int, error) { return 0, nil }
			case "stage sync":
				s.ops.sync = func(int) error { return syscall.ENOSPC }
			case "rename":
				s.ops.rename = func(int, string, int, string) error { return syscall.EACCES }
			}
			var settled storage.PublicationResult
			ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
				return func(result storage.PublicationResult) error { settled = result; return nil }, nil
			})
			if err := s.Write(ctx, "one", []byte("new")); err == nil {
				t.Fatal("injected write failure disappeared")
			}
			if operation == "rename" && settled != storage.PublicationNotApplied {
				t.Fatalf("failed final rename settled %v", settled)
			}
			if operation != "rename" && settled != 0 {
				t.Fatalf("staging reached publication accounting: %v", settled)
			}
			if s.stagedBytes != 0 {
				t.Fatalf("failed write leaked staging budget: %d", s.stagedBytes)
			}
			content, err := s.Read(t.Context(), "one")
			if err != nil || string(content) != "old" {
				t.Fatalf("failed write changed contents: %q %v", content, err)
			}
			entries, err := os.ReadDir(s.state.statePath + "/staging")
			if err != nil || len(entries) != 0 {
				t.Fatalf("failed write left private litter: %+v %v", entries, err)
			}
		})
	}
}

func TestPrivateStageDirectorySyncFailureKeepsAppliedPublication(t *testing.T) {
	s := operationStorage(t, nil)
	if err := s.Write(t.Context(), "one", []byte("old")); err != nil {
		t.Fatal(err)
	}
	scope := operationGrant(t, s, "one", time.Minute)
	original := s.ops.sync
	var directorySyncs []nativeIdentity
	rootIdentity, _, err := identityOf(s.state.rootFD)
	if err != nil {
		t.Fatal(err)
	}
	stageIdentity, _, err := identityOf(s.state.stagingFD)
	if err != nil {
		t.Fatal(err)
	}
	s.ops.sync = func(fd int) error {
		identity, stat, err := identityOf(fd)
		if err != nil {
			return err
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			directorySyncs = append(directorySyncs, identity)
		}
		if identity == stageIdentity {
			return syscall.EIO
		}
		return original(fd)
	}
	var settled storage.PublicationResult
	ctx := storage.WithPublicationAccounting(locking.WithScope(t.Context(), scope), func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous != 3 || next != 11 {
			t.Errorf("publication byte counts %d -> %d", previous, next)
		}
		return func(result storage.PublicationResult) error { settled = result; return nil }, nil
	})
	if err := s.Write(ctx, "one", []byte("replacement")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("missing source-directory sync failure: %v", err)
	}
	if len(directorySyncs) != 2 || directorySyncs[0] != rootIdentity || directorySyncs[1] != stageIdentity {
		t.Fatalf("rename directory durability order: %+v", directorySyncs)
	}
	if settled != storage.PublicationApplied {
		t.Fatalf("source sync failure settled %v", settled)
	}
	content, err := s.Read(t.Context(), "one")
	if err != nil || string(content) != "replacement" {
		t.Fatalf("applied replacement became unavailable: %q %v", content, err)
	}
	resource, err := s.LockService().Resolve(t.Context(), scope.Owner, "one")
	if err != nil || resource.ID != scope.Grants[0].Resource {
		t.Fatalf("source sync failure lost logical identity: %+v %v", resource, err)
	}
	staged, err := os.ReadDir(filepath.Join(s.state.statePath, "staging"))
	if err != nil || len(staged) != 0 {
		t.Fatalf("source sync failure left live staging entries: %+v %v", staged, err)
	}
}

func TestRenameBetweenHardlinksDoesNotCreditDisplacement(t *testing.T) {
	s := operationStorage(t, nil)
	if err := s.Write(t.Context(), "one", []byte("contents")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(s.root, "one"), filepath.Join(s.root, "two")); err != nil {
		t.Fatal(err)
	}
	var settled storage.PublicationResult
	ctx := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous != 0 || next != 0 {
			t.Errorf("same-inode rename credited bytes %d -> %d", previous, next)
		}
		return func(result storage.PublicationResult) error { settled = result; return nil }, nil
	})
	if err := s.Rename(ctx, "one", "two"); err != nil {
		t.Fatal(err)
	}
	if settled != storage.PublicationNotApplied {
		t.Fatalf("same-inode rename settled %v", settled)
	}
	for _, name := range []string{"one", "two"} {
		content, err := s.Read(t.Context(), name)
		if err != nil || string(content) != "contents" {
			t.Fatalf("same-inode rename changed %s: %q %v", name, content, err)
		}
	}
}

func TestOperationCloseFailureRetainsOwnershipWithoutRetryingDescriptor(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.state.Close()
	var consumed int
	s.ops.close = func(fd int) error {
		consumed++
		if err := unix.Close(fd); err != nil {
			return err
		}
		return syscall.EIO
	}
	if _, err := s.Stat(t.Context(), ""); !errors.Is(err, syscall.EIO) {
		t.Fatalf("operation close failure: %v", err)
	}
	if consumed != 1 {
		t.Fatalf("operation consumed %d descriptors", consumed)
	}
	for range 2 {
		if err := s.Close(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("Close lost uncertainty: %v", err)
		}
	}
	if consumed != 1 {
		t.Fatalf("Close retried the consumed descriptor: %d", consumed)
	}
	if next, err := New(root); !errors.Is(err, syscall.EWOULDBLOCK) {
		if next != nil {
			next.Close()
		}
		t.Fatalf("uncertain close released ownership: %v", err)
	}
	// The injected close consumed its descriptor successfully; releasing the retained
	// state handles here is test cleanup after verifying fail-closed ownership.
	if err := s.state.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := New(root)
	if err != nil {
		t.Fatalf("known state cleanup did not release ownership: %v", err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
}
