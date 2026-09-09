package sqlite

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestRawStoreRejectsEnableLocks(t *testing.T) {
	config := lockingTestConfig(t)
	store, err := OpenWithOptions(t.Context(), config.Database, config.Namespace, 0, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := store.EnableLocks(t.Context(), locking.DefaultOptions()); !errors.Is(err, syscall.EINVAL) ||
		locking.CodeOf(err) != locking.Invalid {
		t.Fatalf("raw store enabled lock authority: %v", err)
	}
	assertNoLeaseAuthority(t, store)
	if err := store.Create(t.Context(), "ordinary"); err != nil {
		t.Fatalf("rejected authority changed ordinary mutation behavior: %v", err)
	}
	if _, err := store.Stat(t.Context(), "ordinary"); err != nil {
		t.Fatal(err)
	}
}

func TestSQLitePublicationDistinguishesPreparationFailureFromUncertainUnwind(t *testing.T) {
	for _, failedUnwind := range []bool{false, true} {
		t.Run(fmt.Sprintf("unwind failed %t", failedUnwind), func(t *testing.T) {
			f := newPublicationFixture(t)
			if err := f.store.CheckPublicationAccounting(); err != nil {
				t.Fatalf("native accounting capability: %v", err)
			}
			f.put(t, t.Context(), "file", 3)
			before := f.node(t, "file")
			object := f.stage(t, t.Context(), "file", 7)
			owner := f.owner(t)
			grant := f.grant(t, owner, "file", locking.Exclusive)
			primary := errors.New("quota preparation unavailable")
			var unwind error
			if failedUnwind {
				unwind = errors.New("quota reservation could not be released")
				*f.authorityFence = unwind
			}
			settlements := 0
			ctx := storage.WithPublicationAccounting(publicationScope(t.Context(), owner, grant),
				func(_, _ int64) (storage.PublicationSettlement, error) {
					return func(result storage.PublicationResult) error {
						settlements++
						if result != storage.PublicationNotApplied {
							t.Errorf("preparation rollback settled %v, want NotApplied", result)
						}
						return unwind
					}, primary
				})
			err := f.store.Commit(ctx, "file", object)
			if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, primary) || settlements != 1 ||
				storage.IsPublicationAccountingUncertain(err) != failedUnwind {
				t.Fatalf("preparation returned %v, settlements=%d, failed unwind=%v", err, settlements, failedUnwind)
			}
			if failedUnwind && !errors.Is(err, unwind) {
				t.Fatalf("preparation lost the unwind cause: %v", err)
			}
			status, statusErr := f.store.locks.Status(t.Context())
			if statusErr != nil || status.Unavailable != failedUnwind {
				t.Fatalf("preparation left authority status %+v, error %v", status, statusErr)
			}
			var size int64
			var content string
			if err := f.store.read.QueryRowContext(t.Context(),
				`SELECT size, content FROM nodes WHERE id = ?`, before.ID).Scan(&size, &content); err != nil ||
				size != before.Size || content != string(before.Content) {
				t.Fatalf("preparation changed native file to %d/%q: %v", size, content, err)
			}
			if failedUnwind {
				if _, err := f.store.Stat(t.Context(), "file"); storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, unwind) {
					t.Fatalf("uncertain accounting did not fence native reads: %v", err)
				}
			} else if state, err := f.store.locks.QueryGrant(t.Context(), owner, grant); err != nil || state.State != locking.Active {
				t.Fatalf("known preparation refusal displaced the grant: %+v, %v", state, err)
			}
		})
	}
}

type failedPublicationWitness struct{ cause error }

func (w failedPublicationWitness) Accept(DurableState) error   { return w.cause }
func (failedPublicationWitness) Checkpoint(DurableState) error { return nil }

func TestSQLitePublicationDurabilityFailureFencesBothAuthorities(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	object := f.stage(t, t.Context(), "file", 7)
	owner := f.owner(t)
	grant := f.grant(t, owner, "file", locking.Exclusive)
	witnessFailure := context.Canceled
	settlementFailure := errors.New("quota outcome could not be recorded")
	*f.authorityFence = witnessFailure
	f.store.witness = failedPublicationWitness{cause: witnessFailure}
	defer func() { f.store.witness = nil }()
	settlements := 0
	ctx := storage.WithPublicationAccounting(publicationScope(t.Context(), owner, grant),
		func(_, _ int64) (storage.PublicationSettlement, error) {
			return func(result storage.PublicationResult) error {
				settlements++
				if result != storage.PublicationUnknown {
					t.Errorf("unconfirmed durability settled as %v, want Unknown", result)
				}
				return settlementFailure
			}, nil
		})
	err := f.store.Commit(ctx, "file", object)
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, witnessFailure) || !errors.Is(err, settlementFailure) || settlements != 1 {
		t.Fatalf("unconfirmed commit = %v after %d settlements, want both causes and EIO", err, settlements)
	}
	status, err := f.store.locks.Status(t.Context())
	if err != nil || !status.Unavailable {
		t.Fatalf("authority accepted an unconfirmed namespace: %+v, %v", status, err)
	}
	if _, err := f.store.Stat(t.Context(), "file"); storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, witnessFailure) {
		t.Fatalf("namespace read after unconfirmed durability = %v", err)
	}
	var size int64
	if err := f.store.read.QueryRowContext(t.Context(),
		`SELECT size FROM nodes WHERE namespace = ? AND content = ?`, f.store.namespace, string(object.Key)).Scan(&size); err != nil {
		t.Fatal(err)
	}
	if size != object.Size {
		t.Fatalf("SQLite stored %d bytes before the witness failed, want %d", size, object.Size)
	}
}

func TestSQLitePublicationFenceDoesNotRetainClosedPoolBookkeeping(t *testing.T) {
	for _, abort := range []bool{false, true} {
		name := "close"
		if abort {
			name = "abort"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newPublicationFixture(t)
			fence := errors.New("publication outcome unavailable")
			*fixture.authorityFence = fence
			fixture.store.locks.Fence(fence)
			closeStore := fixture.store.Close
			if abort {
				closeStore = fixture.store.Abort
			}
			if err := closeStore(); !errors.Is(err, fence) {
				t.Fatalf("closing a fenced namespace returned %v, want original fence", err)
			}
			databaseCoordinators.Lock()
			retained := databaseCoordinators.byPath[fixture.store.coordinator.key]
			databaseCoordinators.Unlock()
			if retained != nil {
				t.Fatal("successfully closed SQL pools retained a coordinator reference")
			}
		})
	}
}

type publicationClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []publicationTimer
}

type publicationTimer struct {
	when time.Time
	ch   chan time.Time
}

func (c *publicationClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *publicationClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
	} else {
		c.timers = append(c.timers, publicationTimer{when: c.now.Add(d), ch: ch})
	}
	return ch
}

func (c *publicationClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	remaining := c.timers[:0]
	for _, timer := range c.timers {
		if timer.when.After(c.now) {
			remaining = append(remaining, timer)
		} else {
			timer.ch <- c.now
		}
	}
	c.timers = remaining
}

type publicationPersistence struct {
	mu    sync.Mutex
	start time.Time
	max   time.Duration
}

func (p *publicationPersistence) MaxLease(context.Context) (time.Duration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.max, nil
}

func (p *publicationPersistence) RaiseMaxLease(_ context.Context, d time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if d < p.max {
		return errors.New("lease watermark decreased")
	}
	p.max = d
	return nil
}

func (p *publicationPersistence) RecoveryStart() time.Time { return p.start }

type publicationFixture struct {
	store          *Store
	service        locking.Service
	clock          *publicationClock
	authorityFence *error
}

func newPublicationFixture(t *testing.T) publicationFixture {
	t.Helper()
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "store.db"), "workspace", 4096, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	var authorityFence error
	t.Cleanup(func() {
		err := store.Close()
		if authorityFence != nil {
			if !errors.Is(err, authorityFence) {
				t.Fatalf("store close = %v, want the original fence cause", err)
			}
		} else if err != nil {
			t.Error(err)
		}
	})
	clock := &publicationClock{now: time.Unix(1_700_000_000, 0)}
	options := locking.DefaultOptions()
	options.Clock = clock
	if err := store.enableLocks(t.Context(), options, &publicationPersistence{start: clock.Now()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := store.locks.Close()
		if authorityFence != nil {
			if !errors.Is(err, authorityFence) {
				t.Fatalf("authority close = %v, want the original fence cause", err)
			}
		} else if err != nil {
			t.Error(err)
		}
	})
	return publicationFixture{store: store, service: store.LockService(), clock: clock, authorityFence: &authorityFence}
}

func (f publicationFixture) owner(t *testing.T) locking.OwnerRef {
	t.Helper()
	ticket, err := f.service.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := f.service.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := f.service.CreateOwner(t.Context(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	return owner.Ref
}

func (f publicationFixture) resource(t *testing.T, owner locking.OwnerRef, path string) locking.ResourceRef {
	t.Helper()
	resource, err := f.service.Resolve(t.Context(), owner, path)
	if err != nil {
		t.Fatal(err)
	}
	return resource
}

func (f publicationFixture) grant(t *testing.T, owner locking.OwnerRef, path string, mode locking.Mode) locking.GrantRef {
	t.Helper()
	result, err := f.service.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner, Request: locking.RequestID(path), Resource: f.resource(t, owner, path),
		Mode: mode, TTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Recorded || result.Receipt.Outcome != locking.Granted || result.Grant == nil || result.Grant.State != locking.Active {
		t.Fatal("acquisition did not return a recorded active grant")
	}
	return result.Grant.Ref
}

func (f publicationFixture) stage(t *testing.T, ctx context.Context, path string, size int64) metastore.Object {
	t.Helper()
	key, err := f.store.Reserve(ctx, path, size)
	if err != nil {
		t.Fatal(err)
	}
	return metastore.Object{Key: key, Size: size, ModTime: time.Unix(1_700_000_001, 0)}
}

func (f publicationFixture) put(t *testing.T, ctx context.Context, path string, size int64) {
	t.Helper()
	if err := f.store.Commit(ctx, path, f.stage(t, ctx, path, size)); err != nil {
		t.Fatal(err)
	}
}

func (f publicationFixture) node(t *testing.T, path string) metastore.Node {
	t.Helper()
	node, err := f.store.Stat(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func requirePublicationCode(t *testing.T, err error, code locking.Code) {
	t.Helper()
	var typed *locking.Error
	if !errors.As(err, &typed) || typed.Code != code || storage.ErrnoOf(err) != locking.Errno(code) {
		t.Fatalf("publication error = %v, want lock code %s and errno %v", err, code, locking.Errno(code))
	}
}

func publicationScope(ctx context.Context, owner locking.OwnerRef, grants ...locking.GrantRef) context.Context {
	return locking.WithScope(ctx, locking.MutationScope{Owner: owner, Grants: grants})
}

func TestSQLitePublicationRejectsAnonymousMutationOfGrantedFiles(t *testing.T) {
	for _, mode := range []locking.Mode{locking.Shared, locking.Exclusive} {
		for _, operation := range []string{"commit", "mode", "access time", "modification time", "empty attributes", "remove", "rename source", "rename destination", "self rename"} {
			t.Run(string(mode)+"/"+operation, func(t *testing.T) {
				f := newPublicationFixture(t)
				f.put(t, t.Context(), "locked", 3)
				f.put(t, t.Context(), "other", 4)
				object := f.stage(t, t.Context(), "locked", 5)
				f.grant(t, f.owner(t), "locked", mode)
				before, err := f.store.List(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				space, err := f.store.Space(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				permission := fs.FileMode(0o600)
				at := time.Unix(1_800_000_000, 0)
				switch operation {
				case "commit":
					err = f.store.Commit(t.Context(), "locked", object)
				case "mode":
					err = f.store.SetAttr(t.Context(), "locked", storage.AttrChange{Mode: &permission})
				case "access time":
					err = f.store.SetAttr(t.Context(), "locked", storage.AttrChange{AccessTime: &at})
				case "modification time":
					err = f.store.SetAttr(t.Context(), "locked", storage.AttrChange{ModTime: &at})
				case "empty attributes":
					err = f.store.SetAttr(t.Context(), "locked", storage.AttrChange{})
				case "remove":
					err = f.store.Remove(t.Context(), "locked")
				case "rename source":
					err = f.store.Rename(t.Context(), "locked", "moved")
				case "rename destination":
					err = f.store.Rename(t.Context(), "other", "locked")
				case "self rename":
					err = f.store.Rename(t.Context(), "locked", "locked")
				}
				requirePublicationCode(t, err, locking.Conflict)
				after, err := f.store.List(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, after) {
					t.Fatal("rejected publication changed the namespace")
				}
				afterSpace, err := f.store.Space(t.Context())
				if err != nil || afterSpace != space {
					t.Fatalf("rejected publication changed quota accounting: %v", err)
				}
			})
		}
	}
}

func TestSQLitePublicationKeepsResourceIdentityAcrossContentAndRenames(t *testing.T) {
	f := newPublicationFixture(t)
	if err := f.store.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	f.put(t, t.Context(), "directory/file", 3)
	owner := f.owner(t)
	resource := f.resource(t, owner, "directory/file")
	grant := f.grant(t, owner, "directory/file", locking.Exclusive)
	scoped := publicationScope(t.Context(), owner, grant)
	initial := f.node(t, "directory/file")
	key := f.store.backendKey(initial.ID)
	f.put(t, scoped, "directory/file", 7)
	updated := f.node(t, "directory/file")
	if updated.ID != initial.ID || updated.Content == initial.Content || updated.Size != 7 {
		t.Fatal("content publication did not replace bytes under the same node")
	}
	if err := f.store.Rename(scoped, "directory/file", "directory/moved"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Rename(t.Context(), "directory", "renamed-directory"); err != nil {
		t.Fatalf("a descendant grant became an ancestor path lock: %v", err)
	}
	path := "renamed-directory/moved"
	if got := f.node(t, path); got.ID != initial.ID || f.store.backendKey(got.ID) != key {
		t.Fatal("rename changed the node or backend resource identity")
	}
	if got := f.resource(t, owner, path); got.ID != resource.ID {
		t.Fatal("content replacement or rename changed the lock resource identity")
	}
	requirePublicationCode(t, f.store.Remove(t.Context(), path), locking.Conflict)
	if err := f.store.Create(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	if got := f.resource(t, owner, "directory"); got.ID == resource.ID {
		t.Fatal("the vacated name reused the moved file's lock identity")
	}
}

func TestSQLiteLockResolutionRejectsUnsupportedTargets(t *testing.T) {
	f := newPublicationFixture(t)
	if err := f.store.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	owner := f.owner(t)
	for _, path := range []string{"", "directory", "absent", "absent/file"} {
		t.Run(path, func(t *testing.T) {
			_, err := f.service.Resolve(t.Context(), owner, path)
			requirePublicationCode(t, err, locking.UnsupportedTarget)
		})
	}
}

func TestSQLitePublicationChecksProofsEvenForNoOpMutations(t *testing.T) {
	for _, operation := range []string{"commit", "empty attributes", "self rename"} {
		for _, proof := range []string{"exclusive", "shared", "unrelated", "released", "expired", "owner without proof"} {
			t.Run(operation+"/"+proof, func(t *testing.T) {
				f := newPublicationFixture(t)
				f.put(t, t.Context(), "file", 3)
				f.put(t, t.Context(), "other", 4)
				owner := f.owner(t)
				mode, path := locking.Exclusive, "file"
				if proof == "shared" {
					mode = locking.Shared
				}
				if proof == "unrelated" {
					path = "other"
				}
				grant := f.grant(t, owner, path, mode)
				scoped := publicationScope(t.Context(), owner, grant)
				want := locking.Conflict
				switch proof {
				case "unrelated":
					want = locking.UnrelatedProof
				case "released":
					if _, err := f.service.Release(t.Context(), owner, grant); err != nil {
						t.Fatal(err)
					}
					want = locking.StaleGrant
				case "expired":
					f.clock.advance(11 * time.Second)
					want = locking.StaleGrant
				case "owner without proof":
					scoped = publicationScope(t.Context(), owner)
				}
				before := f.node(t, "file")
				var err error
				switch operation {
				case "commit":
					err = f.store.Commit(scoped, "file", f.stage(t, scoped, "file", 5))
				case "empty attributes":
					err = f.store.SetAttr(scoped, "file", storage.AttrChange{})
				case "self rename":
					err = f.store.Rename(scoped, "file", "file")
				}
				if proof == "exclusive" {
					if err != nil {
						t.Fatalf("live exclusive proof refused: %v", err)
					}
					if operation == "commit" && f.node(t, "file").Size != 5 {
						t.Fatal("authorized commit did not publish its new contents")
					}
				} else {
					requirePublicationCode(t, err, want)
					if after := f.node(t, "file"); after != before {
						t.Fatal("a denied proof changed the file")
					}
				}
			})
		}
	}
}

func TestSQLitePublicationAuthorizesExplicitAttributes(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	owner := f.owner(t)
	grant := f.grant(t, owner, "file", locking.Exclusive)
	mode := fs.FileMode(0o600)
	atime, mtime := time.Unix(1_800_000_000, 3), time.Unix(1_800_000_001, 4)
	if err := f.store.SetAttr(publicationScope(t.Context(), owner, grant), "file", storage.AttrChange{
		Mode: &mode, AccessTime: &atime, ModTime: &mtime,
	}); err != nil {
		t.Fatal(err)
	}
	node := f.node(t, "file")
	if node.Mode != mode || !node.AccessTime.Equal(atime) || !node.ModTime.Equal(mtime) {
		t.Fatal("authorized attribute mutation did not preserve its supplied values")
	}
}

func TestSQLitePublicationRetiresRemovedFileBeforeNameReuse(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	owner := f.owner(t)
	resource := f.resource(t, owner, "file")
	grant := f.grant(t, owner, "file", locking.Exclusive)
	old := f.node(t, "file")
	scoped := publicationScope(t.Context(), owner, grant)
	if err := f.store.Remove(scoped, "file"); err != nil {
		t.Fatal(err)
	}
	status, err := f.service.QueryGrant(t.Context(), owner, grant)
	if err != nil || status.State != locking.TargetGone {
		t.Fatalf("removed file's grant state = %s, error = %v", status.State, err)
	}
	f.put(t, t.Context(), "file", 5)
	if f.node(t, "file").ID == old.ID || f.resource(t, owner, "file").ID == resource.ID {
		t.Fatal("delete/recreate reused the removed resource")
	}
	_, err = f.service.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner, Request: "old-target", Resource: resource, Mode: locking.Exclusive, TTL: time.Second,
	})
	requirePublicationCode(t, err, locking.StaleResource)
	requirePublicationCode(t, f.store.SetAttr(scoped, "file", storage.AttrChange{}), locking.StaleGrant)
	if f.node(t, "file").Size != 5 {
		t.Fatal("retired proof affected the recreated file")
	}
}

func TestSQLitePublicationRenameRequiresBothTargetsAndRetiresOnlyTheDisplacedFile(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "source", 3)
	f.put(t, t.Context(), "destination", 5)
	owner := f.owner(t)
	source := f.node(t, "source")
	destination := f.node(t, "destination")
	sourceGrant := f.grant(t, owner, "source", locking.Exclusive)
	destinationGrant := f.grant(t, owner, "destination", locking.Exclusive)
	for _, grant := range []locking.GrantRef{sourceGrant, destinationGrant} {
		err := f.store.Rename(publicationScope(t.Context(), owner, grant), "source", "destination")
		requirePublicationCode(t, err, locking.Conflict)
		if f.node(t, "source") != source || f.node(t, "destination") != destination {
			t.Fatal("rename with a missing target proof changed the namespace")
		}
	}
	if err := f.store.Rename(publicationScope(t.Context(), owner, sourceGrant, destinationGrant), "source", "destination"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Stat(t.Context(), "source"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("renamed source still resolves: %v", err)
	}
	if f.node(t, "destination").ID != source.ID {
		t.Fatal("replacement did not move the source identity")
	}
	for _, expected := range []struct {
		grant locking.GrantRef
		state locking.GrantState
	}{{sourceGrant, locking.Active}, {destinationGrant, locking.TargetGone}} {
		status, err := f.service.QueryGrant(t.Context(), owner, expected.grant)
		if err != nil || status.State != expected.state {
			t.Fatalf("grant state = %s, want %s, error = %v", status.State, expected.state, err)
		}
	}
	if f.resource(t, owner, "destination").ID != sourceGrant.Resource {
		t.Fatal("replacement left the destination bound to the retired resource")
	}
	requirePublicationCode(t, f.store.Remove(t.Context(), "destination"), locking.Conflict)
}

func TestSQLiteReservationLeavesGrantEnforcementAtCommit(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	owner := f.owner(t)
	grant := f.grant(t, owner, "file", locking.Exclusive)
	before := f.node(t, "file")
	object := f.stage(t, t.Context(), "file", 7)
	if f.node(t, "file") != before {
		t.Fatal("reservation published the staged version")
	}
	requirePublicationCode(t, f.store.Commit(t.Context(), "file", object), locking.Conflict)
	if f.node(t, "file") != before {
		t.Fatal("refused staged commit changed the file")
	}
	if _, err := f.service.Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Commit(t.Context(), "file", object); err != nil {
		t.Fatalf("rejected publication consumed its reservation: %v", err)
	}
	if got := f.node(t, "file"); got.ID != before.ID || got.Content != object.Key || got.Size != object.Size {
		t.Fatal("unprotected commit did not publish its reserved object")
	}
}

func TestSQLiteStagingDoesNotExtendAnExclusiveGrant(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	owner := f.owner(t)
	grant := f.grant(t, owner, "file", locking.Exclusive)
	scoped := publicationScope(t.Context(), owner, grant)
	object := f.stage(t, scoped, "file", 7)
	before := f.node(t, "file")
	f.clock.advance(11 * time.Second)
	requirePublicationCode(t, f.store.Commit(scoped, "file", object), locking.StaleGrant)
	if f.node(t, "file") != before {
		t.Fatal("a grant that expired during staging still published its object")
	}
	if err := f.store.Commit(t.Context(), "file", object); err != nil {
		t.Fatalf("expired protection prevented a subsequent anonymous publication: %v", err)
	}
}

func TestSQLiteLockAuthorityCannotBeReplaced(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	f.grant(t, f.owner(t), "file", locking.Exclusive)
	options := locking.DefaultOptions()
	options.Clock = f.clock
	err := f.store.EnableLocks(t.Context(), options)
	requirePublicationCode(t, err, locking.Invalid)
	requirePublicationCode(t, f.store.Remove(t.Context(), "file"), locking.Conflict)
}

func TestSQLitePublicationRetiresAResolvedTargetWithoutAnyGrant(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	owner := f.owner(t)
	resource := f.resource(t, owner, "file")
	if err := f.store.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	f.put(t, t.Context(), "file", 5)
	_, err := f.service.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner, Request: "removed-target", Resource: resource, Mode: locking.Exclusive, TTL: time.Second,
	})
	requirePublicationCode(t, err, locking.StaleResource)
	if f.resource(t, owner, "file").ID == resource.ID {
		t.Fatal("an ungranted old resource reference was rebound to the recreated file")
	}
}

func TestSQLiteStagedCommitChecksTheActualPublicationTarget(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	object := f.stage(t, t.Context(), "file", 7)
	original := f.node(t, "file")
	if err := f.store.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	f.put(t, t.Context(), "file", 5)
	owner := f.owner(t)
	grant := f.grant(t, owner, "file", locking.Exclusive)
	replacement := f.node(t, "file")
	requirePublicationCode(t, f.store.Commit(t.Context(), "file", object), locking.Conflict)
	if f.node(t, "file") != replacement || f.node(t, "moved") != original {
		t.Fatal("staged commit affected a file before acquiring final target authorization")
	}
	if err := f.store.Commit(publicationScope(t.Context(), owner, grant), "file", object); err != nil {
		t.Fatal(err)
	}
	if got := f.node(t, "file"); got.ID != replacement.ID || got.Content != object.Key || got.Size != 7 {
		t.Fatal("commit did not publish through the actual target's exclusive grant")
	}
	if f.node(t, "moved") != original {
		t.Fatal("staged commit followed the old target to its new name")
	}
}

func TestSQLitePublicationSettlementFailureFencesNamespaceAndAuthority(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	object := f.stage(t, t.Context(), "file", 7)
	owner := f.owner(t)
	grant := f.grant(t, owner, "file", locking.Exclusive)
	cause := errors.New("quota settlement unavailable")
	*f.authorityFence = cause
	prepared, settled := 0, 0
	ctx := storage.WithPublicationAccounting(publicationScope(t.Context(), owner, grant),
		func(previous, next int64) (storage.PublicationSettlement, error) {
			prepared++
			if previous != 3 || next != 7 {
				t.Errorf("publication byte counts = %d -> %d, want 3 -> 7", previous, next)
			}
			return func(result storage.PublicationResult) error {
				settled++
				if result != storage.PublicationApplied {
					t.Errorf("settlement result = %d, want Applied", result)
				}
				return cause
			}, nil
		})
	err := f.store.Commit(ctx, "file", object)
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, cause) {
		t.Fatalf("settlement failure = %v, want EIO retaining its cause", err)
	}
	if prepared != 1 || settled != 1 {
		t.Fatalf("publication accounting prepared %d times and settled %d times", prepared, settled)
	}
	status, err := f.store.locks.Status(t.Context())
	if err != nil || !status.Unavailable {
		t.Fatalf("authority remained available after settlement failure: %v", err)
	}
	_, err = f.store.Stat(t.Context(), "file")
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, cause) {
		t.Fatalf("fenced namespace read = %v, want EIO retaining settlement failure", err)
	}
	err = f.store.Remove(t.Context(), "file")
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, cause) {
		t.Fatalf("fenced namespace mutation = %v, want EIO retaining settlement failure", err)
	}
}

func TestSQLitePublicationOrdersFreshReadsAfterAdmission(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	object := f.stage(t, t.Context(), "file", 7)
	owner := f.owner(t)
	grant := f.grant(t, owner, "file", locking.Exclusive)
	old, err := f.store.beginReadSnapshot(t.Context(), f.store.read)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Rollback()
	admitted, proceed := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	defer release()
	ctx := storage.WithPublicationAccounting(publicationScope(t.Context(), owner, grant),
		func(_, _ int64) (storage.PublicationSettlement, error) {
			close(admitted)
			<-proceed
			return func(storage.PublicationResult) error { return nil }, nil
		})
	written := make(chan error, 1)
	go func() { written <- f.store.Commit(ctx, "file", object) }()
	select {
	case <-admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not reach admitted publication")
	}
	type readResult struct {
		node metastore.Node
		err  error
	}
	read := make(chan readResult, 1)
	go func() {
		node, err := f.store.Stat(t.Context(), "file")
		read <- readResult{node: node, err: err}
	}()
	deadline := time.Now().Add(5 * time.Second)
	stacks := make([]byte, 1<<20)
	for {
		select {
		case result := <-read:
			t.Fatalf("fresh read crossed admitted publication: size=%d error=%v", result.node.Size, result.err)
		default:
		}
		blocked := false
		for _, stack := range strings.Split(string(stacks[:runtime.Stack(stacks, true)]), "\n\n") {
			if strings.Contains(stack, "TestSQLitePublicationOrdersFreshReadsAfterAdmission.func") &&
				strings.Contains(stack, "(*databaseCoordinator).beginHealthyRead") &&
				strings.Contains(stack, "(*Store).Stat") {
				blocked = true
				break
			}
		}
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fresh Stat never reached observation admission")
		}
		runtime.Gosched()
	}
	previous, err := f.store.resolve(t.Context(), old, "file")
	if err != nil || previous.Size != 3 {
		t.Fatalf("previously captured view lost its old version: size=%d error=%v", previous.Size, err)
	}
	release()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("admitted commit did not finish")
	}
	select {
	case result := <-read:
		if result.err != nil || result.node.Size != 7 || result.node.Content != object.Key {
			t.Fatalf("fresh read did not observe the completed publication: size=%d error=%v", result.node.Size, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fresh read did not resume after publication")
	}
}

func TestSQLitePublicationPreparationFailurePreservesReservationAndGrant(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	object := f.stage(t, t.Context(), "file", 7)
	owner := f.owner(t)
	grant := f.grant(t, owner, "file", locking.Exclusive)
	scoped := publicationScope(t.Context(), owner, grant)
	before := f.node(t, "file")
	objects, err := f.store.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	prepared := 0
	ctx := storage.WithPublicationAccounting(scoped,
		func(previous, next int64) (storage.PublicationSettlement, error) {
			prepared++
			if previous != 3 || next != 7 {
				t.Errorf("publication byte counts = %d -> %d, want 3 -> 7", previous, next)
			}
			return nil, syscall.EDQUOT
		})
	err = f.store.Commit(ctx, "file", object)
	if storage.ErrnoOf(err) != syscall.EDQUOT || !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("preparation failure = %v, want EDQUOT", err)
	}
	if prepared != 1 {
		t.Fatalf("accounting preparation ran %d times, want once", prepared)
	}
	if f.node(t, "file") != before {
		t.Fatal("refused preparation changed the node or its object binding")
	}
	afterObjects, err := f.store.ObjectStatus(t.Context())
	if err != nil || afterObjects != objects {
		t.Fatalf("refused preparation changed object maintenance state: %v", err)
	}
	status, err := f.service.QueryGrant(t.Context(), owner, grant)
	if err != nil || status.State != locking.Active {
		t.Fatalf("grant after refused preparation = %s, error = %v", status.State, err)
	}
	authority, err := f.store.locks.Status(t.Context())
	if err != nil || authority.Unavailable {
		t.Fatalf("known preparation refusal fenced the authority: %v", err)
	}
	if err := f.store.Commit(scoped, "file", object); err != nil {
		t.Fatalf("same proof could not commit the retained reservation: %v", err)
	}
	if node := f.node(t, "file"); node.ID != before.ID || node.Content != object.Key || node.Size != object.Size {
		t.Fatal("subsequent authorized commit did not publish the reserved object")
	}
}

func TestSQLitePublicationRejectsConflictBeforeAccounting(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	object := f.stage(t, t.Context(), "file", 7)
	f.grant(t, f.owner(t), "file", locking.Exclusive)
	before := f.node(t, "file")
	prepared, settled := 0, 0
	ctx := storage.WithPublicationAccounting(t.Context(),
		func(_, _ int64) (storage.PublicationSettlement, error) {
			prepared++
			return func(storage.PublicationResult) error {
				settled++
				return nil
			}, nil
		})
	requirePublicationCode(t, f.store.Commit(ctx, "file", object), locking.Conflict)
	if prepared != 0 || settled != 0 {
		t.Fatalf("conflicting mutation reached accounting: prepared=%d settled=%d", prepared, settled)
	}
	if f.node(t, "file") != before {
		t.Fatal("conflicting mutation changed the node or its object binding")
	}
}
