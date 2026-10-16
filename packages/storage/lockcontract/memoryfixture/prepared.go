package memoryfixture

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

// Prepared supplies independent native volumes from one healthy physical schema.
// Every ordinary volume is created before the first native lease binding. Children
// own fresh authorities, object contents, pools and workers, and must close serially.
// Strong-lease and recovery contracts require New: a nonzero database-wide lease
// watermark poisons this factory rather than delaying or weakening a successor.
type Prepared struct {
	mu          sync.Mutex
	database    string
	count       int
	next        int
	allowance   int64
	options     locking.Options
	cleanup     context.Context
	initialized bool
	active      *preparedChild
	failed      error
}

type preparedChild struct {
	meta    *sqlite.LockingStore
	objects *memory.Objects
	storage *objectstore.Storage
	close   func() error
}

// NewPrepared prepares exactly count empty logical volumes, without copying a
// database or its native lease identity. New allocates each volume at most once.
func NewPrepared(t *testing.T, count int, allowance int64, options locking.Options) *Prepared {
	t.Helper()
	p, err := prepare(t.Context(), filepath.Join(t.TempDir(), "volumes.db"), count, allowance, options)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func prepare(ctx context.Context, database string, count int, allowance int64, options locking.Options) (*Prepared, error) {
	if count <= 0 {
		return nil, fmt.Errorf("prepared volume count must be positive: %w", syscall.EINVAL)
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	for i := range count {
		meta, err := sqlite.Open(ctx, database, preparedVolume(i), allowance, sqlite.DefaultWindow())
		if err != nil {
			return nil, fmt.Errorf("preparing volume %d: %w", i, err)
		}
		if err := meta.Close(); err != nil {
			return nil, fmt.Errorf("closing prepared volume %d: %w", i, err)
		}
	}
	return &Prepared{
		database: database, count: count, allowance: allowance, options: options,
		cleanup: context.WithoutCancel(ctx),
	}, nil
}

func preparedVolume(index int) string { return fmt.Sprintf("contract-%d", index) }

// New opens the next empty volume through the complete native constructor. Cleanup
// checks its lease watermark and complete Close before another child may start.
func (p *Prepared) New(t *testing.T) (*sqlite.Store, *objectstore.Storage) {
	t.Helper()
	child, err := p.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.release(child); err != nil {
			t.Errorf("closing prepared volume: %v", err)
		}
	})
	return child.meta.Store, child.storage
}

func (p *Prepared) acquire(ctx context.Context) (*preparedChild, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed != nil {
		return nil, fmt.Errorf("prepared volumes are unusable: %w", p.failed)
	}
	if p.active != nil {
		return nil, fmt.Errorf("the preceding prepared volume is still open: %w", syscall.EBUSY)
	}
	if p.next == p.count {
		return nil, fmt.Errorf("all %d prepared volumes were allocated: %w", p.count, syscall.ENOSPC)
	}
	meta, err := sqlite.OpenLocking(ctx, sqlite.LockingConfig{
		Database: p.database, Volume: preparedVolume(p.next), Allowance: p.allowance,
		SQLite: sqlite.DefaultOptions(), Locks: p.options, Initialize: !p.initialized,
	})
	if err != nil {
		p.failed = fmt.Errorf("opening prepared volume %d: %w", p.next, err)
		return nil, p.failed
	}
	p.initialized = true
	if err := zeroLease(ctx, meta); err != nil {
		p.failed = errors.Join(err, meta.Close())
		return nil, p.failed
	}
	objects := memory.New()
	backing := objectstore.New(objects, meta)
	child := &preparedChild{meta: meta, objects: objects, storage: backing, close: backing.Close}
	p.next++
	p.active = child
	return child, nil
}

func zeroLease(ctx context.Context, meta *sqlite.LockingStore) error {
	if meta.Terminal() {
		return fmt.Errorf("the prepared native owner is closed: %w", syscall.ESTALE)
	}
	maximum, err := meta.MaxLease(ctx)
	if err != nil {
		return fmt.Errorf("reading prepared volume lease watermark: %w", err)
	}
	if maximum != 0 {
		return fmt.Errorf("prepared volume acquired a strong lease (%v): %w", maximum, syscall.EINVAL)
	}
	return nil
}

func (p *Prepared) release(child *preparedChild) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active != child || child == nil {
		return fmt.Errorf("the prepared volume is not the current owner: %w", syscall.EINVAL)
	}
	leaseErr := zeroLease(p.cleanup, child.meta)
	closeErr := child.close()
	p.active = nil
	if err := errors.Join(leaseErr, closeErr); err != nil {
		p.failed = err
		return err
	}
	return nil
}
