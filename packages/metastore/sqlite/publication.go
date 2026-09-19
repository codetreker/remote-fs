package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
	"github.com/codetreker/remote-fs/packages/storage"
)

// EnableLocks attaches one authority before this Store is exposed to callers. Native
// exclusive ownership and canonical lease recovery must already be configured. An
// authority cannot be replaced during the Store's lifetime.
func (s *Store) EnableLocks(ctx context.Context, options locking.Options) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.leaseRecovery == nil {
		return locking.Wrap(locking.Invalid, "native lease recovery must be configured before attaching an authority", nil)
	}
	if err := nativelease.VerifyExclusiveOwnership(s.leaseOwner); err != nil {
		return err
	}
	return s.attachLocksLocked(ctx, options, s)
}

func (s *Store) enableLocks(ctx context.Context, options locking.Options, persistence locking.Persistence) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	return s.attachLocksLocked(ctx, options, persistence)
}

func (s *Store) attachLocksLocked(ctx context.Context, options locking.Options, persistence locking.Persistence) error {
	if s.closed || s.locks != nil {
		return locking.Wrap(locking.Invalid, "lock authority is already attached or the volume is closed", nil)
	}
	if err := s.coordinator.healthy(); err != nil {
		return err
	}
	authority, err := locking.New(ctx, options, sqliteNative{s}, persistence)
	if err != nil {
		return err
	}
	s.locks = authority
	return nil
}

// LockService returns this volume's bound authority, or nil when locking is disabled.
func (s *Store) LockService() locking.Service {
	if s.locks == nil {
		return nil
	}
	return s.locks
}

// CheckPublicationAccounting reports support for accounting at the final volume transition.
func (s *Store) CheckPublicationAccounting() error { return nil }

func (s *Store) beginHealthyRead(ctx context.Context) error {
	if err := s.coordinator.beginHealthyRead(); err != nil {
		return err
	}
	if s.locks != nil {
		if err := s.locks.Check(ctx); err != nil {
			s.coordinator.endHealthyRead()
			return err
		}
	}
	return nil
}

type sqliteNative struct{ store *Store }

func (n sqliteNative) Discover(ctx context.Context, path string, adopt func(locking.BackendKey) (bool, error)) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return err
	}
	var key locking.BackendKey
	return n.ordered(ctx, func(tx *sql.Tx) error {
		node, err := n.store.resolve(ctx, tx, cleaned)
		if errors.Is(err, syscall.ENOENT) {
			return locking.Wrap(locking.UnsupportedTarget, "only an existing regular file can be locked", err)
		}
		if err != nil {
			return err
		}
		if node.Kind != storage.NodeRegular {
			return locking.Wrap(locking.UnsupportedTarget, "only an existing regular file can be locked", nil)
		}
		key = n.store.backendKey(node.ID)
		return nil
	}, func() error {
		_, err := adopt(key)
		return err
	})
}

func (n sqliteNative) Guard(ctx context.Context, key locking.BackendKey, transition func() error) error {
	id, err := n.store.backendNode(key)
	if err != nil {
		return err
	}
	return n.ordered(ctx, func(tx *sql.Tx) error {
		node, err := scanNode(tx.QueryRowContext(ctx,
			`SELECT `+nodeColumns+` FROM nodes n WHERE n.volume = ? AND n.id = ? AND n.detached = 0`, n.store.volume, id))
		if errors.Is(err, sql.ErrNoRows) {
			return locking.Wrap(locking.StaleResource, "the resolved file no longer exists", err)
		}
		if err != nil {
			return err
		}
		if node.Kind != storage.NodeRegular {
			return locking.Wrap(locking.UnsupportedTarget, "the resolved node is not a regular file", nil)
		}
		return nil
	}, transition)
}

// Node identifiers are never reused, so SQLite needs no retained descriptor or row pin.
func (n sqliteNative) Forget(context.Context, locking.BackendKey) error { return nil }

func (n sqliteNative) ordered(ctx context.Context, read func(*sql.Tx) error, transition func() error) error {
	if err := n.store.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer n.store.coordinator.commit.release()
	if err := n.store.inspect(ctx, read); err != nil {
		return err
	}
	return transition()
}

func (s *Store) backendKey(id int64) locking.BackendKey {
	return locking.BackendKey(strconv.FormatInt(s.volume, 10) + ":" + strconv.FormatInt(id, 10))
}

func (s *Store) backendNode(key locking.BackendKey) (int64, error) {
	prefix := strconv.FormatInt(s.volume, 10) + ":"
	if !strings.HasPrefix(string(key), prefix) {
		return 0, locking.Wrap(locking.StaleResource, "resource belongs to another volume", nil)
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(string(key), prefix), 10, 64)
	if err != nil || id < 1 || s.backendKey(id) != key {
		return 0, locking.Wrap(locking.StaleResource, "resource has an invalid node identity", err)
	}
	return id, nil
}

type volumeIntent struct {
	kind    locking.MutationKind
	paths   []string
	node    int64
	nodes   []int64
	added   int64
	scope   storage.UseScope
	cleanup bool
}

type volumePublication struct {
	intent            volumeIntent
	targets           []locking.BackendKey
	nodes             []int64
	retired           []locking.BackendKey
	access            []metastore.FileState
	removed           int64
	recoveredBefore   map[int64]int64
	recoveredReleased int64
	previous          int64
	next              int64
}

func (s *Store) mutateVolume(ctx context.Context, kind locking.MutationKind, paths []string, mutate func(*sql.Tx) error) error {
	return s.mutatePublication(ctx, &volumeIntent{kind: kind, paths: paths}, mutate)
}

func (s *Store) prepareVolumePublication(ctx context.Context, tx *sql.Tx, intent volumeIntent) (*volumePublication, error) {
	publication := &volumePublication{intent: intent, next: intent.added}
	ids := intent.nodes
	if intent.node != 0 {
		ids = []int64{intent.node}
	}
	if ids != nil {
		publication.access = make([]metastore.FileState, len(ids))
		for i, id := range ids {
			if id == 0 {
				continue
			}
			state, err := s.fileState(ctx, tx, id)
			if err != nil {
				return nil, err
			}
			publication.access[i] = state
		}
	} else {
		publication.access = make([]metastore.FileState, len(intent.paths))
		for i, path := range intent.paths {
			node, err := s.resolve(ctx, tx, path)
			if errors.Is(err, syscall.ENOENT) {
				continue
			}
			if err != nil {
				return nil, err
			}
			publication.access[i] = metastore.FileState{Node: node}
		}
	}
	if intent.node == 0 && intent.nodes == nil && len(intent.paths) != 0 &&
		(intent.kind == locking.CreateMutation || intent.kind == locking.WriteMutation) && publication.access[0].ID == 0 {
		parent, _, err := s.resolveParent(ctx, tx, intent.paths[0])
		if err != nil {
			return nil, err
		}
		pending, err := s.nodePendingUnlink(ctx, tx, parent.ID)
		if err != nil {
			return nil, err
		}
		if pending {
			return nil, storage.ErrPendingDelete
		}
	}
	if intent.kind == locking.WriteMutation && intent.scope.Token == "" && len(publication.access) != 0 && publication.access[0].ID != 0 {
		pending, err := s.nodePendingUnlink(ctx, tx, publication.access[0].ID)
		if err != nil {
			return nil, err
		}
		if pending {
			return nil, storage.ErrPendingDelete
		}
	}
	seen := make(map[int64]bool, len(publication.access))
	for _, node := range publication.access {
		if node.ID != 0 && s.coordinator.pins[retainedNode{s.volume, node.ID}] == 0 {
			if publication.recoveredBefore == nil {
				publication.recoveredBefore = make(map[int64]int64)
			}
			count, err := s.recoveredUnlinkContribution(ctx, tx, node.ID)
			if err != nil {
				return nil, err
			}
			publication.recoveredBefore[node.ID] = count
		}
		if node.ID != 0 && node.Kind == storage.NodeRegular && !node.Detached && !seen[node.ID] {
			seen[node.ID] = true
			publication.nodes = append(publication.nodes, node.ID)
			publication.targets = append(publication.targets, s.backendKey(node.ID))
		}
	}
	removed := -1
	switch intent.kind {
	case locking.WriteMutation:
		if len(publication.access) != 0 {
			publication.previous = publication.access[0].Size
		}
	case locking.RemoveMutation:
		if len(publication.access) != 0 {
			removed = 0
		}
	case locking.RenameMutation:
		if len(publication.access) > 1 && publication.access[1].ID != publication.access[0].ID {
			removed = 1
		}
	}
	if removed >= 0 {
		publication.removed = publication.access[removed].ID
		publication.previous = publication.access[removed].Size
	}
	if intent.cleanup && len(publication.access) != 0 {
		publication.previous = publication.access[0].Size
	}
	return publication, nil
}

func (s *Store) finishVolumePublication(ctx context.Context, tx *sql.Tx, publication *volumePublication) error {
	if publication.intent.kind == locking.WriteMutation {
		var node metastore.Node
		var err error
		if publication.intent.node != 0 {
			node, err = s.nodeByID(ctx, tx, publication.intent.node)
		} else {
			node, err = s.resolve(ctx, tx, publication.intent.paths[0])
		}
		if err != nil {
			return err
		}
		before := metastore.FileState{Node: metastore.Node{ID: node.ID, Kind: node.Kind}}
		if len(publication.access) != 0 && publication.access[0].ID != 0 {
			before = publication.access[0]
		}
		if err := s.checkContentPublication(ctx, before, node.Size, publication.intent.scope); err != nil {
			return err
		}
		publication.next = node.Size
	}
	if !publication.intent.cleanup && publication.intent.nodes == nil &&
		(publication.intent.kind == locking.RemoveMutation || publication.intent.kind == locking.RenameMutation) {
		for _, node := range publication.access {
			if node.ID == 0 {
				continue
			}
			if err := s.fileDomain.coordinator.CheckUse(ctx, uint64(node.ID), publication.intent.scope, storage.DeleteName); err != nil {
				return err
			}
		}
	}
	for i, id := range publication.nodes {
		var present bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE volume=? AND id=? AND detached=0)`, s.volume, id).Scan(&present); err != nil {
			return err
		}
		if !present {
			publication.retired = append(publication.retired, publication.targets[i])
		}
	}
	if publication.removed != 0 {
		var surviving int64
		err := tx.QueryRowContext(ctx, `SELECT size FROM nodes WHERE volume=? AND id=?`, s.volume, publication.removed).Scan(&surviving)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		publication.next += surviving
	}
	for id, before := range publication.recoveredBefore {
		after, err := s.recoveredUnlinkContribution(ctx, tx, id)
		if err != nil {
			return err
		}
		if after > before {
			return fmt.Errorf("publication created an unowned deletion obligation: %w", syscall.EIO)
		}
		publication.recoveredReleased += before - after
	}
	return nil
}

func (s *Store) publishVolume(ctx context.Context, tx *sql.Tx, state DurableState, publication *volumePublication) error {
	scope := locking.ScopeFromContext(ctx)
	commit := func() locking.PublicationOutcome {
		if !publication.intent.cleanup {
			if err := metastore.CheckFilePublication(ctx); err != nil {
				return locking.PublicationOutcome{Known: true, Err: err}
			}
		}
		settle, err := storage.PreparePublication(ctx, publication.previous, publication.next)
		if err != nil {
			if storage.IsPublicationAccountingUncertain(err) {
				s.fencePublication(err)
			}
			return locking.PublicationOutcome{Known: true, Err: err}
		}
		err = s.commitPrepared(tx, DurableState(state))
		if err != nil {
			settlementErr := settle(storage.PublicationUnknown)
			combined := errors.Join(err, settlementErr)
			s.coordinator.poisonLocked(combined)
			return locking.PublicationOutcome{Err: combined}
		}
		if err := settle(storage.PublicationApplied); err != nil {
			s.fencePublication(err)
			return locking.PublicationOutcome{Known: true, Changed: true, Retired: publication.retired, Err: err}
		}
		if publication.recoveredReleased > s.fileDomain.recoveredReferences {
			err := fmt.Errorf("recovered reference accounting cannot release %d of %d retained references: %w", publication.recoveredReleased, s.fileDomain.recoveredReferences, syscall.EIO)
			s.fencePublication(err)
			return locking.PublicationOutcome{Known: true, Changed: true, Retired: publication.retired, Err: err}
		}
		s.fileDomain.recoveredReferences -= publication.recoveredReleased
		return locking.PublicationOutcome{Known: true, Changed: true, Retired: publication.retired}
	}
	if publication.intent.cleanup {
		return commit().Err
	}
	if s.locks == nil {
		if err := nativelease.ValidateOpening(s.databasePath, false); err != nil {
			return err
		}
		if err := validateLeaseMutation(ctx, tx); err != nil {
			return err
		}
		if scope.Owner != (locking.OwnerRef{}) || len(scope.Grants) != 0 {
			return locking.Wrap(locking.Unavailable, "this volume has no lock authority", nil)
		}
		return commit().Err
	}
	return s.locks.Publish(ctx, locking.Publication{
		Kind: publication.intent.kind, Targets: publication.targets, Scope: scope,
	}, commit)
}

func (s *Store) fencePublication(err error) {
	s.coordinator.poisonLocked(err)
	if s.locks != nil {
		s.locks.Fence(err)
	}
}
