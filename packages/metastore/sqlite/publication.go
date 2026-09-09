package sqlite

import (
	"context"
	"database/sql"
	"errors"
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
		return locking.Wrap(locking.Invalid, "lock authority is already attached or the namespace is closed", nil)
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

// LockService returns this namespace's bound authority, or nil when locking is disabled.
func (s *Store) LockService() locking.Service {
	if s.locks == nil {
		return nil
	}
	return s.locks
}

// CheckPublicationAccounting reports support for accounting at the final namespace transition.
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
		if !node.Mode.IsRegular() {
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
			`SELECT `+nodeColumns+` FROM nodes n WHERE n.namespace = ? AND n.id = ? AND n.detached = 0`, n.store.namespace, id))
		if errors.Is(err, sql.ErrNoRows) {
			return locking.Wrap(locking.StaleResource, "the resolved file no longer exists", err)
		}
		if err != nil {
			return err
		}
		if !node.Mode.IsRegular() {
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
	return locking.BackendKey(strconv.FormatInt(s.namespace, 10) + ":" + strconv.FormatInt(id, 10))
}

func (s *Store) backendNode(key locking.BackendKey) (int64, error) {
	prefix := strconv.FormatInt(s.namespace, 10) + ":"
	if !strings.HasPrefix(string(key), prefix) {
		return 0, locking.Wrap(locking.StaleResource, "resource belongs to another namespace", nil)
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(string(key), prefix), 10, 64)
	if err != nil || id < 1 || s.backendKey(id) != key {
		return 0, locking.Wrap(locking.StaleResource, "resource has an invalid node identity", err)
	}
	return id, nil
}

type namespaceIntent struct {
	kind    locking.MutationKind
	paths   []string
	node    int64
	cleanup bool
}

type namespacePublication struct {
	intent   namespaceIntent
	targets  []locking.BackendKey
	nodes    []int64
	retired  []locking.BackendKey
	previous int64
	next     int64
}

func (s *Store) mutateNamespace(ctx context.Context, kind locking.MutationKind, paths []string, mutate func(*sql.Tx) error) error {
	return s.mutatePublication(ctx, &namespaceIntent{kind: kind, paths: paths}, mutate)
}

func (s *Store) prepareNamespacePublication(ctx context.Context, tx *sql.Tx, intent namespaceIntent) (*namespacePublication, error) {
	publication := &namespacePublication{intent: intent}
	if intent.node != 0 {
		state, err := s.fileState(ctx, tx, intent.node)
		if err != nil {
			return nil, err
		}
		if state.Mode.IsRegular() && !state.Detached {
			publication.nodes = []int64{state.ID}
			publication.targets = []locking.BackendKey{s.backendKey(state.ID)}
		}
		if intent.kind == locking.WriteMutation || intent.cleanup {
			publication.previous = state.Size
		}
		return publication, nil
	}
	nodes := make([]metastore.Node, len(intent.paths))
	seen := make(map[int64]bool, len(intent.paths))
	for i, path := range intent.paths {
		node, err := s.resolve(ctx, tx, path)
		if errors.Is(err, syscall.ENOENT) {
			continue
		}
		if err != nil {
			return nil, err
		}
		nodes[i] = node
		if node.Mode.IsRegular() && !seen[node.ID] {
			seen[node.ID] = true
			publication.nodes = append(publication.nodes, node.ID)
			publication.targets = append(publication.targets, s.backendKey(node.ID))
		}
	}
	switch intent.kind {
	case locking.WriteMutation, locking.RemoveMutation:
		if nodes[0].ID != 0 && nodes[0].Mode.IsRegular() {
			publication.previous = nodes[0].Size
		}
	case locking.RenameMutation:
		if nodes[1].ID != 0 && nodes[1].ID != nodes[0].ID && nodes[1].Mode.IsRegular() {
			publication.previous = nodes[1].Size
		}
	}
	return publication, nil
}

func (s *Store) finishNamespacePublication(ctx context.Context, tx *sql.Tx, publication *namespacePublication) error {
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
		publication.next = node.Size
	}
	for i, id := range publication.nodes {
		var present bool
		if err := tx.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM nodes WHERE namespace = ? AND id = ? AND detached = 0)`, s.namespace, id).Scan(&present); err != nil {
			return err
		}
		if !present {
			publication.retired = append(publication.retired, publication.targets[i])
		}
	}
	if publication.intent.node == 0 && (publication.intent.kind == locking.RemoveMutation || publication.intent.kind == locking.RenameMutation) && publication.previous != 0 {
		// Retaining a removed destination keeps its current bytes charged. The
		// removal still retires the named strong resource in the same publication.
		var retained int64
		for _, id := range publication.nodes {
			var size int64
			err := tx.QueryRowContext(ctx, `SELECT size FROM nodes WHERE namespace=? AND id=? AND detached=1`, s.namespace, id).Scan(&size)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			retained += size
		}
		publication.next = retained
	}
	return nil
}

func (s *Store) publishNamespace(ctx context.Context, tx *sql.Tx, state DurableState, publication *namespacePublication) error {
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
			return locking.Wrap(locking.Unavailable, "this namespace has no lock authority", nil)
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
