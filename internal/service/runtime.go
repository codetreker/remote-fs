package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

const serviceTransferMaxBytes = localdisk.DefaultMaxObjectBytes

type runtimeLease struct {
	Store   *localstore.Store
	Handler *httprest.Handler
	Release func() error
}

type volumeRuntime struct {
	volume             management.Volume
	store              *localstore.Store
	handler            *httprest.Handler
	opening            bool
	frozen             bool
	refs               int
	idleSince          time.Time
	proof              []byte
	committed          bool
	reserved           bool
	deletionReady      bool
	failure            error
	rollbackRetirement func()
	closeMu            sync.Mutex
}

type runtimeManager struct {
	config     Config
	root       *stateRoot
	auth       *Authenticator
	management *management.Store
	mu         sync.Mutex
	changed    chan struct{}
	entries    map[management.ID]*volumeRuntime
	closed     bool
	stopped    chan struct{}
	stopOnce   sync.Once
	workers    sync.WaitGroup
}

func newRuntimeManager(config Config, root *stateRoot, auth *Authenticator, db *management.Store) *runtimeManager {
	m := &runtimeManager{config: config, root: root, auth: auth, management: db, changed: make(chan struct{}), entries: make(map[management.ID]*volumeRuntime), stopped: make(chan struct{})}
	m.workers.Add(1)
	go m.evictIdle()
	return m
}

func (m *runtimeManager) acquire(ctx context.Context, volume management.Volume, control bool) (*runtimeLease, error) {
	return m.acquireWithQuota(ctx, volume, false, 0, !control, false)
}

func (m *runtimeManager) acquireFile(ctx context.Context, volume management.Volume, _ bool) (*runtimeLease, error) {
	return m.acquireWithQuota(ctx, volume, false, 0, true, false)
}

func (m *runtimeManager) acquireWithQuota(ctx context.Context, volume management.Volume, initialize bool, quota int64, files, deleting bool) (*runtimeLease, error) {
	if err := validateRuntimeVolume(volume); err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, syscall.EIO
		}
		if m.management != nil {
			current, err := m.management.GetVolume(ctx, volume.OwnerUserID, volume.ID)
			if err != nil {
				m.mu.Unlock()
				return nil, err
			}
			if current.StorageName != volume.StorageName || current.OwnerUserID != volume.OwnerUserID {
				m.mu.Unlock()
				return nil, management.ErrCorrupt
			}
			if (!initialize && !deleting && current.State != management.VolumeActive) || current.State == management.VolumeDeleted || current.State == management.VolumeFailed {
				m.mu.Unlock()
				return nil, syscall.EBUSY
			}
		}
		entry := m.entries[volume.ID]
		if entry != nil && (entry.volume.OwnerUserID != volume.OwnerUserID || entry.volume.StorageName != volume.StorageName) {
			m.mu.Unlock()
			return nil, syscall.EIO
		}
		if entry != nil && entry.frozen {
			m.mu.Unlock()
			return nil, syscall.EBUSY
		}
		if entry != nil && entry.opening {
			changed := m.changed
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-changed:
			}
			continue
		}
		if entry != nil && (!files || entry.handler != nil) {
			entry.refs++
			m.mu.Unlock()
			return m.lease(entry), nil
		}
		fresh := entry == nil
		if fresh {
			entry = &volumeRuntime{volume: volume}
			m.entries[volume.ID] = entry
		}
		entry.opening = true
		entry.refs++
		m.workers.Add(1)
		m.mu.Unlock()
		var store *localstore.Store
		var handler *httprest.Handler
		var err error
		if fresh {
			store, handler, err = m.open(ctx, volume, initialize, quota, files)
		} else {
			handler, err = m.handler(volume, entry.store)
		}
		m.mu.Lock()
		entry.opening = false
		if err != nil {
			entry.refs--
			if fresh {
				if store != nil || sqlite.OpenFailureRetainsOwnership(err) {
					entry.store, entry.handler, entry.frozen, entry.failure = store, handler, true, err
				} else {
					delete(m.entries, volume.ID)
				}
			}
		} else {
			if fresh {
				entry.store = store
			}
			entry.handler = handler
			entry.idleSince = time.Now()
		}
		stopped := m.closed
		m.signalLocked()
		m.mu.Unlock()
		m.workers.Done()
		if err != nil {
			return nil, err
		}
		if stopped {
			storeHandlerStop(entry)
			m.release(entry)
			return nil, syscall.EIO
		}
		return m.lease(entry), nil
	}
}

func (m *runtimeManager) signalLocked() { close(m.changed); m.changed = make(chan struct{}) }

func (m *runtimeManager) lease(entry *volumeRuntime) *runtimeLease {
	m.mu.Lock()
	store, handler := entry.store, entry.handler
	m.mu.Unlock()
	var once sync.Once
	return &runtimeLease{Store: store, Handler: handler, Release: func() error {
		once.Do(func() { m.release(entry) })
		return nil
	}}
}

func (m *runtimeManager) release(entry *volumeRuntime) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry.refs--
	if entry.refs == 0 {
		entry.idleSince = time.Now()
	}
	m.signalLocked()
}

func (m *runtimeManager) referencesLocked() int {
	var references int
	for _, entry := range m.entries {
		references += entry.refs
	}
	return references
}

func (m *runtimeManager) removeLocked(entry *volumeRuntime) { delete(m.entries, entry.volume.ID) }

func (m *runtimeManager) open(ctx context.Context, volume management.Volume, initialize bool, quota int64, files bool) (*localstore.Store, *httprest.Handler, error) {
	root := filepath.Join(m.root.VolumesPath, volume.StorageName)
	if initialize {
		directory, err := lifecycleDirectory(root)
		if err != nil {
			return nil, nil, err
		}
		marker, err := lifecycleXattr(int(directory.Fd()), createProofXattr)
		closeErr := directory.Close()
		if err != nil || closeErr != nil {
			return nil, nil, errors.Join(err, closeErr)
		}
		var proof struct {
			Volume    management.ID `json:"volume"`
			Operation management.ID `json:"operation"`
		}
		if err := json.Unmarshal(marker, &proof); err != nil {
			return nil, nil, err
		}
		if proof.Volume != volume.ID || proof.Operation != volume.LastOperationID {
			return nil, nil, syscall.EIO
		}
	}
	native, err := volumeConfig(m.config, root, volume, initialize, quota)
	if err != nil {
		return nil, nil, err
	}
	store, err := localstore.Open(ctx, native)
	if err != nil {
		return nil, nil, err
	}
	fail := func(cause error) (*localstore.Store, *httprest.Handler, error) {
		if closeErr := store.Close(); closeErr != nil {
			return store, nil, errors.Join(cause, closeErr)
		}
		return nil, nil, cause
	}
	if m.management != nil {
		if err := m.applyRetirementFloor(ctx, store, volume.OwnerUserID); err != nil {
			return fail(err)
		}
	}
	if !files {
		return store, nil, nil
	}
	handler, err := m.handler(volume, store)
	if err != nil {
		return fail(err)
	}
	return store, handler, nil
}

func (m *runtimeManager) handler(volume management.Volume, store *localstore.Store) (*httprest.Handler, error) {
	options := httprest.DefaultHandlerOptions()
	options.Volume = string(volume.ID)
	if m.auth != nil {
		options.Authorizer = m.auth.Authorizer(volume.ID)
		options.AccessBinding = m.auth.AccessBinding
	}
	options.Transfers = httprest.TransferOptions{Enabled: true, SpoolDir: m.root.TransfersPath, MaxBytes: serviceTransferMaxBytes, MaxConcurrent: 8, Lifetime: 30 * time.Minute,
		ActionNamespace: func(ctx context.Context, key storage.ActionKey) error {
			return m.registerNamespace(ctx, volume.ID, store, key)
		}}
	return httprest.NewHandlerWithOptions(store, store.Log(), options)
}

func volumeConfig(c Config, root string, volume management.Volume, initialize bool, quota int64) (localstore.Config, error) {
	if err := validateRuntimeVolume(volume); err != nil {
		return localstore.Config{}, err
	}
	locks := locking.DefaultOptions()
	return localstore.Config{Root: root, Volume: volume.StorageName, Quota: quota, QuotaPolicy: sqlite.DurableQuotaPolicy, InitializeQuota: initialize,
		MaxAuthorityActions: c.MaxAuthorityActions, Window: sqlite.DefaultWindow(), Maintenance: objectstore.DefaultOptions(), Locks: &locks, InitializeLocks: initialize}, nil
}

func fileSessionOptions(_ Config) storage.FileSessionOptions {
	return storage.DefaultFileSessionOptions()
}

func validateRuntimeVolume(volume management.Volume) error {
	if volume.ID == "" || volume.OwnerUserID == "" || volume.StorageName == "" || filepath.Base(volume.StorageName) != volume.StorageName || volume.StorageName == "." || volume.StorageName == ".." {
		return syscall.EINVAL
	}
	return nil
}

func (m *runtimeManager) registerNamespace(ctx context.Context, volume management.ID, store *localstore.Store, key storage.ActionKey) error {
	if err := key.Check(); err != nil {
		return err
	}
	if key.Namespace > math.MaxInt64 {
		return syscall.EINVAL
	}
	if m.auth == nil || m.management == nil {
		return syscall.EACCES
	}
	binding, err := m.auth.AccessBinding(ctx)
	if err != nil {
		return err
	}
	if binding.SubjectID != key.SubjectID {
		return syscall.EACCES
	}
	namespace, err := m.management.GetNamespace(ctx, management.ID(key.SubjectID), int64(key.Namespace))
	if err != nil {
		return err
	}
	if err := m.management.RegisterNamespaceParticipation(ctx, namespace.Actor, namespace.Sequence, volume); err != nil {
		return err
	}
	receipt, queryErr := store.QueryImport(ctx, key)
	if receipt.Check() != nil || receipt.Action != key {
		return errors.Join(syscall.EIO, queryErr)
	}
	if receipt.Outcome == storage.DurableActionHistoryExpired {
		return errors.Join(syscall.EACCES, queryErr)
	}
	if receipt.Outcome != storage.DurableActionUnknown {
		if queryErr != nil && !(receipt.Outcome == storage.DurableActionNotExecuted && receipt.Failure != 0 && errors.Is(queryErr, receipt.Failure)) {
			return queryErr
		}
		return nil
	}
	if err := m.management.CheckNamespaceAdmission(ctx, namespace.Actor, namespace.Sequence); err != nil {
		return err
	}
	return store.RegisterActionNamespace(ctx, storage.ActionNamespace{SubjectID: string(namespace.Actor), Sequence: uint64(namespace.Sequence), AcceptUntil: namespace.AcceptUntil})
}

func (m *runtimeManager) applyRetirementFloor(ctx context.Context, store *localstore.Store, actor management.ID) error {
	floor, _, proof, err := m.management.ActorRetirement(ctx, actor)
	if err != nil {
		return err
	}
	if floor == 0 {
		return nil
	}
	if len(proof) <= 32 {
		return management.ErrCorrupt
	}
	var evidence struct {
		CutoffMillis string `json:"cutoffMillis"`
	}
	if err := json.Unmarshal(proof[32:], &evidence); err != nil {
		return errors.Join(management.ErrCorrupt, err)
	}
	millis, err := strconv.ParseInt(evidence.CutoffMillis, 10, 64)
	if err != nil || strconv.FormatInt(millis, 10) != evidence.CutoffMillis {
		return management.ErrCorrupt
	}
	return store.RetireActionNamespaces(ctx, storage.ActionNamespaceRetirement{SubjectID: string(actor), Before: uint64(floor) + 1, RetainAfter: time.UnixMilli(millis).UTC()})
}

func durableRuntimeProof(identity storage.BackendIdentityResult) ([]byte, error) {
	if err := identity.Check(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Volume     storage.VolumeID `json:"volume"`
		RootNodeID uint64           `json:"rootNodeId"`
	}{identity.Volume, identity.RootNodeID})
}

func (m *runtimeManager) Summary(ctx context.Context, volume management.Volume) (storage.VolumeSummaryResult, error) {
	lease, err := m.acquire(ctx, volume, true)
	if err != nil {
		return storage.VolumeSummaryResult{}, err
	}
	defer lease.Release()
	return lease.Store.Summary(ctx)
}

func (m *runtimeManager) Initialize(ctx context.Context, volume management.Volume, quota int64) ([]byte, error) {
	if quota < 0 {
		return nil, syscall.EINVAL
	}
	lease, err := m.acquireWithQuota(ctx, volume, true, quota, false, false)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	identity, err := lease.Store.BackendIdentity(ctx)
	if err != nil {
		return nil, err
	}
	return durableRuntimeProof(identity)
}

func (m *runtimeManager) Freeze(ctx context.Context, volume management.Volume) (LifecycleRetirement, error) {
	return m.freeze(ctx, volume, true)
}

func (m *runtimeManager) freeze(ctx context.Context, volume management.Volume, deletion bool) (LifecycleRetirement, error) {
	m.mu.Lock()
	prior := m.entries[volume.ID]
	if prior != nil && prior.frozen && (prior.committed || prior.reserved) && prior.refs == 0 {
		if deletion && !prior.deletionReady {
			m.mu.Unlock()
			return LifecycleRetirement{}, syscall.EBUSY
		}
		m.mu.Unlock()
		return m.retirement(prior), nil
	}
	m.mu.Unlock()
	lease, err := m.acquireWithQuota(ctx, volume, false, 0, false, true)
	if err != nil {
		return LifecycleRetirement{}, err
	}
	if err := lease.Release(); err != nil {
		return LifecycleRetirement{}, err
	}
	m.mu.Lock()
	entry := m.entries[volume.ID]
	if entry == nil || entry.opening || entry.frozen || entry.refs != 0 || entry.store == nil || entry.store.LiveTransferCount() != 0 {
		m.mu.Unlock()
		return LifecycleRetirement{}, syscall.EBUSY
	}
	entry.frozen = true
	m.mu.Unlock()
	entry.closeMu.Lock()
	identity, proofErr := entry.store.BackendIdentity(ctx)
	if proofErr == nil {
		entry.proof, proofErr = durableRuntimeProof(identity)
	}
	if proofErr != nil {
		entry.closeMu.Unlock()
		m.restoreUncommitted(entry)
		return LifecycleRetirement{}, proofErr
	}
	var reserveErr error
	if entry.handler != nil {
		reserveErr = entry.handler.ReserveRetirement(ctx)
		entry.rollbackRetirement = entry.handler.CancelRetirement
	} else {
		authority, ok := entry.store.LockService().(interface {
			ReserveRetirement(context.Context) error
			CancelRetirement()
		})
		if !ok {
			reserveErr = syscall.EOPNOTSUPP
		} else {
			reserveErr = authority.ReserveRetirement(ctx)
			entry.rollbackRetirement = authority.CancelRetirement
		}
	}
	if reserveErr != nil {
		entry.closeMu.Unlock()
		m.restoreUncommitted(entry)
		return LifecycleRetirement{}, reserveErr
	}
	if deletion {
		if err := entry.store.CheckDeletionReady(ctx); err != nil {
			entry.closeMu.Unlock()
			m.restoreUncommitted(entry)
			return LifecycleRetirement{}, err
		}
	}
	m.mu.Lock()
	entry.reserved = true
	entry.deletionReady = deletion
	m.mu.Unlock()
	entry.closeMu.Unlock()
	return m.retirement(entry), nil
}

func (m *runtimeManager) retirement(entry *volumeRuntime) LifecycleRetirement {
	return LifecycleRetirement{Close: func(ctx context.Context) ([]byte, error) { return m.closeEntry(ctx, entry) }, Cancel: func() error {
		entry.closeMu.Lock()
		defer entry.closeMu.Unlock()
		if entry.committed {
			return syscall.EBUSY
		}
		m.mu.Lock()
		closed := m.closed
		m.mu.Unlock()
		if closed {
			return syscall.EBUSY
		}
		if entry.rollbackRetirement != nil {
			entry.rollbackRetirement()
		}
		m.mu.Lock()
		if m.entries[entry.volume.ID] == entry {
			entry.frozen = false
			entry.reserved = false
			entry.deletionReady = false
			m.signalLocked()
		}
		m.mu.Unlock()
		return nil
	}}
}

func (m *runtimeManager) restoreUncommitted(entry *volumeRuntime) {
	entry.closeMu.Lock()
	defer entry.closeMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry.committed || m.closed || m.entries[entry.volume.ID] != entry {
		return
	}
	if entry.rollbackRetirement != nil {
		entry.rollbackRetirement()
	}
	entry.frozen = false
	entry.reserved = false
	entry.deletionReady = false
	m.signalLocked()
}

func (m *runtimeManager) closeEntry(ctx context.Context, entry *volumeRuntime) ([]byte, error) {
	entry.closeMu.Lock()
	defer entry.closeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	current := m.entries[entry.volume.ID]
	frozen := entry.frozen
	m.mu.Unlock()
	if current != entry {
		if len(entry.proof) > 0 {
			return append([]byte(nil), entry.proof...), nil
		}
		return nil, syscall.EIO
	}
	if !frozen {
		return nil, syscall.EBUSY
	}
	if entry.store == nil {
		return nil, errors.Join(syscall.EIO, entry.failure)
	}
	if len(entry.proof) == 0 {
		identity, err := entry.store.BackendIdentity(ctx)
		if err != nil {
			return nil, err
		}
		proof, err := durableRuntimeProof(identity)
		if err != nil {
			return nil, err
		}
		entry.proof = proof
	}
	m.mu.Lock()
	entry.committed = true
	m.mu.Unlock()
	if entry.handler != nil {
		entry.handler.Stop()
		if err := entry.handler.Close(ctx); err != nil {
			return nil, err
		}
	}
	if err := entry.store.Close(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.removeLocked(entry)
	m.signalLocked()
	m.mu.Unlock()
	return append([]byte(nil), entry.proof...), nil
}

func storeHandlerStop(entry *volumeRuntime) {
	if entry.handler != nil {
		entry.handler.Stop()
	}
}

func (m *runtimeManager) stop() {
	m.stopOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		close(m.stopped)
		for _, entry := range m.entries {
			if !entry.opening {
				storeHandlerStop(entry)
			}
		}
		m.signalLocked()
		m.mu.Unlock()
	})
}

func (m *runtimeManager) Close(ctx context.Context) error {
	m.stop()
	done := make(chan struct{})
	go func() { m.workers.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	for {
		m.mu.Lock()
		if m.referencesLocked() == 0 {
			m.mu.Unlock()
			break
		}
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
	m.mu.Lock()
	entries := make([]*volumeRuntime, 0, len(m.entries))
	for _, entry := range m.entries {
		entry.frozen = true
		entries = append(entries, entry)
	}
	m.mu.Unlock()
	var failures []error
	for _, entry := range entries {
		if _, err := m.closeEntry(ctx, entry); err != nil {
			failures = append(failures, fmt.Errorf("closing volume %s: %w", entry.volume.ID, err))
		}
	}
	return errors.Join(failures...)
}

func (m *runtimeManager) evictIdle() {
	defer m.workers.Done()
	period := m.config.IdleRuntimeTimeout / 2
	if period > time.Second {
		period = time.Second
	}
	if period < time.Millisecond {
		period = time.Millisecond
	}
	timer := time.NewTicker(period)
	defer timer.Stop()
	for {
		var now time.Time
		select {
		case <-m.stopped:
			return
		case now = <-timer.C:
		}
		m.mu.Lock()
		var candidates []management.Volume
		var retries []*volumeRuntime
		for _, entry := range m.entries {
			if entry.frozen && entry.committed && entry.refs == 0 {
				retries = append(retries, entry)
				continue
			}
			if !entry.opening && !entry.frozen && entry.refs == 0 && !entry.idleSince.IsZero() && now.Sub(entry.idleSince) >= m.config.IdleRuntimeTimeout {
				candidates = append(candidates, entry.volume)
			}
		}
		m.mu.Unlock()
		for _, entry := range retries {
			ctx, cancel := context.WithTimeout(context.Background(), m.config.ShutdownTimeout)
			_, _ = m.closeEntry(ctx, entry)
			cancel()
		}
		for _, volume := range candidates {
			ctx, cancel := context.WithTimeout(context.Background(), m.config.ShutdownTimeout)
			retirement, err := m.freeze(ctx, volume, false)
			if err == nil {
				if _, err := retirement.Close(ctx); err != nil {
					_ = retirement.Cancel()
				}
			}
			cancel()
		}
	}
}
