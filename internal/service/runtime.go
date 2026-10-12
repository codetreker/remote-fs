package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

type VolumeSummary struct {
	FileCount      uint64
	DirectoryCount uint64
	UsedBytes      int64
	QuotaBytes     int64
}

type runtimeLease struct {
	Store   *localstore.Store
	Release func() error
}

type volumeRuntime struct {
	store      *localstore.Store
	opening    bool
	frozen     bool
	closed     bool
	failure    error
	references int
	closeMu    sync.Mutex
}

type runtimeManager struct {
	root       *stateRoot
	management *management.Store
	mu         sync.Mutex
	changed    chan struct{}
	entries    map[management.ID]*volumeRuntime
	closed     bool
}

func newRuntimeManager(_ Config, root *stateRoot, _ *Authenticator, db *management.Store) *runtimeManager {
	return &runtimeManager{root: root, management: db, changed: make(chan struct{}), entries: make(map[management.ID]*volumeRuntime)}
}

func (m *runtimeManager) signalLocked() { close(m.changed); m.changed = make(chan struct{}) }

func (m *runtimeManager) acquire(ctx context.Context, volume management.Volume) (*runtimeLease, error) {
	return m.acquireState(ctx, volume, management.VolumeActive)
}

func (m *runtimeManager) acquireState(ctx context.Context, volume management.Volume, state management.VolumeState) (*runtimeLease, error) {
	if !volume.ID.Valid() || !volume.OwnerUserID.Valid() || volume.InitialQuotaBytes < 0 {
		return nil, syscall.EINVAL
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
			if current.OwnerUserID != volume.OwnerUserID || current.InitialQuotaBytes != volume.InitialQuotaBytes {
				m.mu.Unlock()
				return nil, syscall.EIO
			}
			if current.State != state {
				m.mu.Unlock()
				return nil, syscall.EBUSY
			}
		} else if volume.State != state {
			m.mu.Unlock()
			return nil, syscall.EBUSY
		}
		entry := m.entries[volume.ID]
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
		if entry != nil {
			entry.references++
			m.mu.Unlock()
			return m.lease(entry), nil
		}
		entry = &volumeRuntime{opening: true, references: 1}
		m.entries[volume.ID] = entry
		m.mu.Unlock()
		if state == management.VolumeCreating {
			if err := os.Mkdir(m.volumeRoot(volume.ID), 0700); err != nil && !errors.Is(err, os.ErrExist) {
				m.mu.Lock()
				entry.opening, entry.references, entry.failure = false, 0, err
				delete(m.entries, volume.ID)
				m.signalLocked()
				m.mu.Unlock()
				return nil, err
			}
		}
		store, err := localstore.Open(ctx, volumeConfig(m.volumeRoot(volume.ID), volume))
		m.mu.Lock()
		entry.opening = false
		if err != nil {
			entry.references = 0
			entry.failure = err
			if sqlite.OpenFailureRetainsOwnership(err) {
				entry.frozen = true
			} else {
				delete(m.entries, volume.ID)
			}
		} else {
			entry.store = store
		}
		stopped := m.closed
		m.signalLocked()
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if stopped {
			m.release(entry)
			return nil, syscall.EIO
		}
		return m.lease(entry), nil
	}
}

func (m *runtimeManager) lease(entry *volumeRuntime) *runtimeLease {
	var once sync.Once
	return &runtimeLease{Store: entry.store, Release: func() error {
		once.Do(func() { m.release(entry) })
		return nil
	}}
}

func (m *runtimeManager) release(entry *volumeRuntime) {
	m.mu.Lock()
	entry.references--
	m.signalLocked()
	m.mu.Unlock()
}

func (m *runtimeManager) volumeRoot(id management.ID) string {
	return filepath.Join(m.root.VolumesPath, string(id))
}

func volumeConfig(root string, volume management.Volume) localstore.Config {
	return localstore.Config{Root: root, Volume: string(volume.ID), Quota: volume.InitialQuotaBytes, Window: sqlite.DefaultWindow(), Maintenance: objectstore.DefaultOptions()}
}

func (m *runtimeManager) Initialize(ctx context.Context, volume management.Volume) error {
	lease, err := m.acquireState(ctx, volume, management.VolumeCreating)
	if err != nil {
		return err
	}
	return lease.Release()
}

func (m *runtimeManager) Summary(ctx context.Context, volume management.Volume) (summary VolumeSummary, returned error) {
	lease, err := m.acquire(ctx, volume)
	if err != nil {
		return VolumeSummary{}, err
	}
	defer lease.Release()
	snapshot, _, err := lease.Store.Log().Snapshot(ctx)
	if err != nil {
		return VolumeSummary{}, err
	}
	defer func() {
		returned = errors.Join(returned, snapshot.Close())
		if returned != nil {
			summary = VolumeSummary{}
		}
	}()
	for {
		page, err := metastore.NewRowResult(1<<20, 0, func(_ int, _ metastore.Row, lengths metastore.RowPayloadLengths) (int64, error) {
			return 256 + lengths.Name + lengths.Content + lengths.Metadata + lengths.Target, nil
		})
		if err != nil {
			return VolumeSummary{}, err
		}
		done, err := snapshot.Next(ctx, 256, page)
		if err != nil {
			return VolumeSummary{}, err
		}
		rows, err := page.Rows()
		if err != nil {
			return VolumeSummary{}, err
		}
		for _, row := range rows {
			if row.Parent == 0 {
				continue
			}
			switch row.Node.Kind {
			case storage.NodeDirectory:
				summary.DirectoryCount++
			case storage.NodeRegular, storage.NodeSymlink:
				summary.FileCount++
			default:
				return VolumeSummary{}, syscall.EIO
			}
		}
		if done {
			break
		}
	}
	space, err := lease.Store.Space(ctx)
	if err != nil {
		return VolumeSummary{}, err
	}
	if !space.Coherent() {
		return VolumeSummary{}, syscall.EIO
	}
	summary.UsedBytes, summary.QuotaBytes = space.Used, space.Total
	return summary, nil
}

func (m *runtimeManager) Delete(ctx context.Context, volume management.Volume) error {
	if !volume.ID.Valid() || !volume.OwnerUserID.Valid() {
		return syscall.EINVAL
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return syscall.EIO
		}
		if m.management != nil {
			current, err := m.management.GetVolume(ctx, volume.OwnerUserID, volume.ID)
			if err != nil {
				m.mu.Unlock()
				return err
			}
			if current.State != management.VolumeDeleting {
				m.mu.Unlock()
				return syscall.EBUSY
			}
		} else if volume.State != management.VolumeDeleting {
			m.mu.Unlock()
			return syscall.EBUSY
		}
		entry := m.entries[volume.ID]
		if entry == nil {
			m.mu.Unlock()
			return os.RemoveAll(m.volumeRoot(volume.ID))
		}
		entry.frozen = true
		m.signalLocked()
		m.mu.Unlock()
		entry.closeMu.Lock()
		err := m.waitUnused(ctx, entry)
		if err == nil && entry.store == nil {
			err = entry.failure
		}
		if err == nil && !entry.closed {
			if entry.store == nil {
				entry.closeMu.Unlock()
				return syscall.EIO
			}
			err = entry.store.Close()
			if err == nil {
				entry.closed = true
			}
		}
		if err == nil {
			err = os.RemoveAll(m.volumeRoot(volume.ID))
		}
		if err == nil {
			m.mu.Lock()
			if m.entries[volume.ID] == entry {
				delete(m.entries, volume.ID)
			}
			m.signalLocked()
			m.mu.Unlock()
		}
		entry.closeMu.Unlock()
		return err
	}
}

func (m *runtimeManager) waitUnused(ctx context.Context, entry *volumeRuntime) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.mu.Lock()
		ready := !entry.opening && entry.references == 0
		changed := m.changed
		m.mu.Unlock()
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (m *runtimeManager) stop() {
	m.mu.Lock()
	m.closed = true
	m.signalLocked()
	m.mu.Unlock()
}

func (m *runtimeManager) Close(ctx context.Context) error {
	m.stop()
	m.mu.Lock()
	entries := make([]*volumeRuntime, 0, len(m.entries))
	for _, entry := range m.entries {
		entry.frozen = true
		entries = append(entries, entry)
	}
	m.mu.Unlock()
	var failures []error
	for _, entry := range entries {
		entry.closeMu.Lock()
		err := m.waitUnused(ctx, entry)
		if err == nil && !entry.closed {
			if entry.store == nil {
				err = errors.Join(syscall.EIO, entry.failure)
			} else {
				err = entry.store.Close()
			}
			if err == nil {
				entry.closed = true
			}
		}
		entry.closeMu.Unlock()
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
