package smb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

// ChangeStream delivers an ordered, gap-checked history. Close must unblock Next.
// Position and Incarnation are sampled before Next starts; callers need not make
// their accessors safe against concurrent Next calls.
type ChangeStream interface {
	Next() (metastore.Change, error)
	Close() error
	Incarnation() metastore.Incarnation
	Position() metastore.Position
}

// ChangeSource binds observation to the same authority as a share's backend.
// Subscribe begins at the current committed tail; Resume must reject lost history.
// Context cancellation must interrupt subscription admission and Checkpoint.
type ChangeSource struct {
	Subscribe  func(context.Context) (ChangeStream, error)
	Resume     func(context.Context, metastore.Incarnation, metastore.Position) (ChangeStream, error)
	Checkpoint func(context.Context) (metastore.LogBarrier, error)
}

var ErrNotifyRescan = errors.New("directory notification continuity was lost; enumerate again")

type notificationIdentity struct {
	ID        uint64
	Directory bool
}

type notifyGroup struct {
	position  metastore.Position
	events    []wire.Notification
	proofs    []storage.EntryLocation
	wireBytes int64
	bytes     int64
}

type directoryWatcher struct {
	id         int64
	filter     uint32
	recursive  bool
	ready      bool
	barrier    metastore.Position
	groups     []notifyGroup
	bytes      int64
	events     int
	failure    error
	waiters    []*notifyWaiter
	generation uint64
	validating bool
	removed    bool
}

type notifyWaiter struct {
	wake    chan struct{}
	failure error
}

func (w *directoryWatcher) clearGroups() {
	clear(w.groups)
	w.groups = nil
	w.bytes = 0
	w.events = 0
}

func (w *directoryWatcher) fail(err error) {
	w.failure = err
	w.ready = false
	w.generation++
	// Validation borrows these immutable groups outside the manager lock. Their
	// storage remains charged until the validator releases its references.
	if !w.validating {
		w.clearGroups()
	}
}

func (w *directoryWatcher) deliverFailure() error {
	err := w.failure
	w.failure = nil
	w.ready = false
	w.clearGroups()
	for _, waiter := range w.waiters[1:] {
		waiter.failure = err
	}
	return err
}

func notificationImageRelevant(image *metastore.EventImage, id int64, recursive bool) bool {
	if image == nil || image.Location.State != storage.LocationLinked {
		return false
	}
	for i, entry := range image.Location.Ancestors {
		if entry.ParentID == uint64(id) {
			return recursive || i == len(image.Location.Ancestors)-1
		}
	}
	return false
}

type notificationManager struct {
	closeMu     sync.Mutex
	closeErr    error
	closeDone   bool
	source      ChangeSource
	limits      Limits
	ctx         context.Context
	cancel      context.CancelFunc
	setup       sync.Mutex
	mu          sync.Mutex
	stream      ChangeStream
	incarnation metastore.Incarnation
	position    metastore.Position
	failure     error
	closed      bool
	wake        chan struct{}
	watchers    map[string]*directoryWatcher
	wg          sync.WaitGroup
}

func newNotificationManager(ctx context.Context, source ChangeSource, limits Limits) (*notificationManager, error) {
	if source.Subscribe == nil || source.Resume == nil || source.Checkpoint == nil {
		return nil, ErrConfig
	}
	lifetime, cancel := context.WithCancel(context.Background())
	m := &notificationManager{source: source, limits: limits, ctx: lifetime, cancel: cancel, wake: make(chan struct{}), watchers: make(map[string]*directoryWatcher)}
	if err := m.Health(ctx); err != nil {
		_ = m.Close()
		return nil, err
	}
	return m, nil
}

func (m *notificationManager) signalLocked() {
	close(m.wake)
	m.wake = make(chan struct{})
	for _, w := range m.watchers {
		for _, q := range w.waiters {
			select {
			case q.wake <- struct{}{}:
			default:
			}
		}
	}
}

func (m *notificationManager) start(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrStopped
	}
	if m.stream != nil && m.failure == nil {
		m.mu.Unlock()
		return nil
	}
	old := m.stream
	resumeInc, resumePos := m.incarnation, m.position
	m.mu.Unlock()
	if old != nil {
		if err := old.Close(); err != nil {
			return err
		}
	}
	streamCtx, cancel := context.WithCancel(m.ctx)
	stop := context.AfterFunc(ctx, cancel)
	var stream ChangeStream
	var err error
	if old != nil {
		stream, err = m.source.Resume(streamCtx, resumeInc, resumePos)
	} else {
		stream, err = m.source.Subscribe(streamCtx)
	}
	if old != nil && errors.Is(err, syscall.ESTALE) {
		stream, err = m.source.Subscribe(streamCtx)
	}
	stopped := stop()
	if err != nil {
		cancel()
		return err
	}
	if stream == nil {
		cancel()
		return syscall.EIO
	}
	if !stopped || ctx.Err() != nil {
		_ = stream.Close()
		cancel()
		return ctx.Err()
	}
	inc, pos := stream.Incarnation(), stream.Position()
	if inc == "" || pos < 0 {
		_ = stream.Close()
		cancel()
		return syscall.EIO
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = stream.Close()
		cancel()
		return ErrStopped
	}
	m.stream = stream
	m.incarnation = inc
	m.position = pos
	m.failure = nil
	m.wg.Add(1)
	m.mu.Unlock()
	go func() { defer m.wg.Done(); defer cancel(); m.consume(stream) }()
	return nil
}

func (m *notificationManager) consume(stream ChangeStream) {
	for {
		change, err := stream.Next()
		if err == nil {
			if validationErr := metastore.ValidateNotification(change); validationErr != nil {
				err = errors.Join(syscall.EIO, validationErr)
			}
		}
		m.mu.Lock()
		if m.stream != stream || m.closed {
			m.mu.Unlock()
			return
		}
		if err == nil && change.Position <= m.position {
			err = syscall.EIO
		}
		if err != nil {
			m.failure = err
			for _, w := range m.watchers {
				failure := err
				if errors.Is(err, syscall.ESTALE) {
					failure = ErrNotifyRescan
				}
				w.fail(failure)
			}
			m.signalLocked()
			m.mu.Unlock()
			return
		}
		for _, w := range m.watchers {
			if w.removed || w.failure != nil || (w.ready && change.Position <= w.barrier) {
				continue
			}
			events, mapErr := mapNotifications(change, w.id, w.filter, w.recursive)
			if mapErr != nil {
				w.fail(mapErr)
				continue
			}
			if len(events) == 0 {
				continue
			}
			wireBytes := int64(len(wire.NotifyInformation(events)))
			charge := wireBytes + 96 + 32*int64(len(events))
			for _, event := range events {
				charge += int64(len(event.Name))
			}
			for _, image := range []*metastore.EventImage{change.Notification.Before, change.Notification.After} {
				if notificationImageRelevant(image, w.id, w.recursive) {
					charge += 64 + 64*int64(len(image.Location.Ancestors))
					for _, entry := range image.Location.Ancestors {
						charge += int64(len(entry.Name))
					}
				}
			}
			if w.events+len(events) > m.limits.MaxNotifyEvents || charge > m.limits.MaxNotifyBytes-w.bytes {
				w.fail(ErrNotifyRescan)
				continue
			}
			w.bytes += charge
			w.events += len(events)
			group := notifyGroup{position: change.Position, events: events, wireBytes: wireBytes, bytes: charge}
			for _, image := range []*metastore.EventImage{change.Notification.Before, change.Notification.After} {
				if notificationImageRelevant(image, w.id, w.recursive) {
					group.proofs = append(group.proofs, image.Location.Clone())
				}
			}
			w.groups = append(w.groups, group)
		}
		m.position = change.Position
		m.signalLocked()
		m.mu.Unlock()
	}
}

func (m *notificationManager) checkpoint(ctx context.Context) (metastore.LogBarrier, error) {
	barrier, err := m.source.Checkpoint(ctx)
	if err != nil {
		return barrier, err
	}
	for {
		m.mu.Lock()
		if m.closed {
			err = ErrStopped
		} else if m.failure != nil {
			err = m.failure
		} else if barrier.Incarnation == "" || barrier.Position < 0 || barrier.Incarnation != m.incarnation {
			err = syscall.ESTALE
		}
		done := m.position >= barrier.Position
		wake := m.wake
		m.mu.Unlock()
		if err != nil || done {
			return barrier, err
		}
		select {
		case <-ctx.Done():
			return barrier, ctx.Err()
		case <-wake:
		}
	}
}

func (m *notificationManager) Health(ctx context.Context) error {
	m.setup.Lock()
	defer m.setup.Unlock()
	if err := m.start(ctx); err != nil {
		return err
	}
	_, err := m.checkpoint(ctx)
	return err
}

func (m *notificationManager) Close() error {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	if m.closeDone && m.closeErr == nil {
		return nil
	}
	m.mu.Lock()
	m.closed = true
	m.cancel()
	stream := m.stream
	m.signalLocked()
	m.mu.Unlock()
	var err error
	if stream != nil {
		err = stream.Close()
	}
	m.wg.Wait()
	m.closeErr = err
	m.closeDone = true
	return err
}

func (m *notificationManager) remove(reference string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if w := m.watchers[reference]; w != nil {
		w.removed = true
		w.fail(ErrStopped)
		for _, q := range w.waiters {
			select {
			case q.wake <- struct{}{}:
			default:
			}
		}
		if !w.validating {
			delete(m.watchers, reference)
		}
	}
}

func (m *notificationManager) watchRegistered(ctx context.Context, key string, identity notificationIdentity, filter uint32, recursive bool, maxOutput uint32, registered func() error, validate func(context.Context, storage.EntryLocation) error) ([]wire.Notification, error) {
	if validate == nil || filter == 0 || filter & ^uint32(0xfff) != 0 || maxOutput == 0 {
		return nil, syscall.EINVAL
	}
	m.setup.Lock()
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		m.setup.Unlock()
		return nil, ErrStopped
	}
	w := m.watchers[key]
	if w != nil && w.removed {
		m.mu.Unlock()
		m.setup.Unlock()
		return nil, ErrStopped
	}
	if w == nil && len(m.watchers) >= m.limits.MaxOpens {
		m.mu.Unlock()
		m.setup.Unlock()
		return nil, syscall.ENOMEM
	}
	m.mu.Unlock()
	if w == nil {
		if !identity.Directory {
			m.setup.Unlock()
			return nil, syscall.ENOTDIR
		}
		if identity.ID == 0 || identity.ID > uint64(^uint64(0)>>1) {
			m.setup.Unlock()
			return nil, syscall.EIO
		}
		w = &directoryWatcher{id: int64(identity.ID), filter: filter, recursive: recursive}
		m.mu.Lock()
		if err := ctx.Err(); err != nil {
			m.mu.Unlock()
			m.setup.Unlock()
			return nil, err
		}
		if m.closed {
			m.mu.Unlock()
			m.setup.Unlock()
			return nil, ErrStopped
		}
		m.watchers[key] = w
		m.mu.Unlock()
	}
	m.mu.Lock()
	if len(w.waiters) >= m.limits.MaxRequests {
		m.mu.Unlock()
		m.setup.Unlock()
		return nil, syscall.ENOMEM
	}
	waiter := &notifyWaiter{wake: make(chan struct{}, 1)}
	w.waiters = append(w.waiters, waiter)
	initialize := !w.ready && w.failure == nil && !w.validating
	m.mu.Unlock()
	if initialize {
		err := m.start(ctx)
		var barrier metastore.LogBarrier
		if err == nil {
			barrier, err = m.checkpoint(ctx)
		}
		m.mu.Lock()
		if err != nil {
			w.failure = err
		} else if w.failure == nil {
			w.barrier = barrier.Position
			w.ready = true
			n := 0
			for _, g := range w.groups {
				if g.position <= barrier.Position {
					w.bytes -= g.bytes
					w.events -= len(g.events)
				} else {
					w.groups[n] = g
					n++
				}
			}
			clear(w.groups[n:])
			w.groups = w.groups[:n]
		}
		m.signalLocked()
		m.mu.Unlock()
	}
	m.setup.Unlock()
	defer func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for i, q := range w.waiters {
			if q == waiter {
				copy(w.waiters[i:], w.waiters[i+1:])
				w.waiters[len(w.waiters)-1] = nil
				w.waiters = w.waiters[:len(w.waiters)-1]
				break
			}
		}
		m.signalLocked()
	}()
	if registered != nil {
		m.mu.Lock()
		ready := w.ready && w.failure == nil && waiter.failure == nil && !m.closed && m.watchers[key] == w
		m.mu.Unlock()
		if ready {
			if err := registered(); err != nil {
				return nil, err
			}
		}
	}
	for {
		m.mu.Lock()
		if m.closed || w.removed || m.watchers[key] != w {
			m.mu.Unlock()
			return nil, ErrStopped
		}
		if ctx.Err() != nil {
			m.mu.Unlock()
			return nil, ctx.Err()
		}
		if w.waiters[0] == waiter {
			if w.failure != nil {
				err := w.deliverFailure()
				m.mu.Unlock()
				return nil, err
			}
			if waiter.failure != nil {
				err := waiter.failure
				m.mu.Unlock()
				return nil, err
			}
			if len(w.groups) > 0 {
				var events []wire.Notification
				var outputBytes int64
				n := 0
				for _, g := range w.groups {
					candidateBytes := ((outputBytes + 3) &^ 3) + g.wireBytes
					if uint64(candidateBytes) > uint64(maxOutput) {
						break
					}
					events = append(events, g.events...)
					outputBytes = candidateBytes
					n++
				}
				if n == 0 {
					w.fail(ErrNotifyRescan)
					err := w.deliverFailure()
					m.mu.Unlock()
					return nil, err
				}
				generation := w.generation
				selected := w.groups[:n]
				w.validating = true
				m.mu.Unlock()
				var validationErr error
				for _, group := range selected {
					for _, proof := range group.proofs {
						if validationErr = validate(ctx, proof); validationErr != nil {
							break
						}
					}
					if validationErr != nil {
						break
					}
				}
				selected = nil
				m.mu.Lock()
				w.validating = false
				if m.closed || w.removed || m.watchers[key] != w {
					w.fail(ErrStopped)
					if m.watchers[key] == w && w.removed {
						delete(m.watchers, key)
					}
				} else if w.generation != generation && w.failure == nil {
					w.fail(ErrNotifyRescan)
				} else if w.failure == nil {
					if ctx.Err() != nil {
						m.mu.Unlock()
						return nil, ctx.Err()
					} else if validationErr != nil {
						w.fail(validationErr)
					}
				}
				if w.failure != nil {
					err := w.deliverFailure()
					m.mu.Unlock()
					return nil, err
				}
				for _, g := range w.groups[:n] {
					w.bytes -= g.bytes
					w.events -= len(g.events)
				}
				copy(w.groups, w.groups[n:])
				clear(w.groups[len(w.groups)-n:])
				w.groups = w.groups[:len(w.groups)-n]
				m.mu.Unlock()
				return events, nil
			}
		}
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-waiter.wake:
		}
	}
}

func mapNotifications(change metastore.Change, id int64, filter uint32, recursive bool) ([]wire.Notification, error) {
	if err := metastore.ValidateNotification(change); err != nil {
		return nil, fmt.Errorf("invalid directory notification: %w", errors.Join(syscall.EIO, err))
	}
	n := change.Notification
	before, err := notificationPath(n.Before, id, recursive)
	if err != nil {
		return nil, err
	}
	after, err := notificationPath(n.After, id, recursive)
	if err != nil {
		return nil, err
	}
	if before == "" && after == "" {
		return nil, nil
	}
	var attr windowsAttr
	for _, image := range []*metastore.EventImage{n.Before, n.After} {
		if image == nil {
			continue
		}
		attr, err = projectWindowsAttr(storage.FileObservation{Attr: image.Attr, Location: &image.Location}, windowsMetadata{})
		if err != nil {
			return nil, fmt.Errorf("invalid notification metadata: %w", err)
		}
	}
	nameFilter := uint32(1)
	if attr.DOSAttributes&dosDirectory != 0 {
		nameFilter = 2
	}
	if n.ChangeMask&metastore.ChangeName != 0 {
		if filter&nameFilter == 0 {
			return nil, nil
		}
		switch {
		case before != "" && after != "":
			return []wire.Notification{{Action: 4, Name: before}, {Action: 5, Name: after}}, nil
		case before != "":
			return []wire.Notification{{Action: 2, Name: before}}, nil
		case after != "":
			return []wire.Notification{{Action: 1, Name: after}}, nil
		}
		return nil, nil
	}
	var categories uint32
	if n.ChangeMask&metastore.ChangeAttributes != 0 {
		categories |= 4
	}
	if n.ChangeMask&metastore.ChangeSize != 0 {
		categories |= 8
	}
	if n.ChangeMask&(metastore.ChangeModTime|metastore.ChangeContent) != 0 {
		categories |= 16
	}
	if n.ChangeMask&metastore.ChangeAccessTime != 0 {
		categories |= 32
	}
	if n.ChangeMask&metastore.ChangeCreationTime != 0 {
		categories |= 64
	}
	if after != "" && categories&filter != 0 {
		return []wire.Notification{{Action: 3, Name: after}}, nil
	}
	return nil, nil
}

func notificationPath(image *metastore.EventImage, id int64, recursive bool) (string, error) {
	if image == nil || image.Location.State != storage.LocationLinked {
		return "", nil
	}
	entries := image.Location.Ancestors
	found := -1
	for i, entry := range entries {
		if entry.ParentID == uint64(id) {
			found = i
			break
		}
	}
	if found < 0 || (!recursive && found != len(entries)-1) {
		return "", nil
	}
	parts := make([]string, len(entries))
	for i, entry := range entries {
		if _, err := nameKey(entry.Name); err != nil {
			return "", fmt.Errorf("unrepresentable notification name: %w", errors.Join(syscall.EIO, err))
		}
		parts[i] = string(entry.Name)
	}
	if err := validateWindowsPath(strings.Join(parts, "/")); err != nil {
		return "", fmt.Errorf("unrepresentable notification path: %w", errors.Join(syscall.EIO, err))
	}
	return strings.Join(parts[found:], "\\"), nil
}
