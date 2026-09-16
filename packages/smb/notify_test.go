package smb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func notificationTestProof(_ context.Context, location storage.EntryLocation) error {
	return location.Check()
}

func (m *notificationManager) watch(ctx context.Context, key string, identity notificationIdentity, filter uint32, recursive bool, maxOutput uint32) ([]wire.Notification, error) {
	return m.watchRegistered(ctx, key, identity, filter, recursive, maxOutput, nil, notificationTestProof)
}

type notifyTestStream struct {
	changes  chan metastore.Change
	failures chan error
	done     chan struct{}
	once     sync.Once
	at       metastore.Position
}

func (s *notifyTestStream) Next() (metastore.Change, error) {
	select {
	case c := <-s.changes:
		return c, nil
	case err := <-s.failures:
		return metastore.Change{}, err
	case <-s.done:
		return metastore.Change{}, context.Canceled
	}
}
func (s *notifyTestStream) Close() error                       { s.once.Do(func() { close(s.done) }); return nil }
func (s *notifyTestStream) Incarnation() metastore.Incarnation { return "history" }
func (s *notifyTestStream) Position() metastore.Position       { return s.at }
func testNotifyStream() *notifyTestStream {
	return &notifyTestStream{changes: make(chan metastore.Change, 32), failures: make(chan error, 1), done: make(chan struct{})}
}
func testNotifySource(s *notifyTestStream) ChangeSource {
	return ChangeSource{Subscribe: func(context.Context) (ChangeStream, error) { return s, nil }, Resume: func(context.Context, metastore.Incarnation, metastore.Position) (ChangeStream, error) {
		return nil, syscall.ESTALE
	}, Checkpoint: func(context.Context) (metastore.LogBarrier, error) {
		return metastore.LogBarrier{Incarnation: "history"}, nil
	}}
}

type notifyTestFile struct{ windowsFile }

func (notifyTestFile) Stat(context.Context) (windowsAttr, error) {
	return windowsAttr{windowsBasicAttr: windowsBasicAttr{Attr: storage.Attr{ID: 1, Kind: storage.NodeDirectory}}}, nil
}
func namedChange(pos metastore.Position, kind metastore.ChangeKind, name string, nodeKind storage.NodeKind) metastore.Change {
	if nodeKind == 0 {
		nodeKind = storage.NodeRegular
	}
	node := metastore.Node{ID: 2, Kind: nodeKind, MetadataRevision: 1}
	if nodeKind == storage.NodeDirectory {
		node.DirectoryRevision = 1
	}
	location := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 2, Ancestors: []storage.EntryCondition{
		{ParentID: 1, NodeID: 2, EntryID: 2, DirectoryRevision: 1, Name: []byte(name)},
	}}
	image := func() *metastore.EventImage {
		return &metastore.EventImage{Attr: node.Attr(), Location: location.Clone()}
	}
	c := metastore.Change{Position: pos, Kind: kind, Parent: 1, Name: []byte(name), Node: &node, Notification: &metastore.Notification{SubjectID: 2, SubjectKind: nodeKind, ChangeMask: metastore.ChangeName}}
	switch kind {
	case metastore.Created:
		c.Notification.After = image()
	case metastore.Removed:
		c.Node = nil
		c.Notification.Before = image()
	case metastore.Modified:
		c.Notification.Before = image()
		node.Size = 1
		node.MetadataRevision++
		c.Notification.After = image()
		c.Notification.ChangeMask = metastore.ChangeSize
	}
	return c
}
func renamedImage(c metastore.Change, entries ...storage.EntryCondition) *metastore.EventImage {
	return &metastore.EventImage{Attr: c.Node.Attr(), Location: storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: uint64(c.Node.ID), Ancestors: entries}}
}
func waitNotify(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for !condition() {
		select {
		case <-deadline.C:
			t.Fatal("notification observer did not reach expected state")
		case <-time.After(time.Millisecond):
		}
	}
}
func TestNotificationMapping(t *testing.T) {
	cases := []struct {
		name   string
		c      metastore.Change
		id     int64
		filter uint32
		tree   bool
		want   []wire.Notification
	}{
		{"file deletion", namedChange(1, metastore.Removed, "x", 0), 1, 1, false, []wire.Notification{{Action: 2, Name: "x"}}},
		{"directory filter", namedChange(1, metastore.Removed, "x", 0), 1, 2, false, nil},
		{"removed directory", namedChange(1, metastore.Removed, "x", storage.NodeDirectory), 1, 2, false, []wire.Notification{{Action: 2, Name: "x"}}},
		{"size", namedChange(1, metastore.Modified, "x", 0), 1, 8, false, []wire.Notification{{Action: 3, Name: "x"}}},
		{"size does not change attributes", namedChange(1, metastore.Modified, "x", 0), 1, 4, false, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mapNotifications(tc.c, tc.id, tc.filter, tc.tree)
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v %v; want %+v", got, err, tc.want)
			}
		})
	}
	c := namedChange(1, metastore.Created, "new", 0)
	c.Kind = metastore.Renamed
	c.From = &metastore.Location{Parent: 3, Name: []byte("old")}
	c.Notification.Before = renamedImage(c, storage.EntryCondition{ParentID: 1, NodeID: 3, EntryID: 3, DirectoryRevision: 1, Name: []byte("past")}, storage.EntryCondition{ParentID: 3, NodeID: 2, EntryID: 2, DirectoryRevision: 1, Name: []byte("old")})
	for _, tc := range []struct {
		id   int64
		tree bool
		want []wire.Notification
	}{{1, true, []wire.Notification{{Action: 4, Name: "past\\old"}, {Action: 5, Name: "new"}}}, {1, false, []wire.Notification{{Action: 1, Name: "new"}}}, {3, false, []wire.Notification{{Action: 2, Name: "old"}}}, {2, true, nil}} {
		got, err := mapNotifications(c, tc.id, 1, tc.tree)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("rename got %+v %v want %+v", got, err, tc.want)
		}
	}
	c.Notification = nil
	if _, err := mapNotifications(c, 1, 1, true); !errors.Is(err, syscall.EIO) {
		t.Fatalf("corrupt facts: %v", err)
	}
	c = namedChange(1, metastore.Created, string([]byte{0xff}), 0)
	if _, err := mapNotifications(c, 1, 1, false); !errors.Is(err, syscall.EIO) {
		t.Fatalf("invalid UTF8: %v", err)
	}
}

func TestNotificationDirectorySymlinkUsesCapturedDirectoryHint(t *testing.T) {
	c := namedChange(1, metastore.Removed, "link", storage.NodeSymlink)
	payload, err := encodeWindowsMetadata(windowsMetadata{DirectorySymlink: true})
	if err != nil {
		t.Fatal(err)
	}
	c.Notification.Before.Attr.Metadata = storage.Metadata{{Key: windowsMetadataKey, Version: windowsMetadataVersion, Data: payload}}
	files, err := mapNotifications(c, 1, 1, false)
	if err != nil || len(files) != 0 {
		t.Fatalf("file filter = %v %v", files, err)
	}
	directories, err := mapNotifications(c, 1, 2, false)
	if err != nil || len(directories) != 1 || directories[0].Action != 2 || directories[0].Name != "link" {
		t.Fatalf("directory filter = %v %v", directories, err)
	}
}

func TestNotificationCreationTimeHasItsOwnFilter(t *testing.T) {
	c := namedChange(1, metastore.Modified, "file", 0)
	c.Notification.Before.Attr.Size = c.Node.Size
	created := time.Unix(100, 0)
	c.Node.CreationTime = &created
	c.Notification.After.Attr.CreationTime = &created
	c.Notification.ChangeMask = metastore.ChangeCreationTime
	events, err := mapNotifications(c, 1, 64, false)
	if err != nil || len(events) != 1 || events[0].Action != 3 {
		t.Fatalf("creation filter %v %v", events, err)
	}
	events, err = mapNotifications(c, 1, 16, false)
	if err != nil || len(events) != 0 {
		t.Fatalf("last-write filter %v %v", events, err)
	}
}

func TestNotificationRejectsUnrepresentableNames(t *testing.T) {
	for _, name := range []string{"CON", "bad.", "bad ", "a\\b", "a:b", strings.Repeat("😀", 128), string([]byte{0xff})} {
		t.Run(name, func(t *testing.T) {
			c := namedChange(1, metastore.Created, name, storage.NodeRegular)
			if _, err := mapNotifications(c, 1, 1, false); !errors.Is(err, syscall.EIO) {
				t.Fatalf("unrepresentable name: %v", err)
			}
		})
	}
	c := namedChange(1, metastore.Created, "file", storage.NodeRegular)
	c.Parent = 3
	c.Notification.After = renamedImage(c,
		storage.EntryCondition{ParentID: 1, NodeID: 3, EntryID: 3, DirectoryRevision: 1, Name: []byte("CON")},
		storage.EntryCondition{ParentID: 3, NodeID: 2, EntryID: 2, DirectoryRevision: 1, Name: []byte("file")})
	if _, err := mapNotifications(c, 3, 1, false); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unrepresentable ancestor: %v", err)
	}
	if got, err := mapNotifications(c, 4, 1, true); err != nil || len(got) != 0 {
		t.Fatalf("unrelated watcher: %v %v", got, err)
	}
}

func TestNotificationMetadataUsesOnlyCapturedImages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint32
		payload []byte
		want    error
	}{
		{"corrupt payload", windowsMetadataVersion, []byte{1}, syscall.EIO},
		{"unknown version", windowsMetadataVersion + 1, make([]byte, 8), syscall.EOPNOTSUPP},
		{"directory hint on regular file", windowsMetadataVersion, []byte{0, 0, 0, 0, 1, 0, 0, 0}, syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, kind := range []metastore.ChangeKind{metastore.Created, metastore.Removed, metastore.Modified} {
				c := namedChange(1, kind, "file", storage.NodeRegular)
				metadata := storage.Metadata{{Key: windowsMetadataKey, Version: tc.version, Data: tc.payload}}
				if c.Notification.Before != nil {
					c.Notification.Before.Attr.Metadata = metadata.Clone()
				}
				if c.Notification.After != nil {
					c.Notification.After.Attr.Metadata = metadata.Clone()
					c.Node.Metadata = metadata.Clone()
				}
				if _, err := mapNotifications(c, 1, 0xfff, false); !errors.Is(err, tc.want) {
					t.Fatalf("kind %v: %v", kind, err)
				}
			}
		})
	}
	c := namedChange(1, metastore.Removed, "link", storage.NodeSymlink)
	c.Notification.Before.Attr.Metadata = storage.Metadata{{Key: "foreign", Version: 19, Data: []byte{0xff}}}
	events, err := mapNotifications(c, 1, 1, false)
	if err != nil || !reflect.DeepEqual(events, []wire.Notification{{Action: 2, Name: "link"}}) {
		t.Fatalf("absent Windows metadata with foreign payload: %v %v", events, err)
	}
}

func TestNotificationRootAndDetachedModificationHaveNoName(t *testing.T) {
	for _, state := range []storage.LocationState{storage.LocationRoot, storage.LocationDetached} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			c := namedChange(1, metastore.Modified, "file", storage.NodeRegular)
			c.Parent, c.Name = 0, nil
			root := uint64(1)
			if state == storage.LocationRoot {
				root = 2
			}
			location := storage.EntryLocation{State: state, RootNodeID: root, NodeID: 2}
			c.Notification.Before.Location = location
			c.Notification.After.Location = location
			if events, err := mapNotifications(c, int64(root), 0xfff, true); err != nil || len(events) != 0 {
				t.Fatalf("unnamed modification: %v %v", events, err)
			}
		})
	}
}

func TestNotificationCheckpointRetainsLaterEvents(t *testing.T) {
	s := testNotifyStream()
	source := testNotifySource(s)
	entered, release := make(chan struct{}), make(chan struct{})
	registered := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	source.Checkpoint = func(ctx context.Context) (metastore.LogBarrier, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return metastore.LogBarrier{}, ctx.Err()
			}
			return metastore.LogBarrier{Incarnation: "history", Position: 1}, nil
		}
		return metastore.LogBarrier{Incarnation: "history"}, nil
	}
	m, err := newNotificationManager(t.Context(), source, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	type result struct {
		events []wire.Notification
		err    error
	}
	out := make(chan result, 1)
	go func() {
		events, err := m.watchRegistered(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, false, 4096, func() error { close(registered); return nil }, notificationTestProof)
		out <- result{events, err}
	}()
	<-entered
	select {
	case <-registered:
		t.Fatal("watch acknowledged before checkpoint")
	default:
	}
	s.changes <- namedChange(1, metastore.Created, "before", 0)
	s.changes <- namedChange(2, metastore.Created, "after", 0)
	waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.position == 2 })
	close(release)
	got := <-out
	select {
	case <-registered:
	default:
		t.Fatal("watch did not acknowledge registration")
	}
	if got.err != nil || !reflect.DeepEqual(got.events, []wire.Notification{{Action: 1, Name: "after"}}) {
		t.Fatalf("checkpoint: %+v", got)
	}
	s.changes <- namedChange(3, metastore.Created, "between", 0)
	waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.position == 3 })
	// Later filter arguments do not replace the first request's fixed filter.
	events, err := m.watch(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 2, true, 4096)
	if err != nil || len(events) != 1 || events[0].Name != "between" {
		t.Fatalf("retained queue: %+v %v", events, err)
	}
}

func TestNotificationOverflowIsAtomicAndRecovers(t *testing.T) {
	s := testNotifyStream()
	limits := DefaultLimits()
	limits.MaxNotifyEvents = 1
	m, err := newNotificationManager(t.Context(), testNotifySource(s), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() {
		_, err := m.watch(ctx, "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096)
		first <- err
	}()
	waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); w := m.watchers["open"]; return w != nil && w.ready })
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c := namedChange(1, metastore.Created, "new", 0)
	c.Kind = metastore.Renamed
	c.From = &metastore.Location{Parent: 1, Name: []byte("old")}
	c.Notification.Before = renamedImage(c, storage.EntryCondition{ParentID: 1, NodeID: 2, EntryID: 2, DirectoryRevision: 1, Name: []byte("old")})
	s.changes <- c
	waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.position == 1 })
	if events, err := m.watch(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096); !errors.Is(err, ErrNotifyRescan) || len(events) != 0 {
		t.Fatalf("partial rename: %+v %v", events, err)
	}
	out := make(chan error, 1)
	go func() {
		events, err := m.watch(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096)
		if err == nil && (len(events) != 1 || events[0].Name != "recovered") {
			err = errors.New("unexpected recovery result")
		}
		out <- err
	}()
	waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.watchers["open"].ready })
	s.changes <- namedChange(2, metastore.Created, "recovered", 0)
	if err := <-out; err != nil {
		t.Fatal(err)
	}
}

func TestNotificationFIFOAndClose(t *testing.T) {
	s := testNotifyStream()
	m, err := newNotificationManager(t.Context(), testNotifySource(s), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	out := []chan string{make(chan string, 1), make(chan string, 1)}
	for i := range out {
		go func() {
			events, err := m.watch(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, false, 16)
			if err != nil {
				out[i] <- err.Error()
			} else {
				out[i] <- events[0].Name
			}
		}()
		waitNotify(t, func() bool {
			m.mu.Lock()
			defer m.mu.Unlock()
			w := m.watchers["open"]
			return w != nil && len(w.waiters) == i+1 && w.ready
		})
	}
	s.changes <- namedChange(1, metastore.Created, "a", 0)
	s.changes <- namedChange(2, metastore.Created, "b", 0)
	if got := <-out[0]; got != "a" {
		t.Fatalf("first: %s", got)
	}
	if got := <-out[1]; got != "b" {
		t.Fatalf("second: %s", got)
	}
	closed := make(chan error, 1)
	go func() {
		_, err := m.watch(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, false, 16)
		closed <- err
	}()
	waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return len(m.watchers["open"].waiters) == 1 })
	m.remove("open")
	if err := <-closed; !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
}

func TestNotificationCorruptionAndStreamRecovery(t *testing.T) {
	for _, failure := range []error{syscall.ESTALE, syscall.EIO} {
		t.Run(failure.Error(), func(t *testing.T) {
			old, replacement := testNotifyStream(), testNotifyStream()
			source := testNotifySource(old)
			calls := 0
			source.Subscribe = func(context.Context) (ChangeStream, error) {
				calls++
				if calls == 1 {
					return old, nil
				}
				return replacement, nil
			}
			m, err := newNotificationManager(t.Context(), source, DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			result := make(chan error, 1)
			go func() {
				_, err := m.watch(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, false, 4096)
				result <- err
			}()
			waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); w := m.watchers["open"]; return w != nil && w.ready })
			old.failures <- failure
			err = <-result
			if errors.Is(failure, syscall.ESTALE) {
				if !errors.Is(err, ErrNotifyRescan) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			if err := m.Health(t.Context()); err != nil {
				t.Fatal(err)
			}
			go func() {
				events, err := m.watch(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, false, 4096)
				if err == nil && (len(events) != 1 || events[0].Name != "new") {
					err = errors.New("unexpected recovered notification")
				}
				result <- err
			}()
			waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.watchers["open"].ready })
			replacement.changes <- namedChange(1, metastore.Created, "new", 0)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNotificationHealthBoundsAndCancellation(t *testing.T) {
	if _, err := newNotificationManager(t.Context(), ChangeSource{}, DefaultLimits()); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	s := testNotifyStream()
	source := testNotifySource(s)
	source.Checkpoint = func(context.Context) (metastore.LogBarrier, error) {
		return metastore.LogBarrier{Incarnation: "history", Position: 2}, nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := newNotificationManager(ctx, source, DefaultLimits()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-s.done:
	default:
		t.Fatal("failed setup retained stream")
	}
}

func TestNotificationRejectsMalformedRecordsAndSmallOutput(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "small output", true: "missing facts"}[malformed], func(t *testing.T) {
			s := testNotifyStream()
			m, err := newNotificationManager(t.Context(), testNotifySource(s), DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			out := make(chan error, 1)
			go func() {
				_, err := m.watch(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, false, 1)
				out <- err
			}()
			waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); w := m.watchers["open"]; return w != nil && w.ready })
			c := namedChange(1, metastore.Created, "x", 0)
			if malformed {
				c.Notification = nil
			}
			s.changes <- c
			err = <-out
			if malformed && !errors.Is(err, syscall.EIO) {
				t.Fatalf("missing facts: %v", err)
			}
			if !malformed && !errors.Is(err, ErrNotifyRescan) {
				t.Fatalf("small output: %v", err)
			}
		})
	}
}

func TestNotificationAdmissionErrors(t *testing.T) {
	s := testNotifyStream()
	limits := DefaultLimits()
	limits.MaxOpens = 1
	limits.MaxRequests = 1
	m, err := newNotificationManager(t.Context(), testNotifySource(s), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, filter := range []uint32{0, 0x1000} {
		if _, err := m.watch(t.Context(), "a", notificationIdentity{ID: 1, Directory: true}, filter, false, 16); !errors.Is(err, syscall.EINVAL) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	out := make(chan error, 1)
	go func() {
		_, err := m.watch(ctx, "a", notificationIdentity{ID: 1, Directory: true}, 1, false, 16)
		out <- err
	}()
	waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); w := m.watchers["a"]; return w != nil && w.ready })
	for _, key := range []string{"a", "b"} {
		if _, err := m.watch(t.Context(), key, notificationIdentity{ID: 1, Directory: true}, 1, false, 16); !errors.Is(err, syscall.ENOMEM) {
			t.Fatalf("limit %s: %v", key, err)
		}
	}
	cancel()
	if err := <-out; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Health(t.Context()); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if _, err := m.watch(t.Context(), "a", notificationIdentity{ID: 1, Directory: true}, 1, false, 16); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
}

func queuedNotificationManager(t *testing.T, limits Limits, changes ...metastore.Change) (*notificationManager, *notifyTestStream) {
	t.Helper()
	stream := testNotifyStream()
	m, err := newNotificationManager(t.Context(), testNotifySource(stream), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err = m.watchRegistered(ctx, "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096, func() error { cancel(); return nil }, notificationTestProof)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("initialize queued watch: %v", err)
	}
	for _, change := range changes {
		stream.changes <- change
		waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.position == change.Position })
	}
	return m, stream
}

func TestNotificationValidatesOwnedHistoricalProofs(t *testing.T) {
	change := namedChange(1, metastore.Created, "Foo", storage.NodeRegular)
	m, _ := queuedNotificationManager(t, DefaultLimits(), change)
	change.Notification.After.Location.Ancestors[0].Name[0] = 'X'
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "request principal")
	calls := 0
	events, err := m.watchRegistered(ctx, "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 20, nil, func(gotCtx context.Context, proof storage.EntryLocation) error {
		calls++
		if gotCtx.Value(contextKey{}) != "request principal" || len(proof.Ancestors) != 1 || string(proof.Ancestors[0].Name) != "Foo" || proof.Ancestors[0].DirectoryRevision != 1 {
			t.Fatalf("request context or owned event proof lost: %+v", proof)
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		w := m.watchers["open"]
		if !w.validating || w.bytes <= 20 || w.groups[0].wireBytes != 18 {
			t.Fatalf("proof not charged independently of wire size: %+v", w)
		}
		return nil
	})
	if err != nil || calls != 1 || !reflect.DeepEqual(events, []wire.Notification{{Action: 1, Name: "Foo"}}) {
		t.Fatalf("validated delivery: %v calls=%d error=%v", events, calls, err)
	}
}

func TestNotificationProofCapacityRejectsBeforeDelivery(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxNotifyBytes = 20
	m, _ := queuedNotificationManager(t, limits, namedChange(1, metastore.Created, "Foo", storage.NodeRegular))
	called := false
	events, err := m.watchRegistered(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096, nil, func(context.Context, storage.EntryLocation) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrNotifyRescan) || len(events) != 0 || called {
		t.Fatalf("proof exceeds retained-byte budget: %v %v called=%v", events, err, called)
	}
}

func TestNotificationProofFailurePreservesErrorAndRenameAtomicity(t *testing.T) {
	for _, failure := range []error{ErrNotifyRescan, syscall.EACCES, syscall.EIO, context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			change := namedChange(1, metastore.Created, "new", storage.NodeRegular)
			change.Kind = metastore.Renamed
			change.From = &metastore.Location{Parent: 1, Name: []byte("old")}
			change.Notification.Before = renamedImage(change, storage.EntryCondition{ParentID: 1, NodeID: 2, EntryID: 2, DirectoryRevision: 1, Name: []byte("old")})
			m, _ := queuedNotificationManager(t, DefaultLimits(), change)
			calls := 0
			events, err := m.watchRegistered(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096, nil, func(_ context.Context, proof storage.EntryLocation) error {
				calls++
				if calls == 1 {
					if string(proof.Ancestors[0].Name) != "old" {
						t.Fatalf("first proof: %+v", proof)
					}
					return nil
				}
				return failure
			})
			if !errors.Is(err, failure) || len(events) != 0 || calls != 2 {
				t.Fatalf("partially delivered rename or changed error: %v %v calls=%d", events, err, calls)
			}
			m.mu.Lock()
			defer m.mu.Unlock()
			w := m.watchers["open"]
			if w.ready || w.bytes != 0 || len(w.groups) != 0 {
				t.Fatalf("failed proof kept continuity: %+v", w)
			}
		})
	}
}

func TestNotificationValidationOverflowRetainsChargeAndFailsAllWaiters(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxNotifyEvents = 1
	m, stream := queuedNotificationManager(t, limits, namedChange(1, metastore.Created, "Foo", storage.NodeRegular))
	entered, release := make(chan struct{}), make(chan struct{})
	type result struct {
		events []wire.Notification
		err    error
	}
	first, second := make(chan result, 1), make(chan result, 1)
	go func() {
		events, err := m.watchRegistered(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096, nil, func(context.Context, storage.EntryLocation) error {
			close(entered)
			<-release
			return nil
		})
		first <- result{events, err}
	}()
	<-entered
	m.mu.Lock()
	charge := m.watchers["open"].bytes
	m.mu.Unlock()
	go func() {
		events, err := m.watch(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096)
		second <- result{events, err}
	}()
	waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return len(m.watchers["open"].waiters) == 2 })
	stream.changes <- namedChange(2, metastore.Created, "foo", storage.NodeRegular)
	waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.position == 2 })
	m.mu.Lock()
	w := m.watchers["open"]
	charged := w.bytes == charge && charge > 0 && len(w.groups) == 1 && w.validating && errors.Is(w.failure, ErrNotifyRescan)
	m.mu.Unlock()
	close(release)
	if !charged {
		t.Error("overflow refunded or replaced the validator's retained proofs")
	}
	for _, out := range []chan result{first, second} {
		got := <-out
		if !errors.Is(got.err, ErrNotifyRescan) || len(got.events) != 0 {
			t.Fatalf("overflow delivered a partial or stale group: %+v", got)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.watchers["open"].bytes != 0 || len(m.watchers["open"].groups) != 0 {
		t.Fatal("validator completion retained failed groups")
	}
}

func TestNotificationValidationCancellationAndRemoval(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprint(remove), func(t *testing.T) {
			m, _ := queuedNotificationManager(t, DefaultLimits(), namedChange(1, metastore.Created, "Foo", storage.NodeRegular))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			out := make(chan error, 1)
			go func() {
				events, err := m.watchRegistered(ctx, "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096, nil, func(validationCtx context.Context, _ storage.EntryLocation) error {
					close(entered)
					if remove {
						<-release
						return nil
					}
					<-validationCtx.Done()
					return validationCtx.Err()
				})
				if len(events) != 0 {
					err = errors.New("cancelled validation delivered events")
				}
				out <- err
			}()
			<-entered
			want := error(context.Canceled)
			if remove {
				m.remove("open")
				if _, err := m.watch(ctx, "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096); !errors.Is(err, ErrStopped) {
					t.Fatalf("in-flight removal permitted replacement watch: %v", err)
				}
				m.mu.Lock()
				retained := m.watchers["open"] != nil && m.watchers["open"].bytes > 0
				m.mu.Unlock()
				if !retained {
					t.Error("removal refunded active validator")
				}
				close(release)
				want = ErrStopped
			} else {
				cancel()
			}
			if err := <-out; !errors.Is(err, want) {
				t.Fatalf("validation error=%v want=%v", err, want)
			}
			if remove {
				m.mu.Lock()
				defer m.mu.Unlock()
				if m.watchers["open"] != nil {
					t.Fatal("completed removal retained tombstone")
				}
			}
		})
	}
}

func TestNotificationRequiresProofValidator(t *testing.T) {
	m, _ := queuedNotificationManager(t, DefaultLimits())
	if _, err := m.watchRegistered(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096, nil, nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("missing validator admitted: %v", err)
	}
}

func TestNotificationGenericDirectoryProofRejectsCaseCollisionAndOldRevision(t *testing.T) {
	for _, collision := range []bool{false, true} {
		t.Run(fmt.Sprint(collision), func(t *testing.T) {
			session, _, root, parent, child := newClientTestSession(t)
			location := child.observation.Location.Clone()
			location.Ancestors[1].Name = []byte("Foo")
			parent.entries[0].Name = []byte("Foo")
			if collision {
				other := child.observation.Attr.Clone()
				other.ID = 4
				parent.entries = append(parent.entries, storage.DirectoryEntry{EntryID: 24, Name: []byte("foo"), Attr: other})
				page := storage.DirectoryPage{ParentID: 2, Revision: 41, Entries: parent.entries, Done: true}
				if err := page.Check(); err != nil {
					t.Fatalf("case-distinct generic entries must be valid: %v", err)
				}
			} else {
				parent.onList = func(request storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
					if request.Revision != 41 {
						t.Fatalf("historical revision replaced: %+v", request)
					}
					return storage.DirectoryPage{}, &storage.FileError{Code: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision}}
				}
			}
			attr := child.observation.Attr.Clone()
			node := &metastore.Node{ID: int64(attr.ID), Kind: attr.Kind, Size: attr.Size, MetadataRevision: attr.MetadataRevision}
			change := metastore.Change{Position: 1, Kind: metastore.Created, Parent: 2, Name: []byte("Foo"), Node: node, Notification: &metastore.Notification{
				SubjectID: int64(attr.ID), SubjectKind: attr.Kind, ChangeMask: metastore.ChangeName,
				After: &metastore.EventImage{Attr: attr, Location: location},
			}}
			m, _ := queuedNotificationManager(t, DefaultLimits(), change)
			file := &clientFile{session: session, raw: root, access: windowsReadData}
			events, err := m.watchRegistered(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096, nil, file.ValidateNotificationLocation)
			if !errors.Is(err, ErrNotifyRescan) || len(events) != 0 || len(parent.lists) == 0 {
				t.Fatalf("unproven complete name view delivered: %v %v pages=%d", events, err, len(parent.lists))
			}
		})
	}
}

func TestNotificationCancelledHeadPreservesGroupForNextWaiter(t *testing.T) {
	m, _ := queuedNotificationManager(t, DefaultLimits(), namedChange(1, metastore.Created, "Foo", storage.NodeRegular))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	firstEntered, secondEntered := make(chan struct{}), make(chan struct{})
	secondRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseSecond := func() { releaseOnce.Do(func() { close(secondRelease) }) }
	defer releaseSecond()
	type result struct {
		events []wire.Notification
		err    error
	}
	first, second := make(chan result, 1), make(chan result, 1)
	go func() {
		events, err := m.watchRegistered(ctx, "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096, nil, func(validationCtx context.Context, _ storage.EntryLocation) error {
			close(firstEntered)
			<-validationCtx.Done()
			return validationCtx.Err()
		})
		first <- result{events, err}
	}()
	<-firstEntered
	m.mu.Lock()
	charge := m.watchers["open"].bytes
	m.mu.Unlock()
	secondCalls := 0
	go func() {
		events, err := m.watchRegistered(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, true, 4096, nil, func(validationCtx context.Context, proof storage.EntryLocation) error {
			secondCalls++
			if validationCtx.Err() != nil || len(proof.Ancestors) != 1 || string(proof.Ancestors[0].Name) != "Foo" {
				return errors.New("next waiter lost its own context or original proof")
			}
			close(secondEntered)
			<-secondRelease
			return nil
		})
		second <- result{events, err}
	}()
	waitNotify(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return len(m.watchers["open"].waiters) == 2 })
	cancel()
	if got := <-first; !errors.Is(got.err, context.Canceled) || len(got.events) != 0 {
		t.Fatalf("cancelled head: %+v", got)
	}
	select {
	case <-secondEntered:
	case got := <-second:
		t.Fatalf("next waiter did not validate retained group: %+v", got)
	case <-time.After(2 * time.Second):
		t.Fatal("next waiter did not acquire notification delivery")
	}
	m.mu.Lock()
	w := m.watchers["open"]
	retained := charge > 0 && w.bytes == charge && w.events == 1 && len(w.groups) == 1 && w.ready && w.failure == nil && w.validating
	m.mu.Unlock()
	if !retained {
		t.Error("head cancellation changed continuity or refunded the undelivered group")
	}
	releaseSecond()
	got := <-second
	if got.err != nil || secondCalls != 1 || !reflect.DeepEqual(got.events, []wire.Notification{{Action: 1, Name: "Foo"}}) {
		t.Fatalf("next waiter delivery: %+v validations=%d", got, secondCalls)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	w = m.watchers["open"]
	if w.bytes != 0 || w.events != 0 || len(w.groups) != 0 || !w.ready || w.failure != nil {
		t.Fatalf("successful delivery did not consume exactly one group: %+v", w)
	}
}
