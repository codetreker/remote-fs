package smb

import (
	"context"
	"errors"
	"io/fs"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

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

type notifyTestFile struct{ storage.WindowsFile }

func (notifyTestFile) Stat(context.Context) (storage.WindowsAttr, error) {
	return storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 1, Mode: fs.ModeDir}}}, nil
}
func namedChange(pos metastore.Position, kind metastore.ChangeKind, name string, mode fs.FileMode) metastore.Change {
	loc := &metastore.LocationFacts{Ancestors: []metastore.DirectoryAncestor{{DirectoryID: 1}}, LeafName: []byte(name)}
	c := metastore.Change{Position: pos, Kind: kind, Parent: 1, Name: []byte(name), Node: &metastore.Node{ID: 2, Mode: mode}, Notification: &metastore.Notification{SubjectID: 2, SubjectKind: mode.Type(), Directory: mode.IsDir(), ChangeMask: metastore.ChangeName}}
	switch kind {
	case metastore.Created:
		c.Notification.After = loc
	case metastore.Removed:
		c.Node = nil
		c.Notification.Before = loc
	case metastore.Modified:
		c.Notification.Before = loc
		c.Notification.After = loc
		c.Notification.ChangeMask = metastore.ChangeSize
	}
	return c
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
		{"removed directory", namedChange(1, metastore.Removed, "x", fs.ModeDir), 1, 2, false, []wire.Notification{{Action: 2, Name: "x"}}},
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
	c.Notification.Before = &metastore.LocationFacts{Ancestors: []metastore.DirectoryAncestor{{DirectoryID: 1}, {DirectoryID: 3, Name: []byte("past")}}, LeafName: []byte("old")}
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
	c := namedChange(1, metastore.Removed, "link", fs.ModeSymlink)
	c.Notification.Directory = true
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
		events, err := m.watchRegistered(t.Context(), "open", notificationIdentity{ID: 1, Directory: true}, 1, false, 4096, func() error { close(registered); return nil })
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
	c.Notification.Before = &metastore.LocationFacts{Ancestors: []metastore.DirectoryAncestor{{DirectoryID: 1}}, LeafName: []byte("old")}
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
