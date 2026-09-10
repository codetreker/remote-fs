package httprest_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/locked"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// fakeLog exposes controlled log retention and failures behind a real HTTP listener,
// client, and SQLite/object volume. Since preserves the distinction between caught
// up, resumable, and gone so that the tests can detect changes lost across the wire.
type fakeLog struct {
	mu sync.Mutex

	incarnation metastore.Incarnation

	// kept is what the log still holds, oldest first, and tail is the newest position
	// ever recorded whether or not it is still held. They are separate for the reason the
	// contract keeps them separate: a log that has discarded everything cannot otherwise
	// tell a caller that is caught up from one that has missed all of it.
	kept           []metastore.Change
	tail           metastore.Position
	trimmedThrough metastore.Position
	trimmedByAge   bool

	// sinceCalls counts the reads of the log, so that a test can assert that nothing
	// reads it on a schedule.
	sinceCalls   int
	sinceErr     error
	emptyBounded bool

	snapshotErr error
	closeErr    error
	barrierErr  error
	barrier     *metastore.LogBarrier
	pages       [][]metastore.Row

	// stall holds a snapshot part way through its delivery, so that abandoning one is
	// abandoning something genuinely in flight.
	stall bool

	// slowPage is how long the store takes over each page after the first, which is what a
	// large tree on a busy database looks like from the outside.
	slowPage time.Duration

	// open counts the snapshots that have been taken and not yet closed.
	open int
}

func newFakeLog() *fakeLog {
	return &fakeLog{incarnation: "the-log-under-test"}
}

// record appends a change and gives it the next position.
func (l *fakeLog) record(change metastore.Change) metastore.Position {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tail++
	change.Position = l.tail
	l.kept = append(l.kept, change)
	return l.tail
}

// created is one ordinary change, for the tests that care only that a change happened.
func created(name string) metastore.Change {
	return metastore.Change{
		Kind:   metastore.Created,
		Parent: 1,
		Name:   []byte(name),
		Node: &metastore.Node{
			ID:         42,
			Mode:       0o644,
			Size:       int64(len(name)),
			AccessTime: time.Unix(1755000000, 1),
			ModTime:    time.Unix(1755000001, 2),
		},
	}
}

// discard drops the n oldest changes, the way trimming a log does, recording the newest
// position it threw away and which dimension did it.
func (l *fakeLog) discard(n int, byAge bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.trimmedThrough = l.kept[n-1].Position
	l.kept = l.kept[n:]
	l.trimmedByAge = byAge
}

// failSince makes every later read of the log fail, the way a database that has gone away
// does part way through a stream.
func (l *fakeLog) failSince(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sinceErr = err
}

// reads reports how many times the log has been read.
func (l *fakeLog) reads() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sinceCalls
}

// opened reports how many snapshots are open right now.
func (l *fakeLog) opened() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.open
}

func (l *fakeLog) readSince(_ context.Context, after metastore.Position, limit int) ([]metastore.Change, metastore.Retention, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sinceCalls++
	if l.sinceErr != nil {
		return nil, metastore.Retention{}, l.sinceErr
	}
	retention := metastore.Retention{Tail: l.tail, TrimmedThrough: l.trimmedThrough, TrimmedByAge: l.trimmedByAge}
	if len(l.kept) > 0 {
		retention.Oldest = l.kept[0].Position
	}
	var changes []metastore.Change
	for _, change := range l.kept {
		if len(changes) == limit {
			break
		}
		if change.Position > after {
			changes = append(changes, change)
		}
	}
	return changes, retention, nil
}

func (l *fakeLog) Since(ctx context.Context, after metastore.Position, limit int, result *metastore.ChangeResult) (metastore.Retention, error) {
	changes, retention, err := l.readSince(ctx, after, limit)
	if err != nil {
		return metastore.Retention{}, result.Fail(err)
	}
	if l.emptyBounded && limit > 0 {
		return retention, nil
	}
	for _, change := range changes {
		meta := change
		name := meta.Name
		if name == nil {
			meta.Name = nil
		} else {
			meta.Name = []byte{}
		}
		var fromName []byte
		if meta.From != nil {
			from := *meta.From
			fromName = from.Name
			if fromName == nil {
				from.Name = nil
			} else {
				from.Name = []byte{}
			}
			meta.From = &from
		}
		var content metastore.Key
		if meta.Node != nil {
			node := *meta.Node
			content = node.Content
			node.Content = ""
			meta.Node = &node
		}
		reservation, fits, err := result.Reserve(meta, metastore.ChangePayloadLengths{
			Name: int64(len(name)), FromName: int64(len(fromName)), Content: int64(len(content)),
		})
		if err != nil {
			return metastore.Retention{}, err
		}
		if !fits {
			return retention, nil
		}
		if err := reservation.Commit(name, fromName, content); err != nil {
			return metastore.Retention{}, err
		}
	}
	return retention, nil
}

func (l *fakeLog) Incarnation(_ context.Context, maxBytes int64) (metastore.Incarnation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if int64(len(l.incarnation)) > maxBytes {
		return "", syscall.EFBIG
	}
	return l.incarnation, nil
}

func (l *fakeLog) Barrier(_ context.Context, maxBytes int64) (metastore.LogBarrier, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.barrierErr != nil {
		return metastore.LogBarrier{}, l.barrierErr
	}
	if l.barrier != nil {
		return *l.barrier, nil
	}
	if int64(len(l.incarnation)) > maxBytes {
		return metastore.LogBarrier{}, syscall.EFBIG
	}
	return metastore.LogBarrier{Incarnation: l.incarnation, Position: l.tail}, nil
}

func (l *fakeLog) CommittedPosition(context.Context) (metastore.Position, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tail, nil
}

func (l *fakeLog) Snapshot(context.Context) (metastore.Snap, metastore.Position, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.snapshotErr != nil {
		return nil, 0, l.snapshotErr
	}
	l.open++
	return &fakeSnap{log: l, pages: l.pages, stall: l.stall, slow: l.slowPage, closeErr: l.closeErr}, l.tail, nil
}

type fakeSnap struct {
	log      *fakeLog
	pages    [][]metastore.Row
	sent     int
	stall    bool
	slow     time.Duration
	closeErr error
	closed   bool
}

func (s *fakeSnap) readNext(ctx context.Context, _ int) ([]metastore.Row, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.sent == len(s.pages) {
		return nil, true, nil
	}
	if s.stall && s.sent > 0 {
		<-ctx.Done()
		return nil, false, ctx.Err()
	}
	if s.slow > 0 && s.sent > 0 {
		select {
		case <-time.After(s.slow):
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	page := s.pages[s.sent]
	s.sent++
	return page, s.sent == len(s.pages), nil
}

func (s *fakeSnap) Next(ctx context.Context, limit int, result *metastore.RowResult) (bool, error) {
	rows, done, err := s.readNext(ctx, limit)
	if err != nil {
		return false, result.Fail(err)
	}
	for _, row := range rows {
		meta := row
		name, content := meta.Name, meta.Node.Content
		if name == nil {
			meta.Name = nil
		} else {
			meta.Name = []byte{}
		}
		meta.Node.Content = ""
		reservation, fits, err := result.Reserve(meta, metastore.RowPayloadLengths{
			Name: int64(len(name)), Content: int64(len(content)),
		})
		if err != nil {
			return false, err
		}
		if !fits {
			return false, result.Fail(errors.New("the fake snapshot page exceeded its result bound"))
		}
		if err := reservation.Commit(name, content); err != nil {
			return false, err
		}
	}
	return done, nil
}

func (s *fakeSnap) Close() error {
	if s.closed {
		return errors.New("the snapshot was closed twice")
	}
	s.closed = true
	s.log.mu.Lock()
	defer s.log.mu.Unlock()
	s.log.open--
	return s.closeErr
}

// recording is a storage that appends to a log whatever changes it made, the way a store
// that owned both would. The transport learns that the volume moved from the mutating
// request it has just answered, so a test that drives a real request has to leave the log
// looking as a real one would.
type recording struct {
	storage.Storage
	log *fakeLog
}

func (r recording) LockService() locking.Service {
	return r.Storage.(locked.Backend).LockService()
}

func (r recording) CheckBounded() error {
	return r.Storage.(storage.BoundedStorage).CheckBounded()
}

func (r recording) ListBounded(ctx context.Context, path string, result *storage.ListResult) error {
	return r.Storage.(storage.BoundedStorage).ListBounded(ctx, path, result)
}

func (r recording) ReadBounded(ctx context.Context, path string, maxBytes int64) ([]byte, error) {
	return r.Storage.(storage.BoundedStorage).ReadBounded(ctx, path, maxBytes)
}

func (r recording) Mkdir(ctx context.Context, path string) error {
	if err := r.Storage.Mkdir(ctx, path); err != nil {
		return err
	}
	r.log.record(created(path))
	return nil
}

// serveLog serves a real volume with a separately controlled log so that stream
// failures can be injected independently of filesystem operations.
func serveLog(t *testing.T, log metastore.Log, limits httprest.Limits) *httprest.Storage {
	t.Helper()
	return serveLogWatchedFor(t, log, limits, httprest.DefaultSilence)
}

// serveLogWatchedFor is serveLog with the reading end's bound on a quiet stream given
// rather than defaulted, so that a case about that bound need not wait out the default.
func serveLogWatchedFor(t *testing.T, log metastore.Log, limits httprest.Limits, silence time.Duration) *httprest.Storage {
	t.Helper()
	backing := volumeFixture(t)
	var served storage.Storage = backing
	if fake, ok := log.(*fakeLog); ok {
		served = recording{Storage: backing, log: fake}
	}
	h, err := httprest.NewHandlerWithLimits(served, log, limits)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	s, err := httprest.DialWithSilence(srv.URL, srv.Client(), silence)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return s
}

func serveLogWithOptions(t *testing.T, log metastore.Log, handlerOptions httprest.HandlerOptions, dialOptions httprest.DialOptions) *httprest.Storage {
	t.Helper()
	backing := volumeFixture(t)
	var served storage.Storage = backing
	if fake, ok := log.(*fakeLog); ok {
		served = recording{Storage: backing, log: fake}
	}
	handler, err := httprest.NewHandlerWithOptions(served, log, handlerOptions)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	client, err := httprest.DialWithOptions(srv.URL, srv.Client(), dialOptions)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return client
}

// watch subscribes and arranges for the subscription to be closed before the server is,
// because a server waits for the requests still on it and a change stream is one of those.
func watch(t *testing.T, s *httprest.Storage, open func() (*httprest.Subscription, error)) *httprest.Subscription {
	t.Helper()
	sub, err := open()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { sub.Close() })
	return sub
}

func TestOversizedChangeFaultsBeforeAnyChangeFrameIsAccepted(t *testing.T) {
	log := newFakeLog()
	log.record(created(strings.Repeat("x", 1024)))
	handlerOptions := httprest.DefaultHandlerOptions()
	handlerOptions.MaxFrameBytes = 1024
	dialOptions := httprest.DefaultDialOptions()
	dialOptions.MaxFrameBytes = 1024
	s := serveLogWithOptions(t, log, handlerOptions, dialOptions)

	sub, err := s.Resubscribe(t.Context(), log.incarnation, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if change, err := sub.Next(); err == nil {
		t.Fatalf("oversized change returned %+v, %v; want a stream failure before a change", change, err)
	}
}

func TestOversizedSnapshotRowInvalidatesTheWholeProducedPage(t *testing.T) {
	log := newFakeLog()
	log.closeErr = errors.New("snapshot release also failed")
	log.pages = [][]metastore.Row{{
		{Node: metastore.Node{ID: 1, Mode: fs.ModeDir | 0o755}},
		{Parent: 1, Name: []byte(strings.Repeat("x", 1024)), Node: metastore.Node{ID: 2, Mode: 0o644}},
	}}
	handlerOptions := httprest.DefaultHandlerOptions()
	handlerOptions.MaxFrameBytes = 1024
	dialOptions := httprest.DefaultDialOptions()
	dialOptions.MaxFrameBytes = 1024
	s := serveLogWithOptions(t, log, handlerOptions, dialOptions)

	snap, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if rows, err := snap.Next(); err == nil || rows != nil {
		t.Fatalf("oversized snapshot page returned %+v, %v; want no accepted partial rows", rows, err)
	} else if !strings.Contains(err.Error(), log.closeErr.Error()) {
		t.Fatalf("snapshot fault %q dropped Close failure %q", err, log.closeErr)
	}
}

func TestClientAndServerFrameBoundsAreIndependentAndCompatible(t *testing.T) {
	for _, c := range []struct {
		name       string
		clientMax  int64
		wantChange bool
	}{
		{name: "matching bounds", clientMax: 4096, wantChange: true},
		{name: "client chooses a lower bound", clientMax: 1024},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := newFakeLog()
			log.record(created(strings.Repeat("x", 1200)))
			handlerOptions := httprest.DefaultHandlerOptions()
			handlerOptions.MaxFrameBytes = 4096
			dialOptions := httprest.DefaultDialOptions()
			dialOptions.MaxFrameBytes = c.clientMax
			s := serveLogWithOptions(t, log, handlerOptions, dialOptions)
			sub, err := s.Resubscribe(t.Context(), log.incarnation, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			change, err := sub.Next()
			if c.wantChange {
				if err != nil || len(change.Name) != 1200 {
					t.Fatalf("compatible frame returned name=%d, err=%v", len(change.Name), err)
				}
				return
			}
			if err == nil {
				t.Fatalf("client accepted a frame above its own %d-byte bound", c.clientMax)
			}
		})
	}
}

func TestOversizedIncarnationIsRefusedBeforeTheStreamOpens(t *testing.T) {
	log := newFakeLog()
	log.incarnation = metastore.Incarnation(strings.Repeat("x", httprest.MaxIncarnationBytes+1))
	handlerOptions := httprest.DefaultHandlerOptions()
	handlerOptions.MaxFrameBytes = 1024
	dialOptions := httprest.DefaultDialOptions()
	dialOptions.MaxFrameBytes = 1024
	s := serveLogWithOptions(t, log, handlerOptions, dialOptions)
	if sub, err := s.Subscribe(t.Context()); err == nil || sub != nil {
		t.Fatalf("oversized incarnation opened a stream: %v, %v", sub, err)
	}
	if err := s.Mkdir(t.Context(), "committed"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("the same oversized incarnation returned mutation result %v, want EIO", err)
	}
}

func TestAByteBoundedLogCannotReturnNoChangesWhileItsTailIsAhead(t *testing.T) {
	log := newFakeLog()
	log.record(created("missing"))
	log.emptyBounded = true
	s := serveLog(t, log, httprest.DefaultLimits())
	sub, err := s.Resubscribe(t.Context(), log.incarnation, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if change, err := sub.Next(); err == nil {
		t.Fatalf("empty bounded page advanced the stream with %+v, %v", change, err)
	}
}

// A replica that fell out of the log's window has to be told to start over, and told which
// dimension pushed it out: age says it was away too long, volume says the volume changes
// faster than the log was configured to hold. Those are different things for an operator to
// do, and a transport that carried only "start over" would have thrown that away.
func TestAReplicaOutsideTheWindowIsToldToRebuildAndWhy(t *testing.T) {
	for _, c := range []struct {
		name   string
		byAge  bool
		reason httprest.RebuildReason
	}{
		{"discarded for being old", true, httprest.RebuildAge},
		{"discarded for being too many", false, httprest.RebuildVolume},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := newFakeLog()
			for _, name := range []string{"a", "b", "c", "d"} {
				log.record(created(name))
			}
			// Everything up to position 3 is gone, so a replica sitting at 1 cannot be
			// told what happened between there and here.
			log.discard(3, c.byAge)
			s := serveLog(t, log, httprest.DefaultLimits())

			_, err := s.Resubscribe(t.Context(), log.incarnation, 1)
			var rebuild *httprest.RebuildError
			if !errors.As(err, &rebuild) {
				t.Fatalf("resubscribing outside the window gave %v, want a RebuildError", err)
			}
			if rebuild.Reason != c.reason {
				t.Fatalf("the rebuild names %q, want %q", rebuild.Reason, c.reason)
			}
		})
	}
}

// The same answer, for the case a position alone cannot detect. A log that lost its history
// would otherwise be asked to continue at a position it has never heard of, and the
// plausible reply — "that is inside my window, you are caught up" — loses everything in
// between with nothing left to notice it by.
func TestALogThatIsNotTheOneTheReplicaSawRefusesToContinue(t *testing.T) {
	log := newFakeLog()
	for _, name := range []string{"a", "b"} {
		log.record(created(name))
	}
	s := serveLog(t, log, httprest.DefaultLimits())

	// Position 1 is inside the window, and would be resumable if only the position were
	// compared.
	_, err := s.Resubscribe(t.Context(), "some-other-log", 1)
	var rebuild *httprest.RebuildError
	if !errors.As(err, &rebuild) {
		t.Fatalf("resubscribing to a different log gave %v, want a RebuildError", err)
	}
	if rebuild.Reason != httprest.RebuildIncarnation {
		t.Fatalf("the rebuild names %q, want %q", rebuild.Reason, httprest.RebuildIncarnation)
	}
	// The guard is only worth anything if the position on its own would have been
	// accepted, so that is asserted rather than assumed.
	sub := watch(t, s, func() (*httprest.Subscription, error) {
		return s.Resubscribe(t.Context(), log.incarnation, 1)
	})
	if sub.Position() != 1 {
		t.Fatalf("the same position under the right incarnation began at %d, want 1", sub.Position())
	}
}

// Caught up and fell out of the window differ by a full rebuild of the replica, and the
// case where they are hardest to tell apart is a log that has discarded everything: the
// oldest position it holds is then nothing at all, and only the tail separates the two.
func TestCaughtUpIsNotFellOutOfTheWindow(t *testing.T) {
	log := newFakeLog()
	for _, name := range []string{"a", "b", "c"} {
		log.record(created(name))
	}
	log.discard(3, true)
	s := serveLog(t, log, httprest.DefaultLimits())

	// At the tail, having seen all three: nothing was missed, and the empty window is
	// beside the point.
	sub := watch(t, s, func() (*httprest.Subscription, error) {
		return s.Resubscribe(t.Context(), log.incarnation, 3)
	})
	if !sub.CaughtUp() {
		t.Fatal("a replica at the tail was not told it was caught up")
	}
	if sub.Position() != 3 {
		t.Fatalf("the stream begins at %d, want the tail at 3", sub.Position())
	}
	if sub.Incarnation() != log.incarnation {
		t.Fatalf("the stream names incarnation %q, want %q", sub.Incarnation(), log.incarnation)
	}

	// One position behind the tail, with the same empty window: everything is missing.
	_, err := s.Resubscribe(t.Context(), log.incarnation, 2)
	var rebuild *httprest.RebuildError
	if !errors.As(err, &rebuild) {
		t.Fatalf("resubscribing one behind an emptied log gave %v, want a RebuildError", err)
	}
}

// A replica that has missed something is given it, oldest first, beginning after the
// position it holds and never at it.
func TestAResumableReplicaIsGivenWhatItMissed(t *testing.T) {
	log := newFakeLog()
	for _, name := range []string{"a", "b", "c", "d"} {
		log.record(created(name))
	}
	s := serveLog(t, log, httprest.DefaultLimits())

	sub := watch(t, s, func() (*httprest.Subscription, error) {
		return s.Resubscribe(t.Context(), log.incarnation, 2)
	})
	if sub.CaughtUp() {
		t.Fatal("a replica two changes behind was told it was caught up")
	}
	for _, want := range []struct {
		position metastore.Position
		name     string
	}{{3, "c"}, {4, "d"}} {
		change, err := sub.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if change.Position != want.position || string(change.Name) != want.name {
			t.Fatalf("got the change at %d named %q, want %d named %q",
				change.Position, change.Name, want.position, want.name)
		}
	}
}

// Subscribing from now begins at the tail and delivers nothing older. That is what a
// replica about to take a snapshot asks for: the snapshot brings the tree, and the stream
// brings what happens to it afterwards.
func TestSubscribingFromNowStartsAtTheTail(t *testing.T) {
	log := newFakeLog()
	for _, name := range []string{"a", "b", "c"} {
		log.record(created(name))
	}
	s := serveLog(t, log, httprest.DefaultLimits())

	sub := watch(t, s, func() (*httprest.Subscription, error) { return s.Subscribe(t.Context()) })
	if sub.Position() != 3 {
		t.Fatalf("the stream begins at %d, want the tail at 3", sub.Position())
	}
	if !sub.CaughtUp() {
		t.Fatal("a stream beginning at the tail was not reported as caught up")
	}

	// Nothing older arrives: the next change is one recorded after the subscription, not
	// the three that were already there.
	log.record(created("d"))
	if err := s.Mkdir(t.Context(), "poke"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	change, err := sub.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if change.Position != 4 || string(change.Name) != "d" {
		t.Fatalf("the first change on the stream is %d named %q, want 4 named \"d\"", change.Position, change.Name)
	}
}

// waitFor polls until something becomes true, and fails the test if it never does.
func waitFor(t *testing.T, complaint string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(complaint)
}

// R-CON-2: visibility must not wait for an interval to elapse. The two halves of that are
// asserted together, because either one alone would pass for an implementation that polls
// quickly: nothing reads the log while the volume is still, and a change recorded by an
// ordinary request reaches a watching replica anyway.
func TestAChangeReachesAWatcherWithoutAnyIntervalElapsing(t *testing.T) {
	log := newFakeLog()
	s := serveLog(t, log, httprest.DefaultLimits())
	sub := watch(t, s, func() (*httprest.Subscription, error) { return s.Subscribe(t.Context()) })

	// Let the stream settle: it reads the log when it opens, and again until it has
	// nothing more.
	time.Sleep(100 * time.Millisecond)
	settled := log.reads()
	time.Sleep(300 * time.Millisecond)
	if got := log.reads(); got != settled {
		t.Fatalf("the log was read %d times while the volume was still, and %d before that: something reads it on a schedule", got, settled)
	}

	arrived := make(chan metastore.Change, 1)
	failed := make(chan error, 1)
	go func() {
		change, err := sub.Next()
		if err != nil {
			failed <- err
			return
		}
		arrived <- change
	}()

	// One ordinary mutating request. Nothing else tells the server that the volume
	// moved, and nothing else is meant to.
	started := time.Now()
	if err := s.Mkdir(t.Context(), "d"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	select {
	case change := <-arrived:
		if string(change.Name) != "d" {
			t.Fatalf("the change names %q, want \"d\"", change.Name)
		}
		if took := time.Since(started); took > 300*time.Millisecond {
			t.Fatalf("the change took %v to arrive, which is longer than the stillness this test just measured", took)
		}
	case err := <-failed:
		t.Fatalf("next: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no change arrived at the watcher")
	}
}

// A replica can fall out of the window while it is attached, not only while it is away —
// a slow reader, or a volume changing faster than the log holds. The answer is the same
// one, given the same way, because delivering what follows the gap would leave the replica
// silently wrong about everything inside it.
func TestFallingOutOfTheWindowWhileWatchingIsSaidSoToo(t *testing.T) {
	log := newFakeLog()
	log.record(created("a"))
	s := serveLog(t, log, httprest.DefaultLimits())
	sub := watch(t, s, func() (*httprest.Subscription, error) { return s.Subscribe(t.Context()) })

	// Recorded and discarded before the stream reads them, which is what happens to a
	// replica the window has overtaken.
	log.record(created("b"))
	log.record(created("c"))
	log.discard(3, false)

	if err := s.Mkdir(t.Context(), "d"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	_, err := sub.Next()
	var rebuild *httprest.RebuildError
	if !errors.As(err, &rebuild) {
		t.Fatalf("a watcher the window overtook was given %v, want a RebuildError", err)
	}
	if rebuild.Reason != httprest.RebuildVolume {
		t.Fatalf("the rebuild names %q, want %q", rebuild.Reason, httprest.RebuildVolume)
	}
	// Every call afterwards says the same thing rather than a second, unrelated failure
	// from a stream that is already over.
	if _, again := sub.Next(); !errors.As(again, &rebuild) {
		t.Fatalf("reading the stream again gave %v, want the same RebuildError", again)
	}
}

// row builds one snapshot row.
func row(parent int64, name string, node metastore.Node) metastore.Row {
	return metastore.Row{Parent: parent, Name: []byte(name), Node: node}
}

// describe renders a row so that two can be compared exactly, instants included. The
// seconds and the nanoseconds are printed apart because that is how they travel, and a
// comparison that folded them back together would not notice one of the two going missing.
func describe(r metastore.Row) string {
	return fmt.Sprintf("parent=%d name=%q id=%d mode=%v size=%d accessed=%d.%09d changed=%d.%09d content=%q",
		r.Parent, r.Name, r.Node.ID, r.Node.Mode, r.Node.Size,
		r.Node.AccessTime.Unix(), r.Node.AccessTime.Nanosecond(),
		r.Node.ModTime.Unix(), r.Node.ModTime.Nanosecond(), r.Node.Content)
}

// A snapshot has to arrive exactly as it was taken: the position it was cut at, and every
// row byte for byte. The awkward cases are here rather than in a round trip of the message
// types alone, because what a replica records is what came off the wire.
func TestASnapshotDeliversItsPositionAndItsRowsUnaltered(t *testing.T) {
	log := newFakeLog()
	for _, name := range []string{"a", "b", "c"} {
		log.record(created(name))
	}
	want := []metastore.Row{
		// The root: no parent and no name.
		{Parent: 0, Name: nil, Node: metastore.Node{ID: 1, Mode: fs.ModeDir | 0o755}},
		row(1, "plain", metastore.Node{
			ID: 2, Mode: 0o644, Size: 7,
			AccessTime: time.Unix(1755000000, 123456789),
			ModTime:    time.Unix(1755000001, 987654321),
			Content:    metastore.Key("object-key-1"),
		}),
		// A name that is not text, and a key that is not either. encoding/json would
		// replace each of those bytes with U+FFFD in a string field, and a replica holding
		// the result would address a node and an object that are not there.
		row(1, "\xff\xfe not utf-8", metastore.Node{
			ID: 3, Mode: 0o600, Size: 1,
			Content: metastore.Key("\x00\xff key"),
		}),
		// Instants either side of what a nanosecond count spans. Carried as a count, each
		// of these comes back as a different and entirely plausible date.
		row(1, "ancient", metastore.Node{
			ID: 4, Mode: 0o644,
			AccessTime: time.Date(1600, 3, 4, 5, 6, 7, 8, time.UTC),
			ModTime:    time.Date(2400, 9, 10, 11, 12, 13, 14, time.UTC),
		}),
		// A directory, which must not come back as a file of length zero.
		row(1, "sub", metastore.Node{ID: 5, Mode: fs.ModeDir | 0o750}),
	}
	// Delivered in more than one page, because a picture that fits in one frame never
	// exercises the assembly of one that does not.
	log.pages = [][]metastore.Row{want[:2], want[2:4], want[4:]}

	s := serveLog(t, log, httprest.DefaultLimits())
	snap, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()

	if snap.Position() != 3 {
		t.Fatalf("the picture reports position %d, want the tail at 3", snap.Position())
	}
	var got []metastore.Row
	for {
		rows, err := snap.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next page: %v", err)
		}
		got = append(got, rows...)
	}
	if len(got) != len(want) {
		t.Fatalf("the picture holds %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if describe(got[i]) != describe(want[i]) {
			t.Fatalf("row %d arrived as\n\t%s\nwant\n\t%s", i, describe(got[i]), describe(want[i]))
		}
	}
}

// stalledSnapshot opens a snapshot that is genuinely part way through its delivery: one
// page has arrived and the next is being waited for.
func stalledSnapshot(t *testing.T, s *httprest.Storage, log *fakeLog) *httprest.Snapshot {
	t.Helper()
	snap, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if _, err := snap.Next(); err != nil {
		snap.Close()
		t.Fatalf("the first page: %v", err)
	}
	if log.opened() != 1 {
		snap.Close()
		t.Fatalf("%d snapshots are open, want the one this test is about", log.opened())
	}
	return snap
}

// twoStalledPages is a picture that will not finish on its own, so that what releases it
// is whatever the test does.
func twoStalledPages(log *fakeLog) {
	log.pages = [][]metastore.Row{
		{row(0, "", metastore.Node{ID: 1, Mode: fs.ModeDir | 0o755})},
		{row(1, "never sent", metastore.Node{ID: 2, Mode: 0o644})},
	}
	log.stall = true
}

// A snapshot holds a read open inside the store, so a replica that dies part way through
// must not leave one behind. The deadline here is far longer than the test, which is what
// makes the caller going away the only thing that can have released it.
func TestASnapshotAbandonedPartWayThroughIsReleased(t *testing.T) {
	log := newFakeLog()
	twoStalledPages(log)
	limits := httprest.DefaultLimits()
	limits.SnapshotDeadline = time.Minute
	s := serveLog(t, log, limits)

	snap := stalledSnapshot(t, s, log)
	if err := snap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	waitFor(t, "the picture is still open on the server after the replica let go of it", func() bool {
		return log.opened() == 0
	})
}

// The other way one is released: a replica that stops reading without saying anything at
// all. Nothing but the deadline can end that, because a context deadline does not interrupt
// a write and this one is stalled on the far side of the connection.
func TestASnapshotThatOutlivesItsDeadlineIsReleased(t *testing.T) {
	log := newFakeLog()
	twoStalledPages(log)
	limits := httprest.DefaultLimits()
	limits.SnapshotDeadline = 250 * time.Millisecond
	s := serveLog(t, log, limits)

	snap := stalledSnapshot(t, s, log)
	defer snap.Close()
	waitFor(t, "the picture outlived its deadline and is still open", func() bool {
		return log.opened() == 0
	})
	// The replica is told the picture failed rather than left believing it was complete.
	if _, err := snap.Next(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("reading an abandoned picture gave %v, want a failure that is not the end of a whole one", err)
	}
}

// R-INT-3: how many snapshots may be open at once is bounded, because each holds a resource
// inside the store for as long as the network takes. A request arriving when they all are is
// refused with something worth retrying, and retrying has to actually work.
func TestOnlySoManySnapshotsAreOpenAtOnce(t *testing.T) {
	log := newFakeLog()
	twoStalledPages(log)
	limits := httprest.DefaultLimits()
	limits.Snapshots = 1
	limits.SnapshotDeadline = time.Minute
	s := serveLog(t, log, limits)

	first := stalledSnapshot(t, s, log)
	defer first.Close()

	refused, err := s.Snapshot(t.Context())
	if err == nil {
		refused.Close()
		t.Fatal("a second snapshot was taken while the only one allowed was open")
	}
	if !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("a snapshot refused for want of room gave %v, want EAGAIN", err)
	}

	// EAGAIN says try again, so it has to become possible again. Without this the bound
	// would be indistinguishable from a server that had stopped answering.
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	var second *httprest.Snapshot
	waitFor(t, "no snapshot could be taken after the first was let go, so EAGAIN was not worth acting on", func() bool {
		taken, err := s.Snapshot(t.Context())
		if err != nil {
			return false
		}
		second = taken
		return true
	})
	second.Close()
}

// replicationCalls is every way of reaching the replication half, so that a property of
// all three is asserted about all three rather than about whichever one came to mind.
func replicationCalls() map[string]func(*testing.T, *httprest.Storage) error {
	return map[string]func(*testing.T, *httprest.Storage) error{
		"subscribe": func(t *testing.T, s *httprest.Storage) error {
			sub, err := s.Subscribe(t.Context())
			if err == nil {
				sub.Close()
			}
			return err
		},
		"resubscribe": func(t *testing.T, s *httprest.Storage) error {
			sub, err := s.Resubscribe(t.Context(), "any-log-at-all", 0)
			if err == nil {
				sub.Close()
			}
			return err
		},
		"snapshot": func(t *testing.T, s *httprest.Storage) error {
			snap, err := s.Snapshot(t.Context())
			if err == nil {
				snap.Close()
			}
			return err
		},
	}
}

// A volume that keeps no log and a server that cannot be reached are opposite facts,
// and what a caller does about them is opposite too: the first will never be replicable and
// the mount goes on without a local copy, the second will answer in a moment and the mount
// waits. They must not arrive as the same error, and neither may be read as the other.
//
// What separates them is that ENOSYS can only have been said by a handler speaking this
// protocol — the response carried this protocol's mark and the one status that states an
// outcome — whereas everything else this side could not establish is EIO.
func TestNotReplicableIsNotTheSameFailureAsNotReachable(t *testing.T) {
	unreplicable := serveLog(t, nil, httprest.DefaultLimits())

	// Bind and release, so the address is one that was valid a moment ago and has nothing
	// behind it now.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	unreachable, err := httprest.Dial("http://"+address, &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	for name, reach := range replicationCalls() {
		t.Run(name, func(t *testing.T) {
			permanent := reach(t, unreplicable)
			if !errors.Is(permanent, syscall.ENOSYS) {
				t.Fatalf("a volume that keeps no log gave %v, want ENOSYS", permanent)
			}
			if errors.Is(permanent, syscall.EIO) {
				t.Fatalf("a volume that keeps no log reads as a server that could not be reached: %v", permanent)
			}

			transient := reach(t, unreachable)
			if !errors.Is(transient, syscall.EIO) {
				t.Fatalf("a server that is not there gave %v, want EIO", transient)
			}
			if errors.Is(transient, syscall.ENOSYS) {
				t.Fatalf("a server that could not be reached reads as a volume that keeps no log: %v", transient)
			}
		})
	}
}

// A volume with no change log cannot be replicated, and has to say so. An empty stream
// and a picture of no rows are the shape of a volume that exists, holds nothing and
// never changes — which a replica would believe, and go on believing (R-ERR-1, R-ERR-2).
func TestAVolumeWithNoLogRefusesRatherThanLookingEmpty(t *testing.T) {
	s := serveLog(t, nil, httprest.DefaultLimits())
	for name, reach := range replicationCalls() {
		t.Run(name, func(t *testing.T) {
			err := reach(t, s)
			if err == nil {
				t.Fatal("an unreplicable volume answered as though it could be replicated")
			}
			if !errors.Is(err, syscall.ENOSYS) {
				t.Fatalf("gave %v, want ENOSYS", err)
			}
			// ENOSYS is a standing property of this volume, so it must not be reported
			// as a rebuild — which would send a replica off to take a snapshot that will
			// be refused in exactly the same way, forever.
			var rebuild *httprest.RebuildError
			if errors.As(err, &rebuild) {
				t.Fatalf("gave a RebuildError, which asks the replica to try again for good")
			}
		})
	}
}

// A position the log has never reached, under an incarnation that does match, is not a
// window problem and must not be answered as one. Rebuilding would paper over a log that
// lost entries without changing its incarnation, which is the one failure the log's own
// startup reconciliation exists to make loud.
func TestAPositionPastTheTailIsRefusedRatherThanRebuilt(t *testing.T) {
	log := newFakeLog()
	log.record(created("a"))
	s := serveLog(t, log, httprest.DefaultLimits())

	sub, err := s.Resubscribe(t.Context(), log.incarnation, 9)
	if err == nil {
		sub.Close()
		t.Fatal("a position past the log's tail was accepted")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("a position past the tail gave %v, want EINVAL", err)
	}
	var rebuild *httprest.RebuildError
	if errors.As(err, &rebuild) {
		t.Fatal("a position past the tail was answered as a window problem")
	}
}

// frame renders one frame of a stream, byte for byte as it travels.
func frame(event, data string) string {
	return "event: " + event + "\ndata: " + data + "\n\n"
}

// streamOf answers every request with exactly these bytes and then ends the connection.
// Streams a working server never produces only exist on the wire, so the wire is where a
// test writes them.
func streamOf(t *testing.T, body string) *httprest.Storage {
	t.Helper()
	return dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(httprest.HeaderProtocol, httprest.Version)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, body)
	}))
}

const aRow = `{"rows":[{"parent":0,"name":null,"node":{"id":1,"mode":2147484096,"size":0,` +
	`"access_time":{"unix_sec":0,"nanos":0},"mod_time":{"unix_sec":0,"nanos":0},"content":null}}]}`

// A picture that stopped is a tree with nodes missing from it, and a replica built from one
// answers "no such file" for every one of them. It must never be reported as complete —
// including when the stream ended tidily on a frame boundary, which is exactly what a
// connection dropped at that moment also looks like.
func TestASnapshotThatStopsIsNotACompletePicture(t *testing.T) {
	cases := map[string]string{
		"the stream ends on a frame boundary": frame("open", `{"position":7}`) + frame("rows", aRow),
		"the stream ends inside a frame":      frame("open", `{"position":7}`) + "event: rows\ndata: {\"rows\":[",
		"a frame nobody asked for":            frame("open", `{"position":7}`) + frame("change", `{"position":8,"kind":"removed","parent":1,"name":"YQ=="}`),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			s := streamOf(t, body)
			snap, err := s.Snapshot(t.Context())
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			defer snap.Close()
			if snap.Position() != 7 {
				t.Fatalf("the picture reports position %d, want 7", snap.Position())
			}
			var err2 error
			for range 3 {
				if _, err2 = snap.Next(); err2 != nil {
					break
				}
			}
			if err2 == nil {
				t.Fatal("a picture that stopped kept yielding pages")
			}
			if errors.Is(err2, io.EOF) {
				t.Fatalf("a picture that stopped was reported as complete: %v", err2)
			}
		})
	}
}

// A change stream has no natural end, so reaching the end of one means the connection went
// rather than that the volume has settled. A replica that read the second as the first
// would sit on a copy it believes is current, indefinitely.
func TestAChangeStreamThatEndsIsAFailure(t *testing.T) {
	start := frame("start", `{"incarnation":"a-log","position":4,"tail":4}`)
	cases := map[string]string{
		"nothing follows the start":      start,
		"a change and then nothing":      start + frame("change", `{"position":5,"kind":"removed","parent":1,"name":"YQ=="}`),
		"the stream ends inside a frame": start + "event: change\ndata: {\"posi",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			s := streamOf(t, body)
			sub, err := s.Subscribe(t.Context())
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			defer sub.Close()
			var err2 error
			for range 3 {
				if _, err2 = sub.Next(); err2 != nil {
					break
				}
			}
			if err2 == nil {
				t.Fatal("a change stream that ended kept yielding changes")
			}
			if errors.Is(err2, io.EOF) {
				t.Fatalf("the end of a change stream was reported as an ordinary end: %v", err2)
			}
			// It is not an answer about the volume either: nothing here established
			// anything about a file.
			for _, errno := range []syscall.Errno{syscall.ENOENT, syscall.ENOTDIR, syscall.EISDIR} {
				if errors.Is(err2, errno) {
					t.Fatalf("a broken stream arrived as %v: %v", errno, err2)
				}
			}
		})
	}
}

// A stream that fails after its headers have gone has no status left to travel in, so it
// says why in a frame. What that must not become is a stream that merely stopped, with
// nobody able to say what happened.
func TestAFaultOnAStreamReachesTheCaller(t *testing.T) {
	const complaint = "the log could not be read"
	s := streamOf(t, frame("fault", `{"message":"`+complaint+`"}`))
	if _, err := s.Subscribe(t.Context()); err == nil {
		t.Fatal("a stream that faulted before it began was accepted")
	} else if !strings.Contains(err.Error(), complaint) {
		t.Fatalf("the failure reads %q, and does not carry what the server said", err)
	}
	if _, err := s.Snapshot(t.Context()); err == nil {
		t.Fatal("a picture that faulted before it began was accepted")
	} else if !strings.Contains(err.Error(), complaint) {
		t.Fatalf("the failure reads %q, and does not carry what the server said", err)
	}
}

func TestCompletionAndDepartureFramesRequireAnExactEmptyObject(t *testing.T) {
	malformed := map[string]string{
		"null":           `null`,
		"array":          `[]`,
		"scalar":         `1`,
		"unknown member": `{"extra":1}`,
		"trailing value": `{} {}`,
	}
	for name, payload := range malformed {
		t.Run("snapshot done "+name, func(t *testing.T) {
			s := streamOf(t, frame("open", `{"position":1}`)+frame("done", payload))
			snap, err := s.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()
			if _, err := snap.Next(); err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("malformed done frame returned %v", err)
			}
		})
		t.Run("subscription gone "+name, func(t *testing.T) {
			s := streamOf(t, frame("start", `{"incarnation":"a-log","position":1,"tail":1}`)+frame("gone", payload))
			sub, err := s.Subscribe(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			if _, err := sub.Next(); err == nil || errors.Is(err, httprest.ErrServerStopping) {
				t.Fatalf("malformed gone frame returned %v", err)
			}
		})
	}
}

// Everything a plain request checks about an answer is checked about a stream too, because
// a stream is committed to before its first frame arrives.
func TestAStreamThatIsNotThisProtocolIsRefused(t *testing.T) {
	cases := map[string]http.HandlerFunc{
		"an answer carrying no mark": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, frame("open", `{"position":1}`))
		},
		"a status nobody promised": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, httprest.Version)
			w.WriteHeader(http.StatusAccepted)
		},
		"a body that is not this framing": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, httprest.Version)
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"position":1}`)
		},
	}
	for name, respond := range cases {
		t.Run(name, func(t *testing.T) {
			s := dialHandler(t, respond)
			if snap, err := s.Snapshot(t.Context()); err == nil {
				snap.Close()
				t.Fatal("a picture was taken from something that is not this protocol")
			}
			if sub, err := s.Subscribe(t.Context()); err == nil {
				sub.Close()
				t.Fatal("a stream was opened onto something that is not this protocol")
			}
		})
	}
}

// aNode is a well-formed node on the wire, for the message cases that vary everything else.
func aNode() map[string]any {
	return map[string]any{
		"id": 2, "mode": 0o644, "size": 3,
		"access_time": map[string]any{"unix_sec": 1755000000, "nanos": 1},
		"mod_time":    map[string]any{"unix_sec": 1755000001, "nanos": 2},
		"content":     []byte("key"),
	}
}

func decodesInto(t *testing.T, fields map[string]any, into any) error {
	t.Helper()
	encoded, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return json.Unmarshal(encoded, into)
}

// A change that does not say what happened must not be applied, because a replica applies
// what arrives without asking anything back: there is no revalidation behind these messages
// and no timeout that repairs one that was wrong. A creation that lost its node decodes into
// a regular file of length zero dated the epoch, and a rename that lost its source leaves
// the node at its old name for good.
func TestAChangeThatDoesNotSayWhatHappenedIsRefused(t *testing.T) {
	from := map[string]any{"parent": 1, "name": []byte("before")}
	refused := map[string]map[string]any{
		"a creation carrying no node":                 {"position": 1, "kind": "created", "parent": 1, "name": []byte("a")},
		"a modification carrying no node":             {"position": 1, "kind": "modified", "parent": 1, "name": []byte("a")},
		"a rename carrying no node":                   {"position": 1, "kind": "renamed", "parent": 1, "name": []byte("a"), "from": from},
		"a removal carrying a node":                   {"position": 1, "kind": "removed", "parent": 1, "name": []byte("a"), "node": aNode()},
		"a rename that says nothing about where from": {"position": 1, "kind": "renamed", "parent": 1, "name": []byte("a"), "node": aNode()},
		"a creation that came from somewhere":         {"position": 1, "kind": "created", "parent": 1, "name": []byte("a"), "node": aNode(), "from": from},
		"a kind this side does not know":              {"position": 1, "kind": "teleported", "parent": 1, "name": []byte("a"), "node": aNode()},
		"no kind at all":                              {"position": 1, "parent": 1, "name": []byte("a"), "node": aNode()},
	}
	for name, fields := range refused {
		t.Run(name, func(t *testing.T) {
			var change httprest.Change
			if err := decodesInto(t, fields, &change); err == nil {
				t.Fatalf("decoded %+v, want a refusal", fields)
			}
		})
	}

	// The shapes that must still be accepted. Without these the cases above would pass
	// for a decoder that refuses every change there is.
	accepted := map[string]map[string]any{
		"a creation":     {"position": 1, "kind": "created", "parent": 1, "name": []byte("a"), "node": aNode()},
		"a modification": {"position": 2, "kind": "modified", "parent": 1, "name": []byte("a"), "node": aNode()},
		"a removal":      {"position": 3, "kind": "removed", "parent": 1, "name": []byte("a")},
		"a rename":       {"position": 4, "kind": "renamed", "parent": 1, "name": []byte("a"), "node": aNode(), "from": from},
	}
	for name, fields := range accepted {
		t.Run(name, func(t *testing.T) {
			var change httprest.Change
			if err := decodesInto(t, fields, &change); err != nil {
				t.Fatalf("refused %+v: %v", fields, err)
			}
		})
	}
}

func TestReplicationNodesAndPositionsRejectValuesAReplicaCannotPersist(t *testing.T) {
	badNodes := map[string]func(map[string]any){
		"missing identity":        func(node map[string]any) { node["id"] = 0 },
		"negative size":           func(node map[string]any) { node["size"] = -1 },
		"negative access nanos":   func(node map[string]any) { node["access_time"].(map[string]any)["nanos"] = -1 },
		"overflowing mod nanos":   func(node map[string]any) { node["mod_time"].(map[string]any)["nanos"] = 1_000_000_000 },
		"unsupported socket type": func(node map[string]any) { node["mode"] = uint32(fs.ModeSocket | 0o600) },
	}
	for name, spoil := range badNodes {
		t.Run(name, func(t *testing.T) {
			node := aNode()
			spoil(node)
			var change httprest.Change
			if err := decodesInto(t, map[string]any{
				"position": 1, "kind": "created", "parent": 1, "name": []byte("a"), "node": node,
			}, &change); err == nil {
				t.Fatal("invalid node decoded in a change")
			}
			var row httprest.Row
			if err := decodesInto(t, map[string]any{"parent": 1, "name": []byte("a"), "node": node}, &row); err == nil {
				t.Fatal("invalid node decoded in a snapshot row")
			}
		})
	}
	for _, position := range []int64{0, -1} {
		var change httprest.Change
		if err := decodesInto(t, map[string]any{
			"position": position, "kind": "removed", "parent": 1, "name": []byte("a"),
		}, &change); err == nil {
			t.Fatalf("change at invalid position %d decoded", position)
		}
	}
}

func TestAChangeStreamRejectsRepeatedAndBackwardPositions(t *testing.T) {
	for name, next := range map[string]int64{"repeated": 5, "backward": 4} {
		t.Run(name, func(t *testing.T) {
			body := frame("start", `{"incarnation":"a-log","position":4,"tail":6}`) +
				frame("change", `{"position":5,"kind":"removed","parent":1,"name":"YQ=="}`) +
				frame("change", fmt.Sprintf(`{"position":%d,"kind":"removed","parent":1,"name":"Yg=="}`, next))
			s := streamOf(t, body)
			sub, err := s.Subscribe(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			if _, err := sub.Next(); err != nil {
				t.Fatal(err)
			}
			if _, err := sub.Next(); err == nil {
				t.Fatalf("stream accepted %s position %d", name, next)
			}
		})
	}
}

func TestCaughtUpRemainsTrueAfterALiveChangePastTheOpeningTail(t *testing.T) {
	body := frame("start", `{"incarnation":"a-log","position":4,"tail":4}`) +
		frame("change", `{"position":5,"kind":"removed","parent":1,"name":"YQ=="}`)
	s := streamOf(t, body)
	sub, err := s.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if _, err := sub.Next(); err != nil {
		t.Fatal(err)
	}
	if !sub.CaughtUp() || sub.Position() != 5 {
		t.Fatalf("after live change: caughtUp=%v position=%d", sub.CaughtUp(), sub.Position())
	}
}

// The frames a stream is made of carry facts whose absence is indistinguishable from an
// ordinary value, so each one is refused when it does not arrive.
func TestAFrameMissingWhatItCarriesIsRefused(t *testing.T) {
	t.Run("a start that says nothing", func(t *testing.T) {
		refused := map[string]map[string]any{
			// An empty incarnation matches every position every replica ever held, so a
			// replica holding one would be told it was caught up by a log that had lost
			// its history.
			"no incarnation": {"position": 4, "tail": 4},
			"no position":    {"incarnation": "a-log", "tail": 4},
			// Without the log's tail a replica that is behind has no way to learn that it
			// has stopped being behind, and would answer from a copy it knows is missing
			// changes.
			"nothing about how far the log had got": {"incarnation": "a-log", "position": 4},
			// A log that has not reached what the stream is about to deliver is nothing a
			// replica can act on.
			"a tail behind the position where the stream begins": {"incarnation": "a-log", "position": 4, "tail": 3},
			"a rebuild reason nobody knows":                      {"rebuild": "because"},
			"a rebuild carrying a position":                      {"rebuild": "age", "position": 4},
			// A tail is a thing to measure progress against, and a replica told to rebuild
			// has no progress left to measure: everything it held is worth nothing.
			"a rebuild carrying a tail":      {"rebuild": "age", "tail": 9},
			"a rebuild carrying what to do":  {"rebuild": "age", "incarnation": "a-log"},
			"an empty object saying nothing": {},
		}
		for name, fields := range refused {
			t.Run(name, func(t *testing.T) {
				var start httprest.StreamStart
				if err := decodesInto(t, fields, &start); err == nil {
					t.Fatalf("decoded %+v, want a refusal", fields)
				}
			})
		}
		for name, fields := range map[string]map[string]any{
			"a stream that is caught up": {"incarnation": "a-log", "position": 4, "tail": 4},
			"a stream with a backlog":    {"incarnation": "a-log", "position": 4, "tail": 9},
			"a rebuild":                  {"rebuild": "volume"},
		} {
			t.Run(name, func(t *testing.T) {
				var start httprest.StreamStart
				if err := decodesInto(t, fields, &start); err != nil {
					t.Fatalf("refused %+v: %v", fields, err)
				}
			})
		}
	})

	// Position zero is a legitimate cut — a volume nothing has yet changed is at zero —
	// so an absent one arrives as an ordinary answer, and a replica seeded at zero would
	// replay changes it already holds.
	t.Run("a picture that does not say when it was taken", func(t *testing.T) {
		var open httprest.SnapshotOpen
		if err := decodesInto(t, map[string]any{}, &open); err == nil {
			t.Fatal("decoded a picture with no position, want a refusal")
		}
		if err := decodesInto(t, map[string]any{"position": 0}, &open); err != nil {
			t.Fatalf("refused a picture taken at position zero: %v", err)
		}
	})

	// JSON null and an empty list are two characters apart. A page that lost its rows would
	// otherwise take every node it was carrying out of the replica.
	t.Run("a page that carries no rows", func(t *testing.T) {
		var page httprest.SnapshotPage
		if err := json.Unmarshal([]byte(`{"rows":null}`), &page); err == nil {
			t.Fatal("decoded a page carrying no rows, want a refusal")
		}
		if err := json.Unmarshal([]byte(`{"rows":[]}`), &page); err == nil {
			t.Fatal("decoded an empty page that makes no snapshot progress")
		}
	})

	// A row that lost its node reads as a regular file of length zero dated the epoch, and
	// nothing about that looks wrong to whatever walks the tree afterwards.
	t.Run("a row that carries no node", func(t *testing.T) {
		var row httprest.Row
		if err := decodesInto(t, map[string]any{"parent": 1, "name": []byte("a")}, &row); err == nil {
			t.Fatal("decoded a row with no node, want a refusal")
		}
		if err := decodesInto(t, map[string]any{"parent": 1, "name": []byte("a"), "node": aNode()}, &row); err != nil {
			t.Fatalf("refused a whole row: %v", err)
		}
	})
}

// describeChange renders a change so that two can be compared exactly.
func describeChange(c metastore.Change) string {
	rendered := fmt.Sprintf("position=%d kind=%d parent=%d name=%q", c.Position, c.Kind, c.Parent, c.Name)
	if c.From == nil {
		rendered += " from=none"
	} else {
		rendered += fmt.Sprintf(" from=(%d,%q)", c.From.Parent, c.From.Name)
	}
	if c.Node == nil {
		return rendered + " node=none"
	}
	return rendered + " node=" + describe(metastore.Row{Node: *c.Node})
}

// A change has to come back exactly as it went, or a replica records something the
// volume does not hold.
func TestAChangeSurvivesTheRoundTrip(t *testing.T) {
	node := metastore.Node{
		ID: 7, Mode: fs.ModeDir | 0o750, Size: 4096,
		AccessTime: time.Date(1600, 1, 2, 3, 4, 5, 6, time.UTC),
		ModTime:    time.Date(2400, 7, 8, 9, 10, 11, 12, time.UTC),
		Content:    metastore.Key("\x00\xff opaque"),
	}
	for _, want := range []metastore.Change{
		{Position: 1, Kind: metastore.Created, Parent: 1, Name: []byte("\xff\xfe not utf-8"), Node: &node},
		{Position: 9007199254740993, Kind: metastore.Modified, Parent: 5, Name: []byte("日本語"), Node: &node},
		{Position: 3, Kind: metastore.Removed, Parent: 1, Name: []byte("gone")},
		{Position: 4, Kind: metastore.Renamed, Parent: 2, Name: []byte("after"),
			From: &metastore.Location{Parent: 1, Name: []byte("\x00before")}, Node: &node},
	} {
		wire, err := httprest.ChangeOf(want)
		if err != nil {
			t.Fatalf("render %s: %v", describeChange(want), err)
		}
		encoded, err := json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		var decoded httprest.Change
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("%s does not decode from %s: %v", describeChange(want), encoded, err)
		}
		if got := decoded.Metastore(); describeChange(got) != describeChange(want) {
			t.Fatalf("round trip through %s gave\n\t%s\nwant\n\t%s", encoded, describeChange(got), describeChange(want))
		}
	}
}

// A kind this side cannot name must not be sent under a name that means something else.
func TestAChangeOfAnUnnameableKindIsNotSent(t *testing.T) {
	_, err := httprest.ChangeOf(metastore.Change{Position: 1, Kind: metastore.ChangeKind(99), Parent: 1, Name: []byte("a")})
	if err == nil {
		t.Fatal("a change of an unknown kind was rendered for the wire")
	}
}

// A picture that yields nothing and calls itself incomplete would be asked for more
// forever, holding open the read the bounds above exist to release. The replica is told the
// delivery failed rather than left waiting on a server that will never answer again.
func TestAPictureThatYieldsNothingAndIsNotDoneFails(t *testing.T) {
	log := newFakeLog()
	log.pages = [][]metastore.Row{
		{row(0, "", metastore.Node{ID: 1, Mode: fs.ModeDir | 0o755})},
		nil,
		{row(1, "never reached", metastore.Node{ID: 2, Mode: 0o644})},
	}
	s := serveLog(t, log, httprest.DefaultLimits())

	snap, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()
	if _, err := snap.Next(); err != nil {
		t.Fatalf("the first page: %v", err)
	}
	if _, err := snap.Next(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("a picture that stalled gave %v, want a failure that is not the end of a whole one", err)
	}
	waitFor(t, "the picture is still open after its delivery failed", func() bool { return log.opened() == 0 })
}

// A complete picture stays complete when it is read past its end, and a failed one keeps
// reporting the same failure. Either one answering something else the second time would
// hand the caller a fresh verdict about a stream that is over.
func TestAFinishedStreamKeepsItsVerdict(t *testing.T) {
	log := newFakeLog()
	log.pages = [][]metastore.Row{{row(0, "", metastore.Node{ID: 1, Mode: fs.ModeDir | 0o755})}}
	s := serveLog(t, log, httprest.DefaultLimits())

	snap, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()
	if _, err := snap.Next(); err != nil {
		t.Fatalf("the first page: %v", err)
	}
	for range 2 {
		if _, err := snap.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("reading a whole picture past its end gave %v, want io.EOF", err)
		}
	}

	stopped := streamOf(t, frame("open", `{"position":1}`))
	cut, err := stopped.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer cut.Close()
	_, first := cut.Next()
	if first == nil {
		t.Fatal("a picture with no rows and no end kept yielding")
	}
	if _, again := cut.Next(); again == nil || again.Error() != first.Error() {
		t.Fatalf("reading it again gave %v, want the same failure as %v", again, first)
	}
}

// A frame that is not this framing must not be assembled into something that looks like
// one. Each of these is a shape the reader has to refuse rather than guess at.
func TestAMalformedFrameIsRefused(t *testing.T) {
	cases := map[string]string{
		"a frame with no data":                 "event: open\n\n",
		"a frame with no event":                "data: {\"position\":1}\n\n",
		"a frame of three lines":               "event: open\ndata: {\"position\":1}\nid: 4\n\n",
		"a line that is not a field":           "open\n{\"position\":1}\n\n",
		"an event whose data is another field": "event: open\nid: 4\n\n",
		"data that is not JSON":                frame("open", "not json"),
		// A frame larger than this side will assemble. Its payload is a whole, valid frame
		// with padding on it, so the only thing that can refuse it is the bound — which is
		// what keeps a frame that never ends from being an unbounded allocation (R-INT-3).
		"a frame past the largest one read": frame("open", `{"position":1,"padding":"`+strings.Repeat("x", 9<<20)+`"}`),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			s := streamOf(t, body)
			if snap, err := s.Snapshot(t.Context()); err == nil {
				snap.Close()
				t.Fatal("a malformed frame was read as a picture")
			}
		})
	}
}

// A stream that says why it failed and a stream that says nothing are both failures, and
// neither may be reported as an outcome about the volume.
//
// The wording is the assertion for the second one, because the wording is all it produces:
// a diagnostic that presents an empty reason reads as one that was cut short on the way
// here, and whoever is holding it cannot tell which.
func TestAFaultNobodyCanReadIsStillAFailure(t *testing.T) {
	cases := map[string]struct{ body, says string }{
		"a reason that does not decode": {frame("fault", "not json"), "does not decode"},
		"a reason nobody gave":          {frame("fault", `{}`), "gave no reason"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			s := streamOf(t, c.body)
			sub, err := s.Subscribe(t.Context())
			if err == nil {
				sub.Close()
				t.Fatal("a stream that faulted was accepted")
			}
			if !strings.Contains(err.Error(), c.says) {
				t.Fatalf("the failure reads %q, and does not say that the server %s", err, c.says)
			}
			for _, errno := range []syscall.Errno{syscall.ENOENT, syscall.ENOSYS, syscall.ESTALE} {
				if errors.Is(err, errno) {
					t.Fatalf("a stream that faulted arrived as %v: %v", errno, err)
				}
			}
		})
	}
}

// A stream states what it can do for the caller once at the outset, and again only to say
// the caller has fallen out of the window. One that begins a second time is saying
// something this side has no reading for, and guessing at it would have the replica carry
// on from a position nothing agreed on.
func TestAStreamThatBeginsTwiceIsRefused(t *testing.T) {
	start := frame("start", `{"incarnation":"a-log","position":4,"tail":4}`)
	s := streamOf(t, start+start)
	sub, err := s.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()
	_, err = sub.Next()
	if err == nil {
		t.Fatal("a stream that began twice yielded a change")
	}
	var rebuild *httprest.RebuildError
	if errors.As(err, &rebuild) {
		t.Fatalf("a stream that began twice was read as a rebuild: %v", err)
	}
}

// A rebuild has to be recognisable to whatever is holding it, both as the thing it is and
// as an errno, because a caller that lets one past becomes a replica applying changes to a
// copy that is already wrong.
func TestARebuildSaysWhatItIsAndWhichErrnoItIs(t *testing.T) {
	log := newFakeLog()
	log.record(created("a"))
	log.record(created("b"))
	log.discard(2, true)
	s := serveLog(t, log, httprest.DefaultLimits())

	_, err := s.Resubscribe(t.Context(), log.incarnation, 1)
	if err == nil {
		t.Fatal("resubscribing outside the window succeeded")
	}
	// ESTALE is what it is: the position the caller holds no longer names anything here.
	if !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("a rebuild gave %v, which is not ESTALE", err)
	}
	for _, phrase := range []string{"discarded", "old"} {
		if !strings.Contains(err.Error(), phrase) {
			t.Fatalf("the rebuild reads %q, and does not say it was %s", err, phrase)
		}
	}

	// A reason this side does not know is refused rather than rendered as one it does.
	unknown := &httprest.RebuildError{Reason: "because"}
	if !strings.Contains(unknown.Error(), "because") {
		t.Fatalf("a rebuild for an unfamiliar reason reads %q, and does not carry it", unknown)
	}
}

// A resume point names no path, so a failure about one has to name the position and the log
// instead. A message quoting an empty path would read as a report about the root.
func TestAFailedResumeNamesThePositionRatherThanAPath(t *testing.T) {
	log := newFakeLog()
	log.record(created("a"))
	s := serveLog(t, log, httprest.DefaultLimits())

	_, err := s.Resubscribe(t.Context(), log.incarnation, 12345)
	if err == nil {
		t.Fatal("a position past the log's tail was accepted")
	}
	for _, phrase := range []string{"12345", string(log.incarnation)} {
		if !strings.Contains(err.Error(), phrase) {
			t.Fatalf("the failure reads %q, and does not name %s", err, phrase)
		}
	}
}

// A log that cannot answer must not have its silence turned into an answer about the
// volume. Each of these is a failure the server meets before a stream is committed to,
// so it still has a status to say so with.
func TestALogThatCannotAnswerIsNotAnEmptyVolume(t *testing.T) {
	t.Run("a log that will not say what it is", func(t *testing.T) {
		log := newFakeLog()
		// A log matching every position every replica ever held is the worst answer this
		// system has, because the reply it makes possible is "you are caught up".
		log.incarnation = ""
		s := serveLog(t, log, httprest.DefaultLimits())
		sub, err := s.Subscribe(t.Context())
		if err == nil {
			sub.Close()
			t.Fatal("a stream was opened onto a log with no identity")
		}
		// Both ends refuse this, and which one did decides what the failure is: refused by
		// the server, it is an answer with a status behind it, and the stream was never
		// committed to. Refused by the replica, the server had already promised a stream it
		// cannot honour. The wording is what tells them apart, so it is what is asserted.
		if !strings.Contains(err.Error(), "the log reports no incarnation") {
			t.Fatalf("the failure reads %q, and is not the server refusing before it opened a stream", err)
		}
	})

	t.Run("a log that cannot be read", func(t *testing.T) {
		log := newFakeLog()
		log.failSince(errors.New("the database is gone"))
		s := serveLog(t, log, httprest.DefaultLimits())
		if sub, err := s.Subscribe(t.Context()); err == nil {
			sub.Close()
			t.Fatal("a stream was opened onto a log that cannot be read")
		}
	})

	t.Run("a picture that cannot be taken", func(t *testing.T) {
		log := newFakeLog()
		log.snapshotErr = errors.New("no room for another read")
		s := serveLog(t, log, httprest.DefaultLimits())
		snap, err := s.Snapshot(t.Context())
		if err == nil {
			snap.Close()
			t.Fatal("a picture was taken from a log that could not take one")
		}
		if log.opened() != 0 {
			t.Fatalf("%d pictures are open after one that was never taken", log.opened())
		}
	})
}

// A log that stops answering part way through a stream has to say so, because the
// alternative is a stream that goes quiet — which is what a volume nothing is changing
// also looks like.
func TestALogThatStopsAnsweringMidStreamSaysSo(t *testing.T) {
	log := newFakeLog()
	s := serveLog(t, log, httprest.DefaultLimits())
	sub := watch(t, s, func() (*httprest.Subscription, error) { return s.Subscribe(t.Context()) })

	log.failSince(errors.New("the database is gone"))
	if err := s.Mkdir(t.Context(), "d"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := sub.Next(); err == nil {
		t.Fatal("a stream whose log stopped answering yielded a change")
	} else if !strings.Contains(err.Error(), "the database is gone") {
		t.Fatalf("the failure reads %q, and does not carry what the log said", err)
	}
}

func TestBarrierFailureAfterMutationStillWakesTheChangeStream(t *testing.T) {
	log := newFakeLog()
	s := serveLog(t, log, httprest.DefaultLimits())
	sub := watch(t, s, func() (*httprest.Subscription, error) { return s.Subscribe(t.Context()) })
	log.mu.Lock()
	log.barrierErr = errors.New("the barrier row cannot be read")
	log.mu.Unlock()
	if err := s.Mkdir(t.Context(), "committed"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("mutation with unreadable barrier returned %v, want EIO", err)
	}
	change, err := sub.Next()
	if err != nil {
		t.Fatalf("barrier failure left the committed change stream asleep: %v", err)
	}
	if string(change.Name) != "committed" {
		t.Fatalf("woken stream delivered %q", change.Name)
	}
}

func TestServerRejectsInvalidLogBarriersAfterMutation(t *testing.T) {
	for name, barrier := range map[string]metastore.LogBarrier{
		"empty incarnation": {Position: 1},
		"negative position": {Incarnation: "log", Position: -1},
		"incarnation above requested bound": {
			Incarnation: metastore.Incarnation(strings.Repeat("x", httprest.MaxIncarnationBytes+1)),
			Position:    1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			log := newFakeLog()
			log.barrier = &barrier
			s := serveLog(t, log, httprest.DefaultLimits())
			if err := s.Mkdir(t.Context(), "committed"); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid barrier returned %v, want EIO", err)
			}
		})
	}
}

func TestServerReturnsPositionZeroBarrierForASuccessfulSemanticNoOp(t *testing.T) {
	log := newFakeLog()
	s := serveLog(t, log, httprest.DefaultLimits())
	barrier, err := s.SetAttrWithBarrier(t.Context(), "", storage.AttrChange{})
	if err != nil {
		t.Fatalf("empty attribute change: %v", err)
	}
	if barrier.Incarnation != string(log.incarnation) || barrier.Position != 0 {
		t.Fatalf("fresh no-op barrier = %+v, want incarnation %q at position 0", barrier, log.incarnation)
	}
}

func TestMutationBarrierWithWorstCaseJSONEscapingFitsTheMinimumBodyBound(t *testing.T) {
	log := newFakeLog()
	log.incarnation = metastore.Incarnation(strings.Repeat("\x00", 128))
	handlerOptions := httprest.DefaultHandlerOptions()
	handlerOptions.MaxBodyBytes = 1024
	handlerOptions.MaxWriteBytes = 1024
	dialOptions := httprest.DefaultDialOptions()
	dialOptions.MaxBodyBytes = 1024
	dialOptions.MaxWriteBytes = 1024
	s := serveLogWithOptions(t, log, handlerOptions, dialOptions)

	if err := s.Mkdir(t.Context(), "committed"); err != nil {
		t.Fatalf("bounded barrier at the minimum body limit: %v", err)
	}
}

// A change of a kind this protocol cannot name must not be sent under a name that means
// something else. The replica is told the stream failed, which costs a rebuild; a removal
// delivered as a creation costs a copy that is wrong for good.
func TestAChangeThatCannotBeNamedEndsTheStreamRatherThanBeingGuessedAt(t *testing.T) {
	log := newFakeLog()
	s := serveLog(t, log, httprest.DefaultLimits())
	sub := watch(t, s, func() (*httprest.Subscription, error) { return s.Subscribe(t.Context()) })

	unnameable := created("a")
	unnameable.Kind = metastore.ChangeKind(99)
	log.record(unnameable)
	if err := s.Mkdir(t.Context(), "d"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := sub.Next(); err == nil {
		t.Fatal("a change of an unnameable kind was delivered")
	} else if !strings.Contains(err.Error(), "cannot name") {
		t.Fatalf("the failure reads %q, and does not say the kind could not be named", err)
	}
}

// A frame on a change stream that is not one, or is one that does not decode, ends the
// stream rather than being skipped past. A skipped change is a permanent hole in a replica.
func TestAFrameAChangeStreamCannotUseEndsIt(t *testing.T) {
	start := frame("start", `{"incarnation":"a-log","position":4,"tail":4}`)
	cases := map[string]string{
		"a change that does not decode": start + frame("change", `{"position":5,"kind":"created","parent":1,"name":"YQ=="}`),
		"a change that is not JSON":     start + frame("change", "not json"),
		"a frame of some other kind":    start + frame("rows", `{"rows":[]}`),
		"a frame nobody has heard of":   start + frame("teleport", `{}`),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			s := streamOf(t, body)
			sub, err := s.Subscribe(t.Context())
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			defer sub.Close()
			if _, err := sub.Next(); err == nil {
				t.Fatal("a frame the stream cannot use was read as a change")
			}
		})
	}
}

// A server that cannot bound its own writes cannot bound how long a picture stays open,
// because a context deadline does not interrupt a write to a peer that has stopped reading.
// It is refused the operation rather than given an unbounded one, and the refusal happens
// before anything is taken.
func TestAServerThatCannotBoundItsWritesRefusesToTakeAPicture(t *testing.T) {
	log := newFakeLog()
	log.pages = [][]metastore.Row{{row(0, "", metastore.Node{ID: 1, Mode: fs.ModeDir | 0o755})}}
	backing := volumeFixture(t)
	h, err := httprest.NewHandler(backing, log)
	if err != nil {
		t.Fatal(err)
	}

	// A recorder is a response writer with no deadline to set, which is exactly the shape
	// this has to refuse.
	w := serve(t, h, httprest.Request{Op: httprest.OpSnapshot}, nil)
	if w.Code == 200 {
		t.Fatal("a picture was taken by a server that cannot bound how long it would take to send")
	}
	if log.opened() != 0 {
		t.Fatalf("%d pictures are open after one that was refused", log.opened())
	}
}

// interleaved opens two volumes in one database and writes to them in turn, so that the
// positions of each have real gaps in them.
//
// The gaps are the point. A position comes from one sequence shared by every volume in
// the database, so a volume's own positions are consecutive only when nothing else was
// written in between — which is to say almost never. A stand-in that handed out 1, 2, 3
// would agree with any amount of arithmetic about adjacency, and adjacency is exactly what
// must not be assumed.
func interleaved(t *testing.T, rounds int, window sqlite.Window) (quiet *sqlite.Store, everGiven []metastore.Position) {
	t.Helper()
	database := filepath.Join(t.TempDir(), "volumes.db")
	open := func(name string) *sqlite.Store {
		s, err := sqlite.Open(t.Context(), database, name, 0, window)
		if err != nil {
			t.Fatalf("open %q: %v", name, err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}
	quiet, busy := open("quiet"), open("busy")

	// Read after every round rather than at the end, so that the positions the quiet
	// volume was given are known even where the window has since discarded them.
	seen := metastore.Position(0)
	for round := range rounds {
		// Several changes to the busy volume for each one to the quiet volume, so the
		// quiet one's positions are spread far apart rather than merely not adjacent.
		for other := range 3 {
			if err := busy.Create(t.Context(), fmt.Sprintf("busy-%d-%d", round, other)); err != nil {
				t.Fatalf("create in the busy volume: %v", err)
			}
		}
		if err := quiet.Create(t.Context(), fmt.Sprintf("quiet-%d", round)); err != nil {
			t.Fatalf("create in the quiet volume: %v", err)
		}
		changes, _, err := readLogChanges(t, quiet, seen, 1000)
		if err != nil {
			t.Fatalf("read the quiet volume's log: %v", err)
		}
		for _, change := range changes {
			everGiven = append(everGiven, change.Position)
			seen = change.Position
		}
	}
	return quiet, everGiven
}

// positionsOf reports the positions a volume's log still holds.
func positionsOf(t *testing.T, log metastore.Log) []metastore.Position {
	t.Helper()
	changes, _, err := readLogChanges(t, log, 0, 1000)
	if err != nil {
		t.Fatalf("read the log: %v", err)
	}
	positions := make([]metastore.Position, 0, len(changes))
	for _, change := range changes {
		positions = append(positions, change.Position)
	}
	return positions
}

func readLogChanges(t *testing.T, log metastore.Log, after metastore.Position, limit int) ([]metastore.Change, metastore.Retention, error) {
	t.Helper()
	result, err := metastore.NewChangeResult(64<<20, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
		return 256 + lengths.Name + lengths.FromName + lengths.Content, nil
	})
	if err != nil {
		return nil, metastore.Retention{}, err
	}
	retention, err := log.Since(t.Context(), after, limit, result)
	if err != nil {
		return nil, metastore.Retention{}, err
	}
	changes, err := result.Changes()
	return changes, retention, err
}

// retentionOf reports what a log still holds.
func retentionOf(t *testing.T, log metastore.Log) metastore.Retention {
	t.Helper()
	_, retention, err := readLogChanges(t, log, 0, 0)
	if err != nil {
		t.Fatalf("read what the log holds: %v", err)
	}
	return retention
}

// A replica is resumable when the log still holds everything it has not seen. Whether the
// next position happens to be the next integer says nothing about that: positions come from
// a sequence shared by every volume in the database, so a quiet volume's positions are
// spread out by however much its neighbours were written to in between.
//
// Getting this wrong sends a replica off to walk the whole tree again for no reason, which
// is expensive and honest rather than silent — but it is triggered by a volume simply
// not being the only one in its database, which is the ordinary case.
func TestAReplicaResumesAcrossTheGapsInItsPositions(t *testing.T) {
	quiet, _ := interleaved(t, 4, sqlite.DefaultWindow())
	positions := positionsOf(t, quiet)
	if len(positions) < 3 {
		t.Fatalf("the quiet volume recorded %d changes, want at least 3", len(positions))
	}
	// Without this the whole test would pass against dense positions and prove nothing.
	gaps := 0
	for i := 1; i < len(positions); i++ {
		if positions[i] > positions[i-1]+1 {
			gaps++
		}
	}
	if gaps == 0 {
		t.Fatalf("the quiet volume's positions are %v, every one of them next to the last: the interleaving did not produce the gaps this is about", positions)
	}

	s := serveLog(t, quiet, httprest.DefaultLimits())
	for _, at := range positions[:len(positions)-1] {
		sub, err := s.Resubscribe(t.Context(), incarnationOf(t, quiet), at)
		if err != nil {
			t.Fatalf("resuming at position %d of %v: %v", at, positions, err)
		}
		if sub.Position() != at {
			sub.Close()
			t.Fatalf("resuming at %d began at %d", at, sub.Position())
		}
		sub.Close()
	}
}

// A replica that has applied nothing resumes from position zero, and a volume that is not
// the first one written in its database has no change at position 1 — so nothing about zero
// being far below the oldest entry means anything was discarded.
func TestAReplicaThatHasAppliedNothingResumesFromZero(t *testing.T) {
	quiet, _ := interleaved(t, 2, sqlite.DefaultWindow())
	positions := positionsOf(t, quiet)
	if positions[0] <= 1 {
		t.Fatalf("the quiet volume's first change is at position %d; this is about one that does not start at 1", positions[0])
	}

	s := serveLog(t, quiet, httprest.DefaultLimits())
	sub, err := s.Resubscribe(t.Context(), incarnationOf(t, quiet), 0)
	if err != nil {
		t.Fatalf("resuming from zero against a log that has discarded nothing: %v", err)
	}
	defer sub.Close()

	// Everything it has not seen is everything there is, oldest first.
	for _, want := range positions {
		change, err := sub.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if change.Position != want {
			t.Fatalf("got the change at %d, want %d of %v", change.Position, want, positions)
		}
	}
}

func incarnationOf(t *testing.T, log metastore.Log) metastore.Incarnation {
	t.Helper()
	incarnation, err := log.Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatalf("read the incarnation: %v", err)
	}
	return incarnation
}

// A replica sitting on the newest position a trim discarded has missed nothing: everything
// it still needs is on the far side of the cut. Whether the cut and the oldest surviving
// entry are consecutive integers is a fact about what the other volumes in the database
// were doing at the time, and says nothing about whether this replica can carry on.
func TestAReplicaOnTheNewestDiscardedPositionResumes(t *testing.T) {
	window := sqlite.DefaultWindow()
	window.Floor, window.Cap = 1, 4
	quiet, everGiven := interleaved(t, 6, window)

	retention := retentionOf(t, quiet)
	if retention.Oldest == 0 {
		t.Fatal("the quiet volume's log holds nothing, so there is no cut to resume across")
	}
	var cut metastore.Position
	for _, position := range everGiven {
		if position >= retention.Oldest {
			break
		}
		cut = position
	}
	if cut == 0 {
		t.Fatalf("nothing was discarded from %v, so there is no cut to resume across", everGiven)
	}
	// Without a gap after the cut this would pass against arithmetic that assumes the next
	// position is the next integer, which is the thing under test.
	if cut+1 == retention.Oldest {
		t.Fatalf("the cut at %d is next to the oldest entry at %d: this is about a cut with a gap after it", cut, retention.Oldest)
	}

	s := serveLog(t, quiet, httprest.DefaultLimits())
	sub, err := s.Resubscribe(t.Context(), incarnationOf(t, quiet), cut)
	if err != nil {
		t.Fatalf("resuming at %d, the newest position discarded, with %d the oldest still held: %v", cut, retention.Oldest, err)
	}
	defer sub.Close()

	// And it is given what follows the cut, rather than an empty stream that reads as being
	// caught up.
	change, err := sub.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if change.Position != retention.Oldest {
		t.Fatalf("the first change after the cut is at %d, want the oldest still held at %d", change.Position, retention.Oldest)
	}
}

// The other side of the same cut: a replica that has not seen everything the trim discarded
// has lost changes for good, and must be told so.
func TestAReplicaBehindTheCutIsToldToRebuild(t *testing.T) {
	window := sqlite.DefaultWindow()
	window.Floor, window.Cap = 1, 4
	quiet, everGiven := interleaved(t, 6, window)

	retention := retentionOf(t, quiet)
	var behind metastore.Position
	for _, position := range everGiven {
		if position >= retention.Oldest {
			break
		}
		if behind != 0 {
			break
		}
		behind = position
	}
	if behind == 0 || behind >= retention.Oldest {
		t.Fatalf("no position of %v sits behind the cut at %d", everGiven, retention.Oldest)
	}

	s := serveLog(t, quiet, httprest.DefaultLimits())
	sub, err := s.Resubscribe(t.Context(), incarnationOf(t, quiet), behind)
	if err == nil {
		sub.Close()
		t.Fatalf("resuming at %d, behind the cut, succeeded: changes it needed are gone", behind)
	}
	var rebuild *httprest.RebuildError
	if !errors.As(err, &rebuild) {
		t.Fatalf("resuming behind the cut gave %v, want a RebuildError", err)
	}
}

// A server going away on purpose and a server vanishing are different events, and a replica
// does different things about them: told the first, it keeps its copy and comes back with
// the position it holds; left to infer the second, it has a connection that stopped and no
// idea whether anything happened while it was gone.
//
// Being polite must therefore not cost the replica anything. This asserts both halves —
// that the stream ends, and that what ends it says which of the two it was.
func TestStoppingAServerTellsItsReplicasRatherThanBreakingTheirStreams(t *testing.T) {
	log := newFakeLog()
	h := mustHandler(t, log)
	srv := httptest.NewServer(h)
	defer srv.Close()
	srv.Config.RegisterOnShutdown(h.Stop)

	s, err := httprest.Dial(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	sub, err := s.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// A change stream never becomes idle, so a Shutdown that waited for one would sit here
	// until this context expired.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	started := time.Now()
	if err := srv.Config.Shutdown(ctx); err != nil {
		t.Fatalf("stopping with a stream attached: %v", err)
	}
	if took := time.Since(started); took > 5*time.Second {
		t.Fatalf("stopping took %v with a stream attached, which is waiting on the stream rather than ending it", took)
	}

	_, err = sub.Next()
	if !errors.Is(err, httprest.ErrServerStopping) {
		t.Fatalf("the replica was given %v, want it to be told the server was stopping", err)
	}
	// What it says, not only what it is. errors.Is compares identity and never reads the
	// message, so without this the one sentence an operator sees for a stopping server is
	// unasserted — and it is the sentence that has to keep "went away on purpose" apart
	// from "went away".
	if said := err.Error(); !strings.Contains(said, "stopping") || !strings.Contains(said, "rather than letting it break") {
		t.Fatalf("a stopping server told the replica %q, which does not say it left on purpose", said)
	}
	// Nothing was lost, so this must not read as either of the two answers that would cost
	// the replica its copy or leave it unsure what happened.
	var rebuild *httprest.RebuildError
	if errors.As(err, &rebuild) {
		t.Fatal("a server that stopped on purpose was reported as a reason to rebuild")
	}
	if errors.Is(err, syscall.EIO) {
		t.Fatalf("a server that stopped on purpose was reported as one that could not be reached: %v", err)
	}
	// Trying again is exactly what settles it.
	if !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("a server that stopped gave %v, and does not read as something to try again", err)
	}
	// The same answer to every call afterwards, rather than a second, unrelated failure
	// from a stream that is already over.
	if _, again := sub.Next(); !errors.Is(again, httprest.ErrServerStopping) {
		t.Fatalf("reading the stream again gave %v, want the same answer", again)
	}

	// And the point of all of it: what the replica was holding is still good against the
	// server that comes up next. A restart that cost every mount a full snapshot would be
	// the thing this is here to avoid, and the log is what outlives the process.
	restarted := httptest.NewServer(mustHandler(t, log))
	defer restarted.Close()
	next, err := httprest.Dial(restarted.URL, restarted.Client())
	if err != nil {
		t.Fatalf("dial the restarted server: %v", err)
	}
	resumed, err := next.Resubscribe(t.Context(), sub.Incarnation(), sub.Position())
	if err != nil {
		t.Fatalf("resuming after a restart with the incarnation and position the stream left off at: %v", err)
	}
	defer resumed.Close()
	if resumed.Position() != sub.Position() {
		t.Fatalf("the resumed stream begins at %d, want the %d the stopped one left off at", resumed.Position(), sub.Position())
	}
}

func mustHandler(t *testing.T, log metastore.Log) *httprest.Handler {
	t.Helper()
	backing := volumeFixture(t)
	h, err := httprest.NewHandler(backing, log)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// Stopping is safe to ask for twice, and means nothing to a volume that has no streams
// to end. A server holds one handler for its whole life and Shutdown may be called from
// anywhere, so neither of these may be a panic.
func TestStoppingTwiceAndStoppingAnUnreplicableVolumeAreBothHarmless(t *testing.T) {
	backing := volumeFixture(t)
	h, err := httprest.NewHandler(backing, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.Stop()
	h.Stop()
}

// A replica that is behind has to be able to learn that it has stopped being behind, and a
// stream carrying only its starting position cannot tell it: a stream with nothing left to
// replay and one that has not begun replaying look identical from the reading end. The tail
// the log had reached is what closes that, so it has to arrive and it has to be the log's.
func TestAStreamSaysHowFarTheLogHadGot(t *testing.T) {
	log := newFakeLog()
	for _, name := range []string{"a", "b", "c", "d"} {
		log.record(created(name))
	}
	s := serveLog(t, log, httprest.DefaultLimits())

	// Two changes behind: the stream begins at 2 and the log had reached 4, so the replica
	// knows both that it is behind and exactly where being current is.
	behind := watch(t, s, func() (*httprest.Subscription, error) {
		return s.Resubscribe(t.Context(), log.incarnation, 2)
	})
	if behind.Tail() != 4 {
		t.Fatalf("a stream beginning at %d reports the log's tail as %d, want 4", behind.Position(), behind.Tail())
	}
	if behind.CaughtUp() {
		t.Fatal("a replica two changes behind was told it had missed nothing")
	}
	// Applying up to the tail is the moment it is current again, and the positions it is
	// given have to reach that tail for the claim to mean anything.
	for range 2 {
		change, err := behind.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if change.Position > behind.Tail() {
			t.Fatalf("the stream delivered position %d, past the tail of %d it began by naming", change.Position, behind.Tail())
		}
	}

	// At the tail, the two are the same fact and the replica has nothing to wait for.
	current := watch(t, s, func() (*httprest.Subscription, error) { return s.Subscribe(t.Context()) })
	if current.Tail() != current.Position() {
		t.Fatalf("a stream beginning at the tail reports position %d and tail %d, want them equal", current.Position(), current.Tail())
	}
	if !current.CaughtUp() {
		t.Fatal("a stream beginning at the tail was not reported as caught up")
	}
}

// A picture the store is slow to produce is still a picture being delivered, and the reading
// end cannot tell the two apart by itself: it bounds how long a stream may say nothing before
// it stops believing in it, and that bound is armed on every stream this package opens. So a
// server grinding out a page has to say it is still there for the same reason an idle one
// does. Without that, a cold sync of a tree large enough to be worth replicating is judged
// dead on a timer, the mount never comes up, and each retry opens another read the store has
// to hold.
func TestAPictureSlowToProduceIsNotJudgedDead(t *testing.T) {
	log := newFakeLog()
	want := []metastore.Row{
		{Parent: 0, Name: nil, Node: metastore.Node{ID: 1, Mode: fs.ModeDir | 0o755}},
		row(1, "first", metastore.Node{ID: 2, Mode: 0o644}),
		row(1, "second", metastore.Node{ID: 3, Mode: 0o644}),
	}
	log.pages = [][]metastore.Row{want[:1], want[1:2], want[2:]}
	limits := httprest.DefaultLimits()
	limits.Keepalive = 40 * time.Millisecond
	// Each page after the first takes several times the silence this reader allows, which is
	// what a large tree does to a small bound. Nothing about the wire is slowed.
	const silence = 200 * time.Millisecond
	log.slowPage = 3 * silence

	s := serveLogWatchedFor(t, log, limits, silence)
	snap, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()

	var got []metastore.Row
	for {
		rows, err := snap.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("a picture that was being delivered the whole time failed after %d rows: %v", len(got), err)
		}
		got = append(got, rows...)
	}
	if len(got) != len(want) {
		t.Fatalf("the picture holds %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if describe(got[i]) != describe(want[i]) {
			t.Fatalf("row %d arrived as\n\t%s\nwant\n\t%s", i, describe(got[i]), describe(want[i]))
		}
	}
}

// Stopping a server has to release a picture being delivered as well as a change stream. A
// cold sync holds its connection non-idle for exactly as long as it takes, so a shutdown
// waiting for connections to become idle waits for the whole of it — the same hang, by the
// other door — and the replica meanwhile learns nothing about why its picture stopped.
func TestStoppingAServerReleasesAPictureBeingDelivered(t *testing.T) {
	log := newFakeLog()
	twoStalledPages(log)
	limits := httprest.DefaultLimits()
	// Far longer than this test, so that nothing but the stop can be what released it.
	limits.SnapshotDeadline = time.Minute

	backing := volumeFixture(t)
	h, err := httprest.NewHandlerWithLimits(backing, log, limits)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	srv.Config.RegisterOnShutdown(h.Stop)

	s, err := httprest.Dial(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	snap, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()
	if _, err := snap.Next(); err != nil {
		t.Fatalf("the first page: %v", err)
	}
	if log.opened() != 1 {
		t.Fatalf("%d pictures are open, want the one this test is about", log.opened())
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	started := time.Now()
	if err := srv.Config.Shutdown(ctx); err != nil {
		t.Fatalf("stopping with a picture being delivered: %v", err)
	}
	if took := time.Since(started); took > 5*time.Second {
		t.Fatalf("stopping took %v with a picture being delivered, which is waiting for it rather than ending it", took)
	}
	if log.opened() != 0 {
		t.Fatalf("%d pictures are still open on a server that has stopped", log.opened())
	}

	// And the replica is told which of the two it was. A picture cannot be resumed, so what
	// it needs to know is that taking another is worth doing.
	_, err = snap.Next()
	if !errors.Is(err, httprest.ErrServerStopping) {
		t.Fatalf("the replica was given %v, want it to be told the server was stopping", err)
	}
	if errors.Is(err, io.EOF) {
		t.Fatal("a picture that was cut off by a shutdown was reported as complete")
	}
	if !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("a picture ended by a stopping server gave %v, and does not read as something to try again", err)
	}
}

// The done frame is the one that says a picture is whole, so a picture the server stopped
// part way through must not carry one. Nothing a replica does reveals this — it acts on the
// gone frame and never reads past it — so the claim is checked where it is made, on the wire.
func TestAPictureEndedByAShutdownIsNotAnnouncedAsWhole(t *testing.T) {
	log := newFakeLog()
	twoStalledPages(log)
	limits := httprest.DefaultLimits()
	limits.SnapshotDeadline = time.Minute

	backing := volumeFixture(t)
	h, err := httprest.NewHandlerWithLimits(backing, log, limits)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/v3/snapshot")
	if err != nil {
		t.Fatalf("ask for a picture: %v", err)
	}
	defer resp.Body.Close()

	// Read up to the end of the first page, so the delivery is genuinely under way when the
	// server is stopped.
	body := bufio.NewReader(resp.Body)
	var opening strings.Builder
	for !strings.Contains(opening.String(), "event: "+"rows") {
		line, err := body.ReadString('\n')
		if err != nil {
			t.Fatalf("reading the first page: %v", err)
		}
		opening.WriteString(line)
	}
	h.Stop()

	rest, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("reading what followed the stop: %v", err)
	}
	if !strings.Contains(string(rest), "event: gone") {
		t.Fatalf("the stream ended without saying the server was going away: %q", rest)
	}
	if strings.Contains(string(rest), "event: done") {
		t.Fatalf("a picture the server stopped part way through was announced as whole: %q", rest)
	}
}

// A stream on a writer whose writes cannot be given a deadline cannot be ended from outside
// the write it is parked in, and that is the one thing a stopping server has to be able to
// do. It is refused rather than served unbounded, exactly as a picture is.
func TestAStreamWhoseWritesCannotBeBoundedIsRefused(t *testing.T) {
	log := newFakeLog()
	log.record(created("a"))
	backing := volumeFixture(t)
	h, err := httprest.NewHandler(backing, log)
	if err != nil {
		t.Fatal(err)
	}
	// A recorder is a response writer with no deadline to set, which is the shape this has
	// to refuse. Serving it would also park this test in a stream that never ends.
	for _, op := range []httprest.Op{httprest.OpSubscribe, httprest.OpResubscribe} {
		w := serve(t, h, httprest.Request{Op: op, Incarnation: log.incarnation, Position: 1}, nil)
		if w.Code == 200 {
			t.Fatalf("%s was answered with a stream by a server that cannot bound a write to one", op)
		}
	}
}
