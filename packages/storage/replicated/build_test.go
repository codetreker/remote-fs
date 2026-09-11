package replicated_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestRebuildWaitsForChangesCommittedAfterSnapshot(t *testing.T) {
	limits := httprest.DefaultLimits()
	limits.MaxSubscriptions = 1
	s := serve(t, limits)
	for _, name := range []string{"modify.txt", "rename.txt", "delete.txt"} {
		write(t, s, name, "before snapshot")
	}
	gates := newReplayFrameGates(s.events.handler)
	s.events.handler = gates
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 2
	reader := &replayReadTransport{base: transport, entered: make(chan struct{}), beforeSubscribe: gates.waitForResubscribe}
	remote, err := httprest.Dial(s.url, &http.Client{Transport: reader, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	local, err := sqlite.OpenReplica(t.Context(), path.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	mounted, err := replicated.New(t.Context(), local, remote)
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		gates.done.release()
		gates.change.release()
		if err := mounted.Close(); err != nil {
			t.Errorf("closing replica: %v", err)
		}
		transport.CloseIdleConnections()
		joined := make(chan struct{})
		go func() { gates.active.Wait(); close(joined) }()
		requireReplaySignal(t, joined, "HTTP frame handlers to exit")
	})

	s.events.cut()
	requireUnusable(t, mounted)
	gates.armed.Store(true)
	s.events.refuseResume(true)
	s.events.mend()
	requireReplaySignal(t, gates.done.entered, "rebuilt snapshot done frame")
	if got := s.calls.of(httprest.OpCheckpoint); got != 1 {
		t.Fatalf("checkpoint sampled before rebuilt snapshot EOF: got %d calls, want only initial checkpoint", got)
	}

	write(t, s, "late.txt", "created after snapshot")
	write(t, s, "modify.txt", "a longer replacement after snapshot")
	if err := s.elsewhere.Rename(t.Context(), "rename.txt", "renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if err := s.elsewhere.Remove(t.Context(), "delete.txt"); err != nil {
		t.Fatal(err)
	}
	requireReplaySignal(t, gates.change.entered, "post-snapshot change frame")

	reader.armed.Store(true)
	gates.done.release()
	requireReplaySignal(t, reader.entered, "client replay read after snapshot done")
	if got := s.calls.of(httprest.OpCheckpoint); got != 2 {
		t.Fatalf("checkpoint calls after rebuilt snapshot EOF = %d, want initial plus rebuild", got)
	}
	for _, name := range []string{"late.txt", "modify.txt", "rename.txt", "renamed.txt", "delete.txt"} {
		_, err := mounted.Stat(t.Context(), name)
		if errno := storage.ErrnoOf(err); errno != syscall.EIO {
			t.Errorf("rebuild exposed %q before replay: got %v (errno %v), want EIO", name, err, errno)
		}
	}
	if entries, err := mounted.List(t.Context(), ""); storage.ErrnoOf(err) != syscall.EIO {
		t.Errorf("rebuild exposed a listing before replay: entries=%v err=%v, want EIO", entries, err)
	}

	gates.change.release()
	requireHolding(t, mounted, "late.txt")
	requireCaughtUp(t, s, local)
	requireSameTree(t, walkSource(t, s), walkCopy(t, local))
	if got := s.calls.of(httprest.OpSnapshot); got != 2 {
		t.Fatalf("snapshot requests = %d, want initial plus one rebuild", got)
	}
	if got := s.calls.of(httprest.OpSubscribe); got != 2 {
		t.Fatalf("subscriptions = %d, want one original stream for each build", got)
	}
}

type replayFrameGate struct {
	entered     chan struct{}
	released    chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newReplayFrameGate() *replayFrameGate {
	return &replayFrameGate{entered: make(chan struct{}), released: make(chan struct{})}
}

func (g *replayFrameGate) release() { g.releaseOnce.Do(func() { close(g.released) }) }

func (g *replayFrameGate) wait(ctx context.Context) error {
	g.enterOnce.Do(func() { close(g.entered) })
	select {
	case <-g.released:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func requireReplaySignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

type replayFrameGates struct {
	start                *replayFrameGate
	handler              http.Handler
	armed                atomic.Bool
	done, change         *replayFrameGate
	secondChange         *replayFrameGate
	secondAfter          atomic.Int64
	checkpoint           http.HandlerFunc
	holdSnapshotBody     bool
	snapshotBodyReleased chan struct{}
	active               sync.WaitGroup
	resumesMu            sync.Mutex
	resumes              []metastore.Position
	lastResumeDone       <-chan struct{}
}

func newReplayFrameGates(handler http.Handler) *replayFrameGates {
	return &replayFrameGates{handler: handler, done: newReplayFrameGate(), change: newReplayFrameGate()}
}

func (g *replayFrameGates) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.active.Add(1)
	defer g.active.Done()
	var event string
	var gate *replayFrameGate
	switch strings.TrimPrefix(r.URL.Path, httprest.Prefix) {
	case string(httprest.OpCheckpoint):
		if g.checkpoint != nil {
			g.checkpoint(w, r)
		} else {
			g.handler.ServeHTTP(w, r)
		}
		return
	case string(httprest.OpSnapshot):
		event, gate = "done", g.done
	case string(httprest.OpSubscribe):
		event, gate = "change", g.change
	case string(httprest.OpResubscribe):
		at, err := strconv.ParseInt(r.URL.Query().Get("position"), 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		finished := make(chan struct{})
		g.resumesMu.Lock()
		g.resumes = append(g.resumes, metastore.Position(at))
		g.lastResumeDone = finished
		g.resumesMu.Unlock()
		defer close(finished)
		g.handler.ServeHTTP(w, r)
		return
	default:
		g.handler.ServeHTTP(w, r)
		return
	}
	g.handler.ServeHTTP(&replayFrameWriter{ResponseWriter: w, ctx: r.Context(), armed: &g.armed, event: event, gate: gate, second: g.secondChange, after: &g.secondAfter, start: g.start}, r)
	if event == "done" && g.holdSnapshotBody {
		<-r.Context().Done()
		close(g.snapshotBodyReleased)
	}
}

func (g *replayFrameGates) waitForResubscribe(ctx context.Context) error {
	g.resumesMu.Lock()
	finished := g.lastResumeDone
	g.resumesMu.Unlock()
	if finished == nil {
		return nil
	}
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type replayFrameWriter struct {
	start *replayFrameGate
	http.ResponseWriter
	ctx          context.Context
	armed        *atomic.Bool
	event        string
	gate         *replayFrameGate
	eventName    bool
	second       *replayFrameGate
	after        *atomic.Int64
	currentEvent string
	payload      bool
}

func (w *replayFrameWriter) Write(p []byte) (int, error) {
	if w.eventName && string(p) == "start" && w.start != nil {
		if err := w.start.wait(w.ctx); err != nil {
			return 0, err
		}
	}
	if w.eventName && string(p) == w.event && w.armed.Load() {
		if err := w.gate.wait(w.ctx); err != nil {
			return 0, err
		}
	}
	if w.payload && w.currentEvent == "change" && w.second != nil && w.armed.Load() {
		var change struct {
			Position int64 `json:"position"`
		}
		if err := json.Unmarshal(p, &change); err != nil {
			return 0, err
		}
		if change.Position > w.after.Load() {
			if err := w.second.wait(w.ctx); err != nil {
				return 0, err
			}
		}
	}
	if w.eventName {
		w.currentEvent = string(p)
	}
	w.payload = bytes.Equal(p, []byte("data: "))
	w.eventName = bytes.Equal(p, []byte("event: "))
	return w.ResponseWriter.Write(p)
}

func (w *replayFrameWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type replayReadTransport struct {
	beforeSubscribe func(context.Context) error
	base            http.RoundTripper
	armed           atomic.Bool
	entered         chan struct{}
	once            sync.Once
}

func (r *replayReadTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if r.beforeSubscribe != nil && strings.TrimPrefix(request.URL.Path, httprest.Prefix) == string(httprest.OpSubscribe) {
		if err := r.beforeSubscribe(request.Context()); err != nil {
			return nil, err
		}
	}
	response, err := r.base.RoundTrip(request)
	if err == nil && strings.TrimPrefix(request.URL.Path, httprest.Prefix) == string(httprest.OpSubscribe) {
		response.Body = &replayReadBody{ReadCloser: response.Body, observer: r}
	}
	return response, err
}

type replayReadBody struct {
	io.ReadCloser
	observer *replayReadTransport
}

func (r *replayReadBody) Read(p []byte) (int, error) {
	if r.observer.armed.Load() {
		r.observer.once.Do(func() { close(r.observer.entered) })
	}
	return r.ReadCloser.Read(p)
}

type replayBuild struct {
	served   *served
	gates    *replayFrameGates
	reader   *replayReadTransport
	local    *sqlite.Replica
	cancel   context.CancelCauseFunc
	finished chan struct{}
	mounted  *replicated.Storage
	err      error
}

func startReplayBuild(t *testing.T, options replicated.Options, configure func(*served, *replayFrameGates)) *replayBuild {
	t.Helper()
	limits := httprest.DefaultLimits()
	limits.MaxSubscriptions = 1
	s := serve(t, limits)
	return startReplayBuildFrom(t, s, options, configure)
}

func startReplayBuildFrom(t *testing.T, s *served, options replicated.Options, configure func(*served, *replayFrameGates)) *replayBuild {
	t.Helper()
	gates := newReplayFrameGates(s.events.handler)
	gates.armed.Store(true)
	if configure != nil {
		configure(s, gates)
	}
	s.events.handler = gates
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 2
	reader := &replayReadTransport{base: transport, entered: make(chan struct{})}
	remote, err := httprest.Dial(s.url, &http.Client{Transport: reader, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	local, err := sqlite.OpenReplica(t.Context(), path.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	build := &replayBuild{served: s, gates: gates, reader: reader, local: local, cancel: cancel, finished: make(chan struct{})}
	go func() {
		build.mounted, build.err = replicated.NewWithOptions(ctx, local, remote, options)
		close(build.finished)
	}()
	t.Cleanup(func() {
		cancel(context.Canceled)
		if gates.start != nil {
			gates.start.release()
		}
		gates.done.release()
		gates.change.release()
		if gates.secondChange != nil {
			gates.secondChange.release()
		}
		requireReplaySignal(t, build.finished, "initial build to exit")
		if build.mounted != nil {
			if err := build.mounted.Close(); err != nil {
				t.Errorf("closing replica: %v", err)
			}
		} else if err := local.Close(); err != nil {
			t.Errorf("closing failed replica: %v", err)
		}
		transport.CloseIdleConnections()
		joined := make(chan struct{})
		go func() { gates.active.Wait(); close(joined) }()
		requireReplaySignal(t, joined, "initial HTTP handlers to exit")
	})
	return build
}

func (b *replayBuild) holdReplay(t *testing.T) {
	t.Helper()
	requireReplaySignal(t, b.gates.done.entered, "initial snapshot done frame")
	if got := b.served.calls.of(httprest.OpCheckpoint); got != 0 {
		t.Fatalf("checkpoint sampled before client snapshot EOF: %d calls", got)
	}
	if err := b.served.elsewhere.Create(t.Context(), "late.txt"); err != nil {
		t.Fatal(err)
	}
	requireReplaySignal(t, b.gates.change.entered, "initial post-snapshot change")
	b.reader.armed.Store(true)
	b.gates.done.release()
	requireReplaySignal(t, b.reader.entered, "initial replay read")
}

func TestInitialBuildCancellationDuringReplayCannotReturnAUsableCopy(t *testing.T) {
	build := startReplayBuild(t, replicated.DefaultOptions(), nil)
	build.holdReplay(t)
	cause := errors.New("initial build abandoned while replay was blocked")
	build.cancel(cause)
	requireReplaySignal(t, build.finished, "cancelled initial build")
	if build.mounted != nil || storage.ErrnoOf(build.err) != syscall.EIO || !errors.Is(build.err, context.Canceled) || !errors.Is(build.err, cause) {
		t.Fatalf("cancelled replay returned mounted=%v err=%v, want no copy and EIO preserving cancellation", build.mounted != nil, build.err)
	}
}

func TestInitialBuildReplayTimeoutEndsBlockedReplay(t *testing.T) {
	options := replicated.DefaultOptions()
	options.ReplayTimeout = time.Second
	build := startReplayBuild(t, options, nil)
	build.holdReplay(t)
	requireReplaySignal(t, build.finished, "replay deadline")
	if build.mounted != nil || storage.ErrnoOf(build.err) != syscall.EIO || !errors.Is(build.err, context.DeadlineExceeded) {
		t.Fatalf("expired replay returned mounted=%v err=%v, want no copy and EIO preserving deadline", build.mounted != nil, build.err)
	}
}

func TestInitialBuildUsesOneFixedCheckpointOnOriginalStream(t *testing.T) {
	build := startReplayBuild(t, replicated.DefaultOptions(), func(_ *served, gates *replayFrameGates) {
		gates.secondChange = newReplayFrameGate()
	})
	build.holdReplay(t)
	target, err := build.served.meta.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	build.gates.secondAfter.Store(int64(target))
	if err := build.served.elsewhere.Create(t.Context(), "after-checkpoint.txt"); err != nil {
		t.Fatal(err)
	}
	build.gates.change.release()
	requireReplaySignal(t, build.gates.secondChange.entered, "change beyond the fixed checkpoint")
	requireReplaySignal(t, build.finished, "initial build at the fixed checkpoint")
	if build.err != nil {
		t.Fatal(build.err)
	}
	if _, err := build.mounted.Stat(t.Context(), "late.txt"); err != nil {
		t.Fatalf("checkpoint change missing at handoff: %v", err)
	}
	if entries, err := build.mounted.List(t.Context(), ""); err != nil || len(entries) != 1 || entries[0].Name != "late.txt" {
		t.Fatalf("tree at fixed checkpoint: %v, %v", entries, err)
	}
	if got := build.served.calls.of(httprest.OpCheckpoint); got != 1 {
		t.Fatalf("checkpoints = %d, want one fixed target", got)
	}
	if got := build.served.calls.of(httprest.OpSubscribe); got != 1 {
		t.Fatalf("subscriptions = %d, want original stream only", got)
	}
	if got := build.served.calls.of(httprest.OpResubscribe); got != 0 {
		t.Fatalf("resubscriptions = %d, want none", got)
	}
	build.cancel(errors.New("construction caller has finished"))
	build.gates.secondChange.release()
	requireHolding(t, build.mounted, "after-checkpoint.txt")
	requireCaughtUp(t, build.served, build.local)
	requireSameTree(t, walkSource(t, build.served), walkCopy(t, build.local))
}

func TestInitialBuildRejectsCheckpointWithoutASnapshotCompatibleHistory(t *testing.T) {
	for _, test := range []struct {
		name        string
		change      func(*httprest.MutationBarrier)
		unavailable bool
	}{
		{name: "different incarnation", change: func(b *httprest.MutationBarrier) { b.Incarnation = "a different committed history" }},
		{name: "position older than snapshot", change: func(b *httprest.MutationBarrier) { b.Position-- }},
		{name: "authority unavailable", unavailable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			build := startReplayBuild(t, replicated.DefaultOptions(), func(s *served, gates *replayFrameGates) {
				write(t, s, "snapshot.txt", "snapshot content")
				gates.checkpoint = func(w http.ResponseWriter, r *http.Request) {
					if test.unavailable {
						http.Error(w, "checkpoint unavailable", http.StatusServiceUnavailable)
						return
					}
					barrier, err := s.meta.Barrier(r.Context(), httprest.MaxIncarnationBytes)
					if err != nil {
						t.Error(err)
						http.Error(w, "barrier failed", http.StatusInternalServerError)
						return
					}
					wire := httprest.MutationBarrier{Incarnation: string(barrier.Incarnation), Position: int64(barrier.Position)}
					test.change(&wire)
					w.Header().Set(httprest.HeaderProtocol, httprest.Version)
					if err := json.NewEncoder(w).Encode(wire); err != nil {
						t.Error(err)
					}
				}
			})
			requireReplaySignal(t, build.gates.done.entered, "snapshot done before invalid checkpoint")
			build.gates.done.release()
			requireReplaySignal(t, build.finished, "checkpoint failure")
			if build.mounted != nil || storage.ErrnoOf(build.err) != syscall.EIO {
				t.Fatalf("unusable checkpoint returned mounted=%v err=%v, want no copy and EIO", build.mounted != nil, build.err)
			}
			if got := build.served.calls.of(httprest.OpCheckpoint); got != 1 {
				t.Fatalf("checkpoint requests = %d, want one", got)
			}
		})
	}
}

func TestInitialBuildDeadlineCoversCheckpointRequest(t *testing.T) {
	options := replicated.DefaultOptions()
	options.ReplayTimeout = time.Second
	entered := make(chan struct{})
	build := startReplayBuild(t, options, func(_ *served, gates *replayFrameGates) {
		gates.checkpoint = func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-r.Context().Done()
		}
	})
	requireReplaySignal(t, build.gates.done.entered, "snapshot done before checkpoint timeout")
	build.gates.done.release()
	requireReplaySignal(t, entered, "checkpoint request")
	requireReplaySignal(t, build.finished, "checkpoint deadline")
	if build.mounted != nil || storage.ErrnoOf(build.err) != syscall.EIO || !errors.Is(build.err, context.DeadlineExceeded) {
		t.Fatalf("checkpoint timeout returned mounted=%v err=%v, want no copy and EIO preserving deadline", build.mounted != nil, build.err)
	}
}

func TestInitialBuildAtZeroCheckpointNeedsNoReplayEvent(t *testing.T) {
	options := replicated.DefaultOptions()
	options.ReplayTimeout = time.Second
	build := startReplayBuild(t, options, func(_ *served, gates *replayFrameGates) {
		gates.holdSnapshotBody = true
		gates.snapshotBodyReleased = make(chan struct{})
	})
	requireReplaySignal(t, build.gates.done.entered, "empty snapshot done")
	build.gates.done.release()
	requireReplaySignal(t, build.finished, "empty initial build")
	requireReplaySignal(t, build.gates.snapshotBodyReleased, "snapshot connection release before checkpoint")
	if build.err != nil {
		t.Fatal(build.err)
	}
	if build.local.Position() != 0 {
		t.Fatalf("empty replica position = %d, want zero", build.local.Position())
	}
	if entries, err := build.mounted.List(t.Context(), ""); err != nil || len(entries) != 0 {
		t.Fatalf("empty replica: %v, %v", entries, err)
	}
	if got := build.served.calls.of(httprest.OpCheckpoint); got != 1 {
		t.Fatalf("checkpoints = %d, want one", got)
	}
}

func TestFailedRebuildResumesFromInstalledAndAppliedProgress(t *testing.T) {
	build := startReplayBuild(t, replicated.DefaultOptions(), func(_ *served, gates *replayFrameGates) {
		gates.armed.Store(false)
		gates.secondChange = newReplayFrameGate()
	})
	requireReplaySignal(t, build.finished, "initial copy before failed rebuild")
	if build.err != nil {
		t.Fatal(build.err)
	}
	s := build.served
	s.events.cut()
	requireUnusable(t, build.mounted)
	write(t, s, "installed-by-snapshot.txt", "installed while detached")
	build.gates.armed.Store(true)
	s.events.refuseResume(true)
	s.events.mend()
	requireReplaySignal(t, build.gates.done.entered, "replacement snapshot done")
	if err := s.elsewhere.Create(t.Context(), "applied-before-failure.txt"); err != nil {
		t.Fatal(err)
	}
	first, err := s.meta.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	build.gates.secondAfter.Store(int64(first))
	if err := s.elsewhere.Create(t.Context(), "still-pending.txt"); err != nil {
		t.Fatal(err)
	}
	requireReplaySignal(t, build.gates.change.entered, "first replacement-stream change")
	build.reader.armed.Store(true)
	build.gates.done.release()
	requireReplaySignal(t, build.reader.entered, "replacement replay read")
	build.gates.change.release()
	requireReplaySignal(t, build.gates.secondChange.entered, "second replacement-stream change")
	requireReplayPosition(t, build.local, first)
	if _, err := build.mounted.Stat(t.Context(), "applied-before-failure.txt"); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("partial replay made the replacement queryable: %v", err)
	}

	s.events.cut()
	requireUnusable(t, build.mounted)
	s.events.refuseResume(false)
	build.gates.secondChange.release()
	s.events.mend()
	requireHolding(t, build.mounted, "still-pending.txt")
	requireCaughtUp(t, s, build.local)
	requireSameTree(t, walkSource(t, s), walkCopy(t, build.local))
	if got := s.calls.of(httprest.OpSnapshot); got != 2 {
		t.Fatalf("snapshots = %d, want recovery to continue the installed tree", got)
	}
	build.gates.resumesMu.Lock()
	resumes := append([]metastore.Position(nil), build.gates.resumes...)
	build.gates.resumesMu.Unlock()
	if len(resumes) == 0 || resumes[len(resumes)-1] != first {
		t.Fatalf("resume positions = %v, want last applied position %d", resumes, first)
	}
}

func requireReplayPosition(t *testing.T, local *sqlite.Replica, want metastore.Position) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		if got := local.Position(); got == want {
			return
		} else if got > want {
			t.Fatalf("replica passed held position: got %d want %d", got, want)
		}
		select {
		case <-poll.C:
		case <-deadline.C:
			t.Fatalf("replica position = %d, want %d", local.Position(), want)
		}
	}
}

func serveReplayLog(t *testing.T, window sqlite.Window, neighbouringChanges bool) *served {
	t.Helper()
	database := path.Join(t.TempDir(), "source.db")
	if neighbouringChanges {
		neighbour, err := sqlite.Open(t.Context(), database, "neighbour", 0, sqlite.DefaultWindow())
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"one", "two", "three"} {
			if err := neighbour.Create(t.Context(), name); err != nil {
				neighbour.Close()
				t.Fatal(err)
			}
		}
		if err := neighbour.Close(); err != nil {
			t.Fatal(err)
		}
	}
	options := sqlite.DefaultOptions()
	options.Window = window
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: database, Volume: "source", SQLite: options, Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	backing := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := backing.Close(); err != nil {
			t.Errorf("closing source: %v", err)
		}
	})

	limits := httprest.DefaultLimits()
	limits.MaxSubscriptions, limits.EventPage = 1, 1
	handler, err := httprest.NewHandlerWithLimits(backing, meta, limits)
	if err != nil {
		t.Fatal(err)
	}
	faults := &eventFaults{handler: handler, open: map[*http.Request]context.CancelFunc{}}
	counted := &calls{handler: faults, counts: map[string]int{}}
	server := httptest.NewServer(counted)
	t.Cleanup(server.Close)
	elsewhere, err := httprest.Dial(server.URL, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return &served{meta: meta.Store, storage: backing, url: server.URL, server: server, elsewhere: elsewhere, events: faults, calls: counted, silence: httprest.DefaultSilence}
}

func TestInitialBuildAcceptsNonAdjacentLogPositions(t *testing.T) {
	s := serveReplayLog(t, sqlite.DefaultWindow(), true)
	build := startReplayBuildFrom(t, s, replicated.DefaultOptions(), nil)
	requireReplaySignal(t, build.gates.done.entered, "snapshot before position jumps")
	if err := s.elsewhere.Create(t.Context(), "late.txt"); err != nil {
		t.Fatal(err)
	}
	target, err := s.meta.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if target <= 2 {
		t.Fatalf("fixture did not produce a gap from position zero: %d", target)
	}
	requireReplaySignal(t, build.gates.change.entered, "non-adjacent change frame")
	build.reader.armed.Store(true)
	build.gates.done.release()
	requireReplaySignal(t, build.reader.entered, "non-adjacent replay")
	build.gates.change.release()
	requireReplaySignal(t, build.finished, "build across a legal position gap")
	if build.err != nil {
		t.Fatal(build.err)
	}
	if got := build.local.Position(); got != target {
		t.Fatalf("replica position = %d, want %d", got, target)
	}
	requireSameTree(t, walkSource(t, s), walkCopy(t, build.local))
}

func TestInitialBuildRejectsRetentionLossBeforeCheckpoint(t *testing.T) {
	s := serveReplayLog(t, sqlite.Window{Floor: 2, Cap: 2, Age: time.Hour}, false)
	build := startReplayBuildFrom(t, s, replicated.DefaultOptions(), nil)
	requireReplaySignal(t, build.gates.done.entered, "snapshot before retention loss")
	if err := s.elsewhere.Create(t.Context(), "first.txt"); err != nil {
		t.Fatal(err)
	}
	requireReplaySignal(t, build.gates.change.entered, "first retained change")
	for _, name := range []string{"discarded.txt", "tail.txt"} {
		if err := s.elsewhere.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	build.reader.armed.Store(true)
	build.gates.done.release()
	requireReplaySignal(t, build.reader.entered, "replay after retention loss")
	build.gates.change.release()
	requireReplaySignal(t, build.finished, "retention loss failure")
	var rebuild *httprest.RebuildError
	if build.mounted != nil || storage.ErrnoOf(build.err) != syscall.EIO || !errors.As(build.err, &rebuild) || rebuild.Reason != httprest.RebuildVolume {
		t.Fatalf("retention loss returned mounted=%v err=%v, want EIO preserving volume rebuild reason", build.mounted != nil, build.err)
	}
	if got := s.calls.of(httprest.OpSubscribe); got != 1 {
		t.Fatalf("subscriptions = %d, want no replacement stream during initial build", got)
	}
}

type unappliableReplayLog struct{ metastore.Log }

func (l unappliableReplayLog) Since(ctx context.Context, after metastore.Position, limit int, result *metastore.ChangeResult) (metastore.Retention, error) {
	retention, err := l.Log.Since(ctx, after, limit, result)
	if err != nil {
		return retention, err
	}
	changes, err := result.Changes()
	if err != nil {
		return retention, err
	}
	for i := range changes {
		changes[i].Kind, changes[i].Node = metastore.Removed, nil
	}
	return retention, nil
}

func TestInitialBuildRejectsAnUnappliableReplayChange(t *testing.T) {
	build := startReplayBuild(t, replicated.DefaultOptions(), func(s *served, gates *replayFrameGates) {
		handler, err := httprest.NewHandlerWithLimits(s.storage, unappliableReplayLog{s.meta}, httprest.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		gates.handler = handler
	})
	build.holdReplay(t)
	build.gates.change.release()
	requireReplaySignal(t, build.finished, "unappliable replay failure")
	if build.mounted != nil || storage.ErrnoOf(build.err) != syscall.EIO || !strings.Contains(build.err.Error(), "applying snapshot replay") {
		t.Fatalf("unappliable change returned mounted=%v err=%v, want EIO from applying replay", build.mounted != nil, build.err)
	}
	if got := build.local.Position(); got != 0 {
		t.Fatalf("failed apply advanced replica from snapshot position zero to %d", got)
	}
}

type replayContextKey struct{}

type replayContextTransport struct {
	base http.RoundTripper
	seen chan httprest.Op
}

func (r replayContextTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	op := httprest.Op(strings.TrimPrefix(request.URL.Path, httprest.Prefix))
	if op == httprest.OpSnapshot || op == httprest.OpCheckpoint {
		if request.Context().Value(replayContextKey{}) != "construction trace" {
			return nil, fmt.Errorf("%s lost the construction context value", op)
		}
		r.seen <- op
	}
	return r.base.RoundTrip(request)
}

func TestInitialBuildPreservesCallerValuesThroughSnapshotAndCheckpoint(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 2
	t.Cleanup(transport.CloseIdleConnections)
	seen := make(chan httprest.Op, 2)
	remote, err := httprest.Dial(s.url, &http.Client{Transport: replayContextTransport{base: transport, seen: seen}, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	local, err := sqlite.OpenReplica(t.Context(), path.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(t.Context(), replayContextKey{}, "construction trace")
	mounted, err := replicated.New(ctx, local, remote)
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := mounted.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, want := range []httprest.Op{httprest.OpSnapshot, httprest.OpCheckpoint} {
		select {
		case got := <-seen:
			if got != want {
				t.Fatalf("setup request = %s, want %s", got, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("setup never issued %s", want)
		}
	}
}

type snapshotCloseFaultTransport struct {
	base    http.RoundTripper
	failure error
}

func (r snapshotCloseFaultTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := r.base.RoundTrip(request)
	if err == nil && request.URL.Path == httprest.Prefix+string(httprest.OpSnapshot) {
		response.Body = snapshotCloseFaultBody{ReadCloser: response.Body, failure: r.failure}
	}
	return response, err
}

type snapshotCloseFaultBody struct {
	io.ReadCloser
	failure error
}

func (r snapshotCloseFaultBody) Close() error {
	return errors.Join(r.ReadCloser.Close(), r.failure)
}

func TestInitialBuildRejectsSnapshotCloseFailureBeforeCheckpoint(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 2
	t.Cleanup(transport.CloseIdleConnections)
	cause := errors.New("snapshot response close failed")
	remote, err := httprest.Dial(s.url, &http.Client{Transport: snapshotCloseFaultTransport{base: transport, failure: cause}, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	local, err := sqlite.OpenReplica(t.Context(), path.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	mounted, err := replicated.New(t.Context(), local, remote)
	if mounted != nil {
		mounted.Close()
	} else if closeErr := local.Close(); closeErr != nil {
		t.Error(closeErr)
	}
	if mounted != nil || storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, cause) {
		t.Fatalf("snapshot close failure returned mounted=%v err=%v, want no copy and EIO preserving close failure", mounted != nil, err)
	}
	if got := s.calls.of(httprest.OpCheckpoint); got != 0 {
		t.Fatalf("failed snapshot cleanup dispatched %d checkpoints", got)
	}
}

func TestInitialBuildCancellationDuringSubscribeReleasesItsSlot(t *testing.T) {
	build := startReplayBuild(t, replicated.DefaultOptions(), func(_ *served, gates *replayFrameGates) {
		gates.start = newReplayFrameGate()
	})
	requireReplaySignal(t, build.gates.start.entered, "initial subscription start frame")
	cause := errors.New("construction cancelled during subscription handshake")
	build.cancel(cause)
	requireReplaySignal(t, build.finished, "cancelled subscription setup")
	if build.mounted != nil || storage.ErrnoOf(build.err) != syscall.EIO || !errors.Is(build.err, context.Canceled) || !errors.Is(build.err, cause) {
		t.Fatalf("cancelled subscription returned mounted=%v err=%v, want no copy and EIO preserving cancellation", build.mounted != nil, build.err)
	}
	joined := make(chan struct{})
	go func() { build.gates.active.Wait(); close(joined) }()
	requireReplaySignal(t, joined, "cancelled subscription handler to release its slot")
	if got := build.served.calls.of(httprest.OpSnapshot); got != 0 {
		t.Fatalf("cancelled initial subscription dispatched %d snapshots", got)
	}
	build.gates.start.release()
	sub, err := build.served.elsewhere.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("cancelled setup kept the only subscription slot: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Errorf("closing replacement subscription: %v", err)
	}
}
