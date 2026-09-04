package replicated_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// The system under test here is a whole namespace and a copy of it: a metastore with its
// change log, a server over it, and a client that fills a copy from the picture and keeps it
// current from the stream. Nothing is a double — the parts that would be expensive to stand
// up are the object store, which is held in memory, and the network, which is a loopback
// listener.
//
// Failures are injected between the two, at the HTTP layer, because that is where the ones
// that matter live: an event stream that ends, an event stream that cannot be opened again,
// and a log that answers that it cannot carry on.

// served is one namespace, the server in front of it, and the failures that can be arranged
// between the two.
type served struct {
	meta    *sqlite.Store
	storage *objectstore.Storage
	url     string

	// server is kept so that the connections a mount holds can be closed from underneath it.
	server *httptest.Server

	// elsewhere is a client of this namespace that keeps no copy: it is how a second machine
	// changes the namespace in these tests. Changing it through the storage object directly
	// would reach the tree without reaching the handler, and it is the handler that tells the
	// open subscriptions to read the log — so such a change would sit there until something
	// else happened to wake them.
	elsewhere *httprest.Storage

	events *eventFaults
	calls  *calls

	// silence is how long a mount of this namespace lets its stream say nothing before it
	// stops believing in it.
	silence time.Duration
}

// serve stands up a namespace whose tree is in SQLite and whose bytes are in memory.
func serve(t *testing.T, limits httprest.Limits) *served {
	t.Helper()
	return serveWithAllowance(t, limits, 0)
}

// serveWithAllowance is serve, with a ceiling on how many bytes the namespace may hold.
//
// It is how a mutation is refused for a reason that is nobody's mistake: a workspace that is
// full answers EDQUOT, and that answer has to reach the caller as itself rather than as
// anything this copy made of it.
func serveWithAllowance(t *testing.T, limits httprest.Limits, allowance int64) *served {
	t.Helper()

	meta, err := sqlite.Open(t.Context(), path.Join(t.TempDir(), "namespace.db"), "ws", allowance, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the namespace's metastore: %v", err)
	}

	backing := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := backing.Close(); err != nil {
			t.Errorf("closing the namespace: %v", err)
		}
	})
	handler, err := httprest.NewHandlerWithLimits(backing, meta, limits)
	if err != nil {
		t.Fatalf("building the handler: %v", err)
	}
	faults := &eventFaults{handler: handler, open: map[*http.Request]context.CancelFunc{}}
	counted := &calls{handler: faults, counts: map[string]int{}}

	server := httptest.NewServer(counted)
	t.Cleanup(server.Close)

	elsewhere, err := httprest.Dial(server.URL, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dialling the namespace: %v", err)
	}
	return &served{
		meta: meta, storage: backing, url: server.URL, server: server,
		elsewhere: elsewhere, events: faults, calls: counted,
		silence: httprest.DefaultSilence,
	}
}

// sever closes the connections the server holds, which ends every stream on them at once.
//
// Cutting a stream ends it where the server is, so a frame the server is already in the middle
// of writing is still written and still arrives — which is right, and is what makes it the
// wrong tool for arranging that a particular change never comes back. A connection that is
// gone takes what was being written with it.
func (s *served) sever() { s.server.CloseClientConnections() }

// mount builds a copy of the namespace and returns the storage over it, together with the
// copy itself so that a test may compare it against the source node for node.
func mount(t *testing.T, s *served) (*replicated.Storage, *sqlite.Replica) {
	t.Helper()

	replica, err := sqlite.OpenReplica(t.Context(), path.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatalf("opening the copy: %v", err)
	}
	remote, err := httprest.DialWithSilence(s.url, &http.Client{Timeout: 10 * time.Second}, s.silence)
	if err != nil {
		t.Fatalf("dialling the namespace: %v", err)
	}
	mounted, err := replicated.New(t.Context(), replica, remote)
	if err != nil {
		replica.Close()
		t.Fatalf("building the copy: %v", err)
	}
	t.Cleanup(func() { mounted.Close() })
	return mounted, replica
}

// buildFailure returns the error building a copy of s reports, and fails the test if it
// builds one.
func buildFailure(t *testing.T, s *served) error {
	t.Helper()

	replica, err := sqlite.OpenReplica(t.Context(), path.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatalf("opening the copy: %v", err)
	}
	defer replica.Close()

	remote, err := httprest.Dial(s.url, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dialling the namespace: %v", err)
	}
	mounted, err := replicated.New(t.Context(), replica, remote)
	if err == nil {
		mounted.Close()
		t.Fatal("the copy was built; this namespace cannot be replicated and building one is a claim that it can")
	}
	return err
}

// --- counting what crosses the wire ---------------------------------------------------

// calls counts the requests that reach the server, by operation.
//
// The count is the point of the whole feature: a tree that has been copied is walked without
// asking the server anything, and "without asking" is a number rather than an impression.
type calls struct {
	handler http.Handler

	mu     sync.Mutex
	counts map[string]int
}

func (c *calls) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	c.counts[strings.TrimPrefix(r.URL.Path, httprest.Prefix)]++
	c.mu.Unlock()
	c.handler.ServeHTTP(w, r)
}

// total is how many requests have reached the server, of every kind.
func (c *calls) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	total := 0
	for _, count := range c.counts {
		total += count
	}
	return total
}

// of is how many requests for one operation have reached the server.
func (c *calls) of(op httprest.Op) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.counts[string(op)]
}

// since renders what has arrived since a total was taken, for a failure message that says
// which operations were asked for rather than only how many.
func (c *calls) since(before map[string]int) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var arrived []string
	for op, count := range c.counts {
		if extra := count - before[op]; extra > 0 {
			arrived = append(arrived, fmt.Sprintf("%s×%d", op, extra))
		}
	}
	return strings.Join(arrived, " ")
}

func (c *calls) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return maps(c.counts)
}

func maps(from map[string]int) map[string]int {
	to := make(map[string]int, len(from))
	for k, v := range from {
		to[k] = v
	}
	return to
}

// --- breaking the event channel, and only that ------------------------------------------

// eventFaults can end the change streams that are open, refuse the ones that are not, refuse
// a picture of the tree, and slow one down.
//
// What it touches is the replication endpoints and nothing else, which is what makes these
// the failures worth injecting: a server that is gone takes the data channel with it, and the
// case the design turns on is the one where the data channel is fine and the copy has stopped
// being fed. The copy is then worth nothing while everything it is a copy of is still
// reachable — and a mount that answered anyway would be answering from something whose age it
// cannot know.
type eventFaults struct {
	handler http.Handler

	mu sync.Mutex
	// severed refuses new streams outright, so that a stream cut while this is set does not
	// come straight back on the reconnect.
	severed bool
	// stale answers a resume with a bogus incarnation, which is how the server is made to
	// say the log cannot carry on from where the copy stands.
	stale bool
	// refusePicture refuses a snapshot, which is the step a first mount fails at.
	refusePicture bool
	// pictureDelay is how long each frame of a picture is held back, so that a namespace can
	// be written to throughout one.
	pictureDelay time.Duration
	// eventDelay is how long each frame of a change stream is held back, which is what a
	// replica that is behind looks like: the stream is being delivered, and slowly.
	eventDelay time.Duration
	// pictureGate holds a request for a picture until it is closed, which puts the window
	// between a stream being attached and the picture being taken under a test's control —
	// the window whose changes a copy is told about twice and applies once.
	pictureGate chan struct{}
	open        map[*http.Request]context.CancelFunc
}

func (f *eventFaults) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch httprest.Op(strings.TrimPrefix(r.URL.Path, httprest.Prefix)) {
	case httprest.OpSubscribe:
		f.serveStream(w, r, false)
	case httprest.OpResubscribe:
		f.serveStream(w, r, true)
	case httprest.OpSnapshot:
		f.servePicture(w, r)
	default:
		f.handler.ServeHTTP(w, r)
	}
}

func (f *eventFaults) serveStream(w http.ResponseWriter, r *http.Request, resuming bool) {
	f.mu.Lock()
	if f.severed {
		f.mu.Unlock()
		// A status nobody promised, which is what a server that cannot serve looks like from
		// the other end: the outcome is unknown, and this side must not read it as an answer
		// about the namespace.
		http.Error(w, "the event channel is out of order", http.StatusServiceUnavailable)
		return
	}
	if f.stale && resuming {
		f.mu.Unlock()
		f.handler.ServeHTTP(w, withIncarnation(r, "a log this is not"))
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	f.open[r] = cancel
	delay := f.eventDelay
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		delete(f.open, r)
		f.mu.Unlock()
		cancel()
	}()
	if delay > 0 {
		f.handler.ServeHTTP(&heldBack{ResponseWriter: w, delay: delay}, r.WithContext(ctx))
		return
	}
	f.handler.ServeHTTP(w, r.WithContext(ctx))
}

func (f *eventFaults) servePicture(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	refuse, delay, gate := f.refusePicture, f.pictureDelay, f.pictureGate
	f.mu.Unlock()

	if gate != nil {
		<-gate
	}
	if refuse {
		http.Error(w, "no picture of this tree can be taken", http.StatusServiceUnavailable)
		return
	}
	if delay == 0 {
		f.handler.ServeHTTP(w, r)
		return
	}
	f.handler.ServeHTTP(&heldBack{ResponseWriter: w, delay: delay}, r)
}

// heldBack delays every frame of a response, so that a picture takes long enough for the
// namespace to be written to while it is being taken.
type heldBack struct {
	http.ResponseWriter
	delay time.Duration
}

func (h *heldBack) Write(p []byte) (int, error) {
	return h.ResponseWriter.Write(p)
}

func (h *heldBack) Flush() {
	time.Sleep(h.delay)
	h.ResponseWriter.(http.Flusher).Flush()
}

// mountWithGrace is mount with an explicit mutation-barrier confirmation bound.
func mountWithGrace(t *testing.T, s *served, grace time.Duration) (*replicated.Storage, *sqlite.Replica) {
	options := replicated.DefaultOptions()
	options.ConfirmationGrace = grace
	return mountWithOptions(t, s, options)
}

func mountWithOptions(t *testing.T, s *served, options replicated.Options) (*replicated.Storage, *sqlite.Replica) {
	t.Helper()

	replica, err := sqlite.OpenReplica(t.Context(), path.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatalf("opening the copy: %v", err)
	}
	remote, err := httprest.DialWithSilence(s.url, &http.Client{Timeout: 10 * time.Second}, s.silence)
	if err != nil {
		t.Fatalf("dialling the namespace: %v", err)
	}
	mounted, err := replicated.NewWithOptions(t.Context(), replica, remote, options)
	if err != nil {
		replica.Close()
		t.Fatalf("building the copy: %v", err)
	}
	t.Cleanup(func() { mounted.Close() })
	return mounted, replica
}

// Unwrap is what http.NewResponseController follows to reach the flushing and the deadlines
// of the response underneath.
func (h *heldBack) Unwrap() http.ResponseWriter { return h.ResponseWriter }

// cut ends every open change stream and refuses the ones that follow, which is a replica that
// has stopped being fed while everything else about the server still works.
func (f *eventFaults) cut() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.severed = true
	for _, cancel := range f.open {
		cancel()
	}
}

// mend lets change streams be opened again.
func (f *eventFaults) mend() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.severed = false
}

// refuseResume makes the server answer a resume the way it answers one whose position it can
// no longer supply: this is not the log you were watching, build again.
func (f *eventFaults) refuseResume(refuse bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stale = refuse
}

// refuseSnapshot makes a picture of the tree impossible to take.
func (f *eventFaults) refuseSnapshot(refuse bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.refusePicture = refuse
}

// slowSnapshot holds every frame of a picture back by delay.
func (f *eventFaults) slowSnapshot(delay time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pictureDelay = delay
}

// holdBackPicture holds a request for a picture until the returned function is called, so
// that a test decides what is changed after a stream has been attached and before the picture
// is taken. Everything changed in that window arrives on the stream as well as being in the
// picture, and is discarded when it does.
func (f *eventFaults) holdBackPicture() func() {
	f.mu.Lock()
	defer f.mu.Unlock()

	gate := make(chan struct{})
	f.pictureGate = gate
	return sync.OnceFunc(func() { close(gate) })
}

// slowEvents holds every frame of a change stream back by delay, which is a stream that is
// being delivered and is behind — the state a replica is in while it works through what it
// missed, and the one where the difference between "attached again" and "current again"
// matters.
//
// It applies to streams opened after it is set, since a stream's delay is fixed when it is
// opened.
func (f *eventFaults) slowEvents(delay time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.eventDelay = delay
}

func withIncarnation(r *http.Request, incarnation string) *http.Request {
	altered := r.Clone(r.Context())
	query := altered.URL.Query()
	query.Set("incarnation", incarnation)
	altered.URL.RawQuery = query.Encode()
	return altered
}

// --- comparing a copy against what it is a copy of ---------------------------------------

// node is one node of a tree, as both a metastore and a copy of one describe it.
type node struct {
	Path string
	metastore.Node
}

// walkSource reads the whole tree out of the namespace's own metastore, which is the one path
// to it that does not run through anything under test.
func walkSource(t *testing.T, s *served) []node {
	t.Helper()
	return walkTree(t, s.meta.Stat, s.meta.List)
}

// walkCopy reads the whole tree out of the copy.
func walkCopy(t *testing.T, r *sqlite.Replica) []node {
	t.Helper()
	return walkTree(t, r.Stat, r.List)
}

func walkTree(t *testing.T,
	stat func(context.Context, string) (metastore.Node, error),
	list func(context.Context, string) ([]metastore.Child, error),
) []node {
	t.Helper()

	root, err := stat(t.Context(), "")
	if err != nil {
		t.Fatalf("reading the root: %v", err)
	}
	tree := []node{{Path: "", Node: root}}
	for pending := []string{""}; len(pending) > 0; {
		dir := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		children, err := list(t.Context(), dir)
		if err != nil {
			t.Fatalf("listing %q: %v", dir, err)
		}
		for _, child := range children {
			at := path.Join(dir, string(child.Name))
			tree = append(tree, node{Path: at, Node: child.Node})
			if child.Node.IsDir() {
				pending = append(pending, at)
			}
		}
	}
	return tree
}

// requireSameTree compares a copy against its source node for node: the same names, the same
// ids, the same modes, sizes and times.
func requireSameTree(t *testing.T, source, copied []node) {
	t.Helper()

	if len(source) != len(copied) {
		t.Fatalf("the source holds %d nodes and the copy holds %d:\n source %v\n copy   %v",
			len(source), len(copied), source, copied)
	}
	byPath := map[string]node{}
	for _, n := range copied {
		byPath[n.Path] = n
	}
	for _, want := range source {
		got, present := byPath[want.Path]
		if !present {
			t.Fatalf("the copy does not hold %q, which the namespace does", want.Path)
		}
		if got.ID != want.ID || got.Mode != want.Mode || got.Size != want.Size ||
			!got.ModTime.Equal(want.ModTime) || !got.AccessTime.Equal(want.AccessTime) {
			t.Fatalf("the copy holds %q as %+v, the namespace holds it as %+v", want.Path, got.Node, want.Node)
		}
	}
}

// requireCaughtUp waits until the copy has applied everything the namespace has recorded, so
// that a comparison afterwards is between two settled trees rather than a race.
//
// It is not what any assertion turns on: what is asserted is the tree, and this only decides
// when to look at it.
func requireCaughtUp(t *testing.T, s *served, r *sqlite.Replica) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		committed, err := s.meta.CommittedPosition(t.Context())
		if err != nil {
			t.Fatalf("reading the position the namespace was last changed at: %v", err)
		}
		if r.Position() == committed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the copy stands at position %d and the namespace was last changed at %d",
				r.Position(), committed)
		}
		time.Sleep(time.Millisecond)
	}
}

// requireRecordedPast waits until the namespace itself has recorded a change later than at.
//
// It is how a test knows a mutation has reached the namespace without asking the copy, which is
// what is under test. The waiting is not the assertion: what is asserted is what the caller is
// told once the stream behind it goes.
func requireRecordedPast(t *testing.T, s *served, at metastore.Position) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		committed, err := s.meta.CommittedPosition(t.Context())
		if err != nil {
			t.Fatalf("reading the position the namespace was last changed at: %v", err)
		}
		if committed > at {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the namespace still stands at position %d, and the change made through the copy should have reached it", committed)
		}
		time.Sleep(time.Millisecond)
	}
}

// requireErrno fails unless err carries the errno wanted, and never accepts one that reads as
// a fact about the namespace when the truth is that it could not be answered for.
func requireErrno(t *testing.T, what string, err error, want error) {
	t.Helper()

	if err == nil {
		t.Fatalf("%s succeeded, and the answer is one this copy cannot know", what)
	}
	if errors.Is(err, os.ErrNotExist) && !errors.Is(want, os.ErrNotExist) {
		t.Fatalf("%s failed with %v; \"no such file\" is a claim about the namespace, and the truth is that this copy could not be believed", what, err)
	}
	if !errors.Is(err, want) {
		t.Fatalf("%s failed with %v, want %v", what, err, want)
	}
}

// write puts content at path through a client of its own, which is how a second machine
// changes the namespace.
func write(t *testing.T, s *served, at, content string) {
	t.Helper()

	if err := s.elsewhere.Write(t.Context(), at, []byte(content)); err != nil {
		t.Fatalf("writing %q into the namespace: %v", at, err)
	}
}

func mkdir(t *testing.T, s *served, at string) {
	t.Helper()

	if err := s.elsewhere.Mkdir(t.Context(), at); err != nil {
		t.Fatalf("making %q in the namespace: %v", at, err)
	}
}

// walkThrough reads a whole tree through a storage, the way anything that walks a directory
// tree does: one listing per directory and one stat per name.
func walkThrough(t *testing.T, s storage.Storage) []string {
	t.Helper()

	var seen []string
	for pending := []string{""}; len(pending) > 0; {
		dir := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		entries, err := s.List(t.Context(), dir)
		if err != nil {
			t.Fatalf("listing %q: %v", dir, err)
		}
		for _, entry := range entries {
			at := path.Join(dir, entry.Name)
			if _, err := s.Stat(t.Context(), at); err != nil {
				t.Fatalf("stat %q: %v", at, err)
			}
			seen = append(seen, at)
			if entry.Attr.IsDir() {
				pending = append(pending, at)
			}
		}
	}
	return seen
}

// requireUnusable waits until the copy has noticed that it is no longer being fed.
//
// The waiting is not the assertion. A stream ends where the server is, so this side learns of
// it when a read returns rather than at the moment it was cut; what is asserted is everything
// the caller does afterwards.
func requireUnusable(t *testing.T, mounted *replicated.Storage) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := mounted.Stat(t.Context(), "")
		if errors.Is(err, syscall.EIO) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the event channel has been cut and the copy still answers about the root: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

// requireHolding waits until the copy holds the name, and reports how long that took.
func requireHolding(t *testing.T, mounted *replicated.Storage, at string) time.Duration {
	t.Helper()

	started := time.Now()
	deadline := started.Add(10 * time.Second)
	for {
		_, err := mounted.Stat(t.Context(), at)
		if err == nil {
			return time.Since(started)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the copy still does not hold %q: %v", at, err)
		}
		time.Sleep(time.Millisecond)
	}
}

// --- a connection that stops carrying anything -------------------------------------------

// blackhole relays TCP between a mount and the server, and can be told to stop carrying
// bytes without closing anything.
//
// It is the failure no other injection here produces. A server that is closed, a stream that
// is ended, a status that is refused — all of those arrive at the mount as an event. A flow
// that is simply no longer carried arrives as nothing at all, which is byte for byte what a
// namespace nobody is writing to looks like. Telling those two apart is the entire basis on
// which a copy may be answered from, so it is the one that has to be tested.
type blackhole struct {
	frozen atomic.Bool

	mu   sync.Mutex
	held []net.Conn
}

// interpose puts a relay in front of the server and points everything mounted afterwards at
// it. What changes the namespace in these tests keeps reaching the server directly, so a
// mount can be cut off from it while it goes on moving.
func (s *served) interpose(t *testing.T, silence time.Duration) *blackhole {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("opening a relay: %v", err)
	}
	relay := &blackhole{}
	t.Cleanup(func() {
		listener.Close()
		relay.release()
	})

	to := strings.TrimPrefix(s.url, "http://")
	go func() {
		for {
			near, err := listener.Accept()
			if err != nil {
				return
			}
			far, err := net.Dial("tcp", to)
			if err != nil {
				near.Close()
				return
			}
			relay.hold(near)
			relay.hold(far)
			go relay.carry(far, near)
			go relay.carry(near, far)
		}
	}()

	s.url, s.silence = "http://"+listener.Addr().String(), silence
	return relay
}

// freeze stops the relay carrying anything, in either direction, without closing either end.
func (b *blackhole) freeze() { b.frozen.Store(true) }

// thaw lets it carry again, including whatever was written while it was frozen.
func (b *blackhole) thaw() { b.frozen.Store(false) }

func (b *blackhole) carry(dst, src net.Conn) {
	buffer := make([]byte, 32<<10)
	for {
		n, err := src.Read(buffer)
		for n > 0 && b.frozen.Load() {
			// Held rather than discarded, so that thawing delivers what was written while
			// the flow was stopped — which is what a network that comes back does.
			time.Sleep(time.Millisecond)
		}
		if n > 0 {
			if _, err := dst.Write(buffer[:n]); err != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (b *blackhole) hold(c net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.held = append(b.held, c)
}

// release closes what the relay holds, so that a server waiting for its connections to end
// is not waiting on a relay that has stopped carrying them.
func (b *blackhole) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.held {
		c.Close()
	}
	b.held = nil
}
