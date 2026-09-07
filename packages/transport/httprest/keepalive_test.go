package httprest_test

import (
	"errors"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// A change stream that is working and a change stream that is gone look identical from the
// reading end: no bytes arrive on either. Everything a replica is worth rests on telling
// those apart, because the answer to the second one is to stop answering from the copy and
// the answer to the first one is to carry on.
//
// The tests here are of the two halves that make them distinguishable: the server saying
// that it is still there when it has nothing else to say, and the reading end giving up on a
// stream that has said nothing at all.

// TestTheReadingEndWaitsLongerThanTheServerIsSilentFor. The two figures are configured
// separately and mean nothing apart: a reading end that gives up sooner than the server
// speaks would sever every healthy stream on a timer, and one that waits forever is the hole
// the keepalive exists to close.
func TestTheReadingEndWaitsLongerThanTheServerIsSilentFor(t *testing.T) {
	if httprest.DefaultSilence <= httprest.DefaultLimits().Keepalive {
		t.Fatalf("a stream is kept alive every %v and given up on after %v, so every healthy stream is severed on a timer",
			httprest.DefaultLimits().Keepalive, httprest.DefaultSilence)
	}
	t.Logf("kept alive every %v, given up on after %v", httprest.DefaultLimits().Keepalive, httprest.DefaultSilence)
}

// TestAStreamWithNothingToSayIsKeptAlive. A namespace nobody is writing to is the ordinary
// case — most workspaces are idle most of the time — and a bound on silence would end every
// one of those subscriptions if the server did not say anything at all.
//
// Waiting is not by itself evidence: a stream that is merely blocked looks the same as one
// that is healthy. So the last thing this does is make a change and read it, which is the
// only observation that separates them.
func TestAStreamWithNothingToSayIsKeptAlive(t *testing.T) {
	const keepalive = 20 * time.Millisecond
	const silence = 200 * time.Millisecond

	log := &fakeLog{incarnation: "one"}
	limits := httprest.DefaultLimits()
	limits.Keepalive = keepalive
	s := serveQuietly(t, log, limits, nil, silence)

	sub := watch(t, s, func() (*httprest.Subscription, error) { return s.Subscribe(t.Context()) })

	delivered := make(chan error, 1)
	go func() {
		_, err := sub.Next()
		delivered <- err
	}()

	// Several times what the reading end allows. A stream that was not being kept alive is
	// given up on inside the first of them.
	quiet := 5 * silence
	select {
	case err := <-delivered:
		t.Fatalf("the stream ended after saying nothing for less than %v: %v", quiet, err)
	case <-time.After(quiet):
	}

	// And it is a stream, not merely an open socket: what is recorded now arrives on it.
	if err := s.Mkdir(t.Context(), "d"); err != nil {
		t.Fatalf("making a directory: %v", err)
	}
	select {
	case err := <-delivered:
		if err != nil {
			t.Fatalf("the change did not arrive on a stream that had been quiet for %v: %v", quiet, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("nothing arrived on the stream after a change was recorded, having kept it open for %v", quiet)
	}
	t.Logf("quiet for %v, and the change that followed arrived", quiet)
}

// TestAStreamThatStopsArrivingIsReportedRatherThanWaitedOnForever is the case the keepalive
// exists for, and the one the reading end has to act on.
//
// The bytes stop without anything being closed: no end of stream, no reset, nothing that
// arrives at the reading end at all. That is what a machine that vanished, a firewall that
// dropped an idle flow, and a partitioned network all look like — and it is the failure that
// a replica must not sit through, because sitting through it means going on answering from a
// copy that has stopped being fed, with no bound on how long (R-ERR-1, R-ERR-2).
func TestAStreamThatStopsArrivingIsReportedRatherThanWaitedOnForever(t *testing.T) {
	const keepalive = 20 * time.Millisecond
	const silence = 200 * time.Millisecond

	log := &fakeLog{incarnation: "one"}
	limits := httprest.DefaultLimits()
	limits.Keepalive = keepalive
	relay := &blackhole{}
	s := serveQuietly(t, log, limits, relay, silence)

	sub := watch(t, s, func() (*httprest.Subscription, error) { return s.Subscribe(t.Context()) })

	// The stream works before it is cut, so that what follows is a stream that stopped
	// rather than one that never started.
	if err := s.Mkdir(t.Context(), "before"); err != nil {
		t.Fatalf("making a directory: %v", err)
	}
	if _, err := sub.Next(); err != nil {
		t.Fatalf("reading a change from a stream that is working: %v", err)
	}

	relay.freeze()
	started := time.Now()
	_, err := sub.Next()
	noticed := time.Since(started)

	if err == nil {
		t.Fatal("a change arrived on a stream whose bytes stopped being delivered")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("the stream stopped arriving and reported %v, want EIO", err)
	}
	if !strings.Contains(err.Error(), "nothing at all arrived") {
		t.Fatalf("the stream stopped arriving and reported %v, which does not say that it went quiet", err)
	}
	// Bounded by what the reading end allows, plus the room a scheduler needs. A run that
	// took several times the bound would mean the bound is not what ended it.
	if noticed > 4*silence {
		t.Fatalf("the stream was given up on after %v, and it is allowed to say nothing for %v", noticed, silence)
	}
	t.Logf("bytes stopped arriving → reported after %v (allowed %v): %v", noticed, silence, err)
}

// serveQuietly stands a handler up behind a real listener and returns a storage that reaches
// it — through relay when one is given — and that gives up on a quiet stream after silence.
func serveQuietly(t *testing.T, log metastore.Log, limits httprest.Limits, relay *blackhole, silence time.Duration) *httprest.Storage {
	t.Helper()

	backing, err := pairedDirectory(t, t.TempDir())
	if err != nil {
		t.Fatalf("open the namespace: %v", err)
	}
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

	reach := srv.URL
	if relay != nil {
		reach = "http://" + relay.start(t, srv.Listener.Addr().String())
	}
	s, err := httprest.DialWithSilence(reach, srv.Client(), silence)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return s
}

// blackhole is a place to put a relay's bytes once it has been frozen.
//
// Freezing rather than closing is the whole point: a closed connection is an event that
// arrives at both ends, and every layer here already reports one. What has no event at all
// is a flow that simply stops being carried, and that is the one a bound on silence is the
// only defence against.
type blackhole struct {
	frozen atomic.Bool

	// held is every connection this relay has either end of. A frozen relay carries nothing
	// and closes nothing, so the server would wait for these for as long as the test process
	// lived; they are closed when the test is done with them.
	mu   sync.Mutex
	held []net.Conn
}

// start carries bytes between whatever connects to it and to, in both directions, and
// returns the address to connect to.
func (b *blackhole) start(t *testing.T, to string) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("opening a relay: %v", err)
	}
	t.Cleanup(func() {
		listener.Close()
		b.release()
	})

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
			b.hold(near)
			b.hold(far)
			go b.carry(far, near)
			go b.carry(near, far)
		}
	}()
	return listener.Addr().String()
}

// freeze stops the relay carrying anything, in both directions, without closing either end.
func (b *blackhole) freeze() { b.frozen.Store(true) }

func (b *blackhole) hold(c net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.held = append(b.held, c)
}

// release closes what the relay is holding, so that a server waiting for its connections to
// end is not waiting for a relay that has stopped carrying them.
func (b *blackhole) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.held {
		c.Close()
	}
	b.held = nil
}

func (b *blackhole) carry(dst net.Conn, src net.Conn) {
	buffer := make([]byte, 32<<10)
	for {
		n, err := src.Read(buffer)
		if n > 0 && !b.frozen.Load() {
			if _, err := dst.Write(buffer[:n]); err != nil {
				return
			}
		}
		if err != nil {
			if !b.frozen.Load() {
				dst.Close()
			}
			return
		}
	}
}
