package httprest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// The replication half of the dialling end: a way to watch what changes in a namespace,
// and a way to take the picture that watching starts from.
//
// Both live on Storage rather than on a type of their own, because one Dial is one
// namespace and a replica needs both halves of it — the reads and writes that go to the
// server, and the stream that tells it what its local copy is still worth. A namespace
// that keeps no log answers both of these with ENOSYS, which is a fact about that
// namespace and arrives as an ordinary storage error under its own name.
//
// The order these are used in is not interchangeable: subscribe first, take the snapshot
// second, discard the changes at or before the snapshot's position, apply the rest. The
// reverse order does not converge — the scan takes time, changes accumulate while it runs,
// and once enough of them accumulate to push the snapshot's position out of the log's
// window the replica starts over, at a cost proportional to the size of the tree. The
// larger the namespace, the less likely that is ever to finish.

// RebuildError says a log cannot carry on from where the caller asked, so the replica it
// was feeding is worth nothing and has to be built again from a snapshot.
//
// It is a type of its own rather than a flag on a stream, because it is the one answer
// that must not be mistaken for an ordinary end of stream: a caller that reconnected and
// carried on would be applying changes to a copy that is already wrong, forever, with
// nothing left by which to notice.
//
// It reports syscall.ESTALE to errors.Is, which is what it is: the position the caller
// holds no longer names anything in this log.
type RebuildError struct {
	// Reason says which dimension pushed the caller out, because the three call for
	// different things. RebuildIncarnation is a log that is not the one the caller was
	// watching; RebuildAge is a caller that was away too long; RebuildVolume is a
	// namespace changing faster than the log was configured to hold, which is a setting
	// to revisit rather than anything the caller did.
	Reason RebuildReason
}

func (e *RebuildError) Error() string {
	return fmt.Sprintf("the replica must be built again: %s", e.detail())
}

func (e *RebuildError) detail() string {
	switch e.Reason {
	case RebuildIncarnation:
		return "this is not the log the position came from"
	case RebuildAge:
		return "the changes since that position were discarded for being old"
	case RebuildVolume:
		return "the changes since that position were discarded for being too many"
	default:
		return fmt.Sprintf("the server gave the reason %q", e.Reason)
	}
}

func (e *RebuildError) Unwrap() error { return syscall.ESTALE }

// ErrServerStopping ends a change stream because the server said it was going away, rather
// than because anything happened to the namespace or to the connection.
//
// It is worth telling apart from every other way a stream can end. A replica given this has
// lost nothing: its position is still good, the log outlives the process that was serving
// it, and the thing to do is reconnect with Resubscribe and keep the copy it has. A stream
// that merely stopped says none of that — it is equally what a server that vanished looks
// like — so being told is the difference between coming back with a position and coming
// back with nothing.
//
// It reports syscall.EAGAIN to errors.Is: there is nothing wrong here that trying again
// does not settle.
var ErrServerStopping = &stoppingError{}

type stoppingError struct{}

func (e *stoppingError) Error() string {
	return "the server is stopping, and ended the change stream rather than letting it break"
}

func (e *stoppingError) Unwrap() error { return syscall.EAGAIN }

// Subscribe watches the namespace from now on.
//
// The stream begins at the log's current tail: nothing older is delivered, and every
// change recorded after that point is. This is what a replica about to take a snapshot
// asks for — the snapshot brings the tree as it stands, and the stream brings what happens
// to it from then on.
//
// The stream lives as long as ctx does. Nothing about it requires the caller to keep up
// with the network: Subscription.Next hands over one change at a time and blocks in
// between, so a caller that needs to buffer runs it in a goroutine and buffers as much as
// it wants to. What this package does not do is buffer on the caller's behalf, because how
// much to hold, and what to do when that much is not enough, are the caller's to decide.
func (s *Storage) Subscribe(ctx context.Context) (*Subscription, error) {
	return s.subscribe(ctx, Request{Op: OpSubscribe})
}

// Resubscribe continues watching the namespace from a position already applied.
//
// The pair is offered rather than the position alone, and the incarnation is what makes
// the answer trustworthy: a log that lost its history would otherwise be asked to carry on
// from a position it has never heard of, and the reasonable-looking reply — "that is
// inside my window, you are caught up" — loses every change in between and leaves nothing
// behind by which to tell.
//
// A log that cannot carry on says so with a *RebuildError naming which dimension the
// caller fell out of. That is an error rather than a subscription that quietly starts
// somewhere else.
func (s *Storage) Resubscribe(ctx context.Context, incarnation metastore.Incarnation, at metastore.Position) (*Subscription, error) {
	return s.subscribe(ctx, Request{Op: OpResubscribe, Incarnation: incarnation, Position: at})
}

func (s *Storage) subscribe(ctx context.Context, req Request) (*Subscription, error) {
	stream, err := s.dialStream(ctx, req)
	if err != nil {
		return nil, err
	}
	var start StreamStart
	if err := stream.frames.decode(eventStart, &start); err != nil {
		stream.close()
		return nil, unreachable(req, err)
	}
	if start.Rebuild != "" {
		stream.close()
		return nil, &RebuildError{Reason: start.Rebuild}
	}
	return &Subscription{
		stream:      stream,
		incarnation: metastore.Incarnation(start.Incarnation),
		at:          metastore.Position(*start.Position),
		tail:        metastore.Position(*start.Tail),
	}, nil
}

// Subscription is an open stream of changes to one namespace.
type Subscription struct {
	stream *stream

	incarnation metastore.Incarnation
	at          metastore.Position
	tail        metastore.Position
}

// Incarnation names the run of history this stream belongs to. A caller records it beside
// the positions it applies, and offers both back to Resubscribe.
func (sub *Subscription) Incarnation() metastore.Incarnation { return sub.incarnation }

// Position is where the stream begins. Everything up to and including it is the caller's
// already, and every change Next returns is later than it.
func (sub *Subscription) Position() metastore.Position { return sub.at }

// CaughtUp reports that nothing had been missed, so no change is replayed before the live
// ones begin. A stream opened by Subscribe is always caught up: it begins at the tail.
//
// It is Position and Tail compared rather than anything the server sent beside them. Derived
// here, it cannot disagree with them; sent, it could, and a frame saying both that nothing
// was missed and that something is still to come calls for opposite treatment of the copy.
func (sub *Subscription) CaughtUp() bool { return sub.at == sub.tail }

// Tail is how far the log had reached when the stream began. Everything between Position and
// it is replayed before the live changes, so a caller that was behind knows from it when it
// has stopped being behind — which is the moment its copy is worth answering from again, and
// not one change earlier.
func (sub *Subscription) Tail() metastore.Position { return sub.tail }

// Next returns the next change, blocking until one is recorded.
//
// Every error it reports ends the stream, and the same one is reported to every call
// afterwards. A stream that simply stopped is one of them: a change stream has no natural
// end, so reaching the end of one means the connection went rather than that the namespace
// has settled. Telling those apart is the whole difference between a replica that knows it
// is stale and one that does not.
//
// A *RebuildError here means the caller fell out of the log's window while it was watching
// — too slow, or a namespace changing faster than the log holds — and what it has applied
// so far is no longer a copy of anything. ErrServerStopping is the opposite of that: the
// server went away on purpose, nothing has been lost, and the position this subscription
// reached is still the one to come back with.
func (sub *Subscription) Next() (metastore.Change, error) {
	if sub.stream.failed != nil {
		return metastore.Change{}, sub.stream.failed
	}
	event, data, err := sub.stream.frames.next()
	if err != nil {
		return metastore.Change{}, sub.stream.fail(errorEndingTheStream(sub.stream.req, err,
			"a change stream ends only when the connection does"))
	}
	switch event {
	case eventChange:
		var change Change
		if err := decodeFrame(event, data, &change); err != nil {
			return metastore.Change{}, sub.stream.fail(unreachable(sub.stream.req, err))
		}
		return change.Metastore(), nil
	case eventStart:
		// The stream restating what it can do for the caller, part way through, is how it
		// says the caller has fallen out of the window since it attached.
		var start StreamStart
		if err := decodeFrame(event, data, &start); err != nil {
			return metastore.Change{}, sub.stream.fail(unreachable(sub.stream.req, err))
		}
		if start.Rebuild == "" {
			return metastore.Change{}, sub.stream.fail(unreachable(sub.stream.req,
				fmt.Errorf("the stream began again at position %d, and a stream begins once", *start.Position)))
		}
		return metastore.Change{}, sub.stream.fail(&RebuildError{Reason: start.Rebuild})
	case eventGone:
		return metastore.Change{}, sub.stream.fail(ErrServerStopping)
	case eventFault:
		return metastore.Change{}, sub.stream.fail(unreachable(sub.stream.req, faultOf(data)))
	default:
		return metastore.Change{}, sub.stream.fail(unreachable(sub.stream.req,
			fmt.Errorf("a %s frame arrived on a change stream", event)))
	}
}

// Close ends the subscription and releases the connection it holds. A Next blocked waiting
// for a change returns once it has.
func (sub *Subscription) Close() error { return sub.stream.close() }

// Snapshot pulls one consistent picture of the whole tree.
//
// Every row it yields reflects the same instant, and no change later than Position is in
// any of them — so a caller seeds every node it holds at that position, and applies from
// its subscription only what came after.
//
// The picture cannot be resumed if the stream breaks: a consistent cut that is gone is
// gone, and a caller that loses one takes another. That is affordable precisely because
// the subscription was opened first and is unaffected by how long this takes.
//
// The server holds a resource for as long as this is open and bounds both how long that
// may be and how many it will hold at once. A namespace already holding as many as it will
// answers EAGAIN, which is worth retrying; the ENOSYS a namespace with no log answers is
// not.
func (s *Storage) Snapshot(ctx context.Context) (*Snapshot, error) {
	req := Request{Op: OpSnapshot}
	stream, err := s.dialStream(ctx, req)
	if err != nil {
		return nil, err
	}
	var open SnapshotOpen
	if err := stream.frames.decode(eventOpen, &open); err != nil {
		stream.close()
		return nil, unreachable(req, err)
	}
	return &Snapshot{stream: stream, at: metastore.Position(*open.Position)}, nil
}

// Snapshot is one consistent picture of a tree, delivered in pages.
type Snapshot struct {
	stream *stream
	at     metastore.Position
	done   bool
}

// Position is the instant the picture was taken at, expressed as a position in the log. It
// is an exact cut rather than a lower bound: nothing later than it is in the rows, and
// nothing earlier is missing from them.
func (snap *Snapshot) Position() metastore.Position { return snap.at }

// Next returns the next page of rows, and io.EOF once the picture is complete.
//
// io.EOF is reported only after the server has said the picture is whole. A stream that
// stopped without saying so is an error instead — the distinction the whole delivery turns
// on, because a snapshot cut short is a tree with some of its nodes missing, and a replica
// built from one reports "no such file" for every one of them.
//
// ErrServerStopping is the one failure worth telling from the rest: the server said it was
// going away, so another server will have the same picture to give, and taking one from it
// is all this needs.
func (snap *Snapshot) Next() ([]metastore.Row, error) {
	if snap.stream.failed != nil {
		return nil, snap.stream.failed
	}
	if snap.done {
		return nil, io.EOF
	}
	event, data, err := snap.stream.frames.next()
	if err != nil {
		return nil, snap.stream.fail(errorEndingTheStream(snap.stream.req, err,
			"the picture stopped before the server said it was whole"))
	}
	switch event {
	case eventRows:
		var page SnapshotPage
		if err := decodeFrame(event, data, &page); err != nil {
			return nil, snap.stream.fail(unreachable(snap.stream.req, err))
		}
		rows := make([]metastore.Row, 0, len(page.Rows))
		for _, row := range page.Rows {
			rows = append(rows, row.Metastore())
		}
		return rows, nil
	case eventDone:
		snap.done = true
		return nil, io.EOF
	case eventGone:
		// A picture cannot be resumed, so there is nothing here to carry on from — but
		// knowing the server went away on purpose is still worth more than a stream that
		// stopped: taking another from whatever comes up next is the whole answer, and
		// EAGAIN says so where a cut-short picture would say the outcome is unknown.
		return nil, snap.stream.fail(ErrServerStopping)
	case eventFault:
		return nil, snap.stream.fail(unreachable(snap.stream.req, faultOf(data)))
	default:
		return nil, snap.stream.fail(unreachable(snap.stream.req,
			fmt.Errorf("a %s frame arrived on a snapshot stream", event)))
	}
}

// Close releases what the picture holds, here and on the server. A caller that stops part
// way through still calls it, and calling it is what lets the server let go of the read it
// is holding open rather than wait out its deadline.
func (snap *Snapshot) Close() error { return snap.stream.close() }

// stream is one open streaming response and everything needed to end it.
type stream struct {
	req    Request
	frames *frameReader
	body   io.ReadCloser
	cancel context.CancelFunc

	// failed is what ended the stream, and is reported to every call after the first.
	// Reading a stream that has already failed would otherwise report a second, unrelated
	// failure — most likely "read on a closed body" — in place of the one that matters.
	failed error
}

// dialStream performs a streaming request and returns its frames.
//
// Everything a plain request checks is checked here too, and for the same reasons: an
// answer that does not carry this protocol's mark did not come from a server speaking it,
// and a status nobody promised means the outcome is unknown. The one addition is the media
// type, because a stream is committed to before its first frame and an answer that is not
// this framing would otherwise be discovered one unparseable frame at a time.
func (s *Storage) dialStream(ctx context.Context, req Request) (opened *stream, err error) {
	u, err := req.URL(s.base)
	if err != nil {
		return nil, unreachable(req, err)
	}
	// Cancelled by close, which is what returns a read waiting on a stream that has
	// nothing on it, and released here on every path that does not hand the stream over.
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if opened == nil {
			cancel()
		}
	}()

	httpReq, err := http.NewRequestWithContext(ctx, req.Method(), u.String(), nil)
	if err != nil {
		return nil, unreachable(req, err)
	}
	httpReq.Header.Set("Accept", contentEventStream)

	// The caller's client, with its timeout dropped. http.Client.Timeout bounds the whole
	// exchange, the reading of the body included, which is the right bound for an operation
	// and the wrong one for a stream: a change stream is meant to stay open with nothing on
	// it, so the only thing that bound could ever report is that the namespace was quiet. A
	// subscription severed on a timer would also be a subscription whose replica goes
	// unusable on that timer, which is the interval R-CON-2 exists to keep out. Everything
	// else the caller configured is kept, and how long the far side has to answer at all
	// remains whatever its transport enforces.
	streaming := *s.http
	streaming.Timeout = 0

	resp, err := streaming.Do(httpReq)
	if err != nil {
		return nil, unreachable(req, err)
	}
	defer func() {
		if opened == nil {
			resp.Body.Close()
		}
	}()

	if got := resp.Header.Get(HeaderProtocol); got != Version {
		return nil, unreachable(req, fmt.Errorf("the response is marked %q, not %q", got, Version))
	}
	switch resp.StatusCode {
	case http.StatusOK:
	case StatusStorageError:
		body, err := readWhole(resp)
		if err != nil {
			return nil, unreachable(req, err)
		}
		return nil, s.storageError(req, body)
	default:
		return nil, unreachable(req, fmt.Errorf("the server answered %s", resp.Status))
	}
	if got := resp.Header.Get("Content-Type"); got != contentEventStream {
		return nil, unreachable(req, fmt.Errorf("the stream is typed %q, not %q", got, contentEventStream))
	}

	// The stream is bounded by how long it may say nothing at all, which is the only bound
	// left on it: the caller's timeout was dropped above because it bounds the whole
	// exchange, and a stream is meant to stay open. Cancelling is what returns a read that
	// is waiting, so it is what the bound acts through.
	opened = &stream{req: req, frames: newFrameReader(resp.Body, s.silence, cancel), body: resp.Body, cancel: cancel}
	return opened, nil
}

func (s *stream) fail(err error) error {
	s.failed = err
	return err
}

func (s *stream) close() error {
	// Cancelled first: closing a body somebody is reading is what the cancellation makes
	// safe, and it is the cancellation that returns the reader.
	s.cancel()
	return s.body.Close()
}

// errorEndingTheStream explains a stream that stopped, given what an end of it would have
// meant. An end at a frame boundary and an end part way through a frame are the same
// verdict here and differ only in what can be said about it afterwards.
func errorEndingTheStream(req Request, cause error, ended string) error {
	if errors.Is(cause, io.EOF) {
		return unreachable(req, fmt.Errorf("the stream ended: %s", ended))
	}
	return unreachable(req, cause)
}
