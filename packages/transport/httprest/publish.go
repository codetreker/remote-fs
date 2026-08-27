package httprest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// Limits are the bounds a handler puts on the replication endpoints.
//
// They exist because both endpoints accumulate something a request-and-response endpoint
// does not: a subscription holds a connection for as long as a replica watches, and a
// snapshot holds a resource inside the store — a read transaction, a version, a reference
// — for as long as its rows take to cross the wire. The second is the one R-INT-3 is
// about: how long it is held is decided by the network rather than by anything here, and
// the worst moment for it is the one where every replica rebuilds at once, which is what
// a server restart produces.
type Limits struct {
	// Snapshots is how many snapshots may be open at once. A request arriving when they
	// all are is refused with EAGAIN rather than queued, because a replica subscribes
	// before it asks for one — so retrying costs it nothing and loses it nothing, and the
	// server holding the request open would be holding exactly what this bounds.
	Snapshots int

	// SnapshotDeadline is how long one snapshot may take to deliver, after which it is
	// abandoned and what it held is released. A snapshot cannot be resumed — a consistent
	// picture that is gone is gone — so a replica that runs into this starts over.
	//
	// It bounds the whole delivery and has nothing to do with the bound a reader keeps on a
	// quiet stream, which Keepalive answers. Reading the two as a pair is a mistake worth
	// naming, because they look like one: this being the larger figure suggests a slow
	// picture is tolerated, when what decides that is whether anything is being said
	// meanwhile. A picture may take all of this as long as it keeps speaking.
	SnapshotDeadline time.Duration

	// SnapshotPage is how many rows travel in one frame. The snapshot is the largest bulk
	// transfer this system has, and sending it whole would occupy the connection for its
	// whole length.
	SnapshotPage int

	// EventPage is how many changes one read of the log may return while a subscription
	// catches up.
	EventPage int

	// Keepalive is how often a stream with nothing to say says so — both kinds, because
	// both can be quiet for reasons that are nobody's fault. A namespace nobody is writing
	// to produces no events, and a store working through a large tree produces no page for
	// as long as it takes.
	//
	// It is what makes a live stream distinguishable from a dead one. Neither of those
	// silences looks any different from a connection a firewall dropped, a machine that
	// vanished, or a partition — the reader sees the same thing in all of them, which is
	// nothing at all. A replica that could not tell them apart would go on answering from a
	// copy it can no longer justify, for as long as the mistake lasted, which is what
	// R-ERR-1 and R-ERR-2 forbid above everything else.
	//
	// It is paired with the bound the reading end keeps: DefaultSilence is three times this
	// figure. The two are configured separately, so raising this above what a client allows
	// severs every one of that client's streams on a timer — which is loud rather than
	// silent, and is the direction to err in. SnapshotDeadline is not part of that pairing
	// and must not be read as though it were; what it bounds is said there.
	Keepalive time.Duration
}

// DefaultLimits are the bounds a handler uses when it is not given any.
//
// The figures are chosen rather than measured, which the design records as a deliberate
// deferral: what a snapshot costs at scale is unknown until there is a tree large enough
// to measure, and the first thing an operator meets is likely to be one of these.
func DefaultLimits() Limits {
	return Limits{
		Snapshots:        8,
		SnapshotDeadline: 5 * time.Minute,
		SnapshotPage:     1024,
		EventPage:        256,
		Keepalive:        10 * time.Second,
	}
}

func (l Limits) check() error {
	for _, bound := range []struct {
		name  string
		value int64
	}{
		{"Snapshots", int64(l.Snapshots)},
		{"SnapshotDeadline", int64(l.SnapshotDeadline)},
		{"SnapshotPage", int64(l.SnapshotPage)},
		{"EventPage", int64(l.EventPage)},
		{"Keepalive", int64(l.Keepalive)},
	} {
		if bound.value <= 0 {
			return fmt.Errorf("httprest: Limits.%s is %d, and every bound has to leave room for one of whatever it bounds", bound.name, bound.value)
		}
	}
	return nil
}

// publisher wakes the open subscriptions when this server has changed the namespace.
//
// It carries no changes itself, and that is the whole design. Each subscription reads
// metastore.Log.Since from where it last delivered, so catching up and keeping up are one
// code path and cannot disagree about what a change is or what order changes happened in.
// The alternative — the handler passing the tree operation it has just performed to the
// subscriptions — would be a second producer of events, one with no transaction behind it
// and no way to be correct about a rename, and the two would drift apart exactly where
// they were hardest to compare.
//
// Waking is a signal rather than a delivery, so a replica that is slow to read holds up
// nothing but itself, and no subscription waits for an interval to elapse before it sees a
// change (R-CON-2).
//
// What this arrangement assumes is that this process is the only one writing the
// namespace. A second server over the same database would record changes into the same log
// and reach none of these subscriptions, so its changes would sit there until something
// this process did woke them. One server per workspace is the assumption for now, and it
// is stated here rather than discovered later as a namespace that updates only when
// somebody else happens to write to it.
type publisher struct {
	mu    sync.Mutex
	wakes map[chan struct{}]struct{}
}

func newPublisher() *publisher {
	return &publisher{wakes: map[chan struct{}]struct{}{}}
}

// wake tells every open subscription to read the log again.
func (p *publisher) wake() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for w := range p.wakes {
		select {
		case w <- struct{}{}:
		default:
			// Already awake. The signal carries nothing, so a second one would say
			// nothing the first has not: what a subscription reads is the log.
		}
	}
}

// attach returns a channel that wake pokes, and the function that stops it.
func (p *publisher) attach() (<-chan struct{}, func()) {
	woken := make(chan struct{}, 1)
	p.mu.Lock()
	p.wakes[woken] = struct{}{}
	p.mu.Unlock()
	return woken, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(p.wakes, woken)
	}
}

// resumeFrom is where a replica asked a change stream to begin. A nil one asks for the
// log's tail and nothing older, which is what a replica about to take a snapshot wants:
// the snapshot brings everything up to that point, and the stream brings what follows.
type resumeFrom struct {
	incarnation metastore.Incarnation
	position    metastore.Position
}

// serveEvents answers a subscription with a stream of changes.
func (h *Handler) serveEvents(w http.ResponseWriter, r *http.Request, from *resumeFrom) {
	if h.publisher == nil {
		refuseUnreplicable(w)
		return
	}
	ctx := r.Context()

	// Attached before the log is read, and before a single frame is written. A change
	// recorded in between would otherwise wake nothing, and this subscription would sit
	// still holding a stale position until some later change happened to wake it — a
	// failure with no interval to it at all, and so worse than the interval R-CON-2
	// forbids.
	woken, detach := h.publisher.attach()
	defer detach()

	incarnation, err := h.log.Incarnation(ctx)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if incarnation == "" {
		// A log with no identity matches every position any replica ever held, so the one
		// answer it can give a returning replica is the answer that loses everything:
		// "that is within my window, you are caught up".
		writeStorageError(w, errors.New("the log reports no incarnation, so nothing could ever be resumed against it"))
		return
	}

	start, at, err := h.startOf(ctx, incarnation, from)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// A stream that cannot be given a write deadline cannot be ended from outside itself,
	// and this one has to be: it will spend unbounded time inside a write to a replica that
	// has stopped reading, and a shutdown waiting on that waits for as long as that replica
	// cares to say nothing. Refused rather than served unbounded, exactly as a picture is.
	if err := boundedWrites(w); err != nil {
		writeStorageError(w, fmt.Errorf("this server cannot bound a write to a stream, so a stream could not be ended when it stops: %w", err))
		return
	}

	// Past here the response is a success and a stream, so nothing below can report a
	// status and everything that goes wrong travels as a fault frame.
	out, err := openStream(w)
	if err != nil {
		return
	}
	defer out.endWritesWhen(h.stopping)()
	if err := out.send(eventStart, start); err != nil {
		return
	}
	if start.Rebuild != "" {
		return
	}
	if err := h.publish(ctx, out, at, woken); err != nil && !h.endedByStop(err) {
		out.fault(err)
	}
}

// startOf decides what a stream can offer a replica and where it will begin.
func (h *Handler) startOf(ctx context.Context, incarnation metastore.Incarnation, from *resumeFrom) (StreamStart, metastore.Position, error) {
	if from != nil && from.incarnation != incarnation {
		return StreamStart{Rebuild: RebuildIncarnation}, 0, nil
	}

	// A limit of zero asks the log what it holds without asking for any of it. What is
	// wanted here is the retention, and Since is the only thing that reports it; the changes
	// themselves are read by publish, which is the loop that will go on reading them.
	at := metastore.Position(0)
	if from != nil {
		at = from.position
	}
	_, retention, err := h.log.Since(ctx, at, 0)
	if err != nil {
		return StreamStart{}, 0, err
	}

	if from == nil {
		return startAt(incarnation, retention.Tail, retention.Tail), retention.Tail, nil
	}
	if at > retention.Tail {
		// The incarnation matched, so this is the log the replica was watching, and yet it
		// holds a position that log has never reached. Either the replica invented it or
		// the log lost entries without saying so — and rebuilding would paper over the
		// second, which is precisely the failure the log's own startup reconciliation
		// exists to make loud.
		return StreamStart{}, 0, fmt.Errorf("position %d is past the log's tail at %d: %w", at, retention.Tail, syscall.EINVAL)
	}
	if rebuild := rebuildFor(at, retention); rebuild != "" {
		return StreamStart{Rebuild: rebuild}, 0, nil
	}
	return startAt(incarnation, at, retention.Tail), at, nil
}

// startAt says where a stream begins and how far the log had got, which between them say
// whether anything is about to be replayed and when it will have been.
func startAt(incarnation metastore.Incarnation, at, tail metastore.Position) StreamStart {
	position, reached := int64(at), int64(tail)
	return StreamStart{Incarnation: string(incarnation), Position: &position, Tail: &reached}
}

// rebuildFor is the rebuild a replica sitting at some position calls for, and is empty when
// the stream can simply go on.
//
// It asks what the log discarded rather than what survived it, and the difference between
// those two is the whole reason Retention carries both. A replica has missed nothing exactly
// when it has already seen everything the log threw away; how far its position sits below
// the oldest surviving entry is not the same question, because positions are dense in no
// particular way. A store numbering every namespace in one database from a single sequence
// leaves each namespace's positions spread by however much its neighbours were written to in
// between, so a replica that had missed nothing would be sent off to walk the whole tree
// again — and one that had applied nothing at all, sitting at position zero, would be sent
// away by every namespace whose first change is not position 1.
//
// A replica at the tail needs no case of its own. Nothing can have been discarded above the
// newest position ever recorded, so being at the tail already means being at or past
// whatever was thrown away.
func rebuildFor(at metastore.Position, retention metastore.Retention) RebuildReason {
	switch {
	case at >= retention.TrimmedThrough:
		return ""
	case retention.TrimmedByAge:
		return RebuildAge
	default:
		return RebuildVolume
	}
}

// publish delivers what the log holds after at, and then every change recorded afterwards,
// for as long as the replica watches.
//
// No change waits for an interval to come round. The inner loop reads the log until it has
// nothing more, and the outer one blocks until this server changes the namespace or the
// replica goes away.
//
// A subscriber too slow to keep up blocks its own write, and a subscriber that has stopped
// reading altogether blocks it for good — which is not a rare state but the designed one, as
// a replica does not read its change stream while it is filling a picture. The write carries
// no deadline of its own, because a stream is meant to stay open with nothing on it and any
// deadline would sever the healthy case. What ends it is the server stopping, and that
// reaches a write in flight rather than only the moment between two of them.
//
// The one thing that does happen on a timer is the keepalive, and it carries nothing: a
// stream that has said nothing for a while is indistinguishable from a stream that is no
// longer arriving, and the whole worth of a replica rests on being able to tell those apart.
//
// A stream also ends when the server is stopped, and it says so rather than stopping. A
// replica told that keeps what it has and reconnects at the position it holds; one that
// merely saw its connection end cannot tell a server that went away on purpose from one
// that vanished.
func (h *Handler) publish(ctx context.Context, out *frameWriter, at metastore.Position, woken <-chan struct{}) error {
	keepalive := time.NewTicker(h.limits.Keepalive)
	defer keepalive.Stop()

	for {
		for {
			changes, retention, err := h.log.Since(ctx, at, h.limits.EventPage)
			if err != nil {
				return err
			}
			// Falling out of the window while attached is the same answer as arriving
			// having already fallen out of it, and it is given for the same reason: what
			// happened between here and the log's oldest entry is gone, so delivering
			// what follows would leave the replica silently wrong about everything in
			// between, permanently and with nothing left to notice it by.
			if rebuild := rebuildFor(at, retention); rebuild != "" {
				return out.send(eventStart, StreamStart{Rebuild: rebuild})
			}
			if len(changes) == 0 {
				break
			}
			for _, change := range changes {
				wire, err := ChangeOf(change)
				if err != nil {
					return err
				}
				if err := out.send(eventChange, wire); err != nil {
					return err
				}
				at = change.Position
			}
			// A backlog is left part way through when the server is stopping. What the
			// replica has been given stands and it resumes from there, whereas draining the
			// rest first would hold the shutdown open for as long as the backlog is.
			if h.stopped() {
				return out.send(eventGone, struct{}{})
			}
		}
		select {
		case <-woken:
		case <-h.stopping:
			return out.send(eventGone, struct{}{})
		case <-keepalive.C:
			// Failing to write it is how this side learns that nobody is reading any more,
			// which is worth as much as the keepalive itself: without it, a replica that
			// vanished without closing its connection holds a goroutine and a connection
			// here until some later change happens to be published.
			if err := out.alive(); err != nil {
				return err
			}
		case <-ctx.Done():
			// The replica went away. There is nobody left to tell, so there is nothing to
			// say.
			return nil
		}
	}
}

// serveSnapshot answers with one consistent picture of the tree, in pages.
func (h *Handler) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	if h.log == nil {
		refuseUnreplicable(w)
		return
	}
	select {
	case h.snapshots <- struct{}{}:
		defer func() { <-h.snapshots }()
	default:
		writeStorageError(w, fmt.Errorf("%d snapshots are already open, which is as many as this server holds at once: %w",
			h.limits.Snapshots, syscall.EAGAIN))
		return
	}

	// The deadline has to reach the write as well as the reads. A replica that stops
	// reading without closing its connection stalls the write, and a context deadline does
	// not interrupt one — so the picture, and whatever the store is holding for it, would
	// stay open for as long as that replica cared to say nothing. A server that cannot
	// bound its writes is refused the operation rather than given an unbounded one.
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(h.limits.SnapshotDeadline)); err != nil {
		writeStorageError(w, fmt.Errorf("this server cannot bound how long a snapshot may take to send: %w", err))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.limits.SnapshotDeadline)
	defer cancel()

	snap, at, err := h.log.Snapshot(ctx)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	out, err := openStream(w)
	if err != nil {
		snap.Close()
		return
	}
	defer out.endWritesWhen(h.stopping)()

	position := int64(at)
	if err := out.send(eventOpen, SnapshotOpen{Position: &position}); err != nil {
		snap.Close()
		return
	}

	stopping, err := h.pages(ctx, out, snap)
	// Closing belongs to the delivery rather than to cleanup after it. What it releases is
	// held inside the store, and a failure to release it is not something to find out about
	// later from a database that has quietly stopped reclaiming space. A picture that was
	// read whole but could not be closed is reported as a failure for the same reason:
	// this side cannot say what state it was left in, and a replica that starts over pays
	// one retry, where one built on a picture that was not what it claimed pays forever.
	if closeErr := snap.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		if !h.endedByStop(err) {
			out.fault(err)
		}
		return
	}
	if stopping {
		// The replica has already been told this server is going away. A picture it will
		// never receive the rest of must not also be announced as whole.
		return
	}
	// The last frame, and the only one that makes the picture usable. A failure to write it
	// leaves the replica with a stream that stopped, which it treats as one cut short — the
	// right answer, and the only one left once there is nothing further to write.
	out.send(eventDone, struct{}{})
}

// pages streams the whole picture, and reports whether it ended because this server is
// stopping rather than because the picture was complete.
//
// The rows are produced beside the writing rather than in front of it, and that is what lets
// the stream say something while the store is working. The reading end bounds how long a
// stream may go without a word before it stops believing in it, and it arms that bound on
// every stream — it cannot see the difference between a store taking its time over a page
// and a connection that is no longer there, because both look like nothing arriving. A page
// produced in front of the writing would leave the stream silent for exactly as long as the
// store took, so a tree large enough to be worth replicating would be judged dead on a timer
// and every retry would open another read for the store to hold.
func (h *Handler) pages(ctx context.Context, out *frameWriter, snap metastore.Snap) (stopping bool, err error) {
	// A context of this loop's own, so that leaving early stops the production: the picture
	// is closed as soon as this returns, and closing one while a read is still inside it is
	// not something the contract allows.
	producing, stopProducing := context.WithCancel(ctx)
	type produced struct {
		rows []metastore.Row
		done bool
		err  error
	}
	ready := make(chan produced)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			rows, done, err := snap.Next(producing, h.limits.SnapshotPage)
			select {
			case ready <- produced{rows, done, err}:
			case <-producing.Done():
				return
			}
			if done || err != nil {
				return
			}
		}
	}()
	defer func() {
		stopProducing()
		<-finished
	}()

	keepalive := time.NewTicker(h.limits.Keepalive)
	defer keepalive.Stop()

	for {
		select {
		case page := <-ready:
			if page.err != nil {
				return false, page.err
			}
			if len(page.rows) > 0 {
				rows := SnapshotPage{Rows: make([]Row, 0, len(page.rows))}
				for _, row := range page.rows {
					rows.Rows = append(rows.Rows, RowOf(row))
				}
				if err := out.send(eventRows, rows); err != nil {
					return false, err
				}
			}
			if page.done {
				return false, nil
			}
			if len(page.rows) == 0 {
				// A picture that is not complete and yields nothing would have this loop
				// ask forever, holding open the very resource the bounds above exist to
				// release.
				return false, errors.New("the snapshot yielded no rows and reported itself incomplete")
			}
		case <-keepalive.C:
			// Failing to write it is how this side learns that nobody is reading any more,
			// which is worth as much here as it is on a change stream: a picture nobody is
			// receiving is a read transaction held open for nothing.
			if err := out.alive(); err != nil {
				return false, err
			}
		case <-h.stopping:
			// A picture cannot be resumed, so there is nothing to hand over — but saying so
			// is what lets the replica take another from whatever server comes up next,
			// rather than holding this connection open through a shutdown that is waiting
			// for it to become idle.
			return true, out.send(eventGone, struct{}{})
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

// boundedWrites reports whether writes to w can be given a deadline, which is what lets a
// stream be ended from outside the write it is parked in.
//
// A zero time sets no deadline, so this asks the question without answering it: the streams
// below set what they need afterwards.
func boundedWrites(w http.ResponseWriter) error {
	return http.NewResponseController(w).SetWriteDeadline(time.Time{})
}

// endedByStop reports whether a stream failed because this server cut it short on its way
// out, rather than because of anything about the stream.
//
// A write ended that way has a deadline behind it that is still in the past, so a fault frame
// explaining it could not be written either — and the explanation would be wrong in any case.
// Nothing else may be swallowed here: the error has to be the deadline, and the server has to
// be stopping, before either is read as the other.
func (h *Handler) endedByStop(err error) bool {
	return h.stopped() && errors.Is(err, os.ErrDeadlineExceeded)
}

// refuseUnreplicable answers a namespace that has no log at all.
//
// ENOSYS, under its own name, exactly as a namespace with no allowance answers a question
// about its space: it is a standing property of this namespace rather than a gap in the
// protocol or a failure worth retrying. What it must not be answered with is a stream that
// carries nothing and a snapshot of no rows, which is a namespace that exists, is empty,
// and never changes — an answer a replica would believe.
func refuseUnreplicable(w http.ResponseWriter) {
	writeStorageError(w, fmt.Errorf("this namespace keeps no change log, so it cannot be replicated: %w", syscall.ENOSYS))
}
