package httprest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
)

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
	mu               sync.Mutex
	maxSubscriptions int
	wakes            map[chan struct{}]struct{}
}

func newPublisher(maxSubscriptions int) *publisher {
	return &publisher{maxSubscriptions: maxSubscriptions, wakes: map[chan struct{}]struct{}{}}
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
func (p *publisher) attach() (<-chan struct{}, func(), error) {
	p.mu.Lock()
	if len(p.wakes) >= p.maxSubscriptions {
		p.mu.Unlock()
		return nil, nil, fmt.Errorf("the change-stream limit of %d is full: %w", p.maxSubscriptions, syscall.EAGAIN)
	}
	woken := make(chan struct{}, 1)
	p.wakes[woken] = struct{}{}
	p.mu.Unlock()
	return woken, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(p.wakes, woken)
	}, nil
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
		h.refuseUnreplicable(w)
		return
	}
	ctx := r.Context()

	// Attached before the log is read, and before a single frame is written. A change
	// recorded in between would otherwise wake nothing, and this subscription would sit
	// still holding a stale position until some later change happened to wake it — a
	// failure with no interval to it at all, and so worse than the interval R-CON-2
	// forbids.
	woken, detach, err := h.publisher.attach()
	if err != nil {
		h.writeOperationError(w, err)
		return
	}
	defer detach()

	incarnation, err := h.log.Incarnation(ctx, h.maxIncarnationBytes)
	if err != nil {
		h.writeOperationError(w, err)
		return
	}
	if incarnation == "" {
		// A log with no identity matches every position any replica ever held, so the one
		// answer it can give a returning replica is the answer that loses everything:
		// "that is within my window, you are caught up".
		h.writeOperationError(w, errors.New("the log reports no incarnation, so nothing could ever be resumed against it"))
		return
	}

	start, at, err := h.startOf(ctx, incarnation, from)
	if err != nil {
		h.writeOperationError(w, err)
		return
	}
	encodedStart, err := marshalStartFrame(start, h.maxFrameBytes, h.maxIncarnationBytes)
	if err != nil {
		h.writeOperationError(w, err)
		return
	}

	// A stream that cannot be given a write deadline cannot be ended from outside itself,
	// and this one has to be: it will spend unbounded time inside a write to a replica that
	// has stopped reading, and a shutdown waiting on that waits for as long as that replica
	// cares to say nothing. Refused rather than served unbounded, exactly as a picture is.
	if err := boundedWrites(w); err != nil {
		h.writeOperationError(w, fmt.Errorf("this server cannot bound a write to a stream, so a stream could not be ended when it stops: %w", err))
		return
	}

	// Past here the response is a success and a stream, so nothing below can report a
	// status and everything that goes wrong travels as a fault frame.
	out, err := openStream(w, h.maxFrameBytes)
	if err != nil {
		return
	}
	defer out.endWritesWhen(h.stopping)()
	if err := out.sendEncoded(eventStart, encodedStart); err != nil {
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
	result, err := newChangeFrameResult(h.maxFrameBytes)
	if err != nil {
		return StreamStart{}, 0, err
	}
	retention, err := h.log.Since(ctx, at, 0, result)
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
			result, err := newChangeFrameResult(h.maxFrameBytes)
			if err != nil {
				return err
			}
			retention, err := h.log.Since(ctx, at, h.limits.EventPage, result)
			if err != nil {
				return err
			}
			changes, err := result.Changes()
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
				if retention.Tail > at {
					return fmt.Errorf("the log has changes through position %d but returned none after %d: %w",
						retention.Tail, at, syscall.EIO)
				}
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
		case <-ctx.Done():
			return ctx.Err()
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
		}
	}
}

// serveSnapshot answers with one consistent picture of the tree, in pages.
func (h *Handler) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	if h.log == nil {
		h.refuseUnreplicable(w)
		return
	}
	releaseSnapshot, err := h.snapshots.acquireOperation(r.Context())
	if err != nil {
		if errors.Is(err, syscall.EAGAIN) {
			h.writeOperationError(w, fmt.Errorf("%d snapshots are already open, which is as many as this server holds at once: %w",
				h.limits.Snapshots, syscall.EAGAIN))
		} else {
			h.writeRequestBodyFault(w, fmt.Errorf("snapshot admission: %w", err))
		}
		return
	}
	defer releaseSnapshot()

	// The deadline has to reach the write as well as the reads. A replica that stops
	// reading without closing its connection stalls the write, and a context deadline does
	// not interrupt one — so the picture, and whatever the store is holding for it, would
	// stay open for as long as that replica cared to say nothing. A server that cannot
	// bound its writes is refused the operation rather than given an unbounded one.
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(h.limits.SnapshotDeadline)); err != nil {
		h.writeOperationError(w, fmt.Errorf("this server cannot bound how long a snapshot may take to send: %w", err))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.limits.SnapshotDeadline)
	defer cancel()

	snap, at, err := h.log.Snapshot(ctx)
	if err != nil {
		h.writeOperationError(w, err)
		return
	}

	out, err := openStream(w, h.maxFrameBytes)
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
	if closeErr := snap.Close(); closeErr != nil {
		err = errors.Join(err, closeErr)
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
		rows    []metastore.Row
		done    bool
		err     error
		release func()
	}
	ready := make(chan produced)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			release, err := h.acquireSnapshotFrame(producing)
			if err != nil {
				select {
				case ready <- produced{err: err}:
				case <-producing.Done():
				}
				return
			}
			result, err := newSnapshotFrameResult(h.maxFrameBytes)
			if err != nil {
				release()
				select {
				case ready <- produced{err: err}:
				case <-producing.Done():
				}
				return
			}
			done, err := snap.Next(producing, h.limits.SnapshotPage, result)
			rows, rowsErr := result.Rows()
			if err == nil {
				err = rowsErr
			}
			select {
			case ready <- produced{rows: rows, done: done, err: err, release: release}:
			case <-producing.Done():
				release()
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
				if page.release != nil {
					page.release()
				}
				return false, page.err
			}
			if len(page.rows) > 0 {
				rows := SnapshotPage{Rows: make([]Row, 0, len(page.rows))}
				for _, row := range page.rows {
					rows.Rows = append(rows.Rows, RowOf(row))
				}
				if err := out.send(eventRows, rows); err != nil {
					page.release()
					return false, err
				}
			}
			page.release()
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
func (h *Handler) refuseUnreplicable(w http.ResponseWriter) {
	h.writeOperationError(w, fmt.Errorf("this namespace keeps no change log, so it cannot be replicated: %w", syscall.ENOSYS))
}

func (h *Handler) acquireSnapshotFrame(ctx context.Context) (func(), error) {
	release, err := h.snapshotFrames.acquire(ctx, retainedFrameMultiplier*h.maxFrameBytes)
	if err != nil {
		return nil, fmt.Errorf("snapshot-frame admission failed: %w", err)
	}
	return release, nil
}

func marshalStartFrame(start StreamStart, maxFrameBytes, maxIncarnationBytes int64) ([]byte, error) {
	// JSON may expand each byte of an arbitrary string to a six-byte escape. Rejecting
	// from the source length keeps an adversarial Log implementation from making the
	// encoder allocate an oversized frame before the final encoded-length check.
	if int64(len(start.Incarnation)) > maxIncarnationBytes {
		return nil, fmt.Errorf("the log incarnation exceeds the handler's %d-byte identity bound: %w", maxIncarnationBytes, syscall.EFBIG)
	}
	return marshalFrame(eventStart, start, maxFrameBytes)
}

func newChangeFrameResult(maxFrameBytes int64) (*metastore.ChangeResult, error) {
	return metastore.NewChangeResult(maxFrameBytes, 0, func(_ int, meta metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
		if err := payloadLengthsFitFrame(maxFrameBytes, lengths.Name, lengths.FromName, lengths.Content); err != nil {
			return 0, err
		}
		if meta.From != nil {
			from := *meta.From
			meta.From = &from
		}
		if meta.Node != nil {
			node := *meta.Node
			node.Content = ""
			meta.Node = &node
		}
		wire, err := changeShapeOf(meta)
		if err != nil {
			return 0, err
		}
		encoded, err := json.Marshal(wire)
		if err != nil {
			return 0, fmt.Errorf("cannot size a change frame: %w", err)
		}
		payloadBytes := int64(len(encoded))
		for _, length := range []int64{lengths.Name, lengths.FromName, lengths.Content} {
			payloadBytes, err = addFrameBytes(payloadBytes, int64(base64.StdEncoding.EncodedLen(int(length))))
			if err != nil {
				return 0, err
			}
		}
		return encodedFrameBytes(eventChange, payloadBytes)
	})
}

func newSnapshotFrameResult(maxFrameBytes int64) (*metastore.RowResult, error) {
	empty, err := json.Marshal(SnapshotPage{Rows: []Row{}})
	if err != nil {
		return nil, fmt.Errorf("cannot size an empty snapshot frame: %w", err)
	}
	fixed, err := encodedFrameBytes(eventRows, int64(len(empty)))
	if err != nil {
		return nil, err
	}
	return metastore.NewRowResult(maxFrameBytes, fixed, func(index int, meta metastore.Row, lengths metastore.RowPayloadLengths) (int64, error) {
		if err := payloadLengthsFitFrame(maxFrameBytes, lengths.Name, lengths.Content); err != nil {
			return 0, err
		}
		meta.Node.Content = ""
		one, err := json.Marshal(SnapshotPage{Rows: []Row{RowOf(meta)}})
		if err != nil {
			return 0, fmt.Errorf("cannot size a snapshot row: %w", err)
		}
		charge := int64(len(one) - len(empty))
		if index != 0 {
			charge++
		}
		charge, err = addFrameBytes(charge, int64(base64.StdEncoding.EncodedLen(int(lengths.Name))))
		if err != nil {
			return 0, err
		}
		charge, err = addFrameBytes(charge, int64(base64.StdEncoding.EncodedLen(int(lengths.Content))))
		if err != nil {
			return 0, err
		}
		return charge, nil
	})
}

func addFrameBytes(total, more int64) (int64, error) {
	if more < 0 || more > math.MaxInt64-total {
		return 0, fmt.Errorf("stream-frame byte accounting overflowed: %w", syscall.EFBIG)
	}
	return total + more, nil
}

func payloadLengthsFitFrame(maxFrameBytes int64, lengths ...int64) error {
	maxInt := int64(^uint(0) >> 1)
	for _, length := range lengths {
		if length < 0 {
			return fmt.Errorf("a stream payload has negative length: %w", syscall.EIO)
		}
		if length > maxFrameBytes || length > maxInt || length > (maxInt/4)*3 {
			return fmt.Errorf("a %d-byte stream payload cannot fit the %d-byte frame bound: %w", length, maxFrameBytes, syscall.EFBIG)
		}
	}
	return nil
}
