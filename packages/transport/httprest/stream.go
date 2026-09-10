package httprest

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// A replication stream is a sequence of server-sent events: for each frame, a line naming
// what it is, a line of JSON carrying it, and a blank line ending it.
//
// Both replication endpoints use this framing, including the snapshot, which is not a
// stream of events in any ordinary sense. One framing rather than two, because the
// property that matters is the same for both and it is a property of the frames rather
// than of HTTP: every stream ends with a frame that says so, so a stream that stops
// without one was cut short. Nothing below has to reason about whether the transport could
// have reported a truncation — the case that has no answer over close-delimited HTTP/1.x,
// and the reason a plain response body is refused outright when it is framed that way.
const contentEventStream = "text/event-stream"

const (
	fieldEvent = "event: "
	fieldData  = "data: "
)

// frameWriter writes the frames of one stream.
//
// Every frame is flushed as it is written. A change that sits in a buffer is a change that
// has not been delivered, and R-CON-2 forbids visibility that waits for anything to
// elapse — including the moment a write buffer happens to fill.
type frameWriter struct {
	to            io.Writer
	control       *http.ResponseController
	maxFrameBytes int64
}

// openStream begins a stream: it commits the response to a success and to this framing, so
// nothing after it can report a status.
func openStream(w http.ResponseWriter, maxFrameBytes int64) (*frameWriter, error) {
	w.Header().Set("Content-Type", contentEventStream)
	w.WriteHeader(http.StatusOK)
	f := &frameWriter{to: w, control: http.NewResponseController(w), maxFrameBytes: maxFrameBytes}
	// The headers are of no use to the far side until they arrive: a client that has not
	// seen them is still waiting for a response, and cannot tell that from a server that
	// has not answered.
	return f, f.control.Flush()
}

// departureGrace is how long a stream has to say it is going before it is cut off.
//
// A write completes as soon as the kernel accepts the bytes, so a stream anybody is reading
// uses almost none of this: the frame is a few hundred bytes and there is room for it. A
// stream nobody is reading cannot use it at all, because the buffers between the two ends
// are already full — which is what makes the two cases sharply different rather than a
// matter of degree, and this figure a formality rather than a tuning knob. It is paid once
// by a shutdown, concurrently by every stream, and only by the streams that are stuck.
const departureGrace = 100 * time.Millisecond

// endWritesWhen makes whatever this stream is in the middle of writing fail shortly after
// done is closed, and returns the function that takes the arrangement down again.
//
// Between frames a stream can be told to stop through an ordinary channel, and both loops
// that drive one do exactly that. Inside a write there is no such moment. A reader that has
// stopped consuming fills every buffer between the two ends, and the write then blocks until
// they drain — which, for a reader that is not going to read again, is never. A deadline
// already in the past is the only thing that reaches a write in that state, and a
// connection's deadline may be set from another goroutine while a write on it is in flight,
// which is what this relies on.
//
// It waits out departureGrace first, and stands down the moment the stream ends on its own.
// Cutting immediately would race the frame the loop writes to say it is going, and win often
// enough to turn a deliberate departure into a connection that merely stopped — the very
// distinction the frame exists to draw.
//
// Once the deadline is set it stays set, so nothing further can be written to this stream.
// That is the honest end of it: a reader that is not reading cannot be told anything, and
// what it will find when it looks is a stream that stopped.
func (f *frameWriter) endWritesWhen(done <-chan struct{}) (release func()) {
	released, watched := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(watched)
		select {
		case <-released:
			return
		case <-done:
		}
		select {
		case <-released:
		case <-time.After(departureGrace):
			// Its failure would mean the connection is already gone, in which case the
			// write this exists to interrupt is failing of its own accord.
			f.control.SetWriteDeadline(time.Now())
		}
	}()
	return func() {
		close(released)
		<-watched
	}
}

func (f *frameWriter) send(event string, payload any) error {
	// Rendered whole before anything is written, so that a payload that will not encode
	// cannot leave half a frame on a stream that has no way to retract it.
	encoded, err := marshalFrame(event, payload, f.maxFrameBytes)
	if err != nil {
		return err
	}
	return f.sendEncoded(event, encoded)
}

func marshalFrame(event string, payload any, maxFrameBytes int64) ([]byte, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("cannot render the %s frame: %w", event, err)
	}
	frameBytes, err := encodedFrameBytes(event, int64(len(encoded)))
	if err != nil {
		return nil, err
	}
	if frameBytes > maxFrameBytes {
		return nil, fmt.Errorf("the %s frame requires %d bytes under the %d-byte frame bound: %w",
			event, frameBytes, maxFrameBytes, syscall.EFBIG)
	}
	return encoded, nil
}

func (f *frameWriter) sendEncoded(event string, encoded []byte) error {
	for _, part := range []string{fieldEvent, event, "\n", fieldData} {
		if err := writeFrameString(f.to, part); err != nil {
			return err
		}
	}
	if n, err := f.to.Write(encoded); err != nil {
		return err
	} else if n != len(encoded) {
		return io.ErrShortWrite
	}
	if err := writeFrameString(f.to, "\n\n"); err != nil {
		return err
	}
	return f.control.Flush()
}

func writeFrameString(to io.Writer, value string) error {
	n, err := io.WriteString(to, value)
	if err != nil {
		return err
	}
	if n != len(value) {
		return io.ErrShortWrite
	}
	return nil
}

func encodedFrameBytes(event string, payloadBytes int64) (int64, error) {
	if payloadBytes < 0 {
		return 0, fmt.Errorf("a frame payload cannot have negative length: %w", syscall.EINVAL)
	}
	fixed := int64(len(fieldEvent) + len(event) + 1 + len(fieldData) + 2)
	if payloadBytes > math.MaxInt64-fixed {
		return 0, fmt.Errorf("the %s frame length overflows byte accounting: %w", event, syscall.EFBIG)
	}
	return fixed + payloadBytes, nil
}

// fault ends the stream by saying why, in place of everything that would have followed.
// Its own failure to reach the far side changes nothing: the stream is over either way,
// and the far side treats a stream that stopped as a failure whether or not it was told.
func (f *frameWriter) fault(cause error) {
	message := cause.Error()
	const faultEnvelopeBytes int64 = 128
	maxMessage := (f.maxFrameBytes - faultEnvelopeBytes) / 6
	if maxMessage < 0 {
		maxMessage = 0
	}
	if int64(len(message)) > maxMessage {
		message = "the stream failed and its detail exceeds the configured frame bound"
	}
	f.send(eventFault, StreamFault{Message: message})
}

// alive says that the stream is still there, on a stream that has nothing else to say.
//
// It is a server-sent event comment — a line beginning with a colon, which carries no event
// and is discarded by whatever reads it — and it exists because a stream nobody is writing
// to and a stream whose connection is gone are the same observation on this side of it:
// silence. Without it the far side has nothing to distinguish "this volume is quiet"
// from "these bytes stopped arriving twenty minutes ago", and a replica fed by that stream
// would go on answering from a copy it can no longer justify (R-ERR-1, R-ERR-2).
//
// A failure to write one is how this side learns that the reader is gone, which is the same
// thing the other way round: without it a replica that vanished without closing its
// connection holds a goroutine here until something else happens to be published.
func (f *frameWriter) alive() error {
	if _, err := fmt.Fprint(f.to, ": alive\n"); err != nil {
		return err
	}
	return f.control.Flush()
}

// frameReader reads the frames of one stream.
type frameReader struct {
	lines *bufio.Scanner

	// silenced records that the stream was abandoned for having gone quiet, so that the
	// read it interrupted is reported as what it is rather than as the cancellation that
	// carried it out.
	silenced *atomic.Bool
	silence  time.Duration
	maxBytes int64
}

// newFrameReader reads frames from a stream, and gives up on one that has said nothing at
// all for silence.
//
// The bound is on a read that is waiting rather than on the connection, which is what makes
// it right in both directions. A caller that is not reading this stream — a replica taking
// a picture of the tree, which leaves what arrives meanwhile queued on the connection — is
// not relying on it and is not timed out for that; a caller that is waiting is told, within
// one bound, that nothing is coming. It is reset by bytes rather than by frames, so a page
// of a snapshot that takes longer than the bound to cross a slow link is not mistaken for a
// stream that has stopped.
//
// abandon is what makes the waiting read return. There is nothing else that could: a
// response body offers no deadline, and http.Client.Timeout bounds the whole exchange,
// which on a stream that is meant to stay open with nothing on it is a timer that severs
// healthy subscriptions.
func newFrameReader(from io.Reader, silence time.Duration, maxFrameBytes int64, abandon func()) *frameReader {
	silenced := &atomic.Bool{}
	quiet := time.AfterFunc(silence, func() {
		silenced.Store(true)
		abandon()
	})
	quiet.Stop()

	lines := bufio.NewScanner(&watched{from: from, silence: silence, quiet: quiet})
	initial := min(int64(64<<10), maxFrameBytes)
	lines.Buffer(make([]byte, 0, int(initial)), int(maxFrameBytes))
	return &frameReader{lines: lines, silenced: silenced, silence: silence, maxBytes: maxFrameBytes}
}

// watched is a reader whose every wait is bounded.
type watched struct {
	from    io.Reader
	silence time.Duration
	quiet   *time.Timer
}

func (w *watched) Read(p []byte) (int, error) {
	w.quiet.Reset(w.silence)
	n, err := w.from.Read(p)
	w.quiet.Stop()
	return n, err
}

// next returns the next frame's name and its payload.
//
// It reports io.EOF for a stream that ended on a frame boundary, and an error for one that
// ended anywhere else. Neither is by itself a verdict: which of the two is acceptable is
// decided by what the last frame said, because a stream that ended where a frame ends is
// exactly what a connection dropped at that moment also looks like.
func (f *frameReader) next() (string, []byte, error) {
	var frame []string
	var frameBytes int64
	for f.lines.Scan() {
		line := f.lines.Text()
		// A comment: the far side saying that the stream is still there and it has nothing
		// else to say. It carries no event, so it is not part of any frame.
		if strings.HasPrefix(line, ":") {
			continue
		}
		if line != "" {
			lineBytes := int64(len(line)) + 1
			if lineBytes > f.maxBytes-frameBytes {
				return "", nil, fmt.Errorf("a stream frame exceeds its %d-byte bound", f.maxBytes)
			}
			frameBytes += lineBytes
			if len(frame) == 2 {
				return "", nil, fmt.Errorf("a frame carries a line beyond its event and its data: %q", line)
			}
			frame = append(frame, line)
			continue
		}
		if frameBytes == f.maxBytes {
			return "", nil, fmt.Errorf("a stream frame exceeds its %d-byte bound", f.maxBytes)
		}
		frameBytes++
		if len(frame) != 2 {
			return "", nil, fmt.Errorf("a frame of %d lines arrived, and a frame is an event and its data", len(frame))
		}
		event, named := strings.CutPrefix(frame[0], fieldEvent)
		if !named {
			return "", nil, fmt.Errorf("a frame begins with %q, which does not name an event", frame[0])
		}
		payload, carried := strings.CutPrefix(frame[1], fieldData)
		if !carried {
			return "", nil, fmt.Errorf("the %s frame continues with %q, which is not its data", event, frame[1])
		}
		return event, []byte(payload), nil
	}
	// Checked before the error the read came back with, because that error is this side's
	// own cancellation and says nothing about what happened.
	if f.silenced.Load() {
		return "", nil, fmt.Errorf("nothing at all arrived on this stream for %v, so it is no longer being delivered", f.silence)
	}
	if err := f.lines.Err(); err != nil {
		return "", nil, fmt.Errorf("the stream ended early: %w", err)
	}
	if len(frame) != 0 {
		return "", nil, fmt.Errorf("the stream ended part way through a frame, after %d of its lines", len(frame))
	}
	return "", nil, io.EOF
}

// decode reads the next frame, requiring it to be the one named, and unmarshals it into
// payload. A fault frame is reported as the failure it carries rather than as a frame of
// the wrong name, so that whatever went wrong on the far side is what the caller is told.
func (f *frameReader) decode(want string, payload any) error {
	event, data, err := f.next()
	if err != nil {
		return err
	}
	if event == eventFault && want != eventFault {
		return faultOf(data)
	}
	if event != want {
		return fmt.Errorf("a %s frame arrived where a %s frame was expected", event, want)
	}
	return decodeFrame(event, data, payload)
}

func decodeFrame(event string, data []byte, payload any) error {
	if err := json.Unmarshal(data, payload); err != nil {
		return fmt.Errorf("the %s frame does not decode: %w", event, err)
	}
	return nil
}

// faultOf turns a fault frame into the error it reports.
func faultOf(data []byte) error {
	var fault StreamFault
	if err := json.Unmarshal(data, &fault); err != nil {
		return fmt.Errorf("the stream failed, and its reason does not decode: %w", err)
	}
	if fault.Message == "" {
		return errors.New("the stream failed, and the server gave no reason")
	}
	return fmt.Errorf("the stream failed: %s", fault.Message)
}
