package httprest

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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

// maxFrameBytes is the largest frame this side will assemble.
//
// A stream is read from a party that has already been established as speaking this
// protocol, but "speaking this protocol" is not "will not exhaust this process's memory":
// the frame length arrives as the data itself, so without a bound a single frame that
// never ends is an unbounded allocation (R-INT-3). It is far above any frame the handler
// here produces — a page of snapshot rows is hundreds of kilobytes at the default page
// size — so reaching it means something on the far side is not sizing its pages.
const maxFrameBytes = 8 << 20

// frameWriter writes the frames of one stream.
//
// Every frame is flushed as it is written. A change that sits in a buffer is a change that
// has not been delivered, and R-CON-2 forbids visibility that waits for anything to
// elapse — including the moment a write buffer happens to fill.
type frameWriter struct {
	to      io.Writer
	control *http.ResponseController
}

// openStream begins a stream: it commits the response to a success and to this framing, so
// nothing after it can report a status.
func openStream(w http.ResponseWriter) (*frameWriter, error) {
	w.Header().Set("Content-Type", contentEventStream)
	w.WriteHeader(http.StatusOK)
	f := &frameWriter{to: w, control: http.NewResponseController(w)}
	// The headers are of no use to the far side until they arrive: a client that has not
	// seen them is still waiting for a response, and cannot tell that from a server that
	// has not answered.
	return f, f.control.Flush()
}

func (f *frameWriter) send(event string, payload any) error {
	// Rendered whole before anything is written, so that a payload that will not encode
	// cannot leave half a frame on a stream that has no way to retract it.
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("cannot render the %s frame: %w", event, err)
	}
	if _, err := fmt.Fprintf(f.to, "%s%s\n%s%s\n\n", fieldEvent, event, fieldData, encoded); err != nil {
		return err
	}
	return f.control.Flush()
}

// fault ends the stream by saying why, in place of everything that would have followed.
// Its own failure to reach the far side changes nothing: the stream is over either way,
// and the far side treats a stream that stopped as a failure whether or not it was told.
func (f *frameWriter) fault(cause error) {
	f.send(eventFault, StreamFault{Message: cause.Error()})
}

// frameReader reads the frames of one stream.
type frameReader struct {
	lines *bufio.Scanner
}

func newFrameReader(from io.Reader) *frameReader {
	lines := bufio.NewScanner(from)
	lines.Buffer(make([]byte, 0, 64<<10), maxFrameBytes)
	return &frameReader{lines: lines}
}

// next returns the next frame's name and its payload.
//
// It reports io.EOF for a stream that ended on a frame boundary, and an error for one that
// ended anywhere else. Neither is by itself a verdict: which of the two is acceptable is
// decided by what the last frame said, because a stream that ended where a frame ends is
// exactly what a connection dropped at that moment also looks like.
func (f *frameReader) next() (string, []byte, error) {
	var frame []string
	for f.lines.Scan() {
		line := f.lines.Text()
		if line != "" {
			if len(frame) == 2 {
				return "", nil, fmt.Errorf("a frame carries a line beyond its event and its data: %q", line)
			}
			frame = append(frame, line)
			continue
		}
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
