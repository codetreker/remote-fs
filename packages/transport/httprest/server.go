package httprest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Handler serves one namespace.
//
// It is a plain http.Handler, so it can be run on its own or mounted inside an existing
// server; http.StripPrefix is how it is mounted somewhere other than the root.
type Handler struct {
	storage storage.Storage
	log     metastore.Log
	limits  Limits

	// publisher is present exactly when log is, and wakes the open subscriptions.
	publisher *publisher

	// snapshots holds one token per snapshot that may be open at once.
	snapshots chan struct{}

	// stopping is closed by Stop and read by every open change stream.
	stopping  chan struct{}
	stopsOnce sync.Once
}

var _ http.Handler = (*Handler)(nil)

// NewHandler builds a handler over s, publishing log to whatever replicates the namespace.
//
// The log is a second input rather than something discovered through s, because not every
// namespace has one: a local directory is a namespace with no metastore behind it and
// therefore no ordered record of what changed. Such a namespace is served whole here, and
// the two replication endpoints answer ENOSYS — a nil log passed in on purpose, rather
// than an absence this package could infer, so that a caller that has a log and forgets to
// pass it is making a visible choice instead of silently serving a namespace nothing can
// replicate.
func NewHandler(s storage.Storage, log metastore.Log) (*Handler, error) {
	return NewHandlerWithLimits(s, log, DefaultLimits())
}

// NewHandlerWithLimits is NewHandler with the bounds on the replication endpoints given
// rather than defaulted. What the defaults are, and why they are guesses, is on Limits.
func NewHandlerWithLimits(s storage.Storage, log metastore.Log, limits Limits) (*Handler, error) {
	if s == nil {
		return nil, errors.New("httprest: a handler needs a storage to serve")
	}
	if err := limits.check(); err != nil {
		return nil, err
	}
	h := &Handler{
		storage:   s,
		log:       log,
		limits:    limits,
		snapshots: make(chan struct{}, limits.Snapshots),
		stopping:  make(chan struct{}),
	}
	if log != nil {
		h.publisher = newPublisher()
	}
	return h, nil
}

// Stop ends every open change stream, telling each replica that this server is going away.
//
// It exists because a change stream never becomes idle. http.Server.Shutdown waits for
// connections to return to idle and does not cancel request contexts, so a server with one
// replica attached waits out whatever deadline Shutdown was given and then reports that it
// expired — an ordinary stop turned into a stall and a failure. Handing this to
// http.Server.RegisterOnShutdown is what lets the streams let go when shutdown begins:
//
//	server := &http.Server{Handler: handler}
//	server.RegisterOnShutdown(handler.Stop)
//
// A stream ended this way says so, and a replica told this keeps what it has and reconnects
// with the position it holds. That is the point of saying it at all: a connection that
// simply stopped could equally be a server that vanished, and being polite about going away
// must not cost a replica more than being abrupt would have.
//
// It returns as soon as the streams have been told, not when they have gone; waiting for
// them is what Shutdown is already doing. Calling it more than once is harmless, and calling
// it on a handler that serves a namespace with no log does nothing, because such a namespace
// has no streams to end.
func (h *Handler) Stop() {
	h.stopsOnce.Do(func() { close(h.stopping) })
}

// stopped reports whether Stop has been called.
func (h *Handler) stopped() bool {
	select {
	case <-h.stopping:
		return true
	default:
		return false
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Marking every response, including the failures, is what lets the far side tell an
	// answer from this handler apart from one an intermediary made up.
	w.Header().Set(HeaderProtocol, Version)
	// A cached answer from a namespace is a stale answer that reads exactly like a
	// current one, which is the failure this system is least able to survive.
	w.Header().Set("Cache-Control", "no-store")

	req, err := ParseRequest(r.Method, r.URL)
	if err != nil {
		writeFault(w, statusForParseError(err), err)
		return
	}
	h.dispatch(w, r, req)
}

func (h *Handler) dispatch(w http.ResponseWriter, r *http.Request, req Request) {
	ctx := r.Context()
	switch req.Op {
	case OpStat:
		attr, err := h.storage.Stat(ctx, req.Path)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, StatResponse{Attr: AttrOf(attr)})

	case OpSetAttr:
		change, err := readChange(r)
		if err != nil {
			// The change did not arrive whole, so what the caller wanted set is not known.
			// Applying the part that did arrive would leave the rest at whatever it was
			// and answer that the request was carried out.
			writeFault(w, http.StatusBadRequest, err)
			return
		}
		h.report(w, h.storage.SetAttr(ctx, req.Path, change))

	case OpList:
		entries, err := h.storage.List(ctx, req.Path)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, ListResponse{Entries: EntriesOf(entries)})

	case OpRead:
		content, err := h.storage.Read(ctx, req.Path)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeContent(w, content)

	case OpWrite:
		content, err := readBody(r)
		if err != nil {
			// The request did not arrive whole, so the caller's intended contents are
			// not known. Writing the part that did arrive would replace the file with a
			// prefix of itself and report success.
			writeFault(w, http.StatusBadRequest, err)
			return
		}
		h.report(w, h.storage.Write(ctx, req.Path, content))

	case OpCreate:
		h.report(w, h.storage.Create(ctx, req.Path))
	case OpMkdir:
		h.report(w, h.storage.Mkdir(ctx, req.Path))
	case OpRemove:
		h.report(w, h.storage.Remove(ctx, req.Path))
	case OpRemoveDir:
		h.report(w, h.storage.RemoveDir(ctx, req.Path))
	case OpRename:
		h.report(w, h.storage.Rename(ctx, req.Path, req.To))

	case OpSpace:
		// ENOSYS from a namespace with no room of its own to report is an answer about
		// that namespace rather than a gap in this protocol, so it travels under its own
		// name like every other errno.
		space, err := h.storage.Space(ctx)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, SpaceResponse{Space: SpaceOf(space)})

	case OpSubscribe:
		h.serveEvents(w, r, nil)
	case OpResubscribe:
		h.serveEvents(w, r, &resumeFrom{incarnation: req.Incarnation, position: req.Position})
	case OpSnapshot:
		h.serveSnapshot(w, r)
	}
}

// report answers an operation that produces nothing but an outcome.
func (h *Handler) report(w http.ResponseWriter, err error) {
	if err != nil {
		writeStorageError(w, err)
		return
	}
	// Every operation that changes the namespace is answered here, so this is the one
	// place that knows the namespace has just moved — and the moment it knows is the
	// moment the subscriptions are told to read the log again. Nothing is handed to them:
	// what they read is the log itself, which is the only record of what changed.
	if h.publisher != nil {
		h.publisher.wake()
	}
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

// readBody returns the whole request body, or an error if any part of it is missing.
//
// The length check is not redundant with the read error: it catches a body that ends
// early with a clean close, where io.ReadAll has nothing to report.
func readBody(r *http.Request) ([]byte, error) {
	content, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("the request body ended early: %w", err)
	}
	if r.ContentLength >= 0 && int64(len(content)) != r.ContentLength {
		return nil, fmt.Errorf("the request body carried %d bytes, %d were announced", len(content), r.ContentLength)
	}
	return content, nil
}

// readChange returns the attribute change a request carries.
func readChange(r *http.Request) (storage.AttrChange, error) {
	body, err := readBody(r)
	if err != nil {
		return storage.AttrChange{}, err
	}
	var request SetAttrRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return storage.AttrChange{}, fmt.Errorf("the request body is not an attribute change: %w", err)
	}
	return request.Change.Storage(), nil
}

func writeContent(w http.ResponseWriter, content []byte) {
	// Declared explicitly rather than left to the transfer encoding, so that the caller
	// can tell a body that was cut short from a file that is genuinely that size.
	w.Header().Set("Content-Length", strconv.Itoa(len(content)))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	// Rendered whole before anything is written: a marshalling failure part way through
	// would otherwise leave a status already sent and a body cut in half.
	encoded, err := json.Marshal(body)
	if err != nil {
		panic(fmt.Sprintf("httprest: cannot render %T: %v", body, err))
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(encoded)
}

// writeStorageError reports an outcome the storage produced.
func writeStorageError(w http.ResponseWriter, err error) {
	writeJSON(w, StatusStorageError, ErrorResponse{
		Errno:   storage.ErrnoNameOf(err),
		Message: err.Error(),
	})
}

// writeFault reports that the exchange itself went wrong. It never uses
// StatusStorageError, so it cannot be read as the outcome of an operation.
func writeFault(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, ErrorResponse{Message: err.Error()})
}

// statusForParseError keeps a malformed exchange in the part of the status space that
// reads as "the outcome is unknown".
func statusForParseError(err error) int {
	switch {
	case errors.Is(err, ErrUnknownOp):
		return http.StatusNotFound
	case errors.Is(err, ErrMethod):
		return http.StatusMethodNotAllowed
	default:
		return http.StatusBadRequest
	}
}
