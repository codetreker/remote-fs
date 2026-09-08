package httprest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

// Handler serves one namespace.
//
// It is a plain http.Handler, so it can be run on its own or mounted inside an existing
// server; http.StripPrefix is how it is mounted somewhere other than the root.
type Handler struct {
	storage             *locked.Storage
	locks               locking.Service
	lockControls        *bodyAdmission
	log                 metastore.Log
	limits              Limits
	maxBodyBytes        int64
	maxWriteBytes       int64
	maxFrameBytes       int64
	maxIncarnationBytes int64
	bodies              *bodyAdmission
	responses           *bodyAdmission
	snapshotFrames      *bodyAdmission

	// publisher is present exactly when log is, and wakes the open subscriptions.
	publisher *publisher

	// snapshots admits an open snapshot without preallocating from its configured limit.
	snapshots *bodyAdmission

	// stopping is closed by Stop and read by every open change stream.
	stopping  chan struct{}
	stopsOnce sync.Once
}

var _ http.Handler = (*Handler)(nil)

// NewHandler serves a backend whose LockService authorizes every final mutation.
// A backend without that paired authority is rejected before serving. Scoped namespace
// requests and anonymous requests reach that same authority. log describes the namespace's
// committed mutations for metadata replication.
//
// The log is a second input rather than something discovered through s, because not every
// namespace has one: a local directory is a namespace with no metastore behind it and
// therefore no ordered record of what changed. Such a namespace is served whole here, and
// the two replication endpoints answer ENOSYS — a nil log passed in on purpose, rather
// than an absence this package could infer, so that a caller that has a log and forgets to
// pass it is making a visible choice instead of silently serving a namespace nothing can
// replicate.
//
// A non-nil log must describe the same namespace as s. Every successful mutation that changes
// that namespace must record its change atomically before the storage method returns. The handler
// uses the log's barrier as proof that the mutation can become visible to a replica; violating the
// pairing can acknowledge a mutation before its change is present in the log.
func NewHandler(s storage.Storage, log metastore.Log) (*Handler, error) {
	return NewHandlerWithOptions(s, log, DefaultHandlerOptions())
}

// NewHandlerWithLimits preserves the replication-only constructor. Request bodies,
// non-streaming responses, and frame production use the bounded defaults in
// DefaultHandlerOptions.
func NewHandlerWithLimits(s storage.Storage, log metastore.Log, limits Limits) (*Handler, error) {
	options := DefaultHandlerOptions()
	options.Replication = limits
	return NewHandlerWithOptions(s, log, options)
}

// NewHandlerWithOptions is NewHandler with explicit request-body, non-streaming response,
// and replication bounds. The storage and log pairing requirements documented on NewHandler
// apply unchanged.
func NewHandlerWithOptions(s storage.Storage, log metastore.Log, options HandlerOptions) (*Handler, error) {
	if s == nil {
		return nil, errors.New("httprest: a handler needs a storage to serve")
	}
	if err := options.Check(); err != nil {
		return nil, err
	}
	bounded, ok := s.(storage.BoundedStorage)
	if !ok {
		return nil, errors.New("httprest: storage does not implement bounded read and list results")
	}
	backend, ok := bounded.(locked.Backend)
	if !ok {
		return nil, errors.New("httprest: storage has no bound file-lock authority")
	}
	paired, err := locked.New(backend)
	if err != nil {
		return nil, fmt.Errorf("httprest: invalid enforcing namespace: %w", err)
	}
	settled := options.settle()
	h := &Handler{
		storage:             paired,
		locks:               paired.LockService(),
		lockControls:        configuredLockControlAdmission(settled.maxConcurrentLockControls, settled.maxWaitingLockControls),
		log:                 log,
		limits:              settled.replication,
		maxBodyBytes:        settled.maxBodyBytes,
		maxWriteBytes:       settled.maxWriteBytes,
		maxFrameBytes:       settled.maxFrameBytes,
		maxIncarnationBytes: maxIncarnationBytes(settled.maxBodyBytes, settled.maxFrameBytes),
		bodies: newBodyAdmission(
			settled.maxConcurrentBodies,
			settled.maxInFlightBodyBytes,
			settled.maxWaitingBodies,
		),
		responses: newBodyAdmission(
			settled.maxConcurrentResponses,
			settled.maxInFlightResponseBytes,
			settled.maxWaitingResponses,
		),
		snapshotFrames: newBodyAdmission(
			settled.maxConcurrentSnapshotFrames,
			settled.maxInFlightSnapshotFrameBytes,
			settled.maxWaitingSnapshotFrames,
		),
		snapshots: newBodyAdmission(settled.replication.Snapshots, 0, 0),
		stopping:  make(chan struct{}),
	}
	if log != nil {
		h.publisher = newPublisher(settled.replication.MaxSubscriptions)
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
		h.writeFault(w, statusForParseError(err), err)
		return
	}
	if isLockControl(req.Op) {
		h.serveLockControl(w, r, req)
		return
	}
	scope, present, err := requestMutationScope(r, req.Op)
	if err != nil {
		h.writeFault(w, http.StatusBadRequest, err)
		return
	}
	if present || isNamespaceMutation(req.Op) {
		r = r.WithContext(locking.WithScope(r.Context(), scope))
	}
	if req.ContentType() == "" {
		if err := h.requireEmptyBody(r); err != nil {
			if errors.Is(err, syscall.EAGAIN) {
				h.writeOperationError(w, err)
				return
			}
			h.writeRequestBodyFault(w, fmt.Errorf("%s: %w", req.Op, err))
			return
		}
	}
	if req.Op != OpSubscribe && req.Op != OpResubscribe && req.Op != OpSnapshot {
		reservation := h.maxBodyBytes
		if req.Op == OpRead || req.Op == OpList {
			reservation = retainedResponseMultiplier * h.maxBodyBytes
		}
		release, err := h.responses.acquire(r.Context(), reservation)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) {
				h.writeOperationError(w, err)
				return
			}
			h.writeRequestBodyFault(w, fmt.Errorf("%s response admission: %w", req.Op, err))
			return
		}
		defer release()
	}
	h.dispatch(w, r, req)
}

func (h *Handler) dispatch(w http.ResponseWriter, r *http.Request, req Request) {
	ctx := r.Context()
	switch req.Op {
	case OpStat:
		attr, err := h.storage.Stat(ctx, req.Path)
		if err != nil {
			h.writeOperationError(w, err)
			return
		}
		h.writeJSON(w, http.StatusOK, StatResponse{Attr: AttrOf(attr)})

	case OpSetAttr:
		body, release, err := h.readBody(r, h.maxBodyBytes)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) {
				h.writeOperationError(w, err)
				return
			}
			// The change did not arrive whole, so what the caller wanted set is not known.
			// Applying the part that did arrive would leave the rest at whatever it was
			// and answer that the request was carried out.
			h.writeRequestBodyFault(w, err)
			return
		}
		defer release()
		change, err := decodeChange(body)
		if err != nil {
			h.writeFault(w, http.StatusBadRequest, err)
			return
		}
		h.report(ctx, w, h.storage.SetAttr(ctx, req.Path, change))

	case OpList:
		result, err := newListResult(h.maxBodyBytes)
		if err != nil {
			panic(fmt.Sprintf("httprest: cannot construct a validated list result: %v", err))
		}
		err = h.storage.ListBounded(ctx, req.Path, result)
		if err != nil {
			h.writeOperationError(w, err)
			return
		}
		entries, err := result.Entries()
		if err != nil {
			h.writeOperationError(w, err)
			return
		}
		h.writeJSON(w, http.StatusOK, ListResponse{Entries: EntriesOf(entries)})

	case OpRead:
		content, err := h.storage.ReadBounded(ctx, req.Path, h.maxBodyBytes)
		if err != nil {
			h.writeOperationError(w, err)
			return
		}
		h.writeContent(w, content)

	case OpWrite:
		content, release, err := h.readBody(r, h.maxWriteBytes)
		if err != nil {
			if errors.Is(err, errBodyTooLarge) {
				h.writeOperationError(w, fmt.Errorf("%v: %w", err, syscall.EFBIG))
				return
			}
			if errors.Is(err, syscall.EAGAIN) {
				h.writeOperationError(w, err)
				return
			}
			// The request did not arrive whole, so the caller's intended contents are
			// not known. Writing the part that did arrive would replace the file with a
			// prefix of itself and report success.
			h.writeRequestBodyFault(w, err)
			return
		}
		defer release()
		h.report(ctx, w, h.storage.Write(ctx, req.Path, content))

	case OpCreate:
		h.report(ctx, w, h.storage.Create(ctx, req.Path))
	case OpMkdir:
		h.report(ctx, w, h.storage.Mkdir(ctx, req.Path))
	case OpRemove:
		h.report(ctx, w, h.storage.Remove(ctx, req.Path))
	case OpRemoveDir:
		h.report(ctx, w, h.storage.RemoveDir(ctx, req.Path))
	case OpRename:
		h.report(ctx, w, h.storage.Rename(ctx, req.Path, req.To))

	case OpSpace:
		// ENOSYS from a namespace with no room of its own to report is an answer about
		// that namespace rather than a gap in this protocol, so it travels under its own
		// name like every other errno.
		space, err := h.storage.Space(ctx)
		if err != nil {
			h.writeOperationError(w, err)
			return
		}
		h.writeJSON(w, http.StatusOK, SpaceResponse{Space: SpaceOf(space)})

	case OpSubscribe:
		h.serveEvents(w, r, nil)
	case OpResubscribe:
		h.serveEvents(w, r, &resumeFrom{incarnation: req.Incarnation, position: req.Position})
	case OpSnapshot:
		h.serveSnapshot(w, r)
	}
}

// report answers an operation that produces nothing but an outcome.
func (h *Handler) report(ctx context.Context, w http.ResponseWriter, err error) {
	if err != nil {
		h.writeOperationError(w, err)
		return
	}
	// Every operation that changes the namespace is answered here, so this is the one
	// place that knows the namespace has just moved — and the moment it knows is the
	// moment the subscriptions are told to read the log again. Nothing is handed to them:
	// what they read is the log itself, which is the only record of what changed.
	if h.publisher != nil {
		h.publisher.wake()
	}
	response := MutationResponse{}
	if h.log != nil {
		barrier, err := h.mutationBarrier(ctx)
		if err != nil {
			h.writeOperationError(w, fmt.Errorf("the namespace changed but its replication barrier could not be read: %v: %w", err, syscall.EIO))
			return
		}
		response.Barrier = barrier
	}
	h.writeJSON(w, http.StatusOK, response)
}

func (h *Handler) mutationBarrier(ctx context.Context) (*MutationBarrier, error) {
	maxIncarnationBytes := h.maxIncarnationBytes
	barrier, err := h.log.Barrier(ctx, maxIncarnationBytes)
	if err != nil {
		return nil, err
	}
	if barrier.Incarnation == "" {
		return nil, errors.New("the change log reports no incarnation")
	}
	if int64(len(barrier.Incarnation)) > maxIncarnationBytes {
		return nil, fmt.Errorf("the change log returned a %d-byte incarnation above the requested %d-byte bound",
			len(barrier.Incarnation), maxIncarnationBytes)
	}
	if barrier.Position < 0 {
		return nil, fmt.Errorf("the change log reports negative position %d", barrier.Position)
	}
	return &MutationBarrier{Incarnation: string(barrier.Incarnation), Position: int64(barrier.Position)}, nil
}

func maxIncarnationBytes(maxBodyBytes, maxFrameBytes int64) int64 {
	const envelopeBytes int64 = 256
	return min(int64(MaxIncarnationBytes), (maxBodyBytes-envelopeBytes)/6, (maxFrameBytes-envelopeBytes)/6)
}

// readBody returns the whole request body without retaining more than the configured
// maximum, or an error if any part of it is missing.
//
// The length check is not redundant with the read error: it catches a body that ends
// early with a clean close, where io.ReadAll has nothing to report.
func (h *Handler) readBody(r *http.Request, limit int64) ([]byte, func(), error) {
	if r.ContentLength > limit {
		return nil, nil, fmt.Errorf("%w of %d bytes: %d bytes were announced", errBodyTooLarge, limit, r.ContentLength)
	}
	release, err := h.bodies.acquire(r.Context(), limit)
	if err != nil {
		return nil, nil, fmt.Errorf("the request ended before its body could be admitted: %w", err)
	}
	content, err := readAtMost(r.Body, limit)
	if err != nil {
		release()
		if errors.Is(err, errBodyTooLarge) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("the request body ended early: %w", err)
	}
	if r.ContentLength >= 0 && int64(len(content)) != r.ContentLength {
		release()
		return nil, nil, fmt.Errorf("the request body carried %d bytes, %d were announced", len(content), r.ContentLength)
	}
	return content, release, nil
}

// decodeChange returns the attribute change a request body carries.
func decodeChange(body []byte) (storage.AttrChange, error) {
	var request SetAttrRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return storage.AttrChange{}, fmt.Errorf("the request body is not an attribute change: %w", err)
	}
	return request.Change.Storage(), nil
}

func (h *Handler) writeRequestBodyFault(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, errBodyTooLarge) {
		status = http.StatusRequestEntityTooLarge
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusRequestTimeout
	}
	h.writeFault(w, status, err)
}

func (h *Handler) writeContent(w http.ResponseWriter, content []byte) {
	// Declared explicitly rather than left to the transfer encoding, so that the caller
	// can tell a body that was cut short from a file that is genuinely that size.
	w.Header().Set("Content-Length", strconv.Itoa(len(content)))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, body any) {
	if response, ok := body.(ErrorResponse); ok {
		body = boundedErrorResponse(response, h.maxBodyBytes)
	}
	// Rendered whole before anything is written: a marshalling failure part way through
	// would otherwise leave a status already sent and a body cut in half.
	encoded, err := json.Marshal(body)
	if err != nil {
		panic(fmt.Sprintf("httprest: cannot render %T: %v", body, err))
	}
	if int64(len(encoded)) > h.maxBodyBytes {
		status = http.StatusInternalServerError
		encoded, err = json.Marshal(ErrorResponse{Message: "the response exceeds the configured HTTP body limit"})
		if err != nil {
			panic(fmt.Sprintf("httprest: cannot render the bounded response fault: %v", err))
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(encoded)
}

// writeOperationError reports an exact outcome in the storage contract's errno
// vocabulary. The error may come from storage or from a handler-owned bound whose errno
// is already determined before storage is called.
func (h *Handler) writeOperationError(w http.ResponseWriter, err error) {
	response := ErrorResponse{Errno: storage.ErrnoNameOf(err), Message: err.Error()}
	if failure := namespaceLockFailure(err); failure != nil {
		response.LockCode = failure.Code
		recorded := failure.Recorded
		response.Recorded = &recorded
	}
	h.writeJSON(w, StatusStorageError, response)
}

// Only a single error chain can identify one lock failure. Joined failures and an
// enclosing classification own the whole outcome and cannot inherit a nested lock code.
func namespaceLockFailure(err error) *locking.Error {
	errno := storage.ErrnoOf(err)
	for err != nil {
		if failure, ok := err.(*locking.Error); ok {
			if locking.Errno(failure.Code) == errno {
				return failure
			}
			return nil
		}
		if _, classified := err.(interface{ Classification() error }); classified {
			return nil
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			children := joined.Unwrap()
			if len(children) != 1 {
				return nil
			}
			err = children[0]
			continue
		}
		wrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil
		}
		err = wrapped.Unwrap()
	}
	return nil
}

// writeFault reports that the exchange itself went wrong. It never uses
// StatusStorageError, so it cannot be read as the outcome of an operation.
func (h *Handler) writeFault(w http.ResponseWriter, status int, err error) {
	h.writeJSON(w, status, ErrorResponse{Message: err.Error()})
}

func boundedErrorResponse(response ErrorResponse, limit int64) ErrorResponse {
	// encoding/json may expand one input byte to a six-byte escape. The fixed envelope,
	// errno name and replacement diagnostic fit inside the allowance required by
	// HandlerOptions, so a message that cannot fit is replaced before json.Marshal sees it.
	const envelopeBytes int64 = 128
	if int64(len(response.Message)) > (limit-envelopeBytes)/6 {
		response.Message = "the response detail exceeds the configured HTTP body limit"
	}
	return response
}

func newListResult(limit int64) (*storage.ListResult, error) {
	return storage.NewListResult(limit, int64(len(`{"entries":[]}`)), func(i int, nameBytes int64, attr storage.Attr) (int64, error) {
		encodedAttr, err := json.Marshal(AttrOf(attr))
		if err != nil {
			return 0, fmt.Errorf("cannot size listing attributes: %w", err)
		}
		if nameBytes > limit {
			return limit + 1, nil
		}
		if int64(int(nameBytes)) != nameBytes {
			return 0, fmt.Errorf("a listing name is too large for this process: %w", syscall.EFBIG)
		}
		encodedName := int64(base64.StdEncoding.EncodedLen(int(nameBytes)))
		entryBytes := int64(len(`{"name":"","attr":}`)) + encodedName + int64(len(encodedAttr))
		if i != 0 {
			entryBytes++
		}
		return entryBytes, nil
	})
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
