package httprest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/codetreker/remote-fs/packages/storage"
)

// Handler serves one namespace.
//
// It is a plain http.Handler, so it can be run on its own or mounted inside an existing
// server; http.StripPrefix is how it is mounted somewhere other than the root.
type Handler struct {
	storage storage.Storage
}

var _ http.Handler = (*Handler)(nil)

// NewHandler builds a handler over s.
func NewHandler(s storage.Storage) (*Handler, error) {
	if s == nil {
		return nil, errors.New("httprest: a handler needs a storage to serve")
	}
	return &Handler{storage: s}, nil
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
	}
}

// report answers an operation that produces nothing but an outcome.
func (h *Handler) report(w http.ResponseWriter, err error) {
	if err != nil {
		writeStorageError(w, err)
		return
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
