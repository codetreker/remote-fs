// Package httprest carries the storage contract over HTTP: the shape of a request URL,
// the shape of a response body, the handler that answers one, and the storage.Storage
// that reaches one.
//
// Both ends live here because they share the messages, and a message type one end could
// change without the other noticing is the failure this arrangement exists to prevent.
// Neither end needs anything beyond net/http, so taking one of them links nothing that
// was not asked for.
//
// One operation is one request. Nothing here is stateful, and nothing here assumes that
// a single connection carries more than one exchange — the change-event stream this
// system will grow is a separate endpoint on a separate connection, and adding it does
// not disturb anything below.
//
// The handler holds no state of its own and caches nothing: the namespace's facts live
// in the storage it was built over, and a copy of them here could only ever be a copy
// that might be out of date. Every request is answered by calling the storage and
// reporting what it said.
//
// The obligation the dialling end exists to meet is that a failure to reach the far side
// must never arrive as an answer about the namespace. Anything that is not an outcome the
// far side stated in this protocol's own terms — a connection that never opened, a
// deadline, a body that will not parse, a body framed so that it cannot report having been
// cut short, a status nobody promised, an errno name this side does not know — is
// syscall.EIO, and a listing that failed is never an empty listing. A filesystem that
// answers "no such file" when the truth is "I could not ask" makes whatever runs on top
// delete, regenerate or overwrite, irreversibly.
package httprest

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

const (
	// Prefix begins the URL path of every request, relative to wherever the handler is
	// mounted. The version segment is the place a future incompatible change lands.
	Prefix = "/v1/"

	// HeaderProtocol names the response header that identifies an answer as having come
	// from a handler speaking this protocol, and Version is its only accepted value.
	//
	// Status codes alone cannot carry that: an intermediary — a proxy, a captive portal,
	// an authenticating gateway — can return 200 with a body of its own, and a mutation
	// whose response body nobody reads would then be reported as having succeeded when
	// it never reached the handler. Requiring a header the intermediary does not know
	// about closes that.
	HeaderProtocol = "Remote-Fs-Protocol"
	Version        = "1"

	// StatusStorageError marks the one response that carries a storage error: the
	// request reached the storage and the storage refused it. The errno itself travels
	// in the body, because the status space does not map onto errno and any mapping
	// would collapse cases the caller has to keep apart.
	//
	// Every other non-200 status means the operation's outcome is unknown.
	StatusStorageError = http.StatusUnprocessableEntity
)

// The errors ParseRequest and Request.URL report. They describe a malformed exchange,
// never the outcome of an operation, so nothing here may be mistaken for a storage
// answer.
var (
	ErrUnknownOp = errors.New("unknown operation")
	ErrMethod    = errors.New("wrong method for the operation")
	ErrOperands  = errors.New("malformed operands")
)

// Op names one storage operation. The name is the last segment of the request URL path.
type Op string

// The ten operations of the storage contract.
const (
	OpStat      Op = "stat"
	OpSetAttr   Op = "setattr"
	OpList      Op = "list"
	OpRead      Op = "read"
	OpWrite     Op = "write"
	OpCreate    Op = "create"
	OpMkdir     Op = "mkdir"
	OpRemove    Op = "remove"
	OpRemoveDir Op = "removedir"
	OpRename    Op = "rename"
)

// Query keys for the operands. Operands travel in the query string rather than in the
// URL path because the query string is the only part of a URL that survives a namespace
// path intact: http.ServeMux collapses "a//f" to "a/f" and resolves "a/b/../f" before a
// handler sees it, and an encoded slash in a path segment cannot be told apart from a
// separator. url.Values escaping round-trips any byte sequence, valid UTF-8 or not.
const (
	keyPath = "path"
	keyTo   = "to"
)

type opSpec struct {
	method   string
	operands []string
	// body is the media type of the request body, and empty for an operation that sends
	// none. Two operations send one, and they send different things: the contents of a
	// file are bytes nobody may reinterpret, an attribute change is a document.
	body string
}

const (
	contentOctets = "application/octet-stream"
	contentJSON   = "application/json"
)

var ops = map[Op]opSpec{
	OpStat:      {method: http.MethodGet, operands: []string{keyPath}},
	OpSetAttr:   {method: http.MethodPost, operands: []string{keyPath}, body: contentJSON},
	OpList:      {method: http.MethodGet, operands: []string{keyPath}},
	OpRead:      {method: http.MethodGet, operands: []string{keyPath}},
	OpWrite:     {method: http.MethodPost, operands: []string{keyPath}, body: contentOctets},
	OpCreate:    {method: http.MethodPost, operands: []string{keyPath}},
	OpMkdir:     {method: http.MethodPost, operands: []string{keyPath}},
	OpRemove:    {method: http.MethodPost, operands: []string{keyPath}},
	OpRemoveDir: {method: http.MethodPost, operands: []string{keyPath}},
	OpRename:    {method: http.MethodPost, operands: []string{keyPath, keyTo}},
}

// Request is one operation and its operands.
//
// Path is the operand every operation takes, and for OpRename it is the source. To is
// the destination and is meaningful for OpRename alone.
type Request struct {
	Op   Op
	Path string
	To   string
}

// Method reports the HTTP method the operation is sent with. Operations that only read
// use GET so that they can be retried and traced as such; every operation that changes
// the namespace uses POST.
func (r Request) Method() string {
	spec, ok := ops[r.Op]
	if !ok {
		return ""
	}
	return spec.method
}

// ContentType reports the media type the operation's request body travels as, and is
// empty for an operation that sends no body.
func (r Request) ContentType() string {
	spec, ok := ops[r.Op]
	if !ok {
		return ""
	}
	return spec.body
}

// URL renders r as a request URL under base. The base's own path is kept, so a handler
// mounted under a prefix is reachable without the two sides having to agree on anything
// beyond that prefix.
func (r Request) URL(base *url.URL) (*url.URL, error) {
	spec, ok := ops[r.Op]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownOp, r.Op)
	}
	query := url.Values{}
	for _, operand := range spec.operands {
		query.Set(operand, r.operand(operand))
	}
	u := *base
	u.Path = strings.TrimSuffix(base.Path, "/") + Prefix + string(r.Op)
	u.RawPath = ""
	u.RawQuery = query.Encode()
	u.Fragment = ""
	u.RawFragment = ""
	return &u, nil
}

// ParseRequest recovers the operation and its operands from an inbound request.
//
// It is strict on purpose. A query that does not parse, an operand that is absent, one
// that arrives twice, or one nobody asked for is an error rather than something to work
// around: url.Values reports all four as an empty string, and an empty path names the
// root — so the lenient reading of a damaged request is a request against the whole
// namespace.
func ParseRequest(method string, u *url.URL) (Request, error) {
	rest, ok := strings.CutPrefix(u.Path, Prefix)
	if !ok {
		return Request{}, fmt.Errorf("%w: %q does not begin with %q", ErrUnknownOp, u.Path, Prefix)
	}
	op := Op(rest)
	spec, ok := ops[op]
	if !ok {
		return Request{}, fmt.Errorf("%w: %q", ErrUnknownOp, rest)
	}
	if method != spec.method {
		return Request{}, fmt.Errorf("%w: %s is sent with %s, not %s", ErrMethod, op, spec.method, method)
	}

	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return Request{}, fmt.Errorf("%w: %w", ErrOperands, err)
	}
	if len(query) != len(spec.operands) {
		return Request{}, fmt.Errorf("%w: %s takes %v, got %v", ErrOperands, op, spec.operands, keysOf(query))
	}

	req := Request{Op: op}
	for _, operand := range spec.operands {
		values, present := query[operand]
		if !present || len(values) != 1 {
			return Request{}, fmt.Errorf("%w: %s takes exactly one %q, got %d", ErrOperands, op, operand, len(values))
		}
		req.setOperand(operand, values[0])
	}
	return req, nil
}

func (r Request) operand(key string) string {
	if key == keyTo {
		return r.To
	}
	return r.Path
}

func (r *Request) setOperand(key, value string) {
	if key == keyTo {
		r.To = value
		return
	}
	r.Path = value
}

func keysOf(query url.Values) []string {
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	return keys
}
