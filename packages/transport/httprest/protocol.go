// Package httprest carries the storage contract over HTTP: the shape of a request URL,
// the shape of a response body, the handler that answers one, and the storage.Storage
// that reaches one. Beside it, it carries the replication contract: the change stream a
// replica watches, and the consistent picture it starts from.
//
// Both ends live here because they share the messages, and a message type one end could
// change without the other noticing is the failure this arrangement exists to prevent.
// Neither end needs anything beyond net/http, so taking one of them links nothing that
// was not asked for.
//
// Volume calls, explicit lock controls, and replication share one protocol version.
// Lock controls retain authoritative owner and action state in the paired backend. Bulk
// bodies and replication streams have admission separate from the short lock controls.
// Replication endpoints use separate connections so snapshot delivery does not consume
// the connection carrying changes.
//
// The obligation the dialling end exists to meet is that a failure to reach the far side
// must never arrive as an answer about the volume. Anything that is not an outcome the
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
	"strconv"
	"strings"

	"github.com/codetreker/remote-fs/packages/metastore"
)

const (
	// Prefix begins the URL path of every request, relative to wherever the handler is
	// mounted. The version segment is the place a future incompatible change lands.
	Prefix = "/v3/"

	// HeaderProtocol names the response header that identifies an answer as having come
	// from a handler speaking this protocol, and Version is its only accepted value.
	//
	// Status codes alone cannot carry that: an intermediary — a proxy, a captive portal,
	// an authenticating gateway — can return 200 with a body of its own, and a mutation
	// whose response body nobody reads would then be reported as having succeeded when
	// it never reached the handler. Requiring a header the intermediary does not know
	// about closes that.
	HeaderProtocol = "Remote-Fs-Protocol"
	Version        = "3"

	// StatusStorageError marks the one response that carries an operation outcome in the
	// storage contract's errno vocabulary. Most come from the storage. A handler-owned
	// bound may also refuse an operation before storage is called when the refusal is
	// already exact — EFBIG for a write whose body is too large, for example. The errno
	// travels in the body because the status space cannot preserve these distinctions.
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

// Op names one operation. The name is the last segment of the request URL path.
type Op string

// The eleven operations of the storage contract.
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
	OpSpace     Op = "space"
)

// The three operations of the replication contract.
//
// Beginning a stream at the log's tail and continuing one from a recorded position are
// two operations rather than one with an optional resume point, and the reason is the
// same one that makes every operand mandatory below: url.Values reports a query that did
// not parse, an operand that is absent and an operand that arrived twice all as an empty
// string. An optional resume point would let a replica asking to continue at position
// 12345 be answered with a stream that begins at the tail — every change in between lost,
// no error anywhere, and nothing left afterwards by which to notice. The distinction sits
// in the URL path instead, which is the one part of a request that cannot degrade into
// something else.
const (
	OpSubscribe   Op = "subscribe"
	OpResubscribe Op = "resubscribe"
	OpSnapshot    Op = "snapshot"
)

// Query keys for the operands. Operands travel in the query string rather than in the
// URL path because the query string is the only part of a URL that survives a volume
// path intact: http.ServeMux collapses "a//f" to "a/f" and resolves "a/b/../f" before a
// handler sees it, and an encoded slash in a path segment cannot be told apart from a
// separator. url.Values escaping round-trips any byte sequence, valid UTF-8 or not.
const (
	keyPath        = "path"
	keyTo          = "to"
	keyIncarnation = "incarnation"
	keyPosition    = "position"
)

type opSpec struct {
	method   string
	operands []string
	// body is the media type of the request body, and empty for an operation that requires
	// an empty body. Two operations send one, and they send different things: the contents
	// of a file are bytes nobody may reinterpret, an attribute change is a document.
	body string
}

const (
	contentOctets = "application/octet-stream"
	contentJSON   = "application/json"
)

var ops = map[Op]opSpec{
	OpFileControl: {method: http.MethodPost, body: contentJSON},
	OpFile:        {method: http.MethodPost, body: contentJSON},
	OpStat:        {method: http.MethodGet, operands: []string{keyPath}},
	OpSetAttr:     {method: http.MethodPost, operands: []string{keyPath}, body: contentJSON},
	OpList:        {method: http.MethodGet, operands: []string{keyPath}},
	OpRead:        {method: http.MethodGet, operands: []string{keyPath}},
	OpWrite:       {method: http.MethodPost, operands: []string{keyPath}, body: contentOctets},
	OpCreate:      {method: http.MethodPost, operands: []string{keyPath}},
	OpMkdir:       {method: http.MethodPost, operands: []string{keyPath}},
	OpRemove:      {method: http.MethodPost, operands: []string{keyPath}},
	OpRemoveDir:   {method: http.MethodPost, operands: []string{keyPath}},
	OpRename:      {method: http.MethodPost, operands: []string{keyPath, keyTo}},
	// Space describes the whole volume rather than anything under a path, so it takes
	// no operands. A path sent beside it is refused like any operand nobody asked for.
	OpSpace: {method: http.MethodGet},

	// The replication endpoints read; none of them changes anything. Subscribe and
	// Snapshot both mean "as the volume is now", which is a question with no operands.
	OpSubscribe:   {method: http.MethodGet},
	OpResubscribe: {method: http.MethodGet, operands: []string{keyIncarnation, keyPosition}},
	OpSnapshot:    {method: http.MethodGet},
}

// Request is one operation and its operands.
//
// Path is the operand every storage operation but OpSpace takes, and for OpRename it is
// the source. To is the destination and is meaningful for OpRename alone. Incarnation and
// Position are the resume point and are meaningful for OpResubscribe alone. A field an
// operation does not take is not carried, so it does not survive a round trip through a
// URL.
type Request struct {
	Op   Op
	Path string
	To   string

	Incarnation metastore.Incarnation
	Position    metastore.Position
}

// Method reports the HTTP method the operation is sent with. Operations that only read
// use GET so that they can be retried and traced as such; every operation that changes
// the volume uses POST.
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
// volume.
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
		if err := req.setOperand(operand, values[0]); err != nil {
			return Request{}, fmt.Errorf("%w: %s: %w", ErrOperands, op, err)
		}
	}
	return req, nil
}

func (r Request) operand(key string) string {
	switch key {
	case keyTo:
		return r.To
	case keyIncarnation:
		return string(r.Incarnation)
	case keyPosition:
		return strconv.FormatInt(int64(r.Position), 10)
	default:
		return r.Path
	}
}

// setOperand records one operand, refusing a value that is not one.
//
// A position is the only operand that is not a byte sequence, so it is the only one that
// can arrive as something the request cannot hold. Refusing it here rather than repairing
// it is what keeps a damaged resume point from reading as position zero, which is where a
// replica that has seen nothing resumes from — the whole log replayed, or, once the log
// no longer reaches back that far, an unexplained rebuild.
func (r *Request) setOperand(key, value string) error {
	switch key {
	case keyTo:
		r.To = value
	case keyIncarnation:
		r.Incarnation = metastore.Incarnation(value)
	case keyPosition:
		position, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("%q is not a position: %w", value, err)
		}
		if position < 0 {
			return fmt.Errorf("%q is not a position: the first position is zero", value)
		}
		r.Position = metastore.Position(position)
	default:
		r.Path = value
	}
	return nil
}

func keysOf(query url.Values) []string {
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	return keys
}
