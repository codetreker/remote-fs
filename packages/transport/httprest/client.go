package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// Storage is a namespace held by a server: its contents through the storage contract, and
// beside that the replication endpoints a replica of its metadata is built and kept from.
// One Dial is one namespace, and a replica needs both halves of it.
type Storage struct {
	base *url.URL
	http *http.Client
	// maxBodyBytes bounds non-streaming responses and non-write requests.
	maxBodyBytes int64
	// maxWriteBytes bounds file-content requests before they are sent.
	maxWriteBytes int64
	// maxFrameBytes bounds one replication frame retained by a stream reader.
	maxFrameBytes int64
	responses     *bodyAdmission

	// silence is how long a stream may say nothing at all before this side stops believing
	// it is being delivered.
	silence time.Duration
}

// DefaultSilence is how long a stream may say nothing before a caller with no reason of its
// own to choose stops believing in it.
//
// It holds every stream this package opens, changes and snapshots alike, so both have to be
// kept spoken for. A server working through a large tree has nothing to send for as long as
// that takes, and to a reader that is the same nothing an abandoned connection produces.
//
// It has to be a comfortable multiple of the interval the server sends its keepalives at —
// Limits.Keepalive, ten seconds by default — because the two are configured separately and
// a bound below that interval would sever every healthy stream on a timer. Three times it,
// so that losing one keepalive to a stall is not a broken stream. Limits.SnapshotDeadline is
// a larger figure than this and is not the thing to compare it against: that bounds a whole
// delivery, this bounds a silence.
//
// What it bounds is how long a replica may go on answering from a copy whose stream has
// stopped arriving without saying so — a machine that vanished, a firewall that dropped the
// flow, a partition. Nothing shorter than the network's own scheduling is safe, and nothing
// longer is honest; this is chosen rather than measured, like the other bounds here.
const DefaultSilence = 30 * time.Second

var _ storage.Storage = (*Storage)(nil)
var _ storage.BoundedStorage = (*Storage)(nil)

// Dial reaches the namespace served at baseURL, which must be absolute. A base that
// carries a path prefix is honoured, so a handler mounted inside a larger server is
// reachable.
//
// The http.Client is the caller's, and required rather than defaulted, because it
// carries the timeout policy. How long an operation may hang before it is reported as a
// failure is a decision this package cannot make on the caller's behalf: with no timeout
// on the client and no deadline on the context, a call waits indefinitely.
//
// Nothing is contacted here. The name says what the result is for — a storage obtained
// across the wire — rather than promising that the far side answered; whether it is there
// is the answer to the first operation, and to every one after it. The retained-body and
// stream-silence bounds come from DefaultDialOptions.
func Dial(baseURL string, httpClient *http.Client) (*Storage, error) {
	return DialWithOptions(baseURL, httpClient, DefaultDialOptions())
}

// DialWithSilence is Dial with the bound on a quiet stream given rather than defaulted. What
// that bound is for, and what it has to be a multiple of, is on DefaultSilence.
func DialWithSilence(baseURL string, httpClient *http.Client, silence time.Duration) (*Storage, error) {
	if silence <= 0 {
		return nil, fmt.Errorf("httprest: a stream allowed to say nothing for %v is a stream nothing is watching", silence)
	}
	options := DefaultDialOptions()
	options.Silence = silence
	return DialWithOptions(baseURL, httpClient, options)
}

// DialWithOptions is Dial with explicit bounds on quiet streams and retained HTTP bodies.
func DialWithOptions(baseURL string, httpClient *http.Client, options DialOptions) (*Storage, error) {
	if httpClient == nil {
		return nil, errors.New("httprest: an HTTP client is required; it carries the timeout policy")
	}
	settled, err := options.settle()
	if err != nil {
		return nil, err
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("httprest: %q is not a URL: %w", baseURL, err)
	}
	if base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("httprest: %q needs a scheme and a host", baseURL)
	}
	return &Storage{
		base:          base,
		http:          httpClient,
		maxBodyBytes:  settled.MaxBodyBytes,
		maxWriteBytes: settled.MaxWriteBytes,
		maxFrameBytes: settled.MaxFrameBytes,
		responses: newBodyAdmission(
			settled.MaxConcurrentResponses,
			settled.MaxInFlightResponseBytes,
			settled.MaxWaitingResponses,
		),
		silence: settled.Silence,
	}, nil
}

func (s *Storage) CheckBounded() error { return nil }

func (s *Storage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	req := Request{Op: OpStat, Path: path}
	answer, err := s.call(ctx, req, nil)
	if err != nil {
		return storage.Attr{}, err
	}
	defer answer.release()
	// A body that is not this operation's answer — including one that carries no
	// attributes at all — does not decode, so there is nothing further to check here.
	var resp StatResponse
	if err := json.Unmarshal(answer.content, &resp); err != nil {
		return storage.Attr{}, unreachable(req, err)
	}
	return resp.Attr.Storage(), nil
}

func (s *Storage) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	_, err := s.setAttr(ctx, path, change)
	return err
}

func (s *Storage) SetAttrWithBarrier(ctx context.Context, path string, change storage.AttrChange) (MutationBarrier, error) {
	barrier, err := s.setAttr(ctx, path, change)
	return requireMutationBarrier(Request{Op: OpSetAttr, Path: path}, barrier, err)
}

func (s *Storage) setAttr(ctx context.Context, path string, change storage.AttrChange) (*MutationBarrier, error) {
	req := Request{Op: OpSetAttr, Path: path}
	body, err := json.Marshal(SetAttrRequest{Change: AttrChangeOf(change)})
	if err != nil {
		// Rendered from a mode and two instants, so nothing here can refuse to encode.
		// Reporting it as an outcome nobody knows is still the only honest answer, since
		// the request was never sent.
		return nil, unreachable(req, err)
	}
	return s.change(ctx, req, body)
}

func (s *Storage) List(ctx context.Context, path string) ([]storage.Entry, error) {
	req := Request{Op: OpList, Path: path}
	answer, err := s.call(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	defer answer.release()
	var resp ListResponse
	if err := json.Unmarshal(answer.content, &resp); err != nil {
		return nil, unreachable(req, err)
	}
	return resp.Storage(), nil
}

func (s *Storage) ListBounded(ctx context.Context, path string, result *storage.ListResult) (returned error) {
	if result == nil {
		return &operationError{req: Request{Op: OpList, Path: path}, errno: syscall.EINVAL, detail: "a bounded listing needs a result"}
	}
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	req := Request{Op: OpList, Path: path}
	limit := min(result.MaxBytes(), s.maxBodyBytes)
	answer, err := s.callWithin(ctx, req, nil, limit)
	if err != nil {
		return err
	}
	defer answer.release()
	if err := decodeListInto(answer.content, result); err != nil {
		return unreachable(req, err)
	}
	return nil
}

func (s *Storage) Read(ctx context.Context, path string) ([]byte, error) {
	answer, err := s.call(ctx, Request{Op: OpRead, Path: path}, nil)
	if err != nil {
		return nil, err
	}
	answer.release()
	return answer.content, nil
}

func (s *Storage) ReadBounded(ctx context.Context, path string, maxBytes int64) ([]byte, error) {
	req := Request{Op: OpRead, Path: path}
	if maxBytes <= 0 {
		return nil, &operationError{req: req, errno: syscall.EINVAL, detail: "a bounded read needs a positive byte limit"}
	}
	answer, err := s.callWithin(ctx, req, nil, min(maxBytes, s.maxBodyBytes))
	if err != nil {
		return nil, err
	}
	answer.release()
	return answer.content, nil
}

func (s *Storage) Write(ctx context.Context, path string, content []byte) error {
	_, err := s.change(ctx, Request{Op: OpWrite, Path: path}, content)
	return err
}

func (s *Storage) WriteWithBarrier(ctx context.Context, path string, content []byte) (MutationBarrier, error) {
	req := Request{Op: OpWrite, Path: path}
	barrier, err := s.change(ctx, req, content)
	return requireMutationBarrier(req, barrier, err)
}

func (s *Storage) Create(ctx context.Context, path string) error {
	_, err := s.change(ctx, Request{Op: OpCreate, Path: path}, nil)
	return err
}

func (s *Storage) CreateWithBarrier(ctx context.Context, path string) (MutationBarrier, error) {
	req := Request{Op: OpCreate, Path: path}
	barrier, err := s.change(ctx, req, nil)
	return requireMutationBarrier(req, barrier, err)
}

func (s *Storage) Mkdir(ctx context.Context, path string) error {
	_, err := s.change(ctx, Request{Op: OpMkdir, Path: path}, nil)
	return err
}

func (s *Storage) MkdirWithBarrier(ctx context.Context, path string) (MutationBarrier, error) {
	req := Request{Op: OpMkdir, Path: path}
	barrier, err := s.change(ctx, req, nil)
	return requireMutationBarrier(req, barrier, err)
}

func (s *Storage) Remove(ctx context.Context, path string) error {
	_, err := s.change(ctx, Request{Op: OpRemove, Path: path}, nil)
	return err
}

func (s *Storage) RemoveWithBarrier(ctx context.Context, path string) (MutationBarrier, error) {
	req := Request{Op: OpRemove, Path: path}
	barrier, err := s.change(ctx, req, nil)
	return requireMutationBarrier(req, barrier, err)
}

func (s *Storage) RemoveDir(ctx context.Context, path string) error {
	_, err := s.change(ctx, Request{Op: OpRemoveDir, Path: path}, nil)
	return err
}

func (s *Storage) RemoveDirWithBarrier(ctx context.Context, path string) (MutationBarrier, error) {
	req := Request{Op: OpRemoveDir, Path: path}
	barrier, err := s.change(ctx, req, nil)
	return requireMutationBarrier(req, barrier, err)
}

func (s *Storage) Rename(ctx context.Context, from, to string) error {
	_, err := s.change(ctx, Request{Op: OpRename, Path: from, To: to}, nil)
	return err
}

func (s *Storage) RenameWithBarrier(ctx context.Context, from, to string) (MutationBarrier, error) {
	req := Request{Op: OpRename, Path: from, To: to}
	barrier, err := s.change(ctx, req, nil)
	return requireMutationBarrier(req, barrier, err)
}

// Space reports the room the namespace behind the wire has.
//
// A namespace with no room of its own to report says so with ENOSYS, which arrives as an
// ordinary storage error under that name. This side cannot know statically what is behind
// it, so the refusal is an answer rather than an absent method.
func (s *Storage) Space(ctx context.Context) (storage.Space, error) {
	req := Request{Op: OpSpace}
	answer, err := s.call(ctx, req, nil)
	if err != nil {
		return storage.Space{}, err
	}
	defer answer.release()
	// A body that is not this operation's answer — one missing a count, or carrying
	// figures that cannot all be true — does not decode, so there is nothing further to
	// check here.
	var resp SpaceResponse
	if err := json.Unmarshal(answer.content, &resp); err != nil {
		return storage.Space{}, unreachable(req, err)
	}
	return resp.Space.Storage(), nil
}

// change performs an operation whose answer is an exact MutationResponse. A logged namespace
// supplies the barrier needed for read-after-write confirmation; a logless namespace supplies
// an empty JSON object. Any other successful body is not this protocol's answer.
func (s *Storage) change(ctx context.Context, req Request, content []byte) (*MutationBarrier, error) {
	answer, err := s.call(ctx, req, content)
	if err != nil {
		return nil, err
	}
	defer answer.release()
	var response MutationResponse
	if err := json.Unmarshal(answer.content, &response); err != nil {
		return nil, unreachable(req, fmt.Errorf("the mutation response does not decode: %w", err))
	}
	return response.Barrier, nil
}

func requireMutationBarrier(req Request, barrier *MutationBarrier, err error) (MutationBarrier, error) {
	if err != nil {
		return MutationBarrier{}, err
	}
	if barrier == nil {
		return MutationBarrier{}, unreachable(req, errors.New("the successful mutation response carried no replication barrier"))
	}
	return *barrier, nil
}

// call performs one operation and returns the response body. content is the request
// payload, and nil for the operations that send none.
func (s *Storage) call(ctx context.Context, req Request, content []byte) (*retainedBody, error) {
	return s.callWithin(ctx, req, content, s.maxBodyBytes)
}

func (s *Storage) callWithin(ctx context.Context, req Request, content []byte, successLimit int64) (*retainedBody, error) {
	limit := s.maxBodyBytes
	if req.Op == OpWrite {
		limit = s.maxWriteBytes
	}
	if int64(len(content)) > limit {
		tooLarge := fmt.Errorf("the request body is %d bytes, above the configured limit of %d", len(content), limit)
		if req.Op == OpWrite {
			return nil, &operationError{req: req, errno: syscall.EFBIG, detail: tooLarge.Error()}
		}
		return nil, unreachable(req, tooLarge)
	}
	release, err := s.responses.acquire(ctx, retainedResponseMultiplier*s.maxBodyBytes)
	if err != nil {
		if errors.Is(err, syscall.EAGAIN) {
			return nil, &operationError{req: req, errno: syscall.EAGAIN, detail: err.Error()}
		}
		return nil, unreachable(req, err)
	}
	retained := false
	defer func() {
		if !retained {
			release()
		}
	}()
	u, err := req.URL(s.base)
	if err != nil {
		return nil, unreachable(req, err)
	}

	var payload io.Reader
	if content != nil {
		payload = bytes.NewReader(content)
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.Method(), u.String(), payload)
	if err != nil {
		return nil, unreachable(req, err)
	}
	if content != nil {
		httpReq.Header.Set("Content-Type", req.ContentType())
	}

	resp, err := s.http.Do(httpReq)
	if err != nil {
		return nil, unreachable(req, err)
	}
	defer resp.Body.Close()

	// An answer that does not carry the protocol's own mark did not come from a server
	// speaking it. A proxy, a captive portal or an authenticating gateway can return a
	// 200 of its own, and a mutation whose body nobody reads would otherwise be taken
	// for a change that happened.
	if got := resp.Header.Get(HeaderProtocol); got != Version {
		return nil, unreachable(req, fmt.Errorf("the response is marked %q, not %q", got, Version))
	}

	switch resp.StatusCode {
	case http.StatusOK:
		body, err := readWhole(resp, successLimit)
		if errors.Is(err, errBodyTooLarge) && req.Op == OpRead {
			return nil, &operationError{req: req, errno: syscall.EFBIG, detail: err.Error()}
		}
		if err != nil {
			return nil, unreachable(req, err)
		}
		retained = true
		return &retainedBody{content: body, done: release}, nil
	case StatusStorageError:
		body, err := readWhole(resp, s.maxBodyBytes)
		if err != nil {
			return nil, unreachable(req, err)
		}
		return nil, s.storageError(req, body)
	default:
		return nil, unreachable(req, fmt.Errorf("the server answered %s", resp.Status))
	}
}

type retainedBody struct {
	content []byte
	done    func()
}

func (b *retainedBody) release() {
	if b != nil && b.done != nil {
		b.done()
		b.done = nil
	}
}

func decodeListInto(content []byte, result *storage.ListResult) error {
	decoder := json.NewDecoder(bytes.NewReader(content))
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("reading the listing response object: %w", err)
	}
	if token != json.Delim('{') {
		return fmt.Errorf("the listing response begins with %v, not an object", token)
	}
	if !decoder.More() {
		return errors.New("the response carried no listing")
	}
	field, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("reading the listing response field: %w", err)
	}
	if field != "entries" {
		return fmt.Errorf("the listing response begins with field %q, want entries", field)
	}
	token, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("reading the listing array: %w", err)
	}
	if token != json.Delim('[') {
		return errors.New("the response carried no listing array")
	}
	for decoder.More() {
		var entry Entry
		if err := decoder.Decode(&entry); err != nil {
			return err
		}
		if err := result.Add(storage.Entry{Name: string(entry.Name), Attr: entry.Attr.Storage()}); err != nil {
			return err
		}
	}
	if token, err = decoder.Token(); err != nil {
		return fmt.Errorf("reading the end of the listing array: %w", err)
	}
	if token != json.Delim(']') {
		return errors.New("the listing array did not end")
	}
	if decoder.More() {
		return errors.New("the listing response carried fields other than entries")
	}
	if token, err = decoder.Token(); err != nil {
		return fmt.Errorf("reading the end of the listing response: %w", err)
	}
	if token != json.Delim('}') {
		return errors.New("the listing response object did not end")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("the listing response has trailing content: %w", err)
	}
	return nil
}

// storageError turns the one response that states an outcome into that outcome. A body
// it cannot read, or an errno name it does not hold, means this side does not know what
// it was told — which is not the same as knowing the operation failed in a particular
// way, and must not be reported as if it were.
func (s *Storage) storageError(req Request, body []byte) error {
	var resp ErrorResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return unreachable(req, err)
	}
	errno, ok := storage.ErrnoByName(resp.Errno)
	if !ok {
		return unreachable(req, fmt.Errorf("the server reported errno %q, which this side does not know", resp.Errno))
	}
	return &operationError{req: req, errno: errno, detail: resp.Message}
}

// readWhole returns the entire response body without retaining more than limit bytes, and
// refuses a response that could not have told it the body was cut short.
//
// A body that ends early is the one failure that looks like valid data: a file read short
// is still a file, and nothing downstream can tell it from one that is genuinely that
// size. Whether it can be caught at all is decided by the framing. A declared length is
// compared against what arrived. A chunked body ends with a terminating chunk, and
// net/http reports a missing one. A Content-Encoding the transport decoded leaves
// Uncompressed set, and gzip's trailer carries a length and a checksum the decoder
// verifies. Each of those three fails an early end on its own.
//
// Close-delimited HTTP/1.x is the one that cannot: with no length and no chunking the body
// ends when the connection does, so a connection dropped mid-file is byte for byte a
// complete answer. There is nothing to check afterwards, which is why the response is
// refused before it is read — including when it did arrive whole, because that case is
// indistinguishable. The handler in this package declares a length on every response, so
// this refuses nothing it produces; an intermediary re-framing a response is where it
// arises.
func readWhole(resp *http.Response, limit int64) ([]byte, error) {
	if resp.ContentLength < 0 && len(resp.TransferEncoding) == 0 && !resp.Uncompressed {
		return nil, errors.New("the response body ends where the connection does, so a body cut short cannot be told from a whole one")
	}
	if resp.ContentLength > limit {
		return nil, fmt.Errorf("%w of %d bytes: %d bytes were announced", errBodyTooLarge, limit, resp.ContentLength)
	}
	body, err := readAtMost(resp.Body, limit)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			return nil, err
		}
		return nil, fmt.Errorf("the response body ended early: %w", err)
	}
	if resp.ContentLength >= 0 && int64(len(body)) != resp.ContentLength {
		return nil, fmt.Errorf("the response body carried %d bytes, %d were announced", len(body), resp.ContentLength)
	}
	return body, nil
}

// operationError is every error this package returns but one, the exception being the
// RebuildError a change stream answers with.
//
// Unwrap yields the errno alone and never the underlying cause, which is why the cause
// is carried as rendered text instead. A cause left in the errors.Is chain would leak
// errnos that belong to the network into answers about the namespace: dialling a Unix
// socket that is not there produces a chain containing syscall.ENOENT, and a caller
// asking errors.Is(err, syscall.ENOENT) would be told the file does not exist when the
// truth is that the server was never reached.
type operationError struct {
	req    Request
	errno  syscall.Errno
	detail string
}

func (e *operationError) Error() string {
	if e.detail == "" {
		return fmt.Sprintf("%s: %v", e.req.subject(), e.errno)
	}
	return fmt.Sprintf("%s: %s: %v", e.req.subject(), e.detail, e.errno)
}

// subject names the operation and what it was aimed at. An operation taking no operands
// names nothing further: OpSpace describes the whole namespace, and an empty path quoted
// beside it would read as a report about the root.
func (r Request) subject() string {
	switch {
	case r.Op == OpRename:
		return fmt.Sprintf("%s %s to %s", r.Op, strconv.Quote(r.Path), strconv.Quote(r.To))
	case r.Op == OpResubscribe:
		return fmt.Sprintf("%s at position %d of log %s", r.Op, r.Position, strconv.Quote(string(r.Incarnation)))
	case len(ops[r.Op].operands) == 0:
		return string(r.Op)
	default:
		return fmt.Sprintf("%s %s", r.Op, strconv.Quote(r.Path))
	}
}

func (e *operationError) Unwrap() error { return e.errno }

// unreachable reports that the outcome of req is unknown.
func unreachable(req Request, cause error) error {
	return &operationError{req: req, errno: syscall.EIO, detail: cause.Error()}
}
