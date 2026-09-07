package httprest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// newHandler returns a handler over a fresh temporary directory, and that directory, so
// that a test can check what actually landed on disk instead of believing the response. A
// local directory keeps no change log, so this handler serves an unreplicable namespace.
func newHandler(t *testing.T) (http.Handler, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := localdir.New(dir)
	if err != nil {
		t.Fatalf("open the namespace: %v", err)
	}
	h, err := httprest.NewHandler(s, nil)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	return h, dir
}

func serve(t *testing.T, h http.Handler, req httprest.Request, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	base, err := url.Parse("http://server.invalid")
	if err != nil {
		t.Fatal(err)
	}
	u, err := req.URL(base)
	if err != nil {
		t.Fatalf("build the URL for %+v: %v", req, err)
	}
	r := httptest.NewRequest(req.Method(), u.String(), body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestNewHandlerRejectsAMissingStorage(t *testing.T) {
	if _, err := httprest.NewHandler(nil, nil); err == nil {
		t.Fatal("NewHandler with no storage succeeded, want an error")
	}
}

// Every bound on the replication endpoints has to leave room for one of whatever it
// bounds. A handler built with a bound of zero would answer every snapshot with EAGAIN, or
// read the log a page of no changes at a time forever — both of which look like a working
// server that never delivers anything.
func TestNewHandlerRejectsBoundsWithNoRoomInThem(t *testing.T) {
	usable := httprest.DefaultLimits()
	cases := map[string]func(*httprest.Limits){
		"no subscriptions at all":    func(l *httprest.Limits) { l.MaxSubscriptions = 0 },
		"no snapshots at all":        func(l *httprest.Limits) { l.Snapshots = 0 },
		"no time to send one in":     func(l *httprest.Limits) { l.SnapshotDeadline = 0 },
		"pages of no rows":           func(l *httprest.Limits) { l.SnapshotPage = 0 },
		"pages of no changes":        func(l *httprest.Limits) { l.EventPage = 0 },
		"a negative number of them":  func(l *httprest.Limits) { l.Snapshots = -1 },
		"pathological subscriptions": func(l *httprest.Limits) { l.MaxSubscriptions = math.MaxInt },
		"pathological snapshots":     func(l *httprest.Limits) { l.Snapshots = math.MaxInt },
		"pathological snapshot page": func(l *httprest.Limits) { l.SnapshotPage = math.MaxInt },
		"pathological event page":    func(l *httprest.Limits) { l.EventPage = math.MaxInt },
	}
	for name, spoil := range cases {
		t.Run(name, func(t *testing.T) {
			limits := usable
			spoil(&limits)
			if _, err := httprest.NewHandlerWithLimits(failing{syscall.EIO}, nil, limits); err == nil {
				t.Fatalf("NewHandlerWithLimits(%+v) succeeded, want an error", limits)
			}
		})
	}
	if _, err := httprest.NewHandlerWithLimits(failing{syscall.EIO}, nil, usable); err != nil {
		// Without this the cases above would pass for a handler that refuses every set of
		// bounds there is.
		t.Fatalf("NewHandlerWithLimits with usable bounds failed: %v", err)
	}
}

func TestNewHandlerOptionsHaveBoundedDefaultsAndRejectInvalidBounds(t *testing.T) {
	zero := httprest.HandlerOptions{}
	if err := zero.Check(); err != nil {
		t.Fatalf("zero HandlerOptions did not validate with bounded defaults: %v", err)
	}
	if _, err := httprest.NewHandlerWithOptions(failing{syscall.EIO}, nil, zero); err != nil {
		t.Fatalf("zero HandlerOptions did not select bounded defaults: %v", err)
	}
	inheritedWrite := httprest.DefaultHandlerOptions()
	inheritedWrite.MaxBodyBytes = 5 << 20
	inheritedWrite.MaxWriteBytes = 0
	inheritedWrite.MaxInFlightBodyBytes = 5 << 20
	if err := inheritedWrite.Check(); err != nil {
		t.Fatalf("zero MaxWriteBytes did not inherit MaxBodyBytes: %v", err)
	}
	usable := httprest.DefaultHandlerOptions()
	cases := map[string]func(*httprest.HandlerOptions){
		"body below the protocol minimum": func(o *httprest.HandlerOptions) { o.MaxBodyBytes = 1 },
		"negative body bytes":             func(o *httprest.HandlerOptions) { o.MaxBodyBytes = -1 },
		"an effectively unbounded body":   func(o *httprest.HandlerOptions) { o.MaxBodyBytes = math.MaxInt64 },
		"body too large for response accounting": func(o *httprest.HandlerOptions) {
			o.MaxBodyBytes = math.MaxInt64/4 + 1
			o.MaxInFlightBodyBytes = o.MaxBodyBytes
			o.MaxInFlightResponseBytes = math.MaxInt64
		},
		"negative write bytes":          func(o *httprest.HandlerOptions) { o.MaxWriteBytes = -1 },
		"write above the protocol body": func(o *httprest.HandlerOptions) { o.MaxWriteBytes = o.MaxBodyBytes + 1 },
		"negative body operations":      func(o *httprest.HandlerOptions) { o.MaxConcurrentBodies = -1 },
		"negative body waiters":         func(o *httprest.HandlerOptions) { o.MaxWaitingBodies = -1 },
		"aggregate below one body": func(o *httprest.HandlerOptions) {
			o.MaxInFlightBodyBytes = o.MaxBodyBytes - 1
		},
		"negative response operations": func(o *httprest.HandlerOptions) { o.MaxConcurrentResponses = -1 },
		"negative response waiters":    func(o *httprest.HandlerOptions) { o.MaxWaitingResponses = -1 },
		"aggregate below one complete response": func(o *httprest.HandlerOptions) {
			o.MaxInFlightResponseBytes = 4*o.MaxBodyBytes - 1
		},
		"frame below the protocol minimum": func(o *httprest.HandlerOptions) { o.MaxFrameBytes = 1 },
		"an effectively unbounded frame":   func(o *httprest.HandlerOptions) { o.MaxFrameBytes = math.MaxInt64 },
		"negative frame operations":        func(o *httprest.HandlerOptions) { o.MaxConcurrentSnapshotFrames = -1 },
		"negative frame waiters":           func(o *httprest.HandlerOptions) { o.MaxWaitingSnapshotFrames = -1 },
		"aggregate below one complete frame": func(o *httprest.HandlerOptions) {
			o.MaxFrameBytes = 1024
			o.MaxInFlightSnapshotFrameBytes = 3*o.MaxFrameBytes - 1
		},
	}
	for name, spoil := range cases {
		t.Run(name, func(t *testing.T) {
			options := usable
			spoil(&options)
			if err := options.Check(); err == nil {
				t.Fatalf("HandlerOptions.Check accepted %+v", options)
			}
			if _, err := httprest.NewHandlerWithOptions(failing{syscall.EIO}, nil, options); err == nil {
				t.Fatalf("NewHandlerWithOptions(%+v) succeeded, want an error", options)
			}
		})
	}
}

func TestHandlerRefusesStorageWithoutBoundedResults(t *testing.T) {
	dir := t.TempDir()
	bounded, err := localdir.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	unbounded := struct{ storage.Storage }{Storage: bounded}
	if _, err := httprest.NewHandler(unbounded, nil); err == nil {
		t.Fatal("NewHandler accepted a storage without bounded read and list capabilities")
	}
}

func TestHandlerUsesBoundedReadAndListEntrypoints(t *testing.T) {
	probe := &boundedEntrypointProbe{failing: failing{err: syscall.EIO}}
	options := httprest.DefaultHandlerOptions()
	options.MaxBodyBytes = 1024
	options.MaxWriteBytes = 1024
	options.MaxInFlightBodyBytes = 1024
	options.MaxInFlightResponseBytes = 4 * options.MaxBodyBytes
	h, err := httprest.NewHandlerWithOptions(probe, nil, options)
	if err != nil {
		t.Fatal(err)
	}

	read := serve(t, h, httprest.Request{Op: httprest.OpRead, Path: "large"}, nil)
	var readError httprest.ErrorResponse
	if err := json.Unmarshal(read.Body.Bytes(), &readError); err != nil {
		t.Fatal(err)
	}
	if readError.Errno != "EFBIG" || probe.readLimit != options.MaxBodyBytes {
		t.Fatalf("bounded read answered %+v after receiving limit %d", readError, probe.readLimit)
	}

	listed := serve(t, h, httprest.Request{Op: httprest.OpList, Path: "large"}, nil)
	var listError httprest.ErrorResponse
	if err := json.Unmarshal(listed.Body.Bytes(), &listError); err != nil {
		t.Fatal(err)
	}
	if listError.Errno != "EIO" || !probe.listCalled {
		t.Fatalf("bounded list answered %+v, called=%t", listError, probe.listCalled)
	}
}

// Every response has to be identifiable as this protocol's, including the ones that
// report a failure, because the client refuses anything that is not.
func TestEveryResponseIsMarked(t *testing.T) {
	h, _ := newHandler(t)
	cases := []struct {
		name string
		req  httprest.Request
	}{
		{"a success", httprest.Request{Op: httprest.OpList}},
		{"a storage error", httprest.Request{Op: httprest.OpStat, Path: "missing"}},
		{"a rejected path", httprest.Request{Op: httprest.OpStat, Path: "../outside"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := serve(t, h, c.req, nil)
			if got := w.Header().Get(httprest.HeaderProtocol); got != httprest.Version {
				t.Fatalf("%s header is %q, want %q", httprest.HeaderProtocol, got, httprest.Version)
			}
			// A cached answer from a filesystem is a stale answer that reads exactly
			// like a current one.
			if got := w.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("Cache-Control is %q, want no-store", got)
			}
		})
	}
}

func TestSuccessIsAlwaysStatus200(t *testing.T) {
	h, dir := newHandler(t)
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, req := range []httprest.Request{
		{Op: httprest.OpStat, Path: "f"},
		{Op: httprest.OpList},
		{Op: httprest.OpRead, Path: "f"},
		{Op: httprest.OpCreate, Path: "new"},
		{Op: httprest.OpMkdir, Path: "d"},
		{Op: httprest.OpRename, Path: "new", To: "renamed"},
		{Op: httprest.OpRemove, Path: "renamed"},
		{Op: httprest.OpRemoveDir, Path: "d"},
		{Op: httprest.OpSpace},
	} {
		if w := serve(t, h, req, nil); w.Code != http.StatusOK {
			t.Fatalf("%s %q answered %d, want 200: %s", req.Op, req.Path, w.Code, w.Body)
		}
	}
	if w := serve(t, h, httprest.Request{Op: httprest.OpSetAttr, Path: "f"}, changeBody(t, storage.AttrChange{})); w.Code != http.StatusOK {
		t.Fatalf("setattr answered %d, want 200: %s", w.Code, w.Body)
	}
}

// changeBody renders an attribute change the way the client sends one.
func changeBody(t *testing.T, change storage.AttrChange) io.Reader {
	t.Helper()
	encoded, err := json.Marshal(httprest.SetAttrRequest{Change: httprest.AttrChangeOf(change)})
	if err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(encoded)
}

// The change reaches the storage whole, which is checked on the directory rather than in
// the response: the response says only that it happened.
func TestSetAttrReachesTheStorage(t *testing.T) {
	h, dir := newHandler(t)
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	mode := fs.FileMode(0o600)
	changed := time.Unix(1755000000, 123456789)
	w := serve(t, h, httprest.Request{Op: httprest.OpSetAttr, Path: "f"},
		changeBody(t, storage.AttrChange{Mode: &mode, ModTime: &changed}))
	if w.Code != http.StatusOK {
		t.Fatalf("setattr answered %d, want 200: %s", w.Code, w.Body)
	}

	info, err := os.Stat(filepath.Join(dir, "f"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode() != mode {
		t.Fatalf("the file on disk has mode %v, want %v", info.Mode(), mode)
	}
	if !info.ModTime().Equal(changed) {
		t.Fatalf("the file on disk is dated %v, want %v", info.ModTime(), changed)
	}
}

// A body that did not arrive whole leaves the caller's intended change unknown. Applying
// the part of it that did arrive would leave the rest at whatever it was and answer that
// the request was carried out.
func TestAMalformedChangeChangesNothing(t *testing.T) {
	const existing = "the previous contents, which must survive"
	cases := map[string]io.Reader{
		"a body that is not JSON":            bytes.NewReader([]byte("not json")),
		"a body carrying no change":          bytes.NewReader([]byte(`{}`)),
		"a change with a mode it cannot use": bytes.NewReader([]byte(`{"change":{"mode":"rwx"}}`)),
		"a body that ends early":             &errorAfter{[]byte(`{"change":{"mo`), errors.New("connection reset")},
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			h, dir := newHandler(t)
			if err := os.WriteFile(filepath.Join(dir, "f"), []byte(existing), 0o600); err != nil {
				t.Fatal(err)
			}
			w := serve(t, h, httprest.Request{Op: httprest.OpSetAttr, Path: "f"}, body)
			if w.Code == http.StatusOK {
				t.Fatal("a malformed change answered 200")
			}
			// Not the one status the client reads an errno out of: the request never
			// reached the storage, so nothing about the namespace was established.
			if w.Code == httprest.StatusStorageError {
				t.Fatal("a malformed change was reported as a storage error")
			}
			info, err := os.Stat(filepath.Join(dir, "f"))
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode() != 0o600 {
				t.Fatalf("the file on disk has mode %v, want the untouched %v", info.Mode(), fs.FileMode(0o600))
			}
		})
	}
}

func TestAStorageErrorCarriesItsErrnoByName(t *testing.T) {
	h, dir := newHandler(t)
	if err := os.Mkdir(filepath.Join(dir, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		req  httprest.Request
		want string
	}{
		{httprest.Request{Op: httprest.OpStat, Path: "missing"}, "ENOENT"},
		{httprest.Request{Op: httprest.OpRead, Path: "d"}, "EISDIR"},
		{httprest.Request{Op: httprest.OpList, Path: "missing"}, "ENOENT"},
		{httprest.Request{Op: httprest.OpMkdir, Path: "d"}, "EEXIST"},
		{httprest.Request{Op: httprest.OpRemove, Path: "d"}, "EISDIR"},
		{httprest.Request{Op: httprest.OpStat, Path: "../outside"}, "EINVAL"},
		{httprest.Request{Op: httprest.OpRename, Path: "d", To: "../outside"}, "EINVAL"},
	}
	for _, c := range cases {
		w := serve(t, h, c.req, nil)
		if w.Code != httprest.StatusStorageError {
			t.Fatalf("%s %q answered %d, want %d", c.req.Op, c.req.Path, w.Code, httprest.StatusStorageError)
		}
		var resp httprest.ErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("%s %q answered an unparseable body %s: %v", c.req.Op, c.req.Path, w.Body, err)
		}
		if resp.Errno != c.want {
			t.Fatalf("%s %q reported errno %q, want %q", c.req.Op, c.req.Path, resp.Errno, c.want)
		}
	}

	// setattr carries a body, so it is its own case. The mode names a kind of node rather
	// than a permission, which the storage refuses; the errno has to arrive by name like
	// any other.
	kind := fs.ModeSymlink | 0o644
	for _, c := range []struct {
		change storage.AttrChange
		path   string
		want   string
	}{
		{storage.AttrChange{}, "missing", "ENOENT"},
		{storage.AttrChange{Mode: &kind}, "d", "EINVAL"},
		{storage.AttrChange{}, "../outside", "EINVAL"},
	} {
		w := serve(t, h, httprest.Request{Op: httprest.OpSetAttr, Path: c.path}, changeBody(t, c.change))
		if w.Code != httprest.StatusStorageError {
			t.Fatalf("setattr %q answered %d, want %d: %s", c.path, w.Code, httprest.StatusStorageError, w.Body)
		}
		var resp httprest.ErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("setattr %q answered an unparseable body %s: %v", c.path, w.Body, err)
		}
		if resp.Errno != c.want {
			t.Fatalf("setattr %q reported errno %q, want %q", c.path, resp.Errno, c.want)
		}
	}
}

// A storage that reports something outside the protocol's vocabulary leaves the server
// unable to name what happened. It has to say so, not pick the nearest name.
func TestAnUnnameableFailureBecomesEIO(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"an errno outside the vocabulary", syscall.ECONNREFUSED},
		{"an error carrying no errno", errors.New("the backing store is on fire")},
		{"errno zero", syscall.Errno(0)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, err := httprest.NewHandler(failing{c.err}, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, req := range []httprest.Request{
				{Op: httprest.OpStat, Path: "f"},
				{Op: httprest.OpList, Path: "f"},
				{Op: httprest.OpRead, Path: "f"},
				{Op: httprest.OpCreate, Path: "f"},
				{Op: httprest.OpSpace},
			} {
				w := serve(t, h, req, nil)
				if w.Code != httprest.StatusStorageError {
					t.Fatalf("%s answered %d, want %d", req.Op, w.Code, httprest.StatusStorageError)
				}
				var resp httprest.ErrorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Fatalf("%s answered an unparseable body %s: %v", req.Op, w.Body, err)
				}
				if resp.Errno != "EIO" {
					t.Fatalf("%s reported errno %q, want EIO", req.Op, resp.Errno)
				}
				if resp.Message == "" {
					t.Fatalf("%s reported EIO with no message; the original failure is then unrecoverable", req.Op)
				}
			}
		})
	}
}

// A namespace with no room of its own to report answers ENOSYS, and this protocol has no
// separate way to say so: the refusal is the namespace's answer, so it travels as an
// ordinary storage error under its own name. Collapsing it to EIO would turn a standing
// property into a failure worth retrying.
func TestANamespaceWithNoRoomToReportSaysSoByName(t *testing.T) {
	h, err := httprest.NewHandler(failing{syscall.ENOSYS}, nil)
	if err != nil {
		t.Fatal(err)
	}
	w := serve(t, h, httprest.Request{Op: httprest.OpSpace}, nil)
	if w.Code != httprest.StatusStorageError {
		t.Fatalf("space answered %d, want %d: %s", w.Code, httprest.StatusStorageError, w.Body)
	}
	var resp httprest.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("space answered an unparseable body %s: %v", w.Body, err)
	}
	if resp.Errno != "ENOSYS" {
		t.Fatalf("space reported errno %q, want ENOSYS", resp.Errno)
	}
}

// failing is a storage whose every operation reports one fixed error.
type failing struct{ err error }

func (f failing) CheckBounded() error                                { return nil }
func (f failing) Stat(context.Context, string) (storage.Attr, error) { return storage.Attr{}, f.err }
func (f failing) SetAttr(context.Context, string, storage.AttrChange) error {
	return f.err
}
func (f failing) List(context.Context, string) ([]storage.Entry, error) {
	return nil, f.err
}
func (f failing) ListBounded(context.Context, string, *storage.ListResult) error { return f.err }
func (f failing) Read(context.Context, string) ([]byte, error)                   { return nil, f.err }
func (f failing) ReadBounded(context.Context, string, int64) ([]byte, error)     { return nil, f.err }
func (f failing) Write(context.Context, string, []byte) error                    { return f.err }
func (f failing) Create(context.Context, string) error                           { return f.err }
func (f failing) Mkdir(context.Context, string) error                            { return f.err }
func (f failing) Remove(context.Context, string) error                           { return f.err }
func (f failing) RemoveDir(context.Context, string) error                        { return f.err }
func (f failing) Rename(context.Context, string, string) error                   { return f.err }
func (f failing) Space(context.Context) (storage.Space, error)                   { return storage.Space{}, f.err }

type boundedEntrypointProbe struct {
	failing
	readLimit  int64
	listCalled bool
}

func (s *boundedEntrypointProbe) Read(context.Context, string) ([]byte, error) {
	panic("the unbounded Read entrypoint was called")
}

func (s *boundedEntrypointProbe) ReadBounded(_ context.Context, _ string, maxBytes int64) ([]byte, error) {
	s.readLimit = maxBytes
	return nil, syscall.EFBIG
}

func (s *boundedEntrypointProbe) List(context.Context, string) ([]storage.Entry, error) {
	panic("the unbounded List entrypoint was called")
}

func (s *boundedEntrypointProbe) ListBounded(_ context.Context, _ string, result *storage.ListResult) error {
	s.listCalled = true
	return result.Add(storage.Entry{Name: strings.Repeat("x", int(result.MaxBytes()))})
}

func TestReadAnswersTheExactBytesWithALength(t *testing.T) {
	h, dir := newHandler(t)
	cases := map[string][]byte{
		"empty":    {},
		"binary":   {0x00, 0xff, 0xfe, 0x0a, 0x80},
		"text":     []byte("hello"),
		"utf8-not": []byte("\xff\xfe not utf-8"),
	}
	for name, content := range cases {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
			t.Fatal(err)
		}
		w := serve(t, h, httprest.Request{Op: httprest.OpRead, Path: name}, nil)
		if w.Code != http.StatusOK {
			t.Fatalf("read %q answered %d", name, w.Code)
		}
		if got := w.Body.Bytes(); string(got) != string(content) {
			t.Fatalf("read %q gave %q, want %q", name, got, content)
		}
		// Without a declared length the client cannot tell a truncated body from a
		// short file, and a short file is valid-looking data.
		if got := w.Header().Get("Content-Length"); got != strconv.Itoa(len(content)) {
			t.Fatalf("read %q declared Content-Length %q, want %d", name, got, len(content))
		}
	}
}

func TestWriteStoresTheExactBytes(t *testing.T) {
	h, dir := newHandler(t)
	content := []byte("\x00\xff binary\n")
	w := serve(t, h, httprest.Request{Op: httprest.OpWrite, Path: "f"}, bytes.NewReader(content))
	if w.Code != http.StatusOK {
		t.Fatalf("write answered %d: %s", w.Code, w.Body)
	}
	got, err := os.ReadFile(filepath.Join(dir, "f"))
	if err != nil {
		t.Fatalf("read back what landed on disk: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("the file on disk holds %q, want %q", got, content)
	}
}

func TestWriteBodiesAreBoundedBeforeStorage(t *testing.T) {
	const limit = int64(1024)
	newBoundedHandler := func(t *testing.T) (*httprest.Handler, string) {
		t.Helper()
		dir := t.TempDir()
		s, err := localdir.New(dir)
		if err != nil {
			t.Fatalf("open the namespace: %v", err)
		}
		options := httprest.DefaultHandlerOptions()
		options.MaxBodyBytes = limit
		options.MaxWriteBytes = limit
		options.MaxInFlightBodyBytes = limit
		h, err := httprest.NewHandlerWithOptions(s, nil, options)
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		return h, dir
	}
	request := func(t *testing.T, h http.Handler, body io.Reader, declared int64) *httptest.ResponseRecorder {
		t.Helper()
		base, _ := url.Parse("http://server.invalid")
		u, err := (httprest.Request{Op: httprest.OpWrite, Path: "f"}).URL(base)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodPost, u.String(), body)
		r.ContentLength = declared
		if declared < 0 {
			r.TransferEncoding = []string{"chunked"}
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	assertTooLarge := func(t *testing.T, w *httptest.ResponseRecorder, dir string) {
		t.Helper()
		if w.Code != httprest.StatusStorageError {
			t.Fatalf("oversized write answered %d, want %d: %s", w.Code, httprest.StatusStorageError, w.Body)
		}
		var response httprest.ErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode oversized write response: %v", err)
		}
		if response.Errno != "EFBIG" {
			t.Fatalf("oversized write reported %q, want EFBIG", response.Errno)
		}
		if _, err := os.Stat(filepath.Join(dir, "f")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("oversized write reached storage: %v", err)
		}
	}

	t.Run("the declared length is rejected without reading", func(t *testing.T) {
		h, dir := newBoundedHandler(t)
		assertTooLarge(t, request(t, h, panicReader{}, limit+1), dir)
	})

	t.Run("an unknown length is stopped after its first excess byte", func(t *testing.T) {
		h, dir := newBoundedHandler(t)
		body := &repeatingReader{}
		assertTooLarge(t, request(t, h, body, -1), dir)
		if body.read != limit+1 {
			t.Fatalf("the handler read %d bytes, want the limit plus one (%d)", body.read, limit+1)
		}
	})

	t.Run("the boundary is accepted", func(t *testing.T) {
		h, dir := newBoundedHandler(t)
		content := bytes.Repeat([]byte("x"), int(limit))
		w := request(t, h, bytes.NewReader(content), limit)
		if w.Code != http.StatusOK {
			t.Fatalf("write at the limit answered %d: %s", w.Code, w.Body)
		}
		got, err := os.ReadFile(filepath.Join(dir, "f"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, content) {
			t.Fatalf("stored %q, want %q", got, content)
		}
	})
}

func TestBodylessOperationsRejectAnyBodyBeforeStorage(t *testing.T) {
	h, err := httprest.NewHandler(failing{syscall.EIO}, nil)
	if err != nil {
		t.Fatal(err)
	}
	type bodyCase struct {
		name     string
		declared int64
		chunked  bool
	}
	bodies := []bodyCase{
		{name: "declared", declared: 4096},
		{name: "chunked", declared: -1, chunked: true},
		{name: "present despite a zero declaration", declared: 0},
	}
	for _, req := range bodylessRequests() {
		for _, bodyCase := range bodies {
			t.Run(string(req.Op)+"/"+bodyCase.name, func(t *testing.T) {
				body := &observedReader{from: bytes.NewReader([]byte("x"))}
				r := newServerRequest(t, req, body)
				r.ContentLength = bodyCase.declared
				if bodyCase.chunked {
					r.TransferEncoding = []string{"chunked"}
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != http.StatusBadRequest {
					t.Fatalf("bodyless %s answered %d, want 400: %s", req.Op, w.Code, w.Body)
				}
				if w.Code == httprest.StatusStorageError {
					t.Fatal("an unexpected body was reported as a storage outcome")
				}
				wantRead := 1
				wantCalls := 1
				if bodyCase.declared > 0 {
					wantRead = 0
					wantCalls = 0
				}
				if body.read != wantRead {
					t.Fatalf("the body reader delivered %d bytes, want %d", body.read, wantRead)
				}
				if body.calls != wantCalls {
					t.Fatalf("the body reader was called %d times, want %d", body.calls, wantCalls)
				}
				var response httprest.ErrorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if response.Errno != "" || !strings.Contains(response.Message, string(req.Op)) {
					t.Fatalf("unexpected-body fault was %+v", response)
				}
			})
		}
	}
}

func TestBodylessOperationsAcceptAnEmptyChunkedBody(t *testing.T) {
	h, err := httprest.NewHandler(failing{syscall.EIO}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range bodylessRequests() {
		t.Run(string(req.Op), func(t *testing.T) {
			baseline := serve(t, h, req, nil)
			body := &observedReader{from: bytes.NewReader(nil)}
			r := newServerRequest(t, req, body)
			r.ContentLength = -1
			r.TransferEncoding = []string{"chunked"}
			chunked := httptest.NewRecorder()
			h.ServeHTTP(chunked, r)
			if chunked.Code != baseline.Code || chunked.Body.String() != baseline.Body.String() {
				t.Fatalf("empty chunked body answered %d %s, ordinary empty body answered %d %s",
					chunked.Code, chunked.Body, baseline.Code, baseline.Body)
			}
			if body.calls != 1 || body.read != 0 {
				t.Fatalf("empty chunked body was read with %d calls and %d bytes", body.calls, body.read)
			}
		})
	}
}

func TestUnknownLengthBodylessRequestsUseOperationAdmission(t *testing.T) {
	options := httprest.DefaultHandlerOptions()
	options.MaxConcurrentBodies = 1
	h, err := httprest.NewHandlerWithOptions(failing{syscall.EIO}, nil, options)
	if err != nil {
		t.Fatal(err)
	}

	blocked := &blockingReader{entered: make(chan struct{}, 1), release: make(chan struct{})}
	released := false
	t.Cleanup(func() {
		if !released {
			close(blocked.release)
		}
	})
	firstRequest := newServerRequest(t, httprest.Request{Op: httprest.OpStat, Path: "first"}, blocked)
	firstRequest.ContentLength = -1
	firstRequest.TransferEncoding = []string{"chunked"}
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, firstRequest)
		firstDone <- w
	}()
	<-blocked.entered

	body := &observedReader{from: bytes.NewReader(nil)}
	ctx, cancel := context.WithCancel(t.Context())
	secondRequest := newServerRequest(t, httprest.Request{Op: httprest.OpList, Path: "second"}, body).WithContext(ctx)
	secondRequest.ContentLength = -1
	secondRequest.TransferEncoding = []string{"chunked"}
	secondDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, secondRequest)
		secondDone <- w
	}()

	select {
	case w := <-secondDone:
		t.Fatalf("the second unknown-length body bypassed admission and answered %d", w.Code)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	var second *httptest.ResponseRecorder
	select {
	case second = <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("cancelling an empty-body check waiting for admission did not return it")
	}
	if second.Code != http.StatusRequestTimeout {
		t.Fatalf("cancelled empty-body admission answered %d, want 408: %s", second.Code, second.Body)
	}
	if body.calls != 0 {
		t.Fatalf("the body waiting for admission was read %d times", body.calls)
	}

	close(blocked.release)
	released = true
	if first := <-firstDone; first.Code != httprest.StatusStorageError {
		t.Fatalf("the admitted request answered %d: %s", first.Code, first.Body)
	}
}

func newServerRequest(t *testing.T, req httprest.Request, body io.Reader) *http.Request {
	t.Helper()
	base, _ := url.Parse("http://server.invalid")
	u, err := req.URL(base)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewRequest(req.Method(), u.String(), body)
}

func bodylessRequests() []httprest.Request {
	return []httprest.Request{
		{Op: httprest.OpStat, Path: "f"},
		{Op: httprest.OpList, Path: "d"},
		{Op: httprest.OpRead, Path: "f"},
		{Op: httprest.OpCreate, Path: "f"},
		{Op: httprest.OpMkdir, Path: "d"},
		{Op: httprest.OpRemove, Path: "f"},
		{Op: httprest.OpRemoveDir, Path: "d"},
		{Op: httprest.OpRename, Path: "from", To: "to"},
		{Op: httprest.OpSpace},
		{Op: httprest.OpSubscribe},
		{Op: httprest.OpResubscribe, Incarnation: "log", Position: 1},
		{Op: httprest.OpSnapshot},
	}
}

func TestAnOversizedAttributeChangeIsAProtocolFault(t *testing.T) {
	dir := t.TempDir()
	s, err := localdir.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	options := httprest.DefaultHandlerOptions()
	options.MaxBodyBytes = 1024
	options.MaxWriteBytes = 1024
	options.MaxInFlightBodyBytes = 1024
	h, err := httprest.NewHandlerWithOptions(s, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte(`{"change":{}}`), bytes.Repeat([]byte(" "), 1024)...)
	w := serve(t, h, httprest.Request{Op: httprest.OpSetAttr, Path: "f"}, bytes.NewReader(body))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized attribute change answered %d, want 413: %s", w.Code, w.Body)
	}
	if w.Code == httprest.StatusStorageError {
		t.Fatal("an attribute document refused before decoding was reported as a storage outcome")
	}
	info, err := os.Stat(filepath.Join(dir, "f"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode() != 0o600 {
		t.Fatalf("the refused change altered the mode to %v", info.Mode())
	}
}

func TestRequestBodyAdmissionBoundsConcurrentOperationsAndBytes(t *testing.T) {
	const limit = int64(1024)
	cases := map[string]func(*httprest.HandlerOptions){
		"operation bound": func(o *httprest.HandlerOptions) {
			o.MaxConcurrentBodies = 1
			o.MaxInFlightBodyBytes = 2 * limit
		},
		"aggregate byte bound": func(o *httprest.HandlerOptions) {
			o.MaxConcurrentBodies = 2
			o.MaxInFlightBodyBytes = limit
		},
	}
	for name, configure := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			inner, err := localdir.New(dir)
			if err != nil {
				t.Fatal(err)
			}
			blocked := &blockingWrite{
				Storage: inner,
				entered: make(chan struct{}),
				release: make(chan struct{}),
			}
			released := false
			t.Cleanup(func() {
				if !released {
					close(blocked.release)
				}
			})
			options := httprest.DefaultHandlerOptions()
			options.MaxBodyBytes = limit
			options.MaxWriteBytes = limit
			configure(&options)
			h, err := httprest.NewHandlerWithOptions(blocked, nil, options)
			if err != nil {
				t.Fatal(err)
			}

			firstDone := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				firstDone <- serve(t, h, httprest.Request{Op: httprest.OpWrite, Path: "first"}, bytes.NewReader([]byte("x")))
			}()
			<-blocked.entered

			base, _ := url.Parse("http://server.invalid")
			u, err := (httprest.Request{Op: httprest.OpWrite, Path: "second"}).URL(base)
			if err != nil {
				t.Fatal(err)
			}
			body := &observedReader{from: bytes.NewReader([]byte("y"))}
			ctx, cancel := context.WithCancel(t.Context())
			r := httptest.NewRequest(http.MethodPost, u.String(), body).WithContext(ctx)
			secondDone := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				secondDone <- w
			}()

			select {
			case w := <-secondDone:
				t.Fatalf("the second body bypassed admission and answered %d", w.Code)
			case <-time.After(50 * time.Millisecond):
			}
			cancel()
			var second *httptest.ResponseRecorder
			select {
			case second = <-secondDone:
			case <-time.After(time.Second):
				t.Fatal("cancelling a body waiting for admission did not return it")
			}
			if second.Code != http.StatusRequestTimeout {
				t.Fatalf("cancelled admission answered %d, want 408: %s", second.Code, second.Body)
			}
			if body.read != 0 {
				t.Fatalf("a body waiting for admission was read for %d bytes", body.read)
			}

			close(blocked.release)
			released = true
			if first := <-firstDone; first.Code != http.StatusOK {
				t.Fatalf("the admitted write answered %d: %s", first.Code, first.Body)
			}
		})
	}
}

func TestWriteAdmissionReservesTheWriteLimit(t *testing.T) {
	const (
		writeLimit = int64(4 << 20)
		bodyLimit  = int64(5 << 20)
	)
	blocked := &multiBlockingWrite{
		Storage: failing{syscall.EIO},
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	released := false
	t.Cleanup(func() {
		if !released {
			close(blocked.release)
		}
	})
	options := httprest.DefaultHandlerOptions()
	options.MaxBodyBytes = bodyLimit
	options.MaxWriteBytes = writeLimit
	options.MaxConcurrentBodies = 2
	options.MaxInFlightBodyBytes = 2 * writeLimit
	h, err := httprest.NewHandlerWithOptions(blocked, nil, options)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan *httptest.ResponseRecorder, 2)
	for _, path := range []string{"first", "second"} {
		path := path
		go func() {
			done <- serve(t, h, httprest.Request{Op: httprest.OpWrite, Path: path}, bytes.NewReader([]byte("x")))
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-blocked.entered:
		case <-time.After(time.Second):
			t.Fatalf("only %d writes entered storage; admission did not reserve the %d-byte write limit", i, writeLimit)
		}
	}
	close(blocked.release)
	released = true
	for i := 0; i < 2; i++ {
		if response := <-done; response.Code != http.StatusOK {
			t.Fatalf("admitted write answered %d: %s", response.Code, response.Body)
		}
	}
}

func TestHandlerBoundsNonStreamingResponses(t *testing.T) {
	const limit = int64(1024)
	newBounded := func(t *testing.T, s storage.Storage) *httprest.Handler {
		t.Helper()
		options := httprest.DefaultHandlerOptions()
		options.MaxBodyBytes = limit
		options.MaxWriteBytes = limit
		options.MaxInFlightBodyBytes = limit
		h, err := httprest.NewHandlerWithOptions(s, nil, options)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	assertErrno := func(t *testing.T, w *httptest.ResponseRecorder, want string) httprest.ErrorResponse {
		t.Helper()
		if w.Code != httprest.StatusStorageError {
			t.Fatalf("response answered %d, want %d: %s", w.Code, httprest.StatusStorageError, w.Body)
		}
		if int64(w.Body.Len()) > limit {
			t.Fatalf("response retained %d bytes, above limit %d", w.Body.Len(), limit)
		}
		var response httprest.ErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Errno != want {
			t.Fatalf("response reported %q, want %q", response.Errno, want)
		}
		return response
	}

	t.Run("read", func(t *testing.T) {
		dir := t.TempDir()
		s, err := localdir.New(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "large"), bytes.Repeat([]byte("x"), int(limit+1)), 0o600); err != nil {
			t.Fatal(err)
		}
		w := serve(t, newBounded(t, s), httprest.Request{Op: httprest.OpRead, Path: "large"}, nil)
		assertErrno(t, w, "EFBIG")
	})

	t.Run("listing", func(t *testing.T) {
		dir := t.TempDir()
		s, err := localdir.New(dir)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 16; i++ {
			name := fmt.Sprintf("%02d-%s", i, strings.Repeat("n", 96))
			if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		w := serve(t, newBounded(t, s), httprest.Request{Op: httprest.OpList}, nil)
		assertErrno(t, w, "EIO")
	})

	t.Run("error detail", func(t *testing.T) {
		err := fmt.Errorf("%s: %w", strings.Repeat("detail", 1024), syscall.ENOENT)
		w := serve(t, newBounded(t, failing{err}), httprest.Request{Op: httprest.OpStat, Path: "missing"}, nil)
		response := assertErrno(t, w, "ENOENT")
		if response.Message != "the response detail exceeds the configured HTTP body limit" {
			t.Fatalf("oversized error detail was rendered as %q", response.Message)
		}
	})
}

// A body that ends early is the case that matters most: writing what did arrive would
// replace a file with a prefix of its intended contents, and answer that it worked.
func TestATruncatedWriteBodyStoresNothing(t *testing.T) {
	const existing = "the previous contents, which must survive"

	cases := []struct {
		name string
		body io.Reader
		// declared is the Content-Length the request announces; -1 leaves it to the
		// body.
		declared int64
	}{
		{"the connection fails part way through", &errorAfter{[]byte("partial"), errors.New("connection reset")}, -1},
		{"fewer bytes arrive than were announced", bytes.NewReader([]byte("short")), 4096},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, dir := newHandler(t)
			if err := os.WriteFile(filepath.Join(dir, "f"), []byte(existing), 0o644); err != nil {
				t.Fatal(err)
			}

			base, _ := url.Parse("http://server.invalid")
			u, err := httprest.Request{Op: httprest.OpWrite, Path: "f"}.URL(base)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, u.String(), c.body)
			if c.declared >= 0 {
				r.ContentLength = c.declared
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if w.Code == http.StatusOK {
				t.Fatalf("a truncated write answered 200")
			}
			got, err := os.ReadFile(filepath.Join(dir, "f"))
			if err != nil {
				t.Fatalf("the file is gone: %v", err)
			}
			if string(got) != existing {
				t.Fatalf("the file on disk holds %q, want the untouched %q", got, existing)
			}
			if entries, err := os.ReadDir(dir); err != nil {
				t.Fatal(err)
			} else if len(entries) != 1 {
				t.Fatalf("the directory holds %d entries, want only the original file — staging was left behind", len(entries))
			}
		})
	}
}

// errorAfter delivers some bytes and then fails, the way a connection that drops
// mid-request does.
type errorAfter struct {
	head []byte
	err  error
}

// panicReader proves a declared oversized body is refused from its metadata alone.
type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("an oversized declared body was read") }

// repeatingReader has no end, so the test fails by hanging or over-reading if the
// streaming bound is not enforced.
type repeatingReader struct{ read int64 }

func (r *repeatingReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	r.read += int64(len(p))
	return len(p), nil
}

type observedReader struct {
	from  io.Reader
	read  int
	calls int
}

type blockingReader struct {
	entered chan struct{}
	release chan struct{}
}

func (r *blockingReader) Read([]byte) (int, error) {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	<-r.release
	return 0, io.EOF
}

func (r *observedReader) Read(p []byte) (int, error) {
	r.calls++
	n, err := r.from.Read(p)
	r.read += n
	return n, err
}

type blockingWrite struct {
	storage.Storage
	entered chan struct{}
	release chan struct{}
}

func (s *blockingWrite) CheckBounded() error {
	return s.Storage.(storage.BoundedStorage).CheckBounded()
}
func (s *blockingWrite) ListBounded(ctx context.Context, path string, result *storage.ListResult) error {
	return s.Storage.(storage.BoundedStorage).ListBounded(ctx, path, result)
}
func (s *blockingWrite) ReadBounded(ctx context.Context, path string, maxBytes int64) ([]byte, error) {
	return s.Storage.(storage.BoundedStorage).ReadBounded(ctx, path, maxBytes)
}

func (s *blockingWrite) Write(context.Context, string, []byte) error {
	close(s.entered)
	<-s.release
	return nil
}

type multiBlockingWrite struct {
	storage.Storage
	entered chan struct{}
	release chan struct{}
}

func (s *multiBlockingWrite) CheckBounded() error {
	return s.Storage.(storage.BoundedStorage).CheckBounded()
}
func (s *multiBlockingWrite) ListBounded(ctx context.Context, path string, result *storage.ListResult) error {
	return s.Storage.(storage.BoundedStorage).ListBounded(ctx, path, result)
}
func (s *multiBlockingWrite) ReadBounded(ctx context.Context, path string, maxBytes int64) ([]byte, error) {
	return s.Storage.(storage.BoundedStorage).ReadBounded(ctx, path, maxBytes)
}

func (s *multiBlockingWrite) Write(context.Context, string, []byte) error {
	s.entered <- struct{}{}
	<-s.release
	return nil
}

func (e *errorAfter) Read(p []byte) (int, error) {
	if len(e.head) > 0 {
		n := copy(p, e.head)
		e.head = e.head[n:]
		return n, nil
	}
	return 0, e.err
}

func TestMalformedRequestsGetTheirOwnStatus(t *testing.T) {
	h, _ := newHandler(t)
	cases := []struct {
		name   string
		method string
		uri    string
		want   int
	}{
		{"an operation that does not exist", http.MethodGet, "/v2/teleport?path=a", http.StatusNotFound},
		{"nothing under the prefix", http.MethodGet, "/", http.StatusNotFound},
		{"the wrong method", http.MethodGet, "/v2/remove?path=a", http.StatusMethodNotAllowed},
		{"a query that does not parse", http.MethodGet, "/v2/stat?path=%zz", http.StatusBadRequest},
		{"no path operand", http.MethodGet, "/v2/stat", http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(c.method, c.uri, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != c.want {
				t.Fatalf("%s %s answered %d, want %d", c.method, c.uri, w.Code, c.want)
			}
			// A malformed request must not be answerable as a storage outcome: that
			// status is the one the client reads an errno out of.
			if w.Code == httprest.StatusStorageError {
				t.Fatal("a malformed request was reported as a storage error")
			}
		})
	}
}

// A listing must carry names exactly as the namespace holds them. A name that comes back
// altered addresses a file that is not there.
func TestListingCarriesNamesUnaltered(t *testing.T) {
	h, dir := newHandler(t)
	names := []string{"plain", "with space", "hash#mark", "日本語", "\xff\xfe not utf-8"}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("create %q: %v", name, err)
		}
	}

	w := serve(t, h, httprest.Request{Op: httprest.OpList}, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("list answered %d: %s", w.Code, w.Body)
	}
	var resp httprest.ListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unparseable listing %s: %v", w.Body, err)
	}
	got := map[string]bool{}
	for _, e := range resp.Storage() {
		got[e.Name] = true
	}
	for _, name := range names {
		if !got[name] {
			t.Fatalf("the listing does not carry %q; it holds %v", name, got)
		}
	}
}

func TestTheHandlerCanBeMountedUnderAPrefix(t *testing.T) {
	inner, dir := newHandler(t)
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/namespace/", http.StripPrefix("/namespace", inner))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	base, err := url.Parse(srv.URL + "/namespace")
	if err != nil {
		t.Fatal(err)
	}
	u, err := httprest.Request{Op: httprest.OpRead, Path: "f"}.URL(base)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Get(u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "payload" {
		t.Fatalf("read under a prefix answered %d %q, want 200 %q", resp.StatusCode, body, "payload")
	}
}
