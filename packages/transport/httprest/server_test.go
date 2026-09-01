package httprest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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
		"no snapshots at all":       func(l *httprest.Limits) { l.Snapshots = 0 },
		"no time to send one in":    func(l *httprest.Limits) { l.SnapshotDeadline = 0 },
		"pages of no rows":          func(l *httprest.Limits) { l.SnapshotPage = 0 },
		"pages of no changes":       func(l *httprest.Limits) { l.EventPage = 0 },
		"a negative number of them": func(l *httprest.Limits) { l.Snapshots = -1 },
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

func (f failing) Stat(context.Context, string) (storage.Attr, error) { return storage.Attr{}, f.err }
func (f failing) SetAttr(context.Context, string, storage.AttrChange) error {
	return f.err
}
func (f failing) List(context.Context, string) ([]storage.Entry, error) {
	return nil, f.err
}
func (f failing) Read(context.Context, string) ([]byte, error) { return nil, f.err }
func (f failing) Write(context.Context, string, []byte) error  { return f.err }
func (f failing) Create(context.Context, string) error         { return f.err }
func (f failing) Mkdir(context.Context, string) error          { return f.err }
func (f failing) Remove(context.Context, string) error         { return f.err }
func (f failing) RemoveDir(context.Context, string) error      { return f.err }
func (f failing) Rename(context.Context, string, string) error { return f.err }
func (f failing) Space(context.Context) (storage.Space, error) { return storage.Space{}, f.err }

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
		{"an operation that does not exist", http.MethodGet, "/v1/teleport?path=a", http.StatusNotFound},
		{"nothing under the prefix", http.MethodGet, "/", http.StatusNotFound},
		{"the wrong method", http.MethodGet, "/v1/remove?path=a", http.StatusMethodNotAllowed},
		{"a query that does not parse", http.MethodGet, "/v1/stat?path=%zz", http.StatusBadRequest},
		{"no path operand", http.MethodGet, "/v1/stat", http.StatusBadRequest},
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
