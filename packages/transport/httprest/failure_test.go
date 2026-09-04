package httprest_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// storageErrnos are the answers that read as established fact on the far side. A
// transport failure must never arrive as one of them: a caller told "no such file" when
// the truth is "I could not reach the server" regenerates, propagates a deletion, or
// overwrites, and none of that is recoverable.
var storageErrnos = []syscall.Errno{
	syscall.ENOENT, syscall.EEXIST, syscall.EISDIR, syscall.ENOTDIR,
	syscall.ENOTEMPTY, syscall.EINVAL,
}

// exerciseAll runs every operation of the contract and reports what each one gave back.
func exerciseAll(t *testing.T, ctx context.Context, s storage.Storage) {
	t.Helper()

	t.Run("stat", func(t *testing.T) {
		_, err := s.Stat(ctx, "f")
		requireUnreachable(t, err)
	})
	t.Run("list", func(t *testing.T) {
		entries, err := s.List(ctx, "d")
		requireUnreachable(t, err)
		// An empty listing alongside a nil error is the shape that destroys data. The
		// error is what makes the empty slice harmless, so assert both.
		if err == nil && len(entries) == 0 {
			t.Fatal("list reported an empty directory instead of a failure")
		}
	})
	t.Run("read", func(t *testing.T) {
		content, err := s.Read(ctx, "f")
		requireUnreachable(t, err)
		if err == nil {
			t.Fatalf("read returned %q instead of a failure", content)
		}
	})
	t.Run("setattr", func(t *testing.T) {
		mode := fs.FileMode(0o600)
		requireUnreachable(t, s.SetAttr(ctx, "f", storage.AttrChange{Mode: &mode}))
	})
	t.Run("write", func(t *testing.T) { requireUnreachable(t, s.Write(ctx, "f", []byte("x"))) })
	t.Run("create", func(t *testing.T) { requireUnreachable(t, s.Create(ctx, "f")) })
	t.Run("mkdir", func(t *testing.T) { requireUnreachable(t, s.Mkdir(ctx, "d")) })
	t.Run("remove", func(t *testing.T) { requireUnreachable(t, s.Remove(ctx, "f")) })
	t.Run("removedir", func(t *testing.T) { requireUnreachable(t, s.RemoveDir(ctx, "d")) })
	t.Run("rename", func(t *testing.T) { requireUnreachable(t, s.Rename(ctx, "from", "to")) })
	t.Run("space", func(t *testing.T) {
		space, err := s.Space(ctx)
		requireUnreachable(t, err)
		// Zero bytes free is the shape that stops every write while looking like an
		// ordinary answer, so the failure is what has to arrive, never the figures.
		if err == nil {
			t.Fatalf("space reported %+v instead of a failure", space)
		}
		// A namespace that cannot be reached is not a namespace that has no room to
		// report: ENOSYS is a standing property, and nothing above may learn it from a
		// server it never reached.
		if errors.Is(err, syscall.ENOSYS) {
			t.Fatalf("a transport failure arrived as ENOSYS: %v", err)
		}
	})
}

func requireUnreachable(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("the operation reported success although the server never answered it")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("the operation failed with %v, want EIO", err)
	}
	for _, errno := range storageErrnos {
		if errors.Is(err, errno) {
			t.Fatalf("a transport failure arrived as %v: %v", errno, err)
		}
	}
}

// dialHandler returns a storage reaching a handler that answers however the test wants.
func dialHandler(t *testing.T, handler http.Handler) *httprest.Storage {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	s, err := httprest.Dial(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return s
}

func TestNothingListening(t *testing.T) {
	// Bind and release, so the address is one that was valid a moment ago and has
	// nothing behind it now.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := httprest.Dial("http://"+address, &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	exerciseAll(t, t.Context(), s)
}

// A name that resolves to nothing is the case where the underlying failure itself
// carries an errno, and it is the one most likely to be leaked to the caller intact.
func TestAHostThatDoesNotResolve(t *testing.T) {
	s, err := httprest.Dial("http://server.invalid.", &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	exerciseAll(t, t.Context(), s)
}

func TestAnswersThatAreNotThisProtocol(t *testing.T) {
	cases := []struct {
		name    string
		respond http.HandlerFunc
	}{
		{"an errno nobody has heard of", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, httprest.Version)
			w.WriteHeader(httprest.StatusStorageError)
			w.Write([]byte(`{"errno":"EWORMHOLE"}`))
		}},
		{"an errno spelled as a number", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, httprest.Version)
			w.WriteHeader(httprest.StatusStorageError)
			w.Write([]byte(`{"errno":"2"}`))
		}},
		{"a storage error with no errno", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, httprest.Version)
			w.WriteHeader(httprest.StatusStorageError)
			w.Write([]byte(`{"message":"something went wrong"}`))
		}},
		{"a storage error whose body does not parse", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, httprest.Version)
			w.WriteHeader(httprest.StatusStorageError)
			w.Write([]byte(`{"errno":`))
		}},
		// The status space is not the errno space. A gateway that lost the route says
		// 404, and reading that as "no such file" is exactly the mistake.
		{"a 404 from something in the way", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, httprest.Version)
			http.Error(w, "no route to that service", http.StatusNotFound)
		}},
		{"a 500 whose body claims ENOENT", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, httprest.Version)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"errno":"ENOENT"}`))
		}},
		// An intermediary that answers on the server's behalf cannot know this header.
		{"a success from something that is not the server", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"attr":{"mode":420,"size":0,"mod_time_unix_sec":0,"mod_time_nanos":0}}`))
		}},
		{"a storage error from something that is not the server", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(httprest.StatusStorageError)
			w.Write([]byte(`{"errno":"ENOENT"}`))
		}},
		{"the wrong protocol version", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, "99")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"entries":[]}`))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			exerciseAll(t, t.Context(), dialHandler(t, c.respond))
		})
	}
}

// A response that is marked as this protocol's and carries a status of success, but
// whose body is not the answer the operation asked for.
//
// Stat, List and Space catch it by failing to read the body. Mutations require an exact JSON
// response object, optionally carrying a valid replication barrier. Read is absent on purpose:
// a file holds arbitrary bytes, so once the
// response is marked and its declared length checks out, the body is the answer.
func TestABodyThatIsNotTheAnswer(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"not JSON at all", "<html>this is not the server you are looking for</html>"},
		{"JSON of the wrong shape", `{"attr": "not an object", "entries": 7}`},
		{"nothing", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(httprest.HeaderProtocol, httprest.Version)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(c.body))
			}))
			ctx := t.Context()

			_, err := s.Stat(ctx, "f")
			requireUnreachable(t, err)

			entries, err := s.List(ctx, "d")
			requireUnreachable(t, err)
			if err == nil && len(entries) == 0 {
				t.Fatal("list reported an empty directory instead of a failure")
			}

			space, err := s.Space(ctx)
			requireUnreachable(t, err)
			if err == nil {
				t.Fatalf("space reported %+v instead of a failure", space)
			}

			requireUnreachable(t, s.SetAttr(ctx, "f", storage.AttrChange{}))
			requireUnreachable(t, s.Write(ctx, "f", []byte("x")))
			requireUnreachable(t, s.Create(ctx, "f"))
			requireUnreachable(t, s.Mkdir(ctx, "d"))
			requireUnreachable(t, s.Remove(ctx, "f"))
			requireUnreachable(t, s.RemoveDir(ctx, "d"))
			requireUnreachable(t, s.Rename(ctx, "from", "to"))
		})
	}
}

// A read whose body stops early must not come back as a shorter file. Nothing about a
// truncated file looks wrong to whatever reads it, and the mount above writes an open
// file's buffer back on close — so a program that reads a file and rewrites it replaces
// it with the prefix that arrived.
//
// Whether that can be caught at all is decided by the framing. A declared length can be
// compared against what arrived; a chunked body ends with a terminating chunk; a gzip
// stream ends with a trailer carrying a length and a checksum. Close-delimited HTTP/1.x —
// no length, no chunking, the body ends when the connection does — cannot report anything
// about itself, so a connection dropped mid-file is byte for byte a complete answer. Its
// whole form is therefore refused along with its truncated one: the two are the same
// bytes, and refusing a good answer is the only way to refuse the bad one.
func TestAReadThatIsCutShort(t *testing.T) {
	const whole = "the whole of a short file, all of which arrived"
	const prefix = "the whole of a short"

	compressed := func(payload string) string {
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write([]byte(payload)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	gzipped := compressed(whole)

	cases := []struct {
		name    string
		answer  string
		wantErr bool
	}{
		{"a declared length, whole", rawHead("Content-Length: "+strconv.Itoa(len(whole))) + whole, false},
		{"a declared length, cut short", rawHead("Content-Length: 4096") + prefix, true},

		{"chunked, whole", rawHead("Transfer-Encoding: chunked") + chunk(whole) + "0\r\n\r\n", false},
		{"chunked, cut short", rawHead("Transfer-Encoding: chunked") + chunk(prefix), true},

		{"gzip, whole", rawHead("Content-Encoding: gzip") + gzipped, false},
		// Cut where the trailer begins, so what is missing is exactly the length and the
		// checksum that make the stream self-terminating.
		{"gzip, cut short", rawHead("Content-Encoding: gzip") + gzipped[:len(gzipped)-8], true},

		{"close-delimited, cut short", rawHead() + prefix, true},
		{"close-delimited, whole", rawHead() + whole, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := httprest.Dial(newRawServer(t, c.answer), &http.Client{Timeout: 5 * time.Second})
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			content, err := s.Read(t.Context(), "f")
			if c.wantErr {
				if err == nil {
					t.Fatalf("read returned %q and called it a success", content)
				}
				requireUnreachable(t, err)
				return
			}
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if string(content) != whole {
				t.Fatalf("read gave %q, want %q", content, whole)
			}
		})
	}
}

// A space report is subject to the same framing rule as a read, and for a worse reason. A
// report that ends early and is read anyway offers zero bytes free, which refuses every
// write while looking like an ordinary answer from a workspace that is genuinely full.
//
// A close-delimited answer is therefore refused whole, exactly as a read is: with no
// length and no chunking there is nothing about the body to check afterwards, so a report
// cut short and a report that arrived complete are the same bytes.
func TestASpaceReportThatIsCutShort(t *testing.T) {
	const whole = `{"space":{"total":8192,"used":1024,"avail":7168}}`
	const prefix = `{"space":{"total":8192,`

	cases := []struct {
		name    string
		answer  string
		wantErr bool
	}{
		{"a declared length, whole", rawHead("Content-Length: "+strconv.Itoa(len(whole))) + whole, false},
		{"a declared length, cut short", rawHead("Content-Length: 4096") + prefix, true},

		{"chunked, whole", rawHead("Transfer-Encoding: chunked") + chunk(whole) + "0\r\n\r\n", false},
		{"chunked, cut short", rawHead("Transfer-Encoding: chunked") + chunk(prefix), true},

		{"close-delimited, cut short", rawHead() + prefix, true},
		{"close-delimited, whole", rawHead() + whole, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := httprest.Dial(newRawServer(t, c.answer), &http.Client{Timeout: 5 * time.Second})
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			space, err := s.Space(t.Context())
			if c.wantErr {
				if err == nil {
					t.Fatalf("space returned %+v and called it a success", space)
				}
				requireUnreachable(t, err)
				return
			}
			if err != nil {
				t.Fatalf("space: %v", err)
			}
			if want := (storage.Space{Total: 8192, Used: 1024, Avail: 7168}); space != want {
				t.Fatalf("space reported %+v, want %+v", space, want)
			}
		})
	}
}

// The three fields the framing check reads are net/http's, and what they hold for a given
// framing is this test's subject rather than its assumption: a check built on a wrong
// reading of them would refuse every answer or refuse none.
func TestTheTransportReportsTheFraming(t *testing.T) {
	var gzipped bytes.Buffer
	zw := gzip.NewWriter(&gzipped)
	zw.Write([]byte("body"))
	zw.Close()

	cases := []struct {
		name             string
		answer           string
		contentLength    int64
		transferEncoding []string
		uncompressed     bool
	}{
		{"a declared length", rawHead("Content-Length: 4") + "body", 4, nil, false},
		{"chunked", rawHead("Transfer-Encoding: chunked") + chunk("body") + "0\r\n\r\n", -1, []string{"chunked"}, false},
		{"gzip", rawHead("Content-Encoding: gzip") + gzipped.String(), -1, nil, true},
		{"close-delimited", rawHead() + "body", -1, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			caller := &http.Client{Timeout: 5 * time.Second}
			resp, err := caller.Get(newRawServer(t, c.answer) + "/whatever")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			defer resp.Body.Close()
			if body, err := io.ReadAll(resp.Body); err != nil || string(body) != "body" {
				t.Fatalf("the body read as %q, %v", body, err)
			}
			if resp.ContentLength != c.contentLength {
				t.Errorf("ContentLength is %d, want %d", resp.ContentLength, c.contentLength)
			}
			if !slices.Equal(resp.TransferEncoding, c.transferEncoding) {
				t.Errorf("TransferEncoding is %v, want %v", resp.TransferEncoding, c.transferEncoding)
			}
			if resp.Uncompressed != c.uncompressed {
				t.Errorf("Uncompressed is %v, want %v", resp.Uncompressed, c.uncompressed)
			}
		})
	}
}

// rawHead renders a 200 response head carrying this protocol's mark, plus whatever
// framing headers the test names. Each is given without its line ending.
func rawHead(framingHeaders ...string) string {
	head := "HTTP/1.1 200 OK\r\n" +
		httprest.HeaderProtocol + ": " + httprest.Version + "\r\n" +
		"Content-Type: application/octet-stream\r\n" +
		"Connection: close\r\n"
	for _, header := range framingHeaders {
		head += header + "\r\n"
	}
	return head + "\r\n"
}

func chunk(payload string) string {
	return strconv.FormatInt(int64(len(payload)), 16) + "\r\n" + payload + "\r\n"
}

// newRawServer answers every request with answer, byte for byte, and then closes the
// connection. Framings that net/http's own server will not produce — a body with no
// length at all, a chunked body with no terminating chunk — only exist on the wire, so
// the wire is where a test has to write them.
func newRawServer(t *testing.T, answer string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				// The request head is consumed so the client has committed to the
				// exchange before the answer starts.
				head := bufio.NewReader(conn)
				for {
					line, err := head.ReadString('\n')
					if err != nil || line == "\r\n" {
						break
					}
				}
				io.WriteString(conn, answer)
			}()
		}
	}()
	return "http://" + listener.Addr().String()
}

func TestAServerThatHangs(t *testing.T) {
	released := make(chan struct{})
	t.Cleanup(func() { close(released) })

	s := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-released:
		case <-r.Context().Done():
		}
	}))

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	exerciseAll(t, ctx, s)
}

func TestAContextThatIsAlreadyCancelled(t *testing.T) {
	s := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("the server was reached although the caller had already given up")
	}))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	exerciseAll(t, ctx, s)
}

// The deadline the caller set has to be the one that decides, rather than a timeout the
// package chose for itself.
func TestTheCallersDeadlineDecides(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	s := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err := s.Stat(ctx, "f")
	waited := time.Since(started)

	requireUnreachable(t, err)
	if waited < 100*time.Millisecond || waited > 5*time.Second {
		t.Fatalf("the call gave up after %v, want something close to the 150ms deadline", waited)
	}
}

// A connection that dies before the response line arrives is the shape a server restart
// takes.
func TestAConnectionThatCloses(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Consume the request line so the client has committed to the exchange,
			// then vanish.
			bufio.NewReader(conn).ReadString('\n')
			conn.Close()
		}
	}()

	s, err := httprest.Dial("http://"+listener.Addr().String(), &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	exerciseAll(t, t.Context(), s)
}

// JSON null and an empty list are two characters apart and mean opposite things: one is
// a directory with nothing in it, the other is a response that carried no listing at
// all. The second must not be delivered as the first.
func TestAListingThatIsNotThere(t *testing.T) {
	for _, body := range []string{`{"entries":null}`, `{}`, `{"entries":[]}`} {
		t.Run(body, func(t *testing.T) {
			s := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(httprest.HeaderProtocol, httprest.Version)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(body))
			}))
			entries, err := s.List(t.Context(), "d")
			if body == `{"entries":[]}` {
				if err != nil {
					t.Fatalf("an empty directory came back as %v", err)
				}
				if len(entries) != 0 {
					t.Fatalf("an empty directory listed %v", entries)
				}
				return
			}
			requireUnreachable(t, err)
			if err == nil && len(entries) == 0 {
				t.Fatal("a response with no listing was delivered as an empty directory")
			}
		})
	}
}

// The mirror of the listing case, for a stat. A body that carries no attribute set
// unmarshals into a zero Attr, and a zero Attr is not visibly wrong: mode 0 has no type
// bits, so it reads as a regular file, of length 0, dated the epoch. A mount above then
// presents a directory as an empty regular file, a file with content as empty, and every
// node as dated 1970 — none of which looks like a failure to whatever is walking the tree.
//
// The one shape that must still be accepted is at the bottom: a file whose mode really is
// 0 is a legitimate answer, so absence has to be told apart by the shape of the body and
// never by the values in it.
func TestAStatThatCarriesNoAttributes(t *testing.T) {
	cases := []struct {
		body     string
		wantMode fs.FileMode
		wantErr  bool
	}{
		{`{}`, 0, true},
		{`null`, 0, true},
		{`{"attr":null}`, 0, true},
		// A listing delivered to a stat: the right protocol, the wrong answer.
		{`{"entries":[]}`, 0, true},
		// Attributes under a name this side does not read are attributes it did not get.
		{`{"attributes":{"mode":420,"size":7}}`, 0, true},

		// A peer that sends attributes without an identity is not sending nothing, it is
		// saying every node is the same node, and every comparison of that above returns
		// equal. It is refused here, where the other absences are.
		{`{"attr":{"mode":420,"size":7}}`, 0, true},
		{`{"attr":{"id":0,"mode":420,"size":7}}`, 0, true},

		// A mode of zero is a legitimate answer, unlike an identity of zero.
		{`{"attr":{"id":9,"mode":420,"size":7}}`, 0o644, false},
		{`{"attr":{"id":9,"mode":0,"size":0}}`, 0, false},
	}
	for _, c := range cases {
		t.Run(c.body, func(t *testing.T) {
			s := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(httprest.HeaderProtocol, httprest.Version)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(c.body))
			}))
			attr, err := s.Stat(t.Context(), "f")
			if c.wantErr {
				if err == nil {
					t.Fatalf("stat delivered %+v, and the response carried no attributes", attr)
				}
				requireUnreachable(t, err)
				return
			}
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			if attr.Mode != c.wantMode {
				t.Fatalf("stat delivered mode %v, want %v", attr.Mode, c.wantMode)
			}
		})
	}
}

// The same hole inside a listing: an entry whose attributes never arrived becomes an
// entry with a zero Attr, which reads as an empty regular file of that name.
func TestAListingEntryThatCarriesNoAttributes(t *testing.T) {
	cases := []struct {
		body    string
		wantErr bool
	}{
		{`{"entries":[{"name":"Zg=="}]}`, true},
		{`{"entries":[{"name":"Zg==","attr":null}]}`, true},
		{`{"entries":[{"name":"Zg==","attr":{"id":9,"mode":420}},{"name":"Zw=="}]}`, true},
		// An entry whose attributes carry no identity is refused with the rest of them.
		{`{"entries":[{"name":"Zg==","attr":{"mode":0}}]}`, true},
		{`{"entries":[{"name":"Zg==","attr":{"id":9,"mode":0}}]}`, false},
	}
	for _, c := range cases {
		t.Run(c.body, func(t *testing.T) {
			s := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(httprest.HeaderProtocol, httprest.Version)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(c.body))
			}))
			entries, err := s.List(t.Context(), "d")
			if c.wantErr {
				if err == nil {
					t.Fatalf("list delivered %+v, and an entry carried no attributes", entries)
				}
				requireUnreachable(t, err)
				return
			}
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if len(entries) != 1 || entries[0].Name != "f" || entries[0].Attr.Mode != 0 {
				t.Fatalf("list delivered %+v, want one entry named \"f\" of mode 0", entries)
			}
		})
	}
}

// A count that never arrived must not read as the figure zero. All three are byte counts
// whose zero is a legitimate answer — a namespace holding nothing has used none, a full
// one has none available — so nothing in the values tells absence apart from it, and a
// report read as zero available refuses every write on a namespace that has room.
//
// Figures that could not all be true of anything are refused for the same reason they are
// never repaired: they end up in a kernel reply whose fields are unsigned, where a
// negative becomes an enormous positive.
func TestASpaceReportThatIsNotThere(t *testing.T) {
	cases := []struct {
		body    string
		want    storage.Space
		wantErr bool
	}{
		{body: `{}`, wantErr: true},
		{body: `null`, wantErr: true},
		{body: `{"space":null}`, wantErr: true},
		// A stat answer delivered to a space report: the right protocol, the wrong answer.
		{body: `{"attr":{"mode":420,"size":7}}`, wantErr: true},

		{body: `{"space":{"used":1024,"avail":3072}}`, wantErr: true},
		{body: `{"space":{"total":4096,"avail":3072}}`, wantErr: true},
		{body: `{"space":{"total":4096,"used":1024}}`, wantErr: true},
		{body: `{"space":{}}`, wantErr: true},
		// Counts under a name this side does not read are counts it did not get.
		{body: `{"space":{"total":4096,"used":1024,"available":3072}}`, wantErr: true},

		{body: `{"space":{"total":-1,"used":0,"avail":0}}`, wantErr: true},
		{body: `{"space":{"total":4096,"used":-1,"avail":0}}`, wantErr: true},
		{body: `{"space":{"total":4096,"used":0,"avail":-1}}`, wantErr: true},
		{body: `{"space":{"total":4096,"used":4096,"avail":1}}`, wantErr: true},

		{body: `{"space":{"total":4096,"used":1024,"avail":3072}}`,
			want: storage.Space{Total: 4096, Used: 1024, Avail: 3072}},
		// The shapes that must still be accepted: a namespace with nothing written and one
		// with nothing left both carry zeroes, and both are answers a caller may be given.
		{body: `{"space":{"total":0,"used":0,"avail":0}}`, want: storage.Space{}},
		{body: `{"space":{"total":4096,"used":4096,"avail":0}}`,
			want: storage.Space{Total: 4096, Used: 4096}},
		// An allowance lowered underneath content already written, which is why Used may
		// exceed Total.
		{body: `{"space":{"total":4096,"used":8192,"avail":0}}`,
			want: storage.Space{Total: 4096, Used: 8192}},
	}
	for _, c := range cases {
		t.Run(c.body, func(t *testing.T) {
			s := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(httprest.HeaderProtocol, httprest.Version)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(c.body))
			}))
			got, err := s.Space(t.Context())
			if c.wantErr {
				if err == nil {
					t.Fatalf("space delivered %+v from a body that does not report one", got)
				}
				requireUnreachable(t, err)
				return
			}
			if err != nil {
				t.Fatalf("space: %v", err)
			}
			if got != c.want {
				t.Fatalf("space delivered %+v, want %+v", got, c.want)
			}
		})
	}
}

// ENOSYS from Space states a property of the namespace rather than a failure to reach it,
// so it has to arrive as itself, over the same machinery that carries every other errno.
// Delivered as EIO it would read as a condition worth retrying; delivered as figures it
// would be a quantity nobody measured.
func TestANamespaceWithNoRoomToReport(t *testing.T) {
	s := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(httprest.HeaderProtocol, httprest.Version)
		w.WriteHeader(httprest.StatusStorageError)
		w.Write([]byte(`{"errno":"ENOSYS","message":"this namespace has no room of its own to report"}`))
	}))
	space, err := s.Space(t.Context())
	if !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("space reported %+v, %v; want ENOSYS", space, err)
	}
	if errors.Is(err, syscall.EIO) {
		t.Fatalf("a refusal this side understands arrived as EIO as well: %v", err)
	}
}

func TestABodyShorterThanItsDeclaredLength(t *testing.T) {
	s, err := httprest.Dial("http://server.invalid", &http.Client{Transport: shortTransport{}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	content, err := s.Read(t.Context(), "f")
	if err == nil {
		t.Fatalf("read returned %q for a body that declared 4096 bytes", content)
	}
	requireUnreachable(t, err)
}

// shortTransport answers every request with a body shorter than the length it announces,
// and does so without the reader reporting anything wrong.
type shortTransport struct{}

func (shortTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	header := http.Header{}
	header.Set(httprest.HeaderProtocol, httprest.Version)
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        header,
		ContentLength: 4096,
		Body:          io.NopCloser(strings.NewReader("the first few bytes")),
		Request:       r,
	}, nil
}

// An error has to name what was being done to what. Everything above sees these in a log
// and nothing else.
func TestErrorsSayWhatFailed(t *testing.T) {
	s := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(httprest.HeaderProtocol, httprest.Version)
		w.WriteHeader(httprest.StatusStorageError)
		w.Write([]byte(`{"errno":"ENOENT","message":"stat missing: no such file or directory"}`))
	}))
	ctx := t.Context()

	_, statErr := s.Stat(ctx, "a dir/a file")
	for _, want := range []string{"stat", `"a dir/a file"`, "no such file or directory"} {
		if !strings.Contains(statErr.Error(), want) {
			t.Fatalf("the stat error reads %q, which does not mention %q", statErr, want)
		}
	}

	renameErr := s.Rename(ctx, "from here", "to there")
	for _, want := range []string{"rename", `"from here"`, `"to there"`} {
		if !strings.Contains(renameErr.Error(), want) {
			t.Fatalf("the rename error reads %q, which does not mention %q", renameErr, want)
		}
	}

	// An operation that takes no operands names none. A quoted empty path beside it would
	// read as a report about the root.
	_, spaceErr := s.Space(ctx)
	if !strings.HasPrefix(spaceErr.Error(), "space: ") {
		t.Fatalf("the space error reads %q, want it to name the operation and nothing under a path", spaceErr)
	}

	// A storage error carrying no message still has to render.
	bare := dialHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(httprest.HeaderProtocol, httprest.Version)
		w.WriteHeader(httprest.StatusStorageError)
		w.Write([]byte(`{"errno":"EEXIST"}`))
	}))
	bareErr := bare.Create(ctx, "f")
	if !errors.Is(bareErr, syscall.EEXIST) {
		t.Fatalf("create failed with %v, want EEXIST", bareErr)
	}
	for _, want := range []string{"create", `"f"`, "file exists"} {
		if !strings.Contains(bareErr.Error(), want) {
			t.Fatalf("the create error reads %q, which does not mention %q", bareErr, want)
		}
	}
}

// A server reached over a Unix socket is an ordinary deployment, and a socket that is
// not there is what a server that is not running looks like. The failure it produces
// carries syscall.ENOENT inside it:
//
//	dial unix /run/remote-fs.sock: connect: no such file or directory
//
// Anything that leaves that cause in the errors.Is chain answers "no such file" to every
// question asked while the server is down — the exact answer that makes whatever runs on
// top delete or regenerate. This is the case that decides how errors are wrapped here.
func TestASocketThatIsNotThere(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "not-listening.sock")
	overUnix := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			},
		},
	}

	// The dial failure itself carries ENOENT; if it did not, this test would be
	// asserting nothing.
	if _, err := overUnix.Get("http://namespace.local/"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("dialling a missing socket gave %v, which does not carry ENOENT — this test no longer covers what it was written for", err)
	}

	s, err := httprest.Dial("http://namespace.local", overUnix)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	exerciseAll(t, t.Context(), s)
}
