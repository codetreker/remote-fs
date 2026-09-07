package httprest_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// awkwardPaths are the namespace paths that URL construction is most likely to damage:
// characters with a meaning in a URL, characters the path cleaner has an opinion about,
// non-ASCII text, and bytes that are not valid UTF-8 at all.
var awkwardPaths = []string{
	"",
	"a",
	"a/b/c",
	"with space",
	"hash#mark",
	"question?mark",
	"percent%25sign",
	"plus+sign",
	"ampersand&equals=here",
	"semicolon;and,comma",
	"..",
	"../escape",
	"/absolute",
	"a//double",
	"a/./dot",
	"a/b/../f",
	"日本語/ファイル",
	"emoji-🙂",
	"\xff\xfe not utf-8",
	"newline\nand\ttab",
	strings.Repeat("deep/", 20) + "leaf",
}

func allOps() []httprest.Op {
	return []httprest.Op{
		httprest.OpStat, httprest.OpSetAttr, httprest.OpList, httprest.OpRead,
		httprest.OpWrite, httprest.OpCreate, httprest.OpMkdir, httprest.OpRemove,
		httprest.OpRemoveDir, httprest.OpRename, httprest.OpSpace,
		httprest.OpSubscribe, httprest.OpResubscribe, httprest.OpSnapshot,
	}
}

func mustBase(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse base %q: %v", raw, err)
	}
	return u
}

// A request must survive being rendered to a URL and parsed back with every operand
// byte-identical. Anything less silently addresses a different node than the caller
// named.
func TestRequestSurvivesURLRoundTrip(t *testing.T) {
	base := mustBase(t, "http://example.invalid/")
	for _, op := range allOps() {
		for _, p := range awkwardPaths {
			for _, to := range awkwardPaths {
				want := httprest.Request{Op: op}
				switch op {
				case httprest.OpSpace, httprest.OpSubscribe, httprest.OpSnapshot:
					// These take no operands, so they have nothing a URL could damage.
				case httprest.OpResubscribe:
					// A resume point rather than a path: an incarnation is opaque and may
					// be any byte sequence at all, so the awkward names are as good a
					// source of one as anything.
					want.Incarnation, want.Position = metastore.Incarnation(p), 9007199254740993
				case httprest.OpRename:
					want.Path, want.To = p, to
				default:
					want.Path = p
				}

				u, err := want.URL(base)
				if err != nil {
					t.Fatalf("%s %q: URL: %v", op, p, err)
				}
				// Re-parse the wire form rather than reusing the URL value, so that
				// the test exercises the escaping and not the struct.
				reparsed, err := url.ParseRequestURI(u.RequestURI())
				if err != nil {
					t.Fatalf("%s %q: the rendered URI %q does not parse: %v", op, p, u.RequestURI(), err)
				}
				got, err := httprest.ParseRequest(want.Method(), reparsed)
				if err != nil {
					t.Fatalf("%s %q: ParseRequest(%q): %v", op, p, u.RequestURI(), err)
				}
				if got != want {
					t.Fatalf("round trip of %+v through %q gave %+v", want, u.RequestURI(), got)
				}

				if op != httprest.OpRename {
					break
				}
			}
		}
	}
}

// The round trip that matters is the one through a real connection: a URL value can
// agree with itself while the bytes on the wire say something else.
func TestRequestSurvivesTheWire(t *testing.T) {
	type seen struct {
		req httprest.Request
		err error
	}
	got := make(chan seen, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := httprest.ParseRequest(r.Method, r.URL)
		got <- seen{req, err}
	}))
	defer srv.Close()

	base := mustBase(t, srv.URL)
	for _, p := range awkwardPaths {
		want := httprest.Request{Op: httprest.OpRename, Path: p, To: "destination/" + p}
		u, err := want.URL(base)
		if err != nil {
			t.Fatalf("URL for %q: %v", p, err)
		}
		req, err := http.NewRequest(want.Method(), u.String(), nil)
		if err != nil {
			t.Fatalf("new request for %q: %v", p, err)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("send %q: %v", p, err)
		}
		resp.Body.Close()

		s := <-got
		if s.err != nil {
			t.Fatalf("server could not parse the request for %q: %v", p, s.err)
		}
		if s.req != want {
			t.Fatalf("server saw %+v, want %+v", s.req, want)
		}
	}
}

func TestRequestURLRejectsAnUnknownOperation(t *testing.T) {
	_, err := httprest.Request{Op: "teleport", Path: "f"}.URL(mustBase(t, "http://example.invalid/"))
	if !errors.Is(err, httprest.ErrUnknownOp) {
		t.Fatalf("URL for an unknown operation gave %v, want ErrUnknownOp", err)
	}
}

// A base URL that already carries a path prefix must keep it, so that the handler can
// be mounted somewhere other than the root of a host.
func TestRequestURLKeepsTheBasePrefix(t *testing.T) {
	for _, raw := range []string{"http://example.invalid/under/here", "http://example.invalid/under/here/"} {
		u, err := httprest.Request{Op: httprest.OpStat, Path: "f"}.URL(mustBase(t, raw))
		if err != nil {
			t.Fatalf("URL under %q: %v", raw, err)
		}
		if want := "/under/here/v3/stat"; u.Path != want {
			t.Fatalf("base %q gave path %q, want %q", raw, u.Path, want)
		}
	}
}

// An operation that takes no operands is addressed by its name alone. The refusal of an
// empty query for every other operation is what keeps a damaged request from reading as a
// request against the whole namespace, so the one operation that legitimately carries no
// query is asserted here.
func TestSpaceIsAddressedWithNoOperands(t *testing.T) {
	u, err := httprest.Request{Op: httprest.OpSpace}.URL(mustBase(t, "http://example.invalid/"))
	if err != nil {
		t.Fatalf("URL for space: %v", err)
	}
	if want := "/v3/space"; u.RequestURI() != want {
		t.Fatalf("space is requested as %q, want %q", u.RequestURI(), want)
	}
	got, err := httprest.ParseRequest(http.MethodGet, u)
	if err != nil {
		t.Fatalf("ParseRequest for space: %v", err)
	}
	if want := (httprest.Request{Op: httprest.OpSpace}); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestMethods(t *testing.T) {
	reads := map[httprest.Op]bool{
		httprest.OpStat: true, httprest.OpList: true, httprest.OpRead: true, httprest.OpSpace: true,
		httprest.OpSubscribe: true, httprest.OpResubscribe: true, httprest.OpSnapshot: true,
	}
	for _, op := range allOps() {
		want := http.MethodPost
		if reads[op] {
			want = http.MethodGet
		}
		if got := (httprest.Request{Op: op}).Method(); got != want {
			t.Fatalf("%s uses method %q, want %q", op, got, want)
		}
	}
	// An operation that does not exist has no method, rather than a plausible default
	// that would send it somewhere.
	if got := (httprest.Request{Op: "teleport"}).Method(); got != "" {
		t.Fatalf("an unknown operation uses method %q, want none", got)
	}
}

// An operation that sends a body has to say what the body is, and the two that send one
// send different things: a file's contents are bytes nobody may reinterpret, an attribute
// change is a document. An operation that sends none must claim no type at all, so that
// nothing downstream reads a declared type as evidence that a body was sent.
func TestContentTypes(t *testing.T) {
	want := map[httprest.Op]string{
		httprest.OpWrite:   "application/octet-stream",
		httprest.OpSetAttr: "application/json",
	}
	for _, op := range allOps() {
		if got := (httprest.Request{Op: op}).ContentType(); got != want[op] {
			t.Fatalf("%s sends a body of type %q, want %q", op, got, want[op])
		}
	}
	if got := (httprest.Request{Op: "teleport"}).ContentType(); got != "" {
		t.Fatalf("an unknown operation claims content type %q, want none", got)
	}
}

func TestParseRequestRejectsMalformedRequests(t *testing.T) {
	cases := []struct {
		name   string
		method string
		uri    string
		want   error
	}{
		// url.Values.Get reports a query it could not parse as an absent key, and an
		// absent path reads as the root — so the whole namespace would answer for a
		// request nobody could decode.
		{"a query that does not parse", http.MethodGet, "/v3/stat?path=%zz", httprest.ErrOperands},
		{"no path at all", http.MethodGet, "/v3/stat", httprest.ErrOperands},
		{"an empty query", http.MethodGet, "/v3/stat?", httprest.ErrOperands},
		{"the path given twice", http.MethodGet, "/v3/stat?path=a&path=b", httprest.ErrOperands},
		{"an operand nobody asked for", http.MethodGet, "/v3/stat?path=a&to=b", httprest.ErrOperands},
		{"rename without a destination", http.MethodPost, "/v3/rename?path=a", httprest.ErrOperands},
		{"rename with the destination twice", http.MethodPost, "/v3/rename?path=a&to=b&to=c", httprest.ErrOperands},
		// Space describes the whole namespace, so a path beside it is a question nothing
		// can answer rather than one to answer about the root.
		{"space with a path", http.MethodGet, "/v3/space?path=a", httprest.ErrOperands},
		{"space with an empty path", http.MethodGet, "/v3/space?path=", httprest.ErrOperands},
		{"an operation that does not exist", http.MethodGet, "/v3/teleport?path=a", httprest.ErrUnknownOp},
		{"no version prefix", http.MethodGet, "/stat?path=a", httprest.ErrUnknownOp},
		{"a deeper path under the prefix", http.MethodGet, "/v3/stat/extra?path=a", httprest.ErrUnknownOp},
		{"reading with a write method", http.MethodPost, "/v3/stat?path=a", httprest.ErrMethod},
		{"writing with a read method", http.MethodGet, "/v3/remove?path=a", httprest.ErrMethod},
		{"space with a write method", http.MethodPost, "/v3/space", httprest.ErrMethod},
		// A resume point that does not parse must not degrade into one that does. Position
		// zero is where a replica that has seen nothing resumes from, so a damaged one
		// read as zero asks for the whole log — or, once the log no longer reaches that
		// far back, produces a rebuild nobody can account for.
		{"a position that is not a number", http.MethodGet, "/v3/resubscribe?incarnation=x&position=soon", httprest.ErrOperands},
		{"a position that is empty", http.MethodGet, "/v3/resubscribe?incarnation=x&position=", httprest.ErrOperands},
		{"a position before the first one", http.MethodGet, "/v3/resubscribe?incarnation=x&position=-1", httprest.ErrOperands},
		{"a position no int64 holds", http.MethodGet, "/v3/resubscribe?incarnation=x&position=99999999999999999999", httprest.ErrOperands},
		{"a resume point with no incarnation", http.MethodGet, "/v3/resubscribe?position=7", httprest.ErrOperands},
		{"a resume point with no position", http.MethodGet, "/v3/resubscribe?incarnation=x", httprest.ErrOperands},
		// Subscribing means "from now", which is a question with no operands. A resume
		// point sent beside it is a request to continue from somewhere, and answering it
		// from the tail instead would lose everything in between.
		{"subscribing with a resume point", http.MethodGet, "/v3/subscribe?incarnation=x&position=7", httprest.ErrOperands},
		{"a snapshot of a path", http.MethodGet, "/v3/snapshot?path=a", httprest.ErrOperands},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u, err := url.ParseRequestURI(c.uri)
			if err != nil {
				t.Fatalf("parse %q: %v", c.uri, err)
			}
			got, err := httprest.ParseRequest(c.method, u)
			if !errors.Is(err, c.want) {
				t.Fatalf("ParseRequest(%q, %q) = %+v, %v; want an error matching %v", c.method, c.uri, got, err, c.want)
			}
		})
	}
}

// The root is named by an empty path, which is a present operand rather than a missing
// one. Losing that distinction turns every request with a damaged query into a request
// against the whole namespace.
func TestParseRequestAcceptsTheRoot(t *testing.T) {
	u, err := url.ParseRequestURI("/v3/list?path=")
	if err != nil {
		t.Fatal(err)
	}
	got, err := httprest.ParseRequest(http.MethodGet, u)
	if err != nil {
		t.Fatalf("ParseRequest for the root: %v", err)
	}
	if want := (httprest.Request{Op: httprest.OpList}); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func ExampleRequest_URL() {
	base, _ := url.Parse("http://server.example:8080")
	u, _ := httprest.Request{Op: httprest.OpRead, Path: "notes/one two.txt"}.URL(base)
	fmt.Println(u)
	// Output: http://server.example:8080/v3/read?path=notes%2Fone+two.txt
}
