package httprest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/storagetest"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// newPair stands a namespace up behind a real HTTP listener and returns a storage that
// reaches it, together with the directory being served, so that a test can check what
// actually landed on disk.
func newPair(t *testing.T) (*httprest.Storage, string) {
	s, dir, _ := newPairOver(t, false)
	return s, dir
}

// newPairOver is newPair with the HTTP version chosen, and with every response's framing
// recorded on the way past.
func newPairOver(t *testing.T, http2 bool) (*httprest.Storage, string, *framing) {
	t.Helper()
	handler, dir := newHandler(t)

	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = http2
	if http2 {
		// HTTP/2 is only negotiated over TLS here, which is how it is reached in practice.
		srv.StartTLS()
	} else {
		srv.Start()
	}
	t.Cleanup(srv.Close)

	seen := &framing{inner: srv.Client().Transport}
	srv.Client().Transport = seen

	s, err := httprest.Dial(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return s, dir, seen
}

// framing records how each response was framed, so a test can assert what the handler
// produced rather than what this side made of it.
type framing struct {
	inner http.RoundTripper

	mu         sync.Mutex
	protocols  map[string]bool
	undeclared []string
}

func (f *framing) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := f.inner.RoundTrip(r)
	if err != nil {
		return resp, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.protocols == nil {
		f.protocols = map[string]bool{}
	}
	f.protocols[resp.Proto] = true
	if resp.ContentLength < 0 {
		f.undeclared = append(f.undeclared, fmt.Sprintf("%s %s answered %s with no declared length", r.Method, r.URL.RequestURI(), resp.Status))
	}
	return resp, nil
}

// The whole point of the contract suite living outside any implementation: the same
// cases that judge a local directory judge the far end of a network hop. Two
// implementations passing one suite is the evidence that storage.Storage is an
// abstraction rather than a description of localdir.
func TestContract(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage {
		s, _ := newPair(t)
		return s
	})
}

func TestBoundedContract(t *testing.T) {
	storagetest.RunBounded(t, func(t *testing.T) storage.BoundedStorage {
		s, _ := newPair(t)
		return s
	})
}

func TestDialRejectsABaseItCannotUse(t *testing.T) {
	cases := []struct {
		name string
		base string
		c    *http.Client
	}{
		{"no scheme or host", "server.example:8080", &http.Client{}},
		{"no host", "http://", &http.Client{}},
		{"not a URL at all", "://", &http.Client{}},
		{"no HTTP client", "http://server.example", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if s, err := httprest.Dial(c.base, c.c); err == nil {
				t.Fatalf("Dial(%q) gave %v, want an error", c.base, s)
			}
		})
	}
}

func TestDialWithOptionsRejectsUnboundedBodiesAndStreams(t *testing.T) {
	if _, err := httprest.DialWithOptions("http://server.example", &http.Client{}, httprest.DialOptions{}); err != nil {
		t.Fatalf("zero DialOptions did not select bounded defaults: %v", err)
	}
	if _, err := httprest.DialWithOptions("http://server.example", &http.Client{}, httprest.DialOptions{MaxBodyBytes: 5 << 20}); err != nil {
		t.Fatalf("zero MaxWriteBytes did not inherit MaxBodyBytes: %v", err)
	}
	if _, err := httprest.DialWithSilence("http://server.example", &http.Client{}, 0); err == nil {
		t.Fatal("DialWithSilence accepted an explicit zero silence bound")
	}
	usable := httprest.DefaultDialOptions()
	cases := map[string]func(*httprest.DialOptions){
		"a negative body bound":         func(o *httprest.DialOptions) { o.MaxBodyBytes = -1 },
		"an effectively unbounded body": func(o *httprest.DialOptions) { o.MaxBodyBytes = math.MaxInt64 },
		"body too large for response accounting": func(o *httprest.DialOptions) {
			o.MaxBodyBytes = math.MaxInt64/4 + 1
			o.MaxInFlightResponseBytes = math.MaxInt64
		},
		"a negative write bound":        func(o *httprest.DialOptions) { o.MaxWriteBytes = -1 },
		"write above the protocol body": func(o *httprest.DialOptions) { o.MaxWriteBytes = o.MaxBodyBytes + 1 },
		"a negative stream interval":    func(o *httprest.DialOptions) { o.Silence = -1 },
		"negative response operations":  func(o *httprest.DialOptions) { o.MaxConcurrentResponses = -1 },
		"negative response waiters":     func(o *httprest.DialOptions) { o.MaxWaitingResponses = -1 },
		"aggregate below one complete response": func(o *httprest.DialOptions) {
			o.MaxInFlightResponseBytes = 4*o.MaxBodyBytes - 1
		},
		"a frame below the protocol minimum": func(o *httprest.DialOptions) { o.MaxFrameBytes = 1 },
		"an effectively unbounded frame":     func(o *httprest.DialOptions) { o.MaxFrameBytes = math.MaxInt64 },
	}
	for name, spoil := range cases {
		t.Run(name, func(t *testing.T) {
			options := usable
			spoil(&options)
			if _, err := httprest.DialWithOptions("http://server.example", &http.Client{}, options); err == nil {
				t.Fatalf("DialWithOptions(%+v) succeeded, want an error", options)
			}
		})
	}
	if _, err := httprest.DialWithOptions("http://server.example", &http.Client{}, usable); err != nil {
		t.Fatalf("DialWithOptions with usable bounds failed: %v", err)
	}
}

func TestTheClientRefusesAnOversizedWriteBeforeSendingIt(t *testing.T) {
	const limit = int64(8)
	var requests int
	var received []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var err error
		received, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
		}
		w.Header().Set(httprest.HeaderProtocol, httprest.Version)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{}`)
	}))
	t.Cleanup(srv.Close)
	options := httprest.DefaultDialOptions()
	options.MaxBodyBytes = limit
	options.MaxWriteBytes = limit
	s, err := httprest.DialWithOptions(srv.URL, srv.Client(), options)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Write(t.Context(), "f", bytes.Repeat([]byte("x"), int(limit+1)))
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized write returned %v, want EFBIG", err)
	}
	if errors.Is(err, syscall.EIO) {
		t.Fatalf("an intentional local refusal also reports EIO: %v", err)
	}
	if requests != 0 {
		t.Fatalf("the oversized write sent %d requests, want none", requests)
	}

	want := bytes.Repeat([]byte("y"), int(limit))
	if err := s.Write(t.Context(), "f", want); err != nil {
		t.Fatalf("write at the limit: %v", err)
	}
	if requests != 1 || !bytes.Equal(received, want) {
		t.Fatalf("write at the limit sent %d requests carrying %q, want one carrying %q", requests, received, want)
	}
}

func TestMutationResponsesRequireTheirExactObjectShapeAndBarrier(t *testing.T) {
	for name, body := range map[string]string{
		"null response":       `null`,
		"null barrier":        `{"barrier":null}`,
		"unknown-only object": `{"unknown":1}`,
		"extra field":         `{"barrier":{"incarnation":"log","position":1},"extra":1}`,
		"missing incarnation": `{"barrier":{"position":1}}`,
		"missing position":    `{"barrier":{"incarnation":"log"}}`,
		"negative position":   `{"barrier":{"incarnation":"log","position":-1}}`,
		"oversized token":     `{"barrier":{"incarnation":"` + strings.Repeat("x", 2048) + `","position":1}}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(httprest.HeaderProtocol, httprest.Version)
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, body)
			}))
			defer srv.Close()
			client, err := httprest.Dial(srv.URL, srv.Client())
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Create(t.Context(), "f"); !errors.Is(err, syscall.EIO) {
				t.Fatalf("ordinary mutation accepted %s as %v", body, err)
			}
		})
	}

	t.Run("logless response is valid only for ordinary mutation", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, httprest.Version)
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{}`)
		}))
		defer srv.Close()
		client, err := httprest.Dial(srv.URL, srv.Client())
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Create(t.Context(), "f"); err != nil {
			t.Fatal(err)
		}
		if _, err := client.CreateWithBarrier(t.Context(), "f"); !errors.Is(err, syscall.EIO) {
			t.Fatalf("required barrier accepted logless response: %v", err)
		}
	})

	t.Run("position zero is a valid coherent barrier", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(httprest.HeaderProtocol, httprest.Version)
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"barrier":{"incarnation":"log","position":0}}`)
		}))
		defer srv.Close()
		client, err := httprest.Dial(srv.URL, srv.Client())
		if err != nil {
			t.Fatal(err)
		}
		barrier, err := client.CreateWithBarrier(t.Context(), "f")
		if err != nil || barrier.Position != 0 || barrier.Incarnation != "log" {
			t.Fatalf("zero barrier = %+v, %v", barrier, err)
		}
	})
}

func TestWriteAndProtocolBodiesHaveSeparateLimits(t *testing.T) {
	const (
		writeLimit = int64(4 << 20)
		bodyLimit  = int64(5 << 20)
	)
	entries := make([]storage.Entry, 17_000)
	for i := range entries {
		entries[i] = storage.Entry{
			Name: fmt.Sprintf("%05d-%s", i, strings.Repeat("n", 74)),
			Attr: storage.Attr{ID: uint64(i + 1), Mode: 0o600},
		}
	}
	encoded, err := json.Marshal(httprest.ListResponse{Entries: httprest.EntriesOf(entries)})
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(encoded)) <= writeLimit || int64(len(encoded)) > bodyLimit {
		t.Fatalf("listing body is %d bytes, want above %d and at most %d", len(encoded), writeLimit, bodyLimit)
	}
	encoded = nil

	serverOptions := httprest.DefaultHandlerOptions()
	serverOptions.MaxBodyBytes = bodyLimit
	serverOptions.MaxWriteBytes = writeLimit
	serverOptions.MaxInFlightBodyBytes = bodyLimit
	if err := serverOptions.Check(); err != nil {
		t.Fatal(err)
	}
	h, err := httprest.NewHandlerWithOptions(listingStorage{
		failing: failing{syscall.EIO},
		entries: entries,
	}, nil, serverOptions)
	if err != nil {
		t.Fatal(err)
	}

	writeRequest := newServerRequest(t, httprest.Request{Op: httprest.OpWrite, Path: "large"}, panicReader{})
	writeRequest.ContentLength = bodyLimit
	writeResponse := httptest.NewRecorder()
	h.ServeHTTP(writeResponse, writeRequest)
	var refusal httprest.ErrorResponse
	if err := json.Unmarshal(writeResponse.Body.Bytes(), &refusal); err != nil {
		t.Fatal(err)
	}
	if writeResponse.Code != httprest.StatusStorageError || refusal.Errno != "EFBIG" {
		t.Fatalf("server write limit answered %d %+v", writeResponse.Code, refusal)
	}

	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	clientOptions := httprest.DefaultDialOptions()
	clientOptions.MaxBodyBytes = bodyLimit
	clientOptions.MaxWriteBytes = writeLimit
	s, err := httprest.DialWithOptions(srv.URL, srv.Client(), clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(t.Context(), "large", bytes.Repeat([]byte("x"), int(bodyLimit))); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("client write returned %v, want EFBIG", err)
	}
	if requests != 0 {
		t.Fatalf("client sent %d requests for a locally oversized write", requests)
	}
	listed, err := s.List(t.Context(), "")
	if err != nil {
		t.Fatalf("listing above the write limit and within the body limit failed: %v", err)
	}
	if len(listed) != len(entries) || requests != 1 {
		t.Fatalf("listed %d entries through %d requests, want %d through one", len(listed), requests, len(entries))
	}
}

type listingStorage struct {
	failing
	entries []storage.Entry
}

func (s listingStorage) List(context.Context, string) ([]storage.Entry, error) {
	return s.entries, nil
}

func (s listingStorage) ListBounded(_ context.Context, _ string, result *storage.ListResult) error {
	for _, entry := range s.entries {
		if err := result.Add(entry); err != nil {
			return err
		}
	}
	return nil
}

func TestClientListBoundedInvalidatesAnEarlyDecodedPrefix(t *testing.T) {
	entries := []storage.Entry{
		{Name: "a", Attr: storage.Attr{ID: 1, Mode: 0o600}},
		{Name: "b", Attr: storage.Attr{ID: 2, Mode: 0o600}},
	}
	h, err := httprest.NewHandler(listingStorage{failing: failing{syscall.EIO}, entries: entries}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	client, err := httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := storage.NewListResult(1024, 0, func(_ int, _ int64, _ storage.Attr) (int64, error) {
		return 600, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ListBounded(t.Context(), "", result); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ListBounded returned %v, want EIO", err)
	}
	if entries, err := result.Entries(); err == nil || entries != nil {
		t.Fatalf("the failed decode exposed a partial listing: %+v, %v", entries, err)
	}
}

func TestClientListBoundedStreamsACompleteStrictListing(t *testing.T) {
	want := []storage.Entry{
		{Name: "b", Attr: storage.Attr{ID: 2, Mode: 0o600, Size: 7}},
		{Name: "a", Attr: storage.Attr{ID: 1, Mode: fs.ModeDir | 0o700}},
	}
	body, err := json.Marshal(httprest.ListResponse{Entries: httprest.EntriesOf(want)})
	if err != nil {
		t.Fatal(err)
	}
	client := dialListingBody(t, body)
	result := newClientListResult(t)
	if err := client.ListBounded(t.Context(), "", result); err != nil {
		t.Fatalf("ListBounded: %v", err)
	}
	got, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(want, func(a, b storage.Entry) int { return strings.Compare(a.Name, b.Name) })
	if !slices.Equal(got, want) {
		t.Fatalf("streamed listing = %+v, want %+v", got, want)
	}
}

func TestClientListBoundedRejectsEveryIncompleteOrExtendedResponseShape(t *testing.T) {
	for name, body := range map[string]string{
		"empty body":               ``,
		"array instead of object":  `[]`,
		"missing listing":          `{}`,
		"truncated field":          `{"`,
		"wrong first field":        `{"other":[]}`,
		"truncated array field":    `{"entries"`,
		"null listing":             `{"entries":null}`,
		"invalid entry":            `{"entries":[{}]}`,
		"unterminated array":       `{"entries":[`,
		"extra response field":     `{"entries":[],"extra":1}`,
		"unterminated response":    `{"entries":[]`,
		"content after the object": `{"entries":[]} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			result := newClientListResult(t)
			err := dialListingBody(t, []byte(body)).ListBounded(t.Context(), "", result)
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("ListBounded returned %v, want EIO", err)
			}
			if entries, resultErr := result.Entries(); entries != nil || !errors.Is(resultErr, syscall.EIO) {
				t.Fatalf("failed response exposed entries=%+v, err=%v", entries, resultErr)
			}
		})
	}
}

func dialListingBody(t *testing.T, body []byte) *httprest.Storage {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(httprest.HeaderProtocol, httprest.Version)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	client, err := httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newClientListResult(t *testing.T) *storage.ListResult {
	t.Helper()
	result, err := storage.NewListResult(1<<20, 0, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + 256, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestTheClientBoundsReadResponses(t *testing.T) {
	const limit = int64(8)
	options := httprest.DefaultDialOptions()
	options.MaxBodyBytes = limit
	options.MaxWriteBytes = limit
	marked := func(body io.Reader, length int64, transfer []string) *http.Response {
		header := http.Header{}
		header.Set(httprest.HeaderProtocol, httprest.Version)
		return &http.Response{
			Status:           "200 OK",
			StatusCode:       http.StatusOK,
			Header:           header,
			ContentLength:    length,
			TransferEncoding: transfer,
			Body:             io.NopCloser(body),
		}
	}

	t.Run("a declared oversized response is not read", func(t *testing.T) {
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return marked(panicReader{}, limit+1, nil), nil
		})}
		s, err := httprest.DialWithOptions("http://server.invalid", client, options)
		if err != nil {
			t.Fatal(err)
		}
		content, err := s.Read(t.Context(), "f")
		if !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("read returned %q, %v; want EFBIG", content, err)
		}
	})

	t.Run("an unmarked oversized response states no file-size outcome", func(t *testing.T) {
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			response := marked(panicReader{}, limit+1, nil)
			response.Header.Del(httprest.HeaderProtocol)
			return response, nil
		})}
		s, err := httprest.DialWithOptions("http://server.invalid", client, options)
		if err != nil {
			t.Fatal(err)
		}
		content, err := s.Read(t.Context(), "f")
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("read returned %q, %v; want EIO", content, err)
		}
		if errors.Is(err, syscall.EFBIG) {
			t.Fatalf("an unmarked response reported a file-size outcome: %v", err)
		}
	})

	t.Run("an unknown length stops after its first excess byte", func(t *testing.T) {
		body := &repeatingReader{}
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return marked(body, -1, []string{"chunked"}), nil
		})}
		s, err := httprest.DialWithOptions("http://server.invalid", client, options)
		if err != nil {
			t.Fatal(err)
		}
		content, err := s.Read(t.Context(), "f")
		if !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("read returned %q, %v; want EFBIG", content, err)
		}
		if body.read != limit+1 {
			t.Fatalf("the client read %d bytes, want the limit plus one (%d)", body.read, limit+1)
		}
	})

	t.Run("an oversized protocol message is not a file-size answer", func(t *testing.T) {
		body := &repeatingReader{}
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return marked(body, -1, []string{"chunked"}), nil
		})}
		s, err := httprest.DialWithOptions("http://server.invalid", client, options)
		if err != nil {
			t.Fatal(err)
		}
		entries, err := s.List(t.Context(), "d")
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("list returned %v, %v; want EIO", entries, err)
		}
		if errors.Is(err, syscall.EFBIG) {
			t.Fatalf("an oversized listing was reported as a file-size refusal: %v", err)
		}
		if body.read != limit+1 {
			t.Fatalf("the client read %d bytes, want the limit plus one (%d)", body.read, limit+1)
		}
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Content has to cross unchanged whatever bytes it is made of. A file is not text.
func TestContentSurvivesTheRoundTrip(t *testing.T) {
	s, dir := newPair(t)
	cases := map[string][]byte{
		"empty":                     {},
		"one nul":                   {0x00},
		"not utf-8":                 {0xff, 0xfe, 0x80, 0x00, 0x0a},
		"a lone surrogate as bytes": {0xed, 0xa0, 0x80},
		"text":                      []byte("hello\nworld\r\n"),
		"large":                     bytes.Repeat([]byte{0x00, 0x01, 0xfe, 0xff}, 64*1024),
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if err := s.Write(t.Context(), "f", content); err != nil {
				t.Fatalf("write: %v", err)
			}
			// Verified from the other side: the file on disk, not the client's own
			// account of what it sent.
			onDisk, err := os.ReadFile(filepath.Join(dir, "f"))
			if err != nil {
				t.Fatalf("read the file on disk: %v", err)
			}
			if !bytes.Equal(onDisk, content) {
				t.Fatalf("the file on disk holds %d bytes, want %d", len(onDisk), len(content))
			}

			got, err := s.Read(t.Context(), "f")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(got, content) {
				t.Fatalf("read back %d bytes, want %d", len(got), len(content))
			}

			attr, err := s.Stat(t.Context(), "f")
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			if attr.Size != int64(len(content)) {
				t.Fatalf("stat reports size %d, want %d", attr.Size, len(content))
			}
		})
	}
}

// A name that a URL damages addresses a different node than the caller asked for, and
// the caller has no way to notice.
func TestAwkwardNamesAddressTheRightNode(t *testing.T) {
	names := []string{
		"plain",
		"with space",
		"hash#mark",
		"question?mark",
		"percent%25sign",
		"plus+sign",
		"ampersand&equals=here",
		"semicolon;comma,",
		"quote'and\"double",
		"日本語のファイル",
		"emoji-🙂",
		"\xff\xfe not utf-8",
		"dash-and_underscore.ext",
	}
	s, dir := newPair(t)
	for _, name := range names {
		content := []byte("contents of " + name)
		if err := s.Write(t.Context(), name, content); err != nil {
			t.Fatalf("write %q: %v", name, err)
		}
		onDisk, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("the namespace has no file named %q on disk: %v", name, err)
		}
		if !bytes.Equal(onDisk, content) {
			t.Fatalf("the file named %q holds %q, want %q", name, onDisk, content)
		}
		got, err := s.Read(t.Context(), name)
		if err != nil {
			t.Fatalf("read %q: %v", name, err)
		}
		if !bytes.Equal(got, content) {
			t.Fatalf("read %q gave %q, want %q", name, got, content)
		}
	}

	entries, err := s.List(t.Context(), "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	listed := map[string]bool{}
	for _, e := range entries {
		listed[e.Name] = true
	}
	for _, name := range names {
		if !listed[name] {
			t.Fatalf("the listing does not carry %q", name)
		}
	}
}

// Nested and awkward directory paths have to survive too — the separator is the one
// character a URL is most likely to reinterpret.
func TestNestedPathsAddressTheRightNode(t *testing.T) {
	s, dir := newPair(t)
	for _, d := range []string{"a", "a/b b", "a/b b/c#c"} {
		if err := s.Mkdir(t.Context(), d); err != nil {
			t.Fatalf("mkdir %q: %v", d, err)
		}
	}
	const deep = "a/b b/c#c/leaf?.txt"
	if err := s.Write(t.Context(), deep, []byte("deep")); err != nil {
		t.Fatalf("write %q: %v", deep, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "a", "b b", "c#c", "leaf?.txt")); err != nil {
		t.Fatalf("the nested file is not where it should be on disk: %v", err)
	}

	const moved = "a/b b/renamed 🙂.txt"
	if err := s.Rename(t.Context(), deep, moved); err != nil {
		t.Fatalf("rename: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "a", "b b", "renamed 🙂.txt"))
	if err != nil {
		t.Fatalf("read the renamed file on disk: %v", err)
	}
	if string(got) != "deep" {
		t.Fatalf("the renamed file holds %q, want %q", got, "deep")
	}
}

// The client refuses a response whose body would end where the connection does, because
// such a body cannot report having been cut short. That is only safe if our own server
// never produces one, and it is not enough for the handler to set a length: HTTP/2 frames
// responses itself, so both versions are driven here, over every path a client can reach —
// the answers that carry a body, the answers that carry none, and the failures.
func TestEveryAnswerDeclaresItsLength(t *testing.T) {
	for _, version := range []struct {
		name  string
		http2 bool
		proto string
	}{{"HTTP/1.1", false, "HTTP/1.1"}, {"HTTP/2", true, "HTTP/2.0"}} {
		t.Run(version.name, func(t *testing.T) {
			s, _, seen := newPairOver(t, version.http2)
			ctx := t.Context()

			for _, op := range []struct {
				name string
				run  func() error
			}{
				{"mkdir", func() error { return s.Mkdir(ctx, "d") }},
				{"create", func() error { return s.Create(ctx, "d/f") }},
				{"write", func() error { return s.Write(ctx, "d/f", []byte("payload")) }},
				{"read", func() error { _, err := s.Read(ctx, "d/f"); return err }},
				{"read an empty file", func() error { return s.Write(ctx, "d/empty", nil) }},
				{"stat", func() error { _, err := s.Stat(ctx, "d/f"); return err }},
				{"setattr", func() error {
					mode := fs.FileMode(0o600)
					return s.SetAttr(ctx, "d/f", storage.AttrChange{Mode: &mode})
				}},
				{"list", func() error { _, err := s.List(ctx, "d"); return err }},
				{"rename", func() error { return s.Rename(ctx, "d/f", "d/g") }},
				{"remove", func() error { return s.Remove(ctx, "d/g") }},
				{"space", func() error { _, err := s.Space(ctx); return err }},
			} {
				if err := op.run(); err != nil {
					t.Fatalf("%s: %v", op.name, err)
				}
			}
			// The failing paths answer with a different writer, so they are their own case.
			if _, err := s.Stat(ctx, "missing"); err == nil {
				t.Fatal("stat of a missing file succeeded")
			}
			if _, err := s.List(ctx, "missing"); err == nil {
				t.Fatal("list of a missing directory succeeded")
			}
			if _, err := s.Read(ctx, "missing"); err == nil {
				t.Fatal("read of a missing file succeeded")
			}
			if err := s.Mkdir(ctx, "../outside"); err == nil {
				t.Fatal("mkdir outside the root succeeded")
			}
			if err := s.SetAttr(ctx, "missing", storage.AttrChange{}); err == nil {
				t.Fatal("setattr on a missing node succeeded")
			}

			if len(seen.undeclared) != 0 {
				t.Fatalf("responses arrived with no declared length: %v", seen.undeclared)
			}
			// Without this the HTTP/2 case could silently be a second HTTP/1.1 case.
			if !seen.protocols[version.proto] {
				t.Fatalf("the exchange ran over %v, want %s", slices.Collect(maps.Keys(seen.protocols)), version.proto)
			}
		})
	}
}
