package httprest_test

import (
	"bytes"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
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
