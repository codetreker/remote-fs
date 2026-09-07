package httprest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestCanceledRequestDoesNotEnterHTTP(t *testing.T) {
	var calls atomic.Int64
	s := cancellationClient(t, &http.Client{Transport: internalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("unexpected HTTP request")
	})})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, op := range []Op{OpStat, OpSetAttr, OpList, OpRead, OpWrite, OpCreate, OpMkdir, OpRemove, OpRemoveDir, OpRename, OpSpace} {
		t.Run(string(op), func(t *testing.T) {
			body, err := s.call(ctx, Request{Op: op, Path: "f", To: "g"}, nil)
			requireCancellationClassification(t, err, syscall.EINTR)
			if body != nil {
				body.release()
				t.Fatal("canceled request retained a response")
			}
		})
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("canceled requests entered HTTP %d times", got)
	}
}

func TestResponseAdmissionCancellationDoesNotEnterHTTP(t *testing.T) {
	for _, op := range []Op{OpRead, OpWrite} {
		t.Run(string(op), func(t *testing.T) {
			var calls atomic.Int64
			s := cancellationClient(t, &http.Client{Transport: internalRoundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, errors.New("unexpected HTTP request")
			})})
			s.responses = newBodyAdmission(1, retainedResponseMultiplier*s.maxBodyBytes, 1)
			release, err := s.responses.acquire(t.Context(), retainedResponseMultiplier*s.maxBodyBytes)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				body, err := s.call(ctx, Request{Op: op, Path: "f"}, nil)
				if body != nil {
					body.release()
				}
				done <- err
			}()
			waitForAdmissionWaiters(t, s.responses, 1)
			cancel()
			requireCancellationClassification(t, awaitCancellationResult(t, done), syscall.EINTR)
			waitForAdmissionWaiters(t, s.responses, 0)
			if got := calls.Load(); got != 0 {
				t.Fatalf("a request canceled during response admission entered HTTP %d times", got)
			}
		})
	}
}

func TestHTTPReadAndMutationCancellationByPhase(t *testing.T) {
	for _, op := range []Op{OpRead, OpWrite} {
		for _, phase := range []string{"headers", "success-body", "error-body"} {
			t.Run(string(op)+"/"+phase, func(t *testing.T) {
				entered := make(chan struct{})
				reading := make(chan struct{})
				release := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Errorf("reading the request body: %v", err)
						return
					}
					close(entered)
					if phase != "headers" {
						w.Header().Set(HeaderProtocol, Version)
						w.Header().Set("Content-Length", "100")
						status := http.StatusOK
						if phase == "error-body" {
							status = StatusStorageError
						}
						w.WriteHeader(status)
						if _, err := w.Write([]byte("partial")); err != nil {
							t.Errorf("writing the response prefix: %v", err)
							return
						}
						w.(http.Flusher).Flush()
					}
					select {
					case <-r.Context().Done():
					case <-release:
					}
				}))
				t.Cleanup(server.Close)
				t.Cleanup(func() { close(release) })
				client := server.Client()
				transport := client.Transport
				client.Transport = internalRoundTripFunc(func(req *http.Request) (*http.Response, error) {
					resp, err := transport.RoundTrip(req)
					if err == nil {
						resp.Body = &cancellationObservedBody{ReadCloser: resp.Body, reading: reading}
					}
					return resp, err
				})
				s, err := Dial(server.URL, client)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					if op == OpWrite {
						done <- s.Write(ctx, "f", []byte("new content"))
						return
					}
					body, err := s.Read(ctx, "f")
					if body != nil {
						t.Errorf("interrupted read returned partial bytes %q", body)
					}
					done <- err
				}()
				awaitCancellationPhase(t, entered)
				if phase != "headers" {
					awaitCancellationPhase(t, reading)
				}
				cancel()
				want := syscall.EINTR
				if op == OpWrite {
					want = syscall.EIO
				}
				requireCancellationClassification(t, awaitCancellationResult(t, done), want)
			})
		}
	}
}

func TestHTTPFailureClassificationPreservesIndependentFaults(t *testing.T) {
	fault := errors.New("transport failed independently")
	for _, test := range []struct {
		name  string
		cause error
	}{
		{"independent failure", fault},
		{"failure before cancellation", errors.Join(fault, context.Canceled)},
		{"cancellation before failure", errors.Join(context.Canceled, fault)},
		{"deadline", context.DeadlineExceeded},
		{"deadline before cancellation", errors.Join(context.DeadlineExceeded, context.Canceled)},
		{"cancellation before deadline", errors.Join(context.Canceled, context.DeadlineExceeded)},
		{"network ENOENT", syscall.ENOENT},
		{"network EINTR", syscall.EINTR},
		{"network ENOENT before cancellation", errors.Join(syscall.ENOENT, context.Canceled)},
		{"cancellation before network ENOENT", errors.Join(context.Canceled, syscall.ENOENT)},
	} {
		for _, op := range []Op{OpRead, OpWrite} {
			for _, phase := range []string{"headers", "body"} {
				t.Run(test.name+"/"+string(op)+"/"+phase, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					s := cancellationClient(t, &http.Client{Transport: internalRoundTripFunc(func(*http.Request) (*http.Response, error) {
						cancel()
						if phase == "headers" {
							return nil, test.cause
						}
						return &http.Response{
							StatusCode:    http.StatusOK,
							Header:        http.Header{HeaderProtocol: []string{Version}},
							ContentLength: 100,
							Body:          io.NopCloser(cancellationFailureBody{cause: test.cause}),
						}, nil
					})})
					var err error
					if op == OpWrite {
						err = s.Write(ctx, "f", []byte("new content"))
					} else {
						var body []byte
						body, err = s.Read(ctx, "f")
						if body != nil {
							t.Errorf("failed read returned partial bytes %q", body)
						}
					}
					if !errors.Is(err, syscall.EIO) || storage.ErrnoOf(err) != syscall.EIO {
						t.Fatalf("transport fault classified as %v, want EIO", err)
					}
					for _, unrelated := range []error{syscall.ENOENT, syscall.EINTR, fault} {
						if errors.Is(err, unrelated) {
							t.Errorf("transport fault exposed %v through %v", unrelated, err)
						}
					}
					for _, identity := range []error{context.Canceled, context.DeadlineExceeded} {
						if got, want := errors.Is(err, identity), errors.Is(test.cause, identity); got != want {
							t.Errorf("errors.Is(%v, %v) = %v, want %v", err, identity, got, want)
						}
					}
				})
			}
		}
	}
}

func cancellationClient(t *testing.T, client *http.Client) *Storage {
	t.Helper()
	s, err := Dial("http://server.invalid", client)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func requireCancellationClassification(t *testing.T, err error, want syscall.Errno) {
	t.Helper()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled operation lost its cancellation identity: %v", err)
	}
	if !errors.Is(err, want) || storage.ErrnoOf(err) != want {
		t.Fatalf("canceled operation returned %v with classification %v, want %v", err, storage.ErrnoOf(err), want)
	}
	opposite := syscall.EIO
	if want == syscall.EIO {
		opposite = syscall.EINTR
	}
	if errors.Is(err, opposite) {
		t.Fatalf("canceled operation exposed both %v and %v: %v", want, opposite, err)
	}
}

func awaitCancellationPhase(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach the held HTTP phase")
	}
}

func awaitCancellationResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("canceled request did not return")
		return nil
	}
}

type cancellationObservedBody struct {
	io.ReadCloser
	once    sync.Once
	reading chan struct{}
}

func (b *cancellationObservedBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.reading) })
	return b.ReadCloser.Read(p)
}

type cancellationFailureBody struct{ cause error }

func (b cancellationFailureBody) Read(p []byte) (int, error) {
	return copy(p, "partial"), b.cause
}
