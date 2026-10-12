package service

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTrustedProxyClientChain(t *testing.T) {
	for _, test := range []struct {
		name, remote, chain, want string
		bad                       bool
	}{
		{"untrusted forged", "192.0.2.2:345", "198.51.100.1", "192.0.2.2:345", false},
		{"trusted client", "127.0.0.1:345", "198.51.100.1", "198.51.100.1:0", false},
		{"untrusted chain stops", "127.0.0.1:345", "203.0.113.1, 198.51.100.1", "198.51.100.1:0", false},
		{"trusted chain", "127.0.0.1:345", "203.0.113.1, 127.0.0.2", "203.0.113.1:0", false},
		{"malformed trusted", "127.0.0.1:345", "garbage", "", true},
		{"oversized chain", "127.0.0.1:345", strings.Repeat("127.0.0.2,", 33) + "203.0.113.1", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "https://service.invalid/", nil)
			r.RemoteAddr = test.remote
			r.Header.Set("X-Forwarded-For", test.chain)
			forward, err := proxyClientRequest(r, []string{"127.0.0.0/8"})
			if test.bad {
				if err == nil {
					t.Fatal("invalid trusted chain accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if forward.RemoteAddr != test.want {
				t.Fatalf("client=%s want=%s", forward.RemoteAddr, test.want)
			}
			if r.RemoteAddr != test.remote {
				t.Fatal("original request mutated")
			}
		})
	}
}

func TestServiceHTTPDrainsAcceptedRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	s := &Service{lifetime: ctx, cancel: cancel, drained: make(chan struct{})}
	s.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/data" {
			close(entered)
			<-release
		}
		w.WriteHeader(http.StatusNoContent)
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/data", nil)) }()
	<-entered
	control := httptest.NewRecorder()
	s.ServeHTTP(control, httptest.NewRequest("GET", "/health/live", nil))
	if control.Code != http.StatusNoContent {
		t.Fatalf("control status=%d", control.Code)
	}
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
	close(release)
	wg.Wait()
	select {
	case <-s.drained:
	case <-time.After(time.Second):
		t.Fatal("accepted handler did not drain")
	}
	stopped := httptest.NewRecorder()
	s.ServeHTTP(stopped, httptest.NewRequest("GET", "/health/live", nil))
	if stopped.Code != http.StatusServiceUnavailable {
		t.Fatal("stopping service admitted request")
	}
}

type revokingOutput struct {
	http.ResponseWriter
	body       bytes.Buffer
	once       sync.Once
	afterFirst func()
}

func (w *revokingOutput) SetWriteDeadline(time.Time) error { return nil }
func (w *revokingOutput) Write(p []byte) (int, error) {
	n, err := w.body.Write(p)
	w.once.Do(w.afterFirst)
	return n, err
}

func TestFileOutputRechecksRevocationBetweenUnits(t *testing.T) {
	f := newAuthFixture(t, nil)
	volume, _ := routeVolume(t, f, f.admin, "output")
	credential, secret := f.cliLogin()
	p := f.bearerPrincipal(credential.ID, secret)
	ctx := WithPrincipal(context.Background(), p)
	writer := &revokingOutput{ResponseWriter: httptest.NewRecorder(), afterFirst: func() {
		if err := f.store.RevokeCredential(ctx, p.UserID, p.AuthenticationID); err != nil {
			t.Error(err)
		}
	}}
	output := &accessResponseWriter{ResponseWriter: writer, auth: f.auth, ctx: ctx, principal: p, volumeID: volume.ID}
	n, err := output.Write(bytes.Repeat([]byte("x"), 128<<10))
	if n != 64<<10 || !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("partial output=%d error=%v", n, err)
	}
	if writer.body.Len() != 64<<10 {
		t.Fatal("output continued after credential revocation")
	}
}
