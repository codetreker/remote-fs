package httprest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func checkpointClient(t *testing.T, handler http.Handler) *Storage {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestCheckpointCapturesCurrentLogWithoutChangingTheVolume(t *testing.T) {
	meta, backing := memoryfixture.New(t, "checkpoint", 1<<20, locking.DefaultOptions())
	handler := authorizationHandler(t, backing, meta, nil)
	client := checkpointClient(t, handler)
	initial, err := client.Checkpoint(t.Context())
	if err != nil || initial.Incarnation == "" || initial.Position != 0 {
		t.Fatalf("initial checkpoint = %+v, %v", initial, err)
	}
	if err := backing.Write(t.Context(), "file", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := backing.Write(t.Context(), "file", []byte("latest")); err != nil {
		t.Fatal(err)
	}
	before, err := meta.Barrier(t.Context(), MaxIncarnationBytes)
	if err != nil {
		t.Fatal(err)
	}
	node, err := meta.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	children, err := meta.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	space, err := backing.Space(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got, err := client.Checkpoint(t.Context())
		if err != nil || got.Incarnation != string(before.Incarnation) || got.Position != int64(before.Position) || got.Position <= initial.Position {
			t.Fatalf("checkpoint = %+v, %v; want %+v", got, err, before)
		}
	}
	after, err := meta.Barrier(t.Context(), MaxIncarnationBytes)
	if err != nil || after != before {
		t.Fatalf("checkpoint changed log: %+v, %v; before %+v", after, err, before)
	}
	gotNode, err := meta.Stat(t.Context(), "file")
	if err != nil || gotNode != node {
		t.Fatalf("checkpoint changed node: %+v, %v", gotNode, err)
	}
	gotChildren, err := meta.List(t.Context(), "")
	if err != nil || !reflect.DeepEqual(gotChildren, children) {
		t.Fatalf("checkpoint changed listing: %+v, %v", gotChildren, err)
	}
	gotSpace, err := backing.Space(t.Context())
	if err != nil || gotSpace != space {
		t.Fatalf("checkpoint changed usage: %+v, %v", gotSpace, err)
	}
}

type checkpointLog struct {
	metastore.Log
	calls    int
	maxBytes int64
	ctx      context.Context
	barrier  metastore.LogBarrier
	err      error
}

func (l *checkpointLog) Barrier(ctx context.Context, maxBytes int64) (metastore.LogBarrier, error) {
	l.calls++
	l.maxBytes = maxBytes
	l.ctx = ctx
	return l.barrier, l.err
}

func TestCheckpointAuthorizesItsOperationBeforeReadingTheLog(t *testing.T) {
	backing := volumeFixture(t)
	for name, cause := range map[string]error{"allowed": nil, "denied": authz.ErrDenied, "policy failure": errors.New("private policy failure")} {
		t.Run(name, func(t *testing.T) {
			log := &checkpointLog{barrier: metastore.LogBarrier{Incarnation: "current", Position: 7}}
			calls := 0
			h := authorizationHandler(t, backing, log, authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
				calls++
				if log.calls != 0 {
					t.Fatal("log access preceded authorization")
				}
				if ctx.Value(authorizationHostKey{}) != "host-identity" || request != (authz.AccessRequest{Volume: "trusted-volume", Operation: storage.OpReplicationCheckpoint}) {
					t.Fatalf("authorization context/request = %+v", request)
				}
				return cause
			}))
			response := httptest.NewRecorder()
			ctx := context.WithValue(t.Context(), authorizationHostKey{}, "host-identity")
			h.ServeHTTP(response, authorizationRequest(t, ctx, OpCheckpoint, ""))
			if calls != 1 {
				t.Fatalf("authorizer called %d times", calls)
			}
			if cause != nil {
				errno, message := "EIO", "authorization failed"
				if errors.Is(cause, authz.ErrDenied) {
					errno, message = "EACCES", "access denied"
				}
				authorizationAnswer(t, response, errno, message)
				if log.calls != 0 {
					t.Fatalf("refused checkpoint read log %d times", log.calls)
				}
				return
			}
			if response.Code != http.StatusOK || response.Header().Get(HeaderProtocol) != Version || response.Header().Get("Content-Type") != contentJSON || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("checkpoint response = %d %v", response.Code, response.Header())
			}
			if log.calls != 1 || log.maxBytes != h.maxIncarnationBytes || log.ctx.Value(authorizationHostKey{}) != "host-identity" {
				t.Fatalf("log access = calls %d, bound %d", log.calls, log.maxBytes)
			}
			if response.Body.String() != `{"incarnation":"current","position":7}` {
				t.Fatalf("checkpoint body = %q", response.Body.String())
			}
		})
	}
}

func TestCheckpointWithoutLogReportsENOSYSAfterAuthorization(t *testing.T) {
	backing := volumeFixture(t)
	for name, policy := range map[string]authz.Authorizer{
		"no policy": nil,
		"denied":    authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied }),
	} {
		t.Run(name, func(t *testing.T) {
			h := authorizationHandler(t, backing, nil, policy)
			got, err := checkpointClient(t, h).Checkpoint(t.Context())
			want := syscall.ENOSYS
			if policy != nil {
				want = syscall.EACCES
			}
			if !errors.Is(err, want) || got != (MutationBarrier{}) {
				t.Fatalf("checkpoint = %+v, %v; want %v", got, err, want)
			}
		})
	}
}

func TestCheckpointRejectsInvalidAuthoritativeBarriers(t *testing.T) {
	backing := volumeFixture(t)
	for name, log := range map[string]*checkpointLog{
		"missing incarnation":   {barrier: metastore.LogBarrier{Position: 1}},
		"negative position":     {barrier: metastore.LogBarrier{Incarnation: "current", Position: -1}},
		"oversized incarnation": {barrier: metastore.LogBarrier{Incarnation: metastore.Incarnation(strings.Repeat("x", MaxIncarnationBytes+1))}},
		"backend failure":       {err: syscall.EIO},
	} {
		t.Run(name, func(t *testing.T) {
			h := authorizationHandler(t, backing, log, nil)
			got, err := checkpointClient(t, h).Checkpoint(t.Context())
			if !errors.Is(err, syscall.EIO) || got != (MutationBarrier{}) || log.calls != 1 {
				t.Fatalf("invalid checkpoint = %+v, %v; calls=%d", got, err, log.calls)
			}
		})
	}
}

func TestCheckpointRejectsMalformedWireBarriers(t *testing.T) {
	for name, body := range map[string]string{
		"empty object": `{}`, "null": `null`, "array": `[]`, "missing position": `{"incarnation":"current"}`,
		"missing incarnation": `{"position":0}`, "negative position": `{"incarnation":"current","position":-1}`,
		"string position": `{"incarnation":"current","position":"1"}`, "null position": `{"incarnation":"current","position":null}`,
		"nonstring incarnation": `{"incarnation":1,"position":0}`, "unknown field": `{"incarnation":"current","position":0,"extra":true}`,
		"trailing": `{"incarnation":"current","position":0}{}`, "mutation envelope": `{"barrier":{"incarnation":"current","position":0}}`,
		"oversized incarnation": `{"incarnation":"` + strings.Repeat("x", MaxIncarnationBytes+1) + `","position":0}`,
	} {
		t.Run(name, func(t *testing.T) {
			client := checkpointClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.RequestURI() != "/v3/checkpoint" {
					t.Errorf("checkpoint request = %s %s", r.Method, r.URL.RequestURI())
				}
				w.Header().Set(HeaderProtocol, Version)
				w.Header().Set("Content-Type", contentJSON)
				io.WriteString(w, body)
			}))
			got, err := client.Checkpoint(t.Context())
			if !errors.Is(err, syscall.EIO) || got != (MutationBarrier{}) {
				t.Fatalf("malformed checkpoint = %+v, %v", got, err)
			}
		})
	}
}

func TestCheckpointRequiresProtocolAndCompleteBoundedResponse(t *testing.T) {
	for name, damage := range map[string]func(*http.Response){
		"missing protocol": func(r *http.Response) { r.Header.Del(HeaderProtocol) },
		"wrong protocol":   func(r *http.Response) { r.Header.Set(HeaderProtocol, "0") },
		"wrong status":     func(r *http.Response) { r.StatusCode = http.StatusServiceUnavailable },
		"oversized":        func(r *http.Response) { r.ContentLength = DefaultMaxBodyBytes + 1 },
		"truncated":        func(r *http.Response) { r.ContentLength++ },
	} {
		t.Run(name, func(t *testing.T) {
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				r := lockTestResponse(`{"incarnation":"current","position":7}`)
				damage(r)
				return r, nil
			})
			got, err := client.Checkpoint(t.Context())
			if !errors.Is(err, syscall.EIO) || got != (MutationBarrier{}) {
				t.Fatalf("invalid response checkpoint = %+v, %v", got, err)
			}
		})
	}
}

func TestCheckpointPreservesReadCancellationAfterDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dispatched := false
	client := lockTestClient(t, func(r *http.Request) (*http.Response, error) {
		dispatched = true
		if r.Method != http.MethodGet {
			t.Errorf("checkpoint method = %s", r.Method)
		}
		cancel()
		return nil, r.Context().Err()
	})
	got, err := client.Checkpoint(ctx)
	if !dispatched || !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.EINTR) || got != (MutationBarrier{}) {
		t.Fatalf("cancelled checkpoint = %+v, %v; dispatched=%v", got, err, dispatched)
	}
}
