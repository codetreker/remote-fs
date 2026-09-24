package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func TestHTTPLostCloseResponsePreservesPendingRelease(t *testing.T) {
	for _, kind := range []string{"file", "session"} {
		for _, lostResponses := range []int{1, 2} {
			name := kind + "/immediate"
			if lostResponses == 2 {
				name = kind + "/reconciliation"
			}
			t.Run(name, func(t *testing.T) {
				meta, backend := memoryfixture.New(t, "close-lost-barrier", 1<<20, locking.DefaultOptions())
				if err := backend.Create(t.Context(), "file"); err != nil {
					t.Fatal(err)
				}
				log := &retryBarrierLog{Log: meta}
				handler, err := NewHandler(backend, log)
				if err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(handler)
				t.Cleanup(func() {
					server.Close()
					if err := handler.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				client, err := Dial(server.URL, server.Client())
				if err != nil {
					t.Fatal(err)
				}
				session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
				if err != nil {
					t.Fatal(err)
				}
				file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
				if err != nil {
					t.Fatal(err)
				}
				if kind == "session" {
					if result, err := file.CloseWithResult(t.Context()); err != nil || !result.Released {
						t.Fatalf("preparation close=%+v err=%v", result, err)
					}
				}
				log.failures = log.calls.Load() + int32(lostResponses+1)
				original := client.http.Transport
				var mu sync.Mutex
				var actions []storage.LockRequestID
				client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					body, err := request.GetBody()
					if err != nil {
						return nil, err
					}
					var command struct {
						Op     storage.Operation     `json:"op"`
						Action storage.LockRequestID `json:"action"`
					}
					err = json.NewDecoder(body).Decode(&command)
					_ = body.Close()
					if err != nil {
						return nil, err
					}
					response, err := original.RoundTrip(request)
					if err != nil {
						return response, err
					}
					if command.Op == storage.OpFileClose && kind == "file" || command.Op == storage.OpFileSessionClose && kind == "session" {
						mu.Lock()
						actions = append(actions, command.Action)
						lose := len(actions) <= lostResponses
						mu.Unlock()
						if lose {
							_, _ = io.Copy(io.Discard, response.Body)
							_ = response.Body.Close()
							return nil, errors.New("lost close response")
						}
					}
					return response, nil
				})
				close := func() (storage.ReferenceCloseResult, *MutationBarrier, error) {
					if kind == "file" {
						return file.(*remoteFile).CloseWithBarrier(t.Context())
					}
					return session.(*remoteFileSession).CloseWithBarrier(t.Context())
				}
				unknownCalls := 0
				if kind == "file" {
					unknownCalls = lostResponses - 1
				} else {
					unknownCalls = lostResponses
				}
				for range unknownCalls {
					first, barrier, err := close()
					if first.Released || barrier != nil || !errors.Is(err, syscall.EIO) {
						t.Fatalf("unknown close=%+v barrier=%+v err=%v", first, barrier, err)
					}
				}
				pending, barrier, err := close()
				var pendingErr *CloseBarrierPendingError
				if !pending.Released || barrier != nil || !errors.As(err, &pendingErr) || !errors.Is(err, syscall.EIO) {
					t.Fatalf("pending close=%+v barrier=%+v err=%v", pending, barrier, err)
				}
				settled, barrier, err := close()
				if !settled.Released || barrier == nil || err != nil {
					t.Fatalf("settled close=%+v barrier=%+v err=%v", settled, barrier, err)
				}
				mu.Lock()
				defer mu.Unlock()
				if len(actions) != lostResponses+2 || actions[0] == "" {
					t.Fatalf("close actions=%v", actions)
				}
				for _, action := range actions[1:] {
					if action != actions[0] {
						t.Fatalf("close action changed across replay: %v", actions)
					}
				}
			})
		}
	}
}
