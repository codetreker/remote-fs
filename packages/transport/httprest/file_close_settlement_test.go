package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

type httpSettlementBackend struct {
	*objectstore.Storage
	fileCloses      atomic.Int32
	sessionCloses   atomic.Int32
	fileSemantic    error
	sessionSemantic error
}

type httpSettlementSession struct {
	storage.FileSession
	backend *httpSettlementBackend
}
type httpSettlementFile struct {
	storage.File
	backend *httpSettlementBackend
}

func (b *httpSettlementBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &httpSettlementSession{FileSession: session, backend: b}, nil
}
func (s *httpSettlementSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	file, err := s.FileSession.OpenFile(ctx, path, options)
	if err != nil {
		return nil, err
	}
	return &httpSettlementFile{File: file, backend: s.backend}, nil
}
func (s *httpSettlementSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	s.backend.sessionCloses.Add(1)
	result, err := s.FileSession.CloseWithResult(ctx)
	if result.Released {
		err = errors.Join(err, s.backend.sessionSemantic)
	}
	return result, err
}
func (s *httpSettlementSession) Close(ctx context.Context) error {
	_, err := s.CloseWithResult(ctx)
	return err
}
func (f *httpSettlementFile) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	f.backend.fileCloses.Add(1)
	result, err := f.File.CloseWithResult(ctx)
	if result.Released {
		err = errors.Join(err, f.backend.fileSemantic)
	}
	return result, err
}
func (f *httpSettlementFile) Close(ctx context.Context) error {
	_, err := f.CloseWithResult(ctx)
	return err
}

func requireHTTPNeutralSettlement(t *testing.T, result storage.ReferenceCloseResult, err error, state storage.CloseSettlementState) {
	t.Helper()
	var unsettled *storage.CloseSettlementError
	var legacy *CloseBarrierPendingError
	if !result.Released || !result.Determined || !errors.As(err, &unsettled) || unsettled.State != state || unsettled.Check() != nil || errors.As(err, &legacy) {
		t.Fatalf("neutral close = %+v, %v; want settlement state %d", result, err, state)
	}
}

func TestHTTPCloseSettlementRetriesSameActionWithoutRepeatingNativeClose(t *testing.T) {
	for _, kind := range []string{"file", "session"} {
		for _, semantic := range []error{nil, syscall.ENOTEMPTY} {
			name := kind + "/success"
			if semantic != nil {
				name = kind + "/semantic-error"
			}
			t.Run(name, func(t *testing.T) {
				meta, native := memoryfixture.New(t, "neutral-close-settlement", 1<<20, locking.DefaultOptions())
				backend := &httpSettlementBackend{Storage: native}
				if kind == "file" {
					backend.fileSemantic = semantic
				} else {
					backend.sessionSemantic = semantic
				}
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
				operation := storage.OpFileClose
				closeNeutral := file.CloseWithResult
				nativeCalls := &backend.fileCloses
				if kind == "session" {
					if result, err := file.CloseWithResult(t.Context()); !result.Released || err != nil {
						t.Fatalf("preparation close = %+v, %v", result, err)
					}
					operation, closeNeutral, nativeCalls = storage.OpFileSessionClose, session.CloseWithResult, &backend.sessionCloses
				}
				original := client.http.Transport
				var mu sync.Mutex
				var actions []storage.LockRequestID
				loseReplay := false
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
					if command.Op != operation {
						return response, err
					}
					mu.Lock()
					actions = append(actions, command.Action)
					lose := loseReplay
					loseReplay = false
					mu.Unlock()
					if lose && err == nil {
						_, _ = io.Copy(io.Discard, response.Body)
						_ = response.Body.Close()
						return nil, errors.New("lost settlement replay")
					}
					return response, err
				})
				log.failures = log.calls.Load() + 1
				first, err := closeNeutral(t.Context())
				requireHTTPNeutralSettlement(t, first, err, storage.CloseSettlementPending)
				mu.Lock()
				loseReplay = true
				mu.Unlock()
				second, err := closeNeutral(t.Context())
				requireHTTPNeutralSettlement(t, second, err, storage.CloseSettlementUnknown)
				settled, err := closeNeutral(t.Context())
				var marker *storage.CloseSettlementError
				if !settled.Released || !settled.Determined || !errors.Is(err, semantic) || semantic == nil && err != nil || errors.Is(err, syscall.EIO) || errors.As(err, &marker) {
					t.Fatalf("settled close = %+v, %v; want semantic %v", settled, err, semantic)
				}
				if calls := nativeCalls.Load(); calls != 1 {
					t.Fatalf("settlement repeated native close %d times", calls)
				}
				mu.Lock()
				defer mu.Unlock()
				if len(actions) != 3 || actions[0] == "" || actions[0] != actions[1] || actions[0] != actions[2] {
					t.Fatalf("settlement actions = %v", actions)
				}
			})
		}
	}
}

func TestHTTPCloseSettlementProjectionPreservesIndependentFailures(t *testing.T) {
	for _, state := range []storage.CloseSettlementState{0, storage.CloseSettlementPending, storage.CloseSettlementUnknown} {
		legacy := &CloseBarrierPendingError{State: state, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EIO}
		original := errors.Join(syscall.EACCES, legacy)
		projected := neutralCloseSettlement(original)
		var neutral *storage.CloseSettlementError
		var old *CloseBarrierPendingError
		expected := state
		if expected == 0 {
			expected = storage.CloseSettlementPending
		}
		if !errors.As(projected, &neutral) || neutral.State != expected || neutral.Check() != nil || errors.As(projected, &old) || !errors.Is(projected, syscall.EACCES) || !errors.Is(projected, syscall.ENOTEMPTY) || !errors.Is(projected, syscall.EIO) {
			t.Fatalf("projection of state %d = %v", state, projected)
		}
		if legacy.State != state || legacy.SemanticErr != syscall.ENOTEMPTY || legacy.Cause != syscall.EIO {
			t.Fatalf("projection changed legacy error: %+v", legacy)
		}
	}
	if neutralCloseSettlement(nil) != nil || neutralCloseSettlement(syscall.ENOTEMPTY) != syscall.ENOTEMPTY {
		t.Fatal("settled semantic error changed during projection")
	}
}

func TestHTTPExplicitFileAndNodeCloseExposeNeutralSettlement(t *testing.T) {
	for _, kind := range []string{"file", "node"} {
		t.Run(kind, func(t *testing.T) {
			meta, backend := memoryfixture.New(t, "explicit-neutral-settlement", 1<<20, locking.DefaultOptions())
			if err := backend.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			attr, err := backend.Stat(t.Context(), "file")
			if err != nil {
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
			var closer storage.ReferenceCloseActions
			if kind == "file" {
				file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
				if err != nil {
					t.Fatal(err)
				}
				closer = file.(storage.ReferenceCloseActions)
			} else {
				opened, err := session.(storage.NodeReferences).OpenNodeRef(t.Context(), attr.ID, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}, Action: httpFileAction(t, session), MetadataAccess: storage.ReadMetadata})
				if err != nil {
					t.Fatal(err)
				}
				closer = opened.Reference.(storage.ReferenceCloseActions)
			}
			owner, err := closer.CloseOwnerStatus(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			action, err := storage.NewFileActionID(owner.CurrentEpoch)
			if err != nil {
				t.Fatal(err)
			}
			attempt := storage.CloseAttempt{Action: action, Generation: owner.NextGeneration}
			log.failures = log.calls.Load() + 1
			pending, err := closer.CloseWithAction(t.Context(), attempt)
			requireHTTPNeutralSettlement(t, pending, err, storage.CloseSettlementPending)
			settled, err := closer.CloseWithAction(t.Context(), attempt)
			if !settled.Released || err != nil {
				t.Fatalf("explicit settlement = %+v, %v", settled, err)
			}
		})
	}
}
