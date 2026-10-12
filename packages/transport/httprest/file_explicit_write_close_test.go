package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestHTTPReleasedCloseCancelsPendingWriteDetach(t *testing.T) {
	for _, kind := range []string{"implicit file", "explicit file", "parent session"} {
		for _, lost := range []bool{false, true} {
			responseKind := "completed response"
			if lost {
				responseKind = "lost response"
			}
			t.Run(kind+"/"+responseKind, func(t *testing.T) {
				client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
				session, file := httpContentOpen(t, client, backend)
				original := client.http.Transport
				completed := make(chan struct{})
				release := make(chan struct{})
				var once sync.Once
				var mutationCalls atomic.Int32
				var closeCalls atomic.Int32
				client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					body, err := request.GetBody()
					if err != nil {
						return nil, err
					}
					var req fileRequest
					err = json.NewDecoder(body).Decode(&req)
					_ = body.Close()
					if err != nil {
						return nil, err
					}
					response, err := original.RoundTrip(request)
					if req.Op == storage.OpFileClose || req.Op == storage.OpFileSessionClose {
						closeCalls.Add(1)
					}
					if req.Op != storage.OpFileMutate || err != nil {
						return response, err
					}
					mutationCalls.Add(1)
					once.Do(func() { close(completed) })
					<-release
					if !lost {
						return response, nil
					}
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
					return nil, errors.New("lost completed write response")
				})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				t.Cleanup(func() { unblock(); client.http.Transport = original })
				var attempt storage.CloseAttempt
				if kind == "explicit file" {
					owner, err := file.CloseOwnerStatus(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					id, err := storage.NewFileActionID(owner.CurrentEpoch)
					if err != nil {
						t.Fatal(err)
					}
					attempt = storage.CloseAttempt{Action: id, Generation: owner.NextGeneration}
				}
				closeCall := func(ctx context.Context) (storage.ReferenceCloseResult, error) {
					switch kind {
					case "parent session":
						return session.CloseWithResult(ctx)
					case "explicit file":
						return file.CloseWithAction(ctx, attempt)
					default:
						return file.CloseWithResult(ctx)
					}
				}
				command := storage.FileMutation{Action: httpFileAction(t, session), Kind: storage.MutateWriteAt, Data: bytes.Repeat([]byte{'x'}, 4096)}
				writeDone := make(chan error, 1)
				go func() { _, err := file.MutateFile(t.Context(), command); writeDone <- err }()
				select {
				case <-completed:
				case <-time.After(time.Second):
					t.Fatal("server mutation did not complete")
				}
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				type closeOutcome struct {
					result storage.ReferenceCloseResult
					err    error
				}
				closeDone := make(chan closeOutcome, 1)
				go func() {
					result, err := closeCall(ctx)
					closeDone <- closeOutcome{result: result, err: err}
				}()
				// Detachment begins only after the client has decoded Released. The
				// cancellation must reach the blocked local gate, not the close RPC.
				phaseDeadline := time.NewTimer(30 * time.Second)
				phasePoll := time.NewTicker(time.Millisecond)
				for {
					session.mu.Lock()
					entry := session.explicitWrites[file.id+":"+string(command.Action)]
					detaching := entry != nil && entry.detached
					session.mu.Unlock()
					if detaching {
						break
					}
					select {
					case outcome := <-closeDone:
						phaseDeadline.Stop()
						phasePoll.Stop()
						t.Fatalf("close returned before local detachment: result=%+v err=%v", outcome.result, outcome.err)
					case <-phaseDeadline.C:
						phasePoll.Stop()
						t.Fatal("close did not enter local detachment")
					case <-phasePoll.C:
					}
				}
				phaseDeadline.Stop()
				phasePoll.Stop()
				cancel()
				var outcome closeOutcome
				select {
				case outcome = <-closeDone:
				case <-time.After(time.Second):
					unblock()
					t.Fatal("close ignored caller cancellation")
				}
				result, err := outcome.result, outcome.err
				var pending *storage.CloseSettlementError
				if !result.Released || !result.Determined || !errors.As(err, &pending) || pending.State != storage.CloseSettlementPending || !errors.Is(err, context.Canceled) {
					unblock()
					t.Fatalf("canceled close result=%+v err=%v", result, err)
				}
				session.mu.Lock()
				entry := session.explicitWrites[file.id+":"+string(command.Action)]
				retained := entry != nil && entry.detached && bytes.Equal(entry.request.Mutation.Data, command.Data)
				session.mu.Unlock()
				if !retained {
					unblock()
					t.Fatal("cancelled detach lost payload or replay fence")
				}
				cancelled, cancel := context.WithCancel(t.Context())
				cancel()
				retry, err := closeCall(cancelled)
				if !retry.Released || !errors.As(err, &pending) || !errors.Is(err, context.Canceled) {
					unblock()
					t.Fatalf("cached cancelled close=%+v err=%v", retry, err)
				}
				if closeCalls.Load() != 1 {
					unblock()
					t.Fatalf("cached local settlement repeated close RPC=%d", closeCalls.Load())
				}
				unblock()
				select {
				case <-writeDone:
				case <-time.After(time.Second):
					t.Fatal("write did not return")
				}
				session.mu.Lock()
				retained = session.explicitWrites[file.id+":"+string(command.Action)] == entry
				session.mu.Unlock()
				if !retained {
					t.Fatal("late write response freed pending ownership before close retry")
				}
				result, err = closeCall(t.Context())
				if !result.Released || err != nil {
					t.Fatalf("settled retry=%+v err=%v", result, err)
				}
				session.mu.Lock()
				count := len(session.explicitWrites)
				session.mu.Unlock()
				if count != 0 || mutationCalls.Load() != 1 || closeCalls.Load() != 1 {
					t.Fatalf("settled map=%d mutationRPC=%d closeRPC=%d", count, mutationCalls.Load(), closeCalls.Load())
				}
				if kind == "explicit file" {
					result, err = closeCall(t.Context())
					if !result.Released || err != nil || closeCalls.Load() != 2 {
						t.Fatalf("settled explicit replay lost authority check: result=%+v err=%v calls=%d", result, err, closeCalls.Load())
					}
				}
			})
		}
	}
}

func TestHTTPPendingWriteGateWaitCancelsWithoutChangingAction(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	session, file := httpContentOpen(t, client, backend)
	command := storage.FileMutation{Action: httpFileAction(t, session), Kind: storage.MutateWriteAt, Data: []byte("new")}
	request := fileRequest{Op: storage.OpFileMutate, File: file.id, Session: session.id, Action: storage.LockRequestID(command.Action), ResultBytes: MaxFileRecoveryBytes, Mutation: fileMutationOf(command), Path: []byte{}, Data: []byte{}}
	scope, hasScope := session.outgoingMutationScope(t.Context(), request)
	entry, err := session.writeEntry(request, scope, hasScope)
	if err != nil {
		t.Fatal(err)
	}
	entry.pending = true
	entry.unknown = syscall.EIO
	if err := entry.gate.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	_, err = session.callExplicitWrite(ctx, request)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		entry.gate.unlock()
		t.Fatalf("gate wait=%v", err)
	}
	if !entry.pending || entry.unknown != syscall.EIO || len(session.explicitWrites) != 1 {
		entry.gate.unlock()
		t.Fatal("cancel altered pending outcome")
	}
	entry.gate.unlock()
}

func TestHTTPPendingWriteDetachPreservesSemanticError(t *testing.T) {
	session := &remoteFileSession{explicitWrites: make(map[string]*explicitFileWrite)}
	entry := &explicitFileWrite{request: fileRequest{File: "file", Mutation: &fileMutationOptions{Data: []byte("held")}}}
	session.explicitWrites["file:action"] = entry
	if err := entry.gate.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	err := session.settleExplicitWrites(cancelled, "file", syscall.ENOTEMPTY)
	var pending *storage.CloseSettlementError
	if !errors.As(err, &pending) || !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, context.Canceled) || pending.Check() != nil {
		entry.gate.unlock()
		t.Fatalf("pending semantic=%v", err)
	}
	if session.explicitWrites["file:action"] != entry {
		entry.gate.unlock()
		t.Fatal("cancelled detach dropped data")
	}
	entry.gate.unlock()
	if err := session.settleExplicitWrites(t.Context(), "file", syscall.ENOTEMPTY); err != syscall.ENOTEMPTY || len(session.explicitWrites) != 0 {
		t.Fatalf("settled semantic=%v", err)
	}
}
