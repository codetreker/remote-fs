package httprest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestHTTPExplicitWriteRecoveryIsolatedAndFullSizeAtSaturation(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	session, file := httpContentOpen(t, client, backend)
	siblingValue, err := session.OpenFile(t.Context(), "content", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, Use: storage.UseClaim{Uses: storage.ReadData}})
	if err != nil {
		t.Fatal(err)
	}
	sibling := siblingValue.(*remoteFile)
	session.pendingLimit = 1
	original := client.http.Transport
	var lose atomic.Bool
	lose.Store(true)
	var replayBytes atomic.Int64
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.GetBody != nil {
			body, _ := request.GetBody()
			var req fileRequest
			_ = json.NewDecoder(body).Decode(&req)
			_ = body.Close()
			if req.Op == storage.OpFileMutate && strings.HasSuffix(request.URL.Path, string(OpFileRecovery)) {
				replayBytes.Store(int64(len(req.Mutation.Data)))
			}
			response, err := original.RoundTrip(request)
			if req.Op == storage.OpFileMutate && lose.Load() && err == nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				return nil, errors.New("lost explicit response")
			}
			return response, err
		}
		return original.RoundTrip(request)
	})
	defer func() { client.http.Transport = original }()
	data := bytes.Repeat([]byte{'x'}, 1<<20)
	command := storage.FileMutation{Action: httpFileAction(t, session), Kind: storage.MutateWriteAt, Data: data, ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"content.test": {}}}
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown=%v", err)
	}
	if _, err := sibling.ReadAt(t.Context(), 0, 3); err != nil {
		t.Fatalf("sibling blocked=%v", err)
	}
	if _, err := session.Renew(t.Context()); err != nil {
		t.Fatalf("renew blocked=%v", err)
	}
	if _, err := session.Status(t.Context()); err != nil {
		t.Fatalf("status blocked=%v", err)
	}
	changed := command
	changed.Data = []byte("different")
	if _, err := file.MutateFile(t.Context(), changed); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("intent change=%v", err)
	}
	other := command
	other.Action = httpFileAction(t, session)
	if _, err := file.MutateFile(t.Context(), other); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("pending slot bound=%v", err)
	}
	client.fileRequests = newBodyAdmission(1, retainedResponseMultiplier*client.maxBodyBytes, 0)
	client.responses = newBodyAdmission(1, retainedResponseMultiplier*client.maxBodyBytes, 0)
	releaseRequest, err := client.fileRequests.acquire(t.Context(), retainedResponseMultiplier*client.maxBodyBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseRequest()
	releaseResponse, err := client.responses.acquire(t.Context(), retainedResponseMultiplier*client.maxBodyBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseResponse()
	lose.Store(false)
	if _, err := file.MutateFile(t.Context(), command); err != nil {
		t.Fatalf("reserved recovery=%v", err)
	}
	if replayBytes.Load() != 1<<20 {
		t.Fatalf("replay bytes=%d", replayBytes.Load())
	}
	if err := sibling.Close(t.Context()); err != nil {
		t.Fatalf("cleanup at saturation=%v", err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	recoveryOperations := served.recoveryAdmission.operations
	served.mu.Unlock()
	if recoveryOperations != 0 {
		t.Fatalf("server recovery lane leaked=%d", recoveryOperations)
	}
}

func TestHTTPExplicitWriteDetachWaitsForActionGate(t *testing.T) {
	session := &remoteFileSession{explicitWrites: make(map[string]*explicitFileWrite)}
	entry := &explicitFileWrite{request: fileRequest{File: "one"}, pending: true}
	sibling := &explicitFileWrite{request: fileRequest{File: "two"}, pending: true}
	session.explicitWrites["one:action"], session.explicitWrites["two:action"] = entry, sibling
	entry.mu.Lock()
	done := make(chan struct{})
	go func() { session.detachExplicitWrites("one"); close(done) }()
	select {
	case <-done:
		t.Fatal("detached while replay owns gate")
	case <-time.After(20 * time.Millisecond):
	}
	session.mu.Lock()
	count := len(session.explicitWrites)
	session.mu.Unlock()
	if count != 2 {
		t.Fatalf("map removed before gate=%d", count)
	}
	entry.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("detach blocked")
	}
	if !entry.detached || len(session.explicitWrites) != 1 || session.explicitWrites["two:action"] != sibling {
		t.Fatalf("wrong detach=%+v", session.explicitWrites)
	}
	session.detachExplicitWrites("")
	if !sibling.detached || len(session.explicitWrites) != 0 {
		t.Fatal("parent detach failed")
	}
}
func TestHTTPExplicitWriteUnknownReleasedOnlyAfterSettlement(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	session, file := httpContentOpen(t, client, backend)
	loseFileResponses(t, client, storage.OpFileMutate, 100)
	command := storage.FileMutation{Action: httpFileAction(t, session), Kind: storage.MutateWriteAt, Data: []byte("new")}
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	result, err := file.CloseWithResult(t.Context())
	if err != nil || !result.Released {
		t.Fatalf("close=%+v err=%v", result, err)
	}
	session.mu.Lock()
	pending := len(session.explicitWrites)
	session.mu.Unlock()
	if pending != 0 {
		t.Fatalf("released retained data=%d", pending)
	}
}
func TestHTTPFileSyncRejectsActionAndPreservesBarrierClass(t *testing.T) {
	action, _ := storage.NewLockRequestID(1)
	request := fileRequest{Op: storage.OpFileSync, Session: strings.Repeat("a", 64), File: strings.Repeat("b", 64), Path: []byte{}, Data: []byte{}}
	if err := validateFileRequest(request); err != nil {
		t.Fatal(err)
	}
	request.Action = action
	if err := validateFileRequest(request); err == nil {
		t.Fatal("Sync accepts action")
	}
	if !fileMutation(storage.OpFileSync) || fileActionRequired(storage.OpFileSync) || fileControl(storage.OpFileSync) {
		t.Fatal("Sync classification lost barrier/no-journal contract")
	}
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	session, file := httpContentOpen(t, client, backend)
	original := client.http.Transport
	var calls atomic.Int32
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.GetBody != nil {
			body, _ := request.GetBody()
			var req fileRequest
			_ = json.NewDecoder(body).Decode(&req)
			_ = body.Close()
			if req.Op == storage.OpFileSync && calls.Add(1) == 1 {
				return nil, errors.New("lost sync response")
			}
		}
		return original.RoundTrip(request)
	})
	defer func() { client.http.Transport = original }()
	if err := file.Sync(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("sync failure=%v", err)
	}
	if err := file.Sync(t.Context()); err != nil {
		t.Fatalf("repeat sync=%v", err)
	}
	if calls.Load() != 2 || len(session.pending) != 0 || len(session.explicitWrites) != 0 {
		t.Fatalf("Sync journal calls=%d pending=%d", calls.Load(), len(session.pending))
	}

}

func TestHTTPRecoveryLaneIsReservedPerSessionAndRejectsOtherOperations(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	session, file := httpContentOpen(t, client, backend)
	command := storage.FileMutation{Action: httpFileAction(t, session), Kind: storage.MutateWriteAt, Data: []byte("new")}
	original := client.http.Transport
	var lose atomic.Bool
	lose.Store(true)
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := original.RoundTrip(request)
		if request.GetBody != nil {
			body, _ := request.GetBody()
			var req fileRequest
			_ = json.NewDecoder(body).Decode(&req)
			_ = body.Close()
			if req.Op == storage.OpFileMutate && lose.Load() && err == nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				return nil, errors.New("lost response")
			}
		}
		return response, err
	})
	defer func() { client.http.Transport = original }()
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	session.mu.Lock()
	session.recoveryAdmission = newBodyAdmission(1, retainedResponseMultiplier*min(client.maxBodyBytes, MaxFileRecoveryBytes), 0)
	lane := session.recoveryAdmission
	session.mu.Unlock()
	release, err := lane.acquire(t.Context(), retainedResponseMultiplier*min(client.maxBodyBytes, MaxFileRecoveryBytes))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("client recovery saturation=%v", err)
	}
	release()
	handler.files.mu.Lock()
	served := handler.files.sessions[session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	served.recoveryAdmission = newBodyAdmission(1, retainedResponseMultiplier*min(handler.maxBodyBytes, MaxFileRecoveryBytes), 0)
	serverLane := served.recoveryAdmission
	served.mu.Unlock()
	release, err = serverLane.acquire(t.Context(), retainedResponseMultiplier*min(handler.maxBodyBytes, MaxFileRecoveryBytes))
	if err != nil {
		t.Fatal(err)
	}
	lose.Store(false)
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("server recovery saturation=%v", err)
	}
	if _, err := session.Renew(t.Context()); err != nil {
		t.Fatalf("renew at recovery saturation=%v", err)
	}
	release()
	if _, err := file.MutateFile(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(fileRequest{Op: storage.OpFileSync, Session: session.id, File: file.id, Data: []byte{}, Path: []byte{}})
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, client.base.String()+Prefix+string(OpFileRecovery), bytes.NewReader(encoded))
	request.Header.Set("Content-Type", contentJSON)
	response, err := client.http.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("recovery accepts Sync=%d", response.StatusCode)
	}
}
