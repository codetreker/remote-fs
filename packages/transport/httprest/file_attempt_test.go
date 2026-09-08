package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestRetainedHTTPRejectsInconsistentAdvisoryActionReceipts(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	id, err := storage.NewLockRequestID(session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.POSIX, Type: storage.Exclusive, End: 9}
	grant, err := file.SetLock(ctx, 1, lock, id)
	if err != nil || grant.State != storage.LockGranted {
		t.Fatalf("authoritative grant=%+v, error=%v", grant, err)
	}
	original := client.http.Transport
	for _, test := range []struct {
		name   string
		set    bool
		change func(*fileLockAttempt)
	}{
		{"different action", false, func(a *fileLockAttempt) { a.Request = "1:00000000000000000000000000000000" }},
		{"invalid range", false, func(a *fileLockAttempt) { a.Lock.Start = a.Lock.End + 1 }},
		{"hidden conflict owner", false, func(a *fileLockAttempt) { a.Conflict = storage.LockConflict{Owner: 7} }},
		{"negative history", false, func(a *fileLockAttempt) { a.HistoryRemaining = -time.Second }},
		{"different set intent", true, func(a *fileLockAttempt) { a.Lock.Type = storage.Shared }},
		{"pending without wait", false, func(a *fileLockAttempt) { a.State, a.EverGranted = storage.LockPending, false }},
		{"grant without acquisition", false, func(a *fileLockAttempt) { a.EverGranted = false }},
		{"release without prior acquisition", false, func(a *fileLockAttempt) { a.State, a.EverGranted = storage.LockReleased, false }},
		{"cancellation after grant", false, func(a *fileLockAttempt) { a.State = storage.LockCancelled }},
		{"rejection without errno", false, func(a *fileLockAttempt) { a.State, a.EverGranted = storage.LockRejected, false }},
		{"unknown state", false, func(a *fileLockAttempt) { a.State = 255 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() { client.http.Transport = original }()
			client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				response, err := original.RoundTrip(request)
				if err != nil {
					return nil, err
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil {
					return nil, err
				}
				var result fileResponse
				if err := json.Unmarshal(body, &result); err != nil {
					return nil, err
				}
				if result.Attempt == nil {
					return nil, errors.New("authoritative response carries no lock receipt")
				}
				test.change(result.Attempt)
				body, err = json.Marshal(result)
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				response.ContentLength = int64(len(body))
				response.Header.Set("Content-Length", strconv.Itoa(len(body)))
				return response, nil
			})
			var result storage.LockAttempt
			if test.set {
				result, err = file.SetLock(ctx, 1, lock, id)
			} else {
				result, err = file.QueryLock(ctx, 1, id)
			}
			if storage.ErrnoOf(err) != syscall.EIO || result != (storage.LockAttempt{}) {
				t.Fatalf("inconsistent receipt returned result=%+v, error=%v", result, err)
			}
			client.http.Transport = original
			confirmed, err := file.QueryLock(ctx, 1, id)
			if err != nil || confirmed.Request != id || confirmed.State != storage.LockGranted || !confirmed.EverGranted || confirmed.Lock != lock {
				t.Fatalf("authoritative acquisition changed after malformed reply=%+v, error=%v", confirmed, err)
			}
		})
	}
}
