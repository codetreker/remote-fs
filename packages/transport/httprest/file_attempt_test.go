package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
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
	fileRanges, fileOwners := rangeControlFixture(t, session, file, 1)
	id, err := storage.NewLockRequestID(session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.RangeCommand{Domain: storage.DomainRecord, Edit: storage.Replace, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Length: 10}}
	grant, err := fileRanges.Apply(ctx, fileOwners[0], []storage.RangeCommand{lock}, id)
	if err != nil || grant.State != storage.Granted {
		t.Fatalf("authoritative grant=%+v, error=%v", grant, err)
	}
	original := client.http.Transport
	for _, test := range []struct {
		name   string
		set    bool
		change func(*storage.RangeAttempt)
	}{
		{"different action", false, func(a *storage.RangeAttempt) { a.Request = "1:00000000000000000000000000000000" }},
		{"missing commands", false, func(a *storage.RangeAttempt) { a.Commands = nil }},
		{"invalid claim", false, func(a *storage.RangeAttempt) { a.Claims = []storage.ClaimID{"invalid"} }},
		{"invalid effect", false, func(a *storage.RangeAttempt) { a.Effects = []storage.RangeEffect{{Command: storage.RangeCommand{}}} }},
		{"invalid range", false, func(a *storage.RangeAttempt) { a.Commands[0].Range.Length = 0 }},
		{"hidden conflict owner", false, func(a *storage.RangeAttempt) { a.Conflict = storage.RangeConflict{Owner: 7} }},
		{"negative history", false, func(a *storage.RangeAttempt) { a.HistoryRemaining = -time.Second }},
		{"different set intent", true, func(a *storage.RangeAttempt) { a.Commands[0].Mode = storage.RangeShared }},
		{"pending without wait", false, func(a *storage.RangeAttempt) { a.State, a.EverGranted = storage.Pending, false }},
		{"grant without acquisition", false, func(a *storage.RangeAttempt) { a.EverGranted = false }},
		{"release reported as grant", false, func(a *storage.RangeAttempt) {
			a.State = storage.Granted
			a.EverGranted = true
			a.Commands[0].Edit = storage.Subtract
			a.Effects[0].Command.Edit = storage.Subtract
			a.Effects[0].Released = true
		}},
		{"release without prior acquisition", false, func(a *storage.RangeAttempt) { a.State, a.EverGranted = storage.Released, false }},
		{"cancellation after grant", false, func(a *storage.RangeAttempt) { a.State = storage.Cancelled }},
		{"rejection without reason", false, func(a *storage.RangeAttempt) { a.State, a.EverGranted = storage.Rejected, false }},
		{"unknown rejection", false, func(a *storage.RangeAttempt) {
			a.State, a.EverGranted, a.Rejection = storage.Rejected, false, "unknown"
		}},
		{"unknown state", false, func(a *storage.RangeAttempt) { a.State = 255 }},
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
			var result storage.RangeAttempt
			if test.set {
				result, err = fileRanges.Apply(ctx, fileOwners[0], []storage.RangeCommand{lock}, id)
			} else {
				result, err = fileRanges.Query(ctx, fileOwners[0], id)
			}
			if storage.ErrnoOf(err) != syscall.EIO || !reflect.DeepEqual(result, storage.RangeAttempt{}) {
				t.Fatalf("inconsistent receipt returned result=%+v, error=%v", result, err)
			}
			client.http.Transport = original
			confirmed, err := fileRanges.Query(ctx, fileOwners[0], id)
			if err != nil || confirmed.Request != id || confirmed.State != storage.Granted || !confirmed.EverGranted || !reflect.DeepEqual(confirmed.Commands, []storage.RangeCommand{lock}) {
				t.Fatalf("authoritative acquisition changed after malformed reply=%+v, error=%v", confirmed, err)
			}
		})
	}
}
