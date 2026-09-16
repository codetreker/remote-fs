package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"io"
	"net/http"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestRetainedHTTPRejectsInconsistentActionReceipts(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	attr, err := file.Stat(ctx, storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id := retainedAction(t, session)
	intent := storage.RetainRequest{NodeID: attr.Attr.ID}
	grant, err := session.Retain(ctx, intent, id)
	if err != nil || grant.State != storage.FileActionCompleted {
		t.Fatalf("authoritative retain=%+v,%v", grant, err)
	}
	original := client.http.Transport
	for _, test := range []struct {
		name   string
		change func(*fileReceipt)
	}{
		{"different action", func(r *fileReceipt) { r.Action = "1:00000000000000000000000000000000" }},
		{"different operation", func(r *fileReceipt) { r.Operation = storage.OpFileQueryAction }},
		{"negative history", func(r *fileReceipt) { r.HistoryRemaining = -int64(time.Second) }},
		{"pending with effects", func(r *fileReceipt) { r.State = storage.FileActionPending }},
		{"not applied with effects", func(r *fileReceipt) { r.State = storage.FileActionNotApplied }},
		{"unknown with effects", func(r *fileReceipt) { r.State = storage.FileActionUnknown }},
		{"unknown state", func(r *fileReceipt) { r.State = 255 }},
		{"unknown effects", func(r *fileReceipt) { r.Effects = 1 << 31 }},
		{"missing retained reference", func(r *fileReceipt) { r.Reference = 0 }},
		{"missing retained observation", func(r *fileReceipt) { r.Observation = nil }},
		{"invalid conflict range", func(r *fileReceipt) {
			r.Conflict = &fileConflict{Kind: storage.ConflictRange, Range: &storage.HeldRange{Range: storage.RangeAcquisition{ID: 1, Start: 9, End: 1}}}
		}},
		{"invalid conflict claim", func(r *fileReceipt) {
			r.Conflict = &fileConflict{Kind: storage.ConflictClaim, Claim: storage.AccessClaim{Uses: 1 << 63}}
		}},
		{"success with errno", func(r *fileReceipt) { r.Errno = "EAGAIN" }},
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
				var value fileResponse
				if err = json.Unmarshal(body, &value); err != nil {
					return nil, err
				}
				if value.Receipt == nil {
					return nil, errors.New("authoritative response has no receipt")
				}
				test.change(value.Receipt)
				body, err = marshalFileJSON(value)
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				response.ContentLength = int64(len(body))
				response.Header.Set("Content-Length", strconv.Itoa(len(body)))
				return response, nil
			})
			result, err := session.QueryAction(ctx, id)
			if storage.ErrnoOf(err) != syscall.EIO || result.Effects != 0 || result.Reference != 0 {
				t.Fatalf("malformed receipt=%+v,%v", result, err)
			}
			client.http.Transport = original
			confirmed, err := session.QueryAction(ctx, id)
			if err != nil || confirmed.Action != id || confirmed.Reference != grant.Reference || confirmed.Effects != grant.Effects {
				t.Fatalf("native ownership changed=%+v,%v", confirmed, err)
			}
		})
	}
}
