package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestRetainedHTTPNodeOperationsFollowIdentityThroughVolumeChanges(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	session, pin := openRetainedFixture(t, client)
	original, err := pin.Stat(ctx, storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Rename(ctx, "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	replacement, err := backend.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := session.Retain(ctx, storage.RetainRequest{NodeID: original.Attr.ID, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}, retainedAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.Reference(ctx, receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	read, err := file.ReadAt(ctx, storage.FileReadRequest{Length: 64})
	if err != nil || read.Attr.ID != original.Attr.ID || file.NodeID() != original.Attr.ID || pin.NodeID() != original.Attr.ID || string(read.Data) != "original" {
		t.Fatalf("identity open read=%+v, error=%v", read, err)
	}
	metadata := storage.Metadata{{Key: "application", Version: 1, Data: []byte("first")}}
	stamp := time.Unix(1700000000, 123456789)
	attr, err := session.SetNodeAttr(ctx, original.Attr.ID, storage.AttrChange{ExpectedRevision: read.Attr.MetadataRevision, Metadata: &metadata, ModTime: &stamp}, retainedAction(t, session))
	if err != nil || attr.Observation.Attr.ID != original.Attr.ID || !reflect.DeepEqual(attr.Observation.Attr.Metadata, metadata) || !attr.Observation.Attr.ModTime.Equal(stamp) {
		t.Fatalf("identity attributes=%+v, error=%v", attr, err)
	}
	moved, err := backend.Stat(ctx, "moved")
	if err != nil || moved.ID != original.Attr.ID || !reflect.DeepEqual(moved.Metadata, metadata) || !moved.ModTime.Equal(stamp) {
		t.Fatalf("renamed native attributes=%+v, error=%v", moved, err)
	}
	if err := backend.Remove(ctx, "moved"); err != nil {
		t.Fatal(err)
	}
	detached, err := file.Stat(ctx, storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	metadata = storage.Metadata{{Key: "application", Version: 1, Data: []byte("second")}}
	attr, err = session.SetNodeAttr(ctx, original.Attr.ID, storage.AttrChange{ExpectedRevision: detached.Attr.MetadataRevision, Metadata: &metadata}, retainedAction(t, session))
	if err != nil || attr.Observation.Attr.ID != original.Attr.ID || !reflect.DeepEqual(attr.Observation.Attr.Metadata, metadata) {
		t.Fatalf("detached identity attributes=%+v, error=%v", attr, err)
	}
	retained, err := file.Stat(ctx, storage.ObservationOptions{})
	if err != nil || retained.Attr.ID != original.Attr.ID || file.NodeID() != original.Attr.ID || pin.NodeID() != original.Attr.ID || !reflect.DeepEqual(retained.Attr.Metadata, metadata) {
		t.Fatalf("retained attributes=%+v, error=%v", retained, err)
	}
	current, err := backend.Stat(ctx, "file")
	if err != nil || current.ID != replacement.ID || !reflect.DeepEqual(current.Metadata, replacement.Metadata) || !current.ModTime.Equal(replacement.ModTime) {
		t.Fatalf("identity mutation changed replacement=%+v, error=%v", current, err)
	}
	if _, err := backend.Stat(ctx, "moved"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("detached attribute mutation recreated a name: %v", err)
	}
	if _, err := pin.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Retain(ctx, storage.RetainRequest{NodeID: original.Attr.ID}, retainedAction(t, session)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("reclaimed identity open=%v", err)
	}
	if _, err := session.SetNodeAttr(ctx, original.Attr.ID, storage.AttrChange{ExpectedRevision: detached.Attr.MetadataRevision, Metadata: &metadata}, retainedAction(t, session)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("reclaimed identity attributes=%v", err)
	}
}

func TestRetainedHTTPNodeOperationsPreserveValidationAndCancellationErrors(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("preserve")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	before, err := file.Stat(ctx, storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		request storage.RetainRequest
		want    syscall.Errno
	}{
		{"zero identity", storage.RetainRequest{}, syscall.EINVAL},
		{"invalid claim", storage.RetainRequest{NodeID: before.Attr.ID, Claim: storage.AccessClaim{Uses: storage.AccessUse(1) << 63}}, syscall.EINVAL},
		{"unknown identity", storage.RetainRequest{NodeID: math.MaxUint64}, syscall.ESTALE},
		{"stale revision", storage.RetainRequest{NodeID: before.Attr.ID, ExpectedMetadataRevision: before.Attr.MetadataRevision + 1}, syscall.EAGAIN},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, err := session.Retain(ctx, test.request, retainedAction(t, session))
			if !errors.Is(err, test.want) || r.Reference != 0 || r.Effects != 0 {
				t.Fatalf("retain=%+v,%v want %v", r, err, test.want)
			}
		})
	}
	invalid := storage.Metadata{{Key: "application", Version: 0}}
	if _, err := session.SetNodeAttr(ctx, before.Attr.ID, storage.AttrChange{ExpectedRevision: before.Attr.MetadataRevision, Metadata: &invalid}, retainedAction(t, session)); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid attributes=%v", err)
	}
	request, cancel := context.WithCancel(ctx)
	cancel()
	if r, err := session.Retain(request, storage.RetainRequest{NodeID: before.Attr.ID}, retainedAction(t, session)); storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, context.Canceled) || r.Reference != 0 {
		t.Fatalf("cancelled retain=%+v,%v", r, err)
	}
	stamp := time.Unix(1700000000, 0)
	if _, err := session.SetNodeAttr(request, before.Attr.ID, storage.AttrChange{ModTime: &stamp}, retainedAction(t, session)); storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled attributes=%v", err)
	}
	read, err := file.ReadAt(ctx, storage.FileReadRequest{Length: 64})
	if err != nil || string(read.Data) != "preserve" || !reflect.DeepEqual(read.Attr.Metadata, before.Attr.Metadata) || !read.Attr.ModTime.Equal(before.Attr.ModTime) {
		t.Fatalf("refused operation changed native state=%+v,%v", read, err)
	}
}

func TestRetainedHTTPRejectsResponsesThatChangeTheRetainedNode(t *testing.T) {
	ctx := t.Context()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("preserve")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	node := file.NodeID()
	original := client.http.Transport
	defer func() { client.http.Transport = original }()
	for _, op := range []storage.Operation{storage.OpFileStat, storage.OpFileRead, storage.OpFileWrite} {
		t.Run(string(op), func(t *testing.T) {
			action := retainedAction(t, session)
			client.http.Transport = fileRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				response, err := original.RoundTrip(req)
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
				if result.Observation != nil {
					result.Observation.Attr.ID = node + 1
				}
				if result.Receipt != nil && result.Receipt.Observation != nil {
					result.Receipt.Observation.Attr.ID = node + 1
				}
				body, err = marshalFileJSON(result)
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				response.ContentLength = int64(len(body))
				response.Header.Set("Content-Length", strconv.Itoa(len(body)))
				return response, nil
			})
			var err error
			switch op {
			case storage.OpFileStat:
				var r storage.FileObservation
				r, err = file.Stat(ctx, storage.ObservationOptions{})
				if r.Attr.ID != 0 {
					t.Errorf("foreign stat escaped: %+v", r)
				}
			case storage.OpFileRead:
				var r storage.FileRead
				r, err = file.ReadAt(ctx, storage.FileReadRequest{Length: 8})
				if r.Attr.ID != 0 || len(r.Data) != 0 {
					t.Errorf("foreign read escaped: %+v", r)
				}
			case storage.OpFileWrite:
				var r storage.FileActionReceipt
				r, err = file.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("preserve")}, action)
				if r.State != storage.FileActionUnknown || r.Effects != 0 || r.Action != action {
					t.Errorf("foreign mutation receipt escaped: %+v", r)
				}
			}
			client.http.Transport = original
			if storage.ErrnoOf(err) != syscall.EIO {
				t.Fatalf("changed node returned: %v", err)
			}
			if file.NodeID() != node {
				t.Fatal("response rewrote immutable node")
			}
			if op == storage.OpFileWrite {
				r, err := session.QueryAction(ctx, action)
				if err != nil || r.Observation.Attr.ID != node || r.State != storage.FileActionCompleted {
					t.Fatalf("native action lost=%+v,%v", r, err)
				}
			}
		})
	}
}

func TestRetainedHTTPReferenceRequiresNodeAndKeepsItLocally(t *testing.T) {
	ctx := t.Context()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("preserve")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	node := file.NodeID()
	original := client.http.Transport
	defer func() { client.http.Transport = original }()
	client.http.Transport = fileRoundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("NodeID performed a remote call")
		return nil, io.ErrUnexpectedEOF
	})
	if file.NodeID() != node || node == 0 {
		t.Fatal("local node identity changed")
	}
	client.http.Transport = original
	for _, missing := range []bool{false, true} {
		t.Run(strconv.FormatBool(missing), func(t *testing.T) {
			client.http.Transport = fileRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				response, err := original.RoundTrip(req)
				if err != nil {
					return nil, err
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil {
					return nil, err
				}
				var result map[string]json.RawMessage
				if err := json.Unmarshal(body, &result); err != nil {
					return nil, err
				}
				if missing {
					delete(result, "node")
				} else {
					result["node"] = json.RawMessage(`0`)
				}
				body, err = json.Marshal(result)
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				response.ContentLength = int64(len(body))
				response.Header.Set("Content-Length", strconv.Itoa(len(body)))
				return response, nil
			})
			ref, err := session.Reference(ctx, file.Reference())
			client.http.Transport = original
			if storage.ErrnoOf(err) != syscall.EIO || ref != nil {
				t.Fatalf("reference without node=%v,%v", ref, err)
			}
			ref, err = session.Reference(ctx, file.Reference())
			if err != nil || ref.NodeID() != node || ref.Reference() != file.Reference() {
				t.Fatalf("failed reply changed native reference=%v,%v", ref, err)
			}
		})
	}
}
