package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

type informationMutationCapture struct {
	mu       sync.Mutex
	request  fileRequest
	replay   fileRequest
	response fileResponse
	calls    int
	recovery bool
}

func loseInformationMutation(t *testing.T, client *Storage, action storage.FileActionID) *informationMutationCapture {
	t.Helper()
	capture := &informationMutationCapture{}
	original := client.http.Transport
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
		if req.Op != storage.OpFileMutate || req.Mutation.Action != action || err != nil {
			return response, err
		}
		capture.mu.Lock()
		defer capture.mu.Unlock()
		capture.calls++
		if capture.calls > 1 {
			capture.replay = req
			capture.recovery = strings.HasSuffix(request.URL.Path, string(OpFileRecovery))
			return response, nil
		}
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			return nil, err
		}
		capture.request = req
		if err := json.Unmarshal(data, &capture.response); err != nil {
			return nil, err
		}
		return nil, errors.New("lost completed file-information mutation response")
	})
	t.Cleanup(func() { client.http.Transport = original })
	return capture
}

func TestHTTPInformationRecoveryKeepsOriginalResultBound(t *testing.T) {
	_, backend := memoryfixture.New(t, "information-bound-http", 1<<20, locking.DefaultOptions())
	for _, kind := range []string{"basic file", "basic node", "EOF"} {
		t.Run(kind, func(t *testing.T) {
			client, _ := retainedHTTPPair(t, DefaultFileLimits(), backend)
			session, file := httpContentOpenNamed(t, client, backend, strings.ReplaceAll(kind, " ", "_"))
			var mutate func(context.Context, storage.FileMutation) (storage.Attr, *MutationBarrier, error)
			var command storage.FileMutation
			referenceID := file.id
			if kind == "EOF" {
				mutate = file.MutateFileWithBarrier
				command = storage.FileMutation{Action: httpFileAction(t, session), Kind: storage.MutateTruncate, Size: 7, ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"content.test": {}}}
			} else {
				opened, err := session.OpenNodeRef(t.Context(), file.node, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: file.node}, Action: httpFileAction(t, session), MetadataAccess: storage.WriteMetadata})
				if err != nil {
					t.Fatal(err)
				}
				reference := opened.Reference.(*remoteNodeReference)
				if kind == "basic node" {
					mutate = reference.MutateFileWithBarrier
				} else {
					mutate = reference.file.MutateFileWithBarrier
				}
				referenceID = reference.file.id
				changed := time.Unix(123, 456).UTC()
				command = storage.FileMutation{Action: httpFileAction(t, session), Kind: storage.MutateAttributes, Attr: storage.AttrChange{ModTime: &changed}, Metadata: map[string]storage.OpaquePayload{"basic.flags": {Data: []byte{1, 2}}}}
			}
			bounded := func(limit int64) context.Context {
				return storage.WithBoundedAttrResult(t.Context(), limit, func(storage.Attr, int64) error { return nil })
			}
			capture := loseInformationMutation(t, client, command.Action)
			if result, barrier, err := mutate(bounded(4096), command); !errors.Is(err, syscall.EIO) || result.ID != 0 || barrier != nil {
				t.Fatalf("bounded unknown=%+v barrier=%+v err=%v", result, barrier, err)
			}
			key := referenceID + ":" + string(command.Action)
			session.mu.Lock()
			entry := session.explicitWrites[key]
			session.mu.Unlock()
			if entry == nil || entry.request.ResultBytes != 4096 || capture.request.ResultBytes != 4096 {
				t.Fatalf("original bound not retained: entry=%+v request=%+v", entry, capture.request)
			}
			if _, _, err := mutate(bounded(512), command); !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("smaller recovery bound=%v", err)
			}
			session.mu.Lock()
			sameEntry := session.explicitWrites[key] == entry
			session.mu.Unlock()
			if !sameEntry || entry.request.ResultBytes != 4096 || capture.calls != 1 {
				t.Fatalf("smaller bound altered pending intent or sent RPC: same=%v bound=%d calls=%d", sameEntry, entry.request.ResultBytes, capture.calls)
			}
			recoveryContext := t.Context()
			if kind == "EOF" {
				recoveryContext = bounded(8192)
			}
			result, barrier, err := mutate(recoveryContext, command)
			if err != nil || capture.response.Attr == nil || !reflect.DeepEqual(result, capture.response.Attr.Storage()) || !reflect.DeepEqual(barrier, capture.response.Barrier) || capture.calls != 2 || !capture.recovery || !reflect.DeepEqual(capture.replay, capture.request) {
				t.Fatalf("larger/default recovery did not preserve original request: result=%+v err=%v calls=%d recovery=%v original=%+v replay=%+v", result, err, capture.calls, capture.recovery, capture.request, capture.replay)
			}
		})
	}
}

func TestHTTPBasicRecoveryPreservesTypedResultAndFrozenIntent(t *testing.T) {
	meta, backend := memoryfixture.New(t, "basic-http-recovery", 1<<20, locking.DefaultOptions())
	for _, kind := range []string{"byte file", "metadata file", "directory"} {
		t.Run(kind, func(t *testing.T) {
			client := checkpointClient(t, authorizationHandler(t, backend, meta, nil))
			name := strings.ReplaceAll(kind, " ", "_")
			nodeKind := storage.NodeRegular
			if kind == "directory" {
				nodeKind = storage.NodeDirectory
				if err := backend.Mkdir(t.Context(), name); err != nil {
					t.Fatal(err)
				}
			} else if err := backend.Write(t.Context(), name, []byte("content")); err != nil {
				t.Fatal(err)
			}
			root, err := backend.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			node, err := backend.Stat(t.Context(), name)
			if err != nil {
				t.Fatal(err)
			}
			session := httpStateSession(t, client)
			var reference interface {
				Stat(context.Context) (storage.Attr, error)
				MutateFileWithBarrier(context.Context, storage.FileMutation) (storage.Attr, *MutationBarrier, error)
			}
			if kind == "byte file" {
				reference = httpStateFileNamed(t, session, root.ID, node.ID, storage.WriteMetadata, name).(*remoteFile)
			} else {
				opened, err := session.OpenNodeRef(t.Context(), node.ID, storage.NodeRefOptions{Kind: nodeKind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node.ID}, Action: httpFileAction(t, session), MetadataAccess: storage.WriteMetadata})
				if err != nil {
					t.Fatal(err)
				}
				reference = opened.Reference.(*remoteNodeReference)
				if _, exposed := opened.Reference.(storage.File); exposed {
					t.Fatal("metadata reference exposes content operations")
				}
			}
			if _, err := reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("public Stat permission=%v", err)
			}
			changed := time.Unix(123, 456).UTC()
			action := httpFileAction(t, session)
			command := storage.FileMutation{Action: action, Kind: storage.MutateAttributes, Attr: storage.AttrChange{ModTime: &changed}, ExpectedMetadata: map[string][]byte{"basic.flags": {}}, Metadata: map[string]storage.OpaquePayload{"basic.flags": {Data: []byte{1, 2}}}}
			originalTime := changed
			originalCommand := storage.FileMutation{Action: action, Kind: storage.MutateAttributes, Attr: storage.AttrChange{ModTime: &originalTime}, ExpectedMetadata: map[string][]byte{"basic.flags": {}}, Metadata: map[string]storage.OpaquePayload{"basic.flags": {Data: []byte{1, 2}}}}
			capture := loseInformationMutation(t, client, action)
			if result, barrier, err := reference.MutateFileWithBarrier(t.Context(), command); !errors.Is(err, syscall.EIO) || result.ID != 0 || barrier != nil {
				t.Fatalf("unknown result=%+v barrier=%+v err=%v", result, barrier, err)
			}
			changed = changed.Add(time.Second)
			command.Metadata["basic.flags"].Data[0] = 9
			command.ExpectedMetadata["basic.flags"] = []byte{9}
			if _, _, err := reference.MutateFileWithBarrier(t.Context(), command); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("changed action intent=%v", err)
			}
			later := originalTime.Add(time.Hour)
			if err := backend.SetAttr(t.Context(), name, storage.AttrChange{ModTime: &later}); err != nil {
				t.Fatal(err)
			}
			result, barrier, err := reference.MutateFileWithBarrier(t.Context(), originalCommand)
			if err != nil || capture.response.Attr == nil || barrier == nil || barrier.Incarnation == "" || barrier.Position == 0 || !reflect.DeepEqual(result, capture.response.Attr.Storage()) || !reflect.DeepEqual(barrier, capture.response.Barrier) || !result.ModTime.Equal(originalTime) || result.Kind != nodeKind {
				t.Fatalf("typed replay=%+v barrier=%+v original=%+v err=%v", result, barrier, capture.response, err)
			}
			if !bytes.Equal(result.Metadata["basic.flags"].Data, []byte{1, 2}) || capture.calls != 2 || !capture.recovery {
				t.Fatalf("metadata replay=%+v calls=%d recovery=%v", result.Metadata, capture.calls, capture.recovery)
			}
			fresh, err := backend.Stat(t.Context(), name)
			if err != nil || !fresh.ModTime.Equal(later) || !bytes.Equal(fresh.Metadata["basic.flags"].Version, result.Metadata["basic.flags"].Version) {
				t.Fatalf("replay repeated effects: current=%+v err=%v", fresh, err)
			}
		})
	}
}

func TestHTTPEOFRecoveryKeepsAtomicEffectsAndDoesNotBlockOtherActions(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	session, file := httpContentOpen(t, client, backend)
	siblingValue, err := session.OpenFile(t.Context(), "content", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}})
	if err != nil {
		t.Fatal(err)
	}
	sibling := siblingValue.(*remoteFile)
	oldSize := int64(3)
	action := httpFileAction(t, session)
	command := storage.FileMutation{Action: action, Kind: storage.MutateTruncate, Size: 8193, ExpectedSize: &oldSize, ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"content.test": {}}}
	savedSize := oldSize
	saved := storage.FileMutation{Action: action, Kind: storage.MutateTruncate, Size: 8193, ExpectedSize: &savedSize, ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"content.test": {}}}
	capture := loseInformationMutation(t, client, action)
	if result, _, err := file.MutateFileWithBarrier(t.Context(), command); !errors.Is(err, syscall.EIO) || result.ID != 0 {
		t.Fatalf("EOF unknown=%+v err=%v", result, err)
	}
	observed, err := file.ObserveMetadata(t.Context(), storage.MetadataObservationRequest{Purpose: storage.MetadataObservationContentEffect, EffectIndex: 0})
	if err != nil || observed.Value == nil || !bytes.Equal(observed.Value.Data, []byte{'T', 1}) {
		t.Fatalf("EOF and metadata did not commit together=%+v err=%v", observed, err)
	}
	read, err := sibling.ReadAt(t.Context(), 0, 8193)
	if err != nil || read.Attr.Size != 8193 || len(read.Data) != 8193 || !bytes.Equal(read.Data[:3], []byte("old")) || !bytes.Equal(read.Data[3:], make([]byte, 8190)) {
		t.Fatalf("sibling read blocked or expansion incorrect: size=%d bytes=%d err=%v", read.Attr.Size, len(read.Data), err)
	}
	if _, err := session.Renew(t.Context()); err != nil {
		t.Fatalf("renew blocked by unknown EOF=%v", err)
	}
	oldSize = 42
	command.ContentEffects[0] = 1
	command.ExpectedMetadata["content.test"] = []byte{9}
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("aliased EOF intent accepted=%v", err)
	}
	updatedSize := int64(8193)
	other := storage.FileMutation{Action: httpFileAction(t, session), Kind: storage.MutateTruncate, Size: 4, ExpectedSize: &updatedSize}
	if result, err := sibling.MutateFile(t.Context(), other); err != nil || result.Size != 4 {
		t.Fatalf("different action blocked=%+v err=%v", result, err)
	}
	if err := sibling.Close(t.Context()); err != nil {
		t.Fatalf("sibling close blocked=%v", err)
	}
	result, barrier, err := file.MutateFileWithBarrier(t.Context(), saved)
	if err != nil || result.Size != 8193 || capture.response.Attr == nil || !reflect.DeepEqual(result, capture.response.Attr.Storage()) || !reflect.DeepEqual(barrier, capture.response.Barrier) || capture.calls != 2 || !capture.recovery {
		t.Fatalf("original EOF replay=%+v barrier=%+v calls=%d recovery=%v err=%v", result, barrier, capture.calls, capture.recovery, err)
	}
	read, err = file.ReadAt(t.Context(), 0, 8)
	if err != nil || read.Attr.Size != 4 || len(read.Data) != 4 {
		t.Fatalf("EOF replay reapplied old truncation=%+v err=%v", read, err)
	}
	current, err := file.ObserveMetadata(t.Context(), storage.MetadataObservationRequest{Purpose: storage.MetadataObservationContentEffect, EffectIndex: 0})
	if err != nil || current.Value == nil || !bytes.Equal(current.Value.Version, observed.Value.Version) {
		t.Fatalf("EOF replay repeated metadata effect=%+v err=%v", current, err)
	}
}

func TestHTTPInformationMutationCancelledCloseRetainsPendingIntent(t *testing.T) {
	_, backend := memoryfixture.New(t, "information-cancel-http", 1<<20, locking.DefaultOptions())
	for _, kind := range []storage.FileMutationKind{storage.MutateAttributes, storage.MutateTruncate} {
		name := map[storage.FileMutationKind]string{storage.MutateAttributes: "basic", storage.MutateTruncate: "EOF"}[kind]
		t.Run(name, func(t *testing.T) {
			client, _ := retainedHTTPPair(t, DefaultFileLimits(), backend)
			session, file := httpContentOpenNamed(t, client, backend, name)
			var command storage.FileMutation
			var closeCall func(context.Context) (storage.ReferenceCloseResult, error)
			var mutate func(context.Context, storage.FileMutation) (storage.Attr, error)
			var referenceID string
			if kind == storage.MutateAttributes {
				opened, err := session.OpenNodeRef(t.Context(), file.node, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: file.node}, Action: httpFileAction(t, session), MetadataAccess: storage.WriteMetadata})
				if err != nil {
					t.Fatal(err)
				}
				reference := opened.Reference.(*remoteNodeReference)
				changed := time.Unix(123, 456).UTC()
				command = storage.FileMutation{Action: httpFileAction(t, session), Kind: kind, Attr: storage.AttrChange{ModTime: &changed}, Metadata: map[string]storage.OpaquePayload{"basic.flags": {Data: []byte{1, 2}}}}
				closeCall, mutate, referenceID = reference.CloseWithResult, reference.MutateFile, reference.file.id
			} else {
				command = storage.FileMutation{Action: httpFileAction(t, session), Kind: kind, Size: 8193, ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"content.test": {}}}
				closeCall, mutate, referenceID = file.CloseWithResult, file.MutateFile, file.id
			}
			original := client.http.Transport
			completed, release := make(chan struct{}), make(chan struct{})
			var completedOnce, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			var closeCalls, mutationCalls atomic.Int32
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
				if req.Op == storage.OpFileClose {
					closeCalls.Add(1)
				}
				if req.Op != storage.OpFileMutate || err != nil {
					return response, err
				}
				mutationCalls.Add(1)
				completedOnce.Do(func() { close(completed) })
				<-release
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				return nil, errors.New("lost completed information mutation response")
			})
			t.Cleanup(func() { unblock(); client.http.Transport = original })
			done := make(chan error, 1)
			go func() { _, err := mutate(t.Context(), command); done <- err }()
			select {
			case <-completed:
			case <-time.After(time.Second):
				t.Fatal("mutation did not reach authority")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			result, err := closeCall(ctx)
			cancel()
			var pending *storage.CloseSettlementError
			if !result.Released || !result.Determined || !errors.As(err, &pending) || pending.State != storage.CloseSettlementPending || !errors.Is(err, context.DeadlineExceeded) {
				unblock()
				t.Fatalf("cancelled close=%+v err=%v", result, err)
			}
			session.mu.Lock()
			entry := session.explicitWrites[referenceID+":"+string(command.Action)]
			expected, freezeErr := freezeFileRequest(fileRequest{Mutation: fileMutationOf(command)})
			retained := freezeErr == nil && entry != nil && entry.detached && reflect.DeepEqual(entry.request.Mutation, expected.Mutation)
			session.mu.Unlock()
			if !retained {
				unblock()
				t.Fatal("cancelled close dropped exact Basic/EOF intent")
			}
			unblock()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("mutation did not return")
			}
			session.mu.Lock()
			retained = session.explicitWrites[referenceID+":"+string(command.Action)] == entry
			session.mu.Unlock()
			if !retained {
				t.Fatal("late response discarded fenced ownership")
			}
			result, err = closeCall(t.Context())
			if err != nil || !result.Released || closeCalls.Load() != 1 || mutationCalls.Load() != 1 {
				t.Fatalf("local settlement repeated effects=%+v err=%v closeRPC=%d mutationRPC=%d", result, err, closeCalls.Load(), mutationCalls.Load())
			}
			session.mu.Lock()
			remaining := session.explicitWrites[referenceID+":"+string(command.Action)]
			session.mu.Unlock()
			if remaining != nil {
				t.Fatal("settled close retained owned intent")
			}
		})
	}
}
