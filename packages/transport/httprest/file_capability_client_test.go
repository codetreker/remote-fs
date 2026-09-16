package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func TestCapabilityOpenACKCancellationPreservesEffectsAndCleanup(t *testing.T) {
	for _, test := range []struct {
		name                            string
		reference, create, cleanupFails bool
		want                            syscall.Errno
	}{
		{"plain file", false, false, false, syscall.EINTR},
		{"created file", false, true, false, syscall.EIO},
		{"plain reference", true, false, false, syscall.EINTR},
		{"reference cleanup failure", true, false, true, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			kind := storage.NodeRegular
			if test.reference {
				kind = storage.NodeDirectory
			}
			node := &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: kind}}
			if test.cleanupFails {
				node.closeError = syscall.EIO
			}
			native := &capabilityTestSession{node: node}
			client, handler := capabilityHTTPFixture(t, native)
			opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			next := client.http.Transport
			client.http.Transport = fileRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				response, err := next.RoundTrip(req)
				if err != nil {
					return response, err
				}
				body, err := req.GetBody()
				if err != nil {
					return nil, err
				}
				var call fileRequest
				decodeErr := json.NewDecoder(body).Decode(&call)
				_ = body.Close()
				if decodeErr != nil {
					return nil, decodeErr
				}
				if call.Op == storage.OpFileOpenAt || call.Op == storage.OpFileOpenNodeRef {
					data, readErr := io.ReadAll(response.Body)
					_ = response.Body.Close()
					if readErr != nil {
						return nil, readErr
					}
					response.Body = io.NopCloser(bytes.NewReader(data))
					cancel()
				}
				return response, nil
			})
			if test.reference {
				result, e := opened.(storage.NodeReferences).OpenNodeRef(ctx, 41, storage.NodeRefOptions{Kind: kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 41}, MetadataAccess: storage.ReadMetadata})
				err = e
				if result.Reference != nil {
					t.Fatal("unacknowledged reference escaped")
				}
			} else {
				result, e := opened.(storage.AtomicFileOpener).OpenAt(ctx, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}, storage.OpenAtOptions{Read: true, Create: test.create, Existing: storage.Keep, Target: storage.ChildCondition{State: storage.Any}, Use: storage.UseClaim{Uses: storage.ReadData}})
				err = e
				if result.File != nil {
					t.Fatal("unacknowledged file escaped")
				}
			}
			if storage.ErrnoOf(err) != test.want || !errors.Is(err, context.Canceled) {
				t.Fatalf("ACK cancellation: %v;want %v", err, test.want)
			}
			node.mu.Lock()
			closes := node.closes
			closed := node.closed
			node.mu.Unlock()
			if closes != 1 || closed == test.cleanupFails {
				t.Fatalf("cleanup attempts=%d,closed=%v", closes, closed)
			}
			if test.cleanupFails {
				handler.files.mu.Lock()
				for _, session := range handler.files.sessions {
					session.mu.Lock()
					if len(session.files) != 1 {
						t.Error("failed cleanup lost retained registry entry")
					}
					session.mu.Unlock()
				}
				handler.files.mu.Unlock()
			}
		})
	}
}

func TestPartialOpenErrorKeepsItsFailureClassOnTheWire(t *testing.T) {
	native := &capabilityTestSession{openError: context.Canceled, node: &capabilityTestReference{attr: storage.Attr{ID: 41, Kind: storage.NodeRegular}}}
	client, _ := capabilityHTTPFixture(t, native)
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	_, err = session.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("created")}, storage.OpenAtOptions{Read: true, Create: true, Existing: storage.Keep, Target: storage.ChildCondition{State: storage.Absent}, Use: storage.UseClaim{Uses: storage.ReadData}})
	if storage.ErrnoOf(err) != syscall.EIO || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("partial result lost wire failure class: %v", err)
	}
}

func TestReferenceBarrierMethodsAndBoundedDirectoryUseAuthorityResults(t *testing.T) {
	meta, backend := memoryfixture.New(t, "reference-barriers", 1<<20, locking.DefaultOptions())
	if err := backend.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, meta)
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
	opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*remoteFileSession)
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	directory, err := backend.Stat(t.Context(), "directory")
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.OpenNodeRef(t.Context(), directory.ID, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID}, MetadataAccess: storage.ReadMetadata | storage.WriteMetadata, Use: storage.UseClaim{Uses: storage.DeleteName}})
	if err != nil {
		t.Fatal(err)
	}
	ref := result.Reference.(*remoteNodeReference)
	checkBarrier := func(barrier *MutationBarrier) {
		t.Helper()
		current, err := meta.Barrier(t.Context(), MaxIncarnationBytes)
		if err != nil || barrier == nil || barrier.Incarnation != string(current.Incarnation) || barrier.Position != int64(current.Position) {
			t.Fatalf("barrier=%+v,current=%+v,err=%v", barrier, current, err)
		}
	}
	stamp := time.Unix(1700000000, 123)
	attr, barrier, err := ref.SetAttrWithBarrier(t.Context(), storage.AttrChange{ModTime: &stamp})
	if err != nil || !attr.ModTime.Equal(stamp) || attr.ID != directory.ID {
		t.Fatalf("attribute mutation=%+v,%v", attr, err)
	}
	checkBarrier(barrier)
	payload, barrier, err := ref.SetMetadataWithBarrier(t.Context(), "test.barrier", nil, []byte{0xff, 0, 1})
	if err != nil || !bytes.Equal(payload.Data, []byte{0xff, 0, 1}) || len(payload.Version) == 0 {
		t.Fatalf("metadata mutation=%+v,%v", payload, err)
	}
	checkBarrier(barrier)
	state, barrier, err := ref.SetPendingUnlinkWithBarrier(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkIfEmpty})
	if err != nil || !state.PendingUnlink {
		t.Fatalf("pending unlink=%+v,%v", state, err)
	}
	checkBarrier(barrier)
	state, barrier, err = ref.ClearPendingUnlinkWithBarrier(t.Context(), storage.ClearPendingUnlinkCommand{Generation: state.PendingGeneration})
	if err != nil || state.PendingUnlink {
		t.Fatalf("clear pending=%+v,%v", state, err)
	}
	checkBarrier(barrier)
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	target := storage.DirectoryTarget{NodeID: root.ID}
	bounded, err := newListResult(16 << 10)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := session.ReadDirNodeBounded(t.Context(), target, bounded)
	if err != nil || observation.ParentID != root.ID || len(observation.Revision) == 0 {
		t.Fatalf("directory observation=%+v,%v", observation, err)
	}
	entries, err := bounded.Entries()
	if err != nil || len(entries) != 1 || entries[0].Name != "directory" || entries[0].Attr.ID != directory.ID {
		t.Fatalf("bounded directory=%+v,%v", entries, err)
	}
	tiny, err := storage.NewListResult(0, 0, func(int, int64, int64, storage.Attr) (int64, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.ReadDirNodeBounded(t.Context(), target, tiny); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("zero budget: %v", err)
	}
	if entries, err := tiny.Entries(); err == nil || entries != nil {
		t.Fatalf("failed budget retained entries=%+v,%v", entries, err)
	}
	if _, err := session.ReadDirNodeBounded(t.Context(), target, nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("missing result: %v", err)
	}
	if err := ref.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ref.SetAttrWithBarrier(t.Context(), storage.AttrChange{}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed attribute mutation: %v", err)
	}
}
