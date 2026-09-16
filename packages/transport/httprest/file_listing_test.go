package httprest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func TestFileServerListsExactNamesAndNativeMetadata(t *testing.T) {
	meta, backend := memoryfixture.New(t, "generic-http-list", 1<<20, locking.DefaultOptions())
	handler, err := NewHandlerWithOptions(backend, meta, DefaultHandlerOptions())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		handler.Stop()
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, status, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	action := func() storage.FileActionID {
		id, e := storage.NewFileActionID(status.ActionEpoch)
		if e != nil {
			t.Fatal(e)
		}
		return id
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: root.ID}, action())
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.Reference(t.Context(), receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.DirectoryPageRequest{MaxEntries: 16, MaxBytes: 1 << 20}
	empty, err := file.ListAt(t.Context(), request)
	if err != nil || len(empty.Entries) != 0 || !empty.Done {
		t.Fatalf("empty page=%+v %v", empty, err)
	}
	for _, name := range []string{"z", "&", "目录", "CON", "README", "readme", string([]byte{255})} {
		if err := backend.Write(t.Context(), name, []byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	native, ns, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewFileActionID(ns.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := native.Retain(t.Context(), storage.RetainRequest{NodeID: root.ID}, id)
	if err != nil {
		t.Fatal(err)
	}
	nf, err := native.Reference(t.Context(), retained.Reference)
	if err != nil {
		t.Fatal(err)
	}
	want, err := nf.ListAt(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	got, err := file.ListAt(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if got.ParentID != want.ParentID || got.Revision != want.Revision || got.Done != want.Done || got.Next.ParentID != want.Next.ParentID || got.Next.Revision != want.Next.Revision || !bytes.Equal(got.Next.After, want.Next.After) || len(got.Entries) != len(want.Entries) {
		t.Fatalf("directory page identity/version/cursor changed: %+v != %+v", got, want)
	}
	for i := range want.Entries {
		a, b := got.Entries[i], want.Entries[i]
		if a.EntryID != b.EntryID || !bytes.Equal(a.Name, b.Name) || !reflect.DeepEqual(a.Attr.Clone(), b.Attr.Clone()) {
			t.Fatalf("directory entry %d changed: %+v != %+v", i, a, b)
		}
	}
	if len(got.Entries) != 8 || string(got.Entries[0].Name) != "&" || string(got.Entries[3].Name) != "directory" || got.Entries[3].Attr.Kind != storage.NodeDirectory {
		t.Fatalf("names or kinds changed: %+v", got.Entries)
	}
	closeID, _ := storage.NewFileActionID(ns.ActionEpoch)
	if _, err := native.Close(t.Context(), closeID); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Close(t.Context(), action()); err != nil {
		t.Fatal(err)
	}
}

func TestFileClientDirectoryPageRefusesIncompleteOrUnrelatedViews(t *testing.T) {
	attr := protocolAttr()
	attr.ID = 8
	page := storage.DirectoryPage{ParentID: 7, Revision: 2, Entries: []storage.DirectoryEntry{{EntryID: 4, Name: []byte("file"), Attr: attr}}, Done: true}
	for name, alter := range map[string]func(*storage.DirectoryPage){
		"missing revision":        func(p *storage.DirectoryPage) { p.Revision = 0 },
		"wrong revision":          func(p *storage.DirectoryPage) { p.Revision = 3 },
		"duplicate identity":      func(p *storage.DirectoryPage) { p.Entries = append(p.Entries, p.Entries[0]) },
		"incomplete continuation": func(p *storage.DirectoryPage) { p.Done = false },
		"extended done": func(p *storage.DirectoryPage) {
			p.Next = storage.DirectoryCursor{ParentID: 7, Revision: 2, After: []byte("file")}
		},
		"past requested count": func(p *storage.DirectoryPage) {
			other := p.Entries[0]
			other.EntryID = 5
			other.Name = []byte("next")
			other.Attr.ID = 9
			p.Entries = append(p.Entries, other)
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := page
			value.Entries = append([]storage.DirectoryEntry(nil), page.Entries...)
			alter(&value)
			client := protocolClient(t, func(fileRequest) (*http.Response, error) {
				wire, err := fileDirectoryPageOf(value)
				if err != nil {
					t.Fatal(err)
				}
				return protocolResponse(t, 200, fileResponse{Page: wire}), nil
			})
			got, err := protocolFile(client).ListAt(t.Context(), storage.DirectoryPageRequest{Revision: 2, MaxEntries: 1, MaxBytes: 4096})
			if !errors.Is(err, syscall.EIO) || len(got.Entries) != 0 {
				t.Fatalf("invalid page exposed entries: %+v %v", got, err)
			}
		})
	}
}

func TestFileClientDirectoryResponseAdmissionCoversDecode(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	attr := protocolAttr()
	wire, err := fileDirectoryPageOf(storage.DirectoryPage{ParentID: 7, Revision: 2, Entries: []storage.DirectoryEntry{{EntryID: 3, Name: []byte("file"), Attr: func() storage.Attr { v := attr; v.ID = 8; return v }()}}, Done: true})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	client := protocolClient(t, func(fileRequest) (*http.Response, error) {
		n := calls.Add(1)
		response := protocolResponse(t, 200, fileResponse{Page: wire})
		if n == 1 {
			response.Body = &protocolBlockingBody{ReadCloser: response.Body, started: started, release: release}
		}
		return response, nil
	})
	client.fileRequests = newBodyAdmission(1, retainedResponseMultiplier*client.maxBodyBytes, 0)
	request := storage.DirectoryPageRequest{MaxEntries: 1, MaxBytes: 4096}
	done := make(chan error, 1)
	go func() { _, err := protocolFile(client).ListAt(t.Context(), request); done <- err }()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("listing ended before body: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("listing did not enter body")
	}
	if _, err := protocolFile(client).ListAt(t.Context(), request); !errors.Is(err, syscall.EAGAIN) || !storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("occupied decoder admission=%v", err)
	}
	unblock.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listing did not release")
	}
	if _, err := protocolFile(client).ListAt(t.Context(), request); err != nil {
		t.Fatalf("released admission unavailable: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("refused call dispatched or reuse skipped: %d", calls.Load())
	}
}

type protocolBlockingBody struct {
	io.ReadCloser
	started, release chan struct{}
	once             sync.Once
}

func (b *protocolBlockingBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started); <-b.release })
	return b.ReadCloser.Read(p)
}

func TestFileClientHistoricalLinkFactsKeepExactBytes(t *testing.T) {
	attr := protocolAttr()
	attr.Kind = storage.NodeSymlink
	target := []byte("../目录/target")
	attr.Size = int64(len(target))
	location := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 7, Ancestors: []storage.EntryCondition{{ParentID: 1, DirectoryRevision: 2, EntryID: 4, NodeID: 7, Name: []byte{255, 'x'}}}}
	observation, err := fileObservationOf(storage.FileObservation{Attr: attr, Location: &location, LinkTarget: target})
	if err != nil {
		t.Fatal(err)
	}
	id := protocolID(t)
	client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
		r := protocolReceipt(req)
		r.Operation = storage.OpFileRetain
		r.Effects = storage.EffectRetained
		r.Reference = 11
		r.Observation = observation
		return protocolResponse(t, 200, fileResponse{Receipt: r}), nil
	})
	got, err := protocolSession(client).QueryAction(t.Context(), id)
	if err != nil || !reflect.DeepEqual(got.Observation.Location, &location) || string(got.Observation.LinkTarget) != string(target) || got.Observation.Attr.CreationTime != nil {
		t.Fatalf("historical facts=%+v %v", got, err)
	}
}

func TestFileClientDirectoryPageEnforcesExactByteBudget(t *testing.T) {
	first := protocolAttr()
	first.ID = 8
	second := first.Clone()
	second.ID = 9
	page := storage.DirectoryPage{ParentID: 7, Revision: 2, Entries: []storage.DirectoryEntry{{EntryID: 3, Name: []byte("a"), Attr: first}, {EntryID: 4, Name: []byte("bb"), Attr: second}}, Done: true}
	budget := storage.DirectoryPageBaseBytes
	for _, entry := range page.Entries {
		metadataBytes, err := entry.Attr.Metadata.EncodedSize()
		if err != nil {
			t.Fatal(err)
		}
		charge, err := storage.DirectoryEntryBytes(len(entry.Name), metadataBytes)
		if err != nil {
			t.Fatal(err)
		}
		budget += charge
	}
	wire, err := fileDirectoryPageOf(page)
	if err != nil {
		t.Fatal(err)
	}
	client := protocolClient(t, func(fileRequest) (*http.Response, error) {
		return protocolResponse(t, 200, fileResponse{Page: wire}), nil
	})
	got, err := protocolFile(client).ListAt(t.Context(), storage.DirectoryPageRequest{Revision: 2, MaxEntries: 2, MaxBytes: budget})
	if err != nil || len(got.Entries) != 2 || string(got.Entries[0].Name) != "a" || string(got.Entries[1].Name) != "bb" {
		t.Fatalf("exact byte budget=%+v %v", got, err)
	}
	got, err = protocolFile(client).ListAt(t.Context(), storage.DirectoryPageRequest{Revision: 2, MaxEntries: 2, MaxBytes: budget - 1})
	if !errors.Is(err, syscall.EIO) || len(got.Entries) != 0 {
		t.Fatalf("short byte budget exposed prefix: %+v %v", got, err)
	}
}
