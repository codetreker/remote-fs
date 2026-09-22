//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type nativeFileIOFixture struct {
	connection *connection
	tree       *tree
	volume     *objectstore.Storage
	file       storage.File
	id         wire.FileID
}

func newNativeFileIOFixture(t *testing.T, throughHTTP bool) nativeFileIOFixture {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "files.db"), Volume: "smb-io", Allowance: 1 << 20,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(memory.New(), meta)
	var backend storage.FileStorage = volume
	var handler *httprest.Handler
	var server *httptest.Server
	if throughHTTP {
		handler, err = httprest.NewHandler(volume, nil)
		if err != nil {
			t.Fatal(err)
		}
		server = httptest.NewServer(handler)
		backend, err = httprest.Dial(server.URL, server.Client())
		if err != nil {
			t.Fatal(err)
		}
	}
	session, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosHidden})
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenFile(t.Context(), "item", storage.FileOpenOptions{
		OpenAccess:      storage.OpenAccess{Read: true, Write: true, Create: true, Exclusive: true},
		InitialMetadata: map[string][]byte{windowsMetadataKey: metadata},
	})
	if err != nil {
		t.Fatal(err)
	}
	attr, err := file.WriteAt(t.Context(), 0, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	registry := newHandleTestRegistry(t, 2)
	registry.tree.export.share = endpointShare("native", "smb-io", backend)
	registry.tree.authority.raw = session
	registry.tree.authority.actionEpoch = status.ActionEpoch
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	reservation.attachFile(storage.OpenResult{File: file, Attr: attr, Outcome: storage.Opened})
	id, err := reservation.install(fileReadData|fileWriteData|fileAppendData|fileReadAttributes, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := registry.close(ctx); err != nil {
			t.Errorf("close native SMB handles: %v", err)
		}
		if err := session.Close(ctx); err != nil {
			t.Errorf("close native file session: %v", err)
		}
		if server != nil {
			server.Close()
		}
		if handler != nil {
			if err := handler.Close(ctx); err != nil {
				t.Errorf("close HTTP handler: %v", err)
			}
		}
		if err := volume.Close(); err != nil {
			t.Errorf("close native volume: %v", err)
		}
	})
	return nativeFileIOFixture{connection: &connection{server: registry.tree.export.server}, tree: registry.tree, volume: volume, file: file, id: id}
}

func (f nativeFileIOFixture) read(t *testing.T, want string) {
	t.Helper()
	body, status := f.connection.readHandle(t.Context(), f.tree, fileIOReadRequest(f.id, 0, 128, 0))
	if status != statusOK || len(body) < 16 || string(body[16:]) != want {
		t.Fatalf("READ = %x, %#x; want %q", body, status, want)
	}
}

func TestNativeAndHTTPFileIOPublishAtomicWindowsState(t *testing.T) {
	for _, throughHTTP := range []bool{false, true} {
		name := "direct"
		if throughHTTP {
			name = "HTTP"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newNativeFileIOFixture(t, throughHTTP)
			body, status := fixture.connection.writeHandle(t.Context(), fixture.tree, fileIOWriteRequest(fixture.id, 1, []byte("Z")))
			if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != 1 {
				t.Fatalf("range WRITE = %x, %#x", body, status)
			}
			fixture.read(t, "aZc")
			beforeAppend, err := fixture.file.Stat(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			windows, err := decodeWindowsMetadata(beforeAppend.Metadata)
			if err != nil || windows.Attributes != dosHidden|dosArchive || beforeAppend.ChangeTime == nil {
				t.Fatalf("range mutation state = %+v, %+v, %v", beforeAppend, windows, err)
			}

			if _, err := fixture.file.WriteAt(t.Context(), beforeAppend.Size, []byte("!")); err != nil {
				t.Fatal(err)
			}
			body, status = fixture.connection.writeHandle(t.Context(), fixture.tree, fileIOWriteRequest(fixture.id, ^uint64(0), []byte("+")))
			if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != 1 {
				t.Fatalf("append WRITE = %x, %#x", body, status)
			}
			fixture.read(t, "aZc!+")
			if body, status = fixture.connection.flushHandle(t.Context(), fixture.tree, fileIOFlushRequest(fixture.id)); status != statusOK || !bytes.Equal(body, wire.EmptyResponseBody()) {
				t.Fatalf("FLUSH = %x, %#x", body, status)
			}
			body, status = fixture.connection.queryInfo(t.Context(), fixture.tree, queryInfoRequest(fixture.id, 1, 34, 56))
			if status != statusOK || binary.LittleEndian.Uint64(queryInfoData(t, body, 56)[40:]) != 5 {
				t.Fatalf("QUERY_INFO = %x, %#x", body, status)
			}
		})
	}
}
