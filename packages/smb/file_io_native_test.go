//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
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

type nativeIOFixture struct {
	connection *connection
	tree       *tree
	volume     *objectstore.Storage
	session    storage.FileSession
	file       storage.File
	id         wire.FileID
	nodeID     uint64
}

func newNativeIOFixture(t *testing.T, access uint32, content []byte, metadata map[string][]byte) nativeIOFixture {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "files.db"), Volume: "native-io", Allowance: 4096,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("close native volume: %v", err)
		}
	})
	session, err := volume.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close native session: %v", err)
		}
	})
	file, err := session.OpenFile(t.Context(), "item", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true, Exclusive: true}, InitialMetadata: metadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	attr, err := file.WriteAt(t.Context(), 0, content)
	if err != nil {
		t.Fatal(err)
	}
	registry := handleTestRegistry()
	registry.tree.export.share = Share{Name: "native", Volume: "native-io", Backend: volume}
	registry.tree.authority.raw = session
	t.Cleanup(func() {
		if err := registry.close(context.Background()); err != nil {
			t.Errorf("close native handle registry: %v", err)
		}
	})
	reservation := handleTestReserve(t, registry)
	reservation.attachFile(storage.OpenResult{File: file, Attr: attr, Outcome: storage.Opened})
	id, err := reservation.install(access, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	c := newConnection(registry.tree.export.server, &capturedConnection{})
	t.Cleanup(c.cancel)
	return nativeIOFixture{connection: c, tree: registry.tree, volume: volume, session: session, file: file, id: id, nodeID: attr.ID}
}

func nativeIORequest(command uint16, body []byte) wire.Request {
	header := wire.Header{Command: command, Credits: 1}
	packet := requestPacket(header, body)
	return wire.Request{Header: header, Packet: packet, Body: packet[wire.HeaderSize:]}
}

func nativeIOReadRequest(id wire.FileID, offset uint64, length uint32) wire.Request {
	body := make([]byte, 48)
	binary.LittleEndian.PutUint16(body, 49)
	binary.LittleEndian.PutUint32(body[4:], length)
	binary.LittleEndian.PutUint64(body[8:], offset)
	copy(body[16:], id[:])
	return nativeIORequest(wire.Read, body)
}

func nativeIOWriteRequest(id wire.FileID, offset uint64, data []byte) wire.Request {
	body := make([]byte, 48+len(data))
	binary.LittleEndian.PutUint16(body, 49)
	if len(data) != 0 {
		binary.LittleEndian.PutUint16(body[2:], 112)
	}
	binary.LittleEndian.PutUint32(body[4:], uint32(len(data)))
	binary.LittleEndian.PutUint64(body[8:], offset)
	copy(body[16:], id[:])
	copy(body[48:], data)
	return nativeIORequest(wire.Write, body)
}

func nativeIOFlushRequest(id wire.FileID) wire.Request {
	body := make([]byte, 24)
	binary.LittleEndian.PutUint16(body, 24)
	copy(body[8:], id[:])
	return nativeIORequest(wire.Flush, body)
}

func (f nativeIOFixture) read(t *testing.T, offset uint64, length uint32, want string) {
	t.Helper()
	body, status := f.connection.readHandle(t.Context(), f.tree, nativeIOReadRequest(f.id, offset, length))
	if status != statusOK || len(body) < 16 || binary.LittleEndian.Uint16(body) != 17 || body[2] != 80 ||
		binary.LittleEndian.Uint32(body[4:]) != uint32(len(want)) || binary.LittleEndian.Uint32(body[8:]) != 0 || string(body[16:]) != want {
		t.Fatalf("native READ at %d: status %#x, body %x; want %q", offset, status, body, want)
	}
}

func (f nativeIOFixture) write(t *testing.T, offset uint64, data string) {
	t.Helper()
	body, status := f.connection.writeHandle(t.Context(), f.tree, nativeIOWriteRequest(f.id, offset, []byte(data)))
	if status != statusOK || len(body) != 16 || binary.LittleEndian.Uint16(body) != 17 ||
		binary.LittleEndian.Uint32(body[4:]) != uint32(len(data)) || binary.LittleEndian.Uint32(body[8:]) != 0 {
		t.Fatalf("native WRITE at %d: status %#x, body %x; want Count %d", offset, status, body, len(data))
	}
}

func (f nativeIOFixture) retainedAttr(t *testing.T) storage.Attr {
	t.Helper()
	attr, err := f.file.Stat(t.Context())
	if err != nil || attr.ID != f.nodeID {
		t.Fatalf("retained identity = %d, %v; want %d", attr.ID, err, f.nodeID)
	}
	return attr
}

func TestNativeFileCommandsRetainIdentityAndReadCurrentEOF(t *testing.T) {
	f := newNativeIOFixture(t, fileReadData|fileWriteData, []byte("initial"), nil)
	for _, content := range []string{"a current and longer value", "short"} {
		if err := f.volume.Write(t.Context(), "item", []byte(content)); err != nil {
			t.Fatal(err)
		}
		f.read(t, 0, 128, content)
		body, status := f.connection.readHandle(t.Context(), f.tree, nativeIOReadRequest(f.id, uint64(len(content)), 1))
		if status != statusEndOfFile || len(body) != 0 {
			t.Fatalf("current EOF: status %#x, body %x", status, body)
		}
	}
	if err := f.volume.Rename(t.Context(), "item", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := f.volume.Write(t.Context(), "item", []byte("new name owner")); err != nil {
		t.Fatal(err)
	}
	f.write(t, 0, "SHORT")
	f.read(t, 0, 128, "SHORT")
	if err := f.volume.Rename(t.Context(), "item", "moved"); err != nil {
		t.Fatal(err)
	}
	f.write(t, 5, " detached")
	f.read(t, 0, 128, "SHORT detached")
	if data, err := f.volume.Read(t.Context(), "moved"); err != nil || string(data) != "new name owner" {
		t.Fatalf("replacement data = %q, %v", data, err)
	}
	if attr := f.retainedAttr(t); attr.Size != int64(len("SHORT detached")) {
		t.Fatalf("detached EOF = %d", attr.Size)
	}
	body, status := f.connection.flushHandle(t.Context(), f.tree, nativeIOFlushRequest(f.id))
	if status != statusOK || !bytes.Equal(body, wire.EmptyResponseBody()) {
		t.Fatalf("detached FLUSH: status %#x, body %x", status, body)
	}
}

func TestNativeFileWritesPreserveOtherCurrentRanges(t *testing.T) {
	f := newNativeIOFixture(t, fileReadData|fileWriteData, []byte("000000"), nil)
	other, err := f.session.OpenFile(t.Context(), "item", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := other.Close(context.Background()); err != nil {
			t.Errorf("close independent writer: %v", err)
		}
	})
	if _, err := other.WriteAt(t.Context(), 3, []byte("XYZ")); err != nil {
		t.Fatal(err)
	}
	f.write(t, 0, "abc")
	f.read(t, 0, 64, "abcXYZ")
	if _, err := other.WriteAt(t.Context(), 0, []byte("ABC")); err != nil {
		t.Fatal(err)
	}
	f.write(t, 3, "xyz")
	f.read(t, 0, 64, "ABCxyz")
	if attr := f.retainedAttr(t); attr.Size != 6 {
		t.Fatalf("nonoverlapping writes changed EOF to %d", attr.Size)
	}
}

func TestNativeFileAppendUsesCurrentEOF(t *testing.T) {
	for _, access := range []uint32{fileReadData | fileAppendData, fileReadData | fileWriteData} {
		name := "append-only ignores supplied offset"
		offset := uint64(0)
		if access&fileWriteData != 0 {
			name = "write-data negative offset appends"
			offset = ^uint64(0)
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeIOFixture(t, access, []byte("old"), nil)
			if err := f.volume.Write(t.Context(), "item", []byte("external growth")); err != nil {
				t.Fatal(err)
			}
			f.write(t, offset, "+one")
			f.read(t, 0, 128, "external growth+one")
			if err := f.volume.Write(t.Context(), "item", []byte("x")); err != nil {
				t.Fatal(err)
			}
			f.write(t, offset, "+two")
			f.read(t, 0, 128, "x+two")
			if attr := f.retainedAttr(t); attr.Size != 5 {
				t.Fatalf("append EOF = %d, want 5", attr.Size)
			}
		})
	}
}

func TestNativeFileWritesPreserveAbsentOrClearArchive(t *testing.T) {
	for _, test := range []struct {
		name       string
		present    bool
		attributes uint32
	}{
		{name: "absent namespace"},
		{name: "clear archive", present: true, attributes: dosHidden},
		{name: "readonly retained writable handle", present: true, attributes: dosReadOnly},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := map[string][]byte{"other.client": []byte("opaque value")}
			if test.present {
				data, err := encodeWindowsMetadata(windowsMetadata{Attributes: test.attributes})
				if err != nil {
					t.Fatal(err)
				}
				metadata[windowsMetadataKey] = data
			}
			f := newNativeIOFixture(t, fileReadData|fileWriteData, []byte("before"), metadata)
			before := f.retainedAttr(t)
			f.write(t, 0, "AFTER!")
			f.read(t, 0, 64, "AFTER!")
			after := f.retainedAttr(t)
			if !reflect.DeepEqual(after.Metadata, before.Metadata) {
				t.Fatalf("byte write changed opaque metadata: before %+v, after %+v", before.Metadata, after.Metadata)
			}
			windows, err := decodeWindowsMetadata(after.Metadata)
			if err != nil || windows.Attributes != test.attributes || windows.Attributes&dosArchive != 0 {
				t.Fatalf("byte write changed DOS attributes: %+v, %v", windows, err)
			}
			f.write(t, 3, "")
			zero := f.retainedAttr(t)
			if !reflect.DeepEqual(zero, after) {
				t.Fatalf("zero WRITE changed retained attributes: before %+v, after %+v", after, zero)
			}
			f.read(t, 0, 64, "AFTER!")
		})
	}
}

func TestNativeFileWriteQuotaFailureHasNoSuccessfulCount(t *testing.T) {
	for _, access := range []uint32{fileReadData | fileWriteData, fileReadData | fileAppendData} {
		name := "ordinary write"
		if access&fileAppendData != 0 {
			name = "append"
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeIOFixture(t, access, []byte("kept"), nil)
			before := f.retainedAttr(t)
			spaceBefore, err := f.volume.Space(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			body, status := f.connection.writeHandle(t.Context(), f.tree, nativeIOWriteRequest(f.id, 0, bytes.Repeat([]byte{'x'}, 4097)))
			if status != 0xc0000802 || len(body) != 0 {
				t.Fatalf("over-quota WRITE: status %#x, body %x; want DISK_QUOTA_EXCEEDED without Count", status, body)
			}
			f.read(t, 0, 64, "kept")
			if after := f.retainedAttr(t); !reflect.DeepEqual(after, before) {
				t.Fatalf("quota refusal changed retained attributes: before %+v, after %+v", before, after)
			}
			spaceAfter, err := f.volume.Space(t.Context())
			if err != nil || spaceAfter != spaceBefore {
				t.Fatalf("quota refusal changed accounting: before %+v, after %+v, %v", spaceBefore, spaceAfter, err)
			}
			f.write(t, 0, "OK")
			want := "OKpt"
			if access&fileAppendData != 0 {
				want = "keptOK"
			}
			f.read(t, 0, 64, want)
		})
	}
}

func newNativeIOHTTPFixture(t *testing.T, metadata map[string][]byte, intercept func(http.Handler) http.Handler) (nativeIOFixture, *atomic.Int64) {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "files.db"), Volume: "http-io", Allowance: 1 << 20,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("close HTTP fixture volume: %v", err)
		}
	})
	handler, err := httprest.NewHandler(volume, nil)
	if err != nil {
		t.Fatal(err)
	}
	var served http.Handler = handler
	if intercept != nil {
		served = intercept(served)
	}
	calls := new(atomic.Int64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		served.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Errorf("close HTTP handler: %v", err)
		}
	})
	remote, err := httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := remote.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Errorf("close HTTP native session: %v", err)
		}
	})
	file, err := session.OpenFile(t.Context(), "item", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true, Exclusive: true}, InitialMetadata: metadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	attr, err := file.WriteAt(t.Context(), 0, []byte("initial"))
	if err != nil {
		t.Fatal(err)
	}
	registry := handleTestRegistry()
	limits := registry.limits
	limits.MaxIOBytes, limits.MaxFrameBytes, limits.MaxOpens = 64<<10, 128<<10, 2
	charge, err := storage.MetadataRetentionBytes(storage.MaxMetadataBytes)
	if err != nil {
		t.Fatal(err)
	}
	limits.MaxDirectoryBytes = charge + 512
	registry.limits = limits
	registry.tree.export.server.config.Limits = limits
	registry.tree.export.share = Share{Name: "http", Volume: "http-io", Backend: remote}
	registry.tree.authority.raw = session
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := registry.close(ctx); err != nil {
			t.Errorf("close HTTP native registry: %v", err)
		}
	})
	reservation := handleTestReserve(t, registry)
	reservation.attachFile(storage.OpenResult{File: file, Attr: attr, Outcome: storage.Opened})
	id, err := reservation.install(fileReadData|fileWriteData, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	c := newConnection(registry.tree.export.server, &capturedConnection{})
	t.Cleanup(c.cancel)
	return nativeIOFixture{connection: c, tree: registry.tree, volume: volume, session: session, file: file, id: id, nodeID: attr.ID}, calls
}

func nativeIOLargeMetadata() map[string][]byte {
	return map[string][]byte{
		"client.first":  bytes.Repeat([]byte{'a'}, 24<<10),
		"client.second": bytes.Repeat([]byte{'b'}, 24<<10),
	}
}

func nativeIOResultBytes(t *testing.T, registry *handleRegistry) int64 {
	t.Helper()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.resultBytes
}

func TestNativeHTTPFileCommandsKeepMetadataOutsideSMBDataFrameBudget(t *testing.T) {
	f, calls := newNativeIOHTTPFixture(t, nativeIOLargeMetadata(), nil)
	before := f.retainedAttr(t)
	metadataBytes, err := storage.MetadataSize(before.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := storage.MetadataRetentionBytes(int64(metadataBytes))
	if err != nil || retained <= int64(f.connection.server.config.Limits.MaxFrameBytes) {
		t.Fatalf("metadata retention %d, %v does not exceed SMB frame limit", retained, err)
	}
	content := string(bytes.Repeat([]byte{'z'}, f.connection.server.config.Limits.MaxIOBytes))
	beforeCalls := calls.Load()
	f.write(t, 0, content)
	f.read(t, 0, uint32(len(content)), content)
	if got := calls.Load() - beforeCalls; got != 2 {
		t.Fatalf("WRITE and READ made %d HTTP calls, want 2", got)
	}
	if after := f.retainedAttr(t); !reflect.DeepEqual(after.Metadata, before.Metadata) || after.Size != int64(len(content)) {
		t.Fatalf("HTTP byte commands changed metadata or EOF: size %d, metadata equal %v", after.Size, reflect.DeepEqual(after.Metadata, before.Metadata))
	}
	if retained := nativeIOResultBytes(t, f.tree.files); retained != 0 {
		t.Fatalf("completed HTTP commands retained %d result bytes", retained)
	}
}

func TestNativeHTTPFileCommandsReserveResultsBeforeSending(t *testing.T) {
	f, calls := newNativeIOHTTPFixture(t, nativeIOLargeMetadata(), nil)
	registry := f.tree.files
	held, err := registry.reserveResponse()
	if err != nil {
		t.Fatal(err)
	}
	held.attachNode(storage.NodeOpenResult{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(held.releaseResponse)
		if err := held.finish(context.Background()); err != nil {
			t.Errorf("release CREATE result reservation: %v", err)
		}
	}
	t.Cleanup(release)
	remoteBefore := f.retainedAttr(t)
	before, err := f.volume.Stat(t.Context(), "item")
	if err != nil {
		t.Fatal(err)
	}
	// Native optional times use UTC, while HTTP reconstructs the same instants
	// in Local. The wire representation compares every fact without Location.
	if !reflect.DeepEqual(httprest.AttrOf(before), httprest.AttrOf(remoteBefore)) {
		nativeIOAttributeFailure(t, "HTTP and native baseline differ", before, remoteBefore)
	}
	beforeCalls := calls.Load()
	for _, command := range []struct {
		name string
		run  func() ([]byte, uint32)
	}{
		{name: "WRITE", run: func() ([]byte, uint32) {
			return f.connection.writeHandle(t.Context(), f.tree, nativeIOWriteRequest(f.id, 0, []byte("changed")))
		}},
		{name: "READ", run: func() ([]byte, uint32) {
			return f.connection.readHandle(t.Context(), f.tree, nativeIOReadRequest(f.id, 0, 64<<10))
		}},
		{name: "FLUSH", run: func() ([]byte, uint32) {
			return f.connection.flushHandle(t.Context(), f.tree, nativeIOFlushRequest(f.id))
		}},
	} {
		body, status := command.run()
		if status != statusResources || len(body) != 0 {
			t.Fatalf("%s while CREATE owns result capacity: status %#x, body %x", command.name, status, body)
		}
		if got := calls.Load(); got != beforeCalls {
			t.Fatalf("%s entered HTTP before result admission: calls %d, want %d", command.name, got, beforeCalls)
		}
		if got := nativeIOResultBytes(t, registry); got != held.charge {
			t.Fatalf("%s changed held CREATE result charge: %d, want %d", command.name, got, held.charge)
		}
	}
	after, err := f.volume.Stat(t.Context(), "item")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		nativeIOAttributeFailure(t, "unadmitted HTTP write changed native attributes", before, after)
	}
	if data, err := f.volume.Read(t.Context(), "item"); err != nil || string(data) != "initial" {
		t.Fatalf("unadmitted HTTP write changed native bytes: %q, %v", data, err)
	}
	release()
	if got := nativeIOResultBytes(t, registry); got != 0 {
		t.Fatalf("released CREATE retained %d result bytes", got)
	}
	f.write(t, 0, "changed")
	f.read(t, 0, 64<<10, "changed")
	if got := nativeIOResultBytes(t, registry); got != 0 {
		t.Fatalf("subsequent HTTP commands retained %d result bytes", got)
	}
}

func nativeIOAttributeFailure(t *testing.T, message string, before, after storage.Attr) {
	t.Helper()
	metadataEqual := reflect.DeepEqual(before.Metadata, after.Metadata)
	before.Metadata, after.Metadata = nil, nil
	t.Fatalf("%s: before %+v, after %+v, metadata equal %v", message, before, after, metadataEqual)
}

func TestNativeHTTPReadCancellationReturnsResultReservation(t *testing.T) {
	var armed atomic.Bool
	entered, exited, unblock := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var unblockOnce sync.Once
	f, _ := newNativeIOHTTPFixture(t, nativeIOLargeMetadata(), func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !armed.CompareAndSwap(true, false) {
				next.ServeHTTP(w, r)
				return
			}
			defer close(exited)
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				t.Errorf("consume gated HTTP request: %v", err)
				return
			}
			close(entered)
			select {
			case <-r.Context().Done():
			case <-unblock:
			}
		})
	})
	ctx, cancel := context.WithCancel(t.Context())
	type result struct {
		body   []byte
		status uint32
	}
	done := make(chan result, 1)
	settled := false
	t.Cleanup(func() {
		cancel()
		unblockOnce.Do(func() { close(unblock) })
		if !settled {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("HTTP READ did not settle during cleanup")
			}
		}
	})
	armed.Store(true)
	go func() {
		body, status := f.connection.readHandle(ctx, f.tree, nativeIOReadRequest(f.id, 0, 64<<10))
		done <- result{body: body, status: status}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP READ did not enter the request gate")
	}
	if held := nativeIOResultBytes(t, f.tree.files); held != f.tree.files.limits.MaxDirectoryBytes {
		t.Fatalf("in-flight HTTP READ retained %d bytes, want %d", held, f.tree.files.limits.MaxDirectoryBytes)
	}
	cancel()
	select {
	case result := <-done:
		settled = true
		if result.status != statusCancelled || len(result.body) != 0 {
			t.Fatalf("canceled HTTP READ: status %#x, body %x", result.status, result.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP READ did not settle after cancellation")
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP server did not observe READ cancellation")
	}
	if held := nativeIOResultBytes(t, f.tree.files); held != 0 {
		t.Fatalf("canceled HTTP READ retained %d result bytes", held)
	}
	f.read(t, 0, 64<<10, "initial")
	if err := f.tree.files.close(t.Context()); err != nil {
		t.Fatalf("close registry after HTTP cancellation: %v", err)
	}
	handleTestAccounting(t, f.tree.files, 0, 0, 0, 0)
}
