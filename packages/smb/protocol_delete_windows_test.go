package smb_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	smbwindows "github.com/codetreker/remote-fs/packages/smb/windows"
	"github.com/codetreker/remote-fs/packages/storage"
	win "golang.org/x/sys/windows"
)

func TestNativeWindowsDeleteOnClose(t *testing.T) {
	identity, err := smbwindows.CurrentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := smbwindows.AllowIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	limits := smb.DefaultLimits()
	limits.RequestTimeout, limits.CleanupTimeout = 10*time.Second, 10*time.Second
	server, err := smb.New(smb.Config{Authenticator: smbwindows.NewAuthenticator(), AuthorizeIdentity: policy,
		Authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil }),
		Limits:    limits, Logger: slog.New(slog.NewTextHandler(nativeProbeLog{t}, nil))})
	if err != nil {
		t.Fatal(err)
	}
	backend := smb.NewNativeDeleteProbeBackend()
	owner, err := storage.NewDeleteIntentOwner()
	if err != nil {
		t.Fatal(err)
	}
	share := fmt.Sprintf("rfsdelete%x", time.Now().UnixNano())
	export, err := server.Publish(smb.Share{Name: share, Volume: share, BackendVolume: "test-volume", RootNodeID: 1, Backend: backend, DeleteIntentOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if err := export.RecoverDeleteIntents(smb.WithPrincipal(t.Context(), identity)); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	receipts := &nativeDeleteReceipts{}
	transport := &nativeDeleteListener{Listener: listener, receipts: receipts}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, transport) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-served:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(15 * time.Second):
			t.Error("native delete server did not stop")
		}
		stop, release := context.WithTimeout(smb.WithPrincipal(context.Background(), identity), 15*time.Second)
		defer release()
		if err := server.Shutdown(stop); err != nil {
			t.Error(err)
		}
		receipts.report(t)
	})
	drive, remote := nativeUnusedDrive(t), `\\127.0.0.1\`+share
	port := listener.Addr().(*net.TCPAddr).Port
	t.Cleanup(func() { nativeMappingCommand(t, drive, remote, port, true) })
	nativeMappingCommand(t, drive, remote, port, false)

	t.Run("new-file-exclusive-share", func(t *testing.T) {
		path := drive + `\exclusive.txt`
		h := nativeDeleteOpen(t, path, win.GENERIC_READ|0x10000, 0, win.CREATE_NEW, win.FILE_FLAG_DELETE_ON_CLOSE)
		var data [1]byte
		var read uint32
		if err := win.ReadFile(h.Handle, data[:], &read, nil); err != nil || read != 0 {
			t.Fatalf("empty file read=%d error=%v", read, err)
		}
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
		nativeDeleteAbsent(t, path, backend, receipts)
	})
	t.Run("metadata-only-file", func(t *testing.T) {
		path := drive + `\alpha.txt`
		h := nativeDeleteOpen(t, path, win.FILE_READ_ATTRIBUTES|0x10000, 7, win.OPEN_EXISTING, win.FILE_FLAG_DELETE_ON_CLOSE)
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
		nativeDeleteAbsent(t, path, backend, receipts)
	})
	t.Run("empty-directory", func(t *testing.T) {
		path := drive + `\empty`
		h := nativeDeleteOpen(t, path, win.FILE_READ_ATTRIBUTES|0x10000, 7, win.OPEN_EXISTING, win.FILE_FLAG_DELETE_ON_CLOSE|win.FILE_FLAG_BACKUP_SEMANTICS)
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
		nativeDeleteAbsent(t, path, backend, receipts)
	})
	t.Run("two-compatible-handles", func(t *testing.T) {
		path := drive + `\beta.bin`
		survivor := nativeDeleteOpen(t, path, win.GENERIC_READ, 7, win.OPEN_EXISTING, 0)
		initiator := nativeDeleteOpen(t, path, win.FILE_READ_ATTRIBUTES|0x10000, 7, win.OPEN_EXISTING, win.FILE_FLAG_DELETE_ON_CLOSE)
		if err := initiator.Close(); err != nil {
			t.Fatal(err)
		}
		var standard smb.NativeProbeFileStandardInfo
		if unsafe.Sizeof(standard) != 24 || unsafe.Alignof(standard) != 8 || unsafe.Offsetof(standard.DeletePending) != 20 || unsafe.Offsetof(standard.Directory) != 21 {
			t.Fatal("invalid Win32 FILE_STANDARD_INFO layout")
		}
		t.Logf("Win32 FILE_STANDARD_INFO: size=%d alignment=%d pointer remainder=%d", unsafe.Sizeof(standard), unsafe.Alignof(standard), uintptr(unsafe.Pointer(&standard))%8)
		infoErr := win.GetFileInformationByHandleEx(survivor.Handle, win.FileStandardInfo, (*byte)(unsafe.Pointer(&standard)), uint32(unsafe.Sizeof(standard)))
		if infoErr != nil {
			t.Fatal(infoErr)
		}
		if standard.NumberOfLinks != 0 || standard.DeletePending != 1 || standard.Directory != 0 || standard.EndOfFile != 0 || standard.AllocationSize != 0 {
			t.Fatalf("pending standard info=%+v", standard)
		}
		var data [1]byte
		var read uint32
		if err := win.ReadFile(survivor.Handle, data[:], &read, nil); err != nil || read != 0 {
			t.Fatalf("pending survivor read=%d error=%v", read, err)
		}
		name, err := win.UTF16PtrFromString(path)
		if err != nil {
			t.Fatal(err)
		}
		unexpected, err := win.CreateFile(name, win.FILE_READ_ATTRIBUTES, 7, nil, win.OPEN_EXISTING, 0, 0)
		if err == nil {
			_ = win.CloseHandle(unexpected)
			t.Fatal("new native open admitted during delete-pending")
		}
		if !errors.Is(err, win.ERROR_DELETE_PENDING) && !errors.Is(err, win.ERROR_ACCESS_DENIED) {
			t.Fatalf("pending native open: %v", err)
		}
		if err := survivor.Close(); err != nil {
			t.Fatal(err)
		}
		nativeDeleteAbsent(t, path, backend, receipts)
	})
	t.Run("nonempty-directory-close-results", func(t *testing.T) {
		path := drive + `\folder`
		survivor := nativeOpenDirectory(t, path)
		var before smb.NativeProbeFileIDInfo
		if unsafe.Sizeof(before) != 24 || unsafe.Alignof(before) != 8 {
			t.Fatal("invalid Win32 FILE_ID_INFO layout")
		}
		if err := win.GetFileInformationByHandleEx(survivor, win.FileIdInfo, (*byte)(unsafe.Pointer(&before)), uint32(unsafe.Sizeof(before))); err != nil {
			t.Fatal(err)
		}
		if id := binary.LittleEndian.Uint64(before.FileID[:]); id != 2 {
			t.Fatalf("native original directory ID=%d", id)
		}
		nativeFindNames(t, path+`\*`, []string{"nested.txt"})
		h := nativeDeleteOpen(t, path, win.FILE_READ_ATTRIBUTES|0x10000, 7, win.OPEN_EXISTING, win.FILE_FLAG_DELETE_ON_CLOSE|win.FILE_FLAG_BACKUP_SEMANTICS)
		start := receipts.closeCount()
		appErr := h.Close()
		t.Logf("Win32 CloseHandle(nonempty directory): %v", appErr)
		if status := receipts.waitClose(t, start, 0xc0000101); status != 0xc0000101 {
			t.Fatalf("SMB CLOSE(nonempty directory)=%#x", status)
		}
		found := false
		for _, outcome := range backend.ProbeDeleteOutcomes() {
			if outcome.NodeID == 2 && outcome.Outcome == storage.DeleteIntentNotExecuted {
				found = true
			}
		}
		if !found {
			t.Fatal("authority did not retain nonempty directory NotExecuted result")
		}
		var after smb.NativeProbeFileIDInfo
		if err := win.GetFileInformationByHandleEx(survivor, win.FileIdInfo, (*byte)(unsafe.Pointer(&after)), uint32(unsafe.Sizeof(after))); err != nil {
			t.Fatal(err)
		}
		if before != after {
			t.Fatalf("native directory identity changed: before=%x after=%x", before, after)
		}
		nativeWantNames(t, nativeReadAll(t, survivor, 12, "*", true), []string{"nested.txt"})
		authority, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := authority.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		for _, expected := range []struct {
			parent, id uint64
			name       string
		}{{1, 2, "folder"}, {2, 6, "nested.txt"}} {
			result, err := storage.NewListResult(65536, 0, func(_ int, name, metadata int64, _ storage.Attr) (int64, error) {
				return storage.ObservedEntryBytes(name, metadata)
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := authority.(storage.DirectoryReader).ReadDirNodeBounded(t.Context(), storage.DirectoryTarget{NodeID: expected.parent}, result); err != nil {
				t.Fatal(err)
			}
			entries, err := result.Entries()
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, entry := range entries {
				if entry.Name == expected.name && entry.Attr.ID == expected.id {
					found = true
				}
			}
			if !found {
				t.Fatalf("authority lost original %q node%d under node%d", expected.name, expected.id, expected.parent)
			}
		}
		search, err := win.UTF16PtrFromString(path + `\*`)
		if err != nil {
			t.Fatal(err)
		}
		var data win.Win32finddata
		find, findErr := win.FindFirstFile(search, &data)
		t.Logf("Win32 FindFirstFile(nonempty directory after CLOSE): %v", findErr)
		if findErr == nil {
			if err := win.FindClose(find); err != nil {
				t.Fatal(err)
			}
			nativeFindNames(t, path+`\*`, []string{"nested.txt"})
		}
	})
	t.Run("translates-delete-access", func(t *testing.T) {
		path := drive + `\long-😀-name.txt`
		receipts.mu.Lock()
		before := len(receipts.creates)
		receipts.mu.Unlock()
		h := nativeDeleteOpen(t, path, win.FILE_READ_ATTRIBUTES, 7, win.OPEN_EXISTING, win.FILE_FLAG_DELETE_ON_CLOSE)
		receipts.mu.Lock()
		creates := append([]wire.CreateRequest(nil), receipts.creates[before:]...)
		receipts.mu.Unlock()
		if len(creates) != 1 || creates[0].DesiredAccess&0x10000 == 0 {
			t.Fatalf("native delete access normalization=%+v", creates)
		}
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
		nativeDeleteAbsent(t, path, backend, receipts)
	})
	t.Run("connection-loss", func(t *testing.T) {
		path := drive + `\disconnect.txt`
		h := nativeDeleteOpen(t, path, win.FILE_READ_ATTRIBUTES|0x10000, 7, win.CREATE_NEW, win.FILE_FLAG_DELETE_ON_CLOSE)
		var intent storage.DeleteIntentStatus
		for _, candidate := range backend.ProbeDeleteOutcomes() {
			if candidate.Outcome == storage.DeleteIntentArmed {
				intent = candidate
			}
		}
		if intent.ID == "" {
			t.Fatal("native open established no Armed intent")
		}
		transport.disconnect(t)
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		for {
			for _, candidate := range backend.ProbeDeleteOutcomes() {
				if candidate.ID == intent.ID && candidate.Outcome == storage.DeleteIntentCompleted {
					t.Logf("Win32 CloseHandle(disconnected reference): %v", h.Close())
					return
				}
			}
			select {
			case <-deadline.C:
				t.Fatal("connection loss did not complete accepted delete obligation")
			case <-time.After(10 * time.Millisecond):
			}
		}
	})
}

type nativeDeleteHandle struct {
	win.Handle
	closed bool
}

func (h *nativeDeleteHandle) Close() error {
	if h.closed {
		return nil
	}
	h.closed = true
	return win.CloseHandle(h.Handle)
}

func nativeDeleteOpen(t *testing.T, path string, access, share, disposition, flags uint32) *nativeDeleteHandle {
	t.Helper()
	name, err := win.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := win.CreateFile(name, access, share, nil, disposition, flags, 0)
	if err != nil {
		t.Fatal(err)
	}
	owned := &nativeDeleteHandle{Handle: h}
	t.Cleanup(func() {
		if err := owned.Close(); err != nil {
			t.Error(err)
		}
	})
	return owned
}
func nativeDeleteAbsent(t *testing.T, path string, backend storage.FileStorage, receipts *nativeDeleteReceipts) {
	t.Helper()
	leaf := path[strings.LastIndex(path, `\`)+1:]
	receipts.mu.Lock()
	receipts.missingName = leaf
	receipts.missingRequests = make(map[[2]uint64]struct{})
	receipts.missingStatuses = nil
	receipts.mu.Unlock()
	name, err := win.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := win.CreateFile(name, win.FILE_READ_ATTRIBUTES, 7, nil, win.OPEN_EXISTING, win.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err == nil {
		_ = win.CloseHandle(h)
		t.Fatalf("deleted native path %q reopened", path)
	}
	if !errors.Is(err, win.ERROR_FILE_NOT_FOUND) && !errors.Is(err, win.ERROR_PATH_NOT_FOUND) {
		t.Fatalf("deleted native path %q error=%v", path, err)
	}
	receipts.waitMissing(t)
	authority, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := authority.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	result, err := storage.NewListResult(65536, 0, func(_ int, name, metadata int64, _ storage.Attr) (int64, error) {
		return storage.ObservedEntryBytes(name, metadata)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.(storage.DirectoryReader).ReadDirNodeBounded(t.Context(), storage.DirectoryTarget{NodeID: 1}, result); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name == leaf {
			t.Fatalf("authority retained deleted name %q", leaf)
		}
	}
}

type nativeDeleteReceipts struct {
	mu              sync.Mutex
	creates         []wire.CreateRequest
	closes          []uint32
	missingName     string
	missingRequests map[[2]uint64]struct{}
	missingStatuses []uint32
	err             error
}

func (r *nativeDeleteReceipts) fail(err error) { r.mu.Lock(); defer r.mu.Unlock(); r.err = err }
func (r *nativeDeleteReceipts) request(packet []byte) {
	if string(packet[:4]) != "\xfeSMB" {
		return
	}
	requests, err := wire.ParseFrame(packet, wire.Limits{MaxBytes: 2 << 20, MaxCommands: 32, MaxContexts: 16})
	if err != nil {
		r.fail(err)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, request := range requests {
		if request.Header.Command != wire.Create && request.Header.Command != wire.Close {
			continue
		}
		if request.Header.Flags&wire.FlagSigned == 0 {
			r.err = fmt.Errorf("native delete request is unsigned")
			return
		}
		if request.Header.Command == wire.Create {
			create, err := request.Create()
			if err != nil {
				r.err = err
				return
			}
			if r.missingName != "" && create.Name == r.missingName && create.Disposition == 1 && create.Options&0x1000 == 0 {
				if len(r.missingRequests) >= 4 {
					r.err = fmt.Errorf("native absence probe exceeds four requests")
					return
				}
				r.missingRequests[[2]uint64{request.Header.SessionID, request.Header.MessageID}] = struct{}{}
			}
			if create.Options&0x1000 != 0 {
				if len(r.creates) >= 32 {
					r.err = fmt.Errorf("native close-delete trace exceeds 32 opens")
					return
				}
				create.Name, create.Contexts = "", nil
				r.creates = append(r.creates, create)
			}
		}
	}
}
func (r *nativeDeleteReceipts) response(packet []byte) {
	if string(packet[:4]) != "\xfeSMB" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for commands := 0; commands < 32; commands++ {
		header, err := wire.ParseHeader(packet)
		if err != nil {
			r.err = err
			return
		}
		key := [2]uint64{header.SessionID, header.MessageID}
		_, missing := r.missingRequests[key]
		if missing && header.Command == wire.Create {
			if header.Flags&(wire.FlagResponse|wire.FlagSigned) != (wire.FlagResponse | wire.FlagSigned) {
				r.err = fmt.Errorf("native delete probe response is unsigned")
				return
			}
			if missing && header.Command == wire.Create {
				r.missingStatuses = append(r.missingStatuses, header.Status)
				delete(r.missingRequests, key)
			}
		}
		if header.Command == wire.Close {
			if len(r.closes) >= 128 {
				r.err = fmt.Errorf("native close-delete trace exceeds 128 closes")
				return
			}
			if header.Flags&(wire.FlagResponse|wire.FlagSigned) != (wire.FlagResponse | wire.FlagSigned) {
				r.err = fmt.Errorf("native CLOSE response lacks signed response flags")
				return
			}
			r.closes = append(r.closes, header.Status)
		}
		if header.NextCommand == 0 {
			return
		}
		if header.NextCommand < wire.HeaderSize || uint64(header.NextCommand)+wire.HeaderSize > uint64(len(packet)) {
			r.err = wire.ErrMalformed
			return
		}
		packet = packet[header.NextCommand:]
	}
	r.err = fmt.Errorf("native close-delete response exceeds compound bound")
}
func (r *nativeDeleteReceipts) closeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.closes)
}
func (r *nativeDeleteReceipts) waitClose(t *testing.T, start int, expected uint32) uint32 {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		err := r.err
		for _, status := range r.closes[start:] {
			if status == expected {
				r.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				return status
			}
		}
		r.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-deadline.C:
			t.Fatalf("native close emitted no SMB CLOSE with status %#x", expected)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func (r *nativeDeleteReceipts) report(t *testing.T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		t.Error(r.err)
	}
	if len(r.creates) == 0 {
		t.Error("native probe emitted no signed delete-on-close CREATE")
	}
	for _, create := range r.creates {
		t.Logf("native delete CREATE: access=%#x share=%#x disposition=%d options=%#x", create.DesiredAccess, create.ShareAccess, create.Disposition, create.Options)
	}
}

func (r *nativeDeleteReceipts) waitMissing(t *testing.T) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		err := r.err
		statuses := append([]uint32(nil), r.missingStatuses...)
		r.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		for _, status := range statuses {
			if status == 0xc0000034 {
				return
			}
		}
		select {
		case <-deadline.C:
			t.Fatalf("native absent-name OPEN_EXISTING lacks signed STATUS_OBJECT_NAME_NOT_FOUND: statuses=%x", statuses)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type nativeDeleteListener struct {
	net.Listener
	receipts    *nativeDeleteReceipts
	mu          sync.Mutex
	connections []net.Conn
}

func (l *nativeDeleteListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.connections = append(l.connections, c)
	l.mu.Unlock()
	return &nativeDeleteConnection{Conn: c, input: nativeFrameStream{record: l.receipts.request, fail: l.receipts.fail}, output: nativeFrameStream{record: l.receipts.response, fail: l.receipts.fail}}, nil
}
func (l *nativeDeleteListener) disconnect(t *testing.T) {
	l.mu.Lock()
	connections := append([]net.Conn(nil), l.connections...)
	l.mu.Unlock()
	for _, c := range connections {
		if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	}
}

type nativeDeleteConnection struct {
	net.Conn
	input, output nativeFrameStream
}

func (c *nativeDeleteConnection) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.input.capture(p[:n])
	return n, err
}
func (c *nativeDeleteConnection) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.output.capture(p[:n])
	return n, err
}
