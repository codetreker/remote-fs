//go:build windows

package windows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/smb"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
	win "golang.org/x/sys/windows"
)

type nativeAck struct {
	committed chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (a *nativeAck) resume() { a.once.Do(func() { close(a.release) }) }

type nativeHTTPGate struct {
	handler http.Handler
	mu      sync.Mutex
	ack     *nativeAck
	down    atomic.Bool
}

func (g *nativeHTTPGate) holdWrite() *nativeAck {
	g.mu.Lock()
	defer g.mu.Unlock()
	a := &nativeAck{committed: make(chan struct{}), release: make(chan struct{})}
	g.ack = a
	return a
}
func (g *nativeHTTPGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if g.down.Load() {
		connection, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
		return
	}
	var held *nativeAck
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/windows") {
		body, err := io.ReadAll(io.LimitReader(r.Body, (2<<20)+1))
		_ = r.Body.Close()
		if err != nil || len(body) > 2<<20 {
			http.Error(w, "invalid test request body", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var command struct {
			Op storage.Operation `json:"op"`
		}
		if json.Unmarshal(body, &command) == nil && command.Op == storage.OpWindowsWrite {
			g.mu.Lock()
			held, g.ack = g.ack, nil
			g.mu.Unlock()
		}
	}
	if held == nil {
		g.handler.ServeHTTP(w, r)
		return
	}
	response := httptest.NewRecorder()
	g.handler.ServeHTTP(response, r)
	close(held.committed)
	select {
	case <-held.release:
	case <-r.Context().Done():
		return
	}
	for name, values := range response.Header() {
		w.Header()[name] = values
	}
	w.WriteHeader(response.Code)
	_, _ = w.Write(response.Body.Bytes())
}

// The Windows redirector retains a server's transport port across mappings and
// test processes. Every native case uses this same test-owned endpoint.
const nativeSMBTestAddress = "127.0.0.1:51445"

type nativeBridge struct {
	backend       *nativeAuthority
	remote        *httprest.Storage
	http          *httptest.Server
	gate          *nativeHTTPGate
	smb           *smb.Server
	mapping       *Mapping
	wire          nativeWireObservation
	port          uint16
	share         string
	path          string
	serveMu       sync.Mutex
	serveReturned bool
	serveErr      error
}

// This fixture proves the Windows kernel, SSPI, mapping and HTTP boundary. Its
// bounded Go authority is not evidence for the Linux native storage implementation.
func nativeBridgeFixture(t *testing.T, authorize authz.Authorizer) *nativeBridge {
	t.Helper()
	version := win.RtlGetVersion()
	if version.MajorVersion != 10 || version.BuildNumber < 26100 || version.ProductType != 1 {
		t.Fatalf("native SMB acceptance requires Windows11 24H2+: %+v", version)
	}
	if runtime.GOARCH != "arm64" && runtime.GOARCH != "amd64" {
		t.Fatalf("unsupported native architecture %s", runtime.GOARCH)
	}
	b := &nativeBridge{backend: newNativeAuthority(), share: fmt.Sprintf("rfs-native-%d", time.Now().UnixNano())}
	var handler *httprest.Handler
	var transport *http.Transport
	var done chan error
	t.Cleanup(func() {
		if t.Failed() && b.smb != nil {
			b.logFailure(t)
		}
		if b.gate != nil {
			b.gate.down.Store(false)
			b.gate.mu.Lock()
			if b.gate.ack != nil {
				b.gate.ack.resume()
			}
			b.gate.mu.Unlock()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if b.mapping != nil {
			if err := b.mapping.ForceUnmount(ctx); err != nil {
				t.Errorf("remove owned native mapping: %v", err)
			}
		}
		if b.smb != nil {
			if err := b.smb.Shutdown(ctx); err != nil {
				t.Errorf("SMB shutdown: %v", err)
			}
		}
		if handler != nil {
			handler.Stop()
		}
		if b.http != nil {
			b.http.Close()
		}
		if handler != nil {
			if err := handler.Close(ctx); err != nil {
				t.Errorf("HTTP session cleanup: %v", err)
			}
		}
		if transport != nil {
			transport.CloseIdleConnections()
		}
		if done != nil {
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("SMB serve: %v", err)
				}
			case <-ctx.Done():
				t.Error("SMB Serve did not exit")
			}
		}
		if refs := b.backend.openReferences(); refs != 0 {
			t.Errorf("native acceptance retained %d backend references", refs)
		}
		if sessions, refs := b.backend.activeState(); sessions != 0 || refs != 0 {
			t.Errorf("native acceptance retained %d active sessions and %d references", sessions, refs)
		}
	})
	var err error
	handler, err = httprest.NewHandler(b.backend, b.backend)
	if err != nil {
		t.Fatal(err)
	}
	b.gate = &nativeHTTPGate{handler: handler}
	b.http = httptest.NewServer(b.gate)
	transport = b.http.Client().Transport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 5 * time.Second
	b.remote, err = httprest.Dial(b.http.URL, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	b.smb, err = smb.New(smb.Config{Authenticator: NewAuthenticator(), Authorize: authorize, Limits: smb.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	changes := smb.ChangeSource{
		Subscribe: func(ctx context.Context) (smb.ChangeStream, error) { return b.remote.Subscribe(ctx) },
		Resume: func(ctx context.Context, id metastore.Incarnation, at metastore.Position) (smb.ChangeStream, error) {
			return b.remote.Resubscribe(ctx, id, at)
		},
		Checkpoint: func(ctx context.Context) (metastore.LogBarrier, error) {
			x, err := b.remote.Checkpoint(ctx)
			return metastore.LogBarrier{Incarnation: metastore.Incarnation(x.Incarnation), Position: metastore.Position(x.Position)}, err
		},
	}
	if _, err := b.smb.Publish(smb.Share{Name: b.share, Volume: "native-acceptance", Backend: b.remote, Changes: changes}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", nativeSMBTestAddress)
	if err != nil {
		t.Fatalf("bind shared native SMB test endpoint %s: %v", nativeSMBTestAddress, err)
	}
	b.port = uint16(listener.Addr().(*net.TCPAddr).Port)
	done = make(chan error, 1)
	go func() {
		err := b.smb.Serve(context.Background(), nativeObservedListener{Listener: listener, observation: &b.wire})
		b.serveMu.Lock()
		b.serveReturned, b.serveErr = true, err
		b.serveMu.Unlock()
		done <- err
	}()

	return b
}

func (b *nativeBridge) mapDrive(t *testing.T) error {
	t.Helper()
	drives, err := win.GetLogicalDrives()
	if err != nil {
		t.Fatal(err)
	}
	for drive := byte('Z'); drive >= 'D'; drive-- {
		if drives&(1<<uint(drive-'A')) == 0 {
			b.path = string([]byte{drive, ':'})
			break
		}
	}
	if b.path == "" {
		t.Fatal("native acceptance needs one unused drive letter")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	b.mapping, err = Map(ctx, MappingOptions{LocalPath: b.path, Share: b.share, TCPPort: b.port})
	if err != nil {
		t.Logf("native Map failed: %v", err)
		b.logFailure(t)
	}
	return err
}

func (b *nativeBridge) logFailure(t *testing.T) {
	t.Helper()
	t.Logf("advertised share=%q requested UNC=%q server=%+v", b.share, (MappingOptions{Share: b.share}).remotePath(), b.smb.Status())
	b.serveMu.Lock()
	t.Logf("TCP port=%d accepted=%d readBytes=%d writtenBytes=%d Serve returned=%v error=%v", b.port, b.wire.connections.Load(), b.wire.readBytes.Load(), b.wire.writtenBytes.Load(), b.serveReturned, b.serveErr)
	b.serveMu.Unlock()
	b.wire.mu.Lock()
	for i, h := range b.wire.headers {
		t.Logf("SMB header[%d] response=%v protocol=0x%08x command=0x%04x messageID=%d creditCharge=%d flags=0x%08x status=0x%08x", i, h.Response, h.Protocol, h.Command, h.MessageID, h.CreditCharge, h.Flags, h.Status)
	}
	for i, tree := range b.wire.trees {
		t.Logf("TREE_CONNECT[%d] messageID=%d path=%q offset=%d length=%d truncated=%v", i, tree.MessageID, tree.Path, tree.Offset, tree.Length, tree.Truncated)
	}
	for i, c := range b.wire.creates {
		t.Logf("CREATE[%d] messageID=%d security=%d oplock=0x%x impersonation=%d access=0x%x attributes=0x%x share=0x%x disposition=%d options=0x%x contexts=%q truncated=%v", i, c.MessageID, c.SecurityFlags, c.Oplock, c.Impersonation, c.Access, c.Attributes, c.ShareAccess, c.Disposition, c.Options, c.Contexts, c.Truncated)
	}
	for i, o := range b.wire.operations {
		if o.Command == 1 {
			t.Logf("SESSION_SETUP[%d] messageID=%d flags=0x%x securityMode=0x%x capabilities=0x%x channel=%d previousSessionID=%d headerSessionID=%d tokenOffset=%d tokenLength=%d bodyLength=%d", i, o.MessageID, o.Flags, o.SecurityMode, o.Capabilities, o.Channel, o.PreviousSessionID, o.HeaderSessionID, o.TokenOffset, o.TokenLength, o.BodyLength)
			continue
		}
		t.Logf("operation[%d] messageID=%d command=0x%x infoType=%d class=%d controlCode=0x%x", i, o.MessageID, o.Command, o.InfoType, o.Class, o.ControlCode)
	}
	b.wire.mu.Unlock()
}

type nativeHandle struct{ value win.Handle }

func (h *nativeHandle) close() error {
	if h.value == win.InvalidHandle {
		return nil
	}
	value := h.value
	h.value = win.InvalidHandle
	return win.CloseHandle(value)
}

func nativeOpen(t *testing.T, path string, disposition uint32) *nativeHandle {
	t.Helper()
	name, err := win.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	value, err := win.CreateFile(name, win.GENERIC_READ|win.GENERIC_WRITE, win.FILE_SHARE_READ|win.FILE_SHARE_WRITE|win.FILE_SHARE_DELETE, nil, disposition, win.FILE_ATTRIBUTE_NORMAL|win.FILE_FLAG_OVERLAPPED, 0)
	if err != nil {
		t.Fatalf("CreateFile %q: %v", path, err)
	}
	h := &nativeHandle{value: value}
	t.Cleanup(func() {
		if err := h.close(); err != nil {
			t.Error(err)
		}
	})
	return h
}

func nativeTransfer(file *nativeHandle, data []byte, write bool) (uint32, error) {
	h := file.value
	event, err := win.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return 0, err
	}
	defer win.CloseHandle(event)
	overlap := win.Overlapped{HEvent: event}
	defer runtime.KeepAlive(data)
	var count uint32
	if write {
		err = win.WriteFile(h, data, nil, &overlap)
	} else {
		err = win.ReadFile(h, data, nil, &overlap)
	}
	if err == nil {
		err = win.GetOverlappedResult(h, &overlap, &count, false)
		return count, err
	}
	if err != win.ERROR_IO_PENDING {
		return count, err
	}
	status, waitErr := win.WaitForSingleObject(event, 15000)
	if waitErr != nil || status != uint32(win.WAIT_OBJECT_0) {
		cancelErr := win.CancelIoEx(h, &overlap)
		finishErr := win.GetOverlappedResult(h, &overlap, &count, true)
		return count, errors.Join(fmt.Errorf("native I/O completion wait status=%d: %w", status, context.DeadlineExceeded), waitErr, cancelErr, finishErr)
	}
	err = win.GetOverlappedResult(h, &overlap, &count, false)
	runtime.KeepAlive(data)
	return count, err
}

func nativeAction(t *testing.T, session storage.WindowsSession) storage.WindowsActionID {
	t.Helper()
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func nativeRemoteOpen(t *testing.T, client *httprest.Storage, name string, kind storage.WindowsKind, create bool) (storage.WindowsSession, storage.WindowsFile) {
	t.Helper()
	session, err := client.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	disposition := storage.WindowsOpen
	if create {
		disposition = storage.WindowsOpenIf
	}
	lookup := storage.WindowsLookup{}
	if name != "" {
		lookup = storage.WindowsLookup{ParentID: 1, Name: name}
	}
	opened, err := session.Open(t.Context(), storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: disposition, Kind: kind}, Lookup: lookup, Mode: 0644}, nativeAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	return session, opened.File
}
func nativeRemoteWrite(t *testing.T, session storage.WindowsSession, file storage.WindowsFile, content []byte) {
	t.Helper()
	r, err := file.WriteAt(t.Context(), 0, content, nativeAction(t, session))
	if err != nil || r.State != storage.WindowsActionCompleted {
		t.Fatalf("HTTP write=%+v, error=%v", r, err)
	}
}
func nativeEventually(t *testing.T, description string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if check() {
			if time.Now().After(deadline) {
				t.Fatalf("%s became visible after one second", description)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not become visible within one second", description)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNativeWindowsHTTPBridge(t *testing.T) {
	sid, err := CurrentUserSID()
	if err != nil {
		t.Fatal(err)
	}
	authorize, err := AllowSID(sid)
	if err != nil {
		t.Fatal(err)
	}
	b := nativeBridgeFixture(t, authorize)
	if err := b.mapDrive(t); err != nil {
		t.Fatalf("native Map with required signing/custom port/write-through: %v", err)
	}
	status := b.mapping.Status()
	if !status.ParametersAccepted || status.Options.TCPPort != b.port || status.ObservedLocalPath != b.path || status.ObservedRemotePath != status.Options.remotePath() || status.DeviceTarget == "" || status.ConnectionStatus == nil || *status.ConnectionStatus != 0 || status.OwnerSID != sid {
		t.Fatalf("native mapping acceptance/identity observation is incomplete: %+v", status)
	}
	t.Logf("Windows accepted required mapping parameters on TCP port %d; observed drive %s and its DOS device target", status.Options.TCPPort, status.ObservedLocalPath)
	path := filepath.Join(b.path+`\`, "live.bin")
	file := nativeOpen(t, path, win.CREATE_ALWAYS)
	held := b.gate.holdWrite()
	defer held.resume()
	type completedIO struct {
		n   uint32
		err error
	}
	written := make(chan completedIO, 1)
	writeDone := make(chan struct{})
	payload := []byte("before-remote")
	go func() {
		defer close(writeDone)
		n, err := nativeTransfer(file, payload, true)
		written <- completedIO{n, err}
	}()
	t.Cleanup(func() {
		held.resume()
		select {
		case <-writeDone:
		case <-time.After(20 * time.Second):
			t.Error("native WriteFile worker did not drain")
		}
	})
	select {
	case <-held.committed:
	case <-time.After(15 * time.Second):
		t.Fatal("WriteFile did not reach the real HTTP authority")
	}
	if actual, err := b.backend.fileData("live.bin"); err != nil || !bytes.Equal(actual, payload) {
		t.Fatalf("independent authority content=%q,error=%v", actual, err)
	}
	select {
	case result := <-written:
		t.Fatalf("WriteFile completed before HTTP acknowledgement: %+v", result)
	case <-time.After(200 * time.Millisecond):
	}
	held.resume()
	select {
	case result := <-written:
		if result.err != nil || result.n != uint32(len(payload)) {
			t.Fatalf("WriteFile completion=%+v", result)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("WriteFile did not complete after HTTP acknowledgement")
	}
	if b.wire.connections.Load() == 0 || b.wire.writes.Load() == 0 || b.wire.signedWrites.Load() != b.wire.writes.Load() {
		t.Fatalf("native wire observation: connections=%d writes=%d signed writes=%d", b.wire.connections.Load(), b.wire.writes.Load(), b.wire.signedWrites.Load())
	}
	buffer := make([]byte, len(payload))
	if n, err := nativeTransfer(file, buffer, false); err != nil || n != uint32(len(payload)) || !bytes.Equal(buffer, payload) {
		t.Fatalf("ReadFile returned %q/%d,error=%v", buffer, n, err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	if err := b.mapping.Unmount(ctx); !errors.Is(err, ErrMappingBusy) {
		cancel()
		t.Fatalf("open native handle did not preserve busy mapping: %v", err)
	}
	cancel()
	if !b.smb.Status().Serving {
		t.Fatal("busy unmount stopped the SMB endpoint")
	}
	session, remoteFile := nativeRemoteOpen(t, b.remote, "live.bin", storage.WindowsRegularFile, false)
	updated := []byte("after--remote")
	nativeRemoteWrite(t, session, remoteFile, updated)
	nativeEventually(t, "remote write through an already open Win32 handle", func() bool {
		buf := make([]byte, len(updated))
		n, err := nativeTransfer(file, buf, false)
		if err != nil {
			t.Fatalf("ReadFile during healthy HTTP service: %v", err)
		}
		return n == uint32(len(updated)) && bytes.Equal(buf, updated)
	})

	missing := filepath.Join(b.path+`\`, "new.bin")
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("initial absent-file lookup=%v", err)
	}
	_, newRemote := nativeRemoteOpen(t, b.remote, "new.bin", storage.WindowsRegularFile, true)
	if _, err := newRemote.Stat(t.Context()); err != nil {
		t.Fatal(err)
	}
	nativeEventually(t, "remote creation after a negative lookup", func() bool {
		_, err := os.Stat(missing)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		return err == nil
	})

	directorySession, directory := nativeRemoteOpen(t, b.remote, "directory", storage.WindowsDirectory, true)
	directoryPath := filepath.Join(b.path+`\`, "directory")
	if entries, err := os.ReadDir(directoryPath); err != nil || len(entries) != 0 {
		t.Fatalf("initial directory=%v,error=%v", entries, err)
	}
	parent, err := directory.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, err = directorySession.Open(t.Context(), storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsCreate, Kind: storage.WindowsRegularFile}, Lookup: storage.WindowsLookup{ParentID: parent.ID, ParentReference: directory.Reference(), Name: "child.bin"}, Mode: 0644}, nativeAction(t, directorySession))
	if err != nil {
		t.Fatal(err)
	}
	nativeEventually(t, "remote directory entry", func() bool {
		entries, err := os.ReadDir(directoryPath)
		if err != nil {
			t.Fatal(err)
		}
		return len(entries) == 1 && entries[0].Name() == "child.bin"
	})

	b.gate.down.Store(true)
	b.http.CloseClientConnections()
	buffer = make([]byte, len(updated))
	n, readErr := nativeTransfer(file, buffer, false)
	b.gate.down.Store(false)
	if errors.Is(readErr, context.DeadlineExceeded) {
		t.Fatalf("ReadFile did not report the HTTP outage before the test watchdog: %v", readErr)
	}
	if readErr == nil || n != 0 {
		t.Fatalf("HTTP outage returned apparent file data: n=%d,error=%v", n, readErr)
	}
	if errors.Is(readErr, win.ERROR_FILE_NOT_FOUND) || errors.Is(readErr, win.ERROR_PATH_NOT_FOUND) {
		t.Fatalf("HTTP outage became missing data: %v", readErr)
	}
	if !b.smb.Status().Serving {
		t.Fatal("HTTP outage stopped the local SMB listener")
	}
	if err := file.close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := b.mapping.Unmount(ctx); err != nil {
		t.Fatalf("unmount after closing native handle: %v", err)
	}
	if _, exists, err := deviceTarget(b.path); err != nil || exists {
		t.Fatalf("native mapping survived unmount: exists=%v,error=%v", exists, err)
	}
	if !b.smb.Status().Serving {
		t.Fatal("mapping helper closed the separately owned SMB server")
	}
}

func TestNativeWindowsRejectsAnotherSID(t *testing.T) {
	current, err := CurrentUserSID()
	if err != nil {
		t.Fatal(err)
	}
	other := "S-1-5-18"
	if current == other {
		other = "S-1-5-19"
	}
	policy, err := AllowSID(other)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan string, 8)
	authorize := authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
		principal, ok := smb.PrincipalFromContext(ctx)
		if ok {
			select {
			case observed <- principal.SID:
			default:
			}
		}
		return policy.Authorize(ctx, request)
	})
	b := nativeBridgeFixture(t, authorize)
	if err := b.mapDrive(t); err == nil {
		t.Fatal("native mapping accepted a different allowed SID")
	}
	select {
	case actual := <-observed:
		if actual != current {
			t.Fatalf("SSPI authenticated unexpected SID %q", actual)
		}
	default:
		t.Fatal("mapping failed before the real SSPI identity reached authorization")
	}
}
