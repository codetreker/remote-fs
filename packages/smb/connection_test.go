package smb

import (
	"bytes"
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestCancelRespectsSessionAndAsyncIdentity(t *testing.T) {
	c, s, _, _, _, _ := testConnection(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.pending[10] = &pendingRequest{ctx: ctx, cancel: cancel, sessionID: 1, treeID: 1, fileID: wire.FileID{1}}
	r := signedRequest(t, s, fileRequest(wire.Cancel, wire.EmptyResponseBody()))
	if err := c.cancelRequest(r); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("cancel lost")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	c.pending[10].cancel = cancel
	c.pending[10].sessionID = 2
	if err := c.cancelRequest(r); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		t.Fatal("cancel crossed session")
	default:
	}
	c.pending[10].sessionID = 1
	r.Header.Flags |= wire.FlagAsync
	r.Header.AsyncID = 99
	r.Packet = requestPacket(r.Header, r.Body)
	_ = s.signer.Sign(r.Packet)
	if err := c.cancelRequest(r); err != nil {
		t.Fatal(err)
	}
	c.cancelOpen(1, 1, wire.FileID{1}, 11)
	select {
	case <-ctx.Done():
	default:
		t.Fatal("close did not cancel outstanding work")
	}
}

type capturedConnection struct {
	net.Conn
	bytes.Buffer
}

func (c *capturedConnection) Write(b []byte) (int, error) { return c.Buffer.Write(b) }
func (c *capturedConnection) Read(b []byte) (int, error)  { return c.Conn.Read(b) }

func TestCancelledDependentRequestNeverReachesBackend(t *testing.T) {
	c, s, tr, f, _, _ := testConnection(t)
	file := &protocolFile{sessionFile: f}
	tr.files.handles[wire.FileID{1}].file = file
	output := &capturedConnection{Conn: c.net}
	c.net = output
	r := signedRequest(t, s, readCommand(wire.FileID{1}, 0, 3))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.pending[r.Header.MessageID] = &pendingRequest{ctx: ctx, cancel: cancel, sessionID: s.id, frame: r.Header.MessageID, async: true}
	if err := c.process([]wire.Request{r}); err != nil {
		t.Fatal(err)
	}
	h, err := wire.ParseHeader(output.Bytes()[4:])
	if err != nil || h.Status != statusCancelled || h.Flags&wire.FlagAsync == 0 || h.Credits != 0 || file.reads.Load() != 0 {
		t.Fatalf("cancelled dependency %+v %v reads%d", h, err, file.reads.Load())
	}
}

func TestRelatedFileIDsAndResponseCredits(t *testing.T) {
	for _, tc := range []struct {
		command uint16
		offset  int
	}{{wire.Read, 16}, {wire.Write, 16}, {wire.Close, 8}, {wire.Flush, 8}, {wire.Lock, 8}, {wire.IOCTL, 8}, {wire.QueryDirectory, 8}, {wire.ChangeNotify, 8}, {wire.QueryInfo, 24}, {wire.SetInfo, 16}} {
		b := make([]byte, 64)
		id := wire.FileID{3, 4}
		patchRelatedFile(tc.command, b, id)
		r := fileRequest(tc.command, b)
		got, ok := relatedFile(r)
		if !ok || got != id {
			t.Fatalf("command%d file%v", tc.command, got)
		}
	}
	if _, ok := relatedFile(fileRequest(wire.Echo, wire.EmptyResponseBody())); ok {
		t.Fatal("echo has file ID")
	}
	if got := requiredCredits(readCommand(wire.FileID{}, 0, 131072)); got != 2 {
		t.Fatal(got)
	}
	for _, command := range []uint16{wire.QueryDirectory, wire.QueryInfo, wire.ChangeNotify} {
		var b []byte
		switch command {
		case wire.QueryDirectory:
			b = make([]byte, 32)
			smbLE.PutUint16(b, 33)
			smbLE.PutUint32(b[28:], 131072)
		case wire.QueryInfo:
			b = make([]byte, 40)
			smbLE.PutUint16(b, 41)
			smbLE.PutUint32(b[4:], 131072)
		case wire.ChangeNotify:
			b = make([]byte, 32)
			smbLE.PutUint16(b, 32)
			smbLE.PutUint32(b[4:], 131072)
		}
		if got := requiredCredits(fileRequest(command, b)); got != 2 {
			t.Fatalf("command%d charge%d", command, got)
		}
	}
	c, _, _, _, _, _ := testConnection(t)
	if got := c.grantCredits(65535); int(got) != c.server.config.Limits.MaxRequests-1 {
		t.Fatal(got)
	}
}

func TestRenewalFailureFencesTreeAndPreservesCleanupFailure(t *testing.T) {
	c, s, tr, _, ws, _ := testConnection(t)
	c.server.config.Limits.FileSession.Lease = 3 * time.Millisecond
	ws.renewErr = syscall.EIO
	c.wg.Add(1)
	go c.renew(tr, s.principal)
	waitNotify(t, func() bool { tr.closeMu.Lock(); defer tr.closeMu.Unlock(); return tr.closed })
	if tr.files.get(wire.FileID{1}) != nil {
		t.Fatal("renewal failure left open usable")
	}
	c.server.cleanupFailure(syscall.EIO)
	c.server.cleanupFailure(syscall.EBADF)
	if c.server.Status().CleanupFailures != 2 || !errors.Is(c.server.cleanupErr, syscall.EIO) {
		t.Fatal("cleanup cause overwritten")
	}
	// This fixture deliberately records cleanup failure; consume its expected result
	// before the shared cleanup asserts successful service shutdown.
	c.server.mu.Lock()
	c.server.cleanupErr = nil
	c.server.mu.Unlock()
}

func TestSemanticOperationRouting(t *testing.T) {
	for command := wire.TreeDisconnect; command <= wire.SetInfo; command++ {
		if command == wire.Cancel || command == wire.Echo {
			continue
		}
		if operationFor(command) == "" {
			t.Fatalf("missing command%d", command)
		}
	}
	if operationFor(65535) != "" {
		t.Fatal("unknown command authorized")
	}
	if operationFor(wire.Create) != storage.OpWindowsOpen || operationFor(wire.Write) != storage.OpWindowsWrite || operationFor(wire.Lock) != storage.OpWindowsLockBatch {
		t.Fatal("wrong operation")
	}
}

func TestCompoundReplyReservationCoversPayloadAndErrorFrames(t *testing.T) {
	requests := []wire.Request{
		readCommand(wire.FileID{1}, 0, 0), readCommand(wire.FileID{1}, 0, 65536),
		queryCommand(wire.FileID{1}, 1, 4, 65536), dirCommand(wire.FileID{1}, 0, 65536),
		ioctlCommand(fsctlGetReparsePoint, nil), createCommand("file", 1, 1),
	}
	b := make([]byte, 32)
	smbLE.PutUint16(b, 32)
	smbLE.PutUint32(b[4:], 65536)
	requests = append(requests, fileRequest(wire.ChangeNotify, b))
	for _, r := range requests {
		budget := responseBudget(r)
		if budget < 80 || budget&7 != 0 {
			t.Fatalf("command%d reserve%d", r.Header.Command, budget)
		}
		if r.Header.Command == wire.Read && len(r.Body) > 4 && smbLE.Uint32(r.Body[4:]) != 0 && budget < 65536+80 {
			t.Fatal("read payload escapes reservation")
		}
	}
}
