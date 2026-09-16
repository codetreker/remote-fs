package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

type capturedConnection struct {
	net.Conn
	bytes.Buffer
	writeLimit  int
	writeErr    error
	deadlineErr error
}

func (c *capturedConnection) Write(b []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.writeLimit > 0 && len(b) > c.writeLimit {
		b = b[:c.writeLimit]
	}
	return c.Buffer.Write(b)
}
func (c *capturedConnection) Read(b []byte) (int, error)       { return c.Conn.Read(b) }
func (c *capturedConnection) SetWriteDeadline(time.Time) error { return c.deadlineErr }

func transportFixture(t *testing.T) (*connection, *session, *capturedConnection) {
	t.Helper()
	s, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	output := &capturedConnection{}
	c := newConnection(s, output)
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	ss := &session{id: 1, signer: key, trees: make(map[uint32]*tree)}
	c.sessions[1] = ss
	t.Cleanup(c.cancel)
	return c, ss, output
}
func transportRequest(t *testing.T, s *session, command uint16, message uint64, body []byte) wire.Request {
	t.Helper()
	p := requestPacket(wire.Header{Command: command, MessageID: message, SessionID: s.id, TreeID: 1, Credits: 1}, body)
	if err := s.signer.Sign(p); err != nil {
		t.Fatal(err)
	}
	h, err := wire.ParseHeader(p)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Request{Header: h, Packet: p, Body: p[64:]}
}
func TestCancelRespectsSessionAndAsyncIdentity(t *testing.T) {
	c, s, _ := transportFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.pending[10] = &pendingRequest{ctx: ctx, cancel: cancel, sessionID: 1, treeID: 1, fileID: wire.FileID{1}, async: true}
	r := transportRequest(t, s, wire.Cancel, 10, wire.EmptyResponseBody())
	if err := c.cancelRequest(r); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatal("synchronous cancel lost")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	c.pending[10].cancel = cancel
	c.pending[10].sessionID = 2
	if err := c.cancelRequest(r); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal("cancel crossed session")
	}
	c.pending[10].sessionID = 1
	r.Header.Flags |= wire.FlagAsync
	r.Header.MessageID = 0
	r.Header.AsyncID = 11
	r.Packet = requestPacket(r.Header, r.Body)
	if err := s.signer.Sign(r.Packet); err != nil {
		t.Fatal(err)
	}
	if err := c.cancelRequest(r); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatal("async cancel used MessageID instead of AsyncID")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	c.pending[10].cancel = cancel
	r.Packet[48] ^= 1
	if !errors.Is(c.cancelRequest(r), signing.ErrSignature) {
		t.Fatal("invalid signature accepted")
	}
	if ctx.Err() != nil {
		t.Fatal("invalid signature cancelled work")
	}
	c.cancelOpen(1, 1, wire.FileID{1}, 10)
	if ctx.Err() != nil {
		t.Fatal("close cancelled its own request")
	}
	c.cancelOpen(1, 1, wire.FileID{1}, 11)
	if ctx.Err() != context.Canceled {
		t.Fatal("close failed to cancel matching work")
	}
}
func TestCancelledAsyncRequestReturnsSignedTerminal(t *testing.T) {
	c, s, out := transportFixture(t)
	r := transportRequest(t, s, wire.Echo, 10, wire.EmptyResponseBody())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.pending[10] = &pendingRequest{ctx: ctx, cancel: cancel, sessionID: 1, frame: 10, async: true}
	if err := c.process([]wire.Request{r}); err != nil {
		t.Fatal(err)
	}
	packet := out.Bytes()[4:]
	h, err := wire.ParseHeader(packet)
	if err != nil || h.Status != statusCancelled || h.Flags&wire.FlagAsync == 0 || h.AsyncID != 11 || h.Credits != 0 || s.signer.Verify(packet) != nil {
		t.Fatalf("terminal header=%+v error=%v", h, err)
	}
	if len(c.pending) != 0 {
		t.Fatal("completed request retained")
	}
}
func TestResourceRejectionPreservesCompoundSigningAndCredits(t *testing.T) {
	c, s, out := transportFixture(t)
	first := transportRequest(t, s, wire.Echo, 3, wire.EmptyResponseBody())
	second := transportRequest(t, s, wire.Echo, 4, wire.EmptyResponseBody())
	second.Header.Flags |= wire.FlagRelated
	second.Header.SessionID = math.MaxUint64
	second.Packet = requestPacket(second.Header, second.Body)
	if err := s.signer.Sign(second.Packet); err != nil {
		t.Fatal(err)
	}
	if err := c.rejectRequests([]wire.Request{first, second}); err != nil {
		t.Fatal(err)
	}
	p := out.Bytes()[4:]
	for i := 0; i < 2; i++ {
		h, err := wire.ParseHeader(p)
		if err != nil {
			t.Fatal(err)
		}
		n := len(p)
		if h.NextCommand != 0 {
			n = int(h.NextCommand)
		}
		if h.Status != statusResources || h.SessionID != s.id || h.Credits != 1 || s.signer.Verify(p[:n]) != nil {
			t.Fatalf("response%d %+v", i, h)
		}
		p = p[n:]
	}
	if len(p) != 0 {
		t.Fatal("unexpected response bytes")
	}
}
func TestTransportWritesPropagateFailures(t *testing.T) {
	sentinel := errors.New("transport failed")
	for _, tc := range []struct {
		name   string
		output capturedConnection
		want   error
	}{
		{name: "deadline", output: capturedConnection{deadlineErr: sentinel}, want: sentinel},
		{name: "prefix", output: capturedConnection{writeErr: sentinel}, want: sentinel},
		{name: "short prefix", output: capturedConnection{writeLimit: 2}, want: io.ErrShortWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, _ := transportFixture(t)
			c.net = &tc.output
			if err := c.write([]byte("payload")); !errors.Is(err, tc.want) {
				t.Fatalf("write error %v", err)
			}
		})
	}
	c, _, out := transportFixture(t)
	out.writeLimit = 4
	if err := c.write([]byte("long payload")); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(out.Bytes()[:4]) != 12 || string(out.Bytes()[4:]) != "long payload" {
		t.Fatalf("framing %x", out.Bytes())
	}
}
func transportPayload(command uint16, length uint32) wire.Request {
	var b []byte
	switch command {
	case wire.Read:
		b = make([]byte, 48)
		binary.LittleEndian.PutUint16(b, 49)
		binary.LittleEndian.PutUint32(b[4:], length)
	case wire.Write:
		b = make([]byte, 48+int(length))
		binary.LittleEndian.PutUint16(b, 49)
		binary.LittleEndian.PutUint16(b[2:], 112)
		binary.LittleEndian.PutUint32(b[4:], length)
	case wire.QueryDirectory:
		b = make([]byte, 32)
		binary.LittleEndian.PutUint16(b, 33)
		binary.LittleEndian.PutUint32(b[28:], length)
	case wire.QueryInfo:
		b = make([]byte, 40)
		binary.LittleEndian.PutUint16(b, 41)
		binary.LittleEndian.PutUint32(b[4:], length)
	case wire.ChangeNotify:
		b = make([]byte, 32)
		binary.LittleEndian.PutUint16(b, 32)
		binary.LittleEndian.PutUint32(b[4:], length)
	case wire.IOCTL:
		b = make([]byte, 56)
		binary.LittleEndian.PutUint16(b, 57)
		binary.LittleEndian.PutUint32(b[44:], length)
	}
	p := requestPacket(wire.Header{Command: command}, b)
	return wire.Request{Header: wire.Header{Command: command}, Packet: p, Body: b}
}
func TestTransportCreditPayloadBoundaries(t *testing.T) {
	for _, command := range []uint16{wire.Read, wire.Write, wire.QueryDirectory, wire.QueryInfo, wire.ChangeNotify, wire.IOCTL} {
		for _, length := range []uint32{65535, 65536, 65537, 131072} {
			r := transportPayload(command, length)
			want := int((length + 65535) / 65536)
			if got := requiredCredits(r); got != want {
				t.Fatalf("command%d length%d credits%d want%d", command, length, got, want)
			}
			if command != wire.Write && responseBudget(r) < int(length)+64 {
				t.Fatalf("command%d payload escapes response reservation", command)
			}
		}
	}
	c, _, _ := transportFixture(t)
	if got := c.grantCredits(65535); int(got) != c.server.config.Limits.MaxRequests-1 {
		t.Fatalf("capacity grant%d", got)
	}
	if got := c.grantCredits(1); got != 0 {
		t.Fatalf("over capacity grant%d", got)
	}
	c.credits = make(map[uint64]struct{})
	c.nextCredit = math.MaxUint64 - 1
	if got := c.grantCredits(2); got != 1 {
		t.Fatalf("overflow grant%d", got)
	}
}
func TestRelatedFileIdentityAndResponseReservations(t *testing.T) {
	for _, command := range []uint16{wire.Close, wire.Flush, wire.Read, wire.Write, wire.Lock, wire.IOCTL, wire.QueryDirectory, wire.ChangeNotify, wire.QueryInfo, wire.SetInfo} {
		b := make([]byte, 64)
		id := wire.FileID{3, 4}
		patchRelatedFile(command, b, id)
		got, ok := relatedFile(wire.Request{Header: wire.Header{Command: command}, Body: b})
		if !ok || got != id {
			t.Fatalf("command%d identity%v", command, got)
		}
		if _, ok := relatedFile(wire.Request{Header: wire.Header{Command: command}, Body: b[:1]}); ok {
			t.Fatalf("command%d accepted short body", command)
		}
	}
	if _, ok := relatedFile(wire.Request{Header: wire.Header{Command: wire.Echo}}); ok {
		t.Fatal("ECHO has a file identity")
	}
	for _, command := range []uint16{wire.Echo, wire.Create, wire.SessionSetup, wire.Negotiate} {
		budget := responseBudget(wire.Request{Header: wire.Header{Command: command}})
		if budget < 88 || budget&7 != 0 {
			t.Fatalf("command%d reserve%d", command, budget)
		}
	}
}
