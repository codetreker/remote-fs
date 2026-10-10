package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func readRequestForTest(id wire.FileID, offset uint64, length, minimum uint32) wire.Request {
	packet := make([]byte, 64+49)
	body := packet[64:]
	binary.LittleEndian.PutUint16(body, 49)
	binary.LittleEndian.PutUint32(body[4:], length)
	binary.LittleEndian.PutUint64(body[8:], offset)
	copy(body[16:32], id[:])
	binary.LittleEndian.PutUint32(body[32:], minimum)
	return wire.Request{Header: wire.Header{Command: wire.Read}, Body: body, Packet: packet}
}

func writeRequestForTest(id wire.FileID, offset uint64, data []byte) wire.Request {
	packet := make([]byte, 64+max(49, 48+len(data)))
	body := packet[64:]
	binary.LittleEndian.PutUint16(body, 49)
	binary.LittleEndian.PutUint16(body[2:], 112)
	binary.LittleEndian.PutUint32(body[4:], uint32(len(data)))
	binary.LittleEndian.PutUint64(body[8:], offset)
	copy(body[16:32], id[:])
	copy(body[48:], data)
	return wire.Request{Header: wire.Header{Command: wire.Write}, Body: body, Packet: packet}
}

func flushRequestForTest(id wire.FileID) wire.Request {
	packet := make([]byte, 64+24)
	body := packet[64:]
	binary.LittleEndian.PutUint16(body, 24)
	copy(body[8:], id[:])
	return wire.Request{Header: wire.Header{Command: wire.Flush}, Body: body, Packet: packet}
}

type ioFileProbe struct {
	handleFileStub
	read         func(context.Context, int64, int) (storage.FileRead, error)
	sync         func(context.Context) error
	reads, syncs atomic.Int32
}

func (*ioFileProbe) ReferenceNodeID() (uint64, error) { return 7, nil }
func (f *ioFileProbe) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	f.reads.Add(1)
	return f.read(ctx, offset, length)
}
func (f *ioFileProbe) Sync(ctx context.Context) error {
	f.syncs.Add(1)
	return f.sync(ctx)
}

func newIOCommandFixture(t *testing.T, file *ioFileProbe, access uint32) (*connection, *session, *tree, *fileHandle) {
	t.Helper()
	server := &Server{config: endpointConfig()}
	s := &session{id: 99}
	a := &authoritySession{actionEpoch: 1, deadline: time.Now().Add(time.Minute)}
	tree := &tree{kind: volumeTree, id: 3, sessionID: s.id, authority: a, export: &Export{server: server, share: Share{Volume: "v"}}}
	if !tree.beginFileWork(s) {
		t.Fatal("fixture admission failed")
	}
	handle, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	handle.file, handle.access, handle.nodeID, handle.published = file, access, 7, true
	if access&(accessWriteData|accessAppend) != 0 {
		handle.contentEffects = windowsContentEffects()
	}
	handle.state.Store(uint32(handleLive))
	tree.endFileWork()
	return &connection{server: server}, s, tree, handle
}

func TestReadFileUsesOneCapturedRevisionAndMinimumCount(t *testing.T) {
	for _, test := range []struct {
		name            string
		offset          uint64
		length, minimum uint32
		data            string
		size            int64
		status          uint32
	}{
		{"full", 0, 4, 4, "body", 4, statusOK},
		{"short-meets-minimum", 0, 8, 3, "body", 4, statusOK},
		{"short-below-minimum", 0, 8, 5, "body", 4, 0xc0000011},
		{"minimum-exceeds-request", 0, 4, 5, "body", 4, 0xc0000011},
		{"at-eof", 4, 8, 0, "", 4, 0xc0000011},
		{"past-eof", 10, 8, 0, "", 4, 0xc0000011},
		{"zero-length", 0, 0, 0, "", 4, 0xc0000011},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := &ioFileProbe{read: func(_ context.Context, offset int64, length int) (storage.FileRead, error) {
				if offset != int64(test.offset) || length != int(test.length) {
					t.Fatalf("backing range=%d/%d", offset, length)
				}
				return storage.FileRead{Attr: storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: test.size, AllocationKnown: true}, Data: []byte(test.data)}, nil
			}}
			c, s, tree, handle := newIOCommandFixture(t, file, accessReadData)
			body, status := c.readFile(t.Context(), s, tree, readRequestForTest(handle.id, test.offset, test.length, test.minimum))
			if status != test.status || file.reads.Load() != 1 {
				t.Fatalf("status=%#x calls=%d", status, file.reads.Load())
			}
			if status == statusOK && (len(body) != 16+len(test.data) || binary.LittleEndian.Uint32(body[4:]) != uint32(len(test.data)) || string(body[16:]) != test.data) {
				t.Fatalf("read response=%x", body)
			}
		})
	}
}

func TestReadFileRejectsContradictoryCapturedFacts(t *testing.T) {
	valid := storage.FileRead{Attr: storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 4, AllocationKnown: true}, Data: []byte("body")}
	for _, test := range []struct {
		name   string
		change func(*storage.FileRead)
		err    error
	}{
		{"wrong-object", func(r *storage.FileRead) { r.Attr.ID++ }, nil},
		{"wrong-kind", func(r *storage.FileRead) { r.Attr.Kind = storage.NodeDirectory }, nil},
		{"negative-eof", func(r *storage.FileRead) { r.Attr.Size = -1 }, nil},
		{"negative-allocation", func(r *storage.FileRead) { r.Attr.AllocationSize = -1 }, nil},
		{"unmeasured-allocation", func(r *storage.FileRead) { r.Attr.AllocationKnown = false; r.Attr.AllocationSize = 1 }, nil},
		{"data-exceeds-request", func(r *storage.FileRead) { r.Data = []byte("larger") }, nil},
		{"data-exceeds-captured-eof", func(r *storage.FileRead) { r.Attr.Size = 2 }, nil},
		{"zero-before-eof", func(r *storage.FileRead) { r.Data = nil }, nil},
		{"partial-with-error", func(*storage.FileRead) {}, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := valid
			test.change(&result)
			file := &ioFileProbe{read: func(context.Context, int64, int) (storage.FileRead, error) { return result, test.err }}
			c, s, tree, handle := newIOCommandFixture(t, file, accessReadData)
			body, status := c.readFile(t.Context(), s, tree, readRequestForTest(handle.id, 0, 4, 0))
			if status != statusIO || len(body) != 0 || file.reads.Load() != 1 {
				t.Fatalf("invalid captured facts yielded status=%#x response=%x calls=%d", status, body, file.reads.Load())
			}
		})
	}
}

func TestFileIORefusesUnsupportedRequestsBeforeBacking(t *testing.T) {
	for _, test := range []struct {
		name    string
		command uint16
		access  uint32
		change  func(*wire.Request)
		status  uint32
	}{
		{"read-without-data-right", wire.Read, accessReadAttr, func(*wire.Request) {}, statusDenied},
		{"read-negative-offset", wire.Read, accessReadData, func(r *wire.Request) { binary.LittleEndian.PutUint64(r.Body[8:], math.MaxUint64) }, statusInvalid},
		{"read-overflow", wire.Read, accessReadData, func(r *wire.Request) { binary.LittleEndian.PutUint64(r.Body[8:], math.MaxInt64) }, statusInvalid},
		{"read-unbuffered", wire.Read, accessReadData, func(r *wire.Request) { r.Body[3] = wire.ReadFlagUnbuffered }, statusUnsupported},
		{"read-unknown-flag", wire.Read, accessReadData, func(r *wire.Request) { r.Body[3] = 0x80 }, statusUnsupported},
		{"read-rdma", wire.Read, accessReadData, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[36:], wire.ChannelRDMAV1) }, statusInvalid},
		{"write-without-data-right", wire.Write, accessWriteAttr, func(*wire.Request) {}, statusDenied},
		{"write-negative-offset", wire.Write, accessWriteData, func(r *wire.Request) { binary.LittleEndian.PutUint64(r.Body[8:], math.MaxUint64-1) }, statusInvalid},
		{"write-overflow", wire.Write, accessWriteData, func(r *wire.Request) { binary.LittleEndian.PutUint64(r.Body[8:], math.MaxInt64) }, statusInvalid},
		{"write-unbuffered", wire.Write, accessWriteData, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[44:], wire.WriteFlagUnbuffered) }, statusUnsupported},
		{"write-through-only", wire.Write, accessWriteData, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[44:], wire.WriteFlagWriteThrough) }, statusInvalid},
		{"write-unknown-flag", wire.Write, accessWriteData, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[44:], 0x80) }, statusUnsupported},
		{"write-rdma", wire.Write, accessWriteData, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[32:], wire.ChannelRDMAV1) }, statusInvalid},
		{"flush-without-write-right", wire.Flush, accessReadData, func(*wire.Request) {}, statusDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := &ioFileProbe{read: func(context.Context, int64, int) (storage.FileRead, error) {
				t.Fatal("rejected READ dispatched")
				return storage.FileRead{}, nil
			}, sync: func(context.Context) error { t.Fatal("rejected FLUSH dispatched"); return nil }}
			c, s, tree, handle := newIOCommandFixture(t, file, test.access)
			var request wire.Request
			var status uint32
			switch test.command {
			case wire.Read:
				request = readRequestForTest(handle.id, 0, 4, 0)
			case wire.Write:
				request = writeRequestForTest(handle.id, 0, []byte("body"))
			case wire.Flush:
				request = flushRequestForTest(handle.id)
			}
			test.change(&request)
			switch test.command {
			case wire.Read:
				_, status = c.readFile(t.Context(), s, tree, request)
			case wire.Write:
				_, status = c.writeFile(t.Context(), s, tree, request)
			case wire.Flush:
				_, status = c.flushFile(t.Context(), s, tree, request)
			}
			if status != test.status || file.reads.Load() != 0 || file.syncs.Load() != 0 {
				t.Fatalf("status=%#x want=%#x reads=%d syncs=%d", status, test.status, file.reads.Load(), file.syncs.Load())
			}
		})
	}
}

func TestFlushFileConfirmsBackingAndReportsErrors(t *testing.T) {
	for _, err := range []error{nil, syscall.EIO} {
		file := &ioFileProbe{sync: func(context.Context) error { return err }}
		c, s, tree, handle := newIOCommandFixture(t, file, accessAppend)
		ctx, finalize := ioResponseContextForTest(t.Context())
		body, status := c.flushFile(ctx, s, tree, flushRequestForTest(handle.id))
		finalize()
		if file.syncs.Load() != 1 || status != statusError(err) {
			t.Fatalf("sync calls=%d status=%#x err=%v", file.syncs.Load(), status, err)
		}
		if err == nil && (len(body) != 4 || binary.LittleEndian.Uint16(body) != 4) {
			t.Fatalf("FLUSH response=%x", body)
		}
	}
}

func TestFileIOChecksCurrentAuthorizationBeforeDispatch(t *testing.T) {
	file := &ioFileProbe{read: func(context.Context, int64, int) (storage.FileRead, error) {
		t.Fatal("denied read dispatched")
		return storage.FileRead{}, nil
	}, sync: func(context.Context) error { t.Fatal("denied flush dispatched"); return nil }}
	c, s, tree, handle := newIOCommandFixture(t, file, accessReadData|accessWriteData)
	c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
	for _, request := range []wire.Request{readRequestForTest(handle.id, 0, 4, 0), writeRequestForTest(handle.id, 0, []byte("body")), flushRequestForTest(handle.id)} {
		var status uint32
		switch request.Header.Command {
		case wire.Read:
			_, status = c.readFile(t.Context(), s, tree, request)
		case wire.Write:
			_, status = c.writeFile(t.Context(), s, tree, request)
		case wire.Flush:
			_, status = c.flushFile(t.Context(), s, tree, request)
		}
		if status != statusDenied {
			t.Fatalf("command=%d denied status=%#x", request.Header.Command, status)
		}
	}
}

func TestFileIOReauthorizesQueuedWorkBeforeBackingDispatch(t *testing.T) {
	for _, command := range []uint16{wire.Read, wire.Write, wire.Flush} {
		t.Run(fmt.Sprint(command), func(t *testing.T) {
			file := &ioFileProbe{read: func(context.Context, int64, int) (storage.FileRead, error) {
				t.Error("queued READ dispatched after policy revocation")
				return storage.FileRead{}, nil
			}, sync: func(context.Context) error {
				t.Error("queued FLUSH dispatched after policy revocation")
				return nil
			}}
			c, s, tree, handle := newIOCommandFixture(t, file, accessReadData|accessWriteData)
			first, err := tree.admitFileIO(t.Context(), s, handle.id, c.server.config.Limits.MaxHandleIORequests)
			if err != nil {
				t.Fatal(err)
			}
			defer first.finish()
			var checks atomic.Int32
			c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
				if checks.Add(1) > 1 {
					return authz.ErrDenied
				}
				return nil
			})
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			result := make(chan uint32, 1)
			go func() {
				var status uint32
				switch command {
				case wire.Read:
					_, status = c.readFile(ctx, s, tree, readRequestForTest(handle.id, 0, 4, 0))
				case wire.Write:
					_, status = c.writeFile(ctx, s, tree, writeRequestForTest(handle.id, 0, nil))
				case wire.Flush:
					_, status = c.flushFile(ctx, s, tree, flushRequestForTest(handle.id))
				}
				result <- status
			}()
			for {
				handle.ioMu.Lock()
				queued := len(handle.ioQueue) != 0
				handle.ioMu.Unlock()
				if queued {
					break
				}
				select {
				case status := <-result:
					t.Fatalf("request finished before enqueue: %#x", status)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				default:
					runtime.Gosched()
				}
			}
			first.finish()
			select {
			case status := <-result:
				if status != statusDenied || checks.Load() != 2 || file.reads.Load() != 0 || file.syncs.Load() != 0 {
					t.Fatalf("queued status=%#x auth=%d reads=%d syncs=%d", status, checks.Load(), file.reads.Load(), file.syncs.Load())
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestReadFileChannelNoneAndCompressionUseOrdinaryCapturedRead(t *testing.T) {
	file := &ioFileProbe{read: func(context.Context, int64, int) (storage.FileRead, error) {
		return storage.FileRead{Attr: storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 4, AllocationKnown: true}, Data: []byte("body")}, nil
	}}
	c, s, tree, handle := newIOCommandFixture(t, file, accessReadData)
	request := readRequestForTest(handle.id, 0, 4, 0)
	request.Body[2], request.Body[3] = 0xff, wire.ReadFlagRequestCompressed
	binary.LittleEndian.PutUint32(request.Body[40:], math.MaxUint32)
	binary.LittleEndian.PutUint16(request.Body[44:], math.MaxUint16)
	binary.LittleEndian.PutUint16(request.Body[46:], math.MaxUint16)
	body, status := c.readFile(t.Context(), s, tree, request)
	if status != statusOK || string(body[16:]) != "body" || file.reads.Load() != 1 {
		t.Fatalf("ordinary compressed request status=%#x body=%x calls=%d", status, body, file.reads.Load())
	}
}

func ioResponseContextForTest(ctx context.Context) (context.Context, func()) {
	var records []func(ResponseDisposition)
	return context.WithValue(ctx, ioResponseKey{}, &records), func() {
		for _, record := range records {
			record(ResponseSent)
		}
	}
}

func TestFileIOQuotaStatusPreservesUnknownAndDiskFullDistinction(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want uint32
	}{
		{"quota", syscall.EDQUOT, statusQuotaExceeded},
		{"disk-full", syscall.ENOSPC, statusError(syscall.ENOSPC)},
		{"unknown", syscall.EIO, statusIO},
		{"quota-with-unknown", errors.Join(syscall.EDQUOT, syscall.EIO), statusIO},
		{"unknown-with-quota", errors.Join(syscall.EIO, syscall.EDQUOT), statusIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ioStatusError(test.err); got != test.want {
				t.Fatalf("I/O error %v status=%#x want=%#x", test.err, got, test.want)
			}
		})
	}
	if statusQuotaExceeded == statusIO || statusQuotaExceeded == statusError(syscall.ENOSPC) {
		t.Fatal("quota status merged with unknown or disk-full status")
	}
}
