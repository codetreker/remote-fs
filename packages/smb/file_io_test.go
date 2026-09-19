package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileIOProbe struct {
	storage.File
	read                                           func(context.Context, int64, int) (storage.FileRead, error)
	write                                          func(context.Context, int64, []byte) (storage.Attr, error)
	mutate                                         func(context.Context, storage.FileMutation) (storage.Attr, error)
	sync                                           func(context.Context) error
	close                                          func(context.Context) error
	check                                          error
	reads, writes, mutations, syncs, stats, closes atomic.Int32
}

func (p *fileIOProbe) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	p.reads.Add(1)
	return p.read(ctx, offset, length)
}
func (p *fileIOProbe) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	p.writes.Add(1)
	return p.write(ctx, offset, data)
}
func (p *fileIOProbe) CheckConditionalFileMutation() error { return p.check }
func (p *fileIOProbe) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	p.mutations.Add(1)
	return p.mutate(ctx, command)
}
func (p *fileIOProbe) Sync(ctx context.Context) error {
	p.syncs.Add(1)
	if p.sync != nil {
		return p.sync(ctx)
	}
	return nil
}
func (p *fileIOProbe) Stat(context.Context) (storage.Attr, error) {
	p.stats.Add(1)
	return storage.Attr{}, syscall.EACCES
}
func (p *fileIOProbe) Close(ctx context.Context) error {
	p.closes.Add(1)
	if p.close != nil {
		return p.close(ctx)
	}
	return nil
}

func fileIOAttr(size int64) storage.Attr {
	return storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: size}
}

func fileIOFixture(t *testing.T, file storage.File, access uint32, ordinary bool) (*connection, *tree, wire.FileID) {
	t.Helper()
	r := handleTestRegistry()
	r.tree.export.share.Volume = "trusted-volume"
	p := handleTestReserve(t, r)
	if ordinary {
		p.attachFile(storage.OpenResult{File: file, Attr: fileIOAttr(10), Outcome: storage.Opened})
	} else {
		p.attachNode(storage.NodeOpenResult{Reference: file, Attr: fileIOAttr(10), Outcome: storage.Opened})
	}
	id, err := p.install(access, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return &connection{server: r.tree.export.server}, r.tree, id
}

func fileIOReadRequest(id wire.FileID, offset uint64, length, minimum uint32) wire.Request {
	packet := make([]byte, 112)
	body := packet[64:]
	binary.LittleEndian.PutUint16(body, 49)
	binary.LittleEndian.PutUint32(body[4:], length)
	binary.LittleEndian.PutUint64(body[8:], offset)
	copy(body[16:], id[:])
	binary.LittleEndian.PutUint32(body[32:], minimum)
	return wire.Request{Header: wire.Header{Command: wire.Read}, Body: body, Packet: packet}
}

func fileIOWriteRequest(id wire.FileID, offset uint64, data []byte) wire.Request {
	packet := make([]byte, 112+len(data))
	body := packet[64:]
	binary.LittleEndian.PutUint16(body, 49)
	binary.LittleEndian.PutUint16(body[2:], 112)
	binary.LittleEndian.PutUint32(body[4:], uint32(len(data)))
	binary.LittleEndian.PutUint64(body[8:], offset)
	copy(body[16:], id[:])
	copy(body[48:], data)
	return wire.Request{Header: wire.Header{Command: wire.Write}, Body: body, Packet: packet}
}

func fileIOFlushRequest(id wire.FileID) wire.Request {
	body := make([]byte, 24)
	binary.LittleEndian.PutUint16(body, 24)
	copy(body[8:], id[:])
	return wire.Request{Header: wire.Header{Command: wire.Flush}, Body: body}
}

func TestFileReadUsesOneCapturedRevision(t *testing.T) {
	for _, tc := range []struct {
		name            string
		offset          uint64
		length, minimum uint32
		size            int64
		data            string
		failure         error
		status          uint32
	}{
		{"current bytes", 2, 4, 0, 8, "cdef", nil, statusOK},
		{"positive short", 2, 4, 1, 8, "c", nil, statusOK},
		{"zero", 2, 0, 0, 8, "", nil, statusOK},
		{"zero beyond eof", 12, 0, 0, 8, "", nil, statusOK},
		{"at eof", 8, 4, 0, 8, "", nil, statusEndOfFile},
		{"past eof", 12, 4, 0, 8, "", nil, statusEndOfFile},
		{"minimum", 2, 4, 3, 8, "cd", nil, statusEndOfFile},
		{"zero minimum", 2, 0, 1, 8, "", nil, statusEndOfFile},
		{"empty before eof", 2, 4, 0, 8, "", nil, statusIO},
		{"long response", 2, 1, 0, 8, "cd", nil, statusIO},
		{"past captured eof", 7, 4, 0, 8, "cd", nil, statusIO},
		{"negative size", 0, 1, 0, -1, "", nil, statusIO},
		{"partial failure", 2, 4, 0, 8, "cd", syscall.EIO, statusIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &fileIOProbe{read: func(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
				if offset != int64(tc.offset) || length != int(tc.length) || ctx != t.Context() {
					t.Fatal("lost read intent or context")
				}
				return storage.FileRead{Data: []byte(tc.data), Attr: fileIOAttr(tc.size)}, tc.failure
			}}
			c, tree, id := fileIOFixture(t, probe, fileReadData, true)
			body, status := c.readHandle(t.Context(), tree, fileIOReadRequest(id, tc.offset, tc.length, tc.minimum))
			if status != tc.status {
				t.Fatalf("status %x want %x", status, tc.status)
			}
			if status == statusOK {
				if len(body) != 16+len(tc.data) || !bytes.Equal(body[16:], []byte(tc.data)) || binary.LittleEndian.Uint32(body[8:]) != 0 {
					t.Fatalf("response %x", body)
				}
			} else if body != nil {
				t.Fatalf("failure returned data %x", body)
			}
			if probe.reads.Load() != 1 || probe.stats.Load() != 0 {
				t.Fatal("read recaptured or used Stat")
			}
		})
	}
}

func TestFileWriteAcknowledgesWholeNativeOperationOnce(t *testing.T) {
	for _, tc := range []struct {
		name   string
		access uint32
		offset uint64
		data   string
		append bool
	}{
		{"range", fileWriteData, 3, "data", false},
		{"zero range", fileWriteData, 99, "", false},
		{"append only ignores offset", fileAppendData, math.MaxUint64 - 1, "data", true},
		{"append sentinel", fileWriteData, math.MaxUint64, "data", true},
		{"negative offset append", fileWriteData, math.MaxUint64 - 10, "data", true},
		{"zero append", fileAppendData, 99, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var operations []storage.Operation
			probe := &fileIOProbe{}
			probe.write = func(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
				if offset != int64(tc.offset) || string(data) != tc.data || ctx != t.Context() {
					t.Fatal("changed write or context")
				}
				if len(data) == 0 {
					return fileIOAttr(5), nil
				}
				return fileIOAttr(offset + int64(len(data))), nil
			}
			probe.mutate = func(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
				data := command.Data
				command.Data = nil
				want := storage.FileMutation{Kind: storage.MutateAppend}
				if !bytes.Equal(data, []byte(tc.data)) || !reflect.DeepEqual(command, want) || ctx != t.Context() {
					t.Fatalf("changed append: %+v", command)
				}
				return fileIOAttr(5 + int64(len(data))), nil
			}
			c, tree, id := fileIOFixture(t, probe, tc.access, true)
			c.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, r authz.AccessRequest) error {
				if r.Volume != "trusted-volume" || ctx != t.Context() {
					t.Fatal("lost authority/context")
				}
				operations = append(operations, r.Operation)
				return nil
			})
			body, status := c.writeHandle(t.Context(), tree, fileIOWriteRequest(id, tc.offset, []byte(tc.data)))
			if status != statusOK || len(body) != 16 || binary.LittleEndian.Uint32(body[4:]) != uint32(len(tc.data)) || binary.LittleEndian.Uint32(body[8:]) != 0 {
				t.Fatalf("write %x %x", body, status)
			}
			wantOps := []storage.Operation{storage.OpFileWrite}
			if tc.append {
				wantOps = append(wantOps, storage.OpFileMutate)
			}
			if !reflect.DeepEqual(operations, wantOps) || probe.stats.Load() != 0 || probe.writes.Load()+probe.mutations.Load() != 1 || (probe.mutations.Load() == 1) != tc.append {
				t.Fatalf("operations %v write%d append%d stat%d", operations, probe.writes.Load(), probe.mutations.Load(), probe.stats.Load())
			}
		})
	}
}

func TestFileWriteFailureNeverReportsCountOrRetries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status uint32
	}{
		{"unknown", syscall.EIO, statusIO},
		{"rollback mixed conflict", errors.Join(storage.ErrRangeConflict, syscall.EIO), statusIO},
		{"competition", syscall.EAGAIN, 0xc000022d},
		{"condition", storage.ErrConditionConflict, 0xc000022d},
		{"quota", syscall.EDQUOT, 0xc0000802},
		{"disk full", syscall.ENOSPC, 0xc000007f},
		{"cancel", context.Canceled, statusCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, appendOnly := range []bool{false, true} {
				probe := &fileIOProbe{write: func(context.Context, int64, []byte) (storage.Attr, error) { return fileIOAttr(100), tc.err }, mutate: func(context.Context, storage.FileMutation) (storage.Attr, error) { return fileIOAttr(100), tc.err }}
				access := fileWriteData
				if appendOnly {
					access = fileAppendData
				}
				c, tree, id := fileIOFixture(t, probe, access, true)
				body, status := c.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("data")))
				if status != tc.status || body != nil || probe.writes.Load()+probe.mutations.Load() != 1 || probe.stats.Load() != 0 {
					t.Fatalf("append%v response%x status%x calls%d/%d", appendOnly, body, status, probe.writes.Load(), probe.mutations.Load())
				}
			}
		})
	}
}

func TestFileIOAdmissionRejectsBeforeNativeEffects(t *testing.T) {
	for _, tc := range []struct {
		name     string
		command  uint16
		access   uint32
		ordinary bool
		modify   func(*wire.Request)
		status   uint32
	}{
		{"read malformed", wire.Read, fileReadData, true, func(r *wire.Request) { r.Body[0] = 0 }, statusInvalid},
		{"read unknown flag", wire.Read, fileReadData, true, func(r *wire.Request) { r.Body[3] = 4 }, statusInvalid},
		{"read unbuffered", wire.Read, fileReadData, true, func(r *wire.Request) { r.Body[3] = 1 }, statusUnsupported},
		{"read channel", wire.Read, fileReadData, true, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[36:], 1) }, statusUnsupported},
		{"read excessive", wire.Read, fileReadData, true, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[4:], math.MaxUint32) }, statusInvalid},
		{"read negative", wire.Read, fileReadData, true, func(r *wire.Request) { binary.LittleEndian.PutUint64(r.Body[8:], math.MaxUint64) }, statusInvalid},
		{"read overflow", wire.Read, fileReadData, true, func(r *wire.Request) { binary.LittleEndian.PutUint64(r.Body[8:], math.MaxInt64) }, statusInvalid},
		{"read rights", wire.Read, fileReadAttributes, true, nil, statusDenied},
		{"read directory", wire.Read, fileReadData, false, nil, statusInvalidDeviceRequest},
		{"write malformed", wire.Write, fileWriteData, true, func(r *wire.Request) { r.Body[0] = 0 }, statusInvalid},
		{"write through", wire.Write, fileWriteData, true, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[44:], 1) }, statusInvalid},
		{"write unknown flag", wire.Write, fileWriteData, true, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[44:], 4) }, statusInvalid},
		{"write unbuffered", wire.Write, fileWriteData, true, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[44:], 2) }, statusUnsupported},
		{"write channel", wire.Write, fileWriteData, true, func(r *wire.Request) { binary.LittleEndian.PutUint32(r.Body[32:], 1) }, statusUnsupported},
		{"write current position", wire.Write, fileWriteData, true, func(r *wire.Request) { binary.LittleEndian.PutUint64(r.Body[8:], math.MaxUint64-1) }, statusUnsupported},
		{"write overflow", wire.Write, fileWriteData, true, func(r *wire.Request) { binary.LittleEndian.PutUint64(r.Body[8:], math.MaxInt64) }, statusInvalid},
		{"write rights", wire.Write, fileReadData, true, nil, statusDenied},
		{"write directory", wire.Write, fileWriteData, false, nil, statusInvalidDeviceRequest},
		{"flush malformed", wire.Flush, fileWriteData, true, func(r *wire.Request) { r.Body[0] = 0 }, statusInvalid},
		{"flush rights", wire.Flush, fileReadData, true, nil, statusDenied},
		{"flush directory", wire.Flush, fileWriteData, false, nil, statusUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &fileIOProbe{}
			c, tree, id := fileIOFixture(t, probe, tc.access, tc.ordinary)
			var request wire.Request
			switch tc.command {
			case wire.Read:
				request = fileIOReadRequest(id, 0, 1, 0)
			case wire.Write:
				request = fileIOWriteRequest(id, 0, []byte("x"))
			case wire.Flush:
				request = fileIOFlushRequest(id)
			}
			if tc.modify != nil {
				tc.modify(&request)
			}
			var body []byte
			var status uint32
			switch tc.command {
			case wire.Read:
				body, status = c.readHandle(t.Context(), tree, request)
			case wire.Write:
				body, status = c.writeHandle(t.Context(), tree, request)
			case wire.Flush:
				body, status = c.flushHandle(t.Context(), tree, request)
			}
			if status != tc.status || body != nil || probe.reads.Load()+probe.writes.Load()+probe.mutations.Load()+probe.syncs.Load() != 0 {
				t.Fatalf("response %x status %x want %x", body, status, tc.status)
			}
		})
	}
}

func TestFileIORejectsUnknownCapturedFacts(t *testing.T) {
	for _, attr := range []storage.Attr{
		{ID: 8, Kind: storage.NodeRegular, Size: 10},
		{ID: 7, Kind: storage.NodeDirectory, Size: 10},
		{ID: 7, Kind: storage.NodeRegular, Size: -1},
	} {
		probe := &fileIOProbe{read: func(context.Context, int64, int) (storage.FileRead, error) {
			return storage.FileRead{Attr: attr, Data: []byte("x")}, nil
		}, write: func(context.Context, int64, []byte) (storage.Attr, error) { return attr, nil }}
		c, tree, id := fileIOFixture(t, probe, fileReadData|fileWriteData, true)
		if body, status := c.readHandle(t.Context(), tree, fileIOReadRequest(id, 0, 1, 0)); status != statusIO || body != nil {
			t.Fatalf("read accepted %+v: %x", attr, status)
		}
		if body, status := c.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("x"))); status != statusIO || body != nil {
			t.Fatalf("write accepted %+v: %x", attr, status)
		}
	}
	probe := &fileIOProbe{write: func(context.Context, int64, []byte) (storage.Attr, error) { return fileIOAttr(1), nil }}
	c, tree, id := fileIOFixture(t, probe, fileWriteData, true)
	if body, status := c.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 1, []byte("x"))); status != statusIO || body != nil {
		t.Fatal("acknowledged bytes past captured EOF")
	}
}

func TestFileIOBudgetsBeforeNativeResultAndEffects(t *testing.T) {
	for _, command := range []string{"read", "write", "append", "flush"} {
		t.Run(command, func(t *testing.T) {
			probe := &fileIOProbe{}
			access := fileReadData | fileWriteData
			if command == "append" {
				access = fileAppendData
			}
			c, tree, id := fileIOFixture(t, probe, access, true)
			charge, err := storage.MetadataRetentionBytes(storage.MaxMetadataBytes)
			if err != nil {
				t.Fatal(err)
			}
			tree.files.limits.MaxDirectoryBytes = charge + 512
			tree.files.limits.MaxOpens = 2
			held, err := tree.files.reserveResponse()
			if err != nil {
				t.Fatal(err)
			}
			held.attachNode(storage.NodeOpenResult{})
			t.Cleanup(func() {
				if err := held.finish(context.Background()); err != nil {
					t.Error(err)
				}
			})
			held.releaseResponse()
			var body []byte
			var status uint32
			switch command {
			case "read":
				body, status = c.readHandle(t.Context(), tree, fileIOReadRequest(id, 0, 1, 0))
			case "write", "append":
				body, status = c.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("x")))
			case "flush":
				body, status = c.flushHandle(t.Context(), tree, fileIOFlushRequest(id))
			}
			if body != nil || status != statusResources || probe.reads.Load()+probe.writes.Load()+probe.mutations.Load()+probe.syncs.Load() != 0 {
				t.Fatalf("pool result %x %x", body, status)
			}
			if tree.files.resultBytes != charge+512 {
				t.Fatal("failed admission changed existing reservation")
			}
			if err := held.finish(t.Context()); err != nil {
				t.Fatal(err)
			}
			if tree.files.resultBytes != 0 {
				t.Fatal("held result was not released")
			}
		})
	}
	registry := handleTestRegistry()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if release, err := reserveFileCommandResult(ctx, registry.tree, false); release != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled result admission: %v", err)
	}
	registry.retired = true
	if release, err := reserveFileCommandResult(t.Context(), registry.tree, false); release != nil || !errors.Is(err, syscall.EBADF) {
		t.Fatalf("retired result admission: %v", err)
	}
}

func TestFileIOAuthorizesEachNativeAction(t *testing.T) {
	for _, failure := range []error{authz.ErrDenied, errors.Join(authz.ErrDenied, syscall.EIO), errors.Join(authz.ErrDenied, storage.ErrUseConflict), errors.Join(authz.ErrDenied, storage.ErrRangeConflict), errors.Join(authz.ErrDenied, storage.ErrPendingDelete), syscall.EACCES, errors.New("policy backend failed")} {
		for _, command := range []string{"read", "write", "append", "flush"} {
			probe := &fileIOProbe{}
			access := fileReadData | fileWriteData
			if command == "append" {
				access = fileAppendData
			}
			c, tree, id := fileIOFixture(t, probe, access, true)
			c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return failure })
			var body []byte
			var status uint32
			switch command {
			case "read":
				body, status = c.readHandle(t.Context(), tree, fileIOReadRequest(id, 0, 1, 0))
			case "write", "append":
				body, status = c.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("x")))
			case "flush":
				body, status = c.flushHandle(t.Context(), tree, fileIOFlushRequest(id))
			}
			want := statusIO
			if errors.Is(failure, authz.ErrDenied) {
				want = statusDenied
			}
			if body != nil || status != want || probe.reads.Load()+probe.writes.Load()+probe.mutations.Load()+probe.syncs.Load() != 0 {
				t.Fatalf("%s error%v status%x", command, failure, status)
			}
		}
	}
	probe := &fileIOProbe{}
	c, tree, id := fileIOFixture(t, probe, fileAppendData, true)
	c.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, r authz.AccessRequest) error {
		if r.Operation == storage.OpFileMutate {
			return authz.ErrDenied
		}
		return nil
	})
	if body, status := c.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("x"))); body != nil || status != statusDenied || probe.mutations.Load() != 0 {
		t.Fatal("append bypassed mutation authorization")
	}
}

func TestFileFlushWaitsForNativeConfirmation(t *testing.T) {
	for _, failure := range []error{nil, syscall.EIO, context.Canceled} {
		probe := &fileIOProbe{sync: func(ctx context.Context) error {
			if ctx != t.Context() {
				t.Fatal("lost Sync context")
			}
			return failure
		}}
		c, tree, id := fileIOFixture(t, probe, fileAppendData, true)
		body, status := c.flushHandle(t.Context(), tree, fileIOFlushRequest(id))
		if status != fileCommandStatus(failure) || probe.syncs.Load() != 1 || probe.stats.Load() != 0 {
			t.Fatalf("flush status%x calls%d", status, probe.syncs.Load())
		}
		if failure == nil {
			if !bytes.Equal(body, []byte{4, 0, 0, 0}) {
				t.Fatalf("flush response%x", body)
			}
		} else if body != nil {
			t.Fatal("failed flush returned success body")
		}
	}
}

func TestFileReadBorrowLivesThroughResponseConstruction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, releaseRead, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		data := []byte("data")
		probe := &fileIOProbe{read: func(context.Context, int64, int) (storage.FileRead, error) {
			close(entered)
			<-releaseRead
			return storage.FileRead{Attr: fileIOAttr(4), Data: data}, nil
		}, close: func(context.Context) error { close(closed); return nil }}
		c, tree, id := fileIOFixture(t, probe, fileReadData, true)
		type result struct {
			body   []byte
			status uint32
		}
		readDone := make(chan result, 1)
		go func() {
			body, status := c.readHandle(t.Context(), tree, fileIOReadRequest(id, 0, 4, 0))
			readDone <- result{body, status}
		}()
		<-entered
		closeDone := make(chan error, 1)
		go func() { closeDone <- tree.files.closeID(t.Context(), id, tree.files.get(id)) }()
		<-closed
		synctest.Wait()
		charge, err := storage.MetadataRetentionBytes(storage.MaxMetadataBytes)
		if err != nil {
			t.Fatal(err)
		}
		tree.files.mu.Lock()
		held := tree.files.resultBytes
		tree.files.mu.Unlock()
		if held != charge+512 {
			t.Fatalf("active response charge %d", held)
		}
		select {
		case err := <-closeDone:
			t.Fatalf("close finished with active response: %v", err)
		default:
		}
		close(releaseRead)
		r := <-readDone
		if r.status != statusOK || string(r.body[16:]) != "data" {
			t.Fatalf("read response%+v", r)
		}
		data[0] = 'X'
		if string(r.body[16:]) != "data" {
			t.Fatal("response aliases backend payload")
		}
		if err := <-closeDone; err != nil {
			t.Fatal(err)
		}
		tree.files.mu.Lock()
		remaining := tree.files.resultBytes
		tree.files.mu.Unlock()
		if remaining != 0 {
			t.Fatalf("closed response retained %d bytes", remaining)
		}
		if probe.closes.Load() != 1 {
			t.Fatal("reference closed more than once")
		}
	})
}

func TestFileCommandPreservesIncomingProducerContext(t *testing.T) {
	checks := 0
	ctx := storage.WithAttrResultBudget(t.Context(), func(storage.Attr, int64) error { checks++; return syscall.EFBIG })
	probe := &fileIOProbe{read: func(ctx context.Context, _ int64, _ int) (storage.FileRead, error) {
		return storage.FileRead{}, storage.CheckAttrResultBudget(ctx, fileIOAttr(0), 6)
	}}
	c, tree, id := fileIOFixture(t, probe, fileReadData, true)
	if body, status := c.readHandle(ctx, tree, fileIOReadRequest(id, 0, 1, 0)); body != nil || status != statusResources || checks != 1 {
		t.Fatalf("incoming producer check calls%d status%x", checks, status)
	}
	if tree.files.resultBytes != 0 {
		t.Fatal("failed producer check retained result bytes")
	}
}

func TestFileCommandFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status uint32
	}{
		{nil, statusOK}, {authz.ErrDenied, statusIO}, {syscall.EIO, statusIO},
		{&fileAuthorizationError{cause: authz.ErrDenied, classification: syscall.EIO}, statusIO},
		{&fileAuthorizationError{cause: authz.ErrDenied, classification: syscall.EINVAL}, statusInvalid},
		{storage.ErrRangeConflict, 0xc0000054}, {storage.ErrUseConflict, 0xc0000043}, {storage.ErrPendingDelete, 0xc0000056},
		{errors.Join(storage.ErrUseConflict, syscall.EIO), statusIO}, {errors.Join(storage.ErrPendingDelete, syscall.EIO), statusIO},
		{syscall.EBADF, statusFileClosed}, {syscall.ESTALE, statusFileClosed}, {syscall.EDQUOT, 0xc0000802}, {syscall.ENOSPC, 0xc000007f},
		{syscall.EAGAIN, 0xc000022d}, {syscall.EISDIR, statusInvalidDeviceRequest}, {syscall.EROFS, 0xc00000a2},
		{syscall.EINVAL, statusInvalid}, {syscall.ENOSYS, statusUnsupported}, {syscall.ENOMEM, statusResources},
		{context.Canceled, statusCancelled}, {context.DeadlineExceeded, statusIO},
	} {
		if got := fileCommandStatus(tc.err); got != tc.status {
			t.Fatalf("%v: got%x want%x", tc.err, got, tc.status)
		}
	}
}

func TestFileAuthorizationPreservesDecisionAndCause(t *testing.T) {
	for _, cause := range []error{authz.ErrDenied, errors.Join(authz.ErrDenied, syscall.EIO), syscall.EACCES} {
		c, tree, _ := fileIOFixture(t, &fileIOProbe{}, fileReadData, true)
		c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return cause })
		err := c.authorizeFileOperation(t.Context(), tree, storage.OpFileRead)
		want := syscall.EIO
		if errors.Is(cause, authz.ErrDenied) {
			want = syscall.EACCES
		}
		if !errors.Is(err, cause) || err.Error() != cause.Error() || storage.ErrnoOf(err) != want {
			t.Fatalf("authorization cause%v result%v", cause, err)
		}
	}
}

func TestFileIOClosedReferencesAndMissingAppendCapability(t *testing.T) {
	for _, retiring := range []bool{false, true} {
		probe := &fileIOProbe{}
		c, tree, id := fileIOFixture(t, probe, fileReadData|fileWriteData, true)
		if retiring {
			h := tree.files.get(id)
			h.mu.Lock()
			h.retiring = true
			h.mu.Unlock()
		} else {
			id = wire.FileID{0xff}
		}
		for _, command := range []uint16{wire.Read, wire.Write, wire.Flush} {
			var body []byte
			var status uint32
			switch command {
			case wire.Read:
				body, status = c.readHandle(t.Context(), tree, fileIOReadRequest(id, 0, 1, 0))
			case wire.Write:
				body, status = c.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("x")))
			case wire.Flush:
				body, status = c.flushHandle(t.Context(), tree, fileIOFlushRequest(id))
			}
			if status != statusFileClosed || body != nil {
				t.Fatalf("closed command%d status%x", command, status)
			}
		}
	}
	for _, missing := range []bool{false, true} {
		probe := &fileIOProbe{check: syscall.EOPNOTSUPP}
		var file storage.File = probe
		if missing {
			file = struct{ storage.File }{probe}
		}
		c, tree, id := fileIOFixture(t, file, fileAppendData, true)
		if body, status := c.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("x"))); status != statusUnsupported || body != nil || probe.mutations.Load() != 0 {
			t.Fatal("unavailable append called backend")
		}
	}
	probe := &fileIOProbe{}
	c, tree, id := fileIOFixture(t, probe, fileWriteData, true)
	c.server.config.Limits.MaxIOBytes = 1
	if body, status := c.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("xx"))); status != statusInvalid || body != nil {
		t.Fatal("write exceeded negotiated size")
	}
}
