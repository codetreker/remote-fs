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
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileIOProbe struct {
	attr                                       storage.Attr
	read                                       func(context.Context, int64, int) (storage.FileRead, error)
	mutate                                     func(context.Context, storage.FileMutation) (storage.Attr, error)
	sync                                       func(context.Context) error
	statErr, checkErr                          error
	reads, writes, truncates, stats, mutations atomic.Int32
	syncs, closes                              atomic.Int32
}

func (p *fileIOProbe) Stat(context.Context) (storage.Attr, error) {
	p.stats.Add(1)
	return p.attr.Clone(), p.statErr
}
func (p *fileIOProbe) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	p.reads.Add(1)
	return p.read(ctx, offset, length)
}
func (p *fileIOProbe) WriteAt(context.Context, int64, []byte) (storage.Attr, error) {
	p.writes.Add(1)
	return storage.Attr{}, errors.New("ordinary WriteAt must not serve SMB WRITE")
}
func (p *fileIOProbe) Truncate(context.Context, int64) (storage.Attr, error) {
	p.truncates.Add(1)
	return storage.Attr{}, errors.New("ordinary Truncate must not serve Windows length reset")
}
func (*fileIOProbe) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func (p *fileIOProbe) Sync(ctx context.Context) error {
	p.syncs.Add(1)
	if p.sync != nil {
		return p.sync(ctx)
	}
	return nil
}
func (p *fileIOProbe) Close(context.Context) error         { p.closes.Add(1); return nil }
func (p *fileIOProbe) CheckConditionalFileMutation() error { return p.checkErr }
func (p *fileIOProbe) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	p.mutations.Add(1)
	return p.mutate(ctx, command)
}

func fileIOAttr(t *testing.T, size int64, attributes uint32, version byte) storage.Attr {
	t.Helper()
	change := time.Unix(4, 0).UTC()
	data, err := encodeWindowsMetadata(windowsMetadata{Attributes: attributes})
	if err != nil {
		t.Fatal(err)
	}
	return storage.Attr{
		ID: 7, Kind: storage.NodeRegular, Size: size,
		AccessTime: time.Unix(2, 0).UTC(), ModTime: time.Unix(3, 0).UTC(), ChangeTime: &change,
		Metadata: map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{version}, Data: data}},
	}
}

func fileIOFixture(t *testing.T, file *fileIOProbe, access uint32) (*connection, *tree, wire.FileID) {
	t.Helper()
	registry := newHandleTestRegistry(t, 2)
	registry.tree.export.share.Volume = "trusted-volume"
	registry.tree.authority.actionEpoch = 9
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	reservation.attachFile(storage.OpenResult{File: file, Attr: file.attr, Outcome: storage.Opened})
	id, err := reservation.install(access, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := registry.close(context.Background()); err != nil {
			t.Errorf("close file I/O fixture: %v", err)
		}
	})
	return &connection{server: registry.tree.export.server}, registry.tree, id
}

func fileIOReadRequest(id wire.FileID, offset uint64, length, minimum uint32) wire.Request {
	packet := make([]byte, wire.HeaderSize+48)
	body := packet[wire.HeaderSize:]
	binary.LittleEndian.PutUint16(body, 49)
	binary.LittleEndian.PutUint32(body[4:], length)
	binary.LittleEndian.PutUint64(body[8:], offset)
	copy(body[16:], id[:])
	binary.LittleEndian.PutUint32(body[32:], minimum)
	return wire.Request{Header: wire.Header{Command: wire.Read}, Body: body, Packet: packet}
}

func fileIOWriteRequest(id wire.FileID, offset uint64, data []byte) wire.Request {
	packet := make([]byte, wire.HeaderSize+48+len(data))
	body := packet[wire.HeaderSize:]
	binary.LittleEndian.PutUint16(body, 49)
	if len(data) != 0 {
		binary.LittleEndian.PutUint16(body[2:], wire.HeaderSize+48)
	}
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
	for _, test := range []struct {
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
		{"at eof", 8, 4, 0, 8, "", nil, statusEndOfFile},
		{"minimum", 2, 4, 3, 8, "cd", nil, statusEndOfFile},
		{"empty before eof", 2, 4, 0, 8, "", nil, statusIO},
		{"long response", 2, 1, 0, 8, "cd", nil, statusIO},
		{"failed with bytes", 2, 4, 0, 8, "cd", syscall.EIO, statusIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := &fileIOProbe{attr: fileIOAttr(t, 10, 0, 1)}
			probe.read = func(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
				if ctx != t.Context() || offset != int64(test.offset) || length != int(test.length) {
					t.Fatal("read lost request context or range")
				}
				attr := probe.attr.Clone()
				attr.Size = test.size
				return storage.FileRead{Attr: attr, Data: []byte(test.data)}, test.failure
			}
			connection, tree, id := fileIOFixture(t, probe, fileReadData)
			body, status := connection.readHandle(t.Context(), tree, fileIOReadRequest(id, test.offset, test.length, test.minimum))
			if status != test.status {
				t.Fatalf("status = %#x, want %#x", status, test.status)
			}
			if status == statusOK {
				if len(body) != 16+len(test.data) || !bytes.Equal(body[16:], []byte(test.data)) {
					t.Fatalf("READ response = %x", body)
				}
			} else if body != nil {
				t.Fatalf("failed READ returned data: %x", body)
			}
			if probe.reads.Load() != 1 || probe.stats.Load() != 0 {
				t.Fatalf("read calls=%d stat calls=%d", probe.reads.Load(), probe.stats.Load())
			}
		})
	}
}

func TestFileWritePublishesContentTimesAndArchiveAtomically(t *testing.T) {
	for _, test := range []struct {
		name   string
		access uint32
		offset uint64
		kind   storage.FileMutationKind
	}{
		{"range", fileWriteData, 3, storage.MutateWriteAt},
		{"append only", fileAppendData, 2, storage.MutateAppend},
		{"append sentinel", fileWriteData, math.MaxUint64, storage.MutateAppend},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := fileIOAttr(t, 10, dosHidden, 1)
			probe := &fileIOProbe{attr: before}
			var captured storage.FileMutation
			probe.mutate = func(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
				if ctx != t.Context() {
					t.Fatal("mutation lost request context")
				}
				captured = command
				result := before.Clone()
				if command.Kind == storage.MutateAppend {
					result.Size += int64(len(command.Data))
				} else {
					result.Size = max(result.Size, command.Offset+int64(len(command.Data)))
				}
				result.ModTime = time.Unix(8, 0).UTC()
				change := time.Unix(9, 0).UTC()
				result.ChangeTime = &change
				payload := command.Metadata[windowsMetadataKey]
				payload.Version = []byte{2}
				result.Metadata[windowsMetadataKey] = payload
				return result, nil
			}
			connection, tree, id := fileIOFixture(t, probe, test.access)
			var operations []storage.Operation
			connection.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
				if ctx != t.Context() || request.Volume != "trusted-volume" {
					t.Fatal("authorization lost trusted context")
				}
				operations = append(operations, request.Operation)
				return nil
			})
			body, status := connection.writeHandle(t.Context(), tree, fileIOWriteRequest(id, test.offset, []byte("data")))
			if status != statusOK || len(body) != 16 || binary.LittleEndian.Uint32(body[4:]) != 4 {
				t.Fatalf("WRITE response = %x, status %#x", body, status)
			}
			if captured.Kind != test.kind || captured.Offset != map[bool]int64{true: 0, false: int64(test.offset)}[test.kind == storage.MutateAppend] ||
				!bytes.Equal(captured.Data, []byte("data")) || captured.Action == "" {
				t.Fatalf("mutation = %+v", captured)
			}
			if !bytes.Equal(captured.ExpectedMetadata[windowsMetadataKey], []byte{1}) ||
				!bytes.Equal(captured.Metadata[windowsMetadataKey].Version, []byte{1}) {
				t.Fatalf("metadata condition = %+v", captured)
			}
			metadata, err := decodeWindowsMetadata(map[string]storage.OpaquePayload{windowsMetadataKey: captured.Metadata[windowsMetadataKey]})
			if err != nil || metadata.Attributes != dosHidden|dosArchive {
				t.Fatalf("Windows metadata = %+v, %v", metadata, err)
			}
			wantOperations := []storage.Operation{storage.OpFileStat, storage.OpFileMutate, storage.OpFileWrite, storage.OpFileSetMetadata}
			if !reflect.DeepEqual(operations, wantOperations) || probe.stats.Load() != 1 || probe.mutations.Load() != 1 || probe.writes.Load() != 0 {
				t.Fatalf("operations=%v stat=%d mutate=%d write=%d", operations, probe.stats.Load(), probe.mutations.Load(), probe.writes.Load())
			}
		})
	}
}

func TestFileWriteRetriesOnlyKnownMetadataCASRefusal(t *testing.T) {
	probe := &fileIOProbe{attr: fileIOAttr(t, 4, 0, 1)}
	var actions []storage.FileActionID
	probe.mutate = func(_ context.Context, command storage.FileMutation) (storage.Attr, error) {
		actions = append(actions, command.Action)
		if len(actions) == 1 {
			probe.attr = fileIOAttr(t, 4, dosHidden, 2)
			return storage.Attr{}, storage.ErrConditionConflict
		}
		result := probe.attr.Clone()
		result.Size = 5
		result.ModTime = time.Unix(8, 0).UTC()
		change := time.Unix(9, 0).UTC()
		result.ChangeTime = &change
		payload := command.Metadata[windowsMetadataKey]
		payload.Version = []byte{3}
		result.Metadata[windowsMetadataKey] = payload
		return result, nil
	}
	connection, tree, id := fileIOFixture(t, probe, fileWriteData)
	body, status := connection.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 4, []byte("x")))
	if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != 1 || probe.stats.Load() != 2 || probe.mutations.Load() != 2 {
		t.Fatalf("retried WRITE = %x, %#x, stat=%d mutate=%d", body, status, probe.stats.Load(), probe.mutations.Load())
	}
	if actions[0] == actions[1] {
		t.Fatal("metadata CAS retry reused a completed action identity with changed input")
	}
}

func TestFileWriteHonorsCurrentReadOnlyMetadataAndBoundsCASRetries(t *testing.T) {
	readonly := &fileIOProbe{attr: fileIOAttr(t, 4, dosReadOnly, 1)}
	readonly.mutate = func(context.Context, storage.FileMutation) (storage.Attr, error) {
		t.Fatal("READONLY write reached mutation")
		return storage.Attr{}, nil
	}
	connection, tree, id := fileIOFixture(t, readonly, fileWriteData)
	if body, status := connection.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("x"))); body != nil || status != statusMediaWriteProtected {
		t.Fatalf("READONLY WRITE = %x, %#x", body, status)
	}

	conflicting := &fileIOProbe{attr: fileIOAttr(t, 4, 0, 1)}
	conflicting.mutate = func(context.Context, storage.FileMutation) (storage.Attr, error) {
		return storage.Attr{}, storage.ErrConditionConflict
	}
	connection, tree, id = fileIOFixture(t, conflicting, fileWriteData)
	if body, status := connection.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("x"))); body != nil || status != statusRetry {
		t.Fatalf("exhausted metadata CAS = %x, %#x", body, status)
	}
	if conflicting.stats.Load() != windowsMutationAttempts || conflicting.mutations.Load() != windowsMutationAttempts {
		t.Fatalf("CAS attempts: stat=%d mutation=%d", conflicting.stats.Load(), conflicting.mutations.Load())
	}
}

func TestFileWriteFailureNeverReturnsCountOrUsesOrdinaryWrite(t *testing.T) {
	for _, failure := range []error{syscall.EIO, errors.Join(storage.ErrConditionConflict, syscall.EIO), context.Canceled, syscall.EDQUOT} {
		probe := &fileIOProbe{attr: fileIOAttr(t, 4, 0, 1)}
		probe.mutate = func(context.Context, storage.FileMutation) (storage.Attr, error) {
			return fileIOAttr(t, 8, dosArchive, 2), failure
		}
		connection, tree, id := fileIOFixture(t, probe, fileWriteData)
		body, status := connection.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("data")))
		if body != nil || status != fileCommandStatus(failure) || probe.mutations.Load() != 1 || probe.writes.Load() != 0 {
			t.Fatalf("failure %v returned body=%x status=%#x mutate=%d write=%d", failure, body, status, probe.mutations.Load(), probe.writes.Load())
		}
	}
}

func TestZeroFileWriteUsesConditionalHealthCheckWithoutChangingMetadata(t *testing.T) {
	before := fileIOAttr(t, 4, dosHidden, 1)
	probe := &fileIOProbe{attr: before}
	probe.mutate = func(_ context.Context, command storage.FileMutation) (storage.Attr, error) {
		if command.Kind != storage.MutateWriteAt || command.Offset != 99 || len(command.Data) != 0 ||
			len(command.ExpectedMetadata) != 0 || len(command.Metadata) != 0 {
			t.Fatalf("zero WRITE mutation = %+v", command)
		}
		return before.Clone(), nil
	}
	connection, tree, id := fileIOFixture(t, probe, fileWriteData)
	body, status := connection.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 99, nil))
	if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != 0 || probe.mutations.Load() != 1 || probe.writes.Load() != 0 {
		t.Fatalf("zero WRITE = %x, %#x, mutations=%d writes=%d", body, status, probe.mutations.Load(), probe.writes.Load())
	}
}

func TestFileFlushMapsDirectlyToSync(t *testing.T) {
	failure := errors.New("durability check failed")
	probe := &fileIOProbe{attr: fileIOAttr(t, 4, 0, 1), sync: func(ctx context.Context) error {
		if ctx != t.Context() {
			t.Fatal("Sync lost request context")
		}
		return failure
	}}
	connection, tree, id := fileIOFixture(t, probe, fileWriteData)
	body, status := connection.flushHandle(t.Context(), tree, fileIOFlushRequest(id))
	if body != nil || status != statusIO || probe.syncs.Load() != 1 {
		t.Fatalf("failed FLUSH = %x, %#x, syncs=%d", body, status, probe.syncs.Load())
	}
	probe.sync = nil
	body, status = connection.flushHandle(t.Context(), tree, fileIOFlushRequest(id))
	if status != statusOK || !bytes.Equal(body, wire.EmptyResponseBody()) || probe.syncs.Load() != 2 {
		t.Fatalf("successful FLUSH = %x, %#x, syncs=%d", body, status, probe.syncs.Load())
	}
}

func TestFileIORejectsInvalidOrUnadmittedRequestsBeforeBackend(t *testing.T) {
	probe := &fileIOProbe{attr: fileIOAttr(t, 4, 0, 1)}
	probe.read = func(context.Context, int64, int) (storage.FileRead, error) {
		return storage.FileRead{Attr: probe.attr.Clone(), Data: []byte("x")}, nil
	}
	probe.mutate = func(context.Context, storage.FileMutation) (storage.Attr, error) {
		return probe.attr.Clone(), nil
	}
	connection, tree, id := fileIOFixture(t, probe, fileReadData|fileWriteData)

	read := fileIOReadRequest(id, 0, 1, 2)
	if body, status := connection.readHandle(t.Context(), tree, read); body != nil || status != statusInvalid {
		t.Fatalf("minimum above length = %x, %#x", body, status)
	}
	read = fileIOReadRequest(id, math.MaxUint64, 1, 0)
	if body, status := connection.readHandle(t.Context(), tree, read); body != nil || status != statusInvalid {
		t.Fatalf("negative read offset = %x, %#x", body, status)
	}
	write := fileIOWriteRequest(id, math.MaxUint64-1, []byte("x"))
	if body, status := connection.writeHandle(t.Context(), tree, write); body != nil || status != statusUnsupported {
		t.Fatalf("current-position WRITE = %x, %#x", body, status)
	}
	write = fileIOWriteRequest(id, 0, []byte("x"))
	binary.LittleEndian.PutUint32(write.Body[44:], 2)
	if body, status := connection.writeHandle(t.Context(), tree, write); body != nil || status != statusUnsupported {
		t.Fatalf("unbuffered WRITE = %x, %#x", body, status)
	}
	if probe.reads.Load() != 0 || probe.stats.Load() != 0 || probe.mutations.Load() != 0 {
		t.Fatal("invalid requests reached backend")
	}

	connection.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		return authz.ErrDenied
	})
	if body, status := connection.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("x"))); body != nil || status != statusDenied {
		t.Fatalf("denied WRITE = %x, %#x", body, status)
	}
	if probe.stats.Load() != 0 || probe.mutations.Load() != 0 {
		t.Fatal("denied WRITE reached backend")
	}

	connection.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
	tree.files.limits.MaxOpenResultBytes = maxOpenResultCharge() - 1
	if body, status := connection.readHandle(t.Context(), tree, fileIOReadRequest(id, 0, 1, 0)); body != nil || status != statusResources {
		t.Fatalf("unbudgeted READ = %x, %#x", body, status)
	}
	if probe.reads.Load() != 0 {
		t.Fatal("unbudgeted READ reached backend")
	}
	if body, status := connection.writeHandle(t.Context(), tree, fileIOWriteRequest(id, 0, []byte("x"))); body != nil || status != statusResources {
		t.Fatalf("unbudgeted WRITE = %x, %#x", body, status)
	}
	if probe.stats.Load() != 0 || probe.mutations.Load() != 0 {
		t.Fatal("unbudgeted WRITE reached backend")
	}
}

func TestWindowsLengthResetUsesTheSameAtomicArchiveMutation(t *testing.T) {
	before := fileIOAttr(t, 8, dosSystem, 1)
	probe := &fileIOProbe{attr: before}
	probe.mutate = func(_ context.Context, command storage.FileMutation) (storage.Attr, error) {
		if command.Kind != storage.MutateTruncate || command.Size != 3 || command.Action == "" ||
			!bytes.Equal(command.ExpectedMetadata[windowsMetadataKey], []byte{1}) {
			t.Fatalf("truncate mutation = %+v", command)
		}
		metadata, err := decodeWindowsMetadata(map[string]storage.OpaquePayload{windowsMetadataKey: command.Metadata[windowsMetadataKey]})
		if err != nil || metadata.Attributes != dosSystem|dosArchive {
			t.Fatalf("truncate metadata = %+v, %v", metadata, err)
		}
		result := before.Clone()
		result.Size = command.Size
		result.ModTime = time.Unix(8, 0).UTC()
		change := time.Unix(9, 0).UTC()
		result.ChangeTime = &change
		payload := command.Metadata[windowsMetadataKey]
		payload.Version = []byte{2}
		result.Metadata[windowsMetadataKey] = payload
		return result, nil
	}
	connection, tree, id := fileIOFixture(t, probe, fileWriteData)
	handle := tree.files.get(id)
	release, err := reserveFileCommandResult(t.Context(), tree, false)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	result, err := connection.mutateWindowsFile(t.Context(), tree, handle, storage.FileMutation{Kind: storage.MutateTruncate, Size: 3})
	if err != nil || result.Size != 3 || probe.truncates.Load() != 0 || probe.mutations.Load() != 1 {
		t.Fatalf("length reset = %+v, %v, truncates=%d mutations=%d", result, err, probe.truncates.Load(), probe.mutations.Load())
	}
}
