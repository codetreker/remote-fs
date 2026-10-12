//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func directoryRequestForTest(id wire.FileID, class, flags byte, pattern string, length uint32) wire.Request {
	name := wire.EncodeUTF16(pattern)
	packet := make([]byte, 64+max(33, 32+len(name)))
	body := packet[64:]
	binary.LittleEndian.PutUint16(body, 33)
	body[2], body[3] = class, flags
	copy(body[8:24], id[:])
	if len(name) != 0 {
		binary.LittleEndian.PutUint16(body[24:], 96)
		binary.LittleEndian.PutUint16(body[26:], uint16(len(name)))
		copy(body[32:], name)
	}
	binary.LittleEndian.PutUint32(body[28:], length)
	return wire.Request{Header: wire.Header{Command: wire.QueryDirectory}, Body: body, Packet: packet}
}

func openDirectoryForTest(t *testing.T, f realSMBFixture, name string) wire.FileID {
	t.Helper()
	request := createRequestForTest(name, 1, accessReadData)
	binary.LittleEndian.PutUint32(request.Body[40:], createDirectory)
	_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, request)
	if status != statusOK {
		t.Fatalf("directory CREATE %q status=%#x", name, status)
	}
	return id
}

func queryDirectoryForTest(t *testing.T, f realSMBFixture, id wire.FileID, class, flags byte, pattern string, length uint32, want uint32) []byte {
	t.Helper()
	body, status := f.connection.queryDirectory(t.Context(), f.session, f.tree, directoryRequestForTest(id, class, flags, pattern, length))
	if status != want {
		t.Fatalf("directory class=%d flags=%x pattern=%q length=%d status=%#x want=%#x body=%x", class, flags, pattern, length, status, want, body)
	}
	if status != statusOK && status != statusBufferOverflow {
		return nil
	}
	if len(body) < 8 || binary.LittleEndian.Uint16(body) != 9 || binary.LittleEndian.Uint16(body[2:]) != 72 || binary.LittleEndian.Uint32(body[4:]) != uint32(len(body)-8) {
		t.Fatalf("directory success envelope=%x", body)
	}
	return body[8:]
}

type decodedDirectoryEntryForTest struct {
	name string
	data []byte
}

func directoryHeaderForTest(class byte) int {
	return map[byte]int{1: 64, 2: 68, 3: 94, 12: 12, 37: 104, 38: 80}[class]
}

func decodeDirectoryEntriesForTest(t *testing.T, class byte, data []byte) []decodedDirectoryEntryForTest {
	t.Helper()
	header := directoryHeaderForTest(class)
	var entries []decodedDirectoryEntryForTest
	for len(data) != 0 {
		if len(data) < header || binary.LittleEndian.Uint32(data[4:]) != 0 {
			t.Fatalf("invalid directory header or nonzero virtual FileIndex=%x", data)
		}
		nameLengthOffset := 60
		if class == 12 {
			nameLengthOffset = 8
		}
		nameBytes := int(binary.LittleEndian.Uint32(data[nameLengthOffset:]))
		if nameBytes < 0 || header+nameBytes > len(data) {
			t.Fatalf("directory name extent header=%d name=%d data=%x", header, nameBytes, data)
		}
		name, err := wire.DecodeUTF16(data[header : header+nameBytes])
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, decodedDirectoryEntryForTest{name, bytes.Clone(data[:header+nameBytes])})
		next := int(binary.LittleEndian.Uint32(data))
		if next == 0 {
			if header+nameBytes != len(data) {
				t.Fatalf("last record included unreported trailing bytes=%x", data)
			}
			break
		}
		if next&7 != 0 || next < header+nameBytes || next >= len(data) || !bytes.Equal(data[header+nameBytes:next], make([]byte, next-header-nameBytes)) {
			t.Fatalf("invalid NextEntryOffset/padding=%x", data)
		}
		data = data[next:]
	}
	return entries
}

func TestDirectoryRealCaptureValidatesUnmatchedLateNames(t *testing.T) {
	for _, bad := range []string{"z-invalid.", "z-invalid ", "CON", string([]byte{'z', '-', 0xff}), "ZZ"} {
		t.Run(string([]byte(bad)), func(t *testing.T) {
			fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
				if err := f.volume.Mkdir(t.Context(), "d"); err != nil {
					t.Fatal(err)
				}
				if err := f.volume.Write(t.Context(), "d/good", []byte("original")); err != nil {
					t.Fatal(err)
				}
				id := openDirectoryForTest(t, f, "d")
				if bad == "ZZ" {
					if err := f.volume.Create(t.Context(), "d/zz"); err != nil {
						t.Fatal(err)
					}
				}
				if err := f.volume.Create(t.Context(), "d/"+bad); err != nil {
					t.Fatal(err)
				}
				_, status := f.connection.queryDirectory(t.Context(), f.session, f.tree, directoryRequestForTest(id, 12, 0, "good", 1024))
				if status == statusOK || status == statusBufferOverflow || status == 0x80000006 || status == 0xc000000f {
					t.Fatalf("unmatched invalid name %q allowed a successful/invented absence result=%#x", bad, status)
				}
				if data, err := f.volume.Read(t.Context(), "d/good"); err != nil || string(data) != "original" {
					t.Fatalf("failed observation changed valid object=%q error=%v", data, err)
				}
				if _, err := f.volume.Stat(t.Context(), "d/"+bad); err != nil {
					t.Fatalf("failed observation removed raw bad name: %v", err)
				}
				closeFixtureHandle(t, f, id)
			})
		})
	}
}

func TestDirectoryRealClassSwitchFreezesChildrenAndRestartReobserves(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Mkdir(t.Context(), "d"); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"A", "z", "é"} {
			if err := f.volume.Write(t.Context(), "d/"+name, []byte(name)); err != nil {
				t.Fatal(err)
			}
		}
		old, err := f.volume.Stat(t.Context(), "d/z")
		if err != nil {
			t.Fatal(err)
		}
		id := openDirectoryForTest(t, f, "d")
		if _, err := f.tree.findFileHandle(id).node.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("list-only directory reference gained ReadMetadata: %v", err)
		}
		first := decodeDirectoryEntriesForTest(t, 12, queryDirectoryForTest(t, f, id, 12, 2, "*", 512, statusOK))
		if len(first) != 1 || first[0].name != "A" {
			t.Fatalf("first raw-ordered name=%+v", first)
		}
		if err := f.volume.Write(t.Context(), "d/z", []byte("changed contents")); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Rename(t.Context(), "d", "renamed"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Mkdir(t.Context(), "d"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Create(t.Context(), "d/replacement"); err != nil {
			t.Fatal(err)
		}
		next := decodeDirectoryEntriesForTest(t, 37, queryDirectoryForTest(t, f, id, 37, 2, "ignored", 512, statusOK))
		if len(next) != 1 || next[0].name != "z" || binary.LittleEndian.Uint64(next[0].data[40:]) != uint64(old.Size) || binary.LittleEndian.Uint64(next[0].data[96:]) != old.ID {
			t.Fatalf("continuation recaptured children or rebound directory=%+v", next)
		}
		last := decodeDirectoryEntriesForTest(t, 1, queryDirectoryForTest(t, f, id, 1, 0, "", 512, statusOK))
		if len(last) != 1 || last[0].name != "é" {
			t.Fatalf("class switch reset cursor=%+v", last)
		}
		queryDirectoryForTest(t, f, id, 12, 0, "", 512, 0x80000006)
		restarted := decodeDirectoryEntriesForTest(t, 38, queryDirectoryForTest(t, f, id, 38, 1, "z", 512, statusOK))
		if len(restarted) != 1 || restarted[0].name != "z" || binary.LittleEndian.Uint64(restarted[0].data[40:]) != uint64(len("changed contents")) || binary.LittleEndian.Uint64(restarted[0].data[72:]) != old.ID {
			t.Fatalf("restart did not capture original renamed directory's fresh child=%+v", restarted)
		}
		reopened := decodeDirectoryEntriesForTest(t, 12, queryDirectoryForTest(t, f, id, 12, 16, "A", 512, statusOK))
		if len(reopened) != 1 || reopened[0].name != "A" {
			t.Fatalf("REOPEN did not replace expression=%+v", reopened)
		}
		closeFixtureHandle(t, f, id)
	})
}

func TestDirectoryRealNamesIgnoreRichProjectionAndClassFailurePreservesCursor(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Mkdir(t.Context(), "d"); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"a", "z-bad"} {
			if err := f.volume.Create(t.Context(), "d/"+name); err != nil {
				t.Fatal(err)
			}
		}
		bad, err := f.volume.Stat(t.Context(), "d/z-bad")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.raw.(storage.MetadataAccess).SetMetadata(t.Context(), bad.ID, windowsMetadataKey, nil, []byte("invalid")); err != nil {
			t.Fatal(err)
		}
		id := openDirectoryForTest(t, f, "d")
		first := decodeDirectoryEntriesForTest(t, 12, queryDirectoryForTest(t, f, id, 12, 2, "*", 512, statusOK))
		if len(first) != 1 || first[0].name != "a" {
			t.Fatalf("Names inherited rich metadata requirements=%+v", first)
		}
		queryDirectoryForTest(t, f, id, 1, 2, "", 512, statusIO)
		next := decodeDirectoryEntriesForTest(t, 12, queryDirectoryForTest(t, f, id, 12, 2, "", 512, statusOK))
		if len(next) != 1 || next[0].name != "z-bad" {
			t.Fatalf("failed class projection advanced the frozen cursor=%+v", next)
		}
		queryDirectoryForTest(t, f, id, 1, 1, "a", 512, statusIO)
		queryDirectoryForTest(t, f, id, 12, 1, "a", 512, statusOK)
		closeFixtureHandle(t, f, id)
	})
}

func TestDirectoryRealPartialRetriesAndSettledEmptySearch(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Mkdir(t.Context(), "d"); err != nil {
			t.Fatal(err)
		}
		name := "💾long-name.txt"
		if err := f.volume.Create(t.Context(), "d/"+name); err != nil {
			t.Fatal(err)
		}
		for _, class := range []byte{12, 1, 2, 3, 38, 37} {
			id := openDirectoryForTest(t, f, "d")
			header := directoryHeaderForTest(class)
			queryDirectoryForTest(t, f, id, class, 0, "*", uint32(header-1), statusInfoLengthMismatch)
			partial := queryDirectoryForTest(t, f, id, class, 0, "*", uint32(header+1), statusBufferOverflow)
			lengthOffset := 60
			if class == 12 {
				lengthOffset = 8
			}
			if len(partial) != header+1 || binary.LittleEndian.Uint32(partial) != 0 || binary.LittleEndian.Uint32(partial[lengthOffset:]) != uint32(len(wire.EncodeUTF16(name))) || !bytes.Equal(partial[header:], wire.EncodeUTF16(name)[:1]) {
				t.Fatalf("class=%d rounded partial name or changed full length=%x", class, partial)
			}
			full := decodeDirectoryEntriesForTest(t, class, queryDirectoryForTest(t, f, id, class, 0, "", 512, statusOK))
			if len(full) != 1 || full[0].name != name {
				t.Fatalf("partial warning lost pending first record=%+v", full)
			}
			request := directoryRequestForTest(id, class, 4, "", 512)
			binary.LittleEndian.PutUint32(request.Body[4:], ^uint32(0))
			if _, status := f.connection.queryDirectory(t.Context(), f.session, f.tree, request); status != 0x80000006 {
				t.Fatalf("INDEX_SPECIFIED changed ordinary exhausted cursor=%#x", status)
			}
			closeFixtureHandle(t, f, id)
		}
		id := openDirectoryForTest(t, f, "d")
		queryDirectoryForTest(t, f, id, 12, 0, "absent", 512, 0xc000000f)
		queryDirectoryForTest(t, f, id, 12, 0, "", 512, 0x80000006)
		queryDirectoryForTest(t, f, id, 12, 1, ".", 512, 0xc000000f)
		queryDirectoryForTest(t, f, id, 12, 16, "..", 512, 0xc000000f)
		closeFixtureHandle(t, f, id)
	})
}

func TestDirectoryRealContinuationAndEOFRequireCurrentListAuthorization(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Mkdir(t.Context(), "d"); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"a", "b"} {
			if err := f.volume.Create(t.Context(), "d/"+name); err != nil {
				t.Fatal(err)
			}
		}
		id := openDirectoryForTest(t, f, "d")
		queryDirectoryForTest(t, f, id, 12, 2, "*", 512, statusOK)
		f.session.principal = testPrincipal("0123456789abcdef")
		deny := false
		f.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
			if request.Operation == storage.OpFileReadDirNode {
				p, ok := PrincipalFromContext(ctx)
				if !ok || !p.SameIdentity(f.session.principal) {
					t.Fatal("directory page lost current authenticated principal")
				}
				if deny {
					return authz.ErrDenied
				}
			}
			return nil
		})
		deny = true
		queryDirectoryForTest(t, f, id, 12, 2, "", 512, statusDenied)
		deny = false
		next := decodeDirectoryEntriesForTest(t, 12, queryDirectoryForTest(t, f, id, 12, 2, "", 512, statusOK))
		if len(next) != 1 || next[0].name != "b" {
			t.Fatalf("denied continuation advanced cursor=%+v", next)
		}
		deny = true
		queryDirectoryForTest(t, f, id, 12, 0, "", 512, statusDenied)
		deny = false
		queryDirectoryForTest(t, f, id, 12, 0, "", 512, 0x80000006)
		closeFixtureHandle(t, f, id)
	})
}

type directoryStateFaultForTest struct {
	next  http.RoundTripper
	deny  atomic.Bool
	state atomic.Int32
}

func (g *directoryStateFaultForTest) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.GetBody != nil {
		body, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		var call struct{ Op storage.Operation }
		err = json.NewDecoder(body).Decode(&call)
		err = errors.Join(err, body.Close())
		if err != nil {
			return nil, err
		}
		if call.Op == storage.OpFileState {
			g.state.Add(1)
			if g.deny.Load() {
				return nil, syscall.ECONNREFUSED
			}
		}
	}
	return g.next.RoundTrip(request)
}

func TestDirectoryHTTPContinuationAndEOFDoNotUseSnapshotWithoutAuthorityState(t *testing.T) {
	f, transport := newDroppedWriteSMBFixture(t)
	transport.drop = false
	gate := &directoryStateFaultForTest{next: transport.next}
	transport.next = gate
	f.tree.sessionID = f.session.id
	if err := f.volume.Mkdir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		if err := f.volume.Create(t.Context(), "d/"+name); err != nil {
			t.Fatal(err)
		}
	}
	id := openDirectoryForTest(t, f, "d")
	queryDirectoryForTest(t, f, id, 12, 2, "*", 512, statusOK)
	for _, want := range []uint32{statusOK, 0x80000006} {
		before := gate.state.Load()
		gate.deny.Store(true)
		queryDirectoryForTest(t, f, id, 12, 2, "", 512, statusIO)
		if gate.state.Load() <= before {
			t.Fatal("continuation/EOF checked only local scope instead of issuing StateScalars")
		}
		gate.deny.Store(false)
		queryDirectoryForTest(t, f, id, 12, 2, "", 512, want)
	}
	closeFixtureHandle(t, f, id)
}

func TestDirectoryRealFailedRestartAndBudgetRelease(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Mkdir(t.Context(), "d"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Create(t.Context(), "d/old"); err != nil {
			t.Fatal(err)
		}
		id := openDirectoryForTest(t, f, "d")
		queryDirectoryForTest(t, f, id, 12, 2, "*", 512, statusOK)
		before := f.server.Status().RetainedDirectoryBytes
		if before <= 1024 {
			t.Fatalf("snapshot was not charged=%d", before)
		}
		if err := f.volume.Create(t.Context(), "d/z-invalid."); err != nil {
			t.Fatal(err)
		}
		_, status := f.connection.queryDirectory(t.Context(), f.session, f.tree, directoryRequestForTest(id, 12, 1, "*", 512))
		if status == statusOK || status == statusBufferOverflow {
			t.Fatalf("failed restart returned a cached successful page=%#x", status)
		}
		if got := f.server.Status().RetainedDirectoryBytes; got != 1024 {
			t.Fatalf("failed restart retained old entries or capture staging=%d", got)
		}
		if err := f.volume.Remove(t.Context(), "d/z-invalid."); err != nil {
			t.Fatal(err)
		}
		queryDirectoryForTest(t, f, id, 12, 1, "*", 512, statusOK)
		f.server.config.Limits.MaxRetainedDirectoryBytes = f.server.Status().RetainedDirectoryBytes
		second := openDirectoryForTest(t, f, "d")
		queryDirectoryForTest(t, f, second, 12, 0, "*", 512, statusResources)
		closeFixtureHandle(t, f, second)
		closeFixtureHandle(t, f, id)
		if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
			t.Fatalf("CLOSE under global saturation leaked directory state=%d", got)
		}
	})
}

type directoryCaptureGateForTest struct {
	next    http.RoundTripper
	entered chan struct{}
	release chan struct{}
}

func (g *directoryCaptureGateForTest) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.GetBody != nil {
		body, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		var call struct{ Op storage.Operation }
		err = json.NewDecoder(body).Decode(&call)
		err = errors.Join(err, body.Close())
		if err != nil {
			return nil, err
		}
		if call.Op == storage.OpFileReadDirNode {
			select {
			case <-g.entered:
			default:
				close(g.entered)
			}
			select {
			case <-g.release:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		}
	}
	return g.next.RoundTrip(request)
}

func TestDirectoryHTTPRunningCaptureRetainsChargesUntilCloseDrain(t *testing.T) {
	f, transport := newDroppedWriteSMBFixture(t)
	transport.drop = false
	gate := &directoryCaptureGateForTest{next: transport.next, entered: make(chan struct{}), release: make(chan struct{})}
	transport.next = gate
	t.Cleanup(func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
	})
	f.tree.sessionID = f.session.id
	if err := f.volume.Mkdir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	if err := f.volume.Create(t.Context(), "d/child"); err != nil {
		t.Fatal(err)
	}
	id := openDirectoryForTest(t, f, "d")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan uint32, 1)
	go func() {
		_, status := f.connection.queryDirectory(ctx, f.session, f.tree, directoryRequestForTest(id, 12, 0, "*", 512))
		result <- status
	}()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("capture did not reach the actual bounded HTTP directory request")
	}
	charged := f.server.Status().RetainedDirectoryBytes
	if charged <= 1024 {
		t.Fatalf("active HTTP capture staging was not charged=%d", charged)
	}
	closeCtx, stopClose := context.WithTimeout(ctx, 20*time.Millisecond)
	err := f.tree.closeFileHandle(closeCtx, f.tree.findFileHandle(id))
	stopClose()
	if !errors.Is(err, context.DeadlineExceeded) || f.server.Status().RetainedDirectoryBytes != charged {
		t.Fatalf("timed-out CLOSE released live capture or lost cancellation: error=%v charged=%d status=%+v", err, charged, f.server.Status())
	}
	close(gate.release)
	select {
	case status := <-result:
		if status != statusOK {
			t.Fatalf("admitted capture did not finish before native close=%#x", status)
		}
	case <-ctx.Done():
		t.Fatal("capture did not drain")
	}
	if err := f.tree.closeFileHandle(ctx, f.tree.findFileHandle(id)); err != nil {
		t.Fatal(err)
	}
	if status := f.server.Status(); status.Handles != 0 || status.RetainedDirectoryBytes != 0 {
		t.Fatalf("settled CLOSE retained directory capture=%+v", status)
	}
}
