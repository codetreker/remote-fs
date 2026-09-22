package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func directoryTestAttr(id uint64, kind storage.NodeKind, size int64) storage.Attr {
	now := time.Unix(1_700_000_000, 123_456_700).UTC()
	return storage.Attr{
		ID: id, Kind: kind, Size: size, BirthTime: &now, ChangeTime: &now,
		AccessTime: now, ModTime: now,
	}
}

func TestDirectoryEntryEncoding(t *testing.T) {
	fields, err := captureDirectoryEntry(directoryTestAttr(19, storage.NodeRegular, 513))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		class      byte
		base       int
		nameOffset int
		idOffset   int
	}{
		{fileDirectoryInformation, 64, 64, -1},
		{fileFullDirectoryInformation, 68, 68, -1},
		{fileBothDirectoryInformation, 94, 94, -1},
		{fileNamesInformation, 12, 12, -1},
		{fileIDBothDirectoryInformation, 104, 104, 96},
		{fileIDFullDirectoryInformation, 80, 80, 72},
	} {
		t.Run(string(rune(test.class+'0')), func(t *testing.T) {
			entry, err := encodeDirectoryEntry(test.class, 0, nameUnits("a😀"), fields)
			if err != nil {
				t.Fatal(err)
			}
			if len(entry) != test.base+6 {
				t.Fatalf("encoded length = %d", len(entry))
			}
			nameLengthOffset := 60
			if test.class == fileNamesInformation {
				nameLengthOffset = 8
			}
			if got := binary.LittleEndian.Uint32(entry[nameLengthOffset:]); got != 6 {
				t.Fatalf("FileNameLength = %d", got)
			}
			if got := entry[test.nameOffset : test.nameOffset+6]; string(got) != string(wire.EncodeUTF16("a😀")) {
				t.Fatalf("FileName = %x", got)
			}
			if test.idOffset >= 0 && binary.LittleEndian.Uint64(entry[test.idOffset:]) != 19 {
				t.Fatalf("FileId = %d", binary.LittleEndian.Uint64(entry[test.idOffset:]))
			}
		})
	}
	if _, err := encodeDirectoryEntry(0xff, 0, []uint16{'x'}, fields); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported class = %v", err)
	}
	symlink, err := captureDirectoryEntry(directoryTestAttr(20, storage.NodeSymlink, 7))
	if err != nil {
		t.Fatal(err)
	}
	for _, class := range []byte{fileFullDirectoryInformation, fileBothDirectoryInformation, fileIDBothDirectoryInformation, fileIDFullDirectoryInformation} {
		entry, err := encodeDirectoryEntry(class, 0, []uint16{'l'}, symlink)
		if err != nil {
			t.Fatal(err)
		}
		if got := binary.LittleEndian.Uint32(entry[64:68]); got != symlinkReparseTag {
			t.Fatalf("class %d reparse tag = %#x", class, got)
		}
	}
}

func TestDirectoryPatternMatching(t *testing.T) {
	for _, test := range []struct {
		name, pattern string
		want          bool
	}{
		{"readme", "*", true},
		{"readme", "*.*", true},
		{"Report.TXT", "*.txt", true},
		{"ab", "a?", true},
		{"a", "a?", true},
		{"file", "file.*", true},
		{"file.txt", "file.*", true},
		{"file.txt", ".txt", true},
		{"file", "f>l>", true},
		{"file.txt", `file"txt`, true},
		{"file", `file"`, true},
		{"file.txt", "<.txt", true},
		{"file.log.txt", "<.txt", true},
		{"file.log", "<.txt", false},
		{"😀", "?", false},
		{"😀", "??", true},
	} {
		matched, err := directoryPatternMatch(nameUnits(test.name), nameUnits(normalizeDirectoryPattern(test.pattern)), testNameCompare)
		if err != nil || matched != test.want {
			t.Fatalf("match %q against %q = %v, %v", test.name, test.pattern, matched, err)
		}
	}
	for _, pattern := range []string{"", "a\\b", "a/b", "a:b", "a\x00b"} {
		if err := checkDirectoryPattern(pattern); !errors.Is(err, errNameInvalid) {
			t.Fatalf("invalid pattern %q = %v", pattern, err)
		}
	}
	leadingExtension := "." + strings.Repeat("a", maxNameUnits-1)
	if err := checkDirectoryPattern(leadingExtension); err != nil {
		t.Fatalf("valid maximum-length extension pattern = %v", err)
	}
	if utf16Length(normalizeDirectoryPattern(leadingExtension)) != maxNameUnits+1 {
		t.Fatal("test did not exercise the internal wildcard prefix")
	}
}

func TestDirectorySnapshotRejectsAnyInvalidEntryBeforeSelection(t *testing.T) {
	observation := storage.DirectoryObservation{ParentID: 1, Revision: []byte("revision")}
	entries := []storage.Entry{
		{Name: "wanted", Attr: directoryTestAttr(2, storage.NodeRegular, 1)},
		{Name: "bad.", Attr: directoryTestAttr(3, storage.NodeRegular, 1)},
	}
	if _, err := validateDirectorySnapshot(t.Context(), observation, 1, entries, "wanted", testNameCompare); !errors.Is(err, errNameInvalid) {
		t.Fatalf("unmatched invalid name accepted: %v", err)
	}
	entries[1].Name = "other"
	entries[1].Attr.Metadata = map[string]storage.OpaquePayload{"bad key": {Version: []byte{1}}}
	if _, err := validateDirectorySnapshot(t.Context(), observation, 1, entries, "wanted", testNameCompare); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("unmatched invalid metadata accepted: %v", err)
	}
	entries[1] = storage.Entry{Name: "WANTED", Attr: directoryTestAttr(3, storage.NodeRegular, 1)}
	if _, err := validateDirectorySnapshot(t.Context(), observation, 1, entries, "wanted", testNameCompare); !errors.Is(err, errNameAmbiguous) {
		t.Fatalf("case ambiguity accepted: %v", err)
	}
}

type queryDirectorySession struct {
	*endpointFileSession

	mu          sync.Mutex
	observation storage.DirectoryObservation
	entries     []storage.Entry
	err         error
	failAfter   int
	calls       int
	target      storage.DirectoryTarget
	entered     chan struct{}
	release     chan struct{}
}

func (s *queryDirectorySession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	s.mu.Lock()
	s.calls++
	s.target = target
	if target.Scope != nil {
		scope := *target.Scope
		s.target.Scope = &scope
	}
	entered, release, err := s.entered, s.release, s.err
	observation := s.observation.Clone()
	entries := append([]storage.Entry(nil), s.entries...)
	s.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return storage.DirectoryObservation{}, ctx.Err()
		}
	}
	for index, entry := range entries {
		if err != nil && index == s.failAfter {
			return storage.DirectoryObservation{}, err
		}
		if err := result.Add(entry); err != nil {
			return storage.DirectoryObservation{}, err
		}
	}
	if err != nil {
		return storage.DirectoryObservation{}, err
	}
	return observation, nil
}

func (s *queryDirectorySession) readCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *queryDirectorySession) lastTarget() storage.DirectoryTarget {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.target
}

func queryDirectoryRequest(id wire.FileID, class, flags byte, pattern string, output uint32) wire.Request {
	encoded := wire.EncodeUTF16(pattern)
	body := make([]byte, 32+len(encoded))
	binary.LittleEndian.PutUint16(body, 33)
	body[2], body[3] = class, flags
	copy(body[8:24], id[:])
	if len(encoded) != 0 {
		binary.LittleEndian.PutUint16(body[24:26], wire.HeaderSize+32)
		binary.LittleEndian.PutUint16(body[26:28], uint16(len(encoded)))
		copy(body[32:], encoded)
	}
	binary.LittleEndian.PutUint32(body[28:32], output)
	packet := make([]byte, wire.HeaderSize+len(body))
	copy(packet[wire.HeaderSize:], body)
	return wire.Request{Header: wire.Header{Command: wire.QueryDirectory}, Body: packet[wire.HeaderSize:], Packet: packet}
}

func newQueryDirectoryHarness(t *testing.T, reader *queryDirectorySession, maxBytes int64) (*connection, *tree, wire.FileID, *fileHandle) {
	t.Helper()
	registry := newHandleTestRegistry(t, 1)
	registry.limits.MaxDirectoryBytes = maxBytes
	registry.tree.export.share.Volume = "volume"
	registry.tree.authority.raw = reader
	var id wire.FileID
	id[0] = 1
	handle := &fileHandle{
		registry: registry, authority: registry.tree.authority, reference: &handleReferenceProbe{},
		nodeID: 1, grantedAccess: fileReadData, scope: &storage.UseScope{Token: "scope"}, wake: make(chan struct{}),
	}
	registry.handles[id] = handle
	registry.slots = 1
	registry.tree.export.opens = 1
	return &connection{server: registry.tree.export.server}, registry.tree, id, handle
}

func directoryResponseName(t *testing.T, body []byte, class byte) string {
	t.Helper()
	base, err := directoryEntryBaseSize(class)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) < 8+base {
		t.Fatalf("response length = %d", len(body))
	}
	nameLengthOffset := 60
	if class == fileNamesInformation {
		nameLengthOffset = 8
	}
	data := body[8:]
	nameBytes := int(binary.LittleEndian.Uint32(data[nameLengthOffset:]))
	if base+nameBytes > len(data) {
		t.Fatalf("name length %d exceeds response %d", nameBytes, len(data))
	}
	name, err := wire.DecodeUTF16(data[base : base+nameBytes])
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func TestQueryDirectoryRetainsOneSnapshotAndPaginatesLocally(t *testing.T) {
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(),
		observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
		entries: []storage.Entry{
			{Name: "c", Attr: directoryTestAttr(4, storage.NodeRegular, 3)},
			{Name: "A", Attr: directoryTestAttr(2, storage.NodeRegular, 1)},
			{Name: "b", Attr: directoryTestAttr(3, storage.NodeRegular, 2)},
		},
	}
	connection, tree, id, handle := newQueryDirectoryHarness(t, reader, 1<<20)
	for index, want := range []string{"A", "b", "c"} {
		pattern := ""
		if index == 0 {
			pattern = "*"
		}
		body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
			queryDirectoryRequest(id, fileNamesInformation, queryDirectoryReturnSingleEntry, pattern, 64), testNameCompare)
		if status != statusOK || directoryResponseName(t, body, fileNamesInformation) != want {
			t.Fatalf("page %d = %q, %#x", index, directoryResponseName(t, body, fileNamesInformation), status)
		}
	}
	if body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "", 64), testNameCompare); body != nil || status != statusNoMoreFiles {
		t.Fatalf("exhausted page = %x, %#x", body, status)
	}
	if reader.readCalls() != 1 {
		t.Fatalf("backend captures = %d", reader.readCalls())
	}
	if target := reader.lastTarget(); target.NodeID != 1 || target.Scope == nil || target.Scope.Token != "scope" {
		t.Fatalf("directory target = %+v", target)
	}
	reader.mu.Lock()
	reader.entries = []storage.Entry{{Name: "replacement", Attr: directoryTestAttr(5, storage.NodeRegular, 4)}}
	reader.mu.Unlock()
	body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, queryDirectoryRestartScans|queryDirectoryReturnSingleEntry, "", 64), testNameCompare)
	if status != statusOK || directoryResponseName(t, body, fileNamesInformation) != "replacement" || reader.readCalls() != 2 {
		t.Fatalf("restart did not replace snapshot: %q, %#x, calls=%d", directoryResponseName(t, body, fileNamesInformation), status, reader.readCalls())
	}
	handle.cursorMu.Lock()
	if handle.cursor.next != 1 || !handle.cursor.started {
		t.Fatalf("cursor = %+v", handle.cursor)
	}
	handle.cursorMu.Unlock()
}

func TestQueryDirectoryPatternLifecycleAndTerminalStatuses(t *testing.T) {
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(),
		observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
		entries: []storage.Entry{
			{Name: "alpha.txt", Attr: directoryTestAttr(2, storage.NodeRegular, 1)},
			{Name: "beta.log", Attr: directoryTestAttr(3, storage.NodeRegular, 1)},
		},
	}
	connection, tree, id, _ := newQueryDirectoryHarness(t, reader, 1<<20)
	body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "*.missing", 64), testNameCompare)
	if body != nil || status != statusNoSuchFile {
		t.Fatalf("first empty match = %x, %#x", body, status)
	}
	body, status = connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "*.txt", 64), testNameCompare)
	if body != nil || status != statusNoMoreFiles {
		t.Fatalf("continuation changed pattern = %x, %#x", body, status)
	}
	body, status = connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, queryDirectoryRestartScans, "*.txt", 64), testNameCompare)
	if status != statusOK || directoryResponseName(t, body, fileNamesInformation) != "alpha.txt" {
		t.Fatalf("restart pattern = %q, %#x", directoryResponseName(t, body, fileNamesInformation), status)
	}
	body, status = connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "", 64), testNameCompare)
	if body != nil || status != statusNoMoreFiles {
		t.Fatalf("restarted exhaustion = %x, %#x", body, status)
	}
	body, status = connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, queryDirectoryRestartScans, "*.missing", 64), testNameCompare)
	if body != nil || status != statusNoMoreFiles {
		t.Fatalf("restart zero match = %x, %#x", body, status)
	}
	body, status = connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, queryDirectoryReopen, "*.missing", 64), testNameCompare)
	if body != nil || status != statusNoSuchFile {
		t.Fatalf("reopen zero match = %x, %#x", body, status)
	}
}

func TestQueryDirectoryBufferOverflowDoesNotConsumeEntry(t *testing.T) {
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(),
		observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
		entries:             []storage.Entry{{Name: "alpha", Attr: directoryTestAttr(2, storage.NodeRegular, 1)}},
	}
	connection, tree, id, handle := newQueryDirectoryHarness(t, reader, 1<<20)
	body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileIDBothDirectoryInformation, 0, "*", 106), testNameCompare)
	if status != statusBufferOverflow || body != nil {
		t.Fatalf("partial response = %x, %#x", body, status)
	}
	handle.cursorMu.Lock()
	next := handle.cursor.next
	handle.cursorMu.Unlock()
	if next != 0 {
		t.Fatalf("partial entry consumed cursor: %d", next)
	}
	body, status = connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileIDBothDirectoryInformation, 0, "", 120), testNameCompare)
	if status != statusOK || directoryResponseName(t, body, fileIDBothDirectoryInformation) != "alpha" {
		t.Fatalf("retried full entry = %q, %#x", directoryResponseName(t, body, fileIDBothDirectoryInformation), status)
	}
	if got, want := tree.files.directoryBytes, handle.cursor.byteCharge+handle.cursor.patternCharge; got != want {
		t.Fatalf("transient output charge leaked: registry=%d cursor=%d", got, want)
	}
}

func TestQueryDirectoryLinksCompleteEntriesWithinOutputBound(t *testing.T) {
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(),
		observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
		entries: []storage.Entry{
			{Name: "b", Attr: directoryTestAttr(3, storage.NodeRegular, 1)},
			{Name: "a", Attr: directoryTestAttr(2, storage.NodeRegular, 1)},
		},
	}
	connection, tree, id, _ := newQueryDirectoryHarness(t, reader, 1<<20)
	body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "*", 32), testNameCompare)
	if status != statusOK || len(body) != 38 {
		t.Fatalf("response = %d bytes, %#x", len(body), status)
	}
	data := body[8:]
	if binary.LittleEndian.Uint32(data) != 16 || binary.LittleEndian.Uint32(data[16:]) != 0 {
		t.Fatalf("entry links = %d, %d", binary.LittleEndian.Uint32(data), binary.LittleEndian.Uint32(data[16:]))
	}
	if got := directoryResponseName(t, body, fileNamesInformation); got != "a" {
		t.Fatalf("first name = %q", got)
	}
	second, err := wire.DecodeUTF16(data[28:30])
	if err != nil || second != "b" {
		t.Fatalf("second name = %q, %v", second, err)
	}
}

func TestQueryDirectoryFinalEntryDoesNotRequireAlignmentPadding(t *testing.T) {
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(),
		observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
		entries:             []storage.Entry{{Name: "a", Attr: directoryTestAttr(2, storage.NodeRegular, 1)}},
	}
	connection, tree, id, _ := newQueryDirectoryHarness(t, reader, 1<<20)
	body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "*", 14), testNameCompare)
	if status != statusOK || len(body) != 22 || directoryResponseName(t, body, fileNamesInformation) != "a" {
		t.Fatalf("exact response = %d bytes, %q, %#x", len(body), directoryResponseName(t, body, fileNamesInformation), status)
	}
}

func TestQueryDirectoryRestartReleasesOldSnapshotBeforeCapture(t *testing.T) {
	observation := storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")}
	entry := storage.Entry{Name: "a", Attr: directoryTestAttr(2, storage.NodeRegular, 1)}
	retained, err := validateDirectorySnapshot(t.Context(), observation, 1, []storage.Entry{entry}, "*", testNameCompare)
	if err != nil {
		t.Fatal(err)
	}
	snapshotBytes, err := directorySnapshotBytes(observation, retained)
	if err != nil {
		t.Fatal(err)
	}
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(), observation: observation,
		entries: []storage.Entry{entry},
	}
	// The pool has room for one snapshot and one 24-byte response body, but
	// never for two snapshots at once.
	connection, tree, id, _ := newQueryDirectoryHarness(t, reader, snapshotBytes+int64(len("*"))+24)
	for _, flags := range []byte{0, queryDirectoryRestartScans} {
		body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
			queryDirectoryRequest(id, fileNamesInformation, flags, "*", 16), testNameCompare)
		if status != statusOK || directoryResponseName(t, body, fileNamesInformation) != "a" {
			t.Fatalf("flags %#x = %q, %#x", flags, directoryResponseName(t, body, fileNamesInformation), status)
		}
	}
	if reader.readCalls() != 2 {
		t.Fatalf("captures = %d", reader.readCalls())
	}
}

func TestQueryDirectoryFailedRestartRetainsPatternOnly(t *testing.T) {
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(),
		observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
		entries: []storage.Entry{
			{Name: "a.log", Attr: directoryTestAttr(2, storage.NodeRegular, 1)},
			{Name: "b.txt", Attr: directoryTestAttr(3, storage.NodeRegular, 1)},
		},
	}
	connection, tree, id, handle := newQueryDirectoryHarness(t, reader, 1<<20)
	body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "*.txt", 64), testNameCompare)
	if status != statusOK || directoryResponseName(t, body, fileNamesInformation) != "b.txt" {
		t.Fatalf("initial query = %q, %#x", directoryResponseName(t, body, fileNamesInformation), status)
	}
	reader.mu.Lock()
	reader.err = syscall.EIO
	reader.failAfter = 0
	reader.mu.Unlock()
	if body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, queryDirectoryRestartScans, "", 64), testNameCompare); body != nil || status != statusIO {
		t.Fatalf("failed restart = %x, %#x", body, status)
	}
	handle.cursorMu.Lock()
	if handle.cursor.observation.ParentID != 0 || handle.cursor.pattern != "<.txt" {
		t.Fatalf("failed restart cursor = %+v", handle.cursor)
	}
	handle.cursorMu.Unlock()
	reader.mu.Lock()
	reader.err = nil
	reader.mu.Unlock()
	body, status = connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, queryDirectoryRestartScans, "", 64), testNameCompare)
	if status != statusOK || directoryResponseName(t, body, fileNamesInformation) != "b.txt" {
		t.Fatalf("retry lost pattern = %q, %#x", directoryResponseName(t, body, fileNamesInformation), status)
	}
}

func TestQueryDirectoryFailedFirstCaptureRetainsPattern(t *testing.T) {
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(),
		observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
		entries: []storage.Entry{
			{Name: "a.log", Attr: directoryTestAttr(2, storage.NodeRegular, 1)},
			{Name: "b.txt", Attr: directoryTestAttr(3, storage.NodeRegular, 1)},
		},
		err: syscall.EIO, failAfter: 0,
	}
	connection, tree, id, handle := newQueryDirectoryHarness(t, reader, 1<<20)
	if body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "*.txt", 64), testNameCompare); body != nil || status != statusIO {
		t.Fatalf("first failure = %x, %#x", body, status)
	}
	handle.cursorMu.Lock()
	if handle.cursor.pattern != "<.txt" || handle.cursor.observation.ParentID != 0 {
		t.Fatalf("failed first cursor = %+v", handle.cursor)
	}
	handle.cursorMu.Unlock()
	if tree.files.directoryBytes != int64(len("<.txt")) {
		t.Fatalf("retained pattern charge = %d", tree.files.directoryBytes)
	}
	reader.mu.Lock()
	reader.err = nil
	reader.mu.Unlock()
	body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "*.log", 64), testNameCompare)
	if status != statusOK || directoryResponseName(t, body, fileNamesInformation) != "b.txt" {
		t.Fatalf("retry lost first pattern = %q, %#x", directoryResponseName(t, body, fileNamesInformation), status)
	}
}

func TestQueryDirectoryEmptyResultNeedsNoOutputPool(t *testing.T) {
	observation := storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")}
	snapshotBytes, err := directorySnapshotBytes(observation, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader := &queryDirectorySession{endpointFileSession: newEndpointFileSession(), observation: observation}
	connection, tree, id, _ := newQueryDirectoryHarness(t, reader, snapshotBytes+int64(len("*")))
	body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "*", 64), testNameCompare)
	if body != nil || status != statusNoSuchFile {
		t.Fatalf("empty result = %x, %#x", body, status)
	}
	body, status = connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "", 64), testNameCompare)
	if body != nil || status != statusNoMoreFiles {
		t.Fatalf("empty continuation = %x, %#x", body, status)
	}
}

func TestQueryDirectoryShrinksPageToAvailableOutputPool(t *testing.T) {
	observation := storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")}
	entries := []storage.Entry{
		{Name: "a", Attr: directoryTestAttr(2, storage.NodeRegular, 1)},
		{Name: "b", Attr: directoryTestAttr(3, storage.NodeRegular, 1)},
	}
	retained, err := validateDirectorySnapshot(t.Context(), observation, 1, entries, "*", testNameCompare)
	if err != nil {
		t.Fatal(err)
	}
	snapshotBytes, err := directorySnapshotBytes(observation, retained)
	if err != nil {
		t.Fatal(err)
	}
	reader := &queryDirectorySession{endpointFileSession: newEndpointFileSession(), observation: observation, entries: entries}
	connection, tree, id, _ := newQueryDirectoryHarness(t, reader, snapshotBytes+int64(len("*"))+22)
	for _, want := range []string{"a", "b"} {
		body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
			queryDirectoryRequest(id, fileNamesInformation, 0, "*", 32), testNameCompare)
		if status != statusOK || directoryResponseName(t, body, fileNamesInformation) != want {
			t.Fatalf("bounded page = %q, %#x", directoryResponseName(t, body, fileNamesInformation), status)
		}
	}
}

func TestQueryDirectoryRejectsInvalidShapeBeforeBackend(t *testing.T) {
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(),
		observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
	}
	connection, tree, id, _ := newQueryDirectoryHarness(t, reader, 1<<20)
	for _, test := range []struct {
		name   string
		class  byte
		flags  byte
		output uint32
		status uint32
	}{
		{"short fixed record", fileIDBothDirectoryInformation, 0, 103, statusInfoLengthMismatch},
		{"unknown class", 0xff, 0, 4096, statusInvalidInfoClass},
		{"valid unsupported class", 0x4e, 0, 4096, statusUnsupported},
		{"unknown flag", fileNamesInformation, 0x08, 4096, statusInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
				queryDirectoryRequest(id, test.class, test.flags, "*", test.output), testNameCompare)
			if body != nil || status != test.status {
				t.Fatalf("response = %x, %#x", body, status)
			}
		})
	}
	if reader.readCalls() != 0 {
		t.Fatalf("invalid requests reached backend %d times", reader.readCalls())
	}
}

func TestQueryDirectoryCancellationDoesNotInstallSnapshot(t *testing.T) {
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(),
		observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
		entries:             []storage.Entry{{Name: "a", Attr: directoryTestAttr(2, storage.NodeRegular, 1)}},
	}
	connection, tree, id, handle := newQueryDirectoryHarness(t, reader, 1<<20)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	body, status := connection.queryDirectoryWithComparer(ctx, tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "*", 64), testNameCompare)
	if body != nil || status != statusCancelled || reader.readCalls() != 0 {
		t.Fatalf("cancelled query = %x, %#x, calls=%d", body, status, reader.readCalls())
	}
	handle.cursorMu.Lock()
	defer handle.cursorMu.Unlock()
	if handle.cursor.observation.ParentID != 0 || handle.pendingCursor != nil {
		t.Fatalf("cancelled query installed state: cursor=%+v pending=%p", handle.cursor, handle.pendingCursor)
	}
}

func TestQueryDirectoryCancellationDuringSnapshotValidation(t *testing.T) {
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(),
		observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
		entries: []storage.Entry{
			{Name: "b", Attr: directoryTestAttr(3, storage.NodeRegular, 1)},
			{Name: "a", Attr: directoryTestAttr(2, storage.NodeRegular, 1)},
		},
	}
	connection, tree, id, handle := newQueryDirectoryHarness(t, reader, 1<<20)
	ctx, cancel := context.WithCancel(t.Context())
	compare := func(left, right []uint16) (int, error) {
		cancel()
		return testNameCompare(left, right)
	}
	body, status := connection.queryDirectoryWithComparer(ctx, tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "*", 64), compare)
	if body != nil || status != statusCancelled {
		t.Fatalf("cancelled validation = %x, %#x", body, status)
	}
	handle.cursorMu.Lock()
	defer handle.cursorMu.Unlock()
	if handle.cursor.observation.ParentID != 0 || handle.cursor.pattern != "*" || !handle.cursor.started || handle.pendingCursor != nil || tree.files.directoryBytes != 1 {
		t.Fatalf("cancelled validation retained state: cursor=%+v pending=%p bytes=%d", handle.cursor, handle.pendingCursor, tree.files.directoryBytes)
	}
}

func TestQueryDirectoryFailureReleasesIncompleteCapture(t *testing.T) {
	for _, test := range []struct {
		name   string
		reader *queryDirectorySession
		max    int64
		status uint32
	}{
		{
			name: "backend after partial result",
			reader: &queryDirectorySession{
				endpointFileSession: newEndpointFileSession(),
				observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
				entries: []storage.Entry{
					{Name: "a", Attr: directoryTestAttr(2, storage.NodeRegular, 1)},
					{Name: "b", Attr: directoryTestAttr(3, storage.NodeRegular, 1)},
				},
				err: syscall.EIO, failAfter: 1,
			},
			max: 1 << 20, status: statusIO,
		},
		{
			name: "invalid unrelated name",
			reader: &queryDirectorySession{
				endpointFileSession: newEndpointFileSession(),
				observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
				entries: []storage.Entry{
					{Name: "wanted", Attr: directoryTestAttr(2, storage.NodeRegular, 1)},
					{Name: "bad.", Attr: directoryTestAttr(3, storage.NodeRegular, 1)},
				},
			},
			max: 1 << 20, status: statusObjectNameInvalid,
		},
		{
			name: "snapshot budget",
			reader: &queryDirectorySession{
				endpointFileSession: newEndpointFileSession(),
				observation:         storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
				entries:             []storage.Entry{{Name: "large-name", Attr: directoryTestAttr(2, storage.NodeRegular, 1)}},
			},
			max: 300, status: statusResources,
		},
		{
			name: "regular file",
			reader: &queryDirectorySession{
				endpointFileSession: newEndpointFileSession(),
				err:                 syscall.ENOTDIR,
			},
			max: 1 << 20, status: statusInvalid,
		},
		{
			name: "unknown failure retaining not-directory cause",
			reader: &queryDirectorySession{
				endpointFileSession: newEndpointFileSession(),
				err:                 errors.Join(syscall.EIO, syscall.ENOTDIR),
			},
			max: 1 << 20, status: statusIO,
		},
		{
			name: "classified denial retaining not-directory cause",
			reader: &queryDirectorySession{
				endpointFileSession: newEndpointFileSession(),
				err: &fileAuthorizationError{
					cause: errors.Join(errors.New("denied"), syscall.ENOTDIR), classification: syscall.EACCES,
				},
			},
			max: 1 << 20, status: statusDenied,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection, tree, id, handle := newQueryDirectoryHarness(t, test.reader, test.max)
			body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
				queryDirectoryRequest(id, fileNamesInformation, 0, "wanted", 128), testNameCompare)
			if body != nil || status != test.status {
				t.Fatalf("failure = %x, %#x", body, status)
			}
			handle.cursorMu.Lock()
			cursor, pending := handle.cursor, handle.pendingCursor
			handle.cursorMu.Unlock()
			if cursor.observation.ParentID != 0 || cursor.pattern != "wanted" || pending != nil || tree.files.directoryEntries != 0 || tree.files.directoryBytes != int64(len("wanted")) {
				t.Fatalf("failed capture retained state: cursor=%+v pending=%p entries=%d bytes=%d", cursor, pending, tree.files.directoryEntries, tree.files.directoryBytes)
			}
			if err := handle.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if tree.files.directoryBytes != 0 {
				t.Fatalf("close retained pattern charge: %d", tree.files.directoryBytes)
			}
		})
	}
}

func TestQueryDirectoryCloseWaitsForBorrowedCapture(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	reader := &queryDirectorySession{
		endpointFileSession: newEndpointFileSession(), entered: entered, release: release,
		observation: storage.DirectoryObservation{ParentID: 1, Revision: []byte("r1")},
		entries:     []storage.Entry{{Name: "a", Attr: directoryTestAttr(2, storage.NodeRegular, 1)}},
	}
	connection, tree, id, handle := newQueryDirectoryHarness(t, reader, 1<<20)
	type queryResult struct {
		body   []byte
		status uint32
	}
	queryDone := make(chan queryResult, 1)
	go func() {
		body, status := connection.queryDirectoryWithComparer(t.Context(), tree,
			queryDirectoryRequest(id, fileNamesInformation, 0, "*", 64), testNameCompare)
		queryDone <- queryResult{body: body, status: status}
	}()
	<-entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- tree.files.closeID(t.Context(), id, handle) }()
	for {
		handle.mu.Lock()
		retiring := handle.retiring
		handle.mu.Unlock()
		if retiring {
			break
		}
		runtime.Gosched()
	}
	if _, status := connection.queryDirectoryWithComparer(t.Context(), tree,
		queryDirectoryRequest(id, fileNamesInformation, 0, "", 64), testNameCompare); status != statusFileClosed {
		t.Fatalf("query admitted after close fence: %#x", status)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("close passed active query: %v", err)
	default:
	}
	close(release)
	result := <-queryDone
	if result.status != statusOK || directoryResponseName(t, result.body, fileNamesInformation) != "a" {
		t.Fatalf("in-flight query = %q, %#x", directoryResponseName(t, result.body, fileNamesInformation), result.status)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if tree.files.directoryEntries != 0 || tree.files.directoryBytes != 0 {
		t.Fatalf("close retained snapshot: entries=%d bytes=%d", tree.files.directoryEntries, tree.files.directoryBytes)
	}
}
