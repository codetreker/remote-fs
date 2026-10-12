package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type directoryCommandNode struct {
	storage.NodeReference
	state                  storage.ReferenceState
	stateErr, scopeErr     error
	stateCalls, scopeCalls int
	afterState             func()
}

func (*directoryCommandNode) ReferenceNodeID() (uint64, error) { return 7, nil }
func (*directoryCommandNode) CheckReferenceState() error       { return nil }
func (*directoryCommandNode) CheckScopedReference() error      { return nil }
func (n *directoryCommandNode) State(ctx context.Context, projection storage.StateProjection) (storage.ReferenceState, error) {
	n.stateCalls++
	if projection != storage.StateScalars {
		return storage.ReferenceState{}, syscall.EBADF
	}
	if n.afterState != nil {
		n.afterState()
	}
	if err := ctx.Err(); err != nil {
		return storage.ReferenceState{}, err
	}
	return n.state, n.stateErr
}
func (n *directoryCommandNode) Scope(context.Context) (storage.UseScope, error) {
	n.scopeCalls++
	return directorySnapshotScope, n.scopeErr
}

type directoryCommandFixture struct {
	c        *connection
	s        *session
	tree     *tree
	h        *fileHandle
	node     *directoryCommandNode
	source   *directorySnapshotTestSource
	captures int
}

func newDirectoryCommandFixture(t *testing.T) *directoryCommandFixture {
	t.Helper()
	c, s, tree, h := newInfoCommandFixture(t, validInfoFileProbe(), accessReadData)
	node := &directoryCommandNode{state: storage.ReferenceState{Attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory, Size: -1}}}
	h.node, h.file, h.nodeKind = node, nil, storage.NodeDirectory
	source := &directorySnapshotTestSource{unit: 4096, observation: storage.DirectoryObservation{ParentID: 7, Revision: []byte("snapshot")},
		entries: []storage.Entry{directoryTestEntry("a", 8, storage.NodeRegular), directoryTestEntry("bbbb", 9, storage.NodeRegular)}}
	tree.authority.raw = source
	c.server.nameComparer = testNameCompare
	f := &directoryCommandFixture{c: c, s: s, tree: tree, h: h, node: node, source: source}
	source.before = func(target storage.DirectoryTarget, _ *storage.ListResult) error {
		f.captures++
		if target.NodeID != h.nodeID || target.Scope == nil || *target.Scope != directorySnapshotScope {
			t.Fatalf("query substituted opened directory: %+v", target)
		}
		return nil
	}
	t.Cleanup(h.releaseDirectoryEnumeration)
	return f
}

func directoryCommandRequest(id wire.FileID, class, flags byte, pattern string, output uint32) wire.Request {
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
	binary.LittleEndian.PutUint32(body[28:], output)
	return wire.Request{Header: wire.Header{Command: wire.QueryDirectory}, Body: body, Packet: packet}
}

func (f *directoryCommandFixture) query(ctx context.Context, class, flags byte, pattern string, output uint32) ([]byte, uint32) {
	return f.c.queryDirectory(ctx, f.s, f.tree, directoryCommandRequest(f.h.id, class, flags, pattern, output))
}

func TestDirectoryCommandPartialRetryAndClassIndependentCursor(t *testing.T) {
	f := newDirectoryCommandFixture(t)
	body, status := f.query(t.Context(), wire.FileNamesInformation, 0, "*", 13)
	if status != statusBufferOverflow || len(body) != 21 || binary.LittleEndian.Uint32(body[16:]) != 2 || f.h.directory.next != 0 || f.h.directory.returned || f.h.directory.settled {
		t.Fatalf("partial advanced cursor: status=%#x body=%x cursor=%+v", status, body, f.h.directory)
	}
	body, status = f.query(t.Context(), wire.FileNamesInformation, wire.QueryDirectoryReturnSingleEntry, "ignored", 32)
	if status != statusOK || len(body) != 22 || f.h.directory.next != 1 || !f.h.directory.returned {
		t.Fatalf("partial retry=%#x %x cursor=%+v", status, body, f.h.directory)
	}
	body, status = f.query(t.Context(), wire.FileIdBothDirectoryInformation, wire.QueryDirectoryReturnSingleEntry, "ignored", 128)
	if status != statusOK || binary.LittleEndian.Uint64(body[104:]) != 9 || f.h.directory.next != 2 {
		t.Fatalf("class change reset cursor: status=%#x body=%x cursor=%+v", status, body, f.h.directory)
	}
	for i := 0; i < 2; i++ {
		if body, status := f.query(t.Context(), wire.FileNamesInformation, 0, "ignored", 32); status != statusNoMoreFiles || body != nil {
			t.Fatalf("EOF=%#x %x", status, body)
		}
	}
	if f.captures != 1 || f.node.scopeCalls != 1 || f.node.stateCalls != 5 {
		t.Fatalf("continuation did not retain capture and refresh health: captures=%d scopes=%d states=%d", f.captures, f.node.scopeCalls, f.node.stateCalls)
	}
}

func TestDirectoryCommandShortHeaderPreservesEnumerationAndRechecksHealth(t *testing.T) {
	f := newDirectoryCommandFixture(t)
	if _, status := f.query(t.Context(), wire.FileNamesInformation, 0, "*", 11); status != statusInfoLengthMismatch || f.h.directory != nil || f.captures != 0 || f.node.stateCalls != 1 {
		t.Fatalf("initial short query changed state: %#x cursor=%+v captures=%d states=%d", status, f.h.directory, f.captures, f.node.stateCalls)
	}
	if _, status := f.query(t.Context(), wire.FileNamesInformation, wire.QueryDirectoryReturnSingleEntry, "*", 32); status != statusOK {
		t.Fatalf("initial page=%#x", status)
	}
	before := *f.h.directory
	if body, status := f.query(t.Context(), wire.FileBothDirectoryInformation, wire.QueryDirectoryRestartScans, "new*", 93); status != statusInfoLengthMismatch || !bytes.Equal(body, wire.InfoLengthMismatchErrorBody()) || !reflect.DeepEqual(before, *f.h.directory) || f.captures != 1 {
		t.Fatalf("short restart changed enumeration: %#x body=%x before=%+v after=%+v", status, body, before, *f.h.directory)
	}
	f.node.stateErr = syscall.EIO
	if _, status := f.query(t.Context(), wire.FileNamesInformation, 0, "", 11); status != statusIO || !reflect.DeepEqual(before, *f.h.directory) {
		t.Fatalf("length error hid failed health: %#x", status)
	}
}

func TestDirectoryCommandEmptySearchSettlesAndRestartReplacesExpression(t *testing.T) {
	f := newDirectoryCommandFixture(t)
	if _, status := f.query(t.Context(), wire.FileNamesInformation, 0, "none", 32); status != statusNoSuchFile || f.h.directory.returned || !f.h.directory.settled {
		t.Fatalf("initial empty result=%#x cursor=%+v", status, f.h.directory)
	}
	if _, status := f.query(t.Context(), wire.FileNamesInformation, 0, "*", 32); status != statusNoMoreFiles || f.captures != 1 {
		t.Fatalf("ordinary expression reset empty search: %#x captures=%d", status, f.captures)
	}
	for _, flags := range []byte{wire.QueryDirectoryRestartScans, wire.QueryDirectoryReopen} {
		if _, status := f.query(t.Context(), wire.FileNamesInformation, flags, "a", 32); status != statusOK || !reflect.DeepEqual(f.h.directory.pattern, nameUnits("a")) {
			t.Fatalf("flags %x did not replace expression: %#x %v", flags, status, f.h.directory.pattern)
		}
		if _, status := f.query(t.Context(), wire.FileNamesInformation, flags, "", 32); status != statusOK || !reflect.DeepEqual(f.h.directory.pattern, nameUnits("a")) {
			t.Fatalf("flags %x did not retain absent expression: %#x %v", flags, status, f.h.directory.pattern)
		}
	}
}

func TestDirectoryCommandFinalAuthorizationNeverCommitsProgress(t *testing.T) {
	f := newDirectoryCommandFixture(t)
	if _, status := f.query(t.Context(), wire.FileNamesInformation, wire.QueryDirectoryReturnSingleEntry, "*", 32); status != statusOK {
		t.Fatalf("first page=%#x", status)
	}
	before := *f.h.directory
	f.node.afterState = func() {
		f.c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
	}
	if body, status := f.query(t.Context(), wire.FileNamesInformation, 0, "", 32); status != statusDenied || body != nil || !reflect.DeepEqual(before, *f.h.directory) {
		t.Fatalf("denied page committed progress: %#x %x before=%+v after=%+v", status, body, before, *f.h.directory)
	}
	f.node.afterState = nil
	f.c.server.config.Authorize = endpointConfig().Authorize
	if _, status := f.query(t.Context(), wire.FileNamesInformation, 0, "", 32); status != statusOK || f.h.directory.next != 2 {
		t.Fatalf("denied page was lost: %#x cursor=%+v", status, f.h.directory)
	}
	before = *f.h.directory
	f.node.afterState = func() {
		f.c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
	}
	if _, status := f.query(t.Context(), wire.FileNamesInformation, 0, "", 32); status != statusDenied || !reflect.DeepEqual(before, *f.h.directory) {
		t.Fatalf("denied EOF settled: %#x before=%+v after=%+v", status, before, *f.h.directory)
	}
}

func TestDirectoryCommandRichValidationPrecedesFilteredPage(t *testing.T) {
	f := newDirectoryCommandFixture(t)
	f.source.entries[1].Attr.AllocationKnown = false
	f.source.entries[1].Attr.AllocationSize = 0
	if _, status := f.query(t.Context(), wire.FileNamesInformation, 0, "a", 32); status != statusOK {
		t.Fatalf("Names depended on missing child allocation: %#x", status)
	}
	before := *f.h.directory
	if body, status := f.query(t.Context(), wire.FileDirectoryInformation, 0, "", 128); status != statusIO || body != nil || !reflect.DeepEqual(before, *f.h.directory) {
		t.Fatalf("filtered invalid rich fields hidden by EOF: %#x %x cursor=%+v", status, body, f.h.directory)
	}
}

func TestDirectoryCommandRestartScopeFailureHasNoOldFallback(t *testing.T) {
	f := newDirectoryCommandFixture(t)
	if _, status := f.query(t.Context(), wire.FileNamesInformation, wire.QueryDirectoryReturnSingleEntry, "*", 32); status != statusOK {
		t.Fatalf("first page=%#x", status)
	}
	f.node.scopeErr = syscall.EIO
	if body, status := f.query(t.Context(), wire.FileNamesInformation, wire.QueryDirectoryRestartScans, "bbbb", 32); status != statusIO || body != nil || f.h.directory.snapshot != nil || !reflect.DeepEqual(f.h.directory.pattern, nameUnits("bbbb")) {
		t.Fatalf("failed restart kept old capture: %#x %x cursor=%+v", status, body, f.h.directory)
	}
	if got := f.c.server.Status().RetainedDirectoryBytes; got != directoryCursorBytes {
		t.Fatalf("failed restart charge=%d", got)
	}
	f.node.scopeErr = nil
	body, status := f.query(t.Context(), wire.FileNamesInformation, 0, "*", 32)
	name, err := wire.DecodeUTF16(body[20:])
	if status != statusOK || err != nil || name != "bbbb" || f.captures != 2 {
		t.Fatalf("retry did not retain replacement expression: %#x %x %v captures=%d", status, body, err, f.captures)
	}
}

func TestDirectoryCommandRejectsInvalidAndUnauthorizedWork(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*directoryCommandFixture, *wire.Request)
		want  uint32
	}{
		{"malformed", func(_ *directoryCommandFixture, r *wire.Request) { r.Body = r.Body[:31] }, statusInvalid},
		{"bad-flags", func(_ *directoryCommandFixture, r *wire.Request) { r.Body[3] = 8 }, statusInvalid},
		{"unknown-class", func(_ *directoryCommandFixture, r *wire.Request) { r.Body[2] = 255 }, statusInvalidInfoClass},
		{"unsupported-class", func(_ *directoryCommandFixture, r *wire.Request) { r.Body[2] = 0x3c }, statusUnsupported},
		{"output-too-large", func(f *directoryCommandFixture, r *wire.Request) {
			binary.LittleEndian.PutUint32(r.Body[28:], uint32(f.c.server.config.Limits.MaxIOBytes+1))
		}, statusInvalid},
		{"invalid-id", func(_ *directoryCommandFixture, r *wire.Request) { clear(r.Body[8:24]) }, statusInvalidHandle},
		{"other-id", func(_ *directoryCommandFixture, r *wire.Request) { r.Body[8] ^= 1 }, statusInvalidHandle},
		{"non-directory", func(f *directoryCommandFixture, _ *wire.Request) { f.h.nodeKind = storage.NodeRegular }, statusInvalid},
		{"no-list-right", func(f *directoryCommandFixture, _ *wire.Request) { f.h.access = accessReadAttr }, statusDenied},
		{"policy-denied", func(f *directoryCommandFixture, _ *wire.Request) {
			f.c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
		}, statusDenied},
		{"health-failed", func(f *directoryCommandFixture, _ *wire.Request) { f.node.stateErr = syscall.EIO }, statusIO},
		{"health-wrong-object", func(f *directoryCommandFixture, _ *wire.Request) { f.node.state.Attr.ID++ }, statusIO},
		{"health-wrong-kind", func(f *directoryCommandFixture, _ *wire.Request) { f.node.state.Attr.Kind = storage.NodeRegular }, statusIO},
		{"invalid-pattern", func(f *directoryCommandFixture, r *wire.Request) {
			*r = directoryCommandRequest(f.h.id, 12, 0, "bad/path", 32)
		}, statusInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newDirectoryCommandFixture(t)
			request := directoryCommandRequest(f.h.id, 12, 0, "*", 32)
			test.setup(f, &request)
			if body, status := f.c.queryDirectory(t.Context(), f.s, f.tree, request); status != test.want || body != nil || f.captures != 0 {
				t.Fatalf("invalid request status=%#x body=%x captures=%d", status, body, f.captures)
			}
		})
	}
}

func TestDirectoryCommandIndexIgnoredAndBudgetsBoundCompleteOutput(t *testing.T) {
	f := newDirectoryCommandFixture(t)
	request := directoryCommandRequest(f.h.id, 12, wire.QueryDirectoryIndexSpecified|wire.QueryDirectoryReturnSingleEntry, "*", 32)
	binary.LittleEndian.PutUint32(request.Body[4:], math.MaxUint32)
	if body, status := f.c.queryDirectory(t.Context(), f.s, f.tree, request); status != statusOK || binary.LittleEndian.Uint32(body[12:]) != 0 || f.h.directory.next != 1 {
		t.Fatalf("INDEX changed ordinary cursor: %#x %x cursor=%+v", status, body, f.h.directory)
	}
	for _, length := range []uint32{0, 12, 13, 65536, 65537, 1 << 20} {
		request := directoryCommandRequest(wire.FileID{1}, 12, 0, "*", length)
		if got, want := requiredCredits(request), max(1, int((uint64(length)+65535)/65536)); got != want {
			t.Fatalf("credits length=%d got=%d want=%d", length, got, want)
		}
		if got, want := responseBudget(request), max(80, (72+int(length)+7)&^7); got != want {
			t.Fatalf("budget length=%d got=%d want=%d", length, got, want)
		}
	}
	if got := responseBudget(wire.Request{Header: wire.Header{Command: wire.QueryDirectory}}); got != 80 {
		t.Fatalf("malformed directory budget=%d", got)
	}
}
