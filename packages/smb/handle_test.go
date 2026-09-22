package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func fileAdmissionRequest(t *testing.T, command uint16, body []byte, charge uint16) wire.Request {
	t.Helper()
	packet := requestPacket(wire.Header{Command: command, CreditCharge: charge, Credits: 1}, body)
	requests, err := wire.ParseFrame(packet, wire.Limits{MaxBytes: 2 << 20, MaxCommands: 1, MaxContexts: 16})
	if err != nil {
		t.Fatal(err)
	}
	return requests[0]
}

type handleReferenceProbe struct {
	mu         sync.Mutex
	closeCalls int
	close      func(context.Context, int) error
	stat       func(context.Context) (storage.Attr, error)
}

func (p *handleReferenceProbe) Stat(ctx context.Context) (storage.Attr, error) {
	if p.stat != nil {
		return p.stat(ctx)
	}
	return storage.Attr{}, syscall.EBADF
}

func (*handleReferenceProbe) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}

func (*handleReferenceProbe) CheckScopedReference() error { return nil }

func (*handleReferenceProbe) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: "scope"}, nil
}

func (*handleReferenceProbe) CheckReferenceState() error { return nil }

func (*handleReferenceProbe) State(context.Context) (storage.ReferenceState, error) {
	return storage.ReferenceState{}, syscall.EBADF
}

func (p *handleReferenceProbe) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closeCalls++
	attempt := p.closeCalls
	closeFn := p.close
	p.mu.Unlock()
	if closeFn == nil {
		return nil
	}
	return closeFn(ctx, attempt)
}

func (p *handleReferenceProbe) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	err := p.Close(ctx)
	released := storage.ReferenceCloseReleased(err) || storage.ErrnoOf(err) == syscall.ENOTEMPTY
	return storage.ReferenceCloseResult{Released: released}, err
}

func (p *handleReferenceProbe) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closeCalls
}

type handleFileProbe struct{ *handleReferenceProbe }

type releasedHandleReferenceProbe struct {
	*handleReferenceProbe
	err error
}

type deleteIntentSession struct {
	*endpointFileSession
	mu                sync.Mutex
	statuses          map[storage.DeleteIntentID]storage.DeleteIntentStatus
	acknowledgements  []storage.AcknowledgeDeleteIntentCommand
	failFirstAckAfter bool
}

type incompleteFileSession struct {
	storage.FileSession
	closed bool
}

func (s *incompleteFileSession) Close(context.Context) error {
	s.closed = true
	return nil
}

func (s *incompleteFileSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	err := s.Close(ctx)
	return storage.ReferenceCloseResult{Released: err == nil}, err
}

type fileSessionBackend struct {
	*endpointStorage
	fileSession storage.FileSession
}

func (s *fileSessionBackend) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, error) {
	return s.fileSession, nil
}

type failingCapabilitySession struct {
	*endpointFileSession
	err error
}

type publishingFileSession struct {
	*endpointFileSession
	publish func()
}

type releasedFileSessionProbe struct {
	*endpointFileSession
	err error
}

func (s *publishingFileSession) Close(ctx context.Context) error {
	s.publish()
	return s.endpointFileSession.Close(ctx)
}

func (s *publishingFileSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	err := s.Close(ctx)
	return storage.ReferenceCloseResult{Released: storage.ReferenceCloseReleased(err)}, err
}

func (s *releasedFileSessionProbe) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	s.mu.Lock()
	s.closes++
	s.mu.Unlock()
	return storage.ReferenceCloseResult{Released: true}, s.err
}

func (s *failingCapabilitySession) CheckAtomicFileOpen() error { return s.err }

func (s *deleteIntentSession) QueryDeleteIntent(_ context.Context, owner storage.DeleteIntentOwner, id storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	if owner != testDeleteIntentOwner {
		return storage.DeleteIntentStatus{}, syscall.EACCES
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if status, ok := s.statuses[id]; ok {
		return status, nil
	}
	return storage.DeleteIntentStatus{ID: id, Outcome: storage.DeleteIntentUnknown}, nil
}

func (s *deleteIntentSession) ListDeleteIntents(_ context.Context, owner storage.DeleteIntentOwner, cursor storage.DeleteIntentCursor, _ int) (storage.DeleteIntentPage, error) {
	if owner != testDeleteIntentOwner {
		return storage.DeleteIntentPage{}, syscall.EACCES
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor != 0 || len(s.statuses) == 0 {
		return storage.DeleteIntentPage{Next: cursor}, nil
	}
	intents := make([]storage.DeleteIntentStatus, 0, len(s.statuses))
	for _, status := range s.statuses {
		intents = append(intents, status)
	}
	return storage.DeleteIntentPage{Intents: intents, Next: 1}, nil
}

func (s *deleteIntentSession) AcknowledgeDeleteIntent(_ context.Context, command storage.AcknowledgeDeleteIntentCommand) error {
	if command.Owner != testDeleteIntentOwner {
		return syscall.EACCES
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acknowledgements = append(s.acknowledgements, command)
	delete(s.statuses, command.Intent)
	if s.failFirstAckAfter && len(s.acknowledgements) == 1 {
		return syscall.EIO
	}
	return nil
}

func (p *releasedHandleReferenceProbe) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	p.mu.Lock()
	p.closeCalls++
	p.mu.Unlock()
	return storage.ReferenceCloseResult{Released: true}, p.err
}

func (*handleFileProbe) ReadAt(context.Context, int64, int) (storage.FileRead, error) {
	return storage.FileRead{}, syscall.EBADF
}

func (*handleFileProbe) WriteAt(context.Context, int64, []byte) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}

func (*handleFileProbe) Truncate(context.Context, int64) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}

func (*handleFileProbe) Sync(context.Context) error { return syscall.EBADF }

func newHandleTestRegistry(t *testing.T, maxOpens int) *handleRegistry {
	t.Helper()
	config := endpointConfig()
	config.Limits.MaxOpens = maxOpens
	server := &Server{config: config}
	export := &Export{server: server, published: true, share: Share{DeleteIntentOwner: testDeleteIntentOwner}}
	authority := &authoritySession{export: export, deadline: time.Now().Add(time.Hour)}
	session := &session{trees: make(map[uint32]*tree)}
	tree := &tree{kind: volumeTree, id: 1, session: session, export: export, authority: authority}
	tree.files = newHandleRegistry(tree, config.Limits)
	session.trees[tree.id] = tree
	return tree.files
}

func reserveFileHandle(t *testing.T, registry *handleRegistry, reference *handleReferenceProbe, id uint64) (wire.FileID, *fileHandle) {
	t.Helper()
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	file := &handleFileProbe{handleReferenceProbe: reference}
	reservation.attachFile(storage.OpenResult{File: file, Attr: storage.Attr{ID: id, Kind: storage.NodeRegular}, Outcome: storage.Opened})
	fileID, err := reservation.install(3, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	return fileID, registry.get(fileID)
}

func TestHandleRegistryBoundsReservationsAndReportsOwnership(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	reservation, err := registry.reserveResponse()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.reserve(); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("reservation above limit = %v", err)
	}
	status := Status{}
	registry.addStatus(&status)
	if status.OpenReservations != 1 || status.InstalledHandles != 0 || status.ActiveFileOperations != 0 {
		t.Fatalf("reserved status = %+v", status)
	}

	reference := &handleReferenceProbe{}
	file := &handleFileProbe{handleReferenceProbe: reference}
	reservation.attachFile(storage.OpenResult{File: file, Attr: storage.Attr{ID: 9, Kind: storage.NodeRegular}, Outcome: storage.Created})
	reservation.setScope(storage.UseScope{Token: "scope"})
	reservation.setWriteThrough(true)
	id, err := reservation.install(0x120089, 7)
	if err != nil || id == (wire.FileID{}) {
		t.Fatalf("install = %v, %v", id, err)
	}
	handle := registry.get(id)
	if handle == nil || handle.file != file || handle.reference != file || handle.nodeID != 9 ||
		handle.grantedAccess != 0x120089 || handle.shareAccess != 7 || !handle.writeThrough || handle.scope == nil {
		t.Fatalf("installed handle = %+v", handle)
	}
	reservation.releaseResponse()
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	status = Status{}
	registry.addStatus(&status)
	if status.OpenReservations != 0 || status.InstalledHandles != 1 || status.ActiveFileOperations != 0 {
		t.Fatalf("installed status = %+v", status)
	}
	if err := registry.closeID(t.Context(), id, handle); err != nil {
		t.Fatal(err)
	}
	if reference.calls() != 1 {
		t.Fatalf("reference close calls = %d", reference.calls())
	}
	if _, err := registry.reserve(); err != nil {
		t.Fatalf("released capacity = %v", err)
	}
}

func TestTreeFileWorkIsBoundedFencedAndDrained(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	registry.work.limit = 1
	release, err := registry.tree.beginFileWork()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.tree.beginFileWork(); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("file work above limit = %v", err)
	}
	registry.fence()
	if _, err := registry.tree.beginFileWork(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("file work after fence = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := registry.work.wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain with admitted work = %v", err)
	}
	release()
	if err := registry.work.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSessionRetirementAndTreeWorkAdmissionShareOneGate(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	session := registry.tree.session
	release, err := registry.tree.beginFileWork()
	if err != nil {
		t.Fatal(err)
	}
	release()

	session.mu.Lock()
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		_, err := registry.tree.beginFileWork()
		result <- err
	}()
	<-started
	session.retired = true
	session.mu.Unlock()
	if err := <-result; !errors.Is(err, syscall.EBADF) {
		t.Fatalf("work admitted after session retirement = %v", err)
	}
}

func TestHandleLimitsAreExplicit(t *testing.T) {
	for name, change := range map[string]func(*Config){
		"opens":             func(config *Config) { config.Limits.MaxOpens = 0 },
		"open result bytes": func(config *Config) { config.Limits.MaxOpenResultBytes = 0 },
		"directory entries": func(config *Config) { config.Limits.MaxDirectoryEntries = 0 },
		"directory bytes":   func(config *Config) { config.Limits.MaxDirectoryBytes = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			config := endpointConfig()
			change(&config)
			if _, err := New(config); !errors.Is(err, ErrConfig) {
				t.Fatalf("New error = %v", err)
			}
		})
	}
}

func TestTreeConnectRejectsIncompleteOrFailingFileCapabilityChain(t *testing.T) {
	for _, test := range []struct {
		name    string
		session storage.FileSession
		cause   error
	}{
		{name: "missing capability", session: &incompleteFileSession{}, cause: syscall.EOPNOTSUPP},
		{name: "failing capability", session: &failingCapabilitySession{endpointFileSession: newEndpointFileSession(), err: syscall.EIO}, cause: syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := endpointConfig()
			server, connection := testConnection(t, config.Limits)
			backend := &fileSessionBackend{endpointStorage: &endpointStorage{}, fileSession: test.session}
			export, err := server.Publish(endpointShare("data", "volume", backend))
			if err != nil {
				t.Fatal(err)
			}
			session := &session{principal: testPrincipal("0123456789abcdef"), trees: make(map[uint32]*tree)}
			if _, status := connection.connectVolume(WithPrincipal(t.Context(), session.principal), session, "DATA", &wire.Header{}); status != fileCommandStatus(test.cause) {
				t.Fatalf("TREE_CONNECT status = %#x", status)
			}
			if len(session.trees) != 0 {
				t.Fatal("failed preflight published a tree")
			}
			if incomplete, ok := test.session.(*incompleteFileSession); ok && !incomplete.closed {
				t.Fatal("failed preflight did not close FileSession")
			}
			if err := export.Unpublish(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFileResponseAdmissionUsesDeclaredOutputSize(t *testing.T) {
	readBody := func(length uint32) []byte {
		body := make([]byte, 48)
		binary.LittleEndian.PutUint16(body, 49)
		binary.LittleEndian.PutUint32(body[4:8], length)
		return body
	}
	for _, test := range []struct {
		length  uint32
		credits int
	}{
		{length: 65536, credits: 1},
		{length: 65537, credits: 2},
	} {
		request := fileAdmissionRequest(t, wire.Read, readBody(test.length), uint16(test.credits))
		if got := requiredCredits(request); got != test.credits {
			t.Fatalf("required credits for %d bytes = %d, want %d", test.length, got, test.credits)
		}
		if got := responseBudget(request); got != 80+int(test.length) {
			t.Fatalf("response budget for %d bytes = %d", test.length, got)
		}
	}
	limits := DefaultLimits()
	request := fileAdmissionRequest(t, wire.Read, readBody(uint32(limits.MaxIOBytes+1)), 17)
	if err := checkReceivedRequests([]wire.Request{request}, len(request.Packet), true, limits); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("oversized declared read = %v", err)
	}

	left := fileAdmissionRequest(t, wire.Read, readBody(80), 1)
	right := fileAdmissionRequest(t, wire.Read, readBody(80), 1)
	limits.MaxFrameBytes = responseBudget(left) + responseBudget(right) - 1
	limits.MaxIOBytes = limits.MaxFrameBytes
	if err := checkReceivedRequests([]wire.Request{left, right}, len(left.Packet)+len(right.Packet), true, limits); !errors.Is(err, wire.ErrMalformed) {
		t.Fatalf("compound response overflow = %v", err)
	}
}

func TestHandleIDAndResultBudgetAreReservedBeforeOpen(t *testing.T) {
	registry := newHandleTestRegistry(t, 2)
	var fixed wire.FileID
	fixed[0] = 7
	registry.random = func(destination []byte) (int, error) {
		copy(destination, fixed[:])
		return len(destination), nil
	}
	first, err := registry.reserve()
	if err != nil || first.id != fixed {
		t.Fatalf("first reservation = %v, %v", first, err)
	}
	if _, err := registry.reserve(); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("colliding FileId allocation = %v", err)
	}
	registry.mu.Lock()
	if registry.slots != 1 || len(registry.reservations) != 1 || len(registry.reservedIDs) != 1 {
		t.Fatalf("failed collision changed accounting: slots=%d reservations=%d ids=%d",
			registry.slots, len(registry.reservations), len(registry.reservedIDs))
	}
	registry.mu.Unlock()
	first.attachFile(storage.OpenResult{})
	if err := first.finish(t.Context()); err != nil {
		t.Fatal(err)
	}

	failure := errors.New("random source failed")
	registry.random = func([]byte) (int, error) { return 0, failure }
	if _, err := registry.reserve(); !errors.Is(err, failure) {
		t.Fatalf("random failure = %v", err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.slots != 0 || len(registry.reservations) != 0 || len(registry.reservedIDs) != 0 || registry.openResultBytes != 0 {
		t.Fatalf("random failure changed accounting: %+v", registry)
	}
}

func TestOpenResultBudgetIsIndependentAndSharedAcrossTrees(t *testing.T) {
	config := endpointConfig()
	config.Limits.MaxOpens = 2
	config.Limits.MaxOpenResultBytes = maxOpenResultCharge()
	server := &Server{config: config}
	export := &Export{server: server, published: true, share: Share{DeleteIntentOwner: testDeleteIntentOwner}}
	authority := &authoritySession{export: export, deadline: time.Now().Add(time.Hour)}
	leftSession := &session{trees: make(map[uint32]*tree)}
	left := &tree{kind: volumeTree, id: 1, session: leftSession, export: export, authority: authority}
	left.files = newHandleRegistry(left, config.Limits)
	leftSession.trees[left.id] = left
	rightSession := &session{trees: make(map[uint32]*tree)}
	right := &tree{kind: volumeTree, id: 2, session: rightSession, export: export, authority: authority}
	right.files = newHandleRegistry(right, config.Limits)
	rightSession.trees[right.id] = right

	reservation, err := left.files.reserve()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := right.files.reserve(); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("cross-tree result budget = %v", err)
	}
	if err := reservation.resultBudget(storage.Attr{}, 6); err != nil {
		t.Fatal(err)
	}
	reservation.attachFile(storage.OpenResult{})
	status := Status{}
	left.files.addStatus(&status)
	want, _ := storage.MetadataRetentionBytes(6)
	want += 512
	if status.OpenResultBytes != want {
		t.Fatalf("settled open result bytes = %d, want %d", status.OpenResultBytes, want)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := right.files.reserve(); err != nil {
		t.Fatalf("released cross-tree result budget = %v", err)
	}
}

func TestDirectorySnapshotAndOutputAccounting(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	registry.limits.MaxDirectoryEntries = 2
	registry.limits.MaxDirectoryBytes = 4096
	reference := &handleReferenceProbe{}
	_, handle := reserveFileHandle(t, registry, reference, 24)
	if err := handle.resetDirectorySnapshot("*", false); err != nil {
		t.Fatal(err)
	}
	capture, err := handle.beginDirectorySnapshot()
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := capture.entryBudget(0, 4, 6, storage.Attr{ID: 25, Kind: storage.NodeRegular})
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := capture.entryBudget(1, 4, 6, storage.Attr{ID: 26, Kind: storage.NodeRegular})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capture.entryBudget(2, 4, 6, storage.Attr{ID: 27, Kind: storage.NodeRegular}); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("directory entry N+1 = %v", err)
	}
	cursor := directoryCursor{
		pattern: "*", observation: storage.DirectoryObservation{ParentID: 24, Revision: []byte("r1")},
		entries: []storage.ObservedEntry{{RawLeaf: []byte("left"), Attr: storage.Attr{ID: 25, Kind: storage.NodeRegular}}},
	}
	if err := capture.commit(cursor, 1, 256+firstBytes); err != nil {
		t.Fatal(err)
	}
	status := Status{}
	registry.addStatus(&status)
	if status.DirectoryEntries != 1 || status.DirectoryBytes != 1+256+firstBytes {
		t.Fatalf("cursor accounting = %+v (second charge %d)", status, secondBytes)
	}
	releaseOutput, err := handle.reserveDirectoryOutput(512)
	if err != nil {
		t.Fatal(err)
	}
	status = Status{}
	registry.addStatus(&status)
	if status.DirectoryBytes != 1+256+firstBytes+512 {
		t.Fatalf("output accounting = %+v", status)
	}
	releaseOutput()
	handle.clearDirectoryCursor()
	status = Status{}
	registry.addStatus(&status)
	if status.DirectoryEntries != 0 || status.DirectoryBytes != 0 {
		t.Fatalf("released directory accounting = %+v", status)
	}
}

func TestFailedOpenCleanupRetainsReservationAndCapacity(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	failure := errors.Join(syscall.ENOTEMPTY, syscall.EIO)
	reference := &handleReferenceProbe{close: func(_ context.Context, attempt int) error {
		if attempt == 1 {
			return failure
		}
		return nil
	}}
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	reservation.attachNode(storage.NodeOpenResult{
		Reference: reference,
		Attr:      storage.Attr{ID: 18, Kind: storage.NodeDirectory},
		Outcome:   storage.Opened,
	})
	reservation.cleanupOnly = true
	if err := reservation.finish(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("first cleanup = %v", err)
	}
	status := Status{}
	registry.addStatus(&status)
	if status.OpenReservations != 1 || status.CleanupPendingHandles != 1 || status.ActiveFileOperations != 0 {
		t.Fatalf("retained reservation status = %+v", status)
	}
	if _, err := registry.reserve(); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("retained reservation released capacity = %v", err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if reference.calls() != 2 {
		t.Fatalf("reference close calls = %d", reference.calls())
	}
}

func TestResolverReferencesCloseLeafToRootAndRetainOnlyFailures(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var order []string
	root := &handleReferenceProbe{close: func(context.Context, int) error {
		mu.Lock()
		order = append(order, "root")
		mu.Unlock()
		return nil
	}}
	failure := errors.New("child cleanup failed")
	child := &handleReferenceProbe{close: func(_ context.Context, attempt int) error {
		mu.Lock()
		order = append(order, "child")
		mu.Unlock()
		if attempt == 1 {
			return failure
		}
		return nil
	}}
	reservation.adoptAux(root)
	reservation.adoptAux(child)
	reservation.attachFile(storage.OpenResult{})
	if err := reservation.finish(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("first cleanup = %v", err)
	}
	reservation.mu.Lock()
	if len(reservation.aux) != 1 || reservation.aux[0] != child {
		reservation.mu.Unlock()
		t.Fatal("failed auxiliary owner was not retained in acquisition order")
	}
	reservation.mu.Unlock()
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"child", "root", "child"}
	if len(order) != len(want) {
		t.Fatalf("close order = %v", order)
	}
	for index := range want {
		if order[index] != want[index] {
			t.Fatalf("close order = %v", order)
		}
	}
}

func TestInstalledReservationRetainsSlotUntilAuxiliaryCleanupFinishes(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	auxiliary := &handleReferenceProbe{close: func(_ context.Context, attempt int) error {
		if attempt == 1 {
			return syscall.EIO
		}
		return nil
	}}
	reservation.adoptAux(auxiliary)
	file := &handleFileProbe{handleReferenceProbe: &handleReferenceProbe{}}
	reservation.attachFile(storage.OpenResult{
		File: file, Attr: storage.Attr{ID: 31, Kind: storage.NodeRegular}, Outcome: storage.Opened,
	})
	id, err := reservation.install(3, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("first auxiliary cleanup = %v", err)
	}
	if err := registry.closeID(t.Context(), id, registry.get(id)); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	_, idRetained := registry.reservedIDs[id]
	registry.mu.Unlock()
	if !idRetained {
		t.Fatal("FileId released before auxiliary cleanup")
	}
	if _, err := registry.reserve(); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("slot released before auxiliary cleanup = %v", err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	_, idRetained = registry.reservedIDs[id]
	registry.mu.Unlock()
	if idRetained {
		t.Fatal("FileId retained after auxiliary cleanup")
	}
	if _, err := registry.reserve(); err != nil {
		t.Fatalf("slot retained after auxiliary cleanup = %v", err)
	}
}

func TestHandleCloseFencesAndDrainsBorrowedWorkBeforeNativeClose(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	closed := make(chan struct{})
	reference := &handleReferenceProbe{close: func(context.Context, int) error {
		close(closed)
		return nil
	}}
	id, handle := reserveFileHandle(t, registry, reference, 11)
	release, err := handle.borrow()
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- registry.closeID(t.Context(), id, handle) }()

	deadline := time.Now().Add(time.Second)
	for {
		handle.mu.Lock()
		retiring := handle.retiring
		handle.mu.Unlock()
		if retiring {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("close did not fence handle")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-closed:
		t.Fatal("native close ran before admitted work drained")
	default:
	}
	if _, err := handle.borrow(); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("retiring handle borrow = %v", err)
	}
	release()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("native close did not run after drain")
	}
}

func TestCloseWithAttributesCapturesAfterDrainBeforeRelease(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	order := make(chan string, 2)
	reference := &handleReferenceProbe{
		stat: func(context.Context) (storage.Attr, error) {
			order <- "stat"
			return storage.Attr{ID: 26, Kind: storage.NodeRegular, Size: 7}, nil
		},
		close: func(context.Context, int) error {
			order <- "close"
			return nil
		},
	}
	id, handle := reserveFileHandle(t, registry, reference, 26)
	attr, err := registry.closeIDWithAttr(t.Context(), id, handle, true)
	if err != nil || attr.ID != 26 || attr.Size != 7 {
		t.Fatalf("close capture = %+v, %v", attr, err)
	}
	if first, second := <-order, <-order; first != "stat" || second != "close" {
		t.Fatalf("close order = %q, %q", first, second)
	}
}

func TestHandleCloseFailureRetainsOwnershipUntilRetry(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	failure := errors.New("reference cleanup failed")
	reference := &handleReferenceProbe{close: func(_ context.Context, attempt int) error {
		if attempt == 1 {
			return failure
		}
		return nil
	}}
	id, handle := reserveFileHandle(t, registry, reference, 12)
	if err := registry.closeID(t.Context(), id, handle); !errors.Is(err, failure) {
		t.Fatalf("first close = %v", err)
	}
	status := Status{}
	registry.addStatus(&status)
	if status.InstalledHandles != 1 || status.CleanupPendingHandles != 1 {
		t.Fatalf("failed cleanup status = %+v", status)
	}
	if registry.get(id) != handle {
		t.Fatal("failed close lost handle ownership")
	}
	if _, err := handle.borrow(); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("failed-close handle borrow = %v", err)
	}
	if _, err := registry.reserve(); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("failed close released capacity = %v", err)
	}
	if err := registry.closeID(t.Context(), id, handle); err != nil {
		t.Fatal(err)
	}
	if reference.calls() != 2 {
		t.Fatalf("reference close calls = %d", reference.calls())
	}
}

func TestDeleteIntentAckRetryDoesNotRecloseReleasedReference(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	intent := storage.DeleteIntentID("0123456789abcdef0123456789abcdef")
	session := &deleteIntentSession{
		endpointFileSession: newEndpointFileSession(),
		statuses: map[storage.DeleteIntentID]storage.DeleteIntentStatus{
			intent: {ID: intent, NodeID: 22, Outcome: storage.DeleteIntentArmed},
		},
		failFirstAckAfter: true,
	}
	registry.tree.authority.raw = session
	reference := &handleReferenceProbe{close: func(context.Context, int) error {
		session.mu.Lock()
		session.statuses[intent] = storage.DeleteIntentStatus{ID: intent, NodeID: 22, Outcome: storage.DeleteIntentCompleted}
		session.mu.Unlock()
		return nil
	}}
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	reservation.setDeleteIntent(intent)
	file := &handleFileProbe{handleReferenceProbe: reference}
	reservation.attachFile(storage.OpenResult{File: file, Attr: storage.Attr{ID: 22, Kind: storage.NodeRegular}, Outcome: storage.Opened})
	id, err := reservation.install(3, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle := registry.get(id)
	if err := registry.closeID(t.Context(), id, handle); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("lost ACK result = %v", err)
	}
	if reference.calls() != 1 || registry.get(id) != handle {
		t.Fatalf("lost ACK released ownership: closes=%d handle=%p", reference.calls(), registry.get(id))
	}
	if err := registry.closeID(t.Context(), id, handle); err != nil {
		t.Fatal(err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if len(session.acknowledgements) != 2 || session.acknowledgements[0].Action != session.acknowledgements[1].Action ||
		session.acknowledgements[0].Owner != testDeleteIntentOwner {
		t.Fatalf("ACK retries = %+v", session.acknowledgements)
	}
	if reference.calls() != 1 {
		t.Fatalf("ACK retry reclosed reference %d times", reference.calls())
	}
}

func TestUnknownPartialOpenRetainsDeleteIntentWithoutReference(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	intent := storage.DeleteIntentID("123456789abcdef0123456789abcdef0")
	session := &deleteIntentSession{endpointFileSession: newEndpointFileSession(), statuses: make(map[storage.DeleteIntentID]storage.DeleteIntentStatus)}
	registry.tree.authority.raw = session
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	reservation.setDeleteIntent(intent)
	reservation.attachFile(storage.OpenResult{})
	if err := reservation.finish(t.Context()); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("unknown partial open cleanup = %v", err)
	}
	status := Status{}
	registry.addStatus(&status)
	if status.OpenReservations != 1 || status.CleanupPendingHandles != 1 {
		t.Fatalf("unknown partial open ownership = %+v", status)
	}
	session.mu.Lock()
	session.statuses[intent] = storage.DeleteIntentStatus{ID: intent, NodeID: 23, Outcome: storage.DeleteIntentNotExecuted}
	session.mu.Unlock()
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestDefinitiveNoEffectOpenDiscardsDeleteIntent(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	reservation.setDeleteIntent(storage.DeleteIntentID("123456789abcdef0123456789abcdef0"))
	reservation.discardDeleteIntent()
	reservation.attachFile(storage.OpenResult{})
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	status := Status{}
	registry.addStatus(&status)
	if status.OpenReservations != 0 || status.CleanupPendingHandles != 0 {
		t.Fatalf("discarded no-effect intent retained ownership: %+v", status)
	}
}

func TestOwnerScanDiscoversAndAcknowledgesCrashRecoveredIntents(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	intent := storage.DeleteIntentID("23456789abcdef0123456789abcdef01")
	session := &deleteIntentSession{
		endpointFileSession: newEndpointFileSession(),
		statuses: map[storage.DeleteIntentID]storage.DeleteIntentStatus{
			intent: {ID: intent, NodeID: 24, Outcome: storage.DeleteIntentCompleted},
		},
	}
	registry.tree.authority.raw = session
	if err := registry.tree.authority.scanDeleteIntentsWith(t.Context(), session, 1); err != nil {
		t.Fatal(err)
	}
	registry.tree.authority.deleteMu.Lock()
	remaining := len(registry.tree.authority.deleteIntents)
	recoveryPending := registry.tree.authority.recoveryPending
	registry.tree.authority.deleteMu.Unlock()
	if remaining != 0 || recoveryPending {
		t.Fatalf("terminal recovery retained %d intents, pending=%v", remaining, recoveryPending)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if len(session.acknowledgements) != 1 || session.acknowledgements[0].Intent != intent ||
		session.acknowledgements[0].Owner != testDeleteIntentOwner {
		t.Fatalf("recovery acknowledgements = %+v", session.acknowledgements)
	}
}

func TestOwnerScanKeepsNonterminalIntentVisibleUntilLaterScan(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	intent := storage.DeleteIntentID("3456789abcdef0123456789abcdef012")
	session := &deleteIntentSession{
		endpointFileSession: newEndpointFileSession(),
		statuses: map[storage.DeleteIntentID]storage.DeleteIntentStatus{
			intent: {ID: intent, NodeID: 25, Outcome: storage.DeleteIntentPending},
		},
	}
	registry.tree.authority.raw = session
	if err := registry.tree.authority.scanDeleteIntentsWith(t.Context(), session, 1); err != nil {
		t.Fatal(err)
	}
	registry.tree.authority.deleteMu.Lock()
	remaining := len(registry.tree.authority.deleteIntents)
	recoveryPending := registry.tree.authority.recoveryPending
	registry.tree.authority.deleteMu.Unlock()
	if remaining != 0 || !recoveryPending {
		t.Fatalf("pending recovery intents = %d, pending=%v", remaining, recoveryPending)
	}
	session.mu.Lock()
	session.statuses[intent] = storage.DeleteIntentStatus{ID: intent, NodeID: 25, Outcome: storage.DeleteIntentNotExecuted}
	session.mu.Unlock()
	if err := registry.tree.authority.scanDeleteIntentsWith(t.Context(), session, 1); err != nil {
		t.Fatal(err)
	}
	registry.tree.authority.deleteMu.Lock()
	remaining = len(registry.tree.authority.deleteIntents)
	recoveryPending = registry.tree.authority.recoveryPending
	registry.tree.authority.deleteMu.Unlock()
	if remaining != 0 || recoveryPending {
		t.Fatalf("terminal rescan retained %d intents, pending=%v", remaining, recoveryPending)
	}
}

func TestRecoverySessionCloseFailureRetainsRetryOwner(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	session := newEndpointFileSession()
	session.closeErr = syscall.EIO
	backend := &endpointStorage{session: session}
	registry.tree.export.share.Backend = backend
	registry.tree.authority.recoveryPending = true

	if err := registry.tree.authority.recoverDeleteIntents(t.Context()); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("first recovery = %v", err)
	}
	registry.tree.authority.deleteMu.Lock()
	retained := registry.tree.authority.recoverySession
	registry.tree.authority.deleteMu.Unlock()
	if retained != session || backend.sessionOpens.Load() != 1 {
		t.Fatalf("retained recovery session = %p, opens=%d", retained, backend.sessionOpens.Load())
	}

	session.mu.Lock()
	session.closeErr = nil
	session.mu.Unlock()
	if err := registry.tree.authority.recoverDeleteIntents(t.Context()); err != nil {
		t.Fatal(err)
	}
	registry.tree.authority.deleteMu.Lock()
	retained = registry.tree.authority.recoverySession
	registry.tree.authority.deleteMu.Unlock()
	if retained != nil || backend.sessionOpens.Load() != 1 {
		t.Fatalf("recovery retry opened a replacement: retained=%p opens=%d", retained, backend.sessionOpens.Load())
	}
}

func TestAuthorityCloseScansIntentsPublishedByRawClose(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	intent := storage.DeleteIntentID("456789abcdef0123456789abcdef0123")
	recovery := &deleteIntentSession{
		endpointFileSession: newEndpointFileSession(), statuses: make(map[storage.DeleteIntentID]storage.DeleteIntentStatus),
	}
	raw := &publishingFileSession{endpointFileSession: newEndpointFileSession(), publish: func() {
		recovery.mu.Lock()
		recovery.statuses[intent] = storage.DeleteIntentStatus{ID: intent, NodeID: 32, Outcome: storage.DeleteIntentCompleted}
		recovery.mu.Unlock()
	}}
	registry.tree.export.share.Backend = &fileSessionBackend{endpointStorage: &endpointStorage{}, fileSession: recovery}
	registry.tree.authority.raw = raw
	registry.tree.authority.refs = 1
	registry.tree.authority.deleteProduced = true
	if err := registry.tree.authority.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	recovery.mu.Lock()
	defer recovery.mu.Unlock()
	if len(recovery.acknowledgements) != 1 || recovery.acknowledgements[0].Intent != intent {
		t.Fatalf("post-close recovery acknowledgements = %+v", recovery.acknowledgements)
	}
}

func TestAuthorityCloseAttemptsRecoveryAfterRawCloseFailure(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	intent := storage.DeleteIntentID("56789abcdef0123456789abcdef01234")
	recovery := &deleteIntentSession{
		endpointFileSession: newEndpointFileSession(), statuses: make(map[storage.DeleteIntentID]storage.DeleteIntentStatus),
	}
	rawSession := newEndpointFileSession()
	rawSession.closeErr = syscall.EIO
	raw := &publishingFileSession{endpointFileSession: rawSession, publish: func() {
		recovery.mu.Lock()
		recovery.statuses[intent] = storage.DeleteIntentStatus{ID: intent, NodeID: 33, Outcome: storage.DeleteIntentCompleted}
		recovery.mu.Unlock()
	}}
	registry.tree.export.share.Backend = &fileSessionBackend{endpointStorage: &endpointStorage{}, fileSession: recovery}
	registry.tree.authority.raw = raw
	registry.tree.authority.deleteProduced = true
	if err := registry.tree.authority.close(t.Context()); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("authority close = %v", err)
	}
	recovery.mu.Lock()
	defer recovery.mu.Unlock()
	if len(recovery.acknowledgements) != 1 || recovery.acknowledgements[0].Intent != intent {
		t.Fatalf("recovery after raw close failure = %+v", recovery.acknowledgements)
	}
}

func TestAuthorityDoesNotRetryReleasedSessionAfterSemanticCloseError(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	raw := &releasedFileSessionProbe{
		endpointFileSession: newEndpointFileSession(), err: errors.Join(syscall.ENOTEMPTY, syscall.EIO),
	}
	registry.tree.authority.raw = raw
	if err := registry.tree.authority.close(t.Context()); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("released session close = %v", err)
	}
	if !registry.tree.authority.isClosed() {
		t.Fatal("released session retained authority ownership")
	}
	if err := registry.tree.authority.close(t.Context()); err != nil {
		t.Fatalf("released session retry = %v", err)
	}
	if raw.closes != 1 {
		t.Fatalf("released session close calls = %d", raw.closes)
	}
}

func TestReleasedReferenceWithJoinedErrorIsNotRetried(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	reference := &releasedHandleReferenceProbe{
		handleReferenceProbe: &handleReferenceProbe{},
		err:                  errors.Join(syscall.ENOTEMPTY, syscall.EIO),
	}
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	reservation.attachNode(storage.NodeOpenResult{
		Reference: reference,
		Attr:      storage.Attr{ID: 21, Kind: storage.NodeDirectory},
		Outcome:   storage.Opened,
	})
	id, err := reservation.install(0, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle := registry.get(id)
	if err := registry.closeID(t.Context(), id, handle); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("released joined close = %v", err)
	}
	registry.mu.Lock()
	_, retained := registry.handles[id]
	registry.mu.Unlock()
	if retained || reference.calls() != 1 {
		t.Fatalf("released reference retained=%v calls=%d", retained, reference.calls())
	}
}

func TestJoinedDirectoryAndUnknownCloseFailureRetainsHandle(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	reference := &handleReferenceProbe{close: func(_ context.Context, attempt int) error {
		if attempt == 1 {
			return errors.Join(syscall.ENOTEMPTY, syscall.EIO)
		}
		return nil
	}}
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	reservation.attachNode(storage.NodeOpenResult{
		Reference: reference,
		Attr:      storage.Attr{ID: 25, Kind: storage.NodeDirectory},
		Outcome:   storage.Opened,
	})
	id, err := reservation.install(0, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle := registry.get(id)
	if err := registry.closeID(t.Context(), id, handle); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("joined close = %v", err)
	}
	if registry.get(id) != handle {
		t.Fatal("joined cleanup failure released handle")
	}
	if err := registry.closeID(t.Context(), id, handle); err != nil {
		t.Fatal(err)
	}
	if reference.calls() != 2 {
		t.Fatalf("reference close calls = %d", reference.calls())
	}
}

func TestRegistryCleanupAttemptsEveryHandleAndRetainsOnlyFailures(t *testing.T) {
	registry := newHandleTestRegistry(t, 2)
	failure := errors.New("first handle cleanup failed")
	firstReference := &handleReferenceProbe{close: func(_ context.Context, attempt int) error {
		if attempt == 1 {
			return failure
		}
		return nil
	}}
	firstID, first := reserveFileHandle(t, registry, firstReference, 13)
	secondReference := &handleReferenceProbe{}
	secondID, _ := reserveFileHandle(t, registry, secondReference, 14)
	if err := registry.close(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("registry close = %v", err)
	}
	registry.mu.Lock()
	_, firstOwned := registry.handles[firstID]
	_, secondOwned := registry.handles[secondID]
	registry.mu.Unlock()
	if !firstOwned || secondOwned || firstReference.calls() != 1 || secondReference.calls() != 1 {
		t.Fatalf("cleanup ownership: first=%v second=%v calls=%d/%d", firstOwned, secondOwned, firstReference.calls(), secondReference.calls())
	}
	if err := registry.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !first.isClosed() || firstReference.calls() != 2 {
		t.Fatalf("retry did not finish first handle: closed=%v calls=%d", first.isClosed(), firstReference.calls())
	}
}

func TestTerminalDirectoryCloseFailureReleasesHandle(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	reference := &releasedHandleReferenceProbe{handleReferenceProbe: &handleReferenceProbe{}, err: syscall.ENOTEMPTY}
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
	reservation.attachNode(storage.NodeOpenResult{
		Reference: reference,
		Attr:      storage.Attr{ID: 19, Kind: storage.NodeDirectory},
		Outcome:   storage.Opened,
	})
	id, err := reservation.install(0, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle := registry.get(id)
	if err := registry.closeID(t.Context(), id, handle); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("directory close = %v", err)
	}
	registry.mu.Lock()
	_, retained := registry.handles[id]
	registry.mu.Unlock()
	if retained || reference.calls() != 1 {
		t.Fatalf("terminal close retained=%v calls=%d", retained, reference.calls())
	}
}

func TestTreeCleanupClosesEachTreeHandlesBeforeSharedFileSession(t *testing.T) {
	config := endpointConfig()
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	connection := newConnection(server, left)
	recovery := newEndpointFileSession()
	export := &Export{
		server: server, published: true, refs: 2, trees: 2,
		share: endpointShare("data", "volume", &endpointStorage{session: recovery}),
	}
	order := make(chan string, 3)
	raw := newEndpointFileSession()
	raw.closeEntered = make(chan struct{})
	authority := &authoritySession{
		raw: raw, export: export, refs: 2, deadline: time.Now().Add(time.Hour),
		ready: make(chan struct{}), done: make(chan struct{}),
	}
	close(authority.ready)
	close(authority.done)
	first := &tree{kind: volumeTree, id: 1, sessionID: 1, export: export, authority: authority, done: make(chan struct{})}
	first.files = newHandleRegistry(first, config.Limits)
	second := &tree{kind: volumeTree, id: 2, sessionID: 1, export: export, authority: authority, done: make(chan struct{})}
	second.files = newHandleRegistry(second, config.Limits)
	firstReference := &handleReferenceProbe{close: func(context.Context, int) error { order <- "first handle"; return nil }}
	_, _ = reserveFileHandle(t, first.files, firstReference, 15)
	secondReference := &handleReferenceProbe{close: func(context.Context, int) error { order <- "second handle"; return nil }}
	_, _ = reserveFileHandle(t, second.files, secondReference, 16)

	if err := connection.closeTreeContext(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-raw.closeEntered:
		t.Fatal("shared FileSession closed while another tree remained")
	default:
	}
	if got := <-order; got != "first handle" {
		t.Fatalf("first cleanup order = %q", got)
	}
	if err := connection.closeTreeContext(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if got := <-order; got != "second handle" {
		t.Fatalf("second cleanup order = %q", got)
	}
	select {
	case <-raw.closeEntered:
	default:
		t.Fatal("last tree did not close shared FileSession")
	}
	if raw.closes != 1 || export.refs != 0 || export.trees != 0 {
		t.Fatalf("final ownership: closes=%d refs=%d trees=%d", raw.closes, export.refs, export.trees)
	}
}

func TestTreeCleanupRetriesHandlesBeforeClosingFileSession(t *testing.T) {
	config := endpointConfig()
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	connection := newConnection(server, left)
	recovery := newEndpointFileSession()
	export := &Export{
		server: server, published: true, refs: 1, trees: 1,
		share: endpointShare("data", "volume", &endpointStorage{session: recovery}),
	}
	raw := newEndpointFileSession()
	authority := &authoritySession{
		raw: raw, export: export, refs: 1, deadline: time.Now().Add(time.Hour),
		ready: make(chan struct{}), done: make(chan struct{}),
	}
	close(authority.ready)
	close(authority.done)
	tree := &tree{kind: volumeTree, id: 1, sessionID: 1, export: export, authority: authority, done: make(chan struct{})}
	tree.files = newHandleRegistry(tree, config.Limits)
	failure := errors.New("handle cleanup failed")
	reference := &handleReferenceProbe{close: func(_ context.Context, attempt int) error {
		if attempt == 1 {
			return failure
		}
		return nil
	}}
	_, _ = reserveFileHandle(t, tree.files, reference, 20)
	if err := connection.closeTreeContext(t.Context(), tree); !errors.Is(err, failure) {
		t.Fatalf("first tree close = %v", err)
	}
	if raw.closes != 1 || tree.closed || !authority.isStopping() || export.refs != 1 || export.trees != 1 {
		t.Fatalf("failed cleanup ownership: closes=%d tree.closed=%v stopping=%v refs=%d trees=%d",
			raw.closes, tree.closed, authority.isStopping(), export.refs, export.trees)
	}
	if err := connection.closeTreeContext(t.Context(), tree); err != nil {
		t.Fatal(err)
	}
	if raw.closes != 1 || !tree.closed || export.refs != 0 || export.trees != 0 || reference.calls() != 1 {
		t.Fatalf("retry ownership: closes=%d tree.closed=%v refs=%d trees=%d handle closes=%d",
			raw.closes, tree.closed, export.refs, export.trees, reference.calls())
	}
}

func TestConnectionFenceRejectsNewFileWorkBeforeCleanup(t *testing.T) {
	registry := newHandleTestRegistry(t, 1)
	reference := &handleReferenceProbe{}
	_, handle := reserveFileHandle(t, registry, reference, 17)
	release, err := registry.tree.beginFileWork()
	if err != nil {
		t.Fatal(err)
	}
	server := registry.tree.export.server
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	connection := newConnection(server, left)
	session := registry.tree.session
	connection.sessions[1] = session
	connection.fenceFileWork()
	borrowed, err := handle.borrow()
	if err != nil {
		t.Fatalf("pre-fence admitted work could not borrow = %v", err)
	}
	borrowed()
	release()
	if _, err := registry.tree.beginFileWork(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("new work after connection fence = %v", err)
	}
}
