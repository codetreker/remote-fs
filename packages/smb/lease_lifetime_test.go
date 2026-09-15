package smb

import (
	"context"
	"errors"
	"io/fs"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func countLeaseOrphans(a *authoritySession) int {
	count := 0
	for owner := a.leaseOrphans; owner != nil; owner = owner.orphanNext {
		count++
	}
	return count
}

type leaseClosingSession struct {
	storage.WindowsSession
	failures, closes int
	retired          bool
}

func (s *leaseClosingSession) Close(ctx context.Context) error {
	s.closes++
	if s.failures != 0 {
		s.failures--
		return syscall.EIO
	}
	if err := s.WindowsSession.Close(ctx); err != nil {
		return err
	}
	s.retired = true
	return nil
}

func (s *leaseClosingSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	if s.retired {
		return storage.FileSessionStatus{}, syscall.ESTALE
	}
	return s.WindowsSession.Status(ctx)
}

func TestLeaseUnknownOwnerTransfersToSharedAuthorityUntilConfirmedClose(t *testing.T) {
	c, session, first, _, backend, _ := testConnection(t)
	closing := &leaseClosingSession{WindowsSession: backend, failures: 2}
	authority := &authoritySession{session: closing, refs: 2, epoch: 1}
	session.authorities = map[*Export]*authoritySession{first.export: authority}
	first.authority = authority
	first.session = closing
	first.files.authority = authority
	first.files.session = closing
	first.files.leases = newLeaseOwner(c.server.leases, c.clientGUID, first.export.volumeIdentity)
	first.files.leases.authority = authority
	second := &tree{id: 2, sessionID: session.id, export: first.export, session: closing, authority: authority, done: make(chan struct{}), files: newFileDispatcher(first.export.share.Backend, closing, 1, c.server.config.Limits)}
	second.files.authority = authority
	second.files.leases = newLeaseOwner(c.server.leases, c.clientGUID, first.export.volumeIdentity)
	second.files.leases.authority = authority
	session.trees[2] = second
	first.export.refs++
	backend.queryErr = syscall.EIO
	backend.open = func(storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		return storage.WindowsOpenResult{}, syscall.EIO
	}
	if _, status := first.files.create(t.Context(), leaseCreate(t, "", 9, 1)); status != fileIOError {
		t.Fatalf("unknown create status=%x", status)
	}
	request := signedRequest(t, session, fileRequest(wire.TreeDisconnect, wire.EmptyResponseBody()))
	header := request.Header
	if _, status, _ := c.dispatch(t.Context(), request, request, &header); status != 0 {
		t.Fatalf("first tree disconnect status=%x", status)
	}
	if session.trees[1] != nil || authority.refs != 1 || countLeaseOrphans(authority) != 1 || closing.closes != 1 {
		t.Fatalf("shared authority lost departed owner: tree=%v refs=%d orphans=%d closes=%d pending=%v", session.trees[1] != nil, authority.refs, countLeaseOrphans(authority), closing.closes, first.files.leases.occupied())
	}
	if slots, _ := c.server.leases.counts(); slots != 1 {
		t.Fatal("unknown key charge was released with its tree")
	}
	request.Header.TreeID = 2
	request.Packet = requestPacket(request.Header, request.Body)
	_ = session.signer.Sign(request.Packet)
	header = request.Header
	if _, status, _ := c.dispatch(t.Context(), request, request, &header); status != fileIOError || countLeaseOrphans(authority) != 1 || session.trees[2] == nil {
		t.Fatal("failed authority close discarded lease cleanup ownership")
	}
	if _, status, _ := c.dispatch(t.Context(), request, request, &header); status != 0 {
		t.Fatalf("authority close retry status=%x", status)
	}
	if countLeaseOrphans(authority) != 0 || len(session.authorities) != 0 || closing.closes != 3 || first.files.leases.occupied() {
		t.Fatal("confirmed authority close retained orphan state")
	}
	if slots, bytes := c.server.leases.counts(); slots != 0 || bytes != 0 {
		t.Fatalf("confirmed cleanup retained %d/%d", slots, bytes)
	}
}

func TestProtocolLeaseAcknowledgementCannotGrantRights(t *testing.T) {
	c, session, tree, signer, _ := compoundProtocol(t, false)
	request := leaseCreate(t, "file", 10, 1)
	packet := requestPacket(wire.Header{Command: wire.Create, MessageID: 4, SessionID: session, TreeID: tree, Credits: 1}, request.Body)
	_ = signer.Sign(packet)
	sendFrame(t, c, packet)
	response := readFrame(t, c)
	header, err := wire.ParseHeader(response)
	if err != nil || signer.Verify(response) != nil {
		t.Fatal(err)
	}
	id := leasedID(t, response[64:], header.Status)
	ack := make([]byte, 36)
	smbLE.PutUint16(ack, 36)
	ack[8] = 10
	smbLE.PutUint32(ack[24:], 7)
	packet = requestPacket(wire.Header{Command: wire.OplockBreak, MessageID: 5, SessionID: session, TreeID: tree, Credits: 1}, ack)
	_ = signer.Sign(packet)
	sendFrame(t, c, packet)
	response = readFrame(t, c)
	header, _ = wire.ParseHeader(response)
	if header.Status != 0xc0000001 || signer.Verify(response) != nil {
		t.Fatalf("unsolicited ACK=%+v", header)
	}
	closeBody := make([]byte, 24)
	smbLE.PutUint16(closeBody, 24)
	copy(closeBody[8:], id[:])
	packet = requestPacket(wire.Header{Command: wire.Close, MessageID: 6, SessionID: session, TreeID: tree, Credits: 1}, closeBody)
	_ = signer.Sign(packet)
	sendFrame(t, c, packet)
	header, _ = wire.ParseHeader(readFrame(t, c))
	if header.Status != 0 {
		t.Fatal(header.Status)
	}
	packet = requestPacket(wire.Header{Command: wire.OplockBreak, MessageID: 7, SessionID: session, TreeID: tree, Credits: 1}, ack)
	_ = signer.Sign(packet)
	sendFrame(t, c, packet)
	response = readFrame(t, c)
	header, _ = wire.ParseHeader(response)
	if header.Status != 0xc0000034 || signer.Verify(response) != nil {
		t.Fatalf("retired lease ACK=%+v", header)
	}
}

func TestLeaseShutdownCannotReportSuccessWithUnownedTokens(t *testing.T) {
	server, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	admission, err := server.leases.begin(t.Context(), [16]byte{}, wire.LeaseRequest{Version: 1, Key: [16]byte{1}}, "volume", "file")
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Shutdown(t.Context()); !errors.Is(err, ErrBusy) {
		t.Fatalf("unknown lease cleanup reported success: %v", err)
	}
	admission.releaseKnownFailure()
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if server.Status().LeaseSlots != 0 || server.Status().LeaseBytes != 0 {
		t.Fatal("released lease charges remain observable")
	}
}

func TestLeaseAuthorityPrunesSettledOrphans(t *testing.T) {
	table := newLeaseTable(DefaultLimits())
	authority := &authoritySession{}
	for key := byte(1); key <= 8; key++ {
		owner := newLeaseOwner(table, [16]byte{}, "volume")
		pending, err := owner.begin(t.Context(), wire.LeaseRequest{Version: 1, Key: [16]byte{key}}, "file")
		if err != nil {
			t.Fatal(err)
		}
		owner.retire()
		authority.retainLeaseOrphan(owner)
		if countLeaseOrphans(authority) != 1 {
			t.Fatal("orphan was not retained")
		}
		pending.abort()
		authority.pruneLeaseOrphans()
		if countLeaseOrphans(authority) != 0 {
			t.Fatal("empty retired owner accumulated")
		}
	}
	if slots, bytes := table.counts(); slots != 0 || bytes != 0 {
		t.Fatalf("retired names retained %d/%d", slots, bytes)
	}
}

type leaseDelayedCloseReply struct {
	*commandFile
	completed chan struct{}
	resume    chan struct{}
}

func (f *leaseDelayedCloseReply) Close(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	result, err := f.commandFile.Close(ctx, id)
	close(f.completed)
	<-f.resume
	return result, err
}

func TestSharedAuthorityCloseLinearizesBeforeLateLeaseInstall(t *testing.T) {
	for _, failures := range []int{0, 1} {
		t.Run(map[int]string{0: "confirmed", 1: "unknown"}[failures], func(t *testing.T) {
			d, file, base, table := leaseTestDispatcher(t)
			closing := &leaseClosingSession{WindowsSession: base, failures: failures}
			authority := &authoritySession{session: closing, refs: 2, epoch: 1}
			d.authority = authority
			d.leases.authority = authority
			d.session = closing
			root := &leaseDelayedCloseReply{commandFile: &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 1, Mode: fs.ModeDir}}, NameInfo: storage.WindowsNameInfo{State: storage.WindowsNameRoot}}}, completed: make(chan struct{}), resume: make(chan struct{})}
			base.open = func(request storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
				if request.Lookup.Name == "" {
					return storage.WindowsOpenResult{File: root, Attr: root.attr}, nil
				}
				return storage.WindowsOpenResult{File: file, Attr: file.attr}, nil
			}
			request := leaseCreate(t, "file", byte(20+failures), 1)
			done := make(chan uint32, 1)
			go func() { _, status := d.create(t.Context(), request); done <- status }()
			<-root.completed
			other := newFileDispatcher(nil, closing, 1, DefaultLimits())
			other.authority = authority
			other.fence()
			close(root.resume)
			if status := <-done; status != fileIOError || len(d.handles) != 0 {
				t.Fatalf("late install status=%x handles=%d", status, len(d.handles))
			}
			if !authority.isStopping() || authority.isClosed() != (failures == 0) {
				t.Fatal("authority completion state is inaccurate")
			}
			if slots, _ := table.counts(); slots != 1 {
				t.Fatal("late reference lost its charged cleanup token")
			}
			if err := authority.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			d.leases.releaseAll()
			if slots, bytes := table.counts(); slots != 0 || bytes != 0 {
				t.Fatalf("confirmed cleanup retained %d/%d", slots, bytes)
			}
		})
	}
}

func TestSharedConfirmedFenceDrainsTreesWithoutRetiredActions(t *testing.T) {
	c, session, first, _, backend, _ := testConnection(t)
	closing := &leaseClosingSession{WindowsSession: backend}
	authority := &authoritySession{session: closing, refs: 2, epoch: 1}
	session.authorities = map[*Export]*authoritySession{first.export: authority}
	first.authority = authority
	first.files.authority = authority
	first.files.session = closing
	second := &tree{id: 2, sessionID: session.id, export: first.export, session: closing, authority: authority, files: newFileDispatcher(first.export.share.Backend, closing, 1, DefaultLimits())}
	second.files.authority = authority
	file := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 9}}}}
	second.files.handles[wire.FileID{2}] = &fileHandle{file: file}
	session.trees[2] = second
	first.export.refs++
	first.files.fence()
	if !authority.isClosed() {
		t.Fatal("successful fence close was not published")
	}
	if err := c.logoff(t.Context(), session); err != nil {
		t.Fatalf("closed shared authority could not log off: %v", err)
	}
	if len(session.trees) != 0 || len(session.authorities) != 0 || closing.closes != 1 {
		t.Fatal("retired authority action allocation blocked cleanup")
	}
}
