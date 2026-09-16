package smb

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func leaseCreate(t *testing.T, name string, key byte, disposition uint32) wire.Request {
	t.Helper()
	access := uint32(0x80)
	if disposition == 5 {
		access |= 2
	}
	request := createCommand(name, access, disposition)
	request.Body[3] = wire.OplockLease
	data, err := wire.LeaseResponseData(wire.LeaseResponse{Version: wire.LeaseVersion2, Key: [16]byte{key}, Epoch: 65535})
	if err != nil {
		t.Fatal(err)
	}
	smbLE.PutUint32(data[16:], 7)
	return requestContexts(t, request, []wire.CreateContext{{Name: []byte("RqLs"), Data: data}})
}

func leaseTestDispatcher(t *testing.T) (*fileDispatcher, *commandFile, *commandSession, *leaseTable) {
	t.Helper()
	d, file, session, id := commandDispatcher()
	delete(d.handles, id)
	file.attr.Kind = storage.NodeRegular
	file.attr.NameInfo = windowsNameInfo{State: windowsNameLinked, Path: "file"}
	table := newLeaseTable(DefaultLimits())
	d.leases = newLeaseOwner(table, [16]byte{1}, "authority:volume")
	session.open = func(request windowsOpenRequest) (windowsOpenResult, error) {
		if request.Lookup.Name == "" {
			root := &commandFile{attr: windowsAttr{windowsBasicAttr: windowsBasicAttr{Attr: storage.Attr{ID: 1, Kind: storage.NodeDirectory}}, NameInfo: windowsNameInfo{State: windowsNameRoot}}}
			return windowsOpenResult{File: root, Attr: root.attr}, nil
		}
		return windowsOpenResult{File: file, Attr: file.attr, CreateAction: windowsOpened}, nil
	}
	t.Cleanup(func() { _ = session.Close(context.Background()); d.leases.releaseAll() })
	return d, file, session, table
}

func leasedID(t *testing.T, body []byte, status uint32) wire.FileID {
	t.Helper()
	if status != 0 || len(body) < 168 || body[2] != wire.OplockLease {
		t.Fatalf("lease open status=%x body=%x", status, body)
	}
	offset := int(smbLE.Uint32(body[80:])) - 64
	if offset < 88 || offset+44 > len(body) || string(body[offset+16:offset+20]) != "RqLs" || smbLE.Uint32(body[offset+40:]) != 0 {
		t.Fatal("lease response granted cache rights or lost context")
	}
	var id wire.FileID
	copy(id[:], body[64:80])
	return id
}

func TestLeaseCreatePinsIdentityBeforeMutation(t *testing.T) {
	d, file, session, table := leaseTestDispatcher(t)
	body, status := d.create(t.Context(), leaseCreate(t, "file", 1, 1))
	firstID := leasedID(t, body, status)
	base := session.open
	mutations := 0
	session.open = func(request windowsOpenRequest) (windowsOpenResult, error) {
		if request.Disposition == windowsOverwriteIf {
			mutations++
		}
		if request.Lookup.Name == "other" {
			other := &commandFile{attr: windowsAttr{windowsBasicAttr: windowsBasicAttr{Attr: storage.Attr{ID: 8, Kind: storage.NodeRegular}}, NameInfo: windowsNameInfo{State: windowsNameLinked, Path: "other"}}}
			return windowsOpenResult{File: other, Attr: other.attr}, nil
		}
		if request.Lookup.Name == "renamed" && request.Disposition == windowsOverwriteIf && request.Lookup.ExpectedID != 7 {
			t.Error("mutating reuse omitted ExpectedID")
		}
		return base(request)
	}
	if _, status := d.create(t.Context(), leaseCreate(t, "other", 1, 5)); status != fileInvalidParameter || mutations != 0 {
		t.Fatalf("conflicting key status=%x mutations=%d", status, mutations)
	}
	file.attr.NameInfo.Path = "renamed"
	body, status = d.create(t.Context(), leaseCreate(t, "renamed", 1, 5))
	secondID := leasedID(t, body, status)
	if mutations != 1 {
		t.Fatal("same retained object could not reopen after rename")
	}
	if slots, _ := table.counts(); slots != 2 {
		t.Fatalf("lease slots=%d", slots)
	}
	for _, id := range []wire.FileID{firstID, secondID} {
		closeBody := make([]byte, 24)
		smbLE.PutUint16(closeBody, 24)
		copy(closeBody[8:], id[:])
		if _, status := d.closeOrFlush(t.Context(), fileRequest(wire.Close, closeBody)); status != 0 {
			t.Fatalf("leased CLOSE status=%x", status)
		}
	}
	if slots, bytes := table.counts(); slots != 0 || bytes != 0 {
		t.Fatalf("last close retained %d/%d", slots, bytes)
	}
}

func TestLeaseRootReuseKeepsTheCanonicalRootLookup(t *testing.T) {
	d, _, session, table := leaseTestDispatcher(t)
	base := session.open
	calls := 0
	session.open = func(request windowsOpenRequest) (windowsOpenResult, error) {
		calls++
		if err := request.Lookup.Check(); err != nil || request.Lookup.ExpectedID != 0 {
			t.Fatalf("root lookup=%+v %v", request.Lookup, err)
		}
		return base(request)
	}
	for range 2 {
		r := leaseCreate(t, "", 2, 1)
		smbLE.PutUint32(r.Body[40:], 1)
		body, status := d.create(t.Context(), r)
		_ = leasedID(t, body, status)
	}
	if calls != 3 {
		t.Fatalf("root observations=%d", calls)
	}
	if slots, _ := table.counts(); slots != 2 {
		t.Fatal(slots)
	}
}

type leaseProbeCloseFailure struct{ *commandFile }

func (f *leaseProbeCloseFailure) Close(context.Context, windowsActionID) (windowsActionResult, error) {
	return windowsActionResult{}, syscall.EIO
}

func TestLeaseProbeCloseFailureRetainsAdmissionBeforeMutation(t *testing.T) {
	d, file, session, table := leaseTestDispatcher(t)
	body, status := d.create(t.Context(), leaseCreate(t, "file", 3, 1))
	_ = leasedID(t, body, status)
	base := session.open
	mutations := 0
	session.queryErr = syscall.EIO
	session.open = func(request windowsOpenRequest) (windowsOpenResult, error) {
		if request.Disposition == windowsOverwriteIf {
			mutations++
		}
		if request.Lookup.Name == "file" && request.Access == 0 {
			return windowsOpenResult{File: &leaseProbeCloseFailure{file}, Attr: file.attr}, nil
		}
		return base(request)
	}
	if _, status := d.create(t.Context(), leaseCreate(t, "file", 3, 5)); status != fileIOError || mutations != 0 {
		t.Fatalf("probe close status=%x mutations=%d", status, mutations)
	}
	if slots, _ := table.counts(); slots != 2 {
		t.Fatalf("unknown probe slots=%d", slots)
	}
	if _, err := table.begin(t.Context(), [16]byte{1}, wire.LeaseRequest{Version: 2, Key: [16]byte{3}}, "authority:volume", "file"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("fenced key reopened: %v", err)
	}
	_ = session.Close(t.Context())
	d.leases.releaseAll()
	if slots, bytes := table.counts(); slots != 0 || bytes != 0 {
		t.Fatalf("authority cleanup retained slots=%d bytes=%d", slots, bytes)
	}
}

func TestLeaseUnknownCreateOwnsQuotaWithoutAFileID(t *testing.T) {
	d, _, session, table := leaseTestDispatcher(t)
	base := session.open
	session.queryErr = syscall.EIO
	session.open = func(request windowsOpenRequest) (windowsOpenResult, error) {
		if request.Lookup.Name != "" {
			return windowsOpenResult{}, syscall.EIO
		}
		return base(request)
	}
	if _, status := d.create(t.Context(), leaseCreate(t, "unknown", 4, 3)); status != fileIOError || len(d.handles) != 0 {
		t.Fatalf("unknown open status=%x handles=%d", status, len(d.handles))
	}
	if slots, _ := table.counts(); slots != 1 || !d.leases.occupied() {
		t.Fatalf("unknown admission lost owner/slot: %d", slots)
	}
	d.leases.retire()
	if slots, _ := table.counts(); slots != 1 {
		t.Fatal("tree retirement falsely confirmed authority cleanup")
	}
	_ = session.Close(t.Context())
	d.leases.releaseAll()
	if slots, bytes := table.counts(); slots != 0 || bytes != 0 {
		t.Fatalf("cleanup retained %d/%d", slots, bytes)
	}
}

func TestLeaseRetirementRejectsLateCreateInstallation(t *testing.T) {
	d, file, session, table := leaseTestDispatcher(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	base := session.open
	session.open = func(request windowsOpenRequest) (windowsOpenResult, error) {
		if request.Lookup.Name != "" {
			once.Do(func() { close(entered) })
			<-resume
			return windowsOpenResult{File: file, Attr: file.attr}, nil
		}
		return base(request)
	}
	request := leaseCreate(t, "file", 5, 1)
	done := make(chan uint32, 1)
	go func() { _, status := d.create(t.Context(), request); done <- status }()
	<-entered
	d.mu.Lock()
	d.failed = true
	d.leases.retire()
	d.mu.Unlock()
	close(resume)
	if status := <-done; status != fileIOError || len(d.handles) != 0 {
		t.Fatalf("late installation status=%x handles=%d", status, len(d.handles))
	}
	if slots, _ := table.counts(); slots != 1 {
		t.Fatal("late admitted reference lost cleanup ownership")
	}
	_ = session.Close(t.Context())
	d.leases.releaseAll()
	if slots, bytes := table.counts(); slots != 0 || bytes != 0 {
		t.Fatalf("cleanup retained %d/%d", slots, bytes)
	}
}

type leaseResolverSession struct {
	*commandSession
	statusCalls, closeCalls int
	fail                    bool
	root                    *commandFile
}

func (s *leaseResolverSession) Status(context.Context) (storage.FileSessionStatus, error) {
	s.statusCalls++
	if s.fail && s.statusCalls > 1 {
		return storage.FileSessionStatus{}, syscall.EAGAIN
	}
	return storage.FileSessionStatus{ActionEpoch: 1}, nil
}
func (s *leaseResolverSession) Close(context.Context) error {
	s.closeCalls++
	if s.fail {
		return syscall.EIO
	}
	s.root.closed++
	return nil
}

func TestLeaseResolverRetainsNonIOCleanupFailure(t *testing.T) {
	d, _, base, table := leaseTestDispatcher(t)
	root := &commandFile{attr: windowsAttr{windowsBasicAttr: windowsBasicAttr{Attr: storage.Attr{ID: 1, Kind: storage.NodeDirectory}}, NameInfo: windowsNameInfo{State: windowsNameRoot}}}
	session := &leaseResolverSession{commandSession: base, fail: true, root: root}
	base.open = func(windowsOpenRequest) (windowsOpenResult, error) {
		return windowsOpenResult{File: root, Attr: root.attr}, nil
	}
	d.session = session
	if _, status := d.create(t.Context(), leaseCreate(t, `parent\file`, 11, 1)); status != statusError(syscall.EAGAIN) {
		t.Fatalf("resolver failure=%x", status)
	}
	if session.closeCalls != 1 || root.closed != 0 || !d.leases.occupied() {
		t.Fatal("unconfirmed parent close lost its authority owner")
	}
	if slots, _ := table.counts(); slots != 1 {
		t.Fatal("non-EIO cleanup refunded the live key reservation")
	}
	session.fail = false
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	d.leases.releaseAll()
	if slots, bytes := table.counts(); slots != 0 || bytes != 0 || root.closed != 1 {
		t.Fatalf("confirmed parent cleanup retained %d/%d", slots, bytes)
	}
}

func TestLeaseMissingTargetWithRetainedProbeCannotMutate(t *testing.T) {
	d, file, session, table := leaseTestDispatcher(t)
	first := leaseCreate(t, "file", 12, 1)
	smbLE.PutUint32(first.Body[24:], 0x10080)
	smbLE.PutUint32(first.Body[40:], 0x1000)
	body, status := d.create(t.Context(), first)
	_ = leasedID(t, body, status)
	base := session.open
	mutations := 0
	session.open = func(request windowsOpenRequest) (windowsOpenResult, error) {
		if request.Disposition == windowsOverwriteIf {
			mutations++
		}
		if request.Lookup.Name == "missing" {
			return windowsOpenResult{File: file, Attr: file.attr}, syscall.ENOENT
		}
		return base(request)
	}
	if _, status := d.create(t.Context(), leaseCreate(t, "missing", 12, 5)); status != 0xc0000034 || mutations != 0 {
		t.Fatalf("retained probe status=%x mutations=%d", status, mutations)
	}
	if slots, _ := table.counts(); slots != 2 {
		t.Fatal("retained failed probe lost its reservation")
	}
}

func TestLeaseSupersedeConflictIsRejectedBeforeReplacement(t *testing.T) {
	d, _, session, _ := leaseTestDispatcher(t)
	body, status := d.create(t.Context(), leaseCreate(t, "file", 19, 1))
	_ = leasedID(t, body, status)
	base := session.open
	mutations := 0
	session.open = func(request windowsOpenRequest) (windowsOpenResult, error) {
		if request.Disposition == windowsSupersede {
			mutations++
		}
		return base(request)
	}
	request := leaseCreate(t, "file", 19, 0)
	smbLE.PutUint32(request.Body[24:], 0x10080)
	if _, status = d.create(t.Context(), request); status != fileInvalidParameter || mutations != 0 {
		t.Fatal(status, mutations)
	}
	fresh := leaseCreate(t, "file", 20, 0)
	smbLE.PutUint32(fresh.Body[24:], 0x10080)
	body, status = d.create(t.Context(), fresh)
	_ = leasedID(t, body, status)
	if mutations != 1 {
		t.Fatal(mutations)
	}
}
