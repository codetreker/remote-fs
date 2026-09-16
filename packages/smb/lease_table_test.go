package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func leaseTableRequest(version uint16, key byte) wire.LeaseRequest {
	return wire.LeaseRequest{Version: version, Key: [16]byte{key}, State: 7}
}
func leaseTableBegin(t *testing.T, table *leaseTable, request wire.LeaseRequest, identity leaseIdentity) *leaseAdmission {
	t.Helper()
	admission, err := table.begin(t.Context(), [16]byte{}, request, identity.Volume, identity.Name)
	if err != nil {
		t.Fatal(err)
	}
	return admission
}
func leaseTableCommit(t *testing.T, admission *leaseAdmission, identity leaseIdentity, deleteOnClose bool) (*leaseReference, wire.LeaseResponse) {
	t.Helper()
	reference, response, err := admission.commit(identity, deleteOnClose)
	if err != nil {
		t.Fatal(err)
	}
	return reference, response
}
func assertLeaseTableEmpty(t *testing.T, table *leaseTable) {
	t.Helper()
	slots, bytes := table.counts()
	if slots != 0 || bytes != 0 || table.head != nil {
		t.Fatalf("table retains slots=%d bytes=%d head=%p", slots, bytes, table.head)
	}
}

func TestLeaseTableVersionsPreserveMetadataAndGrantNoCaching(t *testing.T) {
	for _, first := range []uint16{wire.LeaseVersion1, wire.LeaseVersion2} {
		t.Run(map[uint16]string{1: "v1", 2: "v2"}[first], func(t *testing.T) {
			table := newLeaseTable(DefaultLimits())
			identity := leaseIdentity{Volume: "volume", NodeID: 1, Root: true}
			req := leaseTableRequest(first, 0)
			req.Epoch = 65535
			req.HasParent = true
			req.ParentKey = [16]byte{9}
			admission := leaseTableBegin(t, table, req, identity)
			if original, required := admission.expected(); original.NodeID != 0 || required {
				t.Fatalf("new key has identity %+v, %v", original, required)
			}
			firstRef, _ := leaseTableCommit(t, admission, identity, false)
			defer firstRef.release()
			for _, version := range []uint16{wire.LeaseVersion1, wire.LeaseVersion2, wire.LeaseVersion1, wire.LeaseVersion2} {
				later := leaseTableRequest(version, 0)
				later.Epoch = 17
				later.State = uint32(version)
				later.HasParent = true
				later.ParentKey = [16]byte{8}
				admission := leaseTableBegin(t, table, later, identity)
				original, required := admission.expected()
				if original != identity || !required {
					t.Fatalf("lost original binding %+v, %v", original, required)
				}
				ref, response := leaseTableCommit(t, admission, identity, false)
				if response.Version != version || response.Key != req.Key {
					t.Fatalf("response identity = %+v", response)
				}
				if version == wire.LeaseVersion2 && first == wire.LeaseVersion2 {
					if response.Epoch != 65535 || !response.HasParent || response.ParentKey != req.ParentKey {
						t.Fatalf("V2 metadata changed: %+v", response)
					}
				} else if response.Epoch != 0 || response.HasParent || response.ParentKey != ([16]byte{}) {
					t.Fatalf("imported V2 metadata: %+v", response)
				}
				encoded, err := wire.LeaseResponseData(response)
				if err != nil {
					t.Fatal(err)
				}
				if binary.LittleEndian.Uint32(encoded[16:20]) != 0 || binary.LittleEndian.Uint64(encoded[24:32]) != 0 {
					t.Fatal("requested caching or duration was granted")
				}
				ref.release()
			}
			if table.ack([16]byte{}, req.Key) != 0xc0000001 {
				t.Fatal("existing nonbreaking lease accepted ACK")
			}
			firstRef.release()
			assertLeaseTableEmpty(t, table)
			if table.ack([16]byte{}, req.Key) != 0xc0000034 {
				t.Fatal("freed lease survives ACK lookup")
			}
		})
	}
}

func TestLeaseTableDeleteOnCloseIsStickyAndKeepsPerOpenBindings(t *testing.T) {
	table := newLeaseTable(DefaultLimits())
	req := leaseTableRequest(wire.LeaseVersion2, 1)
	first := leaseIdentity{Volume: "first", NodeID: 7, Name: "original"}
	a := leaseTableBegin(t, table, req, first)
	one, _ := leaseTableCommit(t, a, first, true)
	second := leaseIdentity{Volume: "second", NodeID: 8, Root: true}
	a = leaseTableBegin(t, table, req, second)
	if identity, requireSame := a.expected(); identity != first || requireSame {
		t.Fatalf("delete-on-close lost exception or original: %+v %v", identity, requireSame)
	}
	two, _ := leaseTableCommit(t, a, second, false)
	if one.binding.identity != first || two.binding.identity != second {
		t.Fatal("new association redirected an older open")
	}
	table.markDirty("second")
	record := table.findLocked(leaseTableKey{key: req.Key}).lease
	if !record.dirty || record.binding.identity != first || !record.deleteOnClose {
		t.Fatal("dirty marking or association reset original lease metadata")
	}
	one.release()
	a = leaseTableBegin(t, table, req, second)
	if identity, required := a.expected(); identity != first || required {
		t.Fatal("sticky delete-on-close ended when its original open closed")
	}
	a.releaseKnownFailure()
	two.release()
	assertLeaseTableEmpty(t, table)
}

func TestLeaseTableRejectsConflictingKeysWithoutRebinding(t *testing.T) {
	table := newLeaseTable(DefaultLimits())
	req := leaseTableRequest(wire.LeaseVersion1, 1)
	original := leaseIdentity{Volume: "volume", NodeID: 7, Name: "file"}
	ref, _ := leaseTableCommit(t, leaseTableBegin(t, table, req, original), original, false)
	if _, err := table.begin(t.Context(), [16]byte{}, req, "another", "file"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("cross-volume reuse = %v", err)
	}
	a := leaseTableBegin(t, table, req, original)
	other := original
	other.NodeID++
	if ref, _, err := a.commit(other, false); !errors.Is(err, syscall.EINVAL) || ref != nil {
		t.Fatalf("replacement was attached: %v", err)
	}
	if _, err := table.begin(t.Context(), [16]byte{}, req, original.Volume, original.Name); !errors.Is(err, syscall.EIO) {
		t.Fatalf("bad outcome did not fence key: %v", err)
	}
	a.releaseKnownFailure()
	a.releaseKnownFailure()
	ref.release()
	ref.release()
	assertLeaseTableEmpty(t, table)
}

func TestLeaseTableReservesBeforeCopyAndTrimsOnlyAfterAssociation(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxOpens = 2
	identity := leaseIdentity{Volume: "volume", NodeID: 7, Name: "f"}
	req := leaseTableRequest(wire.LeaseVersion2, 1)
	full := 2*int64(windowsMaxPathBytes) + int64(len(identity.Volume)) + leaseReservationFixed
	limits.MaxDirectoryBytes = full - 1
	table := newLeaseTable(limits)
	if _, err := table.begin(t.Context(), [16]byte{}, req, identity.Volume, identity.Name); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("underbudget admission = %v", err)
	}
	assertLeaseTableEmpty(t, table)
	limits.MaxDirectoryBytes = full
	table = newLeaseTable(limits)
	admission := leaseTableBegin(t, table, req, identity)
	if slots, bytes := table.counts(); slots != 1 || bytes != full {
		t.Fatalf("reservation = %d slots %d bytes", slots, bytes)
	}
	canonical := identity
	canonical.Name = strings.Repeat("directory/", 200) + "canonical"
	reference, _ := leaseTableCommit(t, admission, canonical, false)
	if slots, bytes := table.counts(); slots != 1 || bytes >= full || bytes < int64(len(canonical.Name)) {
		t.Fatalf("association = %d slots %d bytes", slots, bytes)
	}
	if _, err := table.begin(t.Context(), [16]byte{}, req, canonical.Volume, canonical.Name); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("canonical refresh bypassed full re-reservation: %v", err)
	}
	reference.release()
	assertLeaseTableEmpty(t, table)
	limits = DefaultLimits()
	limits.MaxOpens = 1
	table = newLeaseTable(limits)
	admission = leaseTableBegin(t, table, req, identity)
	if _, err := table.begin(t.Context(), [16]byte{1}, req, identity.Volume, identity.Name); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("global slot bound bypassed across clients: %v", err)
	}
	admission.fence()
	if slots, bytes := table.counts(); slots != 1 || bytes != full {
		t.Fatalf("fenced owner released reservation: %d %d", slots, bytes)
	}
	admission.releaseKnownFailure()
	assertLeaseTableEmpty(t, table)
}

func TestLeaseTableSerializesKeysAndCancelsWaiters(t *testing.T) {
	table := newLeaseTable(DefaultLimits())
	req := leaseTableRequest(wire.LeaseVersion1, 1)
	identity := leaseIdentity{Volume: "volume", NodeID: 7, Name: "file"}
	first := leaseTableBegin(t, table, req, identity)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := table.begin(cancelled, [16]byte{}, req, identity.Volume, identity.Name); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled admission = %v", err)
	}
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	observed := &leaseWaitContext{Context: ctx, entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := table.begin(observed, [16]byte{}, req, identity.Volume, identity.Name)
		done <- err
	}()
	<-observed.entered
	stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter did not leave")
	}
	first.fence()
	if _, err := table.begin(t.Context(), [16]byte{}, req, identity.Volume, identity.Name); !errors.Is(err, syscall.EIO) {
		t.Fatalf("fenced key admitted reuse: %v", err)
	}
	if ref, _, err := first.commit(identity, false); err == nil || ref != nil {
		t.Fatal("retired token accepted delayed commit")
	}
	first.releaseKnownFailure()
	first.fence()
	assertLeaseTableEmpty(t, table)
}

func TestLeaseTableLastCloseLeavesOnlyTheAdmissionBarrier(t *testing.T) {
	table := newLeaseTable(DefaultLimits())
	req := leaseTableRequest(wire.LeaseVersion2, 1)
	req.Epoch = 7
	identity := leaseIdentity{Volume: "volume", NodeID: 7, Name: "file"}
	ref, _ := leaseTableCommit(t, leaseTableBegin(t, table, req, identity), identity, false)
	later := req
	later.Epoch = 19
	later.ParentKey = [16]byte{4}
	later.HasParent = true
	pending := leaseTableBegin(t, table, later, identity)
	ref.release()
	if table.ack([16]byte{}, req.Key) != 0xc0000034 || table.findLocked(leaseTableKey{key: req.Key}).lease != nil {
		t.Fatal("last close left an idle committed lease")
	}
	if expected, same := pending.expected(); expected != identity || !same {
		t.Fatal("last close lost pending preflight snapshot")
	}
	if slots, bytes := table.counts(); slots != 1 || bytes <= pending.charge {
		t.Fatalf("snapshot or barrier lost its charge: %d %d", slots, bytes)
	}
	replacement, response := leaseTableCommit(t, pending, identity, false)
	if response.Epoch != 19 || !response.HasParent || response.ParentKey != later.ParentKey {
		t.Fatalf("freed record retained idle metadata: %+v", response)
	}
	replacement.release()
	assertLeaseTableEmpty(t, table)
}

func TestLeaseTableRenameRetainsOldOpenIdentityAndChargesOldName(t *testing.T) {
	table := newLeaseTable(DefaultLimits())
	req := leaseTableRequest(wire.LeaseVersion1, 1)
	identity := leaseIdentity{Volume: "volume", NodeID: 7, Name: "original"}
	first, _ := leaseTableCommit(t, leaseTableBegin(t, table, req, identity), identity, false)
	table.markDirty(identity.Volume)
	renamed := identity
	renamed.Name = "renamed"
	pending := leaseTableBegin(t, table, req, renamed)
	if expected, same := pending.expected(); expected != identity || !same {
		t.Fatal("rename replaced identity without observation")
	}
	second, _ := leaseTableCommit(t, pending, renamed, false)
	record := table.findLocked(leaseTableKey{key: req.Key}).lease
	if record.binding.identity != renamed || record.dirty || first.binding.identity != identity || second.binding.identity != renamed {
		t.Fatal("canonical refresh redirected a retained association")
	}
	second.release()
	first.release()
	assertLeaseTableEmpty(t, table)
}

type leaseWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *leaseWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestLeaseTableKnownFailureWakesSameKeyWithoutAnIdleRecord(t *testing.T) {
	table := newLeaseTable(DefaultLimits())
	req := leaseTableRequest(wire.LeaseVersion1, 0)
	identity := leaseIdentity{Volume: "volume", NodeID: 7, Name: "file"}
	original := leaseTableBegin(t, table, req, identity)
	ctx := &leaseWaitContext{Context: t.Context(), entered: make(chan struct{})}
	ready := make(chan *leaseAdmission, 1)
	failed := make(chan error, 1)
	go func() {
		a, err := table.begin(ctx, [16]byte{}, req, identity.Volume, identity.Name)
		if err != nil {
			failed <- err
		} else {
			ready <- a
		}
	}()
	select {
	case <-ctx.entered:
	case <-time.After(time.Second):
		t.Fatal("same-key admission did not wait")
	}
	original.releaseKnownFailure()
	select {
	case a := <-ready:
		if expected, same := a.expected(); expected.NodeID != 0 || same {
			t.Fatal("failed provisional open created a lease")
		}
		a.releaseKnownFailure()
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("known cleanup did not wake next admission")
	}
	assertLeaseTableEmpty(t, table)
}

func TestLeaseTableOversizedCanonicalResultRetainsItsCleanupOwner(t *testing.T) {
	table := newLeaseTable(DefaultLimits())
	identity := leaseIdentity{Volume: "volume", NodeID: 7, Name: "file"}
	a := leaseTableBegin(t, table, leaseTableRequest(wire.LeaseVersion2, 1), identity)
	slots, bytes := table.counts()
	identity.Name = strings.Repeat("x", windowsMaxPathBytes+1)
	if ref, _, err := a.commit(identity, false); !errors.Is(err, syscall.EIO) || ref != nil {
		t.Fatalf("oversized result accepted: %v", err)
	}
	if afterSlots, afterBytes := table.counts(); afterSlots != slots || afterBytes != bytes {
		t.Fatal("failed result lost pre-reserved cleanup charge")
	}
	a.releaseKnownFailure()
	if ref, _, err := a.commit(identity, false); err == nil || ref != nil {
		t.Fatal("released admission accepted a late commit")
	}
	assertLeaseTableEmpty(t, table)
}

func TestLeaseTableClientKeysAndIgnoredParentMetadataRemainIndependent(t *testing.T) {
	table := newLeaseTable(DefaultLimits())
	request := leaseTableRequest(wire.LeaseVersion2, 1)
	request.ParentKey = [16]byte{9}
	request.Epoch = 3
	one := leaseIdentity{Volume: "one", NodeID: 1, Root: true}
	first, response := leaseTableCommit(t, leaseTableBegin(t, table, request, one), one, false)
	if response.HasParent || response.ParentKey != ([16]byte{}) {
		t.Fatal("absent parent flag imported a key")
	}
	two := leaseIdentity{Volume: "two", NodeID: 1, Root: true}
	pending, err := table.begin(t.Context(), [16]byte{2}, request, two.Volume, two.Name)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := leaseTableCommit(t, pending, two, false)
	if table.ack([16]byte{3}, request.Key) != 0xc0000034 {
		t.Fatal("another client inherited a lease key")
	}
	first.release()
	if table.ack([16]byte{2}, request.Key) != 0xc0000001 {
		t.Fatal("one client's close released another lease")
	}
	second.release()
	assertLeaseTableEmpty(t, table)
	for _, bad := range []wire.LeaseRequest{{Version: 3}, {Version: wire.LeaseVersion1, State: 8}} {
		if _, err := table.begin(t.Context(), [16]byte{}, bad, "volume", "file"); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid request = %v", err)
		}
	}
	if _, err := table.begin(t.Context(), [16]byte{}, request, "", "file"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("missing volume = %v", err)
	}
	assertLeaseTableEmpty(t, table)
}

func TestLeaseTableReferenceListsReturnToTheirLiveOwnership(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxOpens = 64
	table := newLeaseTable(limits)
	anchors := make([]*leaseReference, 4)
	identities := make([]leaseIdentity, len(anchors))
	for index := range anchors {
		identities[index] = leaseIdentity{Volume: "volume", NodeID: uint64(index + 7), Name: "file"}
		request := leaseTableRequest(wire.LeaseVersion2, byte(index+1))
		anchors[index], _ = leaseTableCommit(t, leaseTableBegin(t, table, request, identities[index]), identities[index], false)
	}
	baselineSlots, baselineBytes := table.counts()
	for round := 0; round < 3; round++ {
		for index, anchor := range anchors {
			request := leaseTableRequest(wire.LeaseVersion2, byte(index+1))
			burst := make([]*leaseReference, 32)
			for i := range burst {
				burst[i], _ = leaseTableCommit(t, leaseTableBegin(t, table, request, identities[index]), identities[index], false)
			}
			record := anchor.record
			if record.refCount != len(burst)+1 {
				t.Fatalf("live reference count = %d", record.refCount)
			}
			traversed := 0
			var previous *leaseReference
			for ref := record.head; ref != nil; ref = ref.tableNext {
				if ref.tablePrev != previous {
					t.Fatal("reference list has a broken backward link")
				}
				traversed++
				previous = ref
			}
			if traversed != record.refCount {
				t.Fatalf("list retains %d nodes for %d opens", traversed, record.refCount)
			}
			for parity := 0; parity < 2; parity++ {
				for i := parity; i < len(burst); i += 2 {
					burst[i].release()
					burst[i].release()
				}
			}
			if record.refCount != 1 || record.head != anchor || anchor.tablePrev != nil || anchor.tableNext != nil {
				t.Fatal("surviving lease retains retired reference capacity or links")
			}
			for _, ref := range burst {
				if ref.tablePrev != nil || ref.tableNext != nil || ref.binding != nil || ref.record != nil {
					t.Fatal("retired reference retains a lease or another reference")
				}
			}
			if slots, bytes := table.counts(); slots != baselineSlots || bytes != baselineBytes {
				t.Fatalf("high-water cycle changed low-water charge: %d/%d, want %d/%d", slots, bytes, baselineSlots, baselineBytes)
			}
		}
	}
	for _, anchor := range anchors {
		anchor.release()
	}
	assertLeaseTableEmpty(t, table)
	size := int64(unsafe.Sizeof(leaseReference{}))
	if leaseReferenceBytes < size || leaseReferenceBytes-size > 15 {
		t.Fatalf("reference charge %d does not match fixed layout %d plus bounded alignment", leaseReferenceBytes, size)
	}
}

func TestLeaseTableEntryListReleasesEveryHighWaterNode(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxOpens = 128
	table := newLeaseTable(limits)
	for round := 0; round < 3; round++ {
		refs := make([]*leaseReference, 64)
		entries := make([]*leaseEntry, len(refs))
		for index := range refs {
			request := leaseTableRequest(wire.LeaseVersion2, byte(index+1))
			identity := leaseIdentity{Volume: "volume", NodeID: uint64(index + 1), Name: "file"}
			refs[index], _ = leaseTableCommit(t, leaseTableBegin(t, table, request, identity), identity, false)
			entries[index] = table.findLocked(leaseTableKey{key: request.Key})
		}
		var previous *leaseEntry
		count := 0
		for entry := table.head; entry != nil; entry = entry.next {
			if entry.prev != previous {
				t.Fatal("entry list has a broken backward link")
			}
			count++
			previous = entry
		}
		if count != len(refs) {
			t.Fatalf("entry list has %d nodes for %d live keys", count, len(refs))
		}
		for parity := 0; parity < 2; parity++ {
			for index := parity; index < len(refs); index += 2 {
				refs[index].release()
			}
		}
		assertLeaseTableEmpty(t, table)
		for _, entry := range entries {
			if entry.prev != nil || entry.next != nil || entry.key != (leaseTableKey{}) || entry.lease != nil || entry.admission != nil {
				t.Fatal("retired entry retains a key, owner or another entry")
			}
		}
	}
}
