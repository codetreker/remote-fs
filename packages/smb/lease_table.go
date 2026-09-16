package smb

import (
	"context"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

type leaseIdentity struct {
	Volume string
	NodeID uint64
	Name   string
	Root   bool
}

type leaseTableKey struct{ client, key [16]byte }

type leaseTable struct {
	mu              sync.Mutex
	head            *leaseEntry
	maxSlots, slots int
	maxBytes, bytes int64
}

type leaseEntry struct {
	key        leaseTableKey
	prev, next *leaseEntry
	lease      *leaseRecord
	admission  *leaseAdmission
}

type leaseBinding struct {
	identity leaseIdentity
	owners   int
	bytes    int64
}

type leaseRecord struct {
	binding       *leaseBinding
	head          *leaseReference
	refCount      int
	version       uint16
	epoch         uint16
	hasParent     bool
	parent        [16]byte
	deleteOnClose bool
	dirty         bool
}

type leaseAdmissionState uint8

const (
	leaseReserved leaseAdmissionState = iota
	leaseFenced
	leaseAssociated
	leaseReleased
)

// The fixed charges include table entries, completion channels and ownership
// objects. Variable strings are accounted separately before they are copied.
const (
	leaseReservationFixed int64 = 2048
	leaseEntryBytes       int64 = 128
	leaseRecordBytes      int64 = 256
	leaseReferenceBytes   int64 = int64((unsafe.Sizeof(leaseReference{}) + 15) &^ uintptr(15))
	leaseBindingBytes     int64 = 128
)

type leaseAdmission struct {
	table        *leaseTable
	key          leaseTableKey
	request      wire.LeaseRequest
	volume, name string
	original     *leaseBinding
	requireSame  bool
	charge       int64
	state        leaseAdmissionState
	done         chan struct{}
	doneClosed   bool
}

type leaseReference struct {
	table                *leaseTable
	key                  leaseTableKey
	record               *leaseRecord
	binding              *leaseBinding
	released             bool
	tablePrev, tableNext *leaseReference
	// Owner links are protected by leaseOwner.mu, independently of table links.
	owner                *leaseOwner
	ownerPrev, ownerNext *leaseReference
}

func newLeaseTable(limits Limits) *leaseTable {
	return &leaseTable{maxSlots: limits.MaxOpens, maxBytes: limits.MaxDirectoryBytes}
}

func (t *leaseTable) begin(ctx context.Context, clientGUID [16]byte, request wire.LeaseRequest, volumeIdentity, requestName string) (*leaseAdmission, error) {
	if request.Version != wire.LeaseVersion1 && request.Version != wire.LeaseVersion2 || request.State&^uint32(7) != 0 || volumeIdentity == "" || len(requestName) > windowsMaxPathBytes {
		return nil, syscall.EINVAL
	}
	fixed := 2*int64(windowsMaxPathBytes) + leaseReservationFixed
	if int64(len(volumeIdentity)) > t.maxBytes-fixed {
		return nil, syscall.ENOMEM
	}
	charge := fixed + int64(len(volumeIdentity))
	key := leaseTableKey{client: clientGUID, key: request.Key}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		t.mu.Lock()
		if err := ctx.Err(); err != nil {
			t.mu.Unlock()
			return nil, err
		}
		entry := t.findLocked(key)
		if entry != nil && entry.admission != nil {
			pending := entry.admission
			if pending.state == leaseFenced {
				t.mu.Unlock()
				return nil, syscall.EIO
			}
			done := pending.done
			t.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if entry != nil && entry.lease != nil && !entry.lease.deleteOnClose && entry.lease.binding.identity.Volume != volumeIdentity {
			t.mu.Unlock()
			return nil, syscall.EINVAL
		}
		if t.slots >= t.maxSlots || charge > t.maxBytes-t.bytes {
			t.mu.Unlock()
			return nil, syscall.ENOMEM
		}
		t.slots++
		t.bytes += charge
		if entry == nil {
			entry = t.insertLocked(key)
			charge -= leaseEntryBytes
		}
		admission := &leaseAdmission{table: t, key: key, request: request, volume: strings.Clone(volumeIdentity), name: strings.Clone(requestName), charge: charge, done: make(chan struct{})}
		if entry.lease != nil {
			admission.original = entry.lease.binding
			admission.original.owners++
			admission.requireSame = !entry.lease.deleteOnClose
		}
		entry.admission = admission
		t.mu.Unlock()
		return admission, nil
	}
}

func (a *leaseAdmission) expected() (leaseIdentity, bool) {
	a.table.mu.Lock()
	defer a.table.mu.Unlock()
	if a.original == nil {
		return leaseIdentity{}, false
	}
	return a.original.identity, a.requireSame
}

func (a *leaseAdmission) commit(identity leaseIdentity, deleteOnClose bool) (*leaseReference, wire.LeaseResponse, error) {
	t := a.table
	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.findLocked(a.key)
	if a.state != leaseReserved || entry == nil || entry.admission != a {
		return nil, wire.LeaseResponse{}, syscall.EIO
	}
	if identity.Volume != a.volume || identity.NodeID == 0 || len(identity.Name) > windowsMaxPathBytes {
		a.fenceLocked()
		return nil, wire.LeaseResponse{}, syscall.EIO
	}
	state := windowsNameLinked
	if identity.Root {
		state = windowsNameRoot
	}
	if err := (windowsNameInfo{State: state, Path: identity.Name}).Check(); err != nil {
		a.fenceLocked()
		return nil, wire.LeaseResponse{}, err
	}
	if a.requireSame && a.original != nil && !sameLeaseObject(a.original.identity, identity) {
		a.fenceLocked()
		return nil, wire.LeaseResponse{}, syscall.EINVAL
	}
	// Convert the reserved budget under the same mutex as installation. Subtracting
	// first avoids transient overflow near the configured byte ceiling.
	t.bytes -= a.charge
	a.charge = 0
	record := entry.lease
	var binding *leaseBinding
	if record != nil && record.binding.identity == identity {
		binding = record.binding
	} else {
		binding = &leaseBinding{identity: leaseIdentity{Volume: a.volume, NodeID: identity.NodeID, Name: strings.Clone(identity.Name), Root: identity.Root}, bytes: leaseBindingBytes + int64(len(a.volume)) + int64(len(identity.Name))}
		t.bytes += binding.bytes
	}
	if record == nil {
		record = &leaseRecord{binding: binding, version: a.request.Version}
		if a.request.Version == wire.LeaseVersion2 {
			record.epoch = a.request.Epoch
			record.hasParent = a.request.HasParent
			if record.hasParent {
				record.parent = a.request.ParentKey
			}
		}
		binding.owners++
		entry.lease = record
		t.bytes += leaseRecordBytes
	} else if sameLeaseObject(record.binding.identity, identity) && record.binding != binding {
		t.releaseBindingLocked(record.binding)
		record.binding = binding
		binding.owners++
	}
	reference := &leaseReference{table: t, key: a.key, record: record, binding: binding}
	binding.owners++
	reference.tableNext = record.head
	if record.head != nil {
		record.head.tablePrev = reference
	}
	record.head = reference
	record.refCount++
	record.deleteOnClose = record.deleteOnClose || deleteOnClose
	if sameLeaseObject(record.binding.identity, identity) {
		record.dirty = false
	}
	t.bytes += leaseReferenceBytes
	response := wire.LeaseResponse{Version: a.request.Version, Key: a.key.key}
	if a.request.Version == wire.LeaseVersion2 {
		response.Epoch = record.epoch
		response.HasParent = record.hasParent
		if record.hasParent {
			response.ParentKey = record.parent
		}
	}
	// The full reservation already covers the newly associated binding and record.
	// Old bindings keep their own charge while another open or admission owns them.
	t.releaseBindingLocked(a.original)
	a.original = nil
	a.volume, a.name = "", ""
	a.state = leaseAssociated
	entry.admission = nil
	a.finishLocked()
	return reference, response, nil
}

func sameLeaseObject(a, b leaseIdentity) bool {
	return a.Volume == b.Volume && a.NodeID == b.NodeID && a.Root == b.Root
}

func (a *leaseAdmission) releaseKnownFailure() {
	t := a.table
	t.mu.Lock()
	defer t.mu.Unlock()
	if a.state == leaseReleased || a.state == leaseAssociated {
		return
	}
	entry := t.findLocked(a.key)
	if entry != nil && entry.admission == a {
		entry.admission = nil
	}
	t.slots--
	t.bytes -= a.charge
	a.charge = 0
	t.releaseBindingLocked(a.original)
	a.original = nil
	a.volume, a.name = "", ""
	a.state = leaseReleased
	a.finishLocked()
	t.removeEmptyLocked(entry)
}

func (a *leaseAdmission) fence() { a.table.mu.Lock(); defer a.table.mu.Unlock(); a.fenceLocked() }
func (a *leaseAdmission) fenceLocked() {
	if a.state == leaseReserved {
		a.state = leaseFenced
		a.finishLocked()
	}
}
func (a *leaseAdmission) finishLocked() {
	if !a.doneClosed {
		close(a.done)
		a.doneClosed = true
	}
}

func (r *leaseReference) release() {
	t := r.table
	t.mu.Lock()
	defer t.mu.Unlock()
	if r.released {
		return
	}
	r.released = true
	t.slots--
	t.bytes -= leaseReferenceBytes
	t.releaseBindingLocked(r.binding)
	r.binding = nil
	if r.tablePrev != nil {
		r.tablePrev.tableNext = r.tableNext
	} else {
		r.record.head = r.tableNext
	}
	if r.tableNext != nil {
		r.tableNext.tablePrev = r.tablePrev
	}
	r.tablePrev, r.tableNext = nil, nil
	r.record.refCount--
	entry := t.findLocked(r.key)
	if r.record.refCount == 0 {
		if entry != nil && entry.lease == r.record {
			entry.lease = nil
		}
		t.releaseBindingLocked(r.record.binding)
		r.record.binding = nil
		r.record.head = nil
		t.bytes -= leaseRecordBytes
	}
	r.record = nil
	t.removeEmptyLocked(entry)
}

func (t *leaseTable) releaseBindingLocked(binding *leaseBinding) {
	if binding == nil {
		return
	}
	binding.owners--
	if binding.owners == 0 {
		t.bytes -= binding.bytes
		binding.identity = leaseIdentity{}
	}
}
func (t *leaseTable) removeEmptyLocked(entry *leaseEntry) {
	if entry != nil && entry.lease == nil && entry.admission == nil {
		t.removeLocked(entry)
		t.bytes -= leaseEntryBytes
	}
}

func (t *leaseTable) findLocked(key leaseTableKey) *leaseEntry {
	for entry := t.head; entry != nil; entry = entry.next {
		if entry.key == key {
			return entry
		}
	}
	return nil
}
func (t *leaseTable) insertLocked(key leaseTableKey) *leaseEntry {
	entry := &leaseEntry{key: key, next: t.head}
	if t.head != nil {
		t.head.prev = entry
	}
	t.head = entry
	return entry
}
func (t *leaseTable) removeLocked(entry *leaseEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		t.head = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	}
	entry.prev, entry.next = nil, nil
	entry.key = leaseTableKey{}
}

func (t *leaseTable) markDirty(volume string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for entry := t.head; entry != nil; entry = entry.next {
		record := entry.lease
		if record == nil {
			continue
		}
		if record.binding.identity.Volume == volume {
			record.dirty = true
			continue
		}
		for reference := record.head; reference != nil; reference = reference.tableNext {
			if reference.binding.identity.Volume == volume {
				record.dirty = true
				break
			}
		}
	}
}

func (t *leaseTable) ack(clientGUID, key [16]byte) uint32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.findLocked(leaseTableKey{client: clientGUID, key: key})
	if entry == nil || entry.lease == nil {
		return 0xc0000034
	}
	return 0xc0000001
}

func (t *leaseTable) counts() (int, int64) { t.mu.Lock(); defer t.mu.Unlock(); return t.slots, t.bytes }
