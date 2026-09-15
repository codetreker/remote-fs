package smb

import (
	"context"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Each owner follows a dispatcher until its remaining admissions transfer to the
// shared authority. The token, rather than a pathname or returned FileID, owns
// cleanup for an admission whose outcome is still unknown.
type leaseOwner struct {
	mu                     sync.Mutex
	table                  *leaseTable
	client                 [16]byte
	volume                 string
	authority              *authoritySession
	closed                 bool
	pending                *leaseOpen
	references             *leaseReference
	orphanPrev, orphanNext *leaseOwner
	orphanAuthority        *authoritySession
}

type leaseOpen struct {
	owner                   *leaseOwner
	admission               *leaseAdmission
	prev, next              *leaseOpen
	attached                bool
	uncertain               bool
	file, probe             storage.WindowsFile
	openAction, probeAction storage.WindowsActionID
}

func newLeaseOwner(table *leaseTable, client [16]byte, volume string) *leaseOwner {
	return &leaseOwner{table: table, client: client, volume: volume}
}

func (o *leaseOwner) begin(ctx context.Context, request wire.LeaseRequest, name string) (*leaseOpen, error) {
	admission, err := o.table.begin(ctx, o.client, request, o.volume, name)
	if err != nil {
		return nil, err
	}
	authorityClosed := o.authority != nil && o.authority.isStopping()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || authorityClosed {
		admission.releaseKnownFailure()
		return nil, ErrStopped
	}
	open := &leaseOpen{owner: o, admission: admission, next: o.pending, attached: true}
	if o.pending != nil {
		o.pending.prev = open
	}
	o.pending = open
	return open, nil
}

func (o *leaseOwner) removePending(p *leaseOpen) {
	if !p.attached {
		return
	}
	if p.prev != nil {
		p.prev.next = p.next
	} else {
		o.pending = p.next
	}
	if p.next != nil {
		p.next.prev = p.prev
	}
	p.prev, p.next = nil, nil
	p.attached = false
}

func (p *leaseOpen) beforeAction(id storage.WindowsActionID, probe bool) error {
	authorityClosed := p.owner.authority != nil && p.owner.authority.isStopping()
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	if p.owner.closed || !p.attached || authorityClosed {
		return ErrStopped
	}
	p.uncertain = true
	if probe {
		p.probeAction = id
	} else {
		p.openAction = id
	}
	return nil
}

func (p *leaseOpen) outcome(file storage.WindowsFile, known, probe bool) {
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	p.uncertain = !known
	if probe {
		p.probe = file
	} else {
		p.file = file
	}
}

func (p *leaseOpen) retain() {
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	if p.attached {
		p.uncertain = true
		p.admission.fence()
	}
}

func (p *leaseOpen) abort() {
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	if !p.attached {
		return
	}
	if p.uncertain || p.file != nil || p.probe != nil {
		p.admission.fence()
		return
	}
	p.owner.removePending(p)
	p.admission.releaseKnownFailure()
}

// The dispatcher holds the authority installation read lock across this operation
// and the following FileID insertion; a successful authority Close cannot pass it.
func (p *leaseOpen) install(identity leaseIdentity, deleteOnClose bool) (*leaseReference, *wire.LeaseResponse, error) {
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	if p.owner.closed {
		p.admission.fence()
		return nil, nil, ErrStopped
	}
	if !p.attached || p.uncertain || p.probe != nil {
		return nil, nil, syscall.EIO
	}
	reference, response, err := p.admission.commit(identity, deleteOnClose)
	if err != nil {
		p.admission.fence()
		return nil, nil, err
	}
	p.owner.removePending(p)
	p.file = nil
	reference.owner = p.owner
	reference.ownerNext = p.owner.references
	if reference.ownerNext != nil {
		reference.ownerNext.ownerPrev = reference
	}
	p.owner.references = reference
	return reference, &response, nil
}

func (o *leaseOwner) retire() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	for p := o.pending; p != nil; p = p.next {
		p.admission.fence()
	}
}

func (o *leaseOwner) release(reference *leaseReference) {
	if o == nil || reference == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.releaseReference(reference)
}

func (o *leaseOwner) releaseReference(reference *leaseReference) {
	if reference.owner != o {
		return
	}
	if reference.ownerPrev != nil {
		reference.ownerPrev.ownerNext = reference.ownerNext
	} else {
		o.references = reference.ownerNext
	}
	if reference.ownerNext != nil {
		reference.ownerNext.ownerPrev = reference.ownerPrev
	}
	reference.owner, reference.ownerPrev, reference.ownerNext = nil, nil, nil
	reference.release()
}

func (o *leaseOwner) occupied() bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pending != nil || o.references != nil
}

// releaseAll requires confirmed closure of the authority, including admissions
// without a returned FileID. Retiring an SMB tree alone does not release them.
func (o *leaseOwner) releaseAll() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	for o.pending != nil {
		p := o.pending
		o.removePending(p)
		p.admission.releaseKnownFailure()
		p.file, p.probe = nil, nil
	}
	for o.references != nil {
		o.releaseReference(o.references)
	}
}

func (o *leaseOwner) identity(attr storage.WindowsAttr) (leaseIdentity, error) {
	if attr.ID == 0 || attr.NameInfo.Check() != nil || attr.NameInfo.State == storage.WindowsNameDetached {
		return leaseIdentity{}, syscall.EIO
	}
	root := attr.NameInfo.State == storage.WindowsNameRoot
	if root && !attr.IsDir() {
		return leaseIdentity{}, syscall.EIO
	}
	return leaseIdentity{Volume: o.volume, NodeID: attr.ID, Name: attr.NameInfo.Path, Root: root}, nil
}

func (d *fileDispatcher) leaseLookup(ctx context.Context, p *leaseOpen, lookup storage.WindowsLookup, intent storage.WindowsOpenIntent) (storage.WindowsLookup, error) {
	previous, requireSame := p.admission.expected()
	if previous.NodeID == 0 {
		return lookup, nil
	}
	if requireSame && previous.Volume != p.owner.volume {
		return lookup, syscall.EINVAL
	}
	id, err := d.actionID()
	if err != nil {
		return lookup, err
	}
	if err := p.beforeAction(id, true); err != nil {
		return lookup, err
	}
	probe, known, err := d.openOutcome(ctx, storage.WindowsOpenRequest{Lookup: lookup, WindowsOpenIntent: storage.WindowsOpenIntent{Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen, OpenReparsePoint: intent.OpenReparsePoint}}, id)
	p.outcome(probe.File, known, true)
	if err != nil {
		if storage.ErrnoOf(err) == syscall.ENOENT && !requireSame && known && probe.File == nil {
			return lookup, nil
		}
		return lookup, err
	}
	identity, identityErr := p.owner.identity(probe.Attr)
	closeID, closeErr := d.actionID()
	if closeErr != nil {
		p.retain()
		d.fence()
		return lookup, closeErr
	}
	if err := p.beforeAction(closeID, true); err != nil {
		return lookup, err
	}
	result, closeErr := probe.File.Close(ctx, closeID)
	if d.mutationResult(closeID, result, closeErr) != 0 {
		p.retain()
		d.fence()
		return lookup, syscall.EIO
	}
	p.outcome(nil, true, true)
	if identityErr != nil {
		return lookup, identityErr
	}
	if requireSame && (identity.NodeID != previous.NodeID || identity.Volume != previous.Volume) {
		return lookup, syscall.EINVAL
	}
	if lookup.Name == "" {
		if !identity.Root {
			return lookup, syscall.EIO
		}
		return lookup, lookup.Check()
	}
	if identity.Root {
		return lookup, syscall.EIO
	}
	lookup.ExpectedID = identity.NodeID
	return lookup, lookup.Check()
}
