package smb

import "context"

// The installation gate spans both the authority Close and the final FileID/lease
// insertion. A late successful backend result cannot install after closure.
func (a *authoritySession) close(ctx context.Context) error {
	a.installMu.Lock()
	defer a.installMu.Unlock()
	if a.closed {
		return nil
	}
	a.stopping = true
	if err := a.session.Close(ctx); err != nil {
		return err
	}
	a.closed = true
	return nil
}

func (a *authoritySession) isClosed() bool {
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	return a.closed
}

func (a *authoritySession) isStopping() bool {
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	return a.stopping
}

// These helpers run while authoritySession.mu is held. Intrusive links do not
// retain a high-water backing array after a formerly occupied owner settles.
func (a *authoritySession) removeLeaseOrphan(owner *leaseOwner) {
	if owner.orphanPrev != nil {
		owner.orphanPrev.orphanNext = owner.orphanNext
	} else {
		a.leaseOrphans = owner.orphanNext
	}
	if owner.orphanNext != nil {
		owner.orphanNext.orphanPrev = owner.orphanPrev
	}
	owner.orphanPrev, owner.orphanNext, owner.orphanAuthority = nil, nil, nil
}

func (a *authoritySession) pruneLeaseOrphans() {
	for owner := a.leaseOrphans; owner != nil; {
		next := owner.orphanNext
		if !owner.occupied() {
			a.removeLeaseOrphan(owner)
		}
		owner = next
	}
}

func (a *authoritySession) retainLeaseOrphan(owner *leaseOwner) {
	a.pruneLeaseOrphans()
	if owner.occupied() && owner.orphanAuthority == nil {
		owner.orphanAuthority = a
		owner.orphanNext = a.leaseOrphans
		if a.leaseOrphans != nil {
			a.leaseOrphans.orphanPrev = owner
		}
		a.leaseOrphans = owner
	}
}

func (a *authoritySession) releaseLeaseOrphans() {
	for a.leaseOrphans != nil {
		owner := a.leaseOrphans
		a.removeLeaseOrphan(owner)
		owner.releaseAll()
	}
}
