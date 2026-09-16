package fileaccess

// ClaimConflict identifies the existing retained use that prevents admission.
type ClaimConflict struct {
	Handle uint64
	Claim  Claim
}

func (c *ClaimConflict) Error() string { return ErrConflict.Error() }
func (c *ClaimConflict) Unwrap() error { return ErrConflict }

func (c *Coordinator) CheckClaim(handle, session, resource uint64, claim Claim) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkClaimLocked(handle, session, resource, claim)
}

func (c *Coordinator) checkClaimLocked(handle, session, resource uint64, claim Claim) error {
	if c.failure != nil {
		return c.failure
	}
	if handle == 0 || session == 0 || resource == 0 {
		return ErrInvalid
	}
	if _, exists := c.claims[handle]; exists {
		return ErrInvalid
	}
	for id, old := range c.claimResources[resource] {
		if claim.Uses&old.claim.Excludes != 0 || old.claim.Uses&claim.Excludes != 0 {
			return &ClaimConflict{Handle: id, Claim: old.claim}
		}
	}
	if len(c.claims) == c.limits.MaxClaims {
		return ErrCapacity
	}
	return nil
}

func (c *Coordinator) RegisterClaim(handle, session, resource uint64, claim Claim, guard Guard) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkClaimLocked(handle, session, resource, claim); err != nil {
		return err
	}
	if err := guard.check(); err != nil {
		return err
	}
	if err := c.changeLocked(nil); err != nil {
		return err
	}
	record := claimRecord{session: session, resource: resource, claim: claim}
	c.claims[handle] = record
	if c.claimResources[resource] == nil {
		c.claimResources[resource] = make(map[uint64]claimRecord)
	}
	c.claimResources[resource][handle] = record
	return nil
}

func (c *Coordinator) CloseClaim(handle uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	if _, exists := c.claims[handle]; !exists {
		return ErrUnknownClaim
	}
	if err := c.changeLocked(nil); err != nil {
		return err
	}
	c.removeClaimLocked(handle)
	return nil
}

func (c *Coordinator) removeClaimLocked(handle uint64) {
	record := c.claims[handle]
	delete(c.claims, handle)
	delete(c.claimResources[record.resource], handle)
	if len(c.claimResources[record.resource]) == 0 {
		delete(c.claimResources, record.resource)
	}
}

// CheckUse validates a supplied handle even for zero use. Handle zero denotes
// an operation without a retained claim; its effect must remain natively ordered.
func (c *Coordinator) CheckUse(resource, handle, uses uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkUseLocked(resource, handle, uses, true)
}

// CheckExclusions checks competing claims for an effect whose authority was
// already established by the native caller. A supplied exception handle must
// still belong to the resource, but need not advertise the effect's uses.
func (c *Coordinator) CheckExclusions(resource, exceptHandle, uses uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkUseLocked(resource, exceptHandle, uses, false)
}

func (c *Coordinator) checkUseLocked(resource, handle, uses uint64, requireUses bool) error {
	if c.failure != nil {
		return c.failure
	}
	if resource == 0 {
		return ErrInvalid
	}
	if handle != 0 {
		record, exists := c.claims[handle]
		if !exists || record.resource != resource {
			return ErrUnknownClaim
		}
		if requireUses && uses & ^record.claim.Uses != 0 {
			return ErrAccess
		}
	}
	for id, other := range c.claimResources[resource] {
		if id != handle && uses&other.claim.Excludes != 0 {
			return &ClaimConflict{Handle: id, Claim: other.claim}
		}
	}
	return nil
}

func (c *Coordinator) ClaimCount(resource uint64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return 0, c.failure
	}
	if resource == 0 {
		return 0, ErrInvalid
	}
	return len(c.claimResources[resource]), nil
}

// ReplaceClaim keeps the existing claim if the replacement conflicts. Native
// callers retain their publication ordering through the corresponding effect.
func (c *Coordinator) ReplaceClaim(handle uint64, claim Claim, guard Guard) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	record, exists := c.claims[handle]
	if !exists {
		return ErrUnknownClaim
	}
	for id, other := range c.claimResources[record.resource] {
		if id != handle && (claim.Uses&other.claim.Excludes != 0 || other.claim.Uses&claim.Excludes != 0) {
			return &ClaimConflict{Handle: id, Claim: other.claim}
		}
	}
	if err := guard.check(); err != nil {
		return err
	}
	if record.claim == claim {
		return nil
	}
	if err := c.changeLocked(nil); err != nil {
		return err
	}
	record.claim = claim
	c.claims[handle], c.claimResources[record.resource][handle] = record, record
	return nil
}
