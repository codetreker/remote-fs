package fileaccess

func (c *Coordinator) RetireOwner(owner Owner) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	if !owner.valid() {
		return ErrInvalid
	}
	if c.owners[owner] == nil {
		for _, wait := range c.waits {
			if wait.owner == owner {
				c.finishWaitLocked(wait, ErrRetired)
			}
		}
		return nil
	}
	if err := c.changeLocked(func(candidate Owner) bool { return candidate == owner }); err != nil {
		return err
	}
	c.removeOwnerLocked(owner)
	return nil
}

func (c *Coordinator) RetireSession(session uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	if session == 0 {
		return ErrInvalid
	}
	if err := c.changeLocked(func(owner Owner) bool { return owner.Session == session }); err != nil {
		return err
	}
	for handle, record := range c.claims {
		if record.session == session {
			c.removeClaimLocked(handle)
		}
	}
	for owner := range c.owners {
		if owner.Session == session {
			c.removeOwnerLocked(owner)
		}
	}
	return nil
}

func (c *Coordinator) removeOwnerLocked(owner Owner) {
	record := c.owners[owner]
	for scope := range record.sets {
		delete(c.scopes[scope], owner)
		if len(c.scopes[scope]) == 0 {
			delete(c.scopes, scope)
		}
	}
	c.ranges -= record.ranges
	delete(c.owners, owner)
}
