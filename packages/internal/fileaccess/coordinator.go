package fileaccess

import (
	"math"
	"sync"
)

type claimRecord struct {
	session, resource uint64
	claim             Claim
}

type ownerRecord struct {
	sets   map[Scope][]Acquisition
	ranges int
}

// Coordinator starts no workers and performs no I/O.
// Native expiry must revoke publication rights before retiring its state here.
type Coordinator struct {
	mu                       sync.Mutex
	limits                   Limits
	revision                 uint64
	failure                  error
	claims                   map[uint64]claimRecord
	claimResources           map[uint64]map[uint64]claimRecord
	owners                   map[Owner]*ownerRecord
	scopes                   map[Scope]map[Owner][]Acquisition
	ranges                   int
	waits                    map[uint64]*Wait
	waitRanges, dependencies int
}

func New(limits Limits) (*Coordinator, error) {
	if err := limits.Check(); err != nil {
		return nil, err
	}
	return &Coordinator{limits: limits, revision: 1, claims: make(map[uint64]claimRecord), claimResources: make(map[uint64]map[uint64]claimRecord), owners: make(map[Owner]*ownerRecord), scopes: make(map[Scope]map[Owner][]Acquisition), waits: make(map[uint64]*Wait)}, nil
}

func (c *Coordinator) Revision() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.revision, c.failure
}

func (c *Coordinator) Invalidate() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	return c.changeLocked(nil)
}

func (c *Coordinator) changeLocked(retired func(Owner) bool) error {
	if c.revision == math.MaxUint64 {
		c.failure = ErrExhausted
		c.finishWaitsLocked(ErrExhausted, nil)
		return c.failure
	}
	c.revision++
	c.finishWaitsLocked(nil, retired)
	return nil
}

func (c *Coordinator) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failure = ErrClosed
	c.finishWaitsLocked(ErrClosed, nil)
	clear(c.claims)
	clear(c.claimResources)
	clear(c.owners)
	clear(c.scopes)
	c.ranges = 0
}

func (c *Coordinator) OwnerRangeCount(owner Owner) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return 0, c.failure
	}
	if !owner.valid() {
		return 0, ErrInvalid
	}
	if record := c.owners[owner]; record != nil {
		return record.ranges, nil
	}
	return 0, nil
}
