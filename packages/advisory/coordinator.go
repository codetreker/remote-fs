// Package advisory owns bounded use claims, range protections, and control
// history within one native volume. Its caller owns reference lifetimes and
// native publication ordering.
package advisory

import (
	"context"
	"fmt"
	"math"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// Config bounds aggregate state across all sessions in one volume.
// MaxMaterializedBytes must accommodate both the current and replacement body
// of a file at MaxFileBytes; each file must also fit the platform slice size.
type Config struct {
	MaxSessions, MaxOwners, MaxRanges, MaxRequests, MaxWaiters, MaxDeadlockEdges int
	MaxCommands                                                                  int
	MaxMaterializedBytes                                                         int64
	MaxMaterializations                                                          int
	MaxFileBytes                                                                 int64
	MaxFileAttempts                                                              int
	FileOperationTimeout                                                         time.Duration
}

func DefaultConfig() Config {
	return Config{
		MaxSessions: 1024, MaxOwners: 32768, MaxRanges: 262144,
		MaxRequests: 262144, MaxWaiters: 8192, MaxDeadlockEdges: 65536,
		MaxCommands:          64,
		MaxMaterializedBytes: 2 << 30, MaxMaterializations: 32, MaxFileBytes: 1 << 30,
		MaxFileAttempts: 8, FileOperationTimeout: 30 * time.Second,
	}
}

func (c Config) Check() error {
	if c.MaxSessions <= 0 || c.MaxOwners <= 0 || c.MaxRanges <= 0 ||
		c.MaxRequests <= 0 || c.MaxWaiters < 0 || c.MaxDeadlockEdges <= 0 || c.MaxCommands <= 0 ||
		c.MaxMaterializedBytes <= 0 || c.MaxMaterializations <= 0 ||
		c.MaxFileBytes <= 0 ||
		c.MaxFileAttempts <= 0 || c.FileOperationTimeout <= 0 {
		return fmt.Errorf("advisory coordinator limits are invalid: %w", syscall.EINVAL)
	}
	if c.MaxFileBytes > int64(^uint(0)>>1) {
		return fmt.Errorf("file limit exceeds the platform slice size: %w", syscall.EINVAL)
	}
	if c.MaxFileBytes > c.MaxMaterializedBytes/2 {
		return fmt.Errorf("volume materialization limit must cover two maximum-size file buffers: %w", syscall.EINVAL)
	}
	return nil
}

// Coordinator is shared by every access path to one native volume. It starts no
// workers. Native callbacks run without its mutex; session leases remain external.
type Coordinator struct {
	mu                sync.Mutex
	config            Config
	now               func() time.Time
	nextSession       uint64
	sessions          map[uint64]*Session
	owners            map[ownerKey]*ownerState
	waiting           []*request
	ranges, requests  int
	registeredOwners  int
	uses              map[storage.UseScope]useClaim
	materializedBytes int64
	materializations  int
}

type actor struct {
	session uint64
	owner   storage.UseOwner
}

type ownerBinding struct {
	node    uint64
	scope   storage.UseScope
	options storage.OwnerOptions
}

type ownerKey struct {
	actor
	node   uint64
	domain storage.ConflictDomain
}

type ownerState struct {
	ranges  []rangeClaim
	pending int
}

type rangeClaim struct {
	id      storage.ClaimID
	command storage.RangeCommand
}

// Order acquires the native observation/publication gate, checks the original
// reference and its session, and calls transition while that gate remains held.
// transition performs bounded in-memory work only. Order must never be entered
// while the coordinator mutex is held, or from an already ordered native callback.
type Order func(context.Context, func() error) error

type request struct {
	key        ownerKey
	id         storage.LockRequestID
	epoch      uint64
	commands   []storage.RangeCommand
	result     storage.RangeAttempt
	expires    time.Time
	order      Order
	pending    bool
	prepared   bool
	converting bool
}

func New(config Config) (*Coordinator, error) {
	if err := config.Check(); err != nil {
		return nil, err
	}
	return &Coordinator{config: config, now: time.Now,
		sessions: make(map[uint64]*Session), owners: make(map[ownerKey]*ownerState),
		uses: make(map[storage.UseScope]useClaim)}, nil
}

// NewSession scopes opaque owners and receipts to a new incarnation.
// fence must revoke all native publication rights before returning nil. Retire
// invokes it without the coordinator mutex; failure preserves existing grants.
func (c *Coordinator) NewSession(options storage.FileSessionOptions, fence func() error) (*Session, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	if fence == nil {
		return nil, fmt.Errorf("advisory session requires a publication fence: %w", syscall.EINVAL)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sessions) >= c.config.MaxSessions || c.nextSession == math.MaxUint64 {
		return nil, syscall.ENOLCK
	}
	c.nextSession++
	s := &Session{coordinator: c, id: c.nextSession, options: options, fence: fence,
		epoch: 1, epochUntil: c.now().Add(options.History), actions: make(map[storage.LockRequestID]*request),
		bindings: make(map[storage.UseOwner]ownerBinding)}
	c.sessions[s.id] = s
	return s, nil
}

func (c *Coordinator) ownerLocked(key ownerKey) *ownerState {
	if o := c.owners[key]; o != nil {
		return o
	}
	o := &ownerState{}
	c.owners[key] = o
	return o
}

func (c *Coordinator) pruneOwnerLocked(key ownerKey) {
	if o := c.owners[key]; o != nil && len(o.ranges) == 0 && o.pending == 0 {
		delete(c.owners, key)
	}
}

func (c *Coordinator) replaceLocked(key ownerKey, ranges []rangeClaim) error {
	o := c.owners[key]
	s := c.sessions[key.session]
	delta := len(ranges) - len(o.ranges)
	if delta > c.config.MaxRanges-c.ranges || delta > s.options.MaxLockRanges-s.ranges {
		return syscall.ENOLCK
	}
	// Refunded fragments must not remain reachable through spare slice capacity.
	if len(ranges) == 0 {
		o.ranges = nil
	} else {
		o.ranges = make([]rangeClaim, len(ranges))
		copy(o.ranges, ranges)
	}
	c.ranges += delta
	s.ranges += delta
	return nil
}

func checkCall(ctx context.Context, node uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if node == 0 {
		return syscall.EINVAL
	}
	return nil
}
