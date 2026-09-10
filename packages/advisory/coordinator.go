// Package advisory coordinates Linux flock and POSIX record locks within one
// native volume. It owns bounded lock state; its caller owns session leases
// and must retire native publication rights before releasing expired grants.
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
		MaxMaterializedBytes: 2 << 30, MaxMaterializations: 32, MaxFileBytes: 1 << 30,
		MaxFileAttempts: 8, FileOperationTimeout: 30 * time.Second,
	}
}

func (c Config) Check() error {
	if c.MaxSessions <= 0 || c.MaxOwners <= 0 || c.MaxRanges <= 0 ||
		c.MaxRequests <= 0 || c.MaxWaiters < 0 || c.MaxDeadlockEdges <= 0 ||
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

// Coordinator must be shared by every access path to the same native volume.
// It performs no I/O and starts no workers. Session lease management is external.
type Coordinator struct {
	mu                sync.Mutex
	config            Config
	now               func() time.Time
	nextSession       uint64
	sessions          map[uint64]*Session
	owners            map[ownerKey]*ownerState
	waiting           []*request
	ranges, requests  int
	materializedBytes int64
	materializations  int
}

type actor struct {
	session uint64
	owner   storage.LockOwner
}

type ownerKey struct {
	actor
	node   uint64
	family storage.LockFamily
}

type ownerState struct {
	ranges  []storage.FileLock
	pending int
}

type request struct {
	key     ownerKey
	id      storage.LockRequestID
	epoch   uint64
	lock    storage.FileLock
	result  storage.LockAttempt
	expires time.Time
}

func New(config Config) (*Coordinator, error) {
	if err := config.Check(); err != nil {
		return nil, err
	}
	return &Coordinator{config: config, now: time.Now,
		sessions: make(map[uint64]*Session), owners: make(map[ownerKey]*ownerState)}, nil
}

// NewSession scopes opaque kernel owners and receipts to a new incarnation.
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
		epoch: 1, epochUntil: c.now().Add(options.History), actions: make(map[storage.LockRequestID]*request)}
	c.sessions[s.id] = s
	return s, nil
}

func (c *Coordinator) ownerLocked(key ownerKey) (*ownerState, error) {
	if o := c.owners[key]; o != nil {
		return o, nil
	}
	s := c.sessions[key.session]
	if len(c.owners) >= c.config.MaxOwners || s.owners >= s.options.MaxLockOwners {
		return nil, syscall.ENOLCK
	}
	o := &ownerState{}
	c.owners[key] = o
	s.owners++
	return o, nil
}

func (c *Coordinator) pruneOwnerLocked(key ownerKey) {
	if o := c.owners[key]; o != nil && len(o.ranges) == 0 && o.pending == 0 {
		delete(c.owners, key)
		c.sessions[key.session].owners--
	}
}

func (c *Coordinator) replaceLocked(key ownerKey, ranges []storage.FileLock) error {
	o := c.owners[key]
	s := c.sessions[key.session]
	delta := len(ranges) - len(o.ranges)
	if delta > c.config.MaxRanges-c.ranges || delta > s.options.MaxLockRanges-s.ranges {
		return syscall.ENOLCK
	}
	o.ranges = ranges
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
