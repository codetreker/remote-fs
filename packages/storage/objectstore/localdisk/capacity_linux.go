package localdisk

import (
	"fmt"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

type physicalCapacity struct {
	mu       sync.Mutex
	reserved int64
}

func (c *physicalCapacity) reserve(rootFD int, bytes, maintenance int64, ops fileOperations) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var st unix.Statfs_t
	if err := ops.fstatfs(rootFD, &st); err != nil {
		return err
	}
	space, err := physicalSpace(st)
	if err != nil {
		return err
	}
	if st.Files > 0 && st.Ffree < 2 {
		return fmt.Errorf("the backing filesystem has fewer than two inodes for staging an object: %w", syscall.ENOSPC)
	}
	if space.Avail <= maintenance {
		return fmt.Errorf("the backing filesystem has no object capacity beyond its maintenance reserve: %w", syscall.ENOSPC)
	}
	available := space.Avail - maintenance
	if available <= c.reserved || bytes > available-c.reserved {
		return fmt.Errorf("the backing filesystem has no object capacity beyond its maintenance reserve: %w", syscall.ENOSPC)
	}
	c.reserved += bytes
	return nil
}

func (c *physicalCapacity) release(bytes int64) {
	c.mu.Lock()
	c.reserved -= bytes
	c.mu.Unlock()
}

func (c *physicalCapacity) pending() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reserved
}
