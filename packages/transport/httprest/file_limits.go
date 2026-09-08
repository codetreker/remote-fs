package httprest

import (
	"fmt"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// FileLimits bounds the handler's capability registry independently of native
// storage admission. Session limits are upper bounds on client enrollment.
// The zero value selects DefaultFileLimits; other values set every field.
type FileLimits struct {
	MaxSessions int
	MaxActions  int
	// Cleanup history is independent of data history. Exhaustion retires the
	// session with EIO so owner cleanup cannot leave renewed locks behind.
	MaxCleanupActions int
	// Session renewal does not extend an unacknowledged reference lifetime.
	PendingAck time.Duration
	Session    storage.FileSessionOptions
}

func DefaultFileLimits() FileLimits {
	return FileLimits{MaxSessions: 64, MaxActions: 16384, MaxCleanupActions: 16384, PendingAck: 5 * time.Second, Session: storage.DefaultFileSessionOptions()}
}

func (l FileLimits) settled() FileLimits {
	if l == (FileLimits{}) {
		return DefaultFileLimits()
	}
	return l
}

func (l FileLimits) Check() error {
	l = l.settled()
	if l.MaxSessions <= 0 || l.MaxSessions > 65536 || l.MaxActions <= 0 || l.MaxActions > 1<<20 || l.MaxCleanupActions <= 0 || l.MaxCleanupActions > 1<<20 || l.PendingAck <= 0 || l.PendingAck > l.Session.Lease || l.Session.History > math.MaxInt64/2 {
		return fmt.Errorf("invalid HTTP file registry limits: %w", syscall.EINVAL)
	}
	return l.Session.Check()
}
