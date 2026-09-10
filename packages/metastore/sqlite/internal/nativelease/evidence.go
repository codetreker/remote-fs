package nativelease

import (
	"fmt"
	"strings"
	"syscall"
	"time"
)

// Evidence binds a monotonic maximum lease duration to one database. It contains no
// owner or grant capabilities. Generation and duration are validated before use.
type Evidence struct {
	DatabaseID string
	StateID    string
	Generation int64
	MaxLease   time.Duration
}

// Witness stores independent, volume-anchored evidence. Advance must durably publish
// the exact next generation before returning. Repeating the same record is idempotent.
type Witness interface {
	Load() (Evidence, bool, error)
	Advance(Evidence) error
}

func ValidID(id string) bool {
	return len(id) == 32 && strings.Trim(id, "0123456789abcdef") == ""
}

func ValidateEvidence(e Evidence) error {
	if !ValidID(e.DatabaseID) || !ValidID(e.StateID) ||
		e.Generation < 0 || e.MaxLease < 0 {
		return fmt.Errorf("lease recovery evidence has an invalid identity or counter: %w", syscall.EIO)
	}
	return nil
}
