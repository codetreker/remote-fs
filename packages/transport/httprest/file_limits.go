package httprest

import (
	"fmt"
	"syscall"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/storage"
)

// FileLimits bounds HTTP session enrollment. Session is the maximum native
// resource budget a caller may request; native sessions own references and history.
// The zero value selects DefaultFileLimits.
type FileLimits struct {
	MaxSessions int
	Session     storage.FileSessionOptions
}

func DefaultFileLimits() FileLimits {
	return FileLimits{MaxSessions: 64, Session: storage.DefaultFileSessionOptions()}
}
func (l FileLimits) settled() FileLimits {
	if l == (FileLimits{}) {
		return DefaultFileLimits()
	}
	return l
}
func (l FileLimits) Check() error {
	l = l.settled()
	if l.MaxSessions <= 0 || l.MaxSessions > 65536 {
		return fmt.Errorf("invalid HTTP session capacity: %w", syscall.EINVAL)
	}
	return l.Session.Check()
}
func checkFileSessionOptions(o, maximum storage.FileSessionOptions) error {
	if err := o.Check(); err != nil {
		return err
	}
	if o.Lease > maximum.Lease || o.History > maximum.History || o.MaxFileSize > maximum.MaxFileSize || o.MaxFiles > maximum.MaxFiles || o.MaxOperations > maximum.MaxOperations || o.MaxWaiters > maximum.MaxWaiters || o.MaxRangeOwners > maximum.MaxRangeOwners || o.MaxRanges > maximum.MaxRanges || o.MaxPendingActions > maximum.MaxPendingActions || o.MaxActions > maximum.MaxActions {
		return fmt.Errorf("file session exceeds the configured resource bounds: %w", syscall.EINVAL)
	}
	return nil
}
func validateFileStatus(s storage.FileSessionStatus, o storage.FileSessionOptions) error {
	if s.Epoch == "" || len(s.Epoch) > MaxLockCapabilityBytes || !utf8.ValidString(s.Epoch) || s.Revision == 0 || s.ActionEpoch == 0 || s.Remaining < 0 || s.Remaining > o.Lease || s.HistoryRemaining < 0 || s.HistoryRemaining > o.History {
		return fmt.Errorf("invalid native file session status: %w", syscall.EIO)
	}
	return nil
}
