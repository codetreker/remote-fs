package fuse

import (
	"fmt"
	"math"
	"sort"
	"syscall"
)

type lockOwner uint64
type lockFamily uint8
type lockType uint8

const (
	flockFamily lockFamily = iota + 1
	posixFamily
)

const (
	unlockType lockType = iota + 1
	sharedType
	exclusiveType
)

type fileLock struct {
	Family     lockFamily
	Type       lockType
	Start, End uint64
	PID        uint32
	Wait       bool
}

func (l fileLock) check() error {
	if l.Family != flockFamily && l.Family != posixFamily {
		return fmt.Errorf("unknown kernel lock family: %w", syscall.EINVAL)
	}
	if l.Type != unlockType && l.Type != sharedType && l.Type != exclusiveType {
		return fmt.Errorf("unknown kernel lock type: %w", syscall.EINVAL)
	}
	if l.Start > l.End || l.End > math.MaxInt64 {
		return fmt.Errorf("kernel lock range exceeds signed file offsets: %w", syscall.EINVAL)
	}
	if l.Family == flockFamily && (l.Start != 0 || l.End != math.MaxInt64) {
		return fmt.Errorf("flock requires a whole-file range: %w", syscall.EINVAL)
	}
	if l.Type == unlockType && l.Wait {
		return fmt.Errorf("unlock cannot wait: %w", syscall.EINVAL)
	}
	return nil
}

func lockOverlaps(a, b fileLock) bool {
	return a.Start <= b.End && b.Start <= a.End
}

// POSIX replacement is planned without changing the held set. A failed CAS or
// capacity check therefore preserves the previous ranges, including conversions.
func replacePOSIXRanges(old []fileLock, change fileLock) []fileLock {
	next := make([]fileLock, 0, len(old)+2)
	for _, held := range old {
		if !lockOverlaps(held, change) {
			next = append(next, held)
			continue
		}
		if held.Start < change.Start {
			left := held
			left.End = change.Start - 1
			next = append(next, left)
		}
		if held.End > change.End {
			right := held
			right.Start = change.End + 1
			next = append(next, right)
		}
	}
	if change.Type != unlockType {
		change.Wait = false
		next = append(next, change)
	}
	sort.Slice(next, func(i, j int) bool { return next[i].Start < next[j].Start })
	merged := next[:0]
	for _, current := range next {
		if len(merged) > 0 {
			previous := &merged[len(merged)-1]
			if previous.End+1 == current.Start && previous.Type == current.Type {
				previous.End = current.End
				continue
			}
		}
		merged = append(merged, current)
	}
	clear(next[len(merged):])
	return merged
}
