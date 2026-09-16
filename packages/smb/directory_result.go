package smb

import (
	"fmt"
	"slices"
	"strings"
	"syscall"
)

// windowsListResult owns a complete directory listing under a caller-defined byte
// charge. Fixed-size Windows metadata travels with each entry; entryBytes accounts
// for the caller's representation. An oversized listing fails with EIO and cannot
// expose its accumulated prefix as a complete directory.
type windowsListResult struct {
	maxBytes   int64
	usedBytes  int64
	entryBytes func(index int, nameBytes int64, attr windowsBasicAttr) (int64, error)
	entries    []windowsEntry
	pending    int
	failure    error
}

// newWindowsListResult requires non-negative bounds and charges. fixedBytes is the
// empty listing's representation and must fit within maxBytes.
func newWindowsListResult(maxBytes, fixedBytes int64, entryBytes func(index int, nameBytes int64, attr windowsBasicAttr) (int64, error)) (*windowsListResult, error) {
	if maxBytes < 0 {
		return nil, fmt.Errorf("a Windows listing byte bound cannot be negative: %w", syscall.EINVAL)
	}
	if fixedBytes < 0 || fixedBytes > maxBytes {
		return nil, fmt.Errorf("the empty Windows listing requires %d bytes under a %d-byte bound: %w", fixedBytes, maxBytes, syscall.EFBIG)
	}
	if entryBytes == nil {
		return nil, fmt.Errorf("a Windows listing result needs an entry byte charge: %w", syscall.EINVAL)
	}
	return &windowsListResult{maxBytes: maxBytes, usedBytes: fixedBytes, entryBytes: entryBytes}, nil
}

// Add charges an independently bounded entry before retaining an owned copy.
func (r *windowsListResult) Add(entry windowsEntry) error {
	reservation, err := r.Reserve(int64(len(entry.Name)), entry.Attr)
	if err != nil {
		return err
	}
	return reservation.Commit(entry.Name)
}

// Reserve admits an entry before its variable-length name is loaded. All four
// timestamps are normalized to UTC before accounting or retention, so neither the
// callback nor the reservation retains caller-owned Location data. Each successful
// reservation must be committed exactly once before Entries can return a list.
func (r *windowsListResult) Reserve(nameBytes int64, attr windowsBasicAttr) (*windowsListReservation, error) {
	if r == nil {
		return nil, fmt.Errorf("a nil Windows listing result cannot retain an entry: %w", syscall.EINVAL)
	}
	if r.failure != nil {
		return nil, r.failure
	}
	if nameBytes < 0 {
		return nil, r.fail(fmt.Errorf("a Windows listing name cannot have negative length: %w", syscall.EIO))
	}
	attr.Metadata = nil
	attr.Attr.CreationTime = nil
	attr.Attr.ChangeTime = nil
	attr.AccessTime = attr.AccessTime.UTC()
	attr.ModTime = attr.ModTime.UTC()
	attr.CreationTime = attr.CreationTime.UTC()
	attr.ChangeTime = attr.ChangeTime.UTC()
	index := len(r.entries) + r.pending
	charge, err := r.entryBytes(index, nameBytes, attr)
	if err != nil {
		return nil, r.fail(err)
	}
	if charge < 0 {
		return nil, r.fail(fmt.Errorf("the Windows listing charge at index %d is negative: %w", index, syscall.EINVAL))
	}
	if charge > r.maxBytes-r.usedBytes {
		return nil, r.fail(fmt.Errorf("the complete Windows listing exceeds its %d-byte result bound: %w", r.maxBytes, syscall.EIO))
	}
	r.usedBytes += charge
	r.pending++
	return &windowsListReservation{result: r, nameBytes: nameBytes, attr: attr}, nil
}

// Entries returns the complete listing sorted by bytewise name. The returned
// slice belongs to the result and remains valid until the result is discarded.
func (r *windowsListResult) Entries() ([]windowsEntry, error) {
	if r.failure != nil {
		return nil, r.failure
	}
	if r.pending != 0 {
		return nil, fmt.Errorf("the Windows listing has %d entries whose names were reserved but not loaded: %w", r.pending, syscall.EIO)
	}
	slices.SortFunc(r.entries, func(a, b windowsEntry) int { return strings.Compare(a.Name, b.Name) })
	if r.entries == nil {
		return []windowsEntry{}, nil
	}
	return r.entries, nil
}

// Fail invalidates the entire listing and preserves its first failure. A producer
// must call Fail when listing fails, so callers cannot observe a plausible prefix.
func (r *windowsListResult) Fail(err error) error {
	if r == nil || err == nil {
		return err
	}
	return r.fail(err)
}

func (r *windowsListResult) fail(err error) error {
	if r.failure == nil {
		r.failure = err
	}
	r.entries = nil
	r.pending = 0
	return r.failure
}

// MaxBytes is the caller-defined bound on the complete listing representation.
func (r *windowsListResult) MaxBytes() int64 {
	if r == nil {
		return 0
	}
	return r.maxBytes
}

// windowsListReservation is one charged entry whose name has not yet been loaded.
type windowsListReservation struct {
	result    *windowsListResult
	nameBytes int64
	attr      windowsBasicAttr
	committed bool
}

// Commit checks the reserved byte length and retains an owned copy of name.
func (r *windowsListReservation) Commit(name string) error {
	if r == nil || r.result == nil || r.committed {
		return fmt.Errorf("a Windows listing reservation can be committed exactly once: %w", syscall.EINVAL)
	}
	if int64(len(name)) != r.nameBytes {
		return r.result.fail(fmt.Errorf("the Windows listing name has %d bytes after %d were reserved: %w", len(name), r.nameBytes, syscall.EIO))
	}
	if r.result.failure != nil {
		return r.result.failure
	}
	r.committed = true
	r.result.pending--
	r.result.entries = append(r.result.entries, windowsEntry{Name: strings.Clone(name), Attr: r.attr})
	return nil
}
