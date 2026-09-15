// Package windowsaccess maintains bounded Windows open and byte-range state.
// The native authority must serialize every call with its publication gate and
// retain that gate through the operation authorized by a successful check.
package windowsaccess

import (
	"errors"
	"slices"
)

type Access uint8

const (
	Read Access = 1 << iota
	Write
	Delete
	ShareAll = Read | Write | Delete
)

var (
	ErrInvalid       = errors.New("invalid Windows access request")
	ErrHandle        = errors.New("unknown Windows access handle")
	ErrAccess        = errors.New("Windows handle access denied")
	ErrSharing       = errors.New("Windows sharing violation")
	ErrDeletePending = errors.New("Windows file is delete pending")
	ErrCapacity      = errors.New("Windows access capacity exhausted")
	ErrRange         = errors.New("invalid Windows lock range")
	ErrConflict      = errors.New("Windows byte-range lock conflict")
	ErrNotLocked     = errors.New("Windows range is not locked")
)

type Limits struct {
	MaxOpens        int
	MaxRanges       int
	MaxBatchEntries int
}

type opened struct {
	node          uint64
	access, share Access
	deletePending bool
	ranges        int
}

type file struct {
	opens         map[uint64]opened
	locks         []heldLock
	deletePending bool
}

type Engine struct {
	limits Limits
	opens  map[uint64]opened
	files  map[uint64]*file
	ranges int
}

func New(limits Limits) (*Engine, error) {
	if limits.MaxOpens <= 0 || limits.MaxRanges <= 0 || limits.MaxBatchEntries <= 0 {
		return nil, ErrInvalid
	}
	return &Engine{limits: limits, opens: make(map[uint64]opened), files: make(map[uint64]*file)}, nil
}

// Open registers an identity-bound handle. Ordinary non-Windows opens register
// their actual data access with ShareAll. Zero access represents metadata only.
func (e *Engine) Open(handle, node uint64, access, share Access) error {
	if err := e.CheckOpen(handle, node, access, share); err != nil {
		return err
	}
	f := e.files[node]
	if f == nil {
		f = &file{opens: make(map[uint64]opened)}
		e.files[node] = f
	}
	op := opened{node: node, access: access, share: share}
	e.opens[handle], f.opens[handle] = op, op
	return nil
}

// CheckOpen performs admission without changing state. The caller must keep
// the authority gate held until Open registers the successful native open.
func (e *Engine) CheckOpen(handle, node uint64, access, share Access) error {
	if handle == 0 || node == 0 || access&^ShareAll != 0 || share&^ShareAll != 0 {
		return ErrInvalid
	}
	if _, exists := e.opens[handle]; exists {
		return ErrInvalid
	}
	f := e.files[node]
	if f != nil {
		if e.DeletePending(node) {
			return ErrDeletePending
		}
		for _, old := range f.opens {
			// Metadata-only opens do not participate in sharing checks.
			// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/8c0e3f4f-0729-49f4-a14d-7f7add593819
			if access != 0 && old.access != 0 && (access&^old.share != 0 || old.access&^share != 0) {
				return ErrSharing
			}
		}
	}
	if len(e.opens) >= e.limits.MaxOpens {
		return ErrCapacity
	}
	return nil
}

// CheckAccess checks a granted handle's access and other opens' sharing masks.
// Actor zero denotes a path operation without a registered handle.
func (e *Engine) CheckAccess(node, actor uint64, access Access) error {
	if node == 0 || access&^ShareAll != 0 {
		return ErrInvalid
	}
	if actor == 0 && access != 0 && e.DeletePending(node) {
		return ErrDeletePending
	}
	if actor != 0 {
		op, ok := e.opens[actor]
		if !ok || op.node != node {
			return ErrHandle
		}
		if access&^op.access != 0 {
			return ErrAccess
		}
	}
	return e.CheckSharing(node, actor, access)
}

// CheckSharing excludes the actor's own sharing mask. Granted access and
// namespace authorization are checked separately by the native operation.
func (e *Engine) CheckSharing(node, actor uint64, access Access) error {
	if node == 0 || access&^ShareAll != 0 {
		return ErrInvalid
	}
	if actor != 0 {
		if op, ok := e.opens[actor]; !ok || op.node != node {
			return ErrHandle
		}
	}
	if f := e.files[node]; f != nil {
		for handle, op := range f.opens {
			if handle != actor && op.access != 0 && access&^op.share != 0 {
				return ErrSharing
			}
		}
	}
	return nil
}

func (e *Engine) HandleNode(handle uint64) (uint64, error) {
	op, ok := e.opens[handle]
	if !ok {
		return 0, ErrHandle
	}
	return op.node, nil
}

// SetDeletePending changes admission state, not directory entries. The caller
// must check object type, namespace state and deletion authorization atomically.
func (e *Engine) SetDeletePending(handle uint64, pending bool) error {
	op, ok := e.opens[handle]
	if !ok {
		return ErrHandle
	}
	if err := e.CheckAccess(op.node, handle, Delete); err != nil {
		return err
	}
	op.deletePending = pending
	e.opens[handle] = op
	e.files[op.node].opens[handle] = op
	return nil
}

func (e *Engine) DeletePending(node uint64) bool {
	f := e.files[node]
	if f == nil {
		return false
	}
	if f.deletePending {
		return true
	}
	for _, op := range f.opens {
		if op.deletePending {
			return true
		}
	}
	return false
}

func (e *Engine) OpenCount(node uint64) int {
	if f := e.files[node]; f != nil {
		return len(f.opens)
	}
	return 0
}

type CloseResult struct {
	Node          uint64
	Last          bool
	DeletePending bool
}

// Close drops this handle's ranges and sharing state. Last and DeletePending
// let the caller finish deletion under the same native publication gate.
func (e *Engine) Close(handle uint64) (CloseResult, error) {
	op, ok := e.opens[handle]
	if !ok {
		return CloseResult{}, ErrHandle
	}
	f := e.files[op.node]
	before := len(f.locks)
	f.locks = slices.DeleteFunc(f.locks, func(lock heldLock) bool { return lock.Owner == handle })
	e.ranges -= before - len(f.locks)
	if op.deletePending {
		f.deletePending = true
	}
	delete(f.opens, handle)
	delete(e.opens, handle)
	result := CloseResult{Node: op.node, Last: len(f.opens) == 0, DeletePending: f.deletePending}
	if result.Last {
		delete(e.files, op.node)
	}
	return result, nil
}

func (e *Engine) RangeCount(handle uint64) int {
	op, ok := e.opens[handle]
	if !ok {
		return 0
	}
	return op.ranges
}

func (e *Engine) adjustRanges(handle uint64, delta int) {
	op := e.opens[handle]
	op.ranges += delta
	e.opens[handle] = op
	e.files[op.node].opens[handle] = op
}

// CheckBatchWork bounds worst-case range comparisons before any batch effect.
func (e *Engine) CheckBatchWork(handle uint64, entries, budget int) error {
	op, ok := e.opens[handle]
	if !ok {
		return ErrHandle
	}
	if entries <= 0 || budget <= 0 {
		return ErrInvalid
	}
	current := len(e.files[op.node].locks)
	if entries > budget || current > budget-entries || entries > budget/(current+entries) {
		return ErrCapacity
	}
	return nil
}
