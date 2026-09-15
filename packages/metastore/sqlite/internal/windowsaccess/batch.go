package windowsaccess

import "slices"

type LockFlags uint8

const (
	Shared          LockFlags = 0x01
	Exclusive       LockFlags = 0x02
	Unlock          LockFlags = 0x04
	FailImmediately LockFlags = 0x10
)

type LockRequest struct {
	Range Range
	Flags LockFlags
}

type BatchResult struct {
	Applied    int
	RolledBack int
}

// LockBatch executes in request order. Conflict rolls back this batch's grants;
// invalid later entries and failed unlocks retain prior successful changes.
// A conflicting single non-immediate request returns ErrConflict without a
// grant; its bounded wait/cancel lifecycle belongs to the native authority.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/670c7eda-e683-4923-9477-414303959613
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/79eb3c91-563b-4d48-a51c-0974f9d144f8
func (e *Engine) LockBatch(handle uint64, requests []LockRequest) (BatchResult, error) {
	op, ok := e.opens[handle]
	if !ok {
		return BatchResult{}, ErrHandle
	}
	if op.access&(Read|Write) == 0 {
		return BatchResult{}, ErrAccess
	}
	if len(requests) == 0 {
		return BatchResult{}, ErrInvalid
	}
	if len(requests) > e.limits.MaxBatchEntries {
		return BatchResult{}, ErrCapacity
	}
	unlock := requests[0].Flags&Unlock != 0
	if !unlock && len(requests) > 1 {
		for _, req := range requests {
			if req.Flags&FailImmediately == 0 {
				return BatchResult{}, ErrInvalid
			}
		}
	}
	f := e.files[op.node]
	start := len(f.locks)
	result := BatchResult{}
	for _, req := range requests {
		mode := req.Flags &^ FailImmediately
		if (unlock && mode != Unlock) || (!unlock && mode != Shared && mode != Exclusive) {
			return result, ErrInvalid
		}
		if !req.Range.Valid() {
			return result, ErrRange
		}
		if unlock {
			if err := e.unlock(f, handle, req.Range); err != nil {
				return result, err
			}
		} else {
			if conflict := e.conflict(f, handle, req.Range, mode == Exclusive, true); conflict != nil {
				result.RolledBack = result.Applied
				e.ranges -= result.Applied
				e.adjustRanges(handle, -result.Applied)
				clear(f.locks[start:])
				f.locks = f.locks[:start]
				result.Applied = 0
				return result, conflict
			}
			if e.ranges >= e.limits.MaxRanges {
				return result, ErrCapacity
			}
			f.locks = append(f.locks, heldLock{Owner: handle, Range: req.Range, Exclusive: mode == Exclusive})
			e.ranges++
			e.adjustRanges(handle, 1)
		}
		result.Applied++
	}
	return result, nil
}

// Exact exclusive matches are removed before shared matches, including when a
// handle holds both at the same range. Adjacent ranges cannot be unlocked as one.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/84e68de6-31a6-4cba-afd4-00b1bac6d1a2
func (e *Engine) unlock(f *file, handle uint64, region Range) error {
	index := -1
	for i, lock := range f.locks {
		if lock.Owner == handle && lock.Range == region {
			index = i
			if lock.Exclusive {
				break
			}
		}
	}
	if index < 0 {
		return ErrNotLocked
	}
	f.locks = slices.Delete(f.locks, index, index+1)
	e.ranges--
	e.adjustRanges(handle, -1)
	return nil
}
