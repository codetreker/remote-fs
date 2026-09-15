package windowsaccess

import "math"

type Range struct {
	Offset uint64
	Length uint64
}

func (r Range) Valid() bool {
	return r.Length == 0 || r.Length-1 <= math.MaxUint64-r.Offset
}

// MS-FSA 2.1.4.10 uses inclusive ends and permits zero-length locks. Only
// {0,0} is expressly non-overlapping; other zero-length ranges retain the
// specified inclusive comparison instead of becoming POSIX EOF locks.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/124bb289-eeef-4653-b9c6-4fb93dd07a21
func (r Range) overlaps(other Range) bool {
	if r == (Range{}) || other == (Range{}) {
		return false
	}
	return r.Offset <= other.Offset+other.Length-1 && r.Offset+r.Length-1 >= other.Offset
}

type Conflict struct {
	Owner     uint64
	Range     Range
	Exclusive bool
}

func (c *Conflict) Error() string { return ErrConflict.Error() }
func (c *Conflict) Unwrap() error { return ErrConflict }

type heldLock = Conflict

// CheckIO checks both sharing and byte-range constraints. Actor zero denotes
// an unregistered path operation. Shared locks prohibit even their owner's
// writes; exclusive locks permit their owner's I/O.
func (e *Engine) CheckIO(node, actor uint64, region Range, write bool) error {
	if !region.Valid() {
		return ErrRange
	}
	access := Read
	if write {
		access = Write
	}
	if err := e.CheckAccess(node, actor, access); err != nil {
		return err
	}
	if conflict := e.conflict(e.files[node], actor, region, write, false); conflict != nil {
		return conflict
	}
	return nil
}

func (e *Engine) conflict(f *file, actor uint64, region Range, exclusive, lockIntent bool) *Conflict {
	if f == nil {
		return nil
	}
	for _, lock := range f.locks {
		if !region.overlaps(lock.Range) {
			continue
		}
		if lock.Exclusive {
			if lock.Owner != actor || (exclusive && lockIntent) {
				return &lock
			}
		} else if exclusive {
			return &lock
		}
	}
	return nil
}
