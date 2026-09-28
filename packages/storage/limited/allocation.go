package limited

import (
	"context"

	"github.com/codetreker/remote-fs/packages/storage"
)

// This wrapper charges logical payload bytes, so inner allocation is not a
// measured charge against the allowance this wrapper reports.
func maskAllocation(attr storage.Attr) storage.Attr {
	attr.AllocationSize = 0
	attr.AllocationKnown = false
	return attr
}

func allocationContext(ctx context.Context) context.Context {
	return storage.WithAttrResultProjection(ctx, maskAllocation)
}

func maskEntries(entries []storage.Entry) []storage.Entry {
	if entries == nil {
		return nil
	}
	mapped := make([]storage.Entry, len(entries))
	copy(mapped, entries)
	for index := range mapped {
		mapped[index].Attr = maskAllocation(mapped[index].Attr)
	}
	return mapped
}

func maskObservedDirectory(directory storage.ObservedDirectory) storage.ObservedDirectory {
	if directory.Entries != nil {
		entries := make([]storage.ObservedEntry, len(directory.Entries))
		copy(entries, directory.Entries)
		directory.Entries = entries
		for index := range directory.Entries {
			directory.Entries[index].Attr = maskAllocation(directory.Entries[index].Attr)
		}
	}
	return directory
}

func maskResult[T any](result T) T {
	switch value := any(result).(type) {
	case storage.Attr:
		return any(maskAllocation(value)).(T)
	case storage.OpenResult:
		value.Attr = maskAllocation(value.Attr)
		return any(value).(T)
	case storage.NodeOpenResult:
		value.Attr = maskAllocation(value.Attr)
		return any(value).(T)
	case storage.NameResult:
		if value.Attr != nil {
			attr := maskAllocation(*value.Attr)
			value.Attr = &attr
		}
		return any(value).(T)
	case storage.ReferenceState:
		value.Attr = maskAllocation(value.Attr)
		return any(value).(T)
	}
	return result
}
