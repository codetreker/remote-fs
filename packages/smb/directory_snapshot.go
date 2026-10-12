package smb

import (
	"bytes"
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	directoryCursorBytes         = 1024
	directoryCaptureFixedBytes   = 64 << 10
	directoryCollectorFixedBytes = 512
	symlinkReparseTag            = 0xa000000c
)

type directorySnapshotEntry struct {
	record     wire.DirectoryEntry
	richStatus uint32
	matched    bool
}

type directorySnapshot struct {
	entries     []directorySnapshotEntry
	observation storage.DirectoryObservation
	bytes       int
}

func (snapshot *directorySnapshot) richStatus() uint32 {
	for _, entry := range snapshot.entries {
		if entry.richStatus != statusOK {
			return entry.richStatus
		}
	}
	return statusOK
}

func reserveDirectoryBytes(export *Export, amount int) error {
	server := export.server
	server.handleMu.Lock()
	defer server.handleMu.Unlock()
	if amount > server.config.Limits.MaxRetainedDirectoryBytes-server.directoryBytes {
		return syscall.ENOMEM
	}
	server.directoryBytes += amount
	export.directoryBytes += amount
	return nil
}

func releaseDirectoryBytes(export *Export, amount int) {
	server := export.server
	server.handleMu.Lock()
	server.directoryBytes -= amount
	export.directoryBytes -= amount
	server.handleMu.Unlock()
}

func reserveDirectoryCursor(t *tree, h *fileHandle) error {
	if h.directory != nil {
		return nil
	}
	if err := reserveDirectoryBytes(t.export, directoryCursorBytes); err != nil {
		return err
	}
	h.directory = &directoryCursor{export: t.export}
	return nil
}

func releaseDirectorySnapshot(t *tree, h *fileHandle) {
	if h.directory == nil || h.directory.snapshot == nil {
		return
	}
	charge := h.directory.snapshot.bytes
	h.directory.snapshot = nil
	releaseDirectoryBytes(t.export, charge)
}

func (h *fileHandle) releaseDirectoryEnumeration() {
	if h.directory == nil {
		return
	}
	cursor := h.directory
	h.directory = nil
	charge := directoryCursorBytes
	if cursor.snapshot != nil {
		charge += cursor.snapshot.bytes
	}
	releaseDirectoryBytes(cursor.export, charge)
}

// HTTP retains its complete bounded success body before entry callbacks run.
// Staging therefore precedes the backing call; entry reservations then cover
// collector, metadata clones, projection and compact records concurrently.
func reserveDirectoryCapture(export *Export) (limit int64, staging int, err error) {
	server := export.server
	server.handleMu.Lock()
	defer server.handleMu.Unlock()
	available := server.config.Limits.MaxRetainedDirectoryBytes - server.directoryBytes
	fixed := directoryCaptureFixedBytes - directoryCursorBytes
	if available < fixed+5*directoryCollectorFixedBytes {
		return 0, 0, syscall.ENOMEM
	}
	limit = min(int64(server.config.Limits.MaxDirectoryBytes), int64(storage.MaxDirectoryBytes), int64((available-fixed)/5))
	if limit < directoryCollectorFixedBytes {
		return 0, 0, syscall.ENOMEM
	}
	staging = fixed + 4*int(limit)
	server.directoryBytes += staging
	export.directoryBytes += staging
	return limit, staging, nil
}

func directoryEntryCaptureBytes(nameBytes, metadataBytes int64) (int64, error) {
	observed, err := storage.ObservedEntryBytes(nameBytes, metadataBytes)
	if err != nil {
		return 0, err
	}
	return 2*observed + 1536, nil
}

func captureDirectorySnapshot(ctx context.Context, t *tree, h *fileHandle, scope storage.UseScope, pattern []uint16) (*directorySnapshot, error) {
	limit, staging, err := reserveDirectoryCapture(t.export)
	if err != nil {
		return nil, err
	}
	acquired := 0
	defer func() { releaseDirectoryBytes(t.export, staging+acquired) }()
	snapshot, err := collectDirectorySnapshot(ctx, t, h, scope, pattern, limit, &acquired)
	if err != nil {
		return nil, err
	}
	// The collector, backing response and projection are confined to the
	// completed helper. Only owned compact names and scalar records escape it.
	acquired -= snapshot.bytes
	return snapshot, nil
}

func collectDirectorySnapshot(ctx context.Context, t *tree, h *fileHandle, scope storage.UseScope, pattern []uint16, limit int64, acquired *int) (*directorySnapshot, error) {
	reader, ok := t.authority.raw.(storage.DirectoryReader)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := reader.CheckDirectoryRead(); err != nil {
		return nil, namespaceFailure(err)
	}
	compiled, err := compileDirectoryPattern(pattern)
	if err != nil {
		return nil, err
	}
	result, err := storage.NewListResult(limit, directoryCollectorFixedBytes, func(index int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
		if index >= storage.MaxDirectoryEntries {
			return 0, syscall.EFBIG
		}
		if attr.ID == 0 || attr.ID == h.nodeID || attr.Kind.Check() != nil || attr.Kind == storage.NodeRegular && attr.Size < 0 {
			return 0, syscall.EIO
		}
		charge, err := directoryEntryCaptureBytes(nameBytes, metadataBytes)
		if err != nil {
			return 0, err
		}
		if err := reserveDirectoryBytes(t.export, int(charge)); err != nil {
			return 0, err
		}
		*acquired += int(charge)
		return charge, nil
	})
	if err != nil {
		return nil, err
	}
	observation, err := reader.ReadDirNodeBounded(ctx, storage.DirectoryTarget{NodeID: h.nodeID, Scope: &scope}, result)
	if err != nil {
		result.Fail(err)
		return nil, namespaceFailure(err)
	}
	if err := observation.Check(); err != nil || observation.ParentID != h.nodeID {
		return nil, syscall.EIO
	}
	entries, err := result.Entries()
	if err != nil {
		return nil, err
	}
	projected, err := projectDirectory(entries, t.export.server.nameComparer)
	if err != nil {
		return nil, err
	}
	unit, unitStatus := directoryAllocationUnit(ctx, t.authority.raw)
	snapshot := &directorySnapshot{
		entries:     make([]directorySnapshotEntry, len(entries)),
		observation: storage.DirectoryObservation{ParentID: observation.ParentID, Revision: bytes.Clone(observation.Revision)},
	}
	for _, name := range projected {
		entry := &snapshot.entries[name.entry]
		entry.record.Name = name.units
		projectDirectoryRecord(entry, entries[name.entry].Attr, unit, unitStatus)
		snapshot.bytes += 256 + 2*len(name.units)
	}
	remaining := t.export.server.config.Limits.MaxDirectoryMatchWork
	for i := range snapshot.entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		matched, err := compiled.match(snapshot.entries[i].record.Name, &remaining)
		if err != nil {
			return nil, err
		}
		snapshot.entries[i].matched = matched
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func directoryAllocationUnit(ctx context.Context, source storage.FileSession) (uint64, uint32) {
	reporter, ok := source.(storage.AllocationReporting)
	if !ok {
		return 0, statusUnsupported
	}
	if err := reporter.CheckAllocationReporting(); err != nil {
		return 0, directoryRichStatus(err)
	}
	unit, err := reporter.AllocationUnitBytes(ctx)
	if err != nil {
		return 0, directoryRichStatus(err)
	}
	if unit == 0 {
		return 0, statusIO
	}
	return unit, statusOK
}

func directoryRichStatus(err error) uint32 {
	if errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.ENOSYS) {
		return statusUnsupported
	}
	return statusIO
}

func projectDirectoryRecord(entry *directorySnapshotEntry, attr storage.Attr, unit uint64, unitStatus uint32) {
	entry.record.FileID = attr.ID
	if attr.Kind == storage.NodeSymlink {
		entry.record.EaSize = symlinkReparseTag
	}
	basic, err := basicInformation(attr)
	if err != nil {
		entry.richStatus = directoryRichStatus(err)
		return
	}
	entry.record.CreationTime = basic.CreationTime
	entry.record.LastAccessTime = basic.LastAccessTime
	entry.record.LastWriteTime = basic.LastWriteTime
	entry.record.ChangeTime = basic.ChangeTime
	entry.record.Attributes = basic.Attributes
	if attr.Kind == storage.NodeRegular {
		entry.record.EndOfFile = attr.Size
	}
	if attr.CheckAllocation() != nil || !attr.AllocationKnown {
		entry.richStatus = statusIO
		return
	}
	if unitStatus != statusOK {
		entry.richStatus = unitStatus
		return
	}
	if uint64(attr.AllocationSize)%unit != 0 {
		entry.richStatus = statusIO
		return
	}
	entry.record.AllocationSize = attr.AllocationSize
}
