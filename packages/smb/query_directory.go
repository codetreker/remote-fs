package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	fileDirectoryInformation       byte = 1
	fileFullDirectoryInformation   byte = 2
	fileBothDirectoryInformation   byte = 3
	fileNamesInformation           byte = 12
	fileIDBothDirectoryInformation byte = 37
	fileIDFullDirectoryInformation byte = 38

	queryDirectoryRestartScans      byte = 0x01
	queryDirectoryReturnSingleEntry byte = 0x02
	queryDirectoryIndexSpecified    byte = 0x04
	queryDirectoryReopen            byte = 0x10

	statusNoMoreFiles      uint32 = 0x80000006
	statusNoSuchFile       uint32 = 0xc000000f
	statusInvalidInfoClass uint32 = 0xc0000003
)

type directoryEntryFields struct {
	information wire.FileInformation
	nodeID      uint64
}

func directoryEntryBaseSize(class byte) (int, error) {
	switch class {
	case fileDirectoryInformation:
		return 64, nil
	case fileFullDirectoryInformation:
		return 68, nil
	case fileBothDirectoryInformation:
		return 94, nil
	case fileNamesInformation:
		return 12, nil
	case fileIDBothDirectoryInformation:
		return 104, nil
	case fileIDFullDirectoryInformation:
		return 80, nil
	default:
		return 0, syscall.EOPNOTSUPP
	}
}

func directoryInformationClassStatus(class byte) uint32 {
	if _, err := directoryEntryBaseSize(class); err == nil {
		return statusOK
	}
	switch class {
	case 0x3c, 0x4e, 0x4f, 0x50, 0x51:
		return statusUnsupported
	default:
		return statusInvalidInfoClass
	}
}

func directoryCommandStatus(err error) uint32 {
	if errors.Is(err, errNameInvalid) {
		return namespaceStatus(err)
	}
	return fileCommandStatus(err)
}

func directoryReadStatus(err error, flags byte) uint32 {
	if flags&queryDirectoryReopen == 0 && storage.ErrnoOf(err) == syscall.ENOTDIR {
		return statusInvalid
	}
	return fileCommandStatus(err)
}

func captureDirectoryEntry(attr storage.Attr) (directoryEntryFields, error) {
	size, err := virtualFileSize(attr)
	if attr.Kind == storage.NodeSymlink {
		if attr.Size < 0 {
			return directoryEntryFields{}, syscall.EIO
		}
		if attr.Size > math.MaxInt64-(virtualAllocationUnit-1) {
			return directoryEntryFields{}, syscall.EOVERFLOW
		}
		size = fileSizeInformation{
			AllocationSize: (attr.Size + virtualAllocationUnit - 1) / virtualAllocationUnit * virtualAllocationUnit,
			EndOfFile:      attr.Size,
			AllocationUnit: virtualAllocationUnit,
		}
		err = nil
	}
	if err != nil {
		return directoryEntryFields{}, err
	}
	information, err := captureCreateInformation(attr, size)
	if err != nil {
		return directoryEntryFields{}, err
	}
	return directoryEntryFields{information: information, nodeID: attr.ID}, nil
}

func encodeDirectoryEntry(class byte, index uint32, name []uint16, fields directoryEntryFields) ([]byte, error) {
	size, err := directoryEntrySize(class, name)
	if err != nil {
		return nil, err
	}
	entry := make([]byte, size)
	if err := writeDirectoryEntry(entry, class, index, name, fields); err != nil {
		return nil, err
	}
	return entry, nil
}

func directoryEntrySize(class byte, name []uint16) (int, error) {
	base, err := directoryEntryBaseSize(class)
	if err != nil {
		return 0, err
	}
	if len(name) == 0 || len(name) > maxNameUnits {
		return 0, syscall.EIO
	}
	return base + len(name)*2, nil
}

func writeDirectoryEntry(entry []byte, class byte, index uint32, name []uint16, fields directoryEntryFields) error {
	base, err := directoryEntryBaseSize(class)
	if err != nil {
		return err
	}
	if len(name) == 0 || len(name) > maxNameUnits || len(entry) < base {
		return syscall.EIO
	}
	nameBytes := len(name) * 2
	clear(entry)
	binary.LittleEndian.PutUint32(entry[4:8], index)
	if class == fileNamesInformation {
		binary.LittleEndian.PutUint32(entry[8:12], uint32(nameBytes))
		encodeUTF16Units(entry[12:], name)
		return nil
	}

	information := fields.information
	binary.LittleEndian.PutUint64(entry[8:16], information.CreationTime)
	binary.LittleEndian.PutUint64(entry[16:24], information.AccessTime)
	binary.LittleEndian.PutUint64(entry[24:32], information.WriteTime)
	binary.LittleEndian.PutUint64(entry[32:40], information.ChangeTime)
	binary.LittleEndian.PutUint64(entry[40:48], information.EndOfFile)
	binary.LittleEndian.PutUint64(entry[48:56], information.AllocationSize)
	binary.LittleEndian.PutUint32(entry[56:60], information.Attributes)
	binary.LittleEndian.PutUint32(entry[60:64], uint32(nameBytes))
	eaSize := uint32(0)
	if information.Attributes&dosReparsePoint != 0 {
		eaSize = symlinkReparseTag
	}

	switch class {
	case fileDirectoryInformation:
		encodeUTF16Units(entry[64:], name)
	case fileFullDirectoryInformation:
		binary.LittleEndian.PutUint32(entry[64:68], eaSize)
		encodeUTF16Units(entry[68:], name)
	case fileBothDirectoryInformation:
		binary.LittleEndian.PutUint32(entry[64:68], eaSize)
		encodeUTF16Units(entry[94:], name)
	case fileIDBothDirectoryInformation:
		binary.LittleEndian.PutUint32(entry[64:68], eaSize)
		binary.LittleEndian.PutUint64(entry[96:104], fields.nodeID)
		encodeUTF16Units(entry[104:], name)
	case fileIDFullDirectoryInformation:
		binary.LittleEndian.PutUint32(entry[64:68], eaSize)
		binary.LittleEndian.PutUint64(entry[72:80], fields.nodeID)
		encodeUTF16Units(entry[80:], name)
	}
	return nil
}

func encodeUTF16Units(destination []byte, units []uint16) {
	for index, unit := range units {
		offset := index * 2
		if offset >= len(destination) {
			return
		}
		destination[offset] = byte(unit)
		if offset+1 < len(destination) {
			destination[offset+1] = byte(unit >> 8)
		}
	}
}

func checkDirectoryPattern(pattern string) error {
	if pattern == "" || !utf8.ValidString(pattern) || utf16Length(pattern) > maxNameUnits {
		return errNameInvalid
	}
	for _, value := range pattern {
		if value < 32 || value == '\\' || value == '/' || value == ':' || value == '|' {
			return errNameInvalid
		}
	}
	if pattern != "." && pattern != ".." && !strings.ContainsAny(pattern, `*?<>"`) {
		return checkWindowsLeaf(pattern)
	}
	return nil
}

func normalizeDirectoryPattern(pattern string) string {
	if pattern == "" || pattern == "*.*" {
		return "*"
	}
	if len(pattern) > 1 && strings.HasPrefix(pattern, ".") {
		pattern = "*" + pattern
	}
	values := []rune(pattern)
	translated := make([]rune, len(values))
	for index, value := range values {
		switch {
		case value == '?':
			translated[index] = '>'
		case value == '.' && index+1 < len(values) && (values[index+1] == '?' || values[index+1] == '*'):
			translated[index] = '"'
		case value == '*' && index+1 < len(values) && values[index+1] == '.':
			translated[index] = '<'
		default:
			translated[index] = value
		}
	}
	return string(translated)
}

// DOS_STAR, DOS_QM, and DOS_DOT follow the Windows object-store matching
// algorithm; ordinary path globbing does not implement these semantics.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/0b034646-4e23-4874-8488-2adac231ff23
func directoryPatternMatch(name, pattern []uint16, compare nameComparer) (bool, error) {
	return directoryPatternMatchContext(context.Background(), name, pattern, compare)
}

func directoryPatternMatchContext(ctx context.Context, name, pattern []uint16, compare nameComparer) (bool, error) {
	current := make([]bool, len(pattern)+1)
	next := make([]bool, len(pattern)+1)
	current[0] = true
	closeEmpty := func(states []bool, nameIndex int) {
		for patternIndex := 0; patternIndex < len(pattern); patternIndex++ {
			if !states[patternIndex] {
				continue
			}
			switch pattern[patternIndex] {
			case '*', '<':
				states[patternIndex+1] = true
			case '>':
				if nameIndex == len(name) || name[nameIndex] == '.' {
					states[patternIndex+1] = true
				}
			case '"':
				if nameIndex == len(name) {
					states[patternIndex+1] = true
				}
			}
		}
	}
	closeEmpty(current, 0)
	for nameIndex, nameUnit := range name {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		clear(next)
		finalDot := nameUnit == '.' && !slicesContainUnit(name[nameIndex+1:], '.')
		for patternIndex, active := range current[:len(pattern)] {
			if !active {
				continue
			}
			switch pattern[patternIndex] {
			case '*':
				next[patternIndex] = true
			case '?':
				next[patternIndex+1] = true
			case '<':
				if !finalDot {
					next[patternIndex] = true
				}
			case '"':
				if nameUnit == '.' {
					next[patternIndex+1] = true
				}
			case '>':
				if nameUnit != '.' {
					next[patternIndex+1] = true
				}
			default:
				order, err := compare([]uint16{nameUnit}, []uint16{pattern[patternIndex]})
				if err != nil {
					return false, err
				}
				if order == 0 {
					next[patternIndex+1] = true
				}
			}
		}
		current, next = next, current
		closeEmpty(current, nameIndex+1)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return current[len(pattern)], nil
}

func slicesContainUnit(values []uint16, wanted uint16) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func validateDirectorySnapshot(ctx context.Context, observation storage.DirectoryObservation, parent uint64, entries []storage.Entry, pattern string, compare nameComparer) ([]storage.ObservedEntry, error) {
	if observation.ParentID != parent || observation.Check() != nil {
		return nil, syscall.EIO
	}
	observed := make([]storage.ObservedEntry, len(entries))
	for index, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		observed[index] = storage.ObservedEntry{RawLeaf: []byte(entry.Name), Attr: entry.Attr}
	}
	complete := storage.ObservedDirectory{Observation: observation.Clone(), Entries: observed}
	if err := complete.Check(); err != nil {
		return nil, errors.Join(syscall.EIO, err)
	}
	contextCompare := func(left, right []uint16) (int, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		order, err := compare(left, right)
		if err != nil {
			return 0, err
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return order, nil
	}
	projected, err := projectDirectory(entries, contextCompare)
	if err != nil {
		return nil, err
	}
	ordered := make([]storage.ObservedEntry, len(projected))
	for index, projection := range projected {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry := observed[projection.entry]
		if _, err := captureDirectoryEntry(entry.Attr); err != nil {
			return nil, err
		}
		ordered[index] = entry
	}
	return ordered, nil
}

func (c *connection) queryDirectory(ctx context.Context, tree *tree, request wire.Request) ([]byte, uint32) {
	return c.queryDirectoryWithComparer(ctx, tree, request, nativeNameCompare)
}

func (c *connection) queryDirectoryWithComparer(ctx context.Context, tree *tree, request wire.Request, compare nameComparer) ([]byte, uint32) {
	query, err := request.QueryDirectory()
	if err != nil || compare == nil || query.OutputLength > uint32(c.server.config.Limits.MaxIOBytes) {
		return nil, statusInvalid
	}
	if status := directoryInformationClassStatus(query.Class); status != statusOK {
		return nil, status
	}
	base, _ := directoryEntryBaseSize(query.Class)
	if query.OutputLength < uint32(base) {
		return nil, statusInfoLengthMismatch
	}
	if query.Flags&^(queryDirectoryRestartScans|queryDirectoryReturnSingleEntry|queryDirectoryIndexSpecified|queryDirectoryReopen) != 0 {
		return nil, statusInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, fileCommandStatus(err)
	}
	handle := tree.files.get(query.FileID)
	if handle == nil {
		return nil, statusFileClosed
	}
	releaseHandle, err := handle.borrow()
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseHandle()
	handle.directoryMu.Lock()
	defer handle.directoryMu.Unlock()
	if handle.grantedAccess&fileReadData == 0 {
		return nil, statusDenied
	}
	if handle.scope == nil || handle.scope.Check() != nil || handle.nodeID == 0 {
		return nil, statusIO
	}
	if err := c.authorizeFileOperation(ctx, tree, storage.OpFileReadDirNode); err != nil {
		return nil, fileCommandStatus(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fileCommandStatus(err)
	}

	handle.cursorMu.Lock()
	initialized := handle.cursor.observation.ParentID != 0
	hasPattern := handle.cursor.pattern != ""
	restart := query.Flags&(queryDirectoryRestartScans|queryDirectoryReopen) != 0
	if initialized && !restart {
		body, status := serveDirectoryPageLocked(ctx, handle, query.Class, query.Flags, query.OutputLength, false, compare)
		handle.cursorMu.Unlock()
		return body, status
	}
	savedPattern := handle.cursor.pattern
	handle.cursorMu.Unlock()
	pattern := savedPattern
	acceptRequestPattern := !hasPattern || restart && query.Pattern != ""
	if acceptRequestPattern {
		rawPattern := query.Pattern
		if rawPattern == "" {
			rawPattern = "*"
		}
		if err := checkDirectoryPattern(rawPattern); err != nil {
			return nil, namespaceStatus(err)
		}
		pattern = normalizeDirectoryPattern(rawPattern)
	}
	firstQuery := !hasPattern || query.Flags&queryDirectoryReopen != 0
	if err := handle.resetDirectorySnapshot(strings.Clone(pattern), true); err != nil {
		return nil, fileCommandStatus(err)
	}

	reservation, err := handle.beginDirectorySnapshot()
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer reservation.release()
	directories, ok := handle.authority.raw.(storage.DirectoryReader)
	if !ok {
		return nil, statusUnsupported
	}
	limit := min(handle.registry.limits.MaxDirectoryBytes, int64(storage.MaxDirectoryBytes))
	result, err := storage.NewListResult(limit, 0, reservation.entryBudget)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	target := storage.DirectoryTarget{NodeID: handle.nodeID, Scope: handle.scope}
	observation, err := directories.ReadDirNodeBounded(ctx, target, result)
	if err != nil {
		return nil, directoryReadStatus(result.Fail(err), query.Flags)
	}
	entries, err := result.Entries()
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	validated, err := validateDirectorySnapshot(ctx, observation, handle.nodeID, entries, pattern, compare)
	if err != nil {
		return nil, directoryCommandStatus(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fileCommandStatus(err)
	}
	extraBytes := int64(len(observation.Revision))
	if extraBytes != 0 {
		if err := reservation.reserve(0, extraBytes); err != nil {
			return nil, fileCommandStatus(err)
		}
	}
	exactBytes, err := directorySnapshotBytes(observation, validated)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	if err := reservation.commit(directoryCursor{
		pattern: strings.Clone(pattern), observation: observation.Clone(), entries: validated, started: !firstQuery,
	}, int64(len(validated)), exactBytes); err != nil {
		return nil, fileCommandStatus(err)
	}
	handle.cursorMu.Lock()
	body, status := serveDirectoryPageLocked(ctx, handle, query.Class, query.Flags, query.OutputLength, firstQuery, compare)
	handle.cursorMu.Unlock()
	return body, status
}

func directorySnapshotBytes(observation storage.DirectoryObservation, entries []storage.ObservedEntry) (int64, error) {
	bytes := int64(256 + len(observation.Revision))
	for _, entry := range entries {
		metadataBytes, err := storage.MetadataSize(entry.Attr.Metadata)
		if err != nil {
			return 0, err
		}
		charge, err := storage.ObservedEntryBytes(int64(len(entry.RawLeaf)), int64(metadataBytes))
		if err != nil {
			return 0, err
		}
		charge += 128 + 2*int64(utf16Length(string(entry.RawLeaf)))
		if charge > int64(^uint64(0)>>1)-bytes {
			return 0, syscall.EOVERFLOW
		}
		bytes += charge
	}
	return bytes, nil
}

func directoryOutputCapacity(handle *fileHandle) int64 {
	registry := handle.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	server := registry.tree.export.server
	server.mu.Lock()
	defer server.mu.Unlock()
	return min(
		registry.limits.MaxDirectoryBytes-registry.directoryBytes,
		registry.limits.MaxDirectoryBytes-registry.tree.export.directoryBytes,
	)
}

func serveDirectoryPageLocked(ctx context.Context, handle *fileHandle, class, flags byte, outputLength uint32, firstQuery bool, compare nameComparer) ([]byte, uint32) {
	cursor := &handle.cursor
	base, err := directoryEntryBaseSize(class)
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	type pagePlan struct {
		start, next int
		bytes       int
		entries     int
		partial     int
	}
	available := directoryOutputCapacity(handle)
	dataLimit := min(int64(outputLength), available-8)
	poolLimited := dataLimit < int64(outputLength)
	plan := pagePlan{start: cursor.next, next: cursor.next, partial: -1}
	for plan.next < len(cursor.entries) {
		if err := ctx.Err(); err != nil {
			return nil, fileCommandStatus(err)
		}
		entryIndex := plan.next
		entry := cursor.entries[entryIndex]
		matched, err := directoryPatternMatchContext(ctx, nameUnits(string(entry.RawLeaf)), nameUnits(cursor.pattern), compare)
		if err != nil {
			return nil, fileCommandStatus(err)
		}
		if !matched {
			plan.next++
			continue
		}
		if available < 8+int64(base) {
			return nil, statusResources
		}
		entrySize, err := directoryEntrySize(class, nameUnits(string(entry.RawLeaf)))
		if err != nil {
			return nil, fileCommandStatus(err)
		}
		entryStart := (plan.bytes + 7) &^ 7
		if int64(entryStart)+int64(entrySize) > dataLimit {
			if plan.entries != 0 {
				break
			}
			if poolLimited {
				return nil, statusResources
			}
			plan.partial = entryIndex
			plan.bytes = int(dataLimit)
			break
		}
		plan.bytes = entryStart + entrySize
		plan.entries++
		plan.next++
		if flags&queryDirectoryReturnSingleEntry != 0 {
			break
		}
	}
	if plan.entries == 0 && plan.partial < 0 {
		cursor.next = plan.next
		cursor.started = true
		if firstQuery {
			return nil, statusNoSuchFile
		}
		return nil, statusNoMoreFiles
	}
	if plan.partial >= 0 {
		cursor.next = plan.partial
		cursor.started = true
		return nil, statusBufferOverflow
	}
	releaseOutput, err := handle.reserveDirectoryOutput(int64(8 + plan.bytes))
	if err != nil {
		return nil, fileCommandStatus(err)
	}
	defer releaseOutput()
	body := make([]byte, 8+plan.bytes)
	binary.LittleEndian.PutUint16(body, 9)
	binary.LittleEndian.PutUint16(body[2:4], wire.HeaderSize+8)
	binary.LittleEndian.PutUint32(body[4:8], uint32(plan.bytes))
	data := body[8:]
	offset, previous := 0, -1
	for entryIndex := plan.start; entryIndex < plan.next; entryIndex++ {
		if err := ctx.Err(); err != nil {
			return nil, fileCommandStatus(err)
		}
		entry := cursor.entries[entryIndex]
		matched, err := directoryPatternMatchContext(ctx, nameUnits(string(entry.RawLeaf)), nameUnits(cursor.pattern), compare)
		if err != nil {
			return nil, fileCommandStatus(err)
		}
		if !matched {
			continue
		}
		name := nameUnits(string(entry.RawLeaf))
		entrySize, err := directoryEntrySize(class, name)
		if err != nil {
			return nil, fileCommandStatus(err)
		}
		fields, err := captureDirectoryEntry(entry.Attr)
		if err != nil {
			return nil, fileCommandStatus(err)
		}
		offset = (offset + 7) &^ 7
		if previous >= 0 {
			binary.LittleEndian.PutUint32(data[previous:previous+4], uint32(offset-previous))
		}
		if err := writeDirectoryEntry(data[offset:offset+entrySize], class, 0, name, fields); err != nil {
			return nil, fileCommandStatus(err)
		}
		previous = offset
		offset += entrySize
	}
	if offset != len(data) {
		return nil, statusIO
	}
	cursor.next = plan.next
	cursor.started = true
	return body, statusOK
}
