package smb

import "github.com/codetreker/remote-fs/packages/smb/internal/wire"

type directoryCursor struct {
	snapshot *directorySnapshot
	pattern  []uint16
	next     int
	returned bool
	settled  bool
	export   *Export
}

type directoryPageCandidate struct {
	body     []byte
	status   uint32
	next     int
	returned bool
	settled  bool
}

func (c *directoryCursor) page(class byte, limit uint32, single bool) (directoryPageCandidate, error) {
	candidate := directoryPageCandidate{next: c.next, returned: c.returned, settled: c.settled}
	count := 0
	for _, entry := range c.snapshot.entries[c.next:] {
		if entry.matched {
			count++
		}
	}
	if count == 0 {
		candidate.next, candidate.settled = len(c.snapshot.entries), true
		candidate.status = statusNoMoreFiles
		if !c.returned && !c.settled {
			candidate.status = statusNoSuchFile
		}
		return candidate, nil
	}
	entries := make([]wire.DirectoryEntry, count)
	index := 0
	for _, entry := range c.snapshot.entries[c.next:] {
		if entry.matched {
			entries[index] = entry.record
			index++
		}
	}
	page, err := wire.EncodeDirectoryPage(class, entries, limit, single)
	if err != nil {
		return directoryPageCandidate{}, err
	}
	candidate.body, err = wire.QueryInfoResponseBody(page.Data)
	if err != nil {
		return directoryPageCandidate{}, err
	}
	candidate.status = statusOK
	if page.Partial {
		candidate.status = statusBufferOverflow
	}
	remaining := page.Completed
	for candidate.next < len(c.snapshot.entries) {
		if c.snapshot.entries[candidate.next].matched {
			if remaining == 0 {
				break
			}
			remaining--
		}
		candidate.next++
	}
	if page.Completed != 0 {
		candidate.returned = true
	}
	return candidate, nil
}
