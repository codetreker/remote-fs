package wire

const (
	FileDirectoryInformation       byte = 0x01
	FileFullDirectoryInformation   byte = 0x02
	FileBothDirectoryInformation   byte = 0x03
	FileNamesInformation           byte = 0x0c
	FileIdBothDirectoryInformation byte = 0x25
	FileIdFullDirectoryInformation byte = 0x26

	QueryDirectoryRestartScans      byte = 0x01
	QueryDirectoryReturnSingleEntry byte = 0x02
	QueryDirectoryIndexSpecified    byte = 0x04
	QueryDirectoryReopen            byte = 0x10
)

type QueryDirectoryRequest struct {
	FileInformationClass byte
	Flags                byte
	FileIndex            uint32
	FileID               FileID
	Pattern              string
	OutputBufferLength   uint32
}

func (request Request) QueryDirectory() (QueryDirectoryRequest, error) {
	var result QueryDirectoryRequest
	if err := request.fixed(QueryDirectory, 33, 32); err != nil {
		return result, err
	}
	result.FileInformationClass = request.Body[2]
	result.Flags = request.Body[3]
	result.FileIndex = le.Uint32(request.Body[4:8])
	copy(result.FileID[:], request.Body[8:24])
	result.OutputBufferLength = le.Uint32(request.Body[28:32])
	if result.Flags & ^byte(QueryDirectoryRestartScans|QueryDirectoryReturnSingleEntry|QueryDirectoryIndexSpecified|QueryDirectoryReopen) != 0 ||
		result.FileID.HasPartialRelatedPlaceholder() || result.OutputBufferLength > maxDirectTCPPayload-HeaderSize-8 {
		return QueryDirectoryRequest{}, ErrMalformed
	}
	offset, length := uint32(le.Uint16(request.Body[24:26])), uint32(le.Uint16(request.Body[26:28]))
	if offset&1 != 0 || length&1 != 0 || length == 0 && offset != 0 {
		return QueryDirectoryRequest{}, ErrMalformed
	}
	pattern, err := request.field(offset, length, HeaderSize+32)
	if err != nil {
		return QueryDirectoryRequest{}, err
	}
	result.Pattern, err = DecodeUTF16(pattern)
	if err != nil {
		return QueryDirectoryRequest{}, err
	}
	return result, nil
}

func IsDirectoryInformationClass(class byte) bool {
	switch class {
	case FileDirectoryInformation, FileFullDirectoryInformation, FileBothDirectoryInformation,
		FileNamesInformation, FileIdBothDirectoryInformation, FileIdFullDirectoryInformation,
		0x3c, 0x4e, 0x4f, 0x50, 0x51:
		return true
	}
	return false
}

func DirectoryInformationHeadSize(class byte) (int, bool) {
	switch class {
	case FileNamesInformation:
		return 12, true
	case FileDirectoryInformation:
		return 64, true
	case FileFullDirectoryInformation:
		return 68, true
	case FileBothDirectoryInformation:
		return 94, true
	case FileIdFullDirectoryInformation:
		return 80, true
	case FileIdBothDirectoryInformation:
		return 104, true
	}
	return 0, false
}

type DirectoryEntry struct {
	Name                                                    []uint16
	CreationTime, LastAccessTime, LastWriteTime, ChangeTime int64
	EndOfFile, AllocationSize                               int64
	Attributes, EaSize                                      uint32
	FileID                                                  uint64
}

type DirectoryPage struct {
	Data      []byte
	Completed int
	Partial   bool
}

func EncodeDirectoryPage(class byte, entries []DirectoryEntry, limit uint32, single bool) (DirectoryPage, error) {
	head, supported := DirectoryInformationHeadSize(class)
	if !supported || limit < uint32(head) || limit > maxDirectTCPPayload-HeaderSize-8 {
		return DirectoryPage{}, ErrMalformed
	}
	var page DirectoryPage
	size := 0
	for _, entry := range entries {
		if err := checkDirectoryEntry(class, entry); err != nil {
			return DirectoryPage{}, err
		}
		start := (size + 7) &^ 7
		length := head + 2*len(entry.Name)
		if start+length > int(limit) {
			if page.Completed == 0 {
				size = int(limit)
				page.Partial = true
			}
			break
		}
		size = start + length
		page.Completed++
		if single {
			break
		}
	}
	if size == 0 {
		return page, nil
	}
	page.Data = make([]byte, size)
	if page.Partial {
		putDirectoryEntry(page.Data, class, entries[0], head)
		return page, nil
	}
	start, previous := 0, 0
	for index, entry := range entries[:page.Completed] {
		if index != 0 {
			le.PutUint32(page.Data[previous:previous+4], uint32(start-previous))
		}
		length := head + 2*len(entry.Name)
		putDirectoryEntry(page.Data[start:start+length], class, entry, head)
		previous, start = start, (start+length+7)&^7
	}
	return page, nil
}

func checkDirectoryEntry(class byte, entry DirectoryEntry) error {
	if len(entry.Name) == 0 || len(entry.Name) > (maxDirectTCPPayload-HeaderSize-8-104)/2 {
		return ErrMalformed
	}
	for i := 0; i < len(entry.Name); i++ {
		unit := entry.Name[i]
		if unit == 0 {
			return ErrMalformed
		}
		if unit >= 0xd800 && unit <= 0xdbff {
			if i+1 == len(entry.Name) || entry.Name[i+1] < 0xdc00 || entry.Name[i+1] > 0xdfff {
				return ErrMalformed
			}
			i++
		} else if unit >= 0xdc00 && unit <= 0xdfff {
			return ErrMalformed
		}
	}
	if class == FileNamesInformation {
		return nil
	}
	if entry.CreationTime < 0 || entry.LastAccessTime < 0 || entry.LastWriteTime < 0 || entry.ChangeTime < 0 ||
		entry.EndOfFile < 0 || entry.AllocationSize < 0 ||
		(class == FileIdFullDirectoryInformation || class == FileIdBothDirectoryInformation) && entry.FileID == 0 {
		return ErrMalformed
	}
	return nil
}

func putDirectoryEntry(data []byte, class byte, entry DirectoryEntry, head int) {
	nameLength := uint32(2 * len(entry.Name))
	if class == FileNamesInformation {
		le.PutUint32(data[8:12], nameLength)
	} else {
		le.PutUint64(data[8:16], uint64(entry.CreationTime))
		le.PutUint64(data[16:24], uint64(entry.LastAccessTime))
		le.PutUint64(data[24:32], uint64(entry.LastWriteTime))
		le.PutUint64(data[32:40], uint64(entry.ChangeTime))
		le.PutUint64(data[40:48], uint64(entry.EndOfFile))
		le.PutUint64(data[48:56], uint64(entry.AllocationSize))
		le.PutUint32(data[56:60], entry.Attributes)
		le.PutUint32(data[60:64], nameLength)
		if class != FileDirectoryInformation {
			le.PutUint32(data[64:68], entry.EaSize)
		}
		if class == FileIdFullDirectoryInformation {
			le.PutUint64(data[72:80], entry.FileID)
		} else if class == FileIdBothDirectoryInformation {
			le.PutUint64(data[96:104], entry.FileID)
		}
	}
	// The first record's overflow form carries the exact available byte prefix.
	// Its full name length lets the client retry that same pending entry.
	// https://winprotocoldoc.z19.web.core.windows.net/MS-FSA/%5bMS-FSA%5d.pdf#page=94
	for position := head; position < len(data); position++ {
		unit := entry.Name[(position-head)/2]
		if (position-head)&1 != 0 {
			unit >>= 8
		}
		data[position] = byte(unit)
	}
}
