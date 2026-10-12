package wire

const (
	InfoFile       byte = 1
	InfoFilesystem byte = 2
	InfoSecurity   byte = 3
)

const (
	FileBasicInformation       byte = 4
	FileStandardInformation    byte = 5
	FileInternalInformation    byte = 6
	FileEndOfFileInformation   byte = 20
	FileNetworkOpenInformation byte = 34
	FileIdInformation          byte = 59
)

type QueryInfoRequest struct {
	InfoType           byte
	FileInfoClass      byte
	OutputBufferLength uint32
	InputBufferLength  uint32
	FileID             FileID
}

type SetInfoRequest struct {
	InfoType              byte
	FileInfoClass         byte
	BufferLength          uint32
	BufferOffset          uint16
	AdditionalInformation uint32
	FileID                FileID
	Buffer                []byte
}

func (request Request) QueryInfo() (QueryInfoRequest, error) {
	var result QueryInfoRequest
	if err := request.fixed(QueryInfo, 41, 40); err != nil {
		return result, err
	}
	result.InfoType = request.Body[2]
	result.FileInfoClass = request.Body[3]
	result.OutputBufferLength = le.Uint32(request.Body[4:8])
	result.InputBufferLength = le.Uint32(request.Body[12:16])
	copy(result.FileID[:], request.Body[24:40])
	if result.FileID.HasPartialRelatedPlaceholder() {
		return QueryInfoRequest{}, ErrMalformed
	}
	return result, nil
}

func (request Request) SetInfo() (SetInfoRequest, error) {
	var result SetInfoRequest
	if err := request.fixed(SetInfo, 33, 32); err != nil {
		return result, err
	}
	result.InfoType = request.Body[2]
	result.FileInfoClass = request.Body[3]
	result.BufferLength = le.Uint32(request.Body[4:8])
	result.BufferOffset = le.Uint16(request.Body[8:10])
	result.AdditionalInformation = le.Uint32(request.Body[12:16])
	copy(result.FileID[:], request.Body[16:32])
	if result.FileID.HasPartialRelatedPlaceholder() {
		return SetInfoRequest{}, ErrMalformed
	}
	var err error
	result.Buffer, err = request.field(uint32(result.BufferOffset), result.BufferLength, HeaderSize+32)
	if err != nil {
		return SetInfoRequest{}, err
	}
	return result, nil
}

func QueryInfoResponseBody(data []byte) ([]byte, error) {
	if len(data) > maxDirectTCPPayload-HeaderSize-8 {
		return nil, ErrMalformed
	}
	body := make([]byte, 8+len(data))
	le.PutUint16(body, 9)
	le.PutUint16(body[2:4], HeaderSize+8)
	le.PutUint32(body[4:8], uint32(len(data)))
	copy(body[8:], data)
	return body, nil
}

func SetInfoResponseBody() []byte { return []byte{2, 0} }

// InfoLengthMismatchErrorBody carries the empty SMB 3.1.1 error context
// required for a QUERY_INFO buffer that cannot hold the fixed information.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d64e0451-64a2-425a-848e-52a7dddab7b9
func InfoLengthMismatchErrorBody() []byte {
	return []byte{9, 0, 1, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
}

type FileBasicInfo struct {
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	ChangeTime     int64
	Attributes     uint32
}

func ParseFileBasicInformation(data []byte) (FileBasicInfo, error) {
	if len(data) != 40 {
		return FileBasicInfo{}, ErrMalformed
	}
	return FileBasicInfo{
		CreationTime: int64(le.Uint64(data[:8])), LastAccessTime: int64(le.Uint64(data[8:16])),
		LastWriteTime: int64(le.Uint64(data[16:24])), ChangeTime: int64(le.Uint64(data[24:32])),
		Attributes: le.Uint32(data[32:36]),
	}, nil
}

func FileBasicInformationBody(info FileBasicInfo) []byte {
	body := make([]byte, 40)
	le.PutUint64(body[:8], uint64(info.CreationTime))
	le.PutUint64(body[8:16], uint64(info.LastAccessTime))
	le.PutUint64(body[16:24], uint64(info.LastWriteTime))
	le.PutUint64(body[24:32], uint64(info.ChangeTime))
	le.PutUint32(body[32:36], info.Attributes)
	return body
}

type FileStandardInfo struct {
	AllocationSize int64
	EndOfFile      int64
	NumberOfLinks  uint32
	DeletePending  bool
	Directory      bool
}

func FileStandardInformationBody(info FileStandardInfo) []byte {
	body := make([]byte, 24)
	le.PutUint64(body[:8], uint64(info.AllocationSize))
	le.PutUint64(body[8:16], uint64(info.EndOfFile))
	le.PutUint32(body[16:20], info.NumberOfLinks)
	if info.DeletePending {
		body[20] = 1
	}
	if info.Directory {
		body[21] = 1
	}
	return body
}

func FileInternalInformationBody(id uint64) []byte {
	body := make([]byte, 8)
	le.PutUint64(body, id)
	return body
}

type FileNetworkOpenInfo struct {
	FileBasicInfo
	AllocationSize int64
	EndOfFile      int64
}

func FileNetworkOpenInformationBody(info FileNetworkOpenInfo) []byte {
	body := make([]byte, 56)
	le.PutUint64(body[:8], uint64(info.CreationTime))
	le.PutUint64(body[8:16], uint64(info.LastAccessTime))
	le.PutUint64(body[16:24], uint64(info.LastWriteTime))
	le.PutUint64(body[24:32], uint64(info.ChangeTime))
	le.PutUint64(body[32:40], uint64(info.AllocationSize))
	le.PutUint64(body[40:48], uint64(info.EndOfFile))
	le.PutUint32(body[48:52], info.Attributes)
	return body
}

func FileIdInformationBody(serial uint64, id [16]byte) []byte {
	body := make([]byte, 24)
	le.PutUint64(body[:8], serial)
	copy(body[8:24], id[:])
	return body
}

func ParseFileEndOfFileInformation(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, ErrMalformed
	}
	return int64(le.Uint64(data)), nil
}
