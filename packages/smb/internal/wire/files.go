package wire

type FileID [16]byte

var InvalidFileID = FileID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

type CreateContext struct {
	Name []byte
	Data []byte
}

type CreateRequest struct {
	SecurityFlags byte
	OplockLevel   byte
	Impersonation uint32
	DesiredAccess uint32
	Attributes    uint32
	ShareAccess   uint32
	Disposition   uint32
	Options       uint32
	Name          string
	Contexts      []CreateContext
}

type CloseRequest struct {
	Flags  uint16
	FileID FileID
}

func (request Request) Create() (CreateRequest, error) {
	var result CreateRequest
	if err := request.fixed(Create, 57, 56); err != nil {
		return result, err
	}
	for _, value := range request.Body[8:24] {
		if value != 0 {
			return result, ErrMalformed
		}
	}
	result.SecurityFlags = request.Body[2]
	result.OplockLevel = request.Body[3]
	result.Impersonation = le.Uint32(request.Body[4:8])
	result.DesiredAccess = le.Uint32(request.Body[24:28])
	result.Attributes = le.Uint32(request.Body[28:32])
	result.ShareAccess = le.Uint32(request.Body[32:36])
	result.Disposition = le.Uint32(request.Body[36:40])
	result.Options = le.Uint32(request.Body[40:44])

	nameOffset := uint32(le.Uint16(request.Body[44:46]))
	nameLength := uint32(le.Uint16(request.Body[46:48]))
	name, err := request.field(nameOffset, nameLength, HeaderSize+56)
	if err != nil {
		return result, err
	}
	result.Name, err = DecodeUTF16(name)
	if err != nil {
		return result, err
	}

	contextOffset := le.Uint32(request.Body[48:52])
	contextLength := le.Uint32(request.Body[52:56])
	contexts, err := request.field(contextOffset, contextLength, HeaderSize+56)
	if err != nil || contextLength != 0 && contextOffset&7 != 0 {
		return result, ErrMalformed
	}
	if nameLength != 0 && contextLength != 0 && uint64(nameOffset)+uint64(nameLength) > uint64(contextOffset) {
		return result, ErrMalformed
	}
	for len(contexts) != 0 {
		if len(result.Contexts) >= request.maxContexts || len(contexts) < 16 {
			return result, ErrMalformed
		}
		next := le.Uint32(contexts[:4])
		end := len(contexts)
		if next != 0 {
			if next < 16 || next&7 != 0 || uint64(next)+16 > uint64(len(contexts)) {
				return result, ErrMalformed
			}
			end = int(next)
		}
		nameStart, nameSize := uint64(le.Uint16(contexts[4:6])), uint64(le.Uint16(contexts[6:8]))
		dataStart, dataSize := uint64(le.Uint16(contexts[10:12])), uint64(le.Uint32(contexts[12:16]))
		if nameSize == 0 || nameStart < 16 || nameStart+nameSize > uint64(end) ||
			dataSize != 0 && (dataStart < 16 || dataStart+dataSize > uint64(end) ||
				dataStart < nameStart+nameSize && nameStart < dataStart+dataSize) {
			return result, ErrMalformed
		}
		context := CreateContext{Name: contexts[int(nameStart):int(nameStart+nameSize)]}
		if dataSize != 0 {
			context.Data = contexts[int(dataStart):int(dataStart+dataSize)]
		}
		result.Contexts = append(result.Contexts, context)
		if next == 0 {
			break
		}
		contexts = contexts[end:]
	}
	return result, nil
}

func (request Request) Close() (CloseRequest, error) {
	var result CloseRequest
	if err := request.fixed(Close, 24, 24); err != nil {
		return result, err
	}
	if request.Header.NextCommand == 0 && len(request.Body) != 24 ||
		request.Header.NextCommand != 0 && len(request.Body) != align8(HeaderSize+24)-HeaderSize {
		return result, ErrMalformed
	}
	for _, value := range request.Body[4:8] {
		if value != 0 {
			return result, ErrMalformed
		}
	}
	result.Flags = le.Uint16(request.Body[2:4])
	copy(result.FileID[:], request.Body[8:24])
	return result, nil
}

type CreateResponse struct {
	OplockLevel    byte
	Flags          byte
	CreateAction   uint32
	CreationTime   uint64
	LastAccessTime uint64
	LastWriteTime  uint64
	ChangeTime     uint64
	AllocationSize uint64
	EndOfFile      uint64
	Attributes     uint32
	FileID         FileID
}

func CreateResponseBody(response CreateResponse) []byte {
	body := make([]byte, 88)
	le.PutUint16(body, 89)
	body[2], body[3] = response.OplockLevel, response.Flags
	le.PutUint32(body[4:8], response.CreateAction)
	le.PutUint64(body[8:16], response.CreationTime)
	le.PutUint64(body[16:24], response.LastAccessTime)
	le.PutUint64(body[24:32], response.LastWriteTime)
	le.PutUint64(body[32:40], response.ChangeTime)
	le.PutUint64(body[40:48], response.AllocationSize)
	le.PutUint64(body[48:56], response.EndOfFile)
	le.PutUint32(body[56:60], response.Attributes)
	copy(body[64:80], response.FileID[:])
	return body
}

type CloseResponse struct {
	Flags          uint16
	CreationTime   uint64
	LastAccessTime uint64
	LastWriteTime  uint64
	ChangeTime     uint64
	AllocationSize uint64
	EndOfFile      uint64
	Attributes     uint32
}

func CloseResponseBody(response CloseResponse) []byte {
	body := make([]byte, 60)
	le.PutUint16(body, 60)
	le.PutUint16(body[2:4], response.Flags)
	le.PutUint64(body[8:16], response.CreationTime)
	le.PutUint64(body[16:24], response.LastAccessTime)
	le.PutUint64(body[24:32], response.LastWriteTime)
	le.PutUint64(body[32:40], response.ChangeTime)
	le.PutUint64(body[40:48], response.AllocationSize)
	le.PutUint64(body[48:56], response.EndOfFile)
	le.PutUint32(body[56:60], response.Attributes)
	return body
}
