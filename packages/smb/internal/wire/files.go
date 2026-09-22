package wire

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

type FlushRequest struct {
	FileID FileID
}

type ReadRequest struct {
	Flags        byte
	Length       uint32
	Offset       uint64
	FileID       FileID
	MinimumCount uint32
	Channel      uint32
	Remaining    uint32
	ChannelInfo  []byte
}

type WriteRequest struct {
	Offset      uint64
	FileID      FileID
	Channel     uint32
	Remaining   uint32
	Flags       uint32
	Data        []byte
	ChannelInfo []byte
}

func (request Request) Create() (CreateRequest, error) {
	var result CreateRequest
	if err := request.fixed(Create, 57, 56); err != nil {
		return result, err
	}
	result.SecurityFlags = request.Body[2]
	result.OplockLevel = request.Body[3]
	result.Impersonation = le.Uint32(request.Body[4:8])
	result.DesiredAccess = le.Uint32(request.Body[24:28])
	result.Attributes = le.Uint32(request.Body[28:32])
	result.ShareAccess = le.Uint32(request.Body[32:36])
	result.Disposition = le.Uint32(request.Body[36:40])
	result.Options = le.Uint32(request.Body[40:44])

	name, err := request.field(uint32(le.Uint16(request.Body[44:46])), uint32(le.Uint16(request.Body[46:48])), HeaderSize+56)
	if err != nil {
		return result, err
	}
	result.Name, err = DecodeUTF16(name)
	if err != nil {
		return result, err
	}

	contexts, err := request.field(le.Uint32(request.Body[48:52]), le.Uint32(request.Body[52:56]), HeaderSize+56)
	if err != nil {
		return result, err
	}
	if len(contexts) != 0 && le.Uint32(request.Body[48:52])&7 != 0 {
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
		nameOffset := uint64(le.Uint16(contexts[4:6]))
		nameLength := uint64(le.Uint16(contexts[6:8]))
		dataOffset := uint64(le.Uint16(contexts[10:12]))
		dataLength := uint64(le.Uint32(contexts[12:16]))
		if nameLength == 0 || nameOffset < 16 || nameOffset+nameLength > uint64(end) ||
			(dataLength != 0 && (dataOffset < 16 || dataOffset+dataLength > uint64(end))) {
			return result, ErrMalformed
		}
		context := CreateContext{Name: contexts[int(nameOffset):int(nameOffset+nameLength)]}
		if dataLength != 0 {
			context.Data = contexts[int(dataOffset):int(dataOffset+dataLength)]
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
	result.Flags = le.Uint16(request.Body[2:4])
	copy(result.FileID[:], request.Body[8:24])
	return result, nil
}

func (request Request) Flush() (FlushRequest, error) {
	var result FlushRequest
	if err := request.fixed(Flush, 24, 24); err != nil {
		return result, err
	}
	copy(result.FileID[:], request.Body[8:24])
	return result, nil
}

func (request Request) FileID() (FileID, error) {
	switch request.Header.Command {
	case Close:
		closeRequest, err := request.Close()
		return closeRequest.FileID, err
	case Flush:
		flushRequest, err := request.Flush()
		return flushRequest.FileID, err
	default:
		return FileID{}, ErrMalformed
	}
}

func (request Request) Read() (ReadRequest, error) {
	var result ReadRequest
	if err := request.fixed(Read, 49, 48); err != nil {
		return result, err
	}
	result.Flags = request.Body[3]
	result.Length = le.Uint32(request.Body[4:8])
	result.Offset = le.Uint64(request.Body[8:16])
	copy(result.FileID[:], request.Body[16:32])
	result.MinimumCount = le.Uint32(request.Body[32:36])
	result.Channel = le.Uint32(request.Body[36:40])
	result.Remaining = le.Uint32(request.Body[40:44])
	var err error
	result.ChannelInfo, err = request.field(uint32(le.Uint16(request.Body[44:46])), uint32(le.Uint16(request.Body[46:48])), HeaderSize+48)
	return result, err
}

func (request Request) Write() (WriteRequest, error) {
	var result WriteRequest
	if err := request.fixed(Write, 49, 48); err != nil {
		return result, err
	}
	result.Offset = le.Uint64(request.Body[8:16])
	copy(result.FileID[:], request.Body[16:32])
	result.Channel = le.Uint32(request.Body[32:36])
	result.Remaining = le.Uint32(request.Body[36:40])
	result.Flags = le.Uint32(request.Body[44:48])
	var err error
	result.Data, err = request.field(uint32(le.Uint16(request.Body[2:4])), le.Uint32(request.Body[4:8]), HeaderSize+48)
	if err != nil {
		return result, err
	}
	result.ChannelInfo, err = request.field(uint32(le.Uint16(request.Body[40:42])), uint32(le.Uint16(request.Body[42:44])), HeaderSize+48)
	return result, err
}
