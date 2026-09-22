package wire

type QueryDirectoryRequest struct {
	Class        byte
	Flags        byte
	Index        uint32
	FileID       FileID
	Pattern      string
	OutputLength uint32
}

type QueryInfoRequest struct {
	InputLength  uint32
	Type         byte
	Class        byte
	OutputLength uint32
	Additional   uint32
	Flags        uint32
	FileID       FileID
	Input        []byte
}

func (request Request) QueryDirectory() (QueryDirectoryRequest, error) {
	var result QueryDirectoryRequest
	if err := request.fixed(QueryDirectory, 33, 32); err != nil {
		return result, err
	}
	result.Class = request.Body[2]
	result.Flags = request.Body[3]
	result.Index = le.Uint32(request.Body[4:8])
	copy(result.FileID[:], request.Body[8:24])
	result.OutputLength = le.Uint32(request.Body[28:32])
	pattern, err := request.field(uint32(le.Uint16(request.Body[24:26])), uint32(le.Uint16(request.Body[26:28])), HeaderSize+32)
	if err != nil {
		return result, err
	}
	result.Pattern, err = DecodeUTF16(pattern)
	return result, err
}

func (request Request) QueryInfo() (QueryInfoRequest, error) {
	var result QueryInfoRequest
	if err := request.fixed(QueryInfo, 41, 40); err != nil {
		return result, err
	}
	result.Type = request.Body[2]
	result.Class = request.Body[3]
	result.OutputLength = le.Uint32(request.Body[4:8])
	result.InputLength = le.Uint32(request.Body[12:16])
	result.Additional = le.Uint32(request.Body[16:20])
	result.Flags = le.Uint32(request.Body[20:24])
	copy(result.FileID[:], request.Body[24:40])

	// Only FullEa and quota queries define an input buffer. The raw declared
	// length remains available for request admission on every information class.
	// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/abb48417-27e5-4a16-82e0-3e5981db97e8
	if result.InputLength != 0 && (result.Type == 1 && result.Class == 15 || result.Type == 4) {
		var err error
		result.Input, err = request.field(uint32(le.Uint16(request.Body[8:10])), result.InputLength, HeaderSize+40)
		return result, err
	}
	return result, nil
}
