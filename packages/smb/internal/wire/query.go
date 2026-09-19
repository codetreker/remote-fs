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
type SetInfoRequest struct {
	Type       byte
	Class      byte
	Additional uint32
	FileID     FileID
	Input      []byte
}
type IOCTLRequest struct {
	Code              uint32
	FileID            FileID
	MaxInputResponse  uint32
	MaxOutputResponse uint32
	Flags             uint32
	Input             []byte
	Output            []byte
}

func (r Request) QueryDirectory() (QueryDirectoryRequest, error) {
	var out QueryDirectoryRequest
	if err := r.fixed(QueryDirectory, 33, 32); err != nil {
		return out, err
	}
	out.Class = r.Body[2]
	out.Flags = r.Body[3]
	out.Index = le.Uint32(r.Body[4:8])
	copy(out.FileID[:], r.Body[8:24])
	out.OutputLength = le.Uint32(r.Body[28:32])
	data, err := r.field(uint32(le.Uint16(r.Body[24:26])), uint32(le.Uint16(r.Body[26:28])), 96)
	if err != nil {
		return out, err
	}
	out.Pattern, err = DecodeUTF16(data)
	return out, err
}

func (r Request) QueryInfo() (QueryInfoRequest, error) {
	var out QueryInfoRequest
	if err := r.fixed(QueryInfo, 41, 40); err != nil {
		return out, err
	}
	out.Type = r.Body[2]
	out.Class = r.Body[3]
	out.OutputLength = le.Uint32(r.Body[4:8])
	out.InputLength = le.Uint32(r.Body[12:16])
	out.Additional = le.Uint32(r.Body[16:20])
	out.Flags = le.Uint32(r.Body[20:24])
	copy(out.FileID[:], r.Body[24:40])
	// Only FullEa and quota queries interpret their input buffer. Its declared
	// length still participates in credit admission for all query classes.
	// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/abb48417-27e5-4a16-82e0-3e5981db97e8
	if out.InputLength != 0 && (out.Type == 1 && out.Class == 15 || out.Type == 4) {
		var err error
		out.Input, err = r.field(uint32(le.Uint16(r.Body[8:10])), out.InputLength, 104)
		return out, err
	}
	return out, nil
}

func (r Request) SetInfo() (SetInfoRequest, error) {
	var out SetInfoRequest
	if err := r.fixed(SetInfo, 33, 32); err != nil {
		return out, err
	}
	out.Type = r.Body[2]
	out.Class = r.Body[3]
	out.Additional = le.Uint32(r.Body[12:16])
	copy(out.FileID[:], r.Body[16:32])
	var err error
	out.Input, err = r.field(uint32(le.Uint16(r.Body[8:10])), le.Uint32(r.Body[4:8]), 96)
	return out, err
}

func (r Request) IOCTL() (IOCTLRequest, error) {
	var out IOCTLRequest
	if err := r.fixed(IOCTL, 57, 56); err != nil {
		return out, err
	}
	out.Code = le.Uint32(r.Body[4:8])
	copy(out.FileID[:], r.Body[8:24])
	out.MaxInputResponse = le.Uint32(r.Body[32:36])
	out.MaxOutputResponse = le.Uint32(r.Body[44:48])
	out.Flags = le.Uint32(r.Body[48:52])
	var err error
	out.Input, err = r.field(le.Uint32(r.Body[24:28]), le.Uint32(r.Body[28:32]), 120)
	if err != nil {
		return out, err
	}
	out.Output, err = r.field(le.Uint32(r.Body[36:40]), le.Uint32(r.Body[40:44]), 120)
	return out, err
}
