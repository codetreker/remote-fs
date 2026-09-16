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

func (r Request) Create() (CreateRequest, error) {
	var out CreateRequest
	if err := r.fixed(Create, 57, 56); err != nil {
		return out, err
	}
	out.SecurityFlags = r.Body[2]
	out.OplockLevel = r.Body[3]
	out.Impersonation = le.Uint32(r.Body[4:8])
	out.DesiredAccess = le.Uint32(r.Body[24:28])
	out.Attributes = le.Uint32(r.Body[28:32])
	out.ShareAccess = le.Uint32(r.Body[32:36])
	out.Disposition = le.Uint32(r.Body[36:40])
	out.Options = le.Uint32(r.Body[40:44])
	name, err := r.field(uint32(le.Uint16(r.Body[44:46])), uint32(le.Uint16(r.Body[46:48])), 120)
	if err != nil {
		return out, err
	}
	out.Name, err = DecodeUTF16(name)
	if err != nil {
		return out, err
	}
	data, err := r.field(le.Uint32(r.Body[48:52]), le.Uint32(r.Body[52:56]), 120)
	if err != nil {
		return out, err
	}
	if len(data) > 0 && le.Uint32(r.Body[48:52])&7 != 0 {
		return out, ErrMalformed
	}
	for len(data) > 0 {
		if len(out.Contexts) >= r.maxContexts || len(data) < 16 {
			return out, ErrMalformed
		}
		next := le.Uint32(data[:4])
		end := len(data)
		if next != 0 {
			if next < 16 || next&7 != 0 || uint64(next)+16 > uint64(len(data)) {
				return out, ErrMalformed
			}
			end = int(next)
		}
		nameOff := uint64(le.Uint16(data[4:6]))
		nameLen := uint64(le.Uint16(data[6:8]))
		dataOff := uint64(le.Uint16(data[10:12]))
		dataLen := uint64(le.Uint32(data[12:16]))
		if nameLen == 0 || nameOff < 16 || nameOff+nameLen > uint64(end) || (dataLen > 0 && (dataOff < 16 || dataOff+dataLen > uint64(end))) {
			return out, ErrMalformed
		}
		ctx := CreateContext{Name: data[int(nameOff):int(nameOff+nameLen)]}
		if dataLen > 0 {
			ctx.Data = data[int(dataOff):int(dataOff+dataLen)]
		}
		out.Contexts = append(out.Contexts, ctx)
		if next == 0 {
			break
		}
		data = data[end:]
	}
	return out, nil
}

func (r Request) FileID() (FileID, error) {
	var id FileID
	switch r.Header.Command {
	case Close:
		if err := r.fixed(Close, 24, 24); err != nil {
			return id, err
		}
	case Flush:
		if err := r.fixed(Flush, 24, 24); err != nil {
			return id, err
		}
	default:
		return id, ErrMalformed
	}
	copy(id[:], r.Body[8:24])
	return id, nil
}

func (r Request) Read() (ReadRequest, error) {
	var out ReadRequest
	if err := r.fixed(Read, 49, 48); err != nil {
		return out, err
	}
	out.Flags = r.Body[3]
	out.Length = le.Uint32(r.Body[4:8])
	out.Offset = le.Uint64(r.Body[8:16])
	copy(out.FileID[:], r.Body[16:32])
	out.MinimumCount = le.Uint32(r.Body[32:36])
	out.Channel = le.Uint32(r.Body[36:40])
	out.Remaining = le.Uint32(r.Body[40:44])
	var err error
	out.ChannelInfo, err = r.field(uint32(le.Uint16(r.Body[44:46])), uint32(le.Uint16(r.Body[46:48])), 112)
	return out, err
}

func (r Request) Write() (WriteRequest, error) {
	var out WriteRequest
	if err := r.fixed(Write, 49, 48); err != nil {
		return out, err
	}
	out.Offset = le.Uint64(r.Body[8:16])
	copy(out.FileID[:], r.Body[16:32])
	out.Channel = le.Uint32(r.Body[32:36])
	out.Remaining = le.Uint32(r.Body[36:40])
	out.Flags = le.Uint32(r.Body[44:48])
	var err error
	out.Data, err = r.field(uint32(le.Uint16(r.Body[2:4])), le.Uint32(r.Body[4:8]), 112)
	if err != nil {
		return out, err
	}
	out.ChannelInfo, err = r.field(uint32(le.Uint16(r.Body[40:42])), uint32(le.Uint16(r.Body[42:44])), 112)
	return out, err
}
