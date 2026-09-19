package wire

type Negotiation struct {
	SecurityMode    uint16
	Dialect         uint16
	ServerGUID      [16]byte
	Capabilities    uint32
	MaxTransactSize uint32
	MaxReadSize     uint32
	MaxWriteSize    uint32
	SystemTime      uint64
	StartTime       uint64
	Token           []byte
	Contexts        []Context
}

type FileInformation struct {
	CreationTime   uint64
	AccessTime     uint64
	WriteTime      uint64
	ChangeTime     uint64
	AllocationSize uint64
	EndOfFile      uint64
	Attributes     uint32
}

type CreateResult struct {
	OplockLevel byte
	Flags       byte
	Action      uint32
	FileInformation
	FileID FileID
}

func NegotiateResponseBody(n Negotiation) ([]byte, error) {
	if len(n.Token) > 65535 || len(n.Contexts) > 65535 {
		return nil, ErrMalformed
	}
	b := make([]byte, 64+len(n.Token))
	le.PutUint16(b, 65)
	le.PutUint16(b[2:4], n.SecurityMode)
	le.PutUint16(b[4:6], n.Dialect)
	copy(b[8:24], n.ServerGUID[:])
	le.PutUint32(b[24:28], n.Capabilities)
	le.PutUint32(b[28:32], n.MaxTransactSize)
	le.PutUint32(b[32:36], n.MaxReadSize)
	le.PutUint32(b[36:40], n.MaxWriteSize)
	le.PutUint64(b[40:48], n.SystemTime)
	le.PutUint64(b[48:56], n.StartTime)
	if len(n.Token) > 0 {
		le.PutUint16(b[56:58], 128)
		le.PutUint16(b[58:60], uint16(len(n.Token)))
		copy(b[64:], n.Token)
	}
	if len(n.Contexts) > 0 {
		if n.Dialect != Dialect311 {
			return nil, ErrMalformed
		}
		le.PutUint16(b[6:8], uint16(len(n.Contexts)))
		b = append(b, make([]byte, (-len(b))&7)...)
		le.PutUint32(b[60:64], uint32(HeaderSize+len(b)))
		for i, ctx := range n.Contexts {
			if len(ctx.Data) > 65535 {
				return nil, ErrMalformed
			}
			start := len(b)
			b = append(b, make([]byte, 8+len(ctx.Data))...)
			le.PutUint16(b[start:], ctx.Type)
			le.PutUint16(b[start+2:], uint16(len(ctx.Data)))
			copy(b[start+8:], ctx.Data)
			if i+1 < len(n.Contexts) {
				b = append(b, make([]byte, (-len(b))&7)...)
			}
		}
	}
	return b, nil
}

func SessionSetupResponseBody(flags uint16, token []byte) ([]byte, error) {
	if len(token) > 65535 {
		return nil, ErrMalformed
	}
	b := make([]byte, 8+len(token))
	le.PutUint16(b, 9)
	le.PutUint16(b[2:4], flags)
	if len(token) > 0 {
		le.PutUint16(b[4:6], 72)
		le.PutUint16(b[6:8], uint16(len(token)))
		copy(b[8:], token)
	}
	return b, nil
}

func TreeConnectResponseBody(shareType byte, flags, capabilities, access uint32) []byte {
	b := make([]byte, 16)
	le.PutUint16(b, 16)
	b[2] = shareType
	le.PutUint32(b[4:8], flags)
	le.PutUint32(b[8:12], capabilities)
	le.PutUint32(b[12:16], access)
	return b
}

func encodeFileInformation(b []byte, f FileInformation) {
	le.PutUint64(b, f.CreationTime)
	le.PutUint64(b[8:16], f.AccessTime)
	le.PutUint64(b[16:24], f.WriteTime)
	le.PutUint64(b[24:32], f.ChangeTime)
	le.PutUint64(b[32:40], f.AllocationSize)
	le.PutUint64(b[40:48], f.EndOfFile)
	le.PutUint32(b[48:52], f.Attributes)
}

func CreateResponseBody(c CreateResult) []byte {
	b := make([]byte, 88)
	le.PutUint16(b, 89)
	b[2] = c.OplockLevel
	b[3] = c.Flags
	le.PutUint32(b[4:8], c.Action)
	encodeFileInformation(b[8:60], c.FileInformation)
	copy(b[64:80], c.FileID[:])
	return b
}

func CloseResponseBody(flags uint16, f FileInformation) []byte {
	b := make([]byte, 60)
	le.PutUint16(b, 60)
	le.PutUint16(b[2:4], flags)
	encodeFileInformation(b[8:60], f)
	return b
}

func ReadResponseBody(data []byte, remaining uint32) []byte {
	b := make([]byte, 16+len(data))
	le.PutUint16(b, 17)
	b[2] = 80
	le.PutUint32(b[4:8], uint32(len(data)))
	le.PutUint32(b[8:12], remaining)
	copy(b[16:], data)
	return b
}

func WriteResponseBody(count, remaining uint32) []byte {
	b := make([]byte, 16)
	le.PutUint16(b, 17)
	le.PutUint32(b[4:8], count)
	le.PutUint32(b[8:12], remaining)
	return b
}

// BufferResponseBody is shared by QUERY_INFO, QUERY_DIRECTORY and CHANGE_NOTIFY.
func BufferResponseBody(data []byte) []byte {
	b := make([]byte, 8+len(data))
	le.PutUint16(b, 9)
	if len(data) > 0 {
		le.PutUint16(b[2:4], 72)
		le.PutUint32(b[4:8], uint32(len(data)))
		copy(b[8:], data)
	}
	return b
}

func IOCTLResponseBody(code uint32, id FileID, flags uint32, output []byte) []byte {
	b := make([]byte, 48+len(output))
	le.PutUint16(b, 49)
	le.PutUint32(b[4:8], code)
	copy(b[8:24], id[:])
	if len(output) > 0 {
		le.PutUint32(b[32:36], 112)
		le.PutUint32(b[36:40], uint32(len(output)))
		copy(b[48:], output)
	}
	le.PutUint32(b[40:44], flags)
	return b
}

func EmptyResponseBody() []byte   { return []byte{4, 0, 0, 0} }
func SetInfoResponseBody() []byte { return []byte{2, 0} }
func ErrorResponseBody() []byte   { return []byte{9, 0, 0, 0, 0, 0, 0, 0, 0} }

type Notification struct {
	Action uint32
	Name   string
}

func NotifyInformation(events []Notification) []byte {
	var data []byte
	for i, event := range events {
		name := EncodeUTF16(event.Name)
		start := len(data)
		length := 12 + len(name)
		if i+1 < len(events) {
			length = (length + 3) &^ 3
		}
		data = append(data, make([]byte, length)...)
		if i+1 < len(events) {
			le.PutUint32(data[start:], uint32(length))
		}
		le.PutUint32(data[start+4:], event.Action)
		le.PutUint32(data[start+8:], uint32(len(name)))
		copy(data[start+12:], name)
	}
	return data
}
