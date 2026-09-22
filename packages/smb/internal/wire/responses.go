package wire

const maxDirectTCPPayload = 0xffffff

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

func NegotiateResponseBody(negotiation Negotiation) ([]byte, error) {
	if len(negotiation.Token) > 65535 || len(negotiation.Contexts) > 65535 || len(negotiation.Contexts) != 0 && negotiation.Dialect != Dialect311 {
		return nil, ErrMalformed
	}
	total := 64 + len(negotiation.Token)
	if len(negotiation.Contexts) != 0 {
		total = align8(total)
		for index, context := range negotiation.Contexts {
			if len(context.Data) > 65535 || total > maxDirectTCPPayload-HeaderSize-8-len(context.Data) {
				return nil, ErrMalformed
			}
			total += 8 + len(context.Data)
			if index+1 < len(negotiation.Contexts) {
				total = align8(total)
			}
		}
	}
	if HeaderSize+total > maxDirectTCPPayload {
		return nil, ErrMalformed
	}

	body := make([]byte, 64+len(negotiation.Token), total)
	le.PutUint16(body, 65)
	le.PutUint16(body[2:4], negotiation.SecurityMode)
	le.PutUint16(body[4:6], negotiation.Dialect)
	copy(body[8:24], negotiation.ServerGUID[:])
	le.PutUint32(body[24:28], negotiation.Capabilities)
	le.PutUint32(body[28:32], negotiation.MaxTransactSize)
	le.PutUint32(body[32:36], negotiation.MaxReadSize)
	le.PutUint32(body[36:40], negotiation.MaxWriteSize)
	le.PutUint64(body[40:48], negotiation.SystemTime)
	le.PutUint64(body[48:56], negotiation.StartTime)
	if len(negotiation.Token) != 0 {
		le.PutUint16(body[56:58], HeaderSize+64)
		le.PutUint16(body[58:60], uint16(len(negotiation.Token)))
		copy(body[64:], negotiation.Token)
	}
	if len(negotiation.Contexts) == 0 {
		return body, nil
	}
	le.PutUint16(body[6:8], uint16(len(negotiation.Contexts)))
	body = append(body, make([]byte, align8(len(body))-len(body))...)
	le.PutUint32(body[60:64], uint32(HeaderSize+len(body)))
	for index, context := range negotiation.Contexts {
		start := len(body)
		body = append(body, make([]byte, 8+len(context.Data))...)
		le.PutUint16(body[start:start+2], context.Type)
		le.PutUint16(body[start+2:start+4], uint16(len(context.Data)))
		copy(body[start+8:], context.Data)
		if index+1 < len(negotiation.Contexts) {
			body = append(body, make([]byte, align8(len(body))-len(body))...)
		}
	}
	return body, nil
}

func SessionSetupResponseBody(flags uint16, token []byte) ([]byte, error) {
	if len(token) > 65535 {
		return nil, ErrMalformed
	}
	body := make([]byte, 8+len(token))
	le.PutUint16(body, 9)
	le.PutUint16(body[2:4], flags)
	if len(token) != 0 {
		le.PutUint16(body[4:6], HeaderSize+8)
		le.PutUint16(body[6:8], uint16(len(token)))
		copy(body[8:], token)
	}
	return body, nil
}

func TreeConnectResponseBody(shareType byte, flags, capabilities, access uint32) []byte {
	body := make([]byte, 16)
	le.PutUint16(body, 16)
	body[2] = shareType
	le.PutUint32(body[4:8], flags)
	le.PutUint32(body[8:12], capabilities)
	le.PutUint32(body[12:16], access)
	return body
}

func encodeFileInformation(body []byte, information FileInformation) {
	le.PutUint64(body, information.CreationTime)
	le.PutUint64(body[8:16], information.AccessTime)
	le.PutUint64(body[16:24], information.WriteTime)
	le.PutUint64(body[24:32], information.ChangeTime)
	le.PutUint64(body[32:40], information.AllocationSize)
	le.PutUint64(body[40:48], information.EndOfFile)
	le.PutUint32(body[48:52], information.Attributes)
}

func CreateResponseBody(result CreateResult) []byte {
	body := make([]byte, 88)
	le.PutUint16(body, 89)
	body[2] = result.OplockLevel
	body[3] = result.Flags
	le.PutUint32(body[4:8], result.Action)
	encodeFileInformation(body[8:60], result.FileInformation)
	copy(body[64:80], result.FileID[:])
	return body
}

func CloseResponseBody(flags uint16, information FileInformation) []byte {
	body := make([]byte, 60)
	le.PutUint16(body, 60)
	le.PutUint16(body[2:4], flags)
	encodeFileInformation(body[8:60], information)
	return body
}

func ReadResponseBody(data []byte, remaining uint32) []byte {
	body := make([]byte, 16+len(data))
	le.PutUint16(body, 17)
	body[2] = HeaderSize + 16
	le.PutUint32(body[4:8], uint32(len(data)))
	le.PutUint32(body[8:12], remaining)
	copy(body[16:], data)
	return body
}

func WriteResponseBody(count, remaining uint32) []byte {
	body := make([]byte, 16)
	le.PutUint16(body, 17)
	le.PutUint32(body[4:8], count)
	le.PutUint32(body[8:12], remaining)
	return body
}

// BufferResponseBody is shared by QUERY_INFO and QUERY_DIRECTORY.
func BufferResponseBody(data []byte) []byte {
	body := make([]byte, 8+len(data))
	le.PutUint16(body, 9)
	if len(data) != 0 {
		le.PutUint16(body[2:4], HeaderSize+8)
		le.PutUint32(body[4:8], uint32(len(data)))
		copy(body[8:], data)
	}
	return body
}

func EmptyResponseBody() []byte { return []byte{4, 0, 0, 0} }

func ErrorResponseBody() []byte { return []byte{9, 0, 0, 0, 0, 0, 0, 0, 0} }

func align8(value int) int { return (value + 7) &^ 7 }
