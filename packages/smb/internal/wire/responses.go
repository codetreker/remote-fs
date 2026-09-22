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

func EmptyResponseBody() []byte { return []byte{4, 0, 0, 0} }

func ErrorResponseBody() []byte { return []byte{9, 0, 0, 0, 0, 0, 0, 0, 0} }

func align8(value int) int { return (value + 7) &^ 7 }
