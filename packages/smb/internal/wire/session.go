package wire

import "unicode/utf16"

type Context struct {
	Type uint16
	Data []byte
}
type NegotiateRequest struct {
	Dialects     []uint16
	SecurityMode uint16
	Capabilities uint32
	ClientGUID   [16]byte
	Contexts     []Context
}
type SessionSetupRequest struct {
	Flags             byte
	SecurityMode      byte
	Capabilities      uint32
	Channel           uint32
	PreviousSessionID uint64
	Token             []byte
}

func DecodeUTF16(data []byte) (string, error) {
	if len(data)&1 != 0 {
		return "", ErrMalformed
	}
	words := make([]uint16, len(data)/2)
	for i := range words {
		words[i] = le.Uint16(data[i*2:])
	}
	for i := 0; i < len(words); i++ {
		w := words[i]
		if w == 0 {
			return "", ErrMalformed
		}
		if w >= 0xd800 && w <= 0xdbff {
			if i+1 >= len(words) || words[i+1] < 0xdc00 || words[i+1] > 0xdfff {
				return "", ErrMalformed
			}
			i++
		} else if w >= 0xdc00 && w <= 0xdfff {
			return "", ErrMalformed
		}
	}
	return string(utf16.Decode(words)), nil
}

func EncodeUTF16(value string) []byte {
	words := utf16.Encode([]rune(value))
	data := make([]byte, len(words)*2)
	for i, w := range words {
		le.PutUint16(data[i*2:], w)
	}
	return data
}

func (r Request) Negotiate() (NegotiateRequest, error) {
	var out NegotiateRequest
	if err := r.fixed(Negotiate, 36, 36); err != nil {
		return out, err
	}
	n := int(le.Uint16(r.Body[2:4]))
	if n == 0 || n > (len(r.Body)-36)/2 {
		return out, ErrMalformed
	}
	out.SecurityMode = le.Uint16(r.Body[4:6])
	out.Capabilities = le.Uint32(r.Body[8:12])
	copy(out.ClientGUID[:], r.Body[12:28])
	out.Dialects = make([]uint16, n)
	has311 := false
	for i := range out.Dialects {
		out.Dialects[i] = le.Uint16(r.Body[36+i*2:])
		has311 = has311 || out.Dialects[i] == Dialect311
	}
	if !has311 {
		return out, nil
	}
	count := int(le.Uint16(r.Body[32:34]))
	off := uint64(le.Uint32(r.Body[28:32]))
	if count < 1 || count > r.maxContexts || off&7 != 0 || off < uint64(HeaderSize+36+n*2) {
		return out, ErrMalformed
	}
	seen := make(map[uint16]bool, count)
	for i := 0; i < count; i++ {
		if off+8 > uint64(len(r.Packet)) {
			return out, ErrMalformed
		}
		p := r.Packet[int(off):]
		kind := le.Uint16(p)
		length := uint64(le.Uint16(p[2:4]))
		if seen[kind] || off+8+length > uint64(len(r.Packet)) {
			return out, ErrMalformed
		}
		seen[kind] = true
		out.Contexts = append(out.Contexts, Context{Type: kind, Data: p[8 : 8+int(length)]})
		off = (off + 8 + length + 7) &^ 7
	}
	return out, nil
}

func (r Request) SessionSetup() (SessionSetupRequest, error) {
	var out SessionSetupRequest
	if err := r.fixed(SessionSetup, 25, 24); err != nil {
		return out, err
	}
	out.Flags = r.Body[2]
	out.SecurityMode = r.Body[3]
	out.Capabilities = le.Uint32(r.Body[4:8])
	out.Channel = le.Uint32(r.Body[8:12])
	out.PreviousSessionID = le.Uint64(r.Body[16:24])
	var err error
	out.Token, err = r.field(uint32(le.Uint16(r.Body[12:14])), uint32(le.Uint16(r.Body[14:16])), 88)
	return out, err
}

func (r Request) TreePath() (string, error) {
	if err := r.fixed(TreeConnect, 9, 8); err != nil {
		return "", err
	}
	if le.Uint16(r.Body[2:4]) != 0 {
		return "", ErrMalformed
	}
	data, err := r.field(uint32(le.Uint16(r.Body[4:6])), uint32(le.Uint16(r.Body[6:8])), 72)
	if err != nil {
		return "", err
	}
	return DecodeUTF16(data)
}

func (r Request) Empty() error {
	switch r.Header.Command {
	case Logoff, TreeDisconnect, Cancel, Echo:
		return r.fixed(r.Header.Command, 4, 4)
	default:
		return ErrMalformed
	}
}
