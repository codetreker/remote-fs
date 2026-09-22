package wire

import "unicode/utf16"

const (
	ContextPreauthIntegrity uint16 = 0x0001
	ContextEncryption       uint16 = 0x0002
	ContextCompression      uint16 = 0x0003
	ContextNetname          uint16 = 0x0005
	ContextTransport        uint16 = 0x0006
	ContextRDMATransform    uint16 = 0x0007
	ContextSigning          uint16 = 0x0008
)

const (
	HashSHA512     uint16 = 0x0001
	SigningAESCMAC uint16 = 0x0001
)

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

type PreauthCapabilities struct {
	HashAlgorithms []uint16
	Salt           []byte
}

type SigningCapabilities struct {
	Algorithms []uint16
}

func DecodeUTF16(data []byte) (string, error) {
	if len(data)&1 != 0 {
		return "", ErrMalformed
	}
	words := make([]uint16, len(data)/2)
	for index := range words {
		words[index] = le.Uint16(data[index*2:])
	}
	for index := 0; index < len(words); index++ {
		word := words[index]
		switch {
		case word == 0:
			return "", ErrMalformed
		case word >= 0xd800 && word <= 0xdbff:
			if index+1 >= len(words) || words[index+1] < 0xdc00 || words[index+1] > 0xdfff {
				return "", ErrMalformed
			}
			index++
		case word >= 0xdc00 && word <= 0xdfff:
			return "", ErrMalformed
		}
	}
	return string(utf16.Decode(words)), nil
}

func EncodeUTF16(value string) []byte {
	words := utf16.Encode([]rune(value))
	data := make([]byte, len(words)*2)
	for index, word := range words {
		le.PutUint16(data[index*2:], word)
	}
	return data
}

func (request Request) Negotiate() (NegotiateRequest, error) {
	var result NegotiateRequest
	if err := request.fixed(Negotiate, 36, 36); err != nil || request.Header.Flags&FlagSigned != 0 {
		return result, ErrMalformed
	}
	dialectCount := int(le.Uint16(request.Body[2:4]))
	if dialectCount == 0 || dialectCount > (len(request.Body)-36)/2 {
		return result, ErrMalformed
	}
	result.SecurityMode = le.Uint16(request.Body[4:6])
	result.Capabilities = le.Uint32(request.Body[8:12])
	copy(result.ClientGUID[:], request.Body[12:28])
	result.Dialects = make([]uint16, dialectCount)
	has311 := false
	for index := range result.Dialects {
		result.Dialects[index] = le.Uint16(request.Body[36+index*2:])
		has311 = has311 || result.Dialects[index] == Dialect311
	}
	if !has311 {
		return result, nil
	}

	contextCount := int(le.Uint16(request.Body[32:34]))
	offset := uint64(le.Uint32(request.Body[28:32]))
	minimum := uint64(HeaderSize + 36 + dialectCount*2)
	if contextCount < 1 || contextCount > request.maxContexts || offset&7 != 0 || offset < minimum {
		return result, ErrMalformed
	}
	seenUnique := make(map[uint16]struct{}, 5)
	preauthCount := 0
	for range contextCount {
		if offset+8 > uint64(len(request.Packet)) {
			return result, ErrMalformed
		}
		encoded := request.Packet[int(offset):]
		kind := le.Uint16(encoded)
		length := uint64(le.Uint16(encoded[2:4]))
		end := offset + 8 + length
		if end < offset || end > uint64(len(request.Packet)) {
			return result, ErrMalformed
		}
		if contextMustBeUnique(kind) {
			if _, exists := seenUnique[kind]; exists {
				return result, ErrMalformed
			}
			seenUnique[kind] = struct{}{}
		}
		if kind == ContextPreauthIntegrity {
			preauthCount++
		}
		result.Contexts = append(result.Contexts, Context{Type: kind, Data: encoded[8 : 8+int(length)]})
		offset = (end + 7) &^ 7
	}
	if preauthCount != 1 {
		return result, ErrMalformed
	}
	return result, nil
}

func contextMustBeUnique(kind uint16) bool {
	switch kind {
	case ContextPreauthIntegrity, ContextEncryption, ContextCompression, ContextRDMATransform, ContextSigning:
		return true
	default:
		return false
	}
}

func DecodePreauthCapabilities(data []byte) (PreauthCapabilities, error) {
	var result PreauthCapabilities
	if len(data) < 6 {
		return result, ErrMalformed
	}
	count := int(le.Uint16(data[:2]))
	saltLength := int(le.Uint16(data[2:4]))
	if count == 0 || count > (len(data)-4)/2 || 4+count*2+saltLength != len(data) {
		return result, ErrMalformed
	}
	result.HashAlgorithms = make([]uint16, count)
	for index := range result.HashAlgorithms {
		result.HashAlgorithms[index] = le.Uint16(data[4+index*2:])
	}
	result.Salt = data[4+count*2:]
	return result, nil
}

func DecodeSigningCapabilities(data []byte) (SigningCapabilities, error) {
	var result SigningCapabilities
	if len(data) < 4 {
		return result, ErrMalformed
	}
	count := int(le.Uint16(data[:2]))
	if count == 0 || count > (len(data)-2)/2 || 2+count*2 != len(data) {
		return result, ErrMalformed
	}
	result.Algorithms = make([]uint16, count)
	for index := range result.Algorithms {
		result.Algorithms[index] = le.Uint16(data[2+index*2:])
	}
	return result, nil
}

func (request Request) SessionSetup() (SessionSetupRequest, error) {
	var result SessionSetupRequest
	if err := request.fixed(SessionSetup, 25, 24); err != nil {
		return result, err
	}
	result.Flags = request.Body[2]
	result.SecurityMode = request.Body[3]
	result.Capabilities = le.Uint32(request.Body[4:8])
	result.Channel = le.Uint32(request.Body[8:12])
	result.PreviousSessionID = le.Uint64(request.Body[16:24])
	var err error
	result.Token, err = request.field(uint32(le.Uint16(request.Body[12:14])), uint32(le.Uint16(request.Body[14:16])), HeaderSize+24)
	return result, err
}

func (request Request) TreePath() (string, error) {
	if err := request.fixed(TreeConnect, 9, 8); err != nil || le.Uint16(request.Body[2:4]) != 0 {
		return "", ErrMalformed
	}
	data, err := request.field(uint32(le.Uint16(request.Body[4:6])), uint32(le.Uint16(request.Body[6:8])), HeaderSize+8)
	if err != nil || len(data) == 0 {
		return "", ErrMalformed
	}
	return DecodeUTF16(data)
}

func (request Request) Empty() error {
	switch request.Header.Command {
	case Logoff, TreeDisconnect, Cancel, Echo:
		if err := request.fixed(request.Header.Command, 4, 4); err != nil {
			return err
		}
		padding := request.Body[4:]
		if request.Header.NextCommand == 0 {
			if len(padding) != 0 {
				return ErrMalformed
			}
			return nil
		}
		want := (-(HeaderSize + 4)) & 7
		if len(padding) != want {
			return ErrMalformed
		}
		for _, value := range padding {
			if value != 0 {
				return ErrMalformed
			}
		}
		return nil
	default:
		return ErrMalformed
	}
}
