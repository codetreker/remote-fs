package wire

const (
	ChannelNone             uint32 = 0
	ChannelRDMAV1           uint32 = 1
	ChannelRDMAV1Invalidate uint32 = 2
	ChannelRDMATransform    uint32 = 3
)

const (
	ReadFlagUnbuffered        byte   = 1
	ReadFlagRequestCompressed byte   = 2
	WriteFlagWriteThrough     uint32 = 1
	WriteFlagUnbuffered       uint32 = 2
)

type ReadRequest struct {
	Padding           byte
	Flags             byte
	Length            uint32
	Offset            uint64
	FileID            FileID
	MinimumCount      uint32
	Channel           uint32
	RemainingBytes    uint32
	ChannelInfoOffset uint16
	ChannelInfoLength uint16
	ChannelInfo       []byte
}

type WriteRequest struct {
	DataOffset        uint16
	Length            uint32
	Offset            uint64
	FileID            FileID
	Channel           uint32
	RemainingBytes    uint32
	ChannelInfoOffset uint16
	ChannelInfoLength uint16
	Flags             uint32
	Data              []byte
	ChannelInfo       []byte
}

type FlushRequest struct {
	FileID FileID
}

func (request Request) Read() (ReadRequest, error) {
	var result ReadRequest
	if err := request.fixed(Read, 49, 48); err != nil {
		return result, err
	}
	result.Padding = request.Body[2]
	result.Flags = request.Body[3]
	result.Length = le.Uint32(request.Body[4:8])
	result.Offset = le.Uint64(request.Body[8:16])
	copy(result.FileID[:], request.Body[16:32])
	result.MinimumCount = le.Uint32(request.Body[32:36])
	result.Channel = le.Uint32(request.Body[36:40])
	result.RemainingBytes = le.Uint32(request.Body[40:44])
	result.ChannelInfoOffset = le.Uint16(request.Body[44:46])
	result.ChannelInfoLength = le.Uint16(request.Body[46:48])
	if result.FileID.HasPartialRelatedPlaceholder() {
		return ReadRequest{}, ErrMalformed
	}
	// Channel NONE makes these fields reserved, so even out-of-range offsets
	// are ignored. https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/320f04f3-1b28-45cd-aaa1-9e5aed810dca
	if result.Channel != ChannelNone {
		var err error
		result.ChannelInfo, err = request.field(uint32(result.ChannelInfoOffset), uint32(result.ChannelInfoLength), HeaderSize+48)
		if err != nil {
			return ReadRequest{}, err
		}
	}
	return result, nil
}

func (request Request) Write() (WriteRequest, error) {
	var result WriteRequest
	if err := request.fixed(Write, 49, 48); err != nil {
		return result, err
	}
	result.DataOffset = le.Uint16(request.Body[2:4])
	result.Length = le.Uint32(request.Body[4:8])
	result.Offset = le.Uint64(request.Body[8:16])
	copy(result.FileID[:], request.Body[16:32])
	result.Channel = le.Uint32(request.Body[32:36])
	result.RemainingBytes = le.Uint32(request.Body[36:40])
	result.ChannelInfoOffset = le.Uint16(request.Body[40:42])
	result.ChannelInfoLength = le.Uint16(request.Body[42:44])
	result.Flags = le.Uint32(request.Body[44:48])
	if result.FileID.HasPartialRelatedPlaceholder() {
		return WriteRequest{}, ErrMalformed
	}
	if result.Channel == ChannelNone && (result.DataOffset > 256 || uint64(result.DataOffset) > uint64(len(request.Packet))) {
		return WriteRequest{}, ErrMalformed
	}
	if result.Length != 0 {
		var err error
		result.Data, err = request.field(uint32(result.DataOffset), result.Length, HeaderSize+48)
		if err != nil {
			return WriteRequest{}, err
		}
	}
	// Channel NONE makes these fields reserved, so even out-of-range offsets
	// are ignored. https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/e7046961-3318-4350-be2a-a8d69bb59ce8
	if result.Channel != ChannelNone {
		var err error
		result.ChannelInfo, err = request.field(uint32(result.ChannelInfoOffset), uint32(result.ChannelInfoLength), HeaderSize+48)
		if err != nil {
			return WriteRequest{}, err
		}
	}
	return result, nil
}

func (request Request) Flush() (FlushRequest, error) {
	var result FlushRequest
	if err := request.fixed(Flush, 24, 24); err != nil {
		return result, err
	}
	if request.Header.NextCommand == 0 && len(request.Body) != 24 ||
		request.Header.NextCommand != 0 && len(request.Body) != align8(HeaderSize+24)-HeaderSize {
		return result, ErrMalformed
	}
	copy(result.FileID[:], request.Body[8:24])
	if result.FileID.HasPartialRelatedPlaceholder() {
		return FlushRequest{}, ErrMalformed
	}
	return result, nil
}

func ReadResponseBody(data []byte) ([]byte, error) {
	if len(data) > maxDirectTCPPayload-HeaderSize-16 {
		return nil, ErrMalformed
	}
	body := make([]byte, 16+len(data))
	le.PutUint16(body, 17)
	body[2] = HeaderSize + 16
	le.PutUint32(body[4:8], uint32(len(data)))
	copy(body[16:], data)
	return body, nil
}

func WriteResponseBody(count uint32) []byte {
	body := make([]byte, 16)
	le.PutUint16(body, 17)
	le.PutUint32(body[4:8], count)
	return body
}

func FlushResponseBody() []byte { return EmptyResponseBody() }
