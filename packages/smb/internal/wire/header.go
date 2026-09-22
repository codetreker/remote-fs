package wire

import (
	"encoding/binary"
	"errors"
)

const HeaderSize = 64

const (
	Negotiate uint16 = iota
	SessionSetup
	Logoff
	TreeConnect
	TreeDisconnect
	Create
	Close
	Flush
	Read
	Write
	Lock
	IOCTL
	Cancel
	Echo
	QueryDirectory
	ChangeNotify
	QueryInfo
	SetInfo
	OplockBreak
)

const (
	FlagResponse uint32 = 1 << iota
	FlagAsync
	FlagRelated
	FlagSigned
	FlagPriorityMask uint32 = 0x70
	FlagDFS          uint32 = 0x10000000
	FlagReplay       uint32 = 0x20000000
)

const (
	Dialect300 uint16 = 0x0300
	Dialect302 uint16 = 0x0302
	Dialect311 uint16 = 0x0311
)

var ErrMalformed = errors.New("malformed SMB message")

var le = binary.LittleEndian

type Limits struct {
	MaxBytes    int
	MaxCommands int
	MaxContexts int
}

type Header struct {
	CreditCharge uint16
	Status       uint32
	Command      uint16
	Credits      uint16
	Flags        uint32
	NextCommand  uint32
	MessageID    uint64
	ProcessID    uint32
	TreeID       uint32
	AsyncID      uint64
	SessionID    uint64
	Signature    [16]byte
}

// Request borrows Packet and Body from the bounded transport frame. Packet is
// the exact command bytes, including compound padding, used by signing and the
// preauthentication transcript.
type Request struct {
	Header      Header
	Body        []byte
	Packet      []byte
	maxContexts int
}

func ParseHeader(packet []byte) (Header, error) {
	if len(packet) < HeaderSize || string(packet[:4]) != "\xfeSMB" || le.Uint16(packet[4:6]) != HeaderSize {
		return Header{}, ErrMalformed
	}
	header := Header{
		CreditCharge: le.Uint16(packet[6:8]),
		Status:       le.Uint32(packet[8:12]),
		Command:      le.Uint16(packet[12:14]),
		Credits:      le.Uint16(packet[14:16]),
		Flags:        le.Uint32(packet[16:20]),
		NextCommand:  le.Uint32(packet[20:24]),
		MessageID:    le.Uint64(packet[24:32]),
		SessionID:    le.Uint64(packet[40:48]),
	}
	if header.Flags&FlagAsync != 0 {
		header.AsyncID = le.Uint64(packet[32:40])
	} else {
		header.ProcessID = le.Uint32(packet[32:36])
		header.TreeID = le.Uint32(packet[36:40])
	}
	copy(header.Signature[:], packet[48:64])
	return header, nil
}

func (header Header) Encode(packet []byte) error {
	if len(packet) < HeaderSize {
		return ErrMalformed
	}
	clear(packet[:HeaderSize])
	copy(packet, "\xfeSMB")
	le.PutUint16(packet[4:6], HeaderSize)
	le.PutUint16(packet[6:8], header.CreditCharge)
	le.PutUint32(packet[8:12], header.Status)
	le.PutUint16(packet[12:14], header.Command)
	le.PutUint16(packet[14:16], header.Credits)
	le.PutUint32(packet[16:20], header.Flags)
	le.PutUint32(packet[20:24], header.NextCommand)
	le.PutUint64(packet[24:32], header.MessageID)
	if header.Flags&FlagAsync != 0 {
		le.PutUint64(packet[32:40], header.AsyncID)
	} else {
		le.PutUint32(packet[32:36], header.ProcessID)
		le.PutUint32(packet[36:40], header.TreeID)
	}
	le.PutUint64(packet[40:48], header.SessionID)
	copy(packet[48:64], header.Signature[:])
	return nil
}

// ParseFrame accepts one direct-TCP payload after its four-byte length prefix.
// Every returned slice remains owned by the caller's packet until processing
// and response signing have completed.
func ParseFrame(packet []byte, limits Limits) ([]Request, error) {
	if limits.MaxBytes < HeaderSize || limits.MaxBytes > 0xffffff || limits.MaxCommands < 1 || limits.MaxContexts < 1 || len(packet) < HeaderSize || len(packet) > limits.MaxBytes {
		return nil, ErrMalformed
	}
	requests := make([]Request, 0, min(limits.MaxCommands, 4))
	remaining := packet
	compoundRelated := false
	for {
		if len(requests) >= limits.MaxCommands {
			return nil, ErrMalformed
		}
		header, err := ParseHeader(remaining)
		related := header.Flags&FlagRelated != 0
		if err != nil || header.Command > OplockBreak || header.Flags&FlagResponse != 0 || len(requests) == 0 && related {
			return nil, ErrMalformed
		}
		if len(requests) == 1 {
			compoundRelated = related
		} else if len(requests) > 1 && related != compoundRelated {
			return nil, ErrMalformed
		}
		commandBytes := len(remaining)
		if header.NextCommand != 0 {
			next := uint64(header.NextCommand)
			if next < HeaderSize+2 || next&7 != 0 || next+HeaderSize > uint64(len(remaining)) {
				return nil, ErrMalformed
			}
			commandBytes = int(next)
		}
		if commandBytes < HeaderSize+2 {
			return nil, ErrMalformed
		}
		command := remaining[:commandBytes]
		requests = append(requests, Request{
			Header: header, Body: command[HeaderSize:], Packet: command,
			maxContexts: limits.MaxContexts,
		})
		if header.NextCommand == 0 {
			return requests, nil
		}
		remaining = remaining[commandBytes:]
	}
}

func EncodeResponse(header Header, body []byte) []byte {
	header.Flags |= FlagResponse
	packet := make([]byte, HeaderSize+len(body))
	_ = header.Encode(packet)
	copy(packet[HeaderSize:], body)
	return packet
}

func (request Request) fixed(command uint16, structureSize uint16, minimum int) error {
	if request.Header.Command != command || len(request.Body) < minimum || le.Uint16(request.Body[:2]) != structureSize {
		return ErrMalformed
	}
	return nil
}

func (request Request) field(offset, length uint32, minimum int) ([]byte, error) {
	start := uint64(offset)
	if length == 0 {
		if offset != 0 && (start < uint64(minimum) || start > uint64(len(request.Packet))) {
			return nil, ErrMalformed
		}
		return nil, nil
	}
	end := start + uint64(length)
	if start < uint64(minimum) || end < start || end > uint64(len(request.Packet)) {
		return nil, ErrMalformed
	}
	return request.Packet[int(start):int(end)], nil
}
