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

type FileID [16]byte

// Request slices borrow the input frame until its processing completes.
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
	h := Header{CreditCharge: le.Uint16(packet[6:8]), Status: le.Uint32(packet[8:12]), Command: le.Uint16(packet[12:14]), Credits: le.Uint16(packet[14:16]), Flags: le.Uint32(packet[16:20]), NextCommand: le.Uint32(packet[20:24]), MessageID: le.Uint64(packet[24:32]), SessionID: le.Uint64(packet[40:48])}
	if h.Flags&FlagAsync != 0 {
		h.AsyncID = le.Uint64(packet[32:40])
	} else {
		h.ProcessID = le.Uint32(packet[32:36])
		h.TreeID = le.Uint32(packet[36:40])
	}
	copy(h.Signature[:], packet[48:64])
	return h, nil
}

func (h Header) Encode(packet []byte) error {
	if len(packet) < HeaderSize {
		return ErrMalformed
	}
	clear(packet[:HeaderSize])
	copy(packet, "\xfeSMB")
	le.PutUint16(packet[4:6], HeaderSize)
	le.PutUint16(packet[6:8], h.CreditCharge)
	le.PutUint32(packet[8:12], h.Status)
	le.PutUint16(packet[12:14], h.Command)
	le.PutUint16(packet[14:16], h.Credits)
	le.PutUint32(packet[16:20], h.Flags)
	le.PutUint32(packet[20:24], h.NextCommand)
	le.PutUint64(packet[24:32], h.MessageID)
	if h.Flags&FlagAsync != 0 {
		le.PutUint64(packet[32:40], h.AsyncID)
	} else {
		le.PutUint32(packet[32:36], h.ProcessID)
		le.PutUint32(packet[36:40], h.TreeID)
	}
	le.PutUint64(packet[40:48], h.SessionID)
	copy(packet[48:64], h.Signature[:])
	return nil
}

// ParseFrame accepts one direct-TCP payload, excluding its four-byte length prefix.
func ParseFrame(packet []byte, limits Limits) ([]Request, error) {
	if limits.MaxBytes < HeaderSize || limits.MaxCommands < 1 || limits.MaxContexts < 1 || len(packet) > limits.MaxBytes {
		return nil, ErrMalformed
	}
	var requests []Request
	for {
		if len(requests) >= limits.MaxCommands {
			return nil, ErrMalformed
		}
		h, err := ParseHeader(packet)
		if err != nil || h.Flags&FlagResponse != 0 {
			return nil, ErrMalformed
		}
		if len(requests) == 0 && h.Flags&FlagRelated != 0 {
			return nil, ErrMalformed
		}
		end := len(packet)
		if h.NextCommand != 0 {
			if h.NextCommand < HeaderSize || h.NextCommand&7 != 0 || uint64(h.NextCommand)+HeaderSize > uint64(len(packet)) {
				return nil, ErrMalformed
			}
			end = int(h.NextCommand)
		}
		if end < HeaderSize+2 {
			return nil, ErrMalformed
		}
		requests = append(requests, Request{Header: h, Body: packet[HeaderSize:end], Packet: packet[:end], maxContexts: limits.MaxContexts})
		if h.NextCommand == 0 {
			return requests, nil
		}
		packet = packet[end:]
	}
}

func EncodeResponse(header Header, body []byte) []byte {
	header.Flags |= FlagResponse
	packet := make([]byte, HeaderSize+len(body))
	_ = header.Encode(packet)
	copy(packet[HeaderSize:], body)
	return packet
}

func (r Request) fixed(command uint16, size uint16, minimum int) error {
	if r.Header.Command != command || len(r.Body) < minimum || le.Uint16(r.Body[:2]) != size {
		return ErrMalformed
	}
	return nil
}

func (r Request) field(offset uint32, length uint32, minimum int) ([]byte, error) {
	if length == 0 {
		if offset != 0 && (uint64(offset) < uint64(minimum) || uint64(offset) > uint64(len(r.Packet))) {
			return nil, ErrMalformed
		}
		return nil, nil
	}
	end := uint64(offset) + uint64(length)
	if uint64(offset) < uint64(minimum) || end > uint64(len(r.Packet)) {
		return nil, ErrMalformed
	}
	return r.Packet[int(offset):int(end)], nil
}
