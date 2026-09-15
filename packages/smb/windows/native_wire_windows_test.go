//go:build windows

package windows

import (
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"unicode/utf16"
)

type nativeWireObservation struct {
	connections    atomic.Uint64
	writes         atomic.Uint64
	signedWrites   atomic.Uint64
	readBytes      atomic.Uint64
	writtenBytes   atomic.Uint64
	mu             sync.Mutex
	headers        []nativeHeaderObservation
	trees          []nativeTreeObservation
	creates        []nativeCreateObservation
	operations     []nativeOperationObservation
	negotiations   []nativeNegotiateObservation
	createReplies  []nativeCreateReplyObservation
	createSequence atomic.Uint64
	leaseResponses atomic.Uint64
	unsafeCaching  atomic.Bool
}

type nativeNegotiateObservation struct {
	Dialect      uint16
	Capabilities uint32
}
type nativeLeaseObservation struct {
	Version  int
	State    uint32
	Epoch    uint16
	Duration uint64
}
type nativeCreateReplyObservation struct {
	MessageID uint64
	Oplock    byte
	Leases    []nativeLeaseObservation
	Truncated bool
}

type nativeOperationObservation struct {
	MessageID                          uint64
	Command                            uint16
	InfoType, Class                    byte
	ControlCode                        uint32
	Flags, SecurityMode                byte
	Capabilities, Channel              uint32
	PreviousSessionID, HeaderSessionID uint64
	TokenOffset, TokenLength           uint16
	BodyLength                         int
}

type nativeCreateObservation struct {
	Sequence                                                             uint64
	Name                                                                 string
	MessageID                                                            uint64
	SecurityFlags, Oplock                                                byte
	Impersonation, Access, Attributes, ShareAccess, Disposition, Options uint32
	Contexts                                                             []string
	Truncated                                                            bool
}

func nativeRetainLast[T any](items []T, item T, limit int) []T {
	if len(items) < limit {
		return append(items, item)
	}
	copy(items, items[1:])
	items[len(items)-1] = item
	return items
}

type nativeTreeObservation struct {
	MessageID      uint64
	Path           string
	Offset, Length uint16
	Truncated      bool
}

func TestNativeWireObservationKeepsBoundedProtocolEvidence(t *testing.T) {
	var observation nativeWireObservation
	request := nativeFrameObserver{observation: &observation}
	response := nativeFrameObserver{observation: &observation, response: true}
	frame := make([]byte, 4+64+128)
	binary.BigEndian.PutUint32(frame[:4], uint32(len(frame)-4))
	copy(frame[4:8], []byte{0xfe, 'S', 'M', 'B'})
	binary.LittleEndian.PutUint16(frame[10:12], 1)
	binary.LittleEndian.PutUint16(frame[16:18], 9)
	binary.LittleEndian.PutUint32(frame[20:24], 8)
	binary.LittleEndian.PutUint64(frame[28:36], 42)
	frame[52] = 1
	for _, b := range frame {
		request.observe([]byte{b})
	}
	binary.LittleEndian.PutUint32(frame[12:16], 0xc0000022)
	response.observe(frame)
	if observation.writes.Load() != 1 || observation.signedWrites.Load() != 1 || len(observation.headers) != 2 {
		t.Fatalf("request accounting: writes=%d signed=%d headers=%d", observation.writes.Load(), observation.signedWrites.Load(), len(observation.headers))
	}
	got := observation.headers[1]
	if !got.Response || got.Protocol != 0xfe534d42 || got.Command != 9 || got.MessageID != 42 || got.CreditCharge != 1 || got.Flags != 8 || got.Status != 0xc0000022 {
		t.Fatalf("response evidence: %+v", got)
	}
	for range 32 {
		response.observe(frame)
	}
	if len(observation.headers) != 16 || observation.writes.Load() != 1 {
		t.Fatalf("bounded response evidence: headers=%d writes=%d", len(observation.headers), observation.writes.Load())
	}
}

type nativeHeaderObservation struct {
	Response     bool
	Protocol     uint32
	Command      uint16
	MessageID    uint64
	CreditCharge uint16
	Flags        uint32
	Status       uint32
}
type nativeObservedListener struct {
	net.Listener
	observation *nativeWireObservation
}

func (l nativeObservedListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.observation.connections.Add(1)
	return &nativeObservedConnection{Conn: connection,
		received: nativeFrameObserver{observation: l.observation},
		sent:     nativeFrameObserver{observation: l.observation, response: true}}, nil
}

// Only bounded protocol metadata is logged. Variable body capture stays within
// the first TREE_CONNECT or CREATE member; context output is limited to names
// and fixed lease grant fields.
type nativeObservedConnection struct {
	net.Conn
	received nativeFrameObserver
	sent     nativeFrameObserver
	writeMu  sync.Mutex
}

type nativeFrameObserver struct {
	observation    *nativeWireObservation
	response       bool
	prefix         [4]byte
	prefixBytes    int
	remaining      int
	header         [64]byte
	headerBytes    int
	frameBytes     int
	tree           [1032]byte
	treeBytes      int
	create         [4096]byte
	createBytes    int
	operation      [24]byte
	operationBytes int
	negotiate      [28]byte
	negotiateBytes int
}

func (c *nativeObservedConnection) Read(buffer []byte) (int, error) {
	n, err := c.Conn.Read(buffer)
	c.received.observation.readBytes.Add(uint64(n))
	c.received.observe(buffer[:n])
	return n, err
}
func (c *nativeObservedConnection) Write(buffer []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	n, err := c.Conn.Write(buffer)
	c.sent.observation.writtenBytes.Add(uint64(n))
	c.sent.observe(buffer[:n])
	return n, err
}
func (c *nativeFrameObserver) observe(data []byte) {
	for len(data) > 0 {
		if c.remaining == 0 {
			n := copy(c.prefix[c.prefixBytes:], data)
			c.prefixBytes += n
			data = data[n:]
			if c.prefixBytes < 4 {
				return
			}
			c.remaining = int(binary.BigEndian.Uint32(c.prefix[:]))
			c.prefixBytes = 0
			c.headerBytes = 0
			c.frameBytes, c.treeBytes, c.createBytes = 0, 0, 0
			c.operationBytes = 0
			c.negotiateBytes = 0
			if c.remaining == 0 {
				continue
			}
		}
		n := min(len(data), c.remaining)
		if c.headerBytes < len(c.header) {
			copied := copy(c.header[c.headerBytes:], data[:n])
			c.headerBytes += copied
			if c.headerBytes == len(c.header) {
				h := nativeHeaderObservation{Response: c.response, Protocol: binary.BigEndian.Uint32(c.header[:4])}
				if h.Protocol == 0xfe534d42 {
					h.Command = binary.LittleEndian.Uint16(c.header[12:14])
					h.MessageID = binary.LittleEndian.Uint64(c.header[24:32])
					h.CreditCharge = binary.LittleEndian.Uint16(c.header[6:8])
					h.Flags = binary.LittleEndian.Uint32(c.header[16:20])
					if c.response {
						h.Status = binary.LittleEndian.Uint32(c.header[8:12])
					}
				}
				c.observation.mu.Lock()
				c.observation.headers = nativeRetainLast(c.observation.headers, h, 16)
				c.observation.mu.Unlock()
			}
			if !c.response && c.headerBytes == len(c.header) && c.header[0] == 0xfe && string(c.header[1:4]) == "SMB" && binary.LittleEndian.Uint16(c.header[12:14]) == 9 {
				c.observation.writes.Add(1)
				var signature byte
				for _, b := range c.header[48:64] {
					signature |= b
				}
				if binary.LittleEndian.Uint32(c.header[16:20])&8 != 0 && signature != 0 {
					c.observation.signedWrites.Add(1)
				}
			}
		}
		if !c.response && c.headerBytes == len(c.header) && binary.BigEndian.Uint32(c.header[:4]) == 0xfe534d42 && binary.LittleEndian.Uint16(c.header[12:14]) == 3 {
			start, end := max(c.frameBytes, 64), min(c.frameBytes+n, 64+len(c.tree))
			if next := int(binary.LittleEndian.Uint32(c.header[20:24])); next != 0 {
				end = min(end, next)
			}
			if start < end {
				copy(c.tree[start-64:end-64], data[start-c.frameBytes:end-c.frameBytes])
				c.treeBytes = end - 64
			}
		}
		if c.headerBytes == len(c.header) && binary.BigEndian.Uint32(c.header[:4]) == 0xfe534d42 && binary.LittleEndian.Uint16(c.header[12:14]) == 5 && (!c.response || binary.LittleEndian.Uint32(c.header[8:12]) == 0) {
			start, end := max(c.frameBytes, 64), min(c.frameBytes+n, 64+len(c.create))
			if next := int(binary.LittleEndian.Uint32(c.header[20:24])); next != 0 {
				end = min(end, next)
			}
			if start < end {
				copy(c.create[start-64:end-64], data[start-c.frameBytes:end-c.frameBytes])
				c.createBytes = end - 64
			}
		}
		if !c.response && c.headerBytes == len(c.header) && binary.BigEndian.Uint32(c.header[:4]) == 0xfe534d42 {
			command := binary.LittleEndian.Uint16(c.header[12:14])
			if command == 14 || command == 16 || command == 11 || command == 1 {
				fixedEnd := 72
				if command == 1 {
					fixedEnd = 88
				}
				start, end := max(c.frameBytes, 64), min(c.frameBytes+n, fixedEnd)
				if next := int(binary.LittleEndian.Uint32(c.header[20:24])); next != 0 {
					end = min(end, next)
				}
				if start < end {
					copy(c.operation[start-64:end-64], data[start-c.frameBytes:end-c.frameBytes])
					c.operationBytes = end - 64
				}
			}
		}
		if c.response && c.headerBytes == 64 && binary.BigEndian.Uint32(c.header[:4]) == 0xfe534d42 && binary.LittleEndian.Uint16(c.header[12:14]) == 0 && binary.LittleEndian.Uint32(c.header[8:12]) == 0 {
			start, end := max(c.frameBytes, 64), min(c.frameBytes+n, 92)
			if next := int(binary.LittleEndian.Uint32(c.header[20:24])); next != 0 {
				end = min(end, next)
			}
			if start < end {
				copy(c.negotiate[start-64:end-64], data[start-c.frameBytes:end-c.frameBytes])
				c.negotiateBytes = end - 64
			}
		}
		c.frameBytes += n
		data = data[n:]
		c.remaining -= n
		if c.remaining == 0 && (c.operationBytes == 8 || c.operationBytes == 24) {
			h := nativeOperationObservation{MessageID: binary.LittleEndian.Uint64(c.header[24:32]), Command: binary.LittleEndian.Uint16(c.header[12:14])}
			switch h.Command {
			case 1:
				if c.operationBytes != 24 {
					continue
				}
				commandEnd := c.frameBytes
				if next := int(binary.LittleEndian.Uint32(c.header[20:24])); next != 0 {
					commandEnd = min(commandEnd, next)
				}
				tokenOffset, tokenLength := binary.LittleEndian.Uint16(c.operation[12:14]), binary.LittleEndian.Uint16(c.operation[14:16])
				if tokenLength != 0 && (tokenOffset < 88 || int(tokenOffset)+int(tokenLength) > commandEnd) {
					clear(c.operation[:])
					continue
				}
				h.Flags, h.SecurityMode = c.operation[2], c.operation[3]
				h.Capabilities, h.Channel = binary.LittleEndian.Uint32(c.operation[4:8]), binary.LittleEndian.Uint32(c.operation[8:12])
				h.TokenOffset, h.TokenLength = binary.LittleEndian.Uint16(c.operation[12:14]), binary.LittleEndian.Uint16(c.operation[14:16])
				h.PreviousSessionID, h.HeaderSessionID = binary.LittleEndian.Uint64(c.operation[16:24]), binary.LittleEndian.Uint64(c.header[40:48])
				h.BodyLength = c.frameBytes - 64
				if next := int(binary.LittleEndian.Uint32(c.header[20:24])); next != 0 {
					h.BodyLength = min(h.BodyLength, next-64)
				}
			case 14:
				h.Class = c.operation[2]
			case 16:
				h.InfoType, h.Class = c.operation[2], c.operation[3]
			case 11:
				h.ControlCode = binary.LittleEndian.Uint32(c.operation[4:8])
			}
			c.observation.mu.Lock()
			c.observation.operations = nativeRetainLast(c.observation.operations, h, 16)
			c.observation.mu.Unlock()
		}
		if c.remaining == 0 && c.negotiateBytes == 28 {
			h := nativeNegotiateObservation{Dialect: binary.LittleEndian.Uint16(c.negotiate[4:6]), Capabilities: binary.LittleEndian.Uint32(c.negotiate[24:28])}
			c.observation.mu.Lock()
			c.observation.negotiations = nativeRetainLast(c.observation.negotiations, h, 8)
			c.observation.mu.Unlock()
		}
		if c.remaining == 0 && c.createBytes >= 56 {
			if c.response {
				c.recordCreateReply()
			} else {
				c.recordCreate()
			}
		}
		if c.remaining == 0 && c.treeBytes >= 8 {
			h := nativeTreeObservation{MessageID: binary.LittleEndian.Uint64(c.header[24:32]), Offset: binary.LittleEndian.Uint16(c.tree[4:6]), Length: binary.LittleEndian.Uint16(c.tree[6:8])}
			start, end := int(h.Offset)-64, int(h.Offset)-64+int(h.Length)
			if start < 8 || end > c.treeBytes || h.Length%2 != 0 {
				h.Truncated = true
			} else {
				words := make([]uint16, h.Length/2)
				for i := range words {
					words[i] = binary.LittleEndian.Uint16(c.tree[start+2*i:])
				}
				h.Path = string(utf16.Decode(words))
			}
			c.observation.mu.Lock()
			c.observation.trees = nativeRetainLast(c.observation.trees, h, 8)
			c.observation.mu.Unlock()
		}
	}
}

func TestNativeWireObservationCapturesOnlyBoundedTreePaths(t *testing.T) {
	var observation nativeWireObservation
	observer := nativeFrameObserver{observation: &observation}
	path := `\\127.0.0.1\rfs-native-123`
	words := utf16.Encode([]rune(path))
	frame := make([]byte, 4+72+2*len(words))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(frame)-4))
	copy(frame[4:8], []byte{0xfe, 'S', 'M', 'B'})
	binary.LittleEndian.PutUint16(frame[16:18], 3)
	binary.LittleEndian.PutUint64(frame[28:36], 17)
	binary.LittleEndian.PutUint16(frame[68:70], 9)
	binary.LittleEndian.PutUint16(frame[72:74], 72)
	binary.LittleEndian.PutUint16(frame[74:76], uint16(2*len(words)))
	for i, word := range words {
		binary.LittleEndian.PutUint16(frame[76+2*i:], word)
	}
	for _, b := range frame {
		observer.observe([]byte{b})
	}
	if len(observation.trees) != 1 || observation.trees[0].Path != path || observation.trees[0].MessageID != 17 || observation.trees[0].Truncated {
		t.Fatalf("fragmented TREE_CONNECT path: %+v", observation.trees)
	}
	binary.LittleEndian.PutUint16(frame[16:18], 1)
	observer.observe(frame)
	if len(observation.trees) != 1 {
		t.Fatal("session payload was interpreted as a tree path")
	}
	binary.LittleEndian.PutUint16(frame[16:18], 3)
	binary.LittleEndian.PutUint16(frame[74:76], 65534)
	observer.observe(frame)
	if len(observation.trees) != 2 || !observation.trees[1].Truncated || observation.trees[1].Path != "" {
		t.Fatalf("out-of-bound path: %+v", observation.trees)
	}
	for range 16 {
		observer.observe(frame)
	}
	if len(observation.trees) != 8 {
		t.Fatalf("retained %d paths", len(observation.trees))
	}
}

func (c *nativeFrameObserver) recordCreate() {
	b := c.create[:c.createBytes]
	h := nativeCreateObservation{Sequence: c.observation.createSequence.Add(1), MessageID: binary.LittleEndian.Uint64(c.header[24:32]), SecurityFlags: b[2], Oplock: b[3], Impersonation: binary.LittleEndian.Uint32(b[4:8]), Access: binary.LittleEndian.Uint32(b[24:28]), Attributes: binary.LittleEndian.Uint32(b[28:32]), ShareAccess: binary.LittleEndian.Uint32(b[32:36]), Disposition: binary.LittleEndian.Uint32(b[36:40]), Options: binary.LittleEndian.Uint32(b[40:44])}
	offset, length := uint64(binary.LittleEndian.Uint32(b[48:52])), uint64(binary.LittleEndian.Uint32(b[52:56]))
	nameOffset, nameLength := uint64(binary.LittleEndian.Uint16(b[44:46])), uint64(binary.LittleEndian.Uint16(b[46:48]))
	if nameLength != 0 {
		if nameOffset < 120 || nameLength > 1024 || nameLength%2 != 0 || nameOffset+nameLength > uint64(64+len(b)) {
			h.Truncated = true
		} else {
			words := make([]uint16, nameLength/2)
			for i := range words {
				words[i] = binary.LittleEndian.Uint16(b[nameOffset-64+uint64(2*i):])
			}
			h.Name = string(utf16.Decode(words))
		}
	}
	if length != 0 {
		if offset < 120 || offset+length > uint64(64+len(b)) || nameLength != 0 && (nameOffset < 120 || nameOffset+nameLength > uint64(64+len(b)) || nameOffset < offset+length && offset < nameOffset+nameLength) {
			h.Truncated = true
		} else {
			contexts := b[offset-64 : offset-64+length]
			for len(contexts) > 0 {
				if len(contexts) < 16 || len(h.Contexts) == 16 {
					h.Truncated = true
					break
				}
				next := int(binary.LittleEndian.Uint32(contexts[:4]))
				end := len(contexts)
				if next != 0 {
					if next < 16 || next > end {
						h.Truncated = true
						break
					}
					end = next
				}
				name, size := int(binary.LittleEndian.Uint16(contexts[4:6])), int(binary.LittleEndian.Uint16(contexts[6:8]))
				data, dataSize := uint64(binary.LittleEndian.Uint16(contexts[10:12])), uint64(binary.LittleEndian.Uint32(contexts[12:16]))
				if name < 16 || size == 0 || size > 32 || name+size > end || dataSize != 0 && (data < 16 || data+dataSize > uint64(end) || uint64(name) < data+dataSize && data < uint64(name+size)) {
					h.Truncated = true
					break
				}
				h.Contexts = append(h.Contexts, string(contexts[name:name+size]))
				if next == 0 {
					break
				}
				contexts = contexts[next:]
			}
		}
	}
	if h.Truncated {
		h.Contexts = nil
		h.Name = ""
	}
	clear(c.create[:])
	c.observation.mu.Lock()
	c.observation.creates = nativeRetainLast(c.observation.creates, h, 8)
	c.observation.mu.Unlock()
}

func TestNativeWireObservationKeepsRecentCreateMetadata(t *testing.T) {
	var observation nativeWireObservation
	observer := nativeFrameObserver{observation: &observation}
	frame := make([]byte, 4+144)
	binary.BigEndian.PutUint32(frame[:4], 144)
	copy(frame[4:8], []byte{0xfe, 'S', 'M', 'B'})
	binary.LittleEndian.PutUint16(frame[16:18], 5)
	b := frame[68:]
	b[2], b[3] = 0, 0xff
	for offset, value := range map[int]uint32{4: 2, 24: 0x12019f, 28: 0x80, 32: 7, 36: 5, 40: 0x40, 48: 120, 52: 24} {
		binary.LittleEndian.PutUint32(b[offset:], value)
	}
	binary.LittleEndian.PutUint16(b[60:62], 16)
	binary.LittleEndian.PutUint16(b[62:64], 4)
	copy(b[72:], "MxAc")
	for id := uint64(1); id <= 20; id++ {
		binary.LittleEndian.PutUint64(frame[28:36], id)
		for _, v := range frame {
			observer.observe([]byte{v})
		}
	}
	if len(observation.creates) != 8 || observation.creates[0].MessageID != 13 {
		t.Fatalf("recent creates: %+v", observation.creates)
	}
	c := observation.creates[7]
	if c.MessageID != 20 || c.Access != 0x12019f || c.Attributes != 0x80 || c.ShareAccess != 7 || c.Disposition != 5 || c.Options != 0x40 || c.Oplock != 0xff || c.Impersonation != 2 || c.Truncated || len(c.Contexts) != 1 || c.Contexts[0] != "MxAc" {
		t.Fatalf("CREATE evidence: %+v", c)
	}
	if len(observation.headers) != 16 || observation.headers[0].MessageID != 5 {
		t.Fatal("handshake-era headers displaced recent operations")
	}
	binary.LittleEndian.PutUint32(frame[24:28], 120)
	observer.observe(frame)
	c = observation.creates[7]
	if !c.Truncated || len(c.Contexts) != 0 {
		t.Fatalf("context crossed NextCommand: %+v", c)
	}
}

func TestNativeWireObservationRecordsOnlyQueryScalars(t *testing.T) {
	var observation nativeWireObservation
	observer := nativeFrameObserver{observation: &observation}
	frame := make([]byte, 4+72)
	binary.BigEndian.PutUint32(frame[:4], 72)
	copy(frame[4:8], []byte{0xfe, 'S', 'M', 'B'})
	frame[70], frame[71] = 1, 5
	binary.LittleEndian.PutUint32(frame[72:76], 0x00140204)
	for _, command := range []uint16{14, 16, 11, 1, 9} {
		binary.LittleEndian.PutUint16(frame[16:18], command)
		observer.observe(frame)
	}
	if len(observation.operations) != 3 || observation.operations[0].Class != 1 || observation.operations[1].InfoType != 1 || observation.operations[1].Class != 5 || observation.operations[2].ControlCode != 0x00140204 {
		t.Fatalf("query scalars: %+v", observation.operations)
	}
}

func TestNativeCreateObservationRejectsOverlappingContextValues(t *testing.T) {
	for _, overlap := range []string{"context data", "filename"} {
		t.Run(overlap, func(t *testing.T) {
			var observation nativeWireObservation
			observer := nativeFrameObserver{observation: &observation, createBytes: 80}
			b := observer.create[:]
			binary.LittleEndian.PutUint32(b[48:52], 120)
			binary.LittleEndian.PutUint32(b[52:56], 24)
			binary.LittleEndian.PutUint16(b[60:62], 16)
			binary.LittleEndian.PutUint16(b[62:64], 4)
			copy(b[72:], "DATA")
			if overlap == "context data" {
				binary.LittleEndian.PutUint16(b[66:68], 16)
				binary.LittleEndian.PutUint32(b[68:72], 4)
			} else {
				binary.LittleEndian.PutUint16(b[44:46], 136)
				binary.LittleEndian.PutUint16(b[46:48], 4)
			}
			observer.recordCreate()
			if len(observation.creates) != 1 || !observation.creates[0].Truncated || len(observation.creates[0].Contexts) != 0 {
				t.Fatalf("overlapping data logged as metadata: %+v", observation.creates)
			}
		})
	}
}

func TestNativeWireObservationKeepsSessionSetupScalarsOnly(t *testing.T) {
	var observation nativeWireObservation
	observer := nativeFrameObserver{observation: &observation}
	frame := make([]byte, 4+88+32)
	binary.BigEndian.PutUint32(frame[:4], uint32(len(frame)-4))
	copy(frame[4:8], []byte{0xfe, 'S', 'M', 'B'})
	binary.LittleEndian.PutUint16(frame[16:18], 1)
	binary.LittleEndian.PutUint64(frame[44:52], 99)
	b := frame[68:92]
	b[2], b[3] = 1, 3
	binary.LittleEndian.PutUint32(b[4:8], 5)
	binary.LittleEndian.PutUint32(b[8:12], 7)
	binary.LittleEndian.PutUint16(b[12:14], 88)
	binary.LittleEndian.PutUint16(b[14:16], 32)
	binary.LittleEndian.PutUint64(b[16:24], 42)
	for i := 92; i < len(frame); i++ {
		frame[i] = 0xcc
	}
	for _, v := range frame {
		observer.observe([]byte{v})
	}
	if len(observation.operations) != 1 {
		t.Fatalf("session evidence=%+v", observation.operations)
	}
	o := observation.operations[0]
	if o.Flags != 1 || o.SecurityMode != 3 || o.Capabilities != 5 || o.Channel != 7 || o.PreviousSessionID != 42 || o.HeaderSessionID != 99 || o.TokenOffset != 88 || o.TokenLength != 32 || o.BodyLength != 56 {
		t.Fatalf("session scalars=%+v", o)
	}
	if observer.operationBytes != 24 || observer.operation != [24]byte(b) {
		t.Fatal("session capture includes token payload")
	}
}

func TestNativeSessionSetupObservationRejectsTokenOverlap(t *testing.T) {
	for _, tc := range []struct {
		name           string
		offset, length uint16
		next           uint32
	}{
		{"overlap previous session", 80, 32, 0}, {"past frame", 88, 33, 0}, {"past compound member", 88, 32, 96},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var observation nativeWireObservation
			observer := nativeFrameObserver{observation: &observation}
			frame := make([]byte, 4+120)
			binary.BigEndian.PutUint32(frame[:4], 120)
			copy(frame[4:8], []byte{0xfe, 'S', 'M', 'B'})
			binary.LittleEndian.PutUint16(frame[16:18], 1)
			binary.LittleEndian.PutUint32(frame[24:28], tc.next)
			binary.LittleEndian.PutUint16(frame[80:82], tc.offset)
			binary.LittleEndian.PutUint16(frame[82:84], tc.length)
			copy(frame[84:], "token-private-material")
			observer.observe(frame)
			if len(observation.operations) != 0 || observer.operation != [24]byte{} {
				t.Fatalf("invalid token span retained session scalars: %+v", observation.operations)
			}
		})
	}
}

func (c *nativeFrameObserver) recordCreateReply() {
	b := c.create[:c.createBytes]
	h := nativeCreateReplyObservation{MessageID: binary.LittleEndian.Uint64(c.header[24:32])}
	if len(b) < 88 {
		h.Truncated = true
	} else {
		h.Oplock = b[2]
		offset, length := uint64(binary.LittleEndian.Uint32(b[80:84])), uint64(binary.LittleEndian.Uint32(b[84:88]))
		if length != 0 {
			if offset < 152 || offset+length > uint64(64+len(b)) {
				h.Truncated = true
			} else {
				contexts := b[offset-64 : offset-64+length]
				for count := 0; len(contexts) > 0; count++ {
					if len(contexts) < 16 || count == 16 {
						h.Truncated = true
						break
					}
					next := int(binary.LittleEndian.Uint32(contexts[:4]))
					end := len(contexts)
					if next != 0 {
						if next < 16 || next > end {
							h.Truncated = true
							break
						}
						end = next
					}
					name, size := int(binary.LittleEndian.Uint16(contexts[4:6])), int(binary.LittleEndian.Uint16(contexts[6:8]))
					data, dataSize := uint64(binary.LittleEndian.Uint16(contexts[10:12])), uint64(binary.LittleEndian.Uint32(contexts[12:16]))
					if name < 16 || size == 0 || size > 32 || name+size > end || dataSize != 0 && (data < 16 || data+dataSize > uint64(end) || uint64(name) < data+dataSize && data < uint64(name+size)) {
						h.Truncated = true
						break
					}
					if string(contexts[name:name+size]) == "RqLs" {
						if dataSize != 32 && dataSize != 52 {
							h.Truncated = true
							break
						}
						v := contexts[data : data+dataSize]
						lease := nativeLeaseObservation{Version: 1, State: binary.LittleEndian.Uint32(v[16:20]), Duration: binary.LittleEndian.Uint64(v[24:32])}
						if dataSize == 52 {
							lease.Version = 2
							lease.Epoch = binary.LittleEndian.Uint16(v[48:50])
						}
						h.Leases = append(h.Leases, lease)
					}
					if next == 0 {
						break
					}
					contexts = contexts[next:]
				}
			}
		}
	}
	clear(c.create[:])
	if h.Truncated || h.Oplock != 0 && h.Oplock != 0xff || h.Oplock == 0xff && len(h.Leases) == 0 {
		c.observation.unsafeCaching.Store(true)
	}
	for _, lease := range h.Leases {
		c.observation.leaseResponses.Add(1)
		if lease.State != 0 || lease.Duration != 0 || h.Oplock != 0xff {
			c.observation.unsafeCaching.Store(true)
		}
	}
	c.observation.mu.Lock()
	c.observation.createReplies = nativeRetainLast(c.observation.createReplies, h, 8)
	c.observation.mu.Unlock()
}

func TestNativeWireObservationDecodesZeroRightsLeaseResponses(t *testing.T) {
	for _, size := range []int{32, 52} {
		t.Run(map[int]string{32: "v1", 52: "v2"}[size], func(t *testing.T) {
			var observation nativeWireObservation
			observer := nativeFrameObserver{observation: &observation, response: true}
			frame := make([]byte, 4+152+24+size)
			binary.BigEndian.PutUint32(frame[:4], uint32(len(frame)-4))
			copy(frame[4:8], []byte{0xfe, 'S', 'M', 'B'})
			binary.LittleEndian.PutUint16(frame[16:18], 5)
			b := frame[68:]
			b[2] = 0xff
			binary.LittleEndian.PutUint32(b[80:84], 152)
			binary.LittleEndian.PutUint32(b[84:88], uint32(24+size))
			ctx := b[88:]
			binary.LittleEndian.PutUint16(ctx[4:6], 16)
			binary.LittleEndian.PutUint16(ctx[6:8], 4)
			binary.LittleEndian.PutUint16(ctx[10:12], 24)
			binary.LittleEndian.PutUint32(ctx[12:16], uint32(size))
			copy(ctx[16:20], "RqLs")
			if size == 52 {
				binary.LittleEndian.PutUint16(ctx[72:74], 19)
			}
			for _, v := range frame {
				observer.observe([]byte{v})
			}
			if observation.leaseResponses.Load() != 1 || observation.unsafeCaching.Load() || len(observation.createReplies) != 1 {
				t.Fatalf("lease evidence=%+v unsafe=%v", observation.createReplies, observation.unsafeCaching.Load())
			}
			lease := observation.createReplies[0].Leases[0]
			version := 1
			epoch := uint16(0)
			if size == 52 {
				version = 2
				epoch = 19
			}
			if lease.Version != version || lease.State != 0 || lease.Duration != 0 || lease.Epoch != epoch {
				t.Fatalf("lease=%+v", lease)
			}
			binary.LittleEndian.PutUint32(ctx[40:44], 1)
			observer.observe(frame)
			if !observation.unsafeCaching.Load() {
				t.Fatal("nonzero caching grant was ignored")
			}
		})
	}
}

func TestNativeWireObservationBoundsLeaseRepliesAndNegotiation(t *testing.T) {
	var observation nativeWireObservation
	observer := nativeFrameObserver{observation: &observation, response: true}
	frame := make([]byte, 4+92)
	binary.BigEndian.PutUint32(frame[:4], 92)
	copy(frame[4:8], []byte{0xfe, 'S', 'M', 'B'})
	binary.LittleEndian.PutUint16(frame[72:74], 0x311)
	binary.LittleEndian.PutUint32(frame[92:96], 0x26)
	observer.observe(frame)
	if len(observation.negotiations) != 1 || observation.negotiations[0].Dialect != 0x311 || observation.negotiations[0].Capabilities != 0x26 {
		t.Fatalf("negotiation=%+v", observation.negotiations)
	}
	observer.createBytes = 88
	observer.create[2] = 0xff
	binary.LittleEndian.PutUint32(observer.create[80:84], 152)
	binary.LittleEndian.PutUint32(observer.create[84:88], 65535)
	observer.recordCreateReply()
	if !observation.unsafeCaching.Load() || len(observation.createReplies) != 1 || !observation.createReplies[0].Truncated || len(observation.createReplies[0].Leases) != 0 {
		t.Fatalf("invalidlease=%+v", observation.createReplies)
	}
}

func TestNativeWireObservationCorrelatesFreshCreateName(t *testing.T) {
	var observation nativeWireObservation
	observer := nativeFrameObserver{observation: &observation, createBytes: 70}
	b := observer.create[:]
	binary.LittleEndian.PutUint16(b[44:46], 120)
	binary.LittleEndian.PutUint16(b[46:48], 14)
	for i, r := range "new.bin" {
		binary.LittleEndian.PutUint16(b[56+2*i:], uint16(r))
	}
	before := observation.createSequence.Load()
	observer.recordCreate()
	if len(observation.creates) != 1 || observation.creates[0].Sequence <= before || observation.creates[0].Name != "new.bin" {
		t.Fatalf("freshCREATE=%+v", observation.creates)
	}
}

func TestNativeLeaseResponseObservationRejectsContextDataOverlap(t *testing.T) {
	var observation nativeWireObservation
	observer := nativeFrameObserver{observation: &observation, response: true, createBytes: 144}
	b := observer.create[:]
	b[2] = 0xff
	binary.LittleEndian.PutUint32(b[80:84], 152)
	binary.LittleEndian.PutUint32(b[84:88], 56)
	ctx := b[88:]
	binary.LittleEndian.PutUint16(ctx[4:6], 24)
	binary.LittleEndian.PutUint16(ctx[6:8], 4)
	binary.LittleEndian.PutUint16(ctx[10:12], 24)
	binary.LittleEndian.PutUint32(ctx[12:16], 32)
	copy(ctx[24:], "RqLs")
	observer.recordCreateReply()
	if !observation.unsafeCaching.Load() || observation.leaseResponses.Load() != 0 || len(observation.createReplies[0].Leases) != 0 {
		t.Fatalf("overlapped lease payload decoded: %+v", observation.createReplies)
	}
}
