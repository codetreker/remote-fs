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
	connections  atomic.Uint64
	writes       atomic.Uint64
	signedWrites atomic.Uint64
	readBytes    atomic.Uint64
	writtenBytes atomic.Uint64
	mu           sync.Mutex
	headers      []nativeHeaderObservation
	trees        []nativeTreeObservation
	creates      []nativeCreateObservation
	operations   []nativeOperationObservation
}

type nativeOperationObservation struct {
	MessageID       uint64
	Command         uint16
	InfoType, Class byte
	ControlCode     uint32
}

type nativeCreateObservation struct {
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

// Only bounded protocol metadata is logged. Body capture stays within the first
// TREE_CONNECT or CREATE compound member; context values are not logged.
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
	operation      [8]byte
	operationBytes int
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
		if !c.response && c.headerBytes == len(c.header) && binary.BigEndian.Uint32(c.header[:4]) == 0xfe534d42 && binary.LittleEndian.Uint16(c.header[12:14]) == 5 {
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
			if command == 14 || command == 16 || command == 11 {
				start, end := max(c.frameBytes, 64), min(c.frameBytes+n, 72)
				if next := int(binary.LittleEndian.Uint32(c.header[20:24])); next != 0 {
					end = min(end, next)
				}
				if start < end {
					copy(c.operation[start-64:end-64], data[start-c.frameBytes:end-c.frameBytes])
					c.operationBytes = end - 64
				}
			}
		}
		c.frameBytes += n
		data = data[n:]
		c.remaining -= n
		if c.remaining == 0 && c.operationBytes == 8 {
			h := nativeOperationObservation{MessageID: binary.LittleEndian.Uint64(c.header[24:32]), Command: binary.LittleEndian.Uint16(c.header[12:14])}
			switch h.Command {
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
		if c.remaining == 0 && c.createBytes >= 56 {
			c.recordCreate()
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
	h := nativeCreateObservation{MessageID: binary.LittleEndian.Uint64(c.header[24:32]), SecurityFlags: b[2], Oplock: b[3], Impersonation: binary.LittleEndian.Uint32(b[4:8]), Access: binary.LittleEndian.Uint32(b[24:28]), Attributes: binary.LittleEndian.Uint32(b[28:32]), ShareAccess: binary.LittleEndian.Uint32(b[32:36]), Disposition: binary.LittleEndian.Uint32(b[36:40]), Options: binary.LittleEndian.Uint32(b[40:44])}
	offset, length := uint64(binary.LittleEndian.Uint32(b[48:52])), uint64(binary.LittleEndian.Uint32(b[52:56]))
	nameOffset, nameLength := uint64(binary.LittleEndian.Uint16(b[44:46])), uint64(binary.LittleEndian.Uint16(b[46:48]))
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
