//go:build windows

package windows

import (
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

type nativeWireObservation struct {
	connections  atomic.Uint64
	writes       atomic.Uint64
	signedWrites atomic.Uint64
	readBytes    atomic.Uint64
	writtenBytes atomic.Uint64
	mu           sync.Mutex
	headers      []nativeHeaderObservation
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

// Only transport and SMB headers are observed. Authentication tokens and file
// payloads are never accumulated or logged by the observer.
type nativeObservedConnection struct {
	net.Conn
	received nativeFrameObserver
	sent     nativeFrameObserver
	writeMu  sync.Mutex
}

type nativeFrameObserver struct {
	observation *nativeWireObservation
	response    bool
	prefix      [4]byte
	prefixBytes int
	remaining   int
	header      [64]byte
	headerBytes int
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
				if len(c.observation.headers) < 16 {
					c.observation.headers = append(c.observation.headers, h)
				}
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
		data = data[n:]
		c.remaining -= n
	}
}
