//go:build windows

package windows

import (
	"encoding/binary"
	"net"
	"sync/atomic"
)

type nativeWireObservation struct {
	connections  atomic.Uint64
	writes       atomic.Uint64
	signedWrites atomic.Uint64
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
	return &nativeObservedConnection{Conn: connection, observation: l.observation}, nil
}

// Only transport and SMB headers are observed. Authentication tokens and file
// payloads are never accumulated or logged by the observer.
type nativeObservedConnection struct {
	net.Conn
	observation *nativeWireObservation
	prefix      [4]byte
	prefixBytes int
	remaining   int
	header      [64]byte
	headerBytes int
}

func (c *nativeObservedConnection) Read(buffer []byte) (int, error) {
	n, err := c.Conn.Read(buffer)
	c.observe(buffer[:n])
	return n, err
}
func (c *nativeObservedConnection) observe(data []byte) {
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
			if c.headerBytes == len(c.header) && c.header[0] == 0xfe && string(c.header[1:4]) == "SMB" && binary.LittleEndian.Uint16(c.header[12:14]) == 9 {
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
