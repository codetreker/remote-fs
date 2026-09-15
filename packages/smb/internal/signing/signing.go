package signing

import (
	"crypto/aes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"sync"
)

var ErrKey = errors.New("SMB session key must contain at least 16 bytes")
var ErrSignature = errors.New("invalid SMB signature")
var ErrDestroyed = errors.New("SMB signing session has been destroyed")

type Session struct {
	mu        sync.RWMutex
	key       [16]byte
	destroyed bool
}

func Preauth(previous [64]byte, packet []byte) [64]byte {
	h := sha512.New()
	_, _ = h.Write(previous[:])
	_, _ = h.Write(packet)
	var out [64]byte
	copy(out[:], h.Sum(nil))
	return out
}

// DeriveKey implements the 128-bit SP800-108 counter-mode KDF used by SMB3.
// The label includes its terminating NUL, separately from the KDF separator.
func DeriveKey(key []byte, label []byte, context []byte) [16]byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte{0, 0, 0, 1})
	_, _ = h.Write(label)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(context)
	_, _ = h.Write([]byte{0, 0, 0, 128})
	var out [16]byte
	copy(out[:], h.Sum(nil))
	return out
}

func NewSession(preauth [64]byte, sessionKey []byte) (*Session, error) {
	if len(sessionKey) < 16 {
		return nil, ErrKey
	}
	return &Session{key: DeriveKey(sessionKey[:16], []byte("SMBSigningKey\x00"), preauth[:])}, nil
}

func NewSession300(sessionKey []byte) (*Session, error) {
	if len(sessionKey) < 16 {
		return nil, ErrKey
	}
	return &Session{key: DeriveKey(sessionKey[:16], []byte("SMB2AESCMAC\x00"), []byte("SmbSign\x00"))}, nil
}

func validPacket(packet []byte) bool {
	return len(packet) >= 64 && string(packet[:4]) == "\xfeSMB" && binary.LittleEndian.Uint16(packet[4:6]) == 64
}

func (s *Session) Sign(packet []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.destroyed {
		return ErrDestroyed
	}
	if !validPacket(packet) {
		return ErrSignature
	}
	flags := binary.LittleEndian.Uint32(packet[16:20])
	binary.LittleEndian.PutUint32(packet[16:20], flags|8)
	clear(packet[48:64])
	sum, err := CMAC(s.key[:], packet)
	if err != nil {
		return err
	}
	copy(packet[48:64], sum[:])
	return nil
}

func (s *Session) Verify(packet []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.destroyed {
		return ErrDestroyed
	}
	if !validPacket(packet) || binary.LittleEndian.Uint32(packet[16:20])&8 == 0 {
		return ErrSignature
	}
	var zero [16]byte
	sum, err := cmac(s.key[:], packet[:48], zero[:], packet[64:])
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare(sum[:], packet[48:64]) != 1 {
		return ErrSignature
	}
	return nil
}

// Destroy waits for active signatures before clearing the derived key.
func (s *Session) Destroy() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.key[:])
	s.destroyed = true
}

func CMAC(key, message []byte) ([16]byte, error) { return cmac(key, message) }

// Subkeys and final-block padding follow RFC4493 sections 2.3 and 2.4.
func cmac(key []byte, parts ...[]byte) ([16]byte, error) {
	var out [16]byte
	c, err := aes.NewCipher(key)
	if err != nil {
		return out, err
	}
	var k1, k2, block [16]byte
	c.Encrypt(k1[:], k1[:])
	double(&k1)
	k2 = k1
	double(&k2)
	n := 0
	for _, part := range parts {
		for _, b := range part {
			if n == 16 {
				c.Encrypt(block[:], block[:])
				n = 0
			}
			block[n] ^= b
			n++
		}
	}
	k := k1
	if n < 16 {
		k = k2
		block[n] ^= 0x80
	}
	for i := range block {
		block[i] ^= k[i]
	}
	c.Encrypt(out[:], block[:])
	return out, nil
}

func double(block *[16]byte) {
	carry := byte(0)
	for i := 15; i >= 0; i-- {
		next := block[i] >> 7
		block[i] = block[i]<<1 | carry
		carry = next
	}
	block[15] ^= 0x87 * (carry & 1)
}
