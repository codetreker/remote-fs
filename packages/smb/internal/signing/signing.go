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

var (
	ErrKey       = errors.New("SMB session key must contain at least 16 bytes")
	ErrSignature = errors.New("invalid SMB signature")
	ErrDestroyed = errors.New("SMB signing session has been destroyed")
)

type Session struct {
	mu        sync.RWMutex
	key       [16]byte
	destroyed bool
}

// Preauth extends the SMB 3.1.1 transcript with exact raw wire bytes.
func Preauth(previous [64]byte, packet []byte) [64]byte {
	hash := sha512.New()
	_, _ = hash.Write(previous[:])
	_, _ = hash.Write(packet)
	var result [64]byte
	copy(result[:], hash.Sum(nil))
	return result
}

// DeriveKey implements the 128-bit SP800-108 counter-mode KDF used by SMB3.
// SMB passes label buffers that include their terminating NUL; the KDF then
// adds its own separator byte.
func DeriveKey(key, label, context []byte) [16]byte {
	hash := hmac.New(sha256.New, key)
	_, _ = hash.Write([]byte{0, 0, 0, 1})
	_, _ = hash.Write(label)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(context)
	_, _ = hash.Write([]byte{0, 0, 0, 128})
	var result [16]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func NewSession(preauth [64]byte, sessionKey []byte) (*Session, error) {
	if len(sessionKey) < 16 {
		return nil, ErrKey
	}
	return &Session{key: DeriveKey(sessionKey[:16], []byte("SMBSigningKey\x00"), preauth[:])}, nil
}

func validPacket(packet []byte) bool {
	return len(packet) >= 64 && string(packet[:4]) == "\xfeSMB" && binary.LittleEndian.Uint16(packet[4:6]) == 64
}

func (session *Session) Sign(packet []byte) error {
	session.mu.RLock()
	defer session.mu.RUnlock()
	if session.destroyed {
		return ErrDestroyed
	}
	if !validPacket(packet) {
		return ErrSignature
	}
	flags := binary.LittleEndian.Uint32(packet[16:20])
	binary.LittleEndian.PutUint32(packet[16:20], flags|8)
	clear(packet[48:64])
	sum, err := CMAC(session.key[:], packet)
	if err != nil {
		return err
	}
	copy(packet[48:64], sum[:])
	return nil
}

func (session *Session) Verify(packet []byte) error {
	session.mu.RLock()
	defer session.mu.RUnlock()
	if session.destroyed {
		return ErrDestroyed
	}
	if !validPacket(packet) || binary.LittleEndian.Uint32(packet[16:20])&8 == 0 {
		return ErrSignature
	}
	var zero [16]byte
	sum, err := cmac(session.key[:], packet[:48], zero[:], packet[64:])
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare(sum[:], packet[48:64]) != 1 {
		return ErrSignature
	}
	return nil
}

// Destroy waits for active signing and verification before clearing the key.
func (session *Session) Destroy() {
	session.mu.Lock()
	defer session.mu.Unlock()
	clear(session.key[:])
	session.destroyed = true
}

func CMAC(key, message []byte) ([16]byte, error) { return cmac(key, message) }

// Subkeys and final-block padding follow RFC 4493 sections 2.3 and 2.4.
func cmac(key []byte, parts ...[]byte) ([16]byte, error) {
	var result [16]byte
	cipher, err := aes.NewCipher(key)
	if err != nil {
		return result, err
	}
	var firstSubkey, secondSubkey, block [16]byte
	cipher.Encrypt(firstSubkey[:], firstSubkey[:])
	double(&firstSubkey)
	secondSubkey = firstSubkey
	double(&secondSubkey)
	used := 0
	for _, part := range parts {
		for _, value := range part {
			if used == len(block) {
				cipher.Encrypt(block[:], block[:])
				used = 0
			}
			block[used] ^= value
			used++
		}
	}
	finalSubkey := firstSubkey
	if used < len(block) {
		finalSubkey = secondSubkey
		block[used] ^= 0x80
	}
	for index := range block {
		block[index] ^= finalSubkey[index]
	}
	cipher.Encrypt(result[:], block[:])
	return result, nil
}

func double(block *[16]byte) {
	carry := byte(0)
	for index := len(block) - 1; index >= 0; index-- {
		next := block[index] >> 7
		block[index] = block[index]<<1 | carry
		carry = next
	}
	block[len(block)-1] ^= 0x87 * (carry & 1)
}
