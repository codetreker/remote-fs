package signing

import (
	"bytes"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
)

func unhex(t *testing.T, value string) []byte {
	t.Helper()
	b, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDestroyDuringSigning(t *testing.T) {
	s, err := NewSession([64]byte{}, make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 80)
	copy(packet, "\xfeSMB")
	packet[4] = 64
	if err := s.Sign(packet); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copyPacket := bytes.Clone(packet)
			for range 20 {
				if err := s.Sign(copyPacket); err != nil && !errors.Is(err, ErrDestroyed) {
					t.Error(err)
				}
				if err := s.Verify(copyPacket); err != nil && !errors.Is(err, ErrDestroyed) {
					t.Error(err)
				}
			}
		}()
	}
	s.Destroy()
	wg.Wait()
	s.Destroy()
	if s.key != ([16]byte{}) {
		t.Fatal("derived key retained")
	}
	if err := s.Sign(packet); !errors.Is(err, ErrDestroyed) {
		t.Fatal(err)
	}
	if err := s.Verify(packet); !errors.Is(err, ErrDestroyed) {
		t.Fatal(err)
	}
}

func TestCMACVectors(t *testing.T) {
	// RFC4493 section 4 covers empty, full, partial and multiple AES blocks.
	key := unhex(t, "2b7e151628aed2a6abf7158809cf4f3c")
	message := unhex(t, "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710")
	for _, tt := range []struct {
		length int
		want   string
	}{{0, "bb1d6929e95937287fa37d129b756746"}, {16, "070a16b46b4d4144f79bdd9dd04a287c"}, {40, "dfa66747de9ae63030ca32611497c827"}, {64, "51f0bebf7e3b9d92fc49741779363cfe"}} {
		sum, err := CMAC(key, message[:tt.length])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(sum[:], unhex(t, tt.want)) {
			t.Fatalf("length %d: %x", tt.length, sum)
		}
		parts := [][]byte{message[:tt.length/2], message[tt.length/2 : tt.length]}
		split, err := cmac(key, parts...)
		if err != nil || split != sum {
			t.Fatalf("fragmented length %d: %x, %v", tt.length, split, err)
		}
	}
	if _, err := CMAC([]byte{1}, nil); err == nil {
		t.Fatal("accepted invalid key")
	}
}

func TestDerivation(t *testing.T) {
	key := make([]byte, 16)
	var preauth [64]byte
	for i := range key {
		key[i] = byte(i)
	}
	for i := range preauth {
		preauth[i] = byte(i)
	}
	derived := DeriveKey(key, []byte("SMBSigningKey\x00"), preauth[:])
	if got := hex.EncodeToString(derived[:]); got != "f7e5401ecc6e79ef9eab401b05004e4f" {
		t.Fatal(got)
	}
	s, err := NewSession(preauth, append(key, 1, 2))
	if err != nil || s.key != derived {
		t.Fatalf("session derivation: %v", err)
	}
	legacy, err := NewSession300(key)
	if err != nil || legacy.key == derived {
		t.Fatalf("legacy domain separation: %v", err)
	}
	if _, err := NewSession(preauth, key[:15]); !errors.Is(err, ErrKey) {
		t.Fatal(err)
	}
	if _, err := NewSession300(nil); !errors.Is(err, ErrKey) {
		t.Fatal(err)
	}
	h := Preauth([64]byte{}, []byte("abc"))
	if got := hex.EncodeToString(h[:]); got != "7682b6b84c6f692545a896ad210299bcc6f74e189d829e165739b11dd83b6f2c8cfacf27e812bf064df756d6235ac33c062b89ecb80f8215516dbbb844ce8480" {
		t.Fatal(got)
	}
}

func TestPacketSignature(t *testing.T) {
	s, err := NewSession([64]byte{}, make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 80)
	copy(packet, "\xfeSMB")
	packet[4] = 64
	packet[64] = 17
	if err := s.Verify(packet); !errors.Is(err, ErrSignature) {
		t.Fatal(err)
	}
	if err := s.Sign(packet); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(packet)
	if err := s.Verify(packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, packet) {
		t.Fatal("verify changed caller packet")
	}
	for i := range packet {
		packet[i] ^= 1
		if err := s.Verify(packet); !errors.Is(err, ErrSignature) {
			t.Fatalf("accepted corruption at %d: %v", i, err)
		}
		packet[i] ^= 1
	}
	if err := s.Sign(packet[:63]); !errors.Is(err, ErrSignature) {
		t.Fatal(err)
	}
	packet[0] = 0
	if err := s.Sign(packet); !errors.Is(err, ErrSignature) {
		t.Fatal(err)
	}
}
