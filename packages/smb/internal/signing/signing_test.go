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
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestCMACVectors(t *testing.T) {
	// RFC 4493 section 4 covers empty, full, partial and multiple AES blocks.
	key := unhex(t, "2b7e151628aed2a6abf7158809cf4f3c")
	message := unhex(t, "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710")
	for _, test := range []struct {
		length int
		want   string
	}{{0, "bb1d6929e95937287fa37d129b756746"}, {16, "070a16b46b4d4144f79bdd9dd04a287c"}, {40, "dfa66747de9ae63030ca32611497c827"}, {64, "51f0bebf7e3b9d92fc49741779363cfe"}} {
		sum, err := CMAC(key, message[:test.length])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(sum[:], unhex(t, test.want)) {
			t.Fatalf("length %d: %x", test.length, sum)
		}
		fragmented, err := cmac(key, message[:test.length/2], message[test.length/2:test.length])
		if err != nil || fragmented != sum {
			t.Fatalf("fragmented length %d: %x, %v", test.length, fragmented, err)
		}
	}
	if _, err := CMAC([]byte{1}, nil); err == nil {
		t.Fatal("accepted invalid key")
	}
}

func TestDerivationAndPreauthentication(t *testing.T) {
	key := make([]byte, 16)
	var transcript [64]byte
	for index := range key {
		key[index] = byte(index)
	}
	for index := range transcript {
		transcript[index] = byte(index)
	}
	derived := DeriveKey(key, []byte("SMBSigningKey\x00"), transcript[:])
	if got := hex.EncodeToString(derived[:]); got != "f7e5401ecc6e79ef9eab401b05004e4f" {
		t.Fatal(got)
	}
	session, err := NewSession(transcript, append(key, 1, 2))
	if err != nil || session.key != derived {
		t.Fatalf("session derivation: %v", err)
	}
	if _, err := NewSession(transcript, key[:15]); !errors.Is(err, ErrKey) {
		t.Fatal(err)
	}
	hash := Preauth([64]byte{}, []byte("abc"))
	if got := hex.EncodeToString(hash[:]); got != "7682b6b84c6f692545a896ad210299bcc6f74e189d829e165739b11dd83b6f2c8cfacf27e812bf064df756d6235ac33c062b89ecb80f8215516dbbb844ce8480" {
		t.Fatal(got)
	}
	changed := Preauth([64]byte{}, []byte("abc\x00"))
	if changed == hash {
		t.Fatal("preauthentication ignored a raw trailing byte")
	}
}

func TestPacketSignature(t *testing.T) {
	session, err := NewSession([64]byte{}, make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 80)
	copy(packet, "\xfeSMB")
	packet[4] = 64
	packet[64] = 17
	if err := session.Verify(packet); !errors.Is(err, ErrSignature) {
		t.Fatal(err)
	}
	if err := session.Sign(packet); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(packet)
	if err := session.Verify(packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, packet) {
		t.Fatal("verification changed caller packet")
	}
	for index := range packet {
		packet[index] ^= 1
		if err := session.Verify(packet); !errors.Is(err, ErrSignature) {
			t.Fatalf("accepted corruption at %d: %v", index, err)
		}
		packet[index] ^= 1
	}
	if err := session.Sign(packet[:63]); !errors.Is(err, ErrSignature) {
		t.Fatal(err)
	}
	packet[0] = 0
	if err := session.Sign(packet); !errors.Is(err, ErrSignature) {
		t.Fatal(err)
	}
}

func TestDestroyDuringSigning(t *testing.T) {
	session, err := NewSession([64]byte{}, make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 80)
	copy(packet, "\xfeSMB")
	packet[4] = 64
	if err := session.Sign(packet); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			copyPacket := bytes.Clone(packet)
			for range 20 {
				if err := session.Sign(copyPacket); err != nil && !errors.Is(err, ErrDestroyed) {
					t.Error(err)
				}
				if err := session.Verify(copyPacket); err != nil && !errors.Is(err, ErrDestroyed) {
					t.Error(err)
				}
			}
		}()
	}
	session.Destroy()
	wait.Wait()
	session.Destroy()
	if session.key != ([16]byte{}) {
		t.Fatal("derived key retained")
	}
	if err := session.Sign(packet); !errors.Is(err, ErrDestroyed) {
		t.Fatal(err)
	}
	if err := session.Verify(packet); !errors.Is(err, ErrDestroyed) {
		t.Fatal(err)
	}
}
