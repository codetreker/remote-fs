package wire

import (
	"bytes"
	"strings"
	"testing"
)

func TestSymlinkReparse(t *testing.T) {
	for _, target := range []string{"file", "..\\目录\\😀"} {
		data, err := ReparseSymlinkData(target)
		if err != nil {
			t.Fatal(err)
		}
		got, relative, err := ParseSymlinkReparse(data)
		if err != nil || got != target || !relative {
			t.Fatalf("reparse %q %t %v", got, relative, err)
		}
		le.PutUint32(data[16:20], 0)
		got, relative, err = ParseSymlinkReparse(data)
		if err != nil || got != target || relative {
			t.Fatalf("absolute flag %q %t %v", got, relative, err)
		}
	}
	for _, target := range []string{"", "\\file", "C:relative", "C:\\absolute", "forward/slash", "nul\x00", "\xff", strings.Repeat("a", 8192), strings.Repeat("a", maxReparseBytes+1)} {
		if _, err := ReparseSymlinkData(target); err == nil {
			t.Fatalf("accepted bad target %q", target)
		}
	}
	valid, err := ReparseSymlinkData("file")
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]byte){
		func(b []byte) { b[0] = 0 }, func(b []byte) { le.PutUint16(b[4:6], 0) }, func(b []byte) { le.PutUint16(b[8:10], 1) }, func(b []byte) { le.PutUint16(b[8:10], 0xfffe) }, func(b []byte) { le.PutUint16(b[10:12], 3) }, func(b []byte) { le.PutUint16(b[12:14], 0xfffe) }, func(b []byte) { le.PutUint32(b[16:20], 2) }, func(b []byte) { le.PutUint16(b[20:22], 0) }, func(b []byte) { le.PutUint16(b[14:16], 3) }, func(b []byte) { le.PutUint16(b[10:12], 0) }, func(b []byte) { le.PutUint16(b[20:22], '\\') },
	} {
		b := bytes.Clone(valid)
		mutate(b)
		if _, _, err := ParseSymlinkReparse(b); err == nil {
			t.Fatal("accepted malformed reparse")
		}
	}
	for _, b := range [][]byte{nil, valid[:19], append(bytes.Clone(valid), 0), make([]byte, maxReparseBytes+1)} {
		if _, _, err := ParseSymlinkReparse(b); err == nil {
			t.Fatal("accepted invalid length")
		}
	}
}

func TestSymlinkErrorResponse(t *testing.T) {
	name := "..\\😀"
	unparsed := "\\目录\\f"
	body, err := SymlinkErrorResponseBody(name, unparsed)
	if err != nil {
		t.Fatal(err)
	}
	if le.Uint16(body) != 9 || body[2] != 1 || int(le.Uint32(body[4:8])) != len(body)-8 || int(le.Uint32(body[8:12])) != len(body)-16 || le.Uint32(body[12:16]) != 0 {
		t.Fatalf("error envelope %x", body)
	}
	p := body[16:]
	if int(le.Uint32(p)) != len(p)-4 || le.Uint32(p[4:8]) != 0x4c4d5953 || le.Uint32(p[8:12]) != SymlinkReparseTag || int(le.Uint16(p[12:14])) != len(p)-16 || int(le.Uint16(p[14:16])) != len(EncodeUTF16(unparsed)) || le.Uint32(p[24:28]) != 1 {
		t.Fatalf("symlink error %x", p)
	}
	n := len(EncodeUTF16(name))
	if len(p) != 28+n || !bytes.Equal(p[28:], EncodeUTF16(name)) || le.Uint16(p[16:18]) != 0 || le.Uint16(p[20:22]) != 0 || int(le.Uint16(p[18:20])) != n || int(le.Uint16(p[22:24])) != n {
		t.Fatal("target encoding corrupted")
	}
	for _, args := range [][2]string{{"\\absolute", ""}, {"file", "\x00"}, {"file", "\xff"}, {"file", strings.Repeat("a", 32768)}} {
		if _, err := SymlinkErrorResponseBody(args[0], args[1]); err == nil {
			t.Fatal("bad symlink error accepted")
		}
	}
}

func TestSymlinkTargetLimit(t *testing.T) {
	// ASCII uses twice as many UTF-16 bytes as UTF-8 bytes; non-ASCII targets
	// within the same backend byte limit consume no more reparse space.
	target := strings.Repeat("a", 4096)
	data, err := ReparseSymlinkData(target)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 20+8192 || le.Uint16(data[8:10]) != 0 || le.Uint16(data[12:14]) != 0 {
		t.Fatal("names do not share one buffer")
	}
	got, relative, err := ParseSymlinkReparse(data)
	if err != nil || !relative || got != target {
		t.Fatalf("target limit: %v", err)
	}
	body, err := SymlinkErrorResponseBody(target, "\\suffix")
	if err != nil || len(body) != 44+8192 {
		t.Fatalf("error target limit: %v", err)
	}
	maximum := strings.Repeat("a", (maxReparseBytes-20)/2)
	if _, err := ReparseSymlinkData(maximum); err != nil {
		t.Fatal(err)
	}
	if _, err := ReparseSymlinkData(maximum + "a"); err == nil {
		t.Fatal("accepted overflow")
	}
}

func FuzzSymlinkReparse(f *testing.F) {
	for _, target := range []string{"file", "..\\😀"} {
		b, err := ReparseSymlinkData(target)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) { _, _, _ = ParseSymlinkReparse(data) })
}
