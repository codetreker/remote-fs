package wire

import "testing"

func TestUTF16(t *testing.T) {
	for _, s := range []string{"", "abc", "目录\\😀"} {
		got, err := DecodeUTF16(EncodeUTF16(s))
		if err != nil || got != s {
			t.Fatalf("%q: %q %v", s, got, err)
		}
	}
	for _, data := range [][]byte{{1}, {0, 0}, {0, 0xd8}, {0, 0xdc}, {0, 0xd8, 65, 0}} {
		if _, err := DecodeUTF16(data); err == nil {
			t.Fatalf("accepted %x", data)
		}
	}
}

func TestNegotiate(t *testing.T) {
	p := requestPacket(Negotiate, 36, 38)
	le.PutUint16(p[66:68], 1)
	le.PutUint16(p[100:102], Dialect300)
	n, err := parseOne(t, p).Negotiate()
	if err != nil || len(n.Dialects) != 1 || n.Dialects[0] != Dialect300 {
		t.Fatalf("negotiate: %+v %v", n, err)
	}
	le.PutUint16(p[100:102], Dialect311)
	p = append(p, make([]byte, 18)...)
	le.PutUint32(p[92:96], 104)
	le.PutUint16(p[96:98], 1)
	le.PutUint16(p[104:106], 1)
	le.PutUint16(p[106:108], 6)
	copy(p[112:118], []byte{1, 0, 0, 0, 1, 0})
	n, err = parseOne(t, p).Negotiate()
	if err != nil || len(n.Contexts) != 1 || len(n.Contexts[0].Data) != 6 {
		t.Fatalf("contexts: %+v %v", n, err)
	}
	for _, change := range []func([]byte){func(b []byte) { le.PutUint16(b[66:68], 0) }, func(b []byte) { le.PutUint16(b[66:68], 0xffff) }, func(b []byte) { le.PutUint32(b[92:96], 105) }, func(b []byte) { le.PutUint32(b[92:96], 0xfffffff8) }, func(b []byte) { le.PutUint16(b[96:98], 17) }, func(b []byte) { le.PutUint16(b[106:108], 0xffff) }} {
		b := append([]byte(nil), p...)
		change(b)
		if _, err := parseOne(t, b).Negotiate(); err == nil {
			t.Fatal("invalid negotiate accepted")
		}
	}
	le.PutUint16(p[96:98], 2)
	p = append(p, make([]byte, 8)...)
	le.PutUint16(p[120:122], 1)
	if _, err := parseOne(t, p).Negotiate(); err == nil {
		t.Fatal("duplicate context accepted")
	}
}

func TestSessionSetupAndTree(t *testing.T) {
	p := requestPacket(SessionSetup, 25, 27)
	le.PutUint16(p[76:78], 88)
	le.PutUint16(p[78:80], 3)
	copy(p[88:], "abc")
	s, err := parseOne(t, p).SessionSetup()
	if err != nil || string(s.Token) != "abc" {
		t.Fatalf("token: %+v %v", s, err)
	}
	le.PutUint16(p[76:78], 80)
	if _, err := parseOne(t, p).SessionSetup(); err == nil {
		t.Fatal("token in header accepted")
	}
	path := EncodeUTF16("\\\\localhost\\share")
	p = requestPacket(TreeConnect, 9, 8+len(path))
	le.PutUint16(p[68:70], 72)
	le.PutUint16(p[70:72], uint16(len(path)))
	copy(p[72:], path)
	got, err := parseOne(t, p).TreePath()
	if err != nil || got != "\\\\localhost\\share" {
		t.Fatalf("path %q %v", got, err)
	}
	p[66] = 1
	if _, err := parseOne(t, p).TreePath(); err == nil {
		t.Fatal("extension accepted")
	}
	for _, cmd := range []uint16{Logoff, TreeDisconnect, Cancel, Echo} {
		if err := parseOne(t, requestPacket(cmd, 4, 4)).Empty(); err != nil {
			t.Fatal(err)
		}
	}
	if err := parseOne(t, requestPacket(Create, 4, 4)).Empty(); err == nil {
		t.Fatal("nonempty command accepted")
	}
}
