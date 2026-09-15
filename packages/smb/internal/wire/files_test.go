package wire

import "testing"

func TestCreate(t *testing.T) {
	p := requestPacket(Create, 57, 56)
	out, err := parseOne(t, p).Create()
	if err != nil || out.Name != "" {
		t.Fatalf("root: %+v %v", out, err)
	}
	p = append(p, EncodeUTF16("file")...)
	le.PutUint16(p[108:110], 120)
	le.PutUint16(p[110:112], 8)
	le.PutUint32(p[88:92], 0x123)
	le.PutUint32(p[96:100], 7)
	ctx := make([]byte, 24)
	le.PutUint16(ctx[4:6], 16)
	le.PutUint16(ctx[6:8], 4)
	copy(ctx[16:20], "MxAc")
	le.PutUint16(ctx[10:12], 20)
	le.PutUint32(ctx[12:16], 4)
	copy(ctx[20:], []byte{1, 2, 3, 4})
	le.PutUint32(p[112:116], 128)
	le.PutUint32(p[116:120], 24)
	p = append(p, ctx...)
	out, err = parseOne(t, p).Create()
	if err != nil || out.Name != "file" || out.DesiredAccess != 0x123 || out.ShareAccess != 7 || len(out.Contexts) != 1 || string(out.Contexts[0].Name) != "MxAc" {
		t.Fatalf("create: %+v %v", out, err)
	}
	for _, change := range []func([]byte){func(b []byte) { le.PutUint16(b[108:110], 64) }, func(b []byte) { le.PutUint16(b[110:112], 0xffff) }, func(b []byte) { le.PutUint32(b[112:116], 129) }, func(b []byte) { le.PutUint32(b[116:120], 0xffffffff) }, func(b []byte) { le.PutUint16(b[132:134], 0) }, func(b []byte) { le.PutUint16(b[138:140], 0) }, func(b []byte) { le.PutUint32(b[128:132], 16) }} {
		b := append([]byte(nil), p...)
		change(b)
		if _, err := parseOne(t, b).Create(); err == nil {
			t.Fatal("malformed create accepted")
		}
	}
	le.PutUint32(p[128:132], 24)
	le.PutUint32(p[116:120], 48)
	p = append(p, ctx...)
	out, err = parseOne(t, p).Create()
	if err != nil || len(out.Contexts) != 2 {
		t.Fatalf("create chain: %+v %v", out, err)
	}
}

func TestReadWriteAndFileID(t *testing.T) {
	p := requestPacket(Read, 49, 48)
	p[80] = 99
	le.PutUint32(p[68:72], 4096)
	le.PutUint64(p[72:80], 1234)
	r, err := parseOne(t, p).Read()
	if err != nil || r.FileID[0] != 99 || r.Offset != 1234 || r.Length != 4096 {
		t.Fatalf("read: %+v %v", r, err)
	}
	le.PutUint16(p[110:112], 2)
	if _, err := parseOne(t, p).Read(); err == nil {
		t.Fatal("missing channel accepted")
	}
	p = requestPacket(Write, 49, 51)
	le.PutUint16(p[66:68], 112)
	le.PutUint32(p[68:72], 3)
	le.PutUint64(p[72:80], 17)
	p[80] = 42
	copy(p[112:], "abc")
	w, err := parseOne(t, p).Write()
	if err != nil || w.Offset != 17 || w.FileID[0] != 42 || string(w.Data) != "abc" {
		t.Fatalf("write: %+v %v", w, err)
	}
	le.PutUint32(p[68:72], 0xffffffff)
	if _, err := parseOne(t, p).Write(); err == nil {
		t.Fatal("oversized data accepted")
	}
	for _, cmd := range []uint16{Close, Flush} {
		p = requestPacket(cmd, 24, 24)
		p[72] = 55
		id, err := parseOne(t, p).FileID()
		if err != nil || id[0] != 55 {
			t.Fatalf("id %x %v", id, err)
		}
	}
	if _, err := parseOne(t, requestPacket(Echo, 4, 4)).FileID(); err == nil {
		t.Fatal("unexpected command accepted")
	}
}

func TestEveryDecoderRejectsWrongStructure(t *testing.T) {
	for _, cmd := range []uint16{Negotiate, SessionSetup, TreeConnect, Create, Read, Write, Close, Flush, Lock, QueryDirectory, QueryInfo, SetInfo, IOCTL, ChangeNotify, Echo} {
		r := parseOne(t, requestPacket(cmd, 0, 64))
		if decode(r) == nil {
			t.Fatalf("command %d accepted wrong structure", cmd)
		}
	}
}

func decode(r Request) error {
	var err error
	switch r.Header.Command {
	case Negotiate:
		_, err = r.Negotiate()
	case SessionSetup:
		_, err = r.SessionSetup()
	case TreeConnect:
		_, err = r.TreePath()
	case Create:
		var create CreateRequest
		create, err = r.Create()
		if err == nil {
			_, _, err = ParseLease(create)
		}
	case Read:
		_, err = r.Read()
	case Write:
		_, err = r.Write()
	case Close, Flush:
		_, err = r.FileID()
	case Lock:
		_, err = r.Lock()
	case QueryDirectory:
		_, err = r.QueryDirectory()
	case QueryInfo:
		_, err = r.QueryInfo()
	case SetInfo:
		_, err = r.SetInfo()
	case IOCTL:
		_, err = r.IOCTL()
	case ChangeNotify:
		_, err = r.Notify()
	case OplockBreak:
		_, err = r.LeaseAck()
	default:
		err = r.Empty()
	}
	return err
}

func FuzzRequestDecoders(f *testing.F) {
	for _, tt := range []struct {
		command, size uint16
		length        int
	}{{Negotiate, 36, 38}, {SessionSetup, 25, 24}, {TreeConnect, 9, 8}, {Create, 57, 56}, {Read, 49, 48}, {Write, 49, 48}, {Close, 24, 24}, {Flush, 24, 24}, {Lock, 48, 48}, {QueryDirectory, 33, 32}, {QueryInfo, 41, 40}, {SetInfo, 33, 32}, {IOCTL, 57, 56}, {ChangeNotify, 32, 32}, {Cancel, 4, 4}} {
		f.Add(requestPacket(tt.command, tt.size, tt.length))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		rs, err := ParseFrame(data, Limits{MaxBytes: 1 << 16, MaxCommands: 8, MaxContexts: 8})
		if err != nil {
			return
		}
		for _, r := range rs {
			_ = decode(r)
		}
	})
}
