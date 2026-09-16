package wire

import "testing"

func TestQueryRequests(t *testing.T) {
	p := requestPacket(QueryDirectory, 33, 34)
	p[66] = 37
	p[72] = 7
	le.PutUint16(p[88:90], 96)
	le.PutUint16(p[90:92], 2)
	le.PutUint32(p[92:96], 1024)
	copy(p[96:], EncodeUTF16("*"))
	d, err := parseOne(t, p).QueryDirectory()
	if err != nil || d.Class != 37 || d.FileID[0] != 7 || d.Pattern != "*" || d.OutputLength != 1024 {
		t.Fatalf("directory: %+v %v", d, err)
	}
	le.PutUint16(p[90:92], 3)
	if _, err := parseOne(t, p).QueryDirectory(); err == nil {
		t.Fatal("bad directory name accepted")
	}
	p = requestPacket(QueryInfo, 41, 42)
	p[66] = 1
	p[67] = 4
	p[88] = 42
	le.PutUint16(p[72:74], 104)
	le.PutUint32(p[76:80], 2)
	copy(p[104:], "hi")
	q, err := parseOne(t, p).QueryInfo()
	if err != nil || q.FileID[0] != 42 || q.Type != 1 || q.Class != 4 || string(q.Input) != "hi" {
		t.Fatalf("query: %+v %v", q, err)
	}
	p = requestPacket(SetInfo, 33, 34)
	p[66] = 1
	p[67] = 10
	p[80] = 77
	le.PutUint16(p[72:74], 96)
	le.PutUint32(p[68:72], 2)
	copy(p[96:], "hi")
	s, err := parseOne(t, p).SetInfo()
	if err != nil || s.FileID[0] != 77 || string(s.Input) != "hi" {
		t.Fatalf("set: %+v %v", s, err)
	}
	p = requestPacket(IOCTL, 57, 60)
	le.PutUint32(p[68:72], 0x1234)
	p[72] = 8
	le.PutUint32(p[88:92], 120)
	le.PutUint32(p[92:96], 2)
	le.PutUint32(p[100:104], 122)
	le.PutUint32(p[104:108], 2)
	copy(p[120:], "abcd")
	i, err := parseOne(t, p).IOCTL()
	if err != nil || i.Code != 0x1234 || i.FileID[0] != 8 || string(i.Input) != "ab" || string(i.Output) != "cd" {
		t.Fatalf("ioctl: %+v %v", i, err)
	}
	le.PutUint32(p[88:92], 64)
	if _, err := parseOne(t, p).IOCTL(); err == nil {
		t.Fatal("header ioctl data accepted")
	}
}
