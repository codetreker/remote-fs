package wire

import "testing"

func TestLocks(t *testing.T) {
	p := requestPacket(Lock, 48, 72)
	le.PutUint16(p[66:68], 2)
	le.PutUint32(p[68:72], 0x123)
	p[72] = 17
	le.PutUint64(p[88:96], 10)
	le.PutUint64(p[96:104], 100)
	le.PutUint32(p[104:108], LockShared|LockFailImmediately)
	le.PutUint32(p[128:132], LockExclusive|LockFailImmediately)
	l, err := parseOne(t, p).Lock()
	if err != nil || len(l.Elements) != 2 || l.Sequence != 0x123 || l.FileID[0] != 17 || l.Elements[0].Offset != 10 || l.Elements[0].Length != 100 {
		t.Fatalf("locks: %+v %v", l, err)
	}
	for _, flags := range []uint32{0, LockShared | LockExclusive, LockUnlock, LockShared, 0xffffffff} {
		b := append([]byte(nil), p...)
		le.PutUint32(b[128:132], flags)
		if _, err := parseOne(t, b).Lock(); err == nil {
			t.Fatalf("invalid flags %x", flags)
		}
	}
	le.PutUint32(p[104:108], LockUnlock)
	le.PutUint32(p[128:132], LockUnlock)
	if _, err := parseOne(t, p).Lock(); err != nil {
		t.Fatal(err)
	}
	le.PutUint32(p[128:132], LockShared)
	if _, err := parseOne(t, p).Lock(); err == nil {
		t.Fatal("mixed unlock accepted")
	}
	le.PutUint16(p[66:68], 0)
	if _, err := parseOne(t, p).Lock(); err == nil {
		t.Fatal("zero locks accepted")
	}
	le.PutUint16(p[66:68], 3)
	if _, err := parseOne(t, p).Lock(); err == nil {
		t.Fatal("truncated locks accepted")
	}
	p = requestPacket(Lock, 48, 48)
	le.PutUint16(p[66:68], 1)
	le.PutUint32(p[104:108], LockExclusive)
	if _, err := parseOne(t, p).Lock(); err != nil {
		t.Fatalf("blocking lock: %v", err)
	}
}

func TestNotify(t *testing.T) {
	p := requestPacket(ChangeNotify, 32, 32)
	le.PutUint16(p[66:68], 1)
	le.PutUint32(p[68:72], 4096)
	p[72] = 2
	le.PutUint32(p[88:92], 0x123)
	n, err := parseOne(t, p).Notify()
	if err != nil || n.Flags != 1 || n.OutputLength != 4096 || n.FileID[0] != 2 || n.Filter != 0x123 {
		t.Fatalf("notify: %+v %v", n, err)
	}
}
