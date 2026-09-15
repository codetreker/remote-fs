package smb

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func requestContexts(t *testing.T, r wire.Request, contexts []wire.CreateContext) wire.Request {
	t.Helper()
	body := append([]byte(nil), r.Body...)
	body = append(body, make([]byte, (-len(body))&7)...)
	start := len(body)
	for i, c := range contexts {
		entry := make([]byte, (24+len(c.Data)+7)&^7)
		if i+1 < len(contexts) {
			smbLE.PutUint32(entry, uint32(len(entry)))
		}
		smbLE.PutUint16(entry[4:], 16)
		smbLE.PutUint16(entry[6:], uint16(len(c.Name)))
		smbLE.PutUint16(entry[10:], 24)
		smbLE.PutUint32(entry[12:], uint32(len(c.Data)))
		copy(entry[16:], c.Name)
		copy(entry[24:], c.Data)
		body = append(body, entry...)
	}
	smbLE.PutUint32(body[48:], uint32(64+start))
	smbLE.PutUint32(body[52:], uint32(len(body)-start))
	parsed, err := wire.ParseFrame(requestPacket(r.Header, body), wire.Limits{MaxBytes: 1 << 20, MaxCommands: 1, MaxContexts: 16})
	if err != nil {
		t.Fatal(err)
	}
	return parsed[0]
}

func TestCreateReturnsRequestedIdentityAndMaximalAccessContexts(t *testing.T) {
	d, f, s, _ := commandDispatcher()
	d.backend = &sessionBackend{}
	s.open = func(storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		return storage.WindowsOpenResult{File: f, Attr: f.attr, CreateAction: storage.WindowsOpened}, nil
	}
	r := requestContexts(t, createCommand("", 1, 1), []wire.CreateContext{{Name: []byte("MxAc")}, {Name: []byte("QFid")}, {Name: []byte("RqLs")}, {Name: []byte("DH2Q")}})
	ctx := context.WithValue(context.Background(), maximalAccessKey{}, func(storage.WindowsAttr) (uint32, error) { return 0x81, nil })
	body, status := d.create(ctx, r)
	if status != 0 {
		t.Fatalf("create %x", status)
	}
	if smbLE.Uint32(body[80:]) != 152 || smbLE.Uint32(body[84:]) != 88 {
		t.Fatalf("contexts %x", body[80:])
	}
	maximal := body[88:]
	if string(maximal[16:20]) != "MxAc" || smbLE.Uint32(maximal[24:]) != 0 || smbLE.Uint32(maximal[28:]) != 0x81 {
		t.Fatalf("maximal %x", maximal)
	}
	identity := maximal[smbLE.Uint32(maximal):]
	if string(identity[16:20]) != "QFid" || smbLE.Uint64(identity[24:]) != f.attr.ID || smbLE.Uint64(identity[32:]) != 0x123456789 {
		t.Fatalf("identity %x", identity)
	}
	if smbLE.Uint32(identity) != 0 {
		t.Fatal("granted unsupported caching or durability")
	}
}

func TestMaximalAccessContextDistinguishesUnchangedFromUnknown(t *testing.T) {
	a := storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{ChangeTime: time.Unix(10, 0)}}
	timestamp := make([]byte, 8)
	smbLE.PutUint64(timestamp, windowsTime(a.ChangeTime))
	r := wire.CreateRequest{Contexts: []wire.CreateContext{{Name: []byte("MxAc"), Data: timestamp}}}
	b, err := createContexts(context.Background(), r, a, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if smbLE.Uint32(b[24:]) != 0xc0000073 || smbLE.Uint32(b[28:]) != 0 {
		t.Fatal("unchanged maximal access was invented")
	}
	r.Contexts[0].Data = nil
	ctx := context.WithValue(context.Background(), maximalAccessKey{}, func(storage.WindowsAttr) (uint32, error) { return 0, syscall.EIO })
	b, err = createContexts(ctx, r, a, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if smbLE.Uint32(b[24:]) != statusIO || smbLE.Uint32(b[28:]) != 0 {
		t.Fatal("unknown maximal access became success")
	}
	c, _, tr, _, _, _ := testConnection(t)
	mask, err := c.maximalAccess(context.Background(), tr, a)
	if err != nil || mask != encodeAccess(storage.WindowsAllAccess) {
		t.Fatalf("maximal %x %v", mask, err)
	}
}

func TestCreateContextsEncodeOnlyZeroLeaseRights(t *testing.T) {
	for _, version := range []uint16{wire.LeaseVersion1, wire.LeaseVersion2} {
		request := wire.CreateRequest{Contexts: []wire.CreateContext{{Name: []byte("RqLs")}, {Name: []byte("QFid")}}}
		lease := &wire.LeaseResponse{Version: version, Key: [16]byte{9}, HasParent: true, ParentKey: [16]byte{7}, Epoch: 65535}
		contexts, err := createContexts(context.Background(), request, storage.WindowsAttr{}, 1, lease)
		if err != nil {
			t.Fatal(err)
		}
		if string(contexts[16:20]) != "RqLs" || contexts[24] != 9 || smbLE.Uint32(contexts[40:44]) != 0 {
			t.Fatal("lease response lost identity or granted caching rights")
		}
		next := int(smbLE.Uint32(contexts))
		if next == 0 || string(contexts[next+16:next+20]) != "QFid" {
			t.Fatal("lease context broke response context chaining")
		}
	}
	_, err := createContexts(context.Background(), wire.CreateRequest{Contexts: []wire.CreateContext{{Name: []byte("RqLs")}}}, storage.WindowsAttr{}, 0, &wire.LeaseResponse{})
	if err == nil {
		t.Fatal("invalid internal lease response was silently omitted")
	}
}
