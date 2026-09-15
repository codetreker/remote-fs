package smb

import (
	"context"
	"errors"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestSecurityDescriptorContainsOnlyVerifiedSIDAndGrantedRights(t *testing.T) {
	c, s, tr, _, _, _ := testConnection(t)
	b := make([]byte, 40)
	smbLE.PutUint16(b, 41)
	b[2] = 3
	smbLE.PutUint32(b[4:], 4096)
	smbLE.PutUint32(b[16:], 7)
	b[24] = 1
	r := fileRequest(wire.QueryInfo, b)
	ctx := WithPrincipal(context.Background(), s.principal)
	response, status := c.securityInfo(ctx, tr, r)
	if status != 0 {
		t.Fatalf("security %x", status)
	}
	d := response[8:]
	owner := smbLE.Uint32(d[4:])
	group := smbLE.Uint32(d[8:])
	acl := smbLE.Uint32(d[16:])
	sid, err := encodeSID(s.principal.SID)
	if err != nil {
		t.Fatal(err)
	}
	if string(d[owner:owner+uint32(len(sid))]) != string(sid) || string(d[group:group+uint32(len(sid))]) != string(sid) || smbLE.Uint32(d[acl+12:]) != 0x1201ff {
		t.Fatal("descriptor changed identity or access")
	}
	b[24] = 2
	if _, status := c.securityInfo(ctx, tr, fileRequest(wire.QueryInfo, b)); status != fileClosed {
		t.Fatal(status)
	}
	b[24] = 1
	smbLE.PutUint32(b[4:], 1)
	if _, status := c.securityInfo(ctx, tr, fileRequest(wire.QueryInfo, b)); status != fileBufferTooSmall {
		t.Fatal(status)
	}
	for _, bad := range []string{"", "other", "S-2-5-1", "S-1-x-1", "S-1-5-x"} {
		if _, err := encodeSID(bad); err == nil {
			t.Fatalf("accepted SID %q", bad)
		}
	}
}

func TestSecurityQueryNeedsOnlyReadControlAndReportsRequiredLength(t *testing.T) {
	c, s, tr, f, _, _ := testConnection(t)
	file := &deniedStatFile{commandFile: f.commandFile}
	h := tr.files.handles[wire.FileID{1}]
	h.file = file
	h.access = 0x20000
	request := queryCommand(wire.FileID{1}, 3, 0, 0)
	smbLE.PutUint32(request.Body[16:], 7)
	ctx := WithPrincipal(context.Background(), s.principal)
	short, status := c.securityInfo(ctx, tr, request)
	if status != fileBufferTooSmall || len(short) != 20 || short[2] != 1 || smbLE.Uint32(short[4:]) != 12 || smbLE.Uint32(short[8:]) != 4 || smbLE.Uint32(short[12:]) != 0 {
		t.Fatalf("security sizing status%x body%x", status, short)
	}
	required := smbLE.Uint32(short[16:])
	if required == 0 {
		t.Fatal("missing required descriptor length")
	}
	smbLE.PutUint32(request.Body[4:], required)
	response, status := c.securityInfo(ctx, tr, request)
	if status != 0 || len(response) != 8+int(required) || file.statCalls != 0 {
		t.Fatalf("READ_CONTROL query status%x len%d required%d statcalls%d", status, len(response), required, file.statCalls)
	}
}

func TestMaximumAllowedHonorsCurrentAuthorization(t *testing.T) {
	c, s, tr, _, _, _ := testConnection(t)
	c.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, a authz.AccessRequest) error {
		if a.WindowsOpen.Access&^(storage.WindowsReadData|storage.WindowsReadAttributes|storage.WindowsReadSecurity) != 0 {
			return authz.ErrDenied
		}
		return nil
	})
	r := createCommand("x", 0x02000000, 1)
	resolved, err := c.maximumAccess(WithPrincipal(context.Background(), s.principal), tr, r)
	if err != nil {
		t.Fatal(err)
	}
	request, err := resolved.Create()
	if err != nil {
		t.Fatal(err)
	}
	if request.DesiredAccess != 0x20081 {
		t.Fatalf("granted %x", request.DesiredAccess)
	}
	c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return errors.New("policy unavailable") })
	if _, err := c.maximumAccess(context.Background(), tr, r); err == nil {
		t.Fatal("authorization outage granted access")
	}
	r = createCommand("x", 1, 1)
	if _, err := c.maximumAccess(context.Background(), tr, r); err != nil {
		t.Fatal("non-maximum request probed policy")
	}
}

func TestMandatoryNameAndVolumeInformation(t *testing.T) {
	d, f, _, id := commandDispatcher()
	d.backend = &sessionBackend{}
	f.attr.NameInfo = storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "parent/file"}
	for _, class := range []byte{9, 18} {
		b, status := encodeFileInfo(class, f.attr, 1, 0)
		if status != 0 || len(b) == 0 {
			t.Fatalf("class %d = %x", class, status)
		}
	}
	f.attr.NameInfo = storage.WindowsNameInfo{State: storage.WindowsNameDetached}
	if _, status := encodeFileInfo(9, f.attr, 1, 0); status != 0xc0000123 {
		t.Fatalf("detached = %x", status)
	}
	for _, class := range []byte{1, 11} {
		if b, status := d.filesystemInfo(context.Background(), class); status != 0 || len(b) == 0 {
			t.Fatalf("fs class%d = %x", class, status)
		}
	}
	b := make([]byte, 40)
	smbLE.PutUint16(b, 41)
	b[2] = 1
	b[3] = 59
	smbLE.PutUint32(b[4:], 1024)
	copy(b[24:], id[:])
	out, status := d.queryInfo(context.Background(), fileRequest(wire.QueryInfo, b))
	if status != 0 || smbLE.Uint64(out[8:]) != 0x123456789 || smbLE.Uint64(out[16:]) != f.attr.ID {
		t.Fatalf("file identity %x %x", status, out)
	}
}
