package smb

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type symlinkFile struct {
	storage.WindowsFile
	info    storage.WindowsSymlinkInfo
	target  string
	failure error
}

func (f *symlinkFile) ReadLink(context.Context) (storage.WindowsSymlinkInfo, error) {
	return f.info, f.failure
}
func (f *symlinkFile) SetLink(_ context.Context, target string, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	f.target = target
	return storage.WindowsActionResult{Action: id, State: storage.WindowsActionCompleted}, f.failure
}

func ioctlCommand(code uint32, input []byte) wire.Request {
	b := make([]byte, 56+len(input))
	smbLE.PutUint16(b, 57)
	smbLE.PutUint32(b[4:], code)
	b[8] = 1
	if len(input) > 0 {
		smbLE.PutUint32(b[24:], 120)
		smbLE.PutUint32(b[28:], uint32(len(input)))
		copy(b[56:], input)
	}
	smbLE.PutUint32(b[44:], 4096)
	smbLE.PutUint32(b[48:], 1)
	return fileRequest(wire.IOCTL, b)
}

func TestSymlinkTargetsRemainInsideVolume(t *testing.T) {
	location := storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "dir/link"}
	for _, tc := range []struct{ target, want string }{{"../file", "..\\file"}, {"/other/file", "..\\other\\file"}, {"/dir/file", "file"}, {"/dir", "."}, {"/", ".."}} {
		got, err := relativeLink(tc.target, location)
		if err != nil || got != tc.want {
			t.Fatalf("%q -> %q %v", tc.target, got, err)
		}
	}
	for _, target := range []string{"../../escape", "/../escape", "C:/local", "\\\\server\\share", "//outside"} {
		if _, err := relativeLink(target, location); err == nil {
			t.Fatalf("escaped with %q", target)
		}
	}
	if _, err := relativeLink("file", storage.WindowsNameInfo{State: storage.WindowsNameDetached}); err == nil {
		t.Fatal("detached relative substitution")
	}
	b, status := createFailure(&storage.WindowsSymlinkError{WindowsSymlinkInfo: storage.WindowsSymlinkInfo{Target: "../file", Location: location, Unparsed: "/😀"}, Err: syscall.ELOOP})
	if status != 0x8000002d || len(b) < 44 || smbLE.Uint16(b[30:]) != 6 {
		t.Fatalf("symlink error %x %x", status, b)
	}
	if _, status := createFailure(syscall.EACCES); status != statusDenied {
		t.Fatal(status)
	}
}

func TestReparseReadsAtomicTargetAndLocation(t *testing.T) {
	c, _, tr, _, _, _ := testConnection(t)
	f := &symlinkFile{info: storage.WindowsSymlinkInfo{Target: "/target", Location: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "dir/link"}}}
	tr.files.handles[wire.FileID{1}].file = f
	b, status := c.reparse(context.Background(), tr, ioctlCommand(fsctlGetReparsePoint, nil))
	if status != 0 {
		t.Fatalf("get %x", status)
	}
	target, relative, err := wire.ParseSymlinkReparse(b[48:])
	if err != nil || !relative || target != "..\\target" {
		t.Fatalf("target %q %v %v", target, relative, err)
	}
	data, err := wire.ReparseSymlinkData("..\\target")
	if err != nil {
		t.Fatal(err)
	}
	if _, status := c.reparse(context.Background(), tr, ioctlCommand(fsctlSetReparsePoint, data)); status != 0 || f.target != "../target" {
		t.Fatalf("set %x %q", status, f.target)
	}
	f.failure = &storage.WindowsError{Failure: storage.WindowsNotReparsePoint, Err: syscall.EINVAL}
	if _, status := c.reparse(context.Background(), tr, ioctlCommand(fsctlGetReparsePoint, nil)); status != 0xc0000275 {
		t.Fatal(status)
	}
	f.failure = nil
	data[0] = 0
	if _, status := c.reparse(context.Background(), tr, ioctlCommand(fsctlSetReparsePoint, data)); status != 0xc0000278 {
		t.Fatal(status)
	}
}

func TestSymlinkOpenReconciliationRetainsTargetAndRemainingPath(t *testing.T) {
	d, _, s, _ := commandDispatcher()
	info := storage.WindowsSymlinkInfo{Target: "target", Location: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "link"}}
	s.open = func(storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		return storage.WindowsOpenResult{}, syscall.EIO
	}
	s.result = storage.WindowsActionResult{State: storage.WindowsActionRejected, Errno: syscall.ELOOP, Symlink: &info}
	s.queryErr = syscall.ELOOP
	id, err := d.actionID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.open(context.Background(), storage.WindowsOpenRequest{}, id)
	var link *storage.WindowsSymlinkError
	if !errors.As(err, &link) || link.Target != "target" {
		t.Fatalf("receipt %v", err)
	}
	s.open = func(r storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		if r.Lookup.Name == "" {
			root := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 1, Mode: fs.ModeDir}}}}
			return storage.WindowsOpenResult{File: root, Attr: root.attr}, nil
		}
		return storage.WindowsOpenResult{}, &storage.WindowsSymlinkError{WindowsSymlinkInfo: info, Err: syscall.ELOOP}
	}
	_, _, err = d.resolve(context.Background(), "link\\child\\file")
	if !errors.As(err, &link) || link.Unparsed != "/child/file" {
		t.Fatalf("suffix %v", err)
	}
	a := storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 2, Mode: fs.ModeSymlink, Size: int64(len(info.Target))}, DOSAttributes: storage.WindowsDOSDirectory}}
	standard := encodeStandardInfo(a)
	if standard[21] != 1 || smbLE.Uint64(standard[8:]) != 0 || fileAttributes(a)&0x410 != 0x410 {
		t.Fatalf("directory reparse metadata %x", standard)
	}
	if strings.Contains(link.Unparsed, "\\") {
		t.Fatal("wire separator leaked into storage")
	}
}
